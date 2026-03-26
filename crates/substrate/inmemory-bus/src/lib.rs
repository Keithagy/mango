use std::{
    future::{self, Ready},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use mango_core::agent::{
    AgentSchema, Event, EventBus, EventStream, EventVisibility, Filter, StreamKey, Subscription,
};
use tokio::sync::broadcast;
use tokio_stream::{
    Stream,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InMemoryEventBusError {
    Closed,
    Lagged(u64),
}

/// Broadcast-backed in-memory event bus.
#[must_use]
#[derive(Debug, Clone)]
pub struct InMemoryAgentBus<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
    sender: broadcast::Sender<Event<S>>,
    schema: PhantomData<S>,
}

impl<S> InMemoryAgentBus<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            schema: PhantomData,
        }
    }

    #[must_use]
    pub fn subscribe_raw(&self) -> broadcast::Receiver<Event<S>> {
        self.sender.subscribe()
    }
}

pub struct InMemoryEventStream<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
    inner: BroadcastStream<Event<S>>,
    subscription: Subscription<S>,
    schema: PhantomData<S>,
}

impl<S> Unpin for InMemoryEventStream<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
}

impl<S> EventStream for InMemoryEventStream<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
    type Item = Event<S>;
    type Error = InMemoryEventBusError;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    if matches_subscription::<S>(
                        &this.subscription,
                        &event.stream,
                        event.visibility,
                    ) {
                        return Poll::Ready(Ok(Some(event)));
                    }
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    return Poll::Ready(Err(InMemoryEventBusError::Lagged(skipped)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(None)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> EventBus for InMemoryAgentBus<S>
where
    S: AgentSchema,
    Event<S>: Clone + Send + 'static,
{
    type Event = Event<S>;
    type Subscription = Subscription<S>;
    type Error = InMemoryEventBusError;
    type Stream<'a>
        = InMemoryEventStream<S>
    where
        Self: 'a;
    type Publish<'a>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'a;

    fn publish(&self, event: Self::Event) -> Self::Publish<'_> {
        let _ = self.sender.send(event);
        future::ready(Ok(()))
    }

    fn subscribe(&self, subscription: Self::Subscription) -> Result<Self::Stream<'_>, Self::Error> {
        Ok(InMemoryEventStream {
            inner: BroadcastStream::new(self.sender.subscribe()),
            subscription,
            schema: PhantomData,
        })
    }
}

fn matches_subscription<S>(
    subscription: &Subscription<S>,
    stream: &StreamKey<S>,
    visibility: EventVisibility,
) -> bool
where
    S: AgentSchema,
{
    matches_filter(&subscription.streams, stream)
        && matches_filter(&subscription.visibility, &visibility)
}

fn matches_filter<T: PartialEq>(filter: &Filter<T>, value: &T) -> bool {
    match filter {
        Filter::Any => true,
        Filter::Only(values) => values.iter().any(|candidate| candidate == value),
    }
}

#[cfg(test)]
mod tests {
    use mango_core::agent::{AgentSchema, EventVisibility, Filter, StreamKey, Subscription};
    use mango_runtime_support::{DefaultAgentIds, EngineId, ToolName, WorkerId};

    use super::matches_subscription;

    #[derive(Debug, Clone)]
    struct TestSchema;

    impl AgentSchema for TestSchema {
        type Ids = DefaultAgentIds;
        type Surface = ();
        type InputKind = ();
        type Input = ();
        type InterruptDetail = ();
        type Directive = ();
        type Output = ();
        type ToolData = ();
        type Status = ();
        type CancellationDetail = ();
        type CompletionDetail = ();
        type EngineId = EngineId;
        type ToolName = ToolName;
    }

    #[test]
    fn matches_any_subscription() {
        let subscription: Subscription<TestSchema> = Subscription {
            streams: Filter::Any,
            visibility: Filter::Any,
        };

        assert!(matches_subscription::<TestSchema>(
            &subscription,
            &StreamKey::Global,
            EventVisibility::Internal,
        ));
    }

    #[test]
    fn filters_by_stream_key() {
        let subscription: Subscription<TestSchema> = Subscription {
            streams: Filter::Only(vec![StreamKey::Global]),
            visibility: Filter::Any,
        };

        assert!(matches_subscription::<TestSchema>(
            &subscription,
            &StreamKey::Global,
            EventVisibility::Internal,
        ));
        assert!(!matches_subscription::<TestSchema>(
            &subscription,
            &StreamKey::Worker(WorkerId::from("maintenance")),
            EventVisibility::Internal,
        ));
    }

    #[test]
    fn filters_by_visibility() {
        let subscription: Subscription<TestSchema> = Subscription {
            streams: Filter::Any,
            visibility: Filter::Only(vec![EventVisibility::UserVisible]),
        };

        assert!(matches_subscription::<TestSchema>(
            &subscription,
            &StreamKey::Global,
            EventVisibility::UserVisible,
        ));
        assert!(!matches_subscription::<TestSchema>(
            &subscription,
            &StreamKey::Global,
            EventVisibility::Internal,
        ));
    }
}
