//! Runtime helpers layered on top of `mango-core`.

mod ids;

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    time::SystemTime,
};

use mango_core::agent::{
    AgentIds, AgentSchema, BusWorker, Event, EventBus, EventPayload, EventStream, EventVisibility,
    Filter, StreamKey, Subscription, Worker,
};

pub use ids::*;

pub type BoxFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Publish an event with a fresh envelope.
///
/// # Errors
///
/// Returns an error if the underlying event bus rejects or fails to publish
/// the event.
pub async fn publish<S, B>(
    bus: &B,
    stream: StreamKey<S>,
    visibility: EventVisibility,
    payload: EventPayload<S>,
) -> Result<(), B::Error>
where
    S: AgentSchema,
    S::Ids: AgentIds<EventId = S::EventId>,
    B: EventBus<Event = Event<S>>,
{
    bus.publish(Event {
        id: S::next_event_id(),
        stream,
        visibility,
        occurred_at: SystemTime::now(),
        payload,
    })
    .await
}

/// Await the next event from a stream.
///
/// # Errors
///
/// Returns an error if polling the underlying stream yields an error.
pub async fn next_event<S>(stream: &mut S) -> Result<Option<S::Item>, S::Error>
where
    S: EventStream + Unpin,
{
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

/// Run two bus workers behind one slot.
#[derive(Debug, Clone)]
pub struct ConcurrentBusWorkers<W, A, B> {
    worker_id: W,
    left: A,
    right: B,
}

trait MergeSubscription: Sized {
    fn merge(left: Self, right: Self) -> Self;
}

impl<S> MergeSubscription for Subscription<S>
where
    S: AgentSchema,
{
    fn merge(left: Self, right: Self) -> Self {
        Self {
            streams: merge_filter(left.streams, right.streams),
            visibility: merge_filter(left.visibility, right.visibility),
        }
    }
}

fn merge_filter<T>(left: Filter<T>, right: Filter<T>) -> Filter<T>
where
    T: Eq,
{
    match (left, right) {
        (Filter::Any, _) | (_, Filter::Any) => Filter::Any,
        (Filter::Only(mut left_values), Filter::Only(right_values)) => {
            for value in right_values {
                if !left_values.contains(&value) {
                    left_values.push(value);
                }
            }

            Filter::Only(left_values)
        }
    }
}

impl<W, A, B> ConcurrentBusWorkers<W, A, B> {
    pub fn new(worker_id: impl Into<W>, left: A, right: B) -> Self {
        Self {
            worker_id: worker_id.into(),
            left,
            right,
        }
    }

    pub fn left(&self) -> &A {
        &self.left
    }

    pub fn right(&self) -> &B {
        &self.right
    }
}

impl<W, A, B> Worker for ConcurrentBusWorkers<W, A, B>
where
    W: Clone,
    A: Worker<WorkerId = W>,
    B: Worker<WorkerId = W, Subscription = A::Subscription>,
    A::Subscription: MergeSubscription,
{
    type WorkerId = W;
    type Subscription = A::Subscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        Self::Subscription::merge(self.left.subscription(), self.right.subscription())
    }
}

impl<S, W, A, C, B> BusWorker<S, B> for ConcurrentBusWorkers<W, A, C>
where
    S: AgentSchema<WorkerId = W>,
    W: Clone + Send + Sync,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>> + Clone + Send + Sync + 'static,
    Subscription<S>: MergeSubscription,
    A: BusWorker<S, B> + Send + Sync + 'static,
    C: BusWorker<S, B, Error = A::Error> + Send + Sync + 'static,
    A::Error: Send + 'static,
    A::Run: Send + 'static,
    C::Run: Send + 'static,
{
    type Error = A::Error;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: B) -> Self::Run {
        Box::pin(async move {
            tokio::try_join!(self.left.run(bus.clone()), self.right.run(bus))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use mango_core::agent::{AgentSchema, Filter, StreamKey, Subscription, Worker};

    use super::{ConcurrentBusWorkers, DefaultAgentIds, EngineId, ToolName};

    #[derive(Clone)]
    struct TestWorker {
        worker_id: &'static str,
        subscription: Subscription<TestSchema>,
    }

    impl Worker for TestWorker {
        type WorkerId = &'static str;
        type Subscription = Subscription<TestSchema>;

        fn worker_id(&self) -> Self::WorkerId {
            self.worker_id
        }

        fn subscription(&self) -> Self::Subscription {
            self.subscription.clone()
        }
    }

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
    fn concurrent_workers_merge_stream_filters() {
        let left = TestWorker {
            worker_id: "left",
            subscription: Subscription::for_stream(StreamKey::Session(
                <TestSchema as mango_core::agent::AgentSchema>::next_session_id(),
            )),
        };
        let right = TestWorker {
            worker_id: "right",
            subscription: Subscription::for_stream(StreamKey::Thread(
                <TestSchema as mango_core::agent::AgentSchema>::next_thread_id(),
            )),
        };
        let composite = ConcurrentBusWorkers::new("presentation", left, right);

        let Filter::Only(streams) = composite.subscription().streams else {
            panic!("expected merged stream filter");
        };

        assert_eq!(streams.len(), 2);
        assert!(
            streams
                .iter()
                .any(|stream| matches!(stream, StreamKey::Session(_)))
        );
        assert!(
            streams
                .iter()
                .any(|stream| matches!(stream, StreamKey::Thread(_)))
        );
        assert_eq!(composite.subscription().visibility, Filter::Any);
    }

    #[test]
    fn concurrent_workers_keep_any_filter_when_either_side_is_broad() {
        let left = TestWorker {
            worker_id: "left",
            subscription: Subscription::all(),
        };
        let right = TestWorker {
            worker_id: "right",
            subscription: Subscription::for_stream(StreamKey::Thread(
                <TestSchema as mango_core::agent::AgentSchema>::next_thread_id(),
            )),
        };
        let composite = ConcurrentBusWorkers::new("presentation", left, right);

        assert_eq!(composite.subscription(), Subscription::all());
    }
}
