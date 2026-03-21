//! Support types for the Mango examples.

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use mango_core::agent::{
    AgentIds, AgentRuntime, AgentSchema, AgentWorker, BusWorker, ErrorDescriptor, ErrorEvent,
    Event, EventBus, EventPayload, EventStream, EventVisibility, Filter, SessionContext,
    SessionWorker, StreamKey, Subscription,
};
pub use mango_runtime_support::{
    BoxFuture, ConcurrentBusWorkers, DefaultAgentIds, EngineId, InferenceRunId, StatusId,
    ToolCallId, ToolName, WorkerId, next_event, publish,
};
use mango_shim_inmemory::InMemoryAgentBus;
pub use mango_shim_inmemory::InMemoryEventBusError;

pub trait ExampleAppError: Sized {
    fn task_join(message: String) -> Self;
}

#[must_use]
#[derive(Debug)]
pub struct ExampleBus<S, E>
where
    S: AgentSchema + Clone + Sync,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
    inner: InMemoryAgentBus<S>,
    error: PhantomData<fn() -> E>,
}

impl<S, E> Clone for ExampleBus<S, E>
where
    S: AgentSchema + Clone + Sync,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            error: PhantomData,
        }
    }
}

impl<S, E> ExampleBus<S, E>
where
    S: AgentSchema + Clone + Sync,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: InMemoryAgentBus::new(capacity),
            error: PhantomData,
        }
    }
}

pub struct ExampleBusStream<'a, S, E>
where
    S: AgentSchema + Clone + Sync + 'a,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
    inner: <InMemoryAgentBus<S> as EventBus>::Stream<'a>,
    error: PhantomData<fn() -> E>,
}

impl<'a, S, E> Unpin for ExampleBusStream<'a, S, E>
where
    S: AgentSchema + Clone + Sync + 'a,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
}

impl<'a, S, E> EventStream for ExampleBusStream<'a, S, E>
where
    S: AgentSchema + Clone + Sync + 'a,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError>,
{
    type Item = Event<S>;
    type Error = E;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(E::from)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, E> EventBus for ExampleBus<S, E>
where
    S: AgentSchema + Clone + Sync,
    Event<S>: Clone + Send + 'static,
    E: From<InMemoryEventBusError> + Send + 'static,
{
    type Event = Event<S>;
    type Subscription = Subscription<S>;
    type Error = E;
    type Stream<'a>
        = ExampleBusStream<'a, S, E>
    where
        Self: 'a;
    type Publish<'a>
        = BoxFuture<'a, Self::Error>
    where
        Self: 'a;

    fn publish(&self, event: Self::Event) -> Self::Publish<'_> {
        Box::pin(async move { self.inner.publish(event).await.map_err(E::from) })
    }

    fn subscribe(&self, subscription: Self::Subscription) -> Result<Self::Stream<'_>, Self::Error> {
        Ok(ExampleBusStream {
            inner: self.inner.subscribe(subscription)?,
            error: PhantomData,
        })
    }
}

#[must_use]
#[derive(Clone)]
pub struct ExampleWorkers<Ingress, Egress, Control, Inference, Tools, Presentation> {
    ingress: Ingress,
    egress: Egress,
    control: Control,
    inference: Inference,
    tools: Tools,
    presentation: Presentation,
}

impl<Ingress, Egress, Control, Inference, Tools, Presentation>
    ExampleWorkers<Ingress, Egress, Control, Inference, Tools, Presentation>
{
    pub fn new(
        ingress: Ingress,
        egress: Egress,
        control: Control,
        inference: Inference,
        tools: Tools,
        presentation: Presentation,
    ) -> Self {
        Self {
            ingress,
            egress,
            control,
            inference,
            tools,
            presentation,
        }
    }
}

#[must_use]
#[derive(Clone)]
pub struct ExampleRuntime<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
where
    S: AgentSchema + Clone,
    S::Surface: Send + Sync + 'static,
    SessionContext<S>: Send + Sync + 'static,
{
    error: PhantomData<fn() -> E>,
    bus: B,
    session: SessionContext<S>,
    workers: ExampleWorkers<Ingress, Egress, Control, Inference, Tools, Presentation>,
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
    ExampleRuntime<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
where
    S: AgentSchema + Clone,
    S::Surface: Send + Sync + 'static,
    SessionContext<S>: Send + Sync + 'static,
{
    pub fn new(
        bus: B,
        session: SessionContext<S>,
        workers: ExampleWorkers<Ingress, Egress, Control, Inference, Tools, Presentation>,
    ) -> Self {
        Self {
            error: PhantomData,
            bus,
            session,
            workers,
        }
    }

    pub fn bus(&self) -> &B {
        &self.bus
    }

    pub fn session(&self) -> &SessionContext<S> {
        &self.session
    }

    pub fn ingress(&self) -> &Ingress {
        &self.workers.ingress
    }

    pub fn egress(&self) -> &Egress {
        &self.workers.egress
    }
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation> AgentRuntime
    for ExampleRuntime<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    SessionContext<S>: Send + Sync + 'static,
    E: ExampleAppError + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    Ingress: SessionWorker<B, SessionContext<S>, Error = E>
        + AgentWorker<S>
        + Clone
        + Send
        + Sync
        + 'static,
    Egress: SessionWorker<B, SessionContext<S>, Error = E>
        + AgentWorker<S>
        + Clone
        + Send
        + Sync
        + 'static,
    Control: BusWorker<B, Error = E> + AgentWorker<S> + Clone + Send + Sync + 'static,
    Inference: BusWorker<B, Error = E> + AgentWorker<S> + Clone + Send + Sync + 'static,
    Tools: BusWorker<B, Error = E> + AgentWorker<S> + Clone + Send + Sync + 'static,
    Presentation: BusWorker<B, Error = E> + AgentWorker<S> + Clone + Send + Sync + 'static,
    Ingress::Run: Send,
    Egress::Run: Send,
    Control::Run: Send,
    Inference::Run: Send,
    Tools::Run: Send,
    Presentation::Run: Send,
{
    type Error = E;
    type Schema = S;
    type Bus = B;
    type Ingress = Ingress;
    type Egress = Egress;
    type Control = Control;
    type Inference = Inference;
    type Tools = Tools;
    type Presentation = Presentation;
    type RunSession = BoxFuture<'static, Self::Error>;

    fn bus(&self) -> &Self::Bus {
        &self.bus
    }

    fn ingress(&self) -> &Self::Ingress {
        &self.workers.ingress
    }

    fn egress(&self) -> &Self::Egress {
        &self.workers.egress
    }

    fn control(&self) -> &Self::Control {
        &self.workers.control
    }

    fn inference(&self) -> &Self::Inference {
        &self.workers.inference
    }

    fn tools(&self) -> &Self::Tools {
        &self.workers.tools
    }

    fn presentation(&self) -> &Self::Presentation {
        &self.workers.presentation
    }

    fn run_session(&self, session: SessionContext<S>) -> Self::RunSession {
        let bus = self.bus.clone();
        let ingress_worker = self.workers.ingress.clone();
        let egress_worker = self.workers.egress.clone();
        let control_worker = self.workers.control.clone();
        let inference_worker = self.workers.inference.clone();
        let tools_worker = self.workers.tools.clone();
        let presentation_worker = self.workers.presentation.clone();

        Box::pin(async move {
            let egress_session = session.clone();
            let egress_bus = bus.clone();
            let control_bus = bus.clone();
            let inference_bus = bus.clone();
            let tools_bus = bus.clone();
            let presentation_bus = bus.clone();

            let egress =
                tokio::spawn(async move { egress_worker.run(egress_bus, egress_session).await });
            let control = tokio::spawn(async move { control_worker.run(control_bus).await });
            let inference = tokio::spawn(async move { inference_worker.run(inference_bus).await });
            let tools = tokio::spawn(async move { tools_worker.run(tools_bus).await });
            let presentation =
                tokio::spawn(async move { presentation_worker.run(presentation_bus).await });

            // Yield once so subscribers are ready before ingress publishes the first turn.
            tokio::task::yield_now().await;
            ingress_worker.run(bus, session).await?;

            join_worker(egress).await?;
            join_worker(control).await?;
            join_worker(inference).await?;
            join_worker(tools).await?;
            join_worker(presentation).await?;
            Ok(())
        })
    }
}

pub fn new_session<S>(surface: S::Surface) -> SessionContext<S>
where
    S: AgentSchema,
    S::Ids: AgentIds<SessionId = S::SessionId, ThreadId = S::ThreadId>,
{
    let session: SessionContext<S> = mango_core::agent::SessionContext {
        session_id: S::next_session_id(),
        thread_id: S::next_thread_id(),
        surface,
    };
    session
}

pub fn session_stream<S>(session: &SessionContext<S>) -> StreamKey<S>
where
    S: AgentSchema,
{
    StreamKey::Session(session.session_id.clone())
}

pub fn session_subscription<S>(session: &SessionContext<S>) -> Subscription<S>
where
    S: AgentSchema,
{
    Subscription {
        streams: Filter::Only(vec![session_stream::<S>(session)]),
        visibility: Filter::Any,
    }
}

pub fn all_subscription<S>() -> Subscription<S>
where
    S: AgentSchema,
{
    Subscription {
        streams: Filter::Any,
        visibility: Filter::Any,
    }
}

/// Await a worker task and normalize join errors.
pub async fn join_worker<E>(handle: tokio::task::JoinHandle<Result<(), E>>) -> Result<(), E>
where
    E: ExampleAppError,
{
    handle
        .await
        .map_err(|error| E::task_join(error.to_string()))?
}

pub fn error_descriptor(code: impl Into<String>, message: impl Into<String>) -> ErrorDescriptor {
    ErrorDescriptor {
        code: code.into(),
        message: message.into(),
        retryable: false,
    }
}

/// Publish a worker error on the session stream.
pub async fn publish_worker_error<S, B>(
    bus: &B,
    worker_id: &S::WorkerId,
    session: &SessionContext<S>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Result<(), B::Error>
where
    S: AgentSchema,
    S::Ids: AgentIds<EventId = S::EventId>,
    B: EventBus<Event = Event<S>>,
{
    let message = message.into();
    publish::<S, B>(
        bus,
        session_stream::<S>(session),
        EventVisibility::Both,
        EventPayload::Error(ErrorEvent::<S> {
            worker_id: worker_id.clone(),
            stream: session_stream::<S>(session),
            error: error_descriptor(code, message),
        }),
    )
    .await
}
