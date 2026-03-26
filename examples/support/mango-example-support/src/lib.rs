//! Support types for the Mango examples.

use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use mango_core::agent::{
    AgentIds, AgentRuntime, AgentSchema, BusWorker, ErrorDescriptor, ErrorEvent, Event, EventBus,
    EventPayload, EventStream, EventVisibility, Filter, LifecycleEventHooks, RuntimeBridge,
    RuntimeSubstrate, RuntimeSurface, SessionContext, SessionWorker, StreamKey, Subscription,
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
pub struct ExampleSubstrate<Bus, Control> {
    bus: Bus,
    control: Control,
}

impl<Bus, Control> ExampleSubstrate<Bus, Control> {
    pub fn new(bus: Bus, control: Control) -> Self {
        Self { bus, control }
    }

    pub fn bus(&self) -> &Bus {
        &self.bus
    }

    pub fn control(&self) -> &Control {
        &self.control
    }
}

impl<S, E, Bus, Control> RuntimeSubstrate<S> for ExampleSubstrate<Bus, Control>
where
    S: AgentSchema,
    Bus: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
    Control: BusWorker<S, Bus, Error = E>,
{
    type Error = E;
    type Bus = Bus;
    type Control = Control;

    fn bus(&self) -> &Self::Bus {
        &self.bus
    }

    fn control(&self) -> &Self::Control {
        &self.control
    }
}

#[must_use]
#[derive(Clone)]
pub struct ExampleSurface<Ingress, Egress, Presentation> {
    ingress: Ingress,
    egress: Egress,
    presentation: Presentation,
}

impl<Ingress, Egress, Presentation> ExampleSurface<Ingress, Egress, Presentation> {
    pub fn new(ingress: Ingress, egress: Egress, presentation: Presentation) -> Self {
        Self {
            ingress,
            egress,
            presentation,
        }
    }

    pub fn ingress(&self) -> &Ingress {
        &self.ingress
    }

    pub fn egress(&self) -> &Egress {
        &self.egress
    }

    pub fn presentation(&self) -> &Presentation {
        &self.presentation
    }
}

impl<S, E, B, Ingress, Egress, Presentation> RuntimeSurface<S, B>
    for ExampleSurface<Ingress, Egress, Presentation>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
    Ingress: SessionWorker<S, B, Error = E>,
    Egress: SessionWorker<S, B, Error = E>,
    Presentation: BusWorker<S, B, Error = E>,
{
    type Error = E;
    type Ingress = Ingress;
    type Egress = Egress;
    type Presentation = Presentation;

    fn ingress(&self) -> &Self::Ingress {
        &self.ingress
    }

    fn egress(&self) -> &Self::Egress {
        &self.egress
    }

    fn presentation(&self) -> &Self::Presentation {
        &self.presentation
    }
}

#[must_use]
#[derive(Clone)]
pub struct ExampleBridge<Inference, Tools> {
    inference: Inference,
    tools: Tools,
}

impl<Inference, Tools> ExampleBridge<Inference, Tools> {
    pub fn new(inference: Inference, tools: Tools) -> Self {
        Self { inference, tools }
    }

    pub fn inference(&self) -> &Inference {
        &self.inference
    }

    pub fn tools(&self) -> &Tools {
        &self.tools
    }
}

impl<S, E, B, Inference, Tools> RuntimeBridge<S, B> for ExampleBridge<Inference, Tools>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
    Inference: BusWorker<S, B, Error = E>,
    Tools: BusWorker<S, B, Error = E>,
{
    type Error = E;
    type Inference = Inference;
    type Tools = Tools;

    fn inference(&self) -> &Self::Inference {
        &self.inference
    }

    fn tools(&self) -> &Self::Tools {
        &self.tools
    }
}

struct StartedWorkers<E> {
    egress: tokio::task::JoinHandle<Result<(), E>>,
    control: tokio::task::JoinHandle<Result<(), E>>,
    inference: tokio::task::JoinHandle<Result<(), E>>,
    tools: tokio::task::JoinHandle<Result<(), E>>,
    presentation: tokio::task::JoinHandle<Result<(), E>>,
}

impl<E> StartedWorkers<E> {
    fn abort(self) {
        self.egress.abort();
        self.control.abort();
        self.inference.abort();
        self.tools.abort();
        self.presentation.abort();
    }
}

struct SessionStartupState<SessionId, E> {
    session_id: SessionId,
    workers: StartedWorkers<E>,
}

pub struct ExampleLifecycleHooks<SessionId, E> {
    sessions: Mutex<Vec<SessionStartupState<SessionId, E>>>,
}

impl<SessionId, E> Default for ExampleLifecycleHooks<SessionId, E> {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(Vec::new()),
        }
    }
}

impl<SessionId, E> ExampleLifecycleHooks<SessionId, E> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn store_started_workers(
        &self,
        session_id: SessionId,
        workers: StartedWorkers<E>,
    ) -> Result<(), E>
    where
        SessionId: PartialEq,
        E: ExampleAppError,
    {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| E::task_join("example lifecycle state is poisoned".to_string()))?;

        if sessions.iter().any(|state| state.session_id == session_id) {
            workers.abort();
            return Err(E::task_join(
                "session startup already completed for this session".to_string(),
            ));
        }

        sessions.push(SessionStartupState {
            session_id,
            workers,
        });
        Ok(())
    }

    fn take_started_workers(&self, session_id: &SessionId) -> Result<StartedWorkers<E>, E>
    where
        SessionId: PartialEq,
        E: ExampleAppError,
    {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| E::task_join("example lifecycle state is poisoned".to_string()))?;

        let Some(index) = sessions
            .iter()
            .position(|state| &state.session_id == session_id)
        else {
            return Err(E::task_join(
                "session was run before startup completed".to_string(),
            ));
        };

        Ok(sessions.swap_remove(index).workers)
    }
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
    LifecycleEventHooks<
        S,
        ExampleSubstrate<B, Control>,
        ExampleSurface<Ingress, Egress, Presentation>,
        ExampleBridge<Inference, Tools>,
    > for ExampleLifecycleHooks<S::SessionId, E>
where
    S: AgentSchema + Clone + Send + Sync + 'static,
    S::Surface: Send + Sync + 'static,
    S::SessionId: Clone + PartialEq + Send + Sync + 'static,
    SessionContext<S>: Send + Sync + 'static,
    E: ExampleAppError + Send + 'static,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    Ingress: SessionWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Egress: SessionWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Control: BusWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Inference: BusWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Tools: BusWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Presentation: BusWorker<S, B, Error = E> + Clone + Send + Sync + 'static,
    Ingress::Run: Send,
    Egress::Run: Send + 'static,
    Control::Run: Send + 'static,
    Inference::Run: Send + 'static,
    Tools::Run: Send + 'static,
    Presentation::Run: Send + 'static,
{
    type Error = E;
    type Startup<'a>
        = BoxFuture<'a, Self::Error>
    where
        Self: 'a,
        ExampleSubstrate<B, Control>: 'a,
        ExampleSurface<Ingress, Egress, Presentation>: 'a,
        ExampleBridge<Inference, Tools>: 'a;
    type RunSession<'a>
        = BoxFuture<'a, Self::Error>
    where
        Self: 'a,
        ExampleSubstrate<B, Control>: 'a,
        ExampleSurface<Ingress, Egress, Presentation>: 'a,
        ExampleBridge<Inference, Tools>: 'a;

    fn startup<'a>(
        &'a self,
        substrate: &'a ExampleSubstrate<B, Control>,
        surface: &'a ExampleSurface<Ingress, Egress, Presentation>,
        bridge: &'a ExampleBridge<Inference, Tools>,
        session: SessionContext<S>,
    ) -> Self::Startup<'a> {
        let bus = substrate.bus.clone();
        let egress_bus = bus.clone();
        let control_bus = bus.clone();
        let inference_bus = bus.clone();
        let tools_bus = bus.clone();
        let egress_worker = surface.egress.clone();
        let control_worker = substrate.control.clone();
        let inference_worker = bridge.inference.clone();
        let tools_worker = bridge.tools.clone();
        let presentation_worker = surface.presentation.clone();

        Box::pin(async move {
            let egress_session = session.clone();
            let workers = StartedWorkers {
                egress: tokio::spawn(
                    async move { egress_worker.run(egress_bus, egress_session).await },
                ),
                control: tokio::spawn(async move { control_worker.run(control_bus).await }),
                inference: tokio::spawn(async move { inference_worker.run(inference_bus).await }),
                tools: tokio::spawn(async move { tools_worker.run(tools_bus).await }),
                presentation: tokio::spawn(async move { presentation_worker.run(bus).await }),
            };

            self.store_started_workers(session.session_id.clone(), workers)?;

            // Yield once so subscribers are ready before ingress publishes the first turn.
            tokio::task::yield_now().await;
            Ok(())
        })
    }

    fn run_session<'a>(
        &'a self,
        substrate: &'a ExampleSubstrate<B, Control>,
        surface: &'a ExampleSurface<Ingress, Egress, Presentation>,
        _bridge: &'a ExampleBridge<Inference, Tools>,
        session: SessionContext<S>,
    ) -> Self::RunSession<'a> {
        let bus = substrate.bus.clone();
        let ingress_worker = surface.ingress.clone();
        let session_id = session.session_id.clone();

        Box::pin(async move {
            if let Err(error) = ingress_worker.run(bus, session).await {
                if let Ok(workers) = self.take_started_workers(&session_id) {
                    workers.abort();
                }
                return Err(error);
            }

            let workers = self.take_started_workers(&session_id)?;
            join_worker(workers.egress).await?;
            join_worker(workers.control).await?;
            join_worker(workers.inference).await?;
            join_worker(workers.tools).await?;
            join_worker(workers.presentation).await?;
            Ok(())
        })
    }
}

#[must_use]
pub struct ExampleRuntime<
    S,
    E,
    B,
    Ingress,
    Egress,
    Control,
    Inference,
    Tools,
    Presentation,
    Lifecycle = ExampleLifecycleHooks<<<S as AgentSchema>::Ids as AgentIds>::SessionId, E>,
> where
    S: AgentSchema,
{
    error: PhantomData<fn() -> E>,
    schema: PhantomData<fn() -> S>,
    substrate: ExampleSubstrate<B, Control>,
    surface: ExampleSurface<Ingress, Egress, Presentation>,
    bridge: ExampleBridge<Inference, Tools>,
    lifecycle: Lifecycle,
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation>
    ExampleRuntime<
        S,
        E,
        B,
        Ingress,
        Egress,
        Control,
        Inference,
        Tools,
        Presentation,
        ExampleLifecycleHooks<<<S as AgentSchema>::Ids as AgentIds>::SessionId, E>,
    >
where
    S: AgentSchema,
{
    pub fn new(
        substrate: ExampleSubstrate<B, Control>,
        surface: ExampleSurface<Ingress, Egress, Presentation>,
        bridge: ExampleBridge<Inference, Tools>,
    ) -> Self {
        Self::with_lifecycle(substrate, surface, bridge, ExampleLifecycleHooks::default())
    }
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation, Lifecycle>
    ExampleRuntime<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation, Lifecycle>
where
    S: AgentSchema,
{
    pub fn with_lifecycle(
        substrate: ExampleSubstrate<B, Control>,
        surface: ExampleSurface<Ingress, Egress, Presentation>,
        bridge: ExampleBridge<Inference, Tools>,
        lifecycle: Lifecycle,
    ) -> Self {
        Self {
            error: PhantomData,
            schema: PhantomData,
            substrate,
            surface,
            bridge,
            lifecycle,
        }
    }

    pub fn substrate(&self) -> &ExampleSubstrate<B, Control> {
        &self.substrate
    }

    pub fn surface(&self) -> &ExampleSurface<Ingress, Egress, Presentation> {
        &self.surface
    }

    pub fn bridge(&self) -> &ExampleBridge<Inference, Tools> {
        &self.bridge
    }

    pub fn lifecycle(&self) -> &Lifecycle {
        &self.lifecycle
    }
}

impl<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation, Lifecycle> AgentRuntime
    for ExampleRuntime<S, E, B, Ingress, Egress, Control, Inference, Tools, Presentation, Lifecycle>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = E>,
    Ingress: SessionWorker<S, B, Error = E>,
    Egress: SessionWorker<S, B, Error = E>,
    Control: BusWorker<S, B, Error = E>,
    Inference: BusWorker<S, B, Error = E>,
    Tools: BusWorker<S, B, Error = E>,
    Presentation: BusWorker<S, B, Error = E>,
    Lifecycle: LifecycleEventHooks<
            S,
            ExampleSubstrate<B, Control>,
            ExampleSurface<Ingress, Egress, Presentation>,
            ExampleBridge<Inference, Tools>,
            Error = E,
        >,
{
    type Error = E;
    type Schema = S;
    type Substrate = ExampleSubstrate<B, Control>;
    type Surface = ExampleSurface<Ingress, Egress, Presentation>;
    type Bridge = ExampleBridge<Inference, Tools>;
    type Lifecycle = Lifecycle;

    fn substrate(&self) -> &Self::Substrate {
        &self.substrate
    }

    fn surface(&self) -> &Self::Surface {
        &self.surface
    }

    fn bridge(&self) -> &Self::Bridge {
        &self.bridge
    }

    fn lifecycle(&self) -> &Self::Lifecycle {
        &self.lifecycle
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
///
/// # Errors
///
/// Returns the worker error, or a normalized join error if the task panics or
/// is cancelled before completion.
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
///
/// # Errors
///
/// Returns an error if publishing the worker error event fails.
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
