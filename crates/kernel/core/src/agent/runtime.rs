//! Runtime assembly contracts.

use std::future::Future;

use crate::agent::{
    interaction::SessionContext,
    schema::AgentSchema,
    substrate::{Event, EventBus, Subscription},
};

pub type RuntimeStartup<'a, R> = <<R as AgentRuntime>::Lifecycle as LifecycleEventHooks<
    <R as AgentRuntime>::Schema,
    <R as AgentRuntime>::Substrate,
    <R as AgentRuntime>::Surface,
    <R as AgentRuntime>::Bridge,
>>::Startup<'a>;

pub type RuntimeSessionRun<'a, R> = <<R as AgentRuntime>::Lifecycle as LifecycleEventHooks<
    <R as AgentRuntime>::Schema,
    <R as AgentRuntime>::Substrate,
    <R as AgentRuntime>::Surface,
    <R as AgentRuntime>::Bridge,
>>::RunSession<'a>;

/// Schema-bound runtime assembly organized around Mango's major boundaries.
pub trait AgentRuntime {
    type Error;
    type Schema: AgentSchema;
    type Substrate: RuntimeSubstrate<Self::Schema, Error = Self::Error>;
    type Surface: RuntimeSurface<
            Self::Schema,
            <Self::Substrate as RuntimeSubstrate<Self::Schema>>::Bus,
            Error = Self::Error,
        >;
    type Bridge: RuntimeBridge<
            Self::Schema,
            <Self::Substrate as RuntimeSubstrate<Self::Schema>>::Bus,
            Error = Self::Error,
        >;
    type Lifecycle: LifecycleEventHooks<
            Self::Schema,
            Self::Substrate,
            Self::Surface,
            Self::Bridge,
            Error = Self::Error,
        >;

    fn substrate(&self) -> &Self::Substrate;
    fn surface(&self) -> &Self::Surface;
    fn bridge(&self) -> &Self::Bridge;
    fn lifecycle(&self) -> &Self::Lifecycle;

    fn startup(&self, session: SessionContext<Self::Schema>) -> RuntimeStartup<'_, Self> {
        self.lifecycle()
            .startup(self.substrate(), self.surface(), self.bridge(), session)
    }

    fn run_session(&self, session: SessionContext<Self::Schema>) -> RuntimeSessionRun<'_, Self> {
        self.lifecycle()
            .run_session(self.substrate(), self.surface(), self.bridge(), session)
    }
}

/// Runtime-owned substrate boundary.
pub trait RuntimeSubstrate<S>
where
    S: AgentSchema,
{
    type Error;
    type Bus: EventBus<Event = Event<S>, Subscription = Subscription<S>, Error = Self::Error>;
    type Control: BusWorker<S, Self::Bus, Error = Self::Error>;

    fn bus(&self) -> &Self::Bus;
    fn control(&self) -> &Self::Control;
}

/// Runtime-owned surface boundary.
pub trait RuntimeSurface<S, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>>,
{
    type Error;
    type Ingress: SessionWorker<S, B, Error = Self::Error>;
    type Egress: SessionWorker<S, B, Error = Self::Error>;
    type Presentation: BusWorker<S, B, Error = Self::Error>;

    fn ingress(&self) -> &Self::Ingress;
    fn egress(&self) -> &Self::Egress;
    fn presentation(&self) -> &Self::Presentation;
}

/// Runtime-owned bridge boundary.
pub trait RuntimeBridge<S, B>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>>,
{
    type Error;
    type Inference: BusWorker<S, B, Error = Self::Error>;
    type Tools: BusWorker<S, B, Error = Self::Error>;

    fn inference(&self) -> &Self::Inference;
    fn tools(&self) -> &Self::Tools;
}

/// Lifecycle hooks for session startup and execution.
pub trait LifecycleEventHooks<S, Substrate, Surface, Bridge>
where
    S: AgentSchema,
{
    type Error;
    type Startup<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a,
        Substrate: 'a,
        Surface: 'a,
        Bridge: 'a;
    type RunSession<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a,
        Substrate: 'a,
        Surface: 'a,
        Bridge: 'a;

    fn startup<'a>(
        &'a self,
        substrate: &'a Substrate,
        surface: &'a Surface,
        bridge: &'a Bridge,
        session: SessionContext<S>,
    ) -> Self::Startup<'a>;

    fn run_session<'a>(
        &'a self,
        substrate: &'a Substrate,
        surface: &'a Surface,
        bridge: &'a Bridge,
        session: SessionContext<S>,
    ) -> Self::RunSession<'a>;
}

/// Worker metadata owned by an agent schema.
pub trait AgentWorker<S: AgentSchema>:
    Worker<WorkerId = S::WorkerId, Subscription = Subscription<S>>
{
}

impl<S, T> AgentWorker<S> for T
where
    S: AgentSchema,
    T: Worker<WorkerId = S::WorkerId, Subscription = Subscription<S>>,
{
}

/// Worker metadata exposed to the runtime.
pub trait Worker {
    type WorkerId: Clone;
    type Subscription: Clone;

    fn worker_id(&self) -> Self::WorkerId;
    fn subscription(&self) -> Self::Subscription;
}

/// Worker that runs against a schema-bound event bus.
pub trait BusWorker<S, B>: AgentWorker<S>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>>,
{
    type Error;
    type Run: Future<Output = Result<(), Self::Error>>;

    fn run(self, bus: B) -> Self::Run;
}

/// Worker that runs against a schema-bound event bus with session input.
pub trait SessionWorker<S, B>: AgentWorker<S>
where
    S: AgentSchema,
    B: EventBus<Event = Event<S>, Subscription = Subscription<S>>,
{
    type Error;
    type Run: Future<Output = Result<(), Self::Error>>;

    fn run(self, bus: B, session: SessionContext<S>) -> Self::Run;
}
