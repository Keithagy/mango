//! Runtime worker contracts.

use std::future::Future;

use crate::agent::{
    interaction::SessionContext,
    schema::AgentSchema,
    substrate::{Event, EventBus, Subscription},
};

/// Schema-bound runtime assembly.
pub trait AgentRuntime {
    type Error;
    type Schema: AgentSchema;
    type Bus: EventBus<
            Event = Event<Self::Schema>,
            Subscription = Subscription<Self::Schema>,
            Error = Self::Error,
        >;
    type Ingress: SessionWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type Egress: SessionWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type Control: BusWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type Inference: BusWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type Tools: BusWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type Presentation: BusWorker<Self::Schema, Self::Bus, Error = Self::Error>;
    type RunSession: Future<Output = Result<(), Self::Error>>;

    fn bus(&self) -> &Self::Bus;
    fn ingress(&self) -> &Self::Ingress;
    fn egress(&self) -> &Self::Egress;
    fn control(&self) -> &Self::Control;
    fn inference(&self) -> &Self::Inference;
    fn tools(&self) -> &Self::Tools;
    fn presentation(&self) -> &Self::Presentation;
    fn run_session(&self, session: SessionContext<Self::Schema>) -> Self::RunSession;
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
