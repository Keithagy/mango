//! Runtime worker contracts.

use std::future::Future;

use crate::agent::{
    interaction::SessionContext,
    schema::AgentSchema,
    substrate::{Event, EventBus, Subscription},
};

/// Schema-bound worker alias.
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

pub trait BusWorker<B>: Worker<Subscription = B::Subscription>
where
    B: EventBus,
{
    type Error;
    type Run: Future<Output = Result<(), Self::Error>>;

    fn run(self, bus: B) -> Self::Run;
}

pub trait SessionWorker<B, Session>: Worker<Subscription = B::Subscription>
where
    B: EventBus,
{
    type Error;
    type Run: Future<Output = Result<(), Self::Error>>;

    fn run(self, bus: B, session: Session) -> Self::Run;
}

/// Schema-bound runtime assembly.
pub trait AgentRuntime {
    type Error;
    type Schema: AgentSchema;
    type Bus: EventBus<
            Event = Event<Self::Schema>,
            Subscription = Subscription<Self::Schema>,
            Error = Self::Error,
        >;
    type Ingress: SessionWorker<Self::Bus, SessionContext<Self::Schema>, Error = Self::Error>
        + AgentWorker<Self::Schema>;
    type Egress: SessionWorker<Self::Bus, SessionContext<Self::Schema>, Error = Self::Error>
        + AgentWorker<Self::Schema>;
    type Control: BusWorker<Self::Bus, Error = Self::Error> + AgentWorker<Self::Schema>;
    type Inference: BusWorker<Self::Bus, Error = Self::Error> + AgentWorker<Self::Schema>;
    type Tools: BusWorker<Self::Bus, Error = Self::Error> + AgentWorker<Self::Schema>;
    type Presentation: BusWorker<Self::Bus, Error = Self::Error> + AgentWorker<Self::Schema>;
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
