//! Event substrate types and bus contracts.

use std::{
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use crate::agent::{
    execution::ExecutionEvent, interaction::InteractionEvent, presentation::PresentationEvent,
    schema::AgentSchema,
};

/// Routing scope.
#[derive(Debug, Clone)]
pub enum StreamKey<S: AgentSchema> {
    Global,
    Session(S::SessionId),
    Thread(S::ThreadId),
    Worker(S::WorkerId),
}

impl<S: AgentSchema> PartialEq for StreamKey<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Global, Self::Global) => true,
            (Self::Session(left), Self::Session(right)) => left == right,
            (Self::Thread(left), Self::Thread(right)) => left == right,
            (Self::Worker(left), Self::Worker(right)) => left == right,
            _ => false,
        }
    }
}

impl<S: AgentSchema> Eq for StreamKey<S> {}

impl<S: AgentSchema> Hash for StreamKey<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Global => {}
            Self::Session(session_id) => session_id.hash(state),
            Self::Thread(thread_id) => thread_id.hash(state),
            Self::Worker(worker_id) => worker_id.hash(state),
        }
    }
}

/// Audience hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventVisibility {
    Internal,
    UserVisible,
    Both,
}

/// Transport record.
#[derive(Debug, Clone)]
pub struct Event<S: AgentSchema> {
    pub id: S::EventId,
    pub stream: StreamKey<S>,
    pub visibility: EventVisibility,
    pub occurred_at: SystemTime,
    pub payload: EventPayload<S>,
}

/// Top-level event domains.
#[derive(Debug, Clone)]
pub enum EventPayload<S: AgentSchema> {
    Interaction(InteractionEvent<S>),
    Execution(ExecutionEvent<S>),
    Presentation(PresentationEvent<S>),
    Error(ErrorEvent<S>),
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter<T> {
    Any,
    Only(Vec<T>),
}

impl<T> Filter<T> {
    pub fn any() -> Self {
        Self::Any
    }

    pub fn only(values: impl Into<Vec<T>>) -> Self {
        Self::Only(values.into())
    }
}

/// Metadata subscription filter.
#[must_use]
#[derive(Debug, Clone)]
pub struct Subscription<S: AgentSchema> {
    pub streams: Filter<StreamKey<S>>,
    pub visibility: Filter<EventVisibility>,
}

impl<S: AgentSchema> PartialEq for Subscription<S> {
    fn eq(&self, other: &Self) -> bool {
        self.streams == other.streams && self.visibility == other.visibility
    }
}

impl<S: AgentSchema> Eq for Subscription<S> {}

impl<S: AgentSchema> Subscription<S> {
    pub fn all() -> Self {
        Self {
            streams: Filter::Any,
            visibility: Filter::Any,
        }
    }

    pub fn for_stream(stream: StreamKey<S>) -> Self {
        Self {
            streams: Filter::Only(vec![stream]),
            visibility: Filter::Any,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorDescriptor {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

/// Uniform worker failure payload.
#[derive(Debug, Clone)]
pub struct ErrorEvent<S: AgentSchema> {
    pub worker_id: S::WorkerId,
    pub stream: StreamKey<S>,
    pub error: ErrorDescriptor,
}

impl<S: AgentSchema> PartialEq for ErrorEvent<S> {
    fn eq(&self, other: &Self) -> bool {
        self.worker_id == other.worker_id
            && self.stream == other.stream
            && self.error == other.error
    }
}

impl<S: AgentSchema> Eq for ErrorEvent<S> {}

pub trait EventStream {
    type Item;
    type Error;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>>;
}

/// Transport-agnostic event bus.
pub trait EventBus {
    type Event;
    type Subscription;
    type Error;
    type Stream<'a>: EventStream<Item = Self::Event, Error = Self::Error> + 'a
    where
        Self: 'a;
    type Publish<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;

    fn publish(&self, event: Self::Event) -> Self::Publish<'_>;

    /// Subscribe to matching events from the bus.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription cannot be registered by the
    /// underlying bus implementation.
    fn subscribe(&self, subscription: Self::Subscription) -> Result<Self::Stream<'_>, Self::Error>;
}
