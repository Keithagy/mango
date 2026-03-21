use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
};

use mango_core::agent::AgentIds;
use uuid::Uuid;

/// UUID-backed identifier.
#[must_use]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UuidId<Tag>(Uuid, PhantomData<fn() -> Tag>);

impl<Tag> UuidId<Tag> {
    pub fn new(value: Uuid) -> Self {
        Self(value, PhantomData)
    }

    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    #[must_use]
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl<Tag> From<Uuid> for UuidId<Tag> {
    fn from(value: Uuid) -> Self {
        Self::new(value)
    }
}

impl<Tag> Debug for UuidId<Tag> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("UuidId").field(&self.0).finish()
    }
}

impl<Tag> Display for UuidId<Tag> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Borrowed-or-owned string identifier.
#[must_use]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LabelId<Tag>(Cow<'static, str>, PhantomData<fn() -> Tag>);

impl<Tag> LabelId<Tag> {
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self(value.into(), PhantomData)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    #[must_use]
    pub fn into_owned(self) -> String {
        self.0.into_owned()
    }
}

impl<Tag> From<&'static str> for LabelId<Tag> {
    fn from(value: &'static str) -> Self {
        Self::new(Cow::Borrowed(value))
    }
}

impl<Tag> From<String> for LabelId<Tag> {
    fn from(value: String) -> Self {
        Self::new(Cow::Owned(value))
    }
}

impl<Tag> Debug for LabelId<Tag> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LabelId").field(&self.0).finish()
    }
}

impl<Tag> Display for LabelId<Tag> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<Tag> AsRef<str> for LabelId<Tag> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

macro_rules! define_uuid_id_tag {
    ($tag:ident, $alias:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $tag {}

        pub type $alias = UuidId<$tag>;
    };
}

macro_rules! define_label_id_tag {
    ($tag:ident, $alias:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $tag {}

        pub type $alias = LabelId<$tag>;
    };
}

define_uuid_id_tag!(EventTag, EventId);
define_uuid_id_tag!(SessionTag, SessionId);
define_uuid_id_tag!(ThreadTag, ThreadId);
define_uuid_id_tag!(TurnTag, TurnId);
define_uuid_id_tag!(InputStreamTag, InputStreamId);
define_uuid_id_tag!(RevisionTag, RevisionId);
define_uuid_id_tag!(InferenceRequestTag, InferenceRequestId);
define_uuid_id_tag!(InferenceRunTag, InferenceRunId);
define_uuid_id_tag!(StatusTag, StatusId);

define_label_id_tag!(ToolCallTag, ToolCallId);
define_label_id_tag!(WorkerTag, WorkerId);
define_label_id_tag!(EngineTag, EngineId);
define_label_id_tag!(ToolNameTag, ToolName);

fn fresh_uuid_id<Tag>() -> UuidId<Tag> {
    UuidId::new(Uuid::new_v4())
}

fn fresh_label_id<Tag>() -> LabelId<Tag> {
    LabelId::from(Uuid::new_v4().to_string())
}

/// Default identifier bundle for examples and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultAgentIds;

impl AgentIds for DefaultAgentIds {
    type SessionId = SessionId;
    type ThreadId = ThreadId;
    type TurnId = TurnId;
    type InputStreamId = InputStreamId;
    type RevisionId = RevisionId;
    type InferenceRequestId = InferenceRequestId;
    type InferenceRunId = InferenceRunId;
    type StatusId = StatusId;
    type ToolCallId = ToolCallId;
    type EventId = EventId;
    type WorkerId = WorkerId;

    fn next_session_id() -> Self::SessionId {
        fresh_uuid_id()
    }

    fn next_thread_id() -> Self::ThreadId {
        fresh_uuid_id()
    }

    fn next_turn_id() -> Self::TurnId {
        fresh_uuid_id()
    }

    fn next_input_stream_id() -> Self::InputStreamId {
        fresh_uuid_id()
    }

    fn next_revision_id() -> Self::RevisionId {
        fresh_uuid_id()
    }

    fn next_inference_request_id() -> Self::InferenceRequestId {
        fresh_uuid_id()
    }

    fn next_inference_run_id() -> Self::InferenceRunId {
        fresh_uuid_id()
    }

    fn next_status_id() -> Self::StatusId {
        fresh_uuid_id()
    }

    fn next_tool_call_id() -> Self::ToolCallId {
        fresh_label_id()
    }

    fn next_event_id() -> Self::EventId {
        fresh_uuid_id()
    }
}
