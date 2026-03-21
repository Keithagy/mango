//! Schema-owned core vocabulary and identifiers.

use std::{fmt::Debug, hash::Hash};

/// Schema-owned lifecycle vocabulary.
pub trait AgentSchema: AgentSchemaIds {
    type Ids: AgentIds;

    type Surface: Clone + Debug;

    type InputKind: Clone + Debug;

    type Input: Clone + Debug;

    type InterruptDetail: Clone + Debug;

    type Directive: Clone + Debug;

    type Output: Clone + Debug;

    type ToolData: Clone + Debug;

    type Status: Clone + Debug;

    type CancellationDetail: Clone + Debug;

    type CompletionDetail: Clone + Debug;

    type EngineId: Clone + Debug + Eq + Hash;

    type ToolName: Clone + Debug + Eq + Hash;

    #[must_use]
    fn next_session_id() -> Self::SessionId
    where
        Self::Ids: AgentIds<SessionId = Self::SessionId>,
    {
        <Self::Ids as AgentIds>::next_session_id()
    }

    #[must_use]
    fn next_thread_id() -> Self::ThreadId
    where
        Self::Ids: AgentIds<ThreadId = Self::ThreadId>,
    {
        <Self::Ids as AgentIds>::next_thread_id()
    }

    #[must_use]
    fn next_turn_id() -> Self::TurnId
    where
        Self::Ids: AgentIds<TurnId = Self::TurnId>,
    {
        <Self::Ids as AgentIds>::next_turn_id()
    }

    #[must_use]
    fn next_input_stream_id() -> Self::InputStreamId
    where
        Self::Ids: AgentIds<InputStreamId = Self::InputStreamId>,
    {
        <Self::Ids as AgentIds>::next_input_stream_id()
    }

    #[must_use]
    fn next_revision_id() -> Self::RevisionId
    where
        Self::Ids: AgentIds<RevisionId = Self::RevisionId>,
    {
        <Self::Ids as AgentIds>::next_revision_id()
    }

    #[must_use]
    fn next_inference_request_id() -> Self::InferenceRequestId
    where
        Self::Ids: AgentIds<InferenceRequestId = Self::InferenceRequestId>,
    {
        <Self::Ids as AgentIds>::next_inference_request_id()
    }

    #[must_use]
    fn next_inference_run_id() -> Self::InferenceRunId
    where
        Self::Ids: AgentIds<InferenceRunId = Self::InferenceRunId>,
    {
        <Self::Ids as AgentIds>::next_inference_run_id()
    }

    #[must_use]
    fn next_status_id() -> Self::StatusId
    where
        Self::Ids: AgentIds<StatusId = Self::StatusId>,
    {
        <Self::Ids as AgentIds>::next_status_id()
    }

    #[must_use]
    fn next_tool_call_id() -> Self::ToolCallId
    where
        Self::Ids: AgentIds<ToolCallId = Self::ToolCallId>,
    {
        <Self::Ids as AgentIds>::next_tool_call_id()
    }

    #[must_use]
    fn next_event_id() -> Self::EventId
    where
        Self::Ids: AgentIds<EventId = Self::EventId>,
    {
        <Self::Ids as AgentIds>::next_event_id()
    }
}

/// Bounds for lifecycle identifiers.
pub trait AgentId: Clone + Debug + PartialEq + Eq + Hash {}

impl<T> AgentId for T where T: Clone + Debug + PartialEq + Eq + Hash {}

/// Schema-owned identifier bundle.
pub trait AgentIds: Clone + Debug {
    type SessionId: AgentId;
    type ThreadId: AgentId;
    type TurnId: AgentId;
    type InputStreamId: AgentId;
    type RevisionId: AgentId;
    type InferenceRequestId: AgentId;
    type InferenceRunId: AgentId;
    type StatusId: AgentId;
    type ToolCallId: AgentId;
    type EventId: AgentId;
    type WorkerId: AgentId;

    #[must_use]
    fn next_session_id() -> Self::SessionId;
    #[must_use]
    fn next_thread_id() -> Self::ThreadId;
    #[must_use]
    fn next_turn_id() -> Self::TurnId;
    #[must_use]
    fn next_input_stream_id() -> Self::InputStreamId;
    #[must_use]
    fn next_revision_id() -> Self::RevisionId;
    #[must_use]
    fn next_inference_request_id() -> Self::InferenceRequestId;
    #[must_use]
    fn next_inference_run_id() -> Self::InferenceRunId;
    #[must_use]
    fn next_status_id() -> Self::StatusId;
    #[must_use]
    fn next_tool_call_id() -> Self::ToolCallId;
    #[must_use]
    fn next_event_id() -> Self::EventId;
}

/// Schema-bound identifier aliases.
pub trait AgentSchemaIds {
    type SessionId: AgentId;
    type ThreadId: AgentId;
    type TurnId: AgentId;
    type InputStreamId: AgentId;
    type RevisionId: AgentId;
    type InferenceRequestId: AgentId;
    type InferenceRunId: AgentId;
    type StatusId: AgentId;
    type ToolCallId: AgentId;
    type EventId: AgentId;
    type WorkerId: AgentId;
}

impl<S: AgentSchema> AgentSchemaIds for S {
    type SessionId = <S::Ids as AgentIds>::SessionId;
    type ThreadId = <S::Ids as AgentIds>::ThreadId;
    type TurnId = <S::Ids as AgentIds>::TurnId;
    type InputStreamId = <S::Ids as AgentIds>::InputStreamId;
    type RevisionId = <S::Ids as AgentIds>::RevisionId;
    type InferenceRequestId = <S::Ids as AgentIds>::InferenceRequestId;
    type InferenceRunId = <S::Ids as AgentIds>::InferenceRunId;
    type StatusId = <S::Ids as AgentIds>::StatusId;
    type ToolCallId = <S::Ids as AgentIds>::ToolCallId;
    type EventId = <S::Ids as AgentIds>::EventId;
    type WorkerId = <S::Ids as AgentIds>::WorkerId;
}
