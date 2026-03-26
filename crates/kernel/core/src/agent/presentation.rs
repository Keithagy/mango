//! Presentation state and outbound events.

use crate::agent::{interaction::InputStability, schema::AgentSchema, substrate::ErrorDescriptor};

#[derive(Debug, Clone)]
pub enum PresentationEvent<S: AgentSchema> {
    Status(StatusEvent<S>),
    Outbound(OutboundEvent<S>),
}

#[derive(Debug, Clone)]
pub enum StatusEvent<S: AgentSchema> {
    Opened {
        status_id: S::StatusId,
        session_id: S::SessionId,
        run_id: Option<S::InferenceRunId>,
        status: S::Status,
    },
    Updated {
        status_id: S::StatusId,
        sequence: u64,
        status: S::Status,
    },
    Closed {
        status_id: S::StatusId,
    },
}

/// Surface-ready projection events.
#[derive(Debug, Clone)]
pub enum OutboundEvent<S: AgentSchema> {
    InputEcho {
        session_id: S::SessionId,
        stream_id: S::InputStreamId,
        revision_id: S::RevisionId,
        input: S::Input,
        stability: InputStability<S>,
    },
    Output {
        session_id: S::SessionId,
        run_id: S::InferenceRunId,
        sequence: u64,
        output: S::Output,
    },
    ToolProgress {
        session_id: S::SessionId,
        call_id: S::ToolCallId,
        status: S::Status,
    },
    StatusOpened {
        session_id: S::SessionId,
        status_id: S::StatusId,
        status: S::Status,
    },
    StatusUpdated {
        session_id: S::SessionId,
        status_id: S::StatusId,
        status: S::Status,
    },
    StatusClosed {
        session_id: S::SessionId,
        status_id: S::StatusId,
    },
    Error {
        session_id: S::SessionId,
        error: ErrorDescriptor,
    },
}
