//! Execution lifecycle types.

use crate::agent::{schema::AgentSchema, substrate::ErrorDescriptor};

/// Runtime-level cancellation causes.
#[derive(Debug, Clone)]
pub enum Cancellation<S: AgentSchema> {
    UserInterrupted,
    SupersededByNewInput,
    SessionClosed,
    RuntimeShutdown,
    Detail(S::CancellationDetail),
}

/// Runtime-level completion outcomes.
#[derive(Debug, Clone)]
pub enum Completion<S: AgentSchema> {
    Completed,
    MaxOutputReached,
    ToolBoundaryReached,
    Detail(S::CompletionDetail),
}

#[derive(Debug, Clone)]
pub enum ExecutionEvent<S: AgentSchema> {
    Control(ControlEvent<S>),
    Inference(InferenceEvent<S>),
    Tool(ToolEvent<S>),
}

/// Execution control events.
#[derive(Debug, Clone)]
pub enum ControlEvent<S: AgentSchema> {
    Requested {
        request_id: S::InferenceRequestId,
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        turn_id: Option<S::TurnId>,
        directive: S::Directive,
        supersedes: Option<S::InferenceRunId>,
    },
    CancelRequested {
        session_id: S::SessionId,
        run_id: Option<S::InferenceRunId>,
        cause: Cancellation<S>,
    },
}

#[derive(Debug, Clone)]
pub enum InferenceEvent<S: AgentSchema> {
    Started {
        run_id: S::InferenceRunId,
        request_id: S::InferenceRequestId,
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        directive: S::Directive,
        engine: S::EngineId,
    },
    Output {
        run_id: S::InferenceRunId,
        sequence: u64,
        output: S::Output,
    },
    Completed {
        run_id: S::InferenceRunId,
        result: Completion<S>,
    },
    Cancelled {
        run_id: S::InferenceRunId,
        cause: Cancellation<S>,
    },
    Failed {
        run_id: S::InferenceRunId,
        error: ErrorDescriptor,
    },
}

#[derive(Debug, Clone)]
pub enum ToolEvent<S: AgentSchema> {
    Requested {
        call_id: S::ToolCallId,
        run_id: S::InferenceRunId,
        tool: S::ToolName,
        input: S::ToolData,
    },
    Started {
        call_id: S::ToolCallId,
    },
    Progress {
        call_id: S::ToolCallId,
        update: S::Status,
    },
    Succeeded {
        call_id: S::ToolCallId,
        output: S::ToolData,
    },
    Failed {
        call_id: S::ToolCallId,
        error: ErrorDescriptor,
    },
    Cancelled {
        call_id: S::ToolCallId,
        cause: Cancellation<S>,
    },
}
