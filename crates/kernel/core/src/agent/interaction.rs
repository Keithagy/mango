//! Interaction lifecycle types.

use crate::agent::schema::AgentSchema;

/// Input stability states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputStability<S: AgentSchema> {
    Tentative,
    Stable,
    Final,
    Revision { replaces: S::RevisionId },
    Retraction { target: S::RevisionId },
}

/// Runtime-level interrupt causes.
#[derive(Debug, Clone)]
pub enum InterruptCause<S: AgentSchema> {
    ExplicitUserAction,
    SurfaceDisconnected,
    Timeout,
    Detail(S::InterruptDetail),
}

#[derive(Debug, Clone)]
pub struct SessionContext<S: AgentSchema> {
    pub session_id: S::SessionId,
    pub thread_id: S::ThreadId,
    pub surface: S::Surface,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionCloseReason {
    Normal,
    Superseded,
    SurfaceDisconnected,
    Shutdown,
}

/// Interaction lifecycle events.
#[derive(Debug, Clone)]
pub enum InteractionEvent<S: AgentSchema> {
    SessionOpened {
        session: SessionContext<S>,
    },
    SessionClosed {
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        reason: SessionCloseReason,
    },
    InputStreamOpened {
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        stream_id: S::InputStreamId,
        kind: S::InputKind,
    },
    InputDelta {
        stream_id: S::InputStreamId,
        revision_id: S::RevisionId,
        sequence: u64,
        input: S::Input,
        stability: InputStability<S>,
    },
    InputCommitted {
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        stream_id: S::InputStreamId,
        revision_id: S::RevisionId,
        turn_id: S::TurnId,
        input: S::Input,
    },
    InputInterrupted {
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        cause: InterruptCause<S>,
    },
    InputStreamClosed {
        session_id: S::SessionId,
        thread_id: S::ThreadId,
        stream_id: S::InputStreamId,
    },
}
