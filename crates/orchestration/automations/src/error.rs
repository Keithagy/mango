use thiserror::Error;

#[derive(Debug, Error)]
pub enum AutomationsError {
    #[error("state backend failed: {0}")]
    State(String),
    #[error("control plane I/O failed: {0}")]
    Io(String),
    #[error("wasm guest failed: {0}")]
    Guest(String),
    #[error("provider invocation failed: {0}")]
    Provider(String),
    #[error("automation `{0}` was not found")]
    AutomationNotFound(String),
    #[error("automation `{automation_id}` has no active revision")]
    NoActiveRevision { automation_id: String },
    #[error("revision {revision_id} for automation `{automation_id}` was not found")]
    RevisionNotFound {
        automation_id: String,
        revision_id: u64,
    },
    #[error(
        "revision {revision_id} for automation `{automation_id}` is incompatible with the current state"
    )]
    IncompatibleState {
        automation_id: String,
        revision_id: u64,
    },
    #[error("automation `{automation_id}` does not declare capability `{capability}`")]
    MissingCapability {
        automation_id: String,
        capability: String,
    },
}
