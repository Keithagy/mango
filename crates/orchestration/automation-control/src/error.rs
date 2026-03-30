use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("store failed: {0}")]
    Store(String),
    #[error("runtime failed: {0}")]
    Runtime(String),
    #[error("artifact store failed: {0}")]
    Artifact(String),
    #[error("revision {0} was not found")]
    RevisionNotFound(u64),
    #[error("activation {0} was not found")]
    ActivationNotFound(u64),
    #[error("guest ABI version mismatch: expected {expected}, got {actual}")]
    AbiVersionMismatch { expected: u32, actual: u32 },
}
