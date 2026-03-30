use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use mango_automation_sdk::{AutomationCommand, AutomationEvent, AutomationRegistration, TraceNote};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ControlPlaneError;

pub type RevisionId = u64;
pub type ActivationId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredRevision {
    pub revision_id: RevisionId,
    pub artifact_hash: String,
    pub artifact_path: PathBuf,
    pub registration: AutomationRegistration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Wakeup {
    pub token: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationTraceRecord {
    pub at: i64,
    pub event: AutomationEvent,
    pub commands: Vec<AutomationCommand>,
    pub guest_trace: Vec<TraceNote>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationRecord {
    pub activation_id: ActivationId,
    pub revision_id: RevisionId,
    pub profile: Value,
    pub state: Value,
    pub wakeups: BTreeMap<String, Wakeup>,
    pub trace_log: Vec<ActivationTraceRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub revisions: BTreeMap<RevisionId, RegisteredRevision>,
    pub activations: BTreeMap<ActivationId, ActivationRecord>,
    pub next_revision_id: RevisionId,
    pub next_activation_id: ActivationId,
}

impl ControlPlaneState {
    pub fn allocate_revision_id(&mut self) -> RevisionId {
        self.next_revision_id += 1;
        self.next_revision_id
    }

    pub fn allocate_activation_id(&mut self) -> ActivationId {
        self.next_activation_id += 1;
        self.next_activation_id
    }
}

pub trait ControlPlaneStore: Clone + Send + Sync + 'static {
    fn snapshot(&self) -> Result<ControlPlaneState, ControlPlaneError>;

    fn transact<T, F>(&self, mutate: F) -> Result<T, ControlPlaneError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, ControlPlaneError>;
}

#[derive(Debug, Clone, Default)]
pub struct MemoryControlPlaneStore {
    state: Arc<Mutex<ControlPlaneState>>,
}

impl MemoryControlPlaneStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlPlaneStore for MemoryControlPlaneStore {
    fn snapshot(&self) -> Result<ControlPlaneState, ControlPlaneError> {
        Ok(lock_mutex(&self.state, "memory control plane")?.clone())
    }

    fn transact<T, F>(&self, mutate: F) -> Result<T, ControlPlaneError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, ControlPlaneError>,
    {
        let mut state = lock_mutex(&self.state, "memory control plane")?;
        mutate(&mut state)
    }
}

#[derive(Debug, Clone)]
pub struct JsonFileControlPlaneStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl JsonFileControlPlaneStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl ControlPlaneStore for JsonFileControlPlaneStore {
    fn snapshot(&self) -> Result<ControlPlaneState, ControlPlaneError> {
        let _guard = lock_mutex(&self.lock, "json control plane")?;
        read_state_file(&self.path)
    }

    fn transact<T, F>(&self, mutate: F) -> Result<T, ControlPlaneError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, ControlPlaneError>,
    {
        let _guard = lock_mutex(&self.lock, "json control plane")?;
        let mut state = read_state_file(&self.path)?;
        let output = mutate(&mut state)?;
        write_state_file(&self.path, &state)?;
        Ok(output)
    }
}

fn lock_mutex<'a, T>(
    mutex: &'a Mutex<T>,
    label: &str,
) -> Result<MutexGuard<'a, T>, ControlPlaneError> {
    mutex
        .lock()
        .map_err(|_| ControlPlaneError::Store(format!("{label} mutex was poisoned")))
}

fn read_state_file(path: &Path) -> Result<ControlPlaneState, ControlPlaneError> {
    if !path.exists() {
        return Ok(ControlPlaneState::default());
    }

    let contents =
        fs::read_to_string(path).map_err(|error| ControlPlaneError::Store(error.to_string()))?;
    serde_json::from_str(&contents).map_err(|error| ControlPlaneError::Store(error.to_string()))
}

fn write_state_file(path: &Path, state: &ControlPlaneState) -> Result<(), ControlPlaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| ControlPlaneError::Store(error.to_string()))?;
    }

    let json = serde_json::to_string_pretty(state)
        .map_err(|error| ControlPlaneError::Store(error.to_string()))?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, json).map_err(|error| ControlPlaneError::Store(error.to_string()))?;
    fs::rename(&temp_path, path).map_err(|error| ControlPlaneError::Store(error.to_string()))
}
