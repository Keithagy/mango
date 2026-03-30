use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{AutomationsError, ControlPlaneState};

pub trait ControlPlaneStore: Clone + Send + Sync + 'static {
    /// Read the current persisted control-plane snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing store cannot be read or decoded.
    fn snapshot(&self) -> Result<ControlPlaneState, AutomationsError>;

    /// Run a transactional mutation against the persisted control-plane state.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing store cannot be loaded or persisted, or
    /// when the caller-supplied mutation returns an error.
    fn transact<T, F>(&self, mutate: F) -> Result<T, AutomationsError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, AutomationsError>;
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
    fn snapshot(&self) -> Result<ControlPlaneState, AutomationsError> {
        Ok(lock_mutex(&self.state, "memory control plane")?.clone())
    }

    fn transact<T, F>(&self, mutate: F) -> Result<T, AutomationsError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, AutomationsError>,
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
    fn snapshot(&self) -> Result<ControlPlaneState, AutomationsError> {
        let _guard = lock_mutex(&self.lock, "json control plane")?;
        read_state_file(&self.path)
    }

    fn transact<T, F>(&self, mutate: F) -> Result<T, AutomationsError>
    where
        F: FnOnce(&mut ControlPlaneState) -> Result<T, AutomationsError>,
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
) -> Result<MutexGuard<'a, T>, AutomationsError> {
    mutex
        .lock()
        .map_err(|_| AutomationsError::State(format!("{label} mutex was poisoned")))
}

fn read_state_file(path: &Path) -> Result<ControlPlaneState, AutomationsError> {
    if !path.exists() {
        return Ok(ControlPlaneState::default());
    }

    let contents =
        fs::read_to_string(path).map_err(|error| AutomationsError::State(error.to_string()))?;
    serde_json::from_str(&contents).map_err(|error| AutomationsError::State(error.to_string()))
}

fn write_state_file(path: &Path, state: &ControlPlaneState) -> Result<(), AutomationsError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| AutomationsError::State(error.to_string()))?;
    }

    let json = serde_json::to_string_pretty(state)
        .map_err(|error| AutomationsError::State(error.to_string()))?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, json).map_err(|error| AutomationsError::State(error.to_string()))?;
    fs::rename(&temp_path, path).map_err(|error| AutomationsError::State(error.to_string()))
}
