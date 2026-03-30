use std::{fs, path::Path};

use mango_automation_protocol::{
    AUTOMATION_ABI_VERSION, AdvanceEnvelope, AdvanceRequest, AdvanceResponse, RegistrationEnvelope,
    RegistrationResponse,
};
use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};

use crate::AutomationsError;

pub trait AutomationRuntime: Clone + Send + Sync + 'static {
    /// Load a guest artifact and read its registration metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact cannot be read, instantiated, or does
    /// not produce a valid registration response.
    fn register(&self, artifact_path: &Path) -> Result<RegistrationResponse, AutomationsError>;

    /// Drive a guest artifact for one event transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact cannot be instantiated or the guest
    /// cannot decode the request or encode a valid response.
    fn advance(
        &self,
        artifact_path: &Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError>;
}

#[derive(Debug, Clone)]
pub struct WasmAutomationRuntime {
    engine: Engine,
}

impl Default for WasmAutomationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmAutomationRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: Engine::default(),
        }
    }

    /// Load a Wasm guest and read its registration response.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest cannot be loaded or the registration
    /// envelope is invalid.
    pub fn register(&self, artifact_path: &Path) -> Result<RegistrationResponse, AutomationsError> {
        let mut guest = LoadedGuest::from_file(&self.engine, artifact_path)?;
        let payload = guest.register()?;
        match serde_json::from_slice::<RegistrationEnvelope>(&payload)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?
        {
            RegistrationEnvelope::Ok(response) => Ok(response),
            RegistrationEnvelope::Err(message) => Err(AutomationsError::Guest(message)),
        }
    }

    /// Load a Wasm guest and advance it for a single request.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest cannot be loaded or the advance envelope
    /// is invalid.
    pub fn advance(
        &self,
        artifact_path: &Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError> {
        let mut guest = LoadedGuest::from_file(&self.engine, artifact_path)?;
        let payload = guest.advance(request)?;
        match serde_json::from_slice::<AdvanceEnvelope>(&payload)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?
        {
            AdvanceEnvelope::Ok(response) => Ok(response),
            AdvanceEnvelope::Err(message) => Err(AutomationsError::Guest(message)),
        }
    }
}

impl AutomationRuntime for WasmAutomationRuntime {
    fn register(&self, artifact_path: &Path) -> Result<RegistrationResponse, AutomationsError> {
        Self::register(self, artifact_path)
    }

    fn advance(
        &self,
        artifact_path: &Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError> {
        Self::advance(self, artifact_path, request)
    }
}

struct LoadedGuest {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    free: TypedFunc<(u32, u32), ()>,
    register: TypedFunc<(), u64>,
    advance: TypedFunc<(u32, u32), u64>,
}

impl LoadedGuest {
    fn from_file(engine: &Engine, artifact_path: &Path) -> Result<Self, AutomationsError> {
        let module_bytes =
            fs::read(artifact_path).map_err(|error| AutomationsError::Io(error.to_string()))?;
        let module = Module::new(engine, module_bytes)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let mut store = Store::new(engine, ());
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;

        let version = instance
            .get_typed_func::<(), u32>(&mut store, "mango_automation_abi_version")
            .map_err(|error| AutomationsError::Guest(error.to_string()))?
            .call(&mut store, ())
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        if version != AUTOMATION_ABI_VERSION {
            return Err(AutomationsError::Guest(format!(
                "automation ABI version mismatch: guest={version}, host={AUTOMATION_ABI_VERSION}"
            )));
        }

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| AutomationsError::Guest("guest does not export memory".to_string()))?;
        let alloc = instance
            .get_typed_func::<u32, u32>(&mut store, "mango_automation_alloc")
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let free = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "mango_automation_free")
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let register = instance
            .get_typed_func::<(), u64>(&mut store, "mango_automation_register")
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let advance = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "mango_automation_advance")
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;

        Ok(Self {
            store,
            memory,
            alloc,
            free,
            register,
            advance,
        })
    }

    fn register(&mut self) -> Result<Vec<u8>, AutomationsError> {
        let packed = self
            .register
            .call(&mut self.store, ())
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        self.read_guest_buffer(packed)
    }

    fn advance(&mut self, request: &AdvanceRequest) -> Result<Vec<u8>, AutomationsError> {
        let request_bytes = serde_json::to_vec(request)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let request_len = u32::try_from(request_bytes.len()).map_err(|_| {
            AutomationsError::Guest("guest request exceeded Wasm32 ABI".to_string())
        })?;
        let ptr = self
            .alloc
            .call(&mut self.store, request_len)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        self.memory
            .write(&mut self.store, ptr as usize, &request_bytes)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let packed = self
            .advance
            .call(&mut self.store, (ptr, request_len))
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        self.free
            .call(&mut self.store, (ptr, request_len))
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        self.read_guest_buffer(packed)
    }

    fn read_guest_buffer(&mut self, packed: u64) -> Result<Vec<u8>, AutomationsError> {
        let ptr = u32::try_from(packed & u64::from(u32::MAX)).map_err(|_| {
            AutomationsError::Guest("guest buffer pointer overflowed Wasm32 ABI".to_string())
        })?;
        let len = u32::try_from(packed >> 32).map_err(|_| {
            AutomationsError::Guest("guest buffer length overflowed Wasm32 ABI".to_string())
        })?;
        let mut bytes = vec![0_u8; len as usize];
        self.memory
            .read(&self.store, ptr as usize, &mut bytes)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        self.free
            .call(&mut self.store, (ptr, len))
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        Ok(bytes)
    }
}
