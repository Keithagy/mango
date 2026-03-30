use std::{fs, path::Path};

use mango_automation_sdk::{
    AUTOMATION_ABI_VERSION, AbiResponse, AutomationRegistration, StepRequest, StepResponse,
    unpack_handle,
};
use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};

use crate::ControlPlaneError;

#[derive(Debug, Clone)]
pub struct WasmGuestRuntime {
    engine: Engine,
}

impl Default for WasmGuestRuntime {
    fn default() -> Self {
        Self {
            engine: Engine::default(),
        }
    }
}

impl WasmGuestRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_registration_from_file(
        &self,
        artifact_path: &Path,
    ) -> Result<AutomationRegistration, ControlPlaneError> {
        let bytes = fs::read(artifact_path)
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))?;
        self.read_registration(&bytes)
    }

    pub fn step_from_file(
        &self,
        artifact_path: &Path,
        request: &StepRequest,
    ) -> Result<StepResponse, ControlPlaneError> {
        let bytes = fs::read(artifact_path)
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))?;
        self.step(&bytes, request)
    }

    pub fn read_registration(
        &self,
        artifact_bytes: &[u8],
    ) -> Result<AutomationRegistration, ControlPlaneError> {
        let mut invocation = GuestInvocation::new(&self.engine, artifact_bytes)?;
        let abi_version = invocation
            .abi_version
            .call(&mut invocation.store, ())
            .map_err(runtime_err)?;
        if abi_version != AUTOMATION_ABI_VERSION {
            return Err(ControlPlaneError::AbiVersionMismatch {
                expected: AUTOMATION_ABI_VERSION,
                actual: abi_version,
            });
        }

        let handle = invocation
            .register
            .call(&mut invocation.store, ())
            .map_err(runtime_err)?;
        let response: AbiResponse<AutomationRegistration> = invocation.read_response(handle)?;
        match response {
            AbiResponse::Ok(registration) => Ok(registration),
            AbiResponse::Err { message } => Err(ControlPlaneError::Runtime(message)),
        }
    }

    pub fn step(
        &self,
        artifact_bytes: &[u8],
        request: &StepRequest,
    ) -> Result<StepResponse, ControlPlaneError> {
        let mut invocation = GuestInvocation::new(&self.engine, artifact_bytes)?;
        let abi_version = invocation
            .abi_version
            .call(&mut invocation.store, ())
            .map_err(runtime_err)?;
        if abi_version != AUTOMATION_ABI_VERSION {
            return Err(ControlPlaneError::AbiVersionMismatch {
                expected: AUTOMATION_ABI_VERSION,
                actual: abi_version,
            });
        }

        let payload = serde_json::to_vec(request)
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))?;
        let request_ptr = invocation
            .alloc
            .call(&mut invocation.store, payload.len() as u32)
            .map_err(runtime_err)?;
        invocation
            .memory
            .write(&mut invocation.store, request_ptr as usize, &payload)
            .map_err(runtime_err)?;
        let handle = invocation
            .step
            .call(&mut invocation.store, (request_ptr, payload.len() as u32))
            .map_err(runtime_err)?;
        invocation
            .dealloc
            .call(&mut invocation.store, (request_ptr, payload.len() as u32))
            .map_err(runtime_err)?;

        let response: AbiResponse<StepResponse> = invocation.read_response(handle)?;
        match response {
            AbiResponse::Ok(step) => Ok(step),
            AbiResponse::Err { message } => Err(ControlPlaneError::Runtime(message)),
        }
    }
}

struct GuestInvocation {
    store: Store<()>,
    memory: Memory,
    abi_version: TypedFunc<(), u32>,
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    register: TypedFunc<(), u64>,
    step: TypedFunc<(u32, u32), u64>,
}

impl GuestInvocation {
    fn new(engine: &Engine, artifact_bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        let module = Module::new(engine, artifact_bytes)
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))?;
        let mut store = Store::new(engine, ());
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ControlPlaneError::Runtime("guest did not export memory".to_string()))?;
        let abi_version = instance
            .get_typed_func::<(), u32>(&mut store, "mango_automation_abi_version")
            .map_err(runtime_err)?;
        let alloc = instance
            .get_typed_func::<u32, u32>(&mut store, "mango_automation_alloc")
            .map_err(runtime_err)?;
        let dealloc = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "mango_automation_dealloc")
            .map_err(runtime_err)?;
        let register = instance
            .get_typed_func::<(), u64>(&mut store, "mango_automation_register")
            .map_err(runtime_err)?;
        let step = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "mango_automation_step")
            .map_err(runtime_err)?;

        Ok(Self {
            store,
            memory,
            abi_version,
            alloc,
            dealloc,
            register,
            step,
        })
    }

    fn read_response<T: serde::de::DeserializeOwned>(
        &mut self,
        handle: u64,
    ) -> Result<T, ControlPlaneError> {
        let (ptr, len) = unpack_handle(handle);
        let mut bytes = vec![0_u8; len as usize];
        self.memory
            .read(&mut self.store, ptr as usize, &mut bytes)
            .map_err(runtime_err)?;
        self.dealloc
            .call(&mut self.store, (ptr, len))
            .map_err(runtime_err)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| ControlPlaneError::Runtime(error.to_string()))
    }
}

fn runtime_err(error: impl ToString) -> ControlPlaneError {
    ControlPlaneError::Runtime(error.to_string())
}
