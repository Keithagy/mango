//! Guest-side SDK for Mango Wasm automations.
//!
//! Runtime-generated automation crates are expected to depend on this crate,
//! implement [`Automation`], and export themselves with [`export_automation!`].
//! The generated guest artifact remains isolated: it can only communicate with
//! the control plane through the protocol types re-exported here.

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;

pub use mango_automation_protocol::{
    AUTOMATION_ABI_VERSION, AUTOMATION_SDK_VERSION, AdvanceEnvelope, AdvanceRequest,
    AdvanceResponse, AutomationDescriptor, AutomationEvent, Capability, EffectKind, EffectRequest,
    EffectResult, EventDisposition, RegistrationEnvelope, RegistrationResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestContext {
    pub automation_id: String,
    pub revision_id: u64,
    pub now: i64,
    pub config: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Decision<S> {
    pub state: S,
    pub effects: Vec<EffectRequest>,
    pub status: Option<String>,
    pub disposition: EventDisposition,
}

impl<S> Decision<S> {
    #[must_use]
    pub fn new(state: S) -> Self {
        Self {
            state,
            effects: Vec::new(),
            status: None,
            disposition: EventDisposition::Handled,
        }
    }

    #[must_use]
    pub fn with_effect(mut self, effect: EffectRequest) -> Self {
        self.effects.push(effect);
        self
    }

    #[must_use]
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = Some(status.into());
        self
    }

    #[must_use]
    pub fn handled(mut self) -> Self {
        self.disposition = EventDisposition::Handled;
        self
    }

    #[must_use]
    pub fn unhandled(mut self) -> Self {
        self.disposition = EventDisposition::Unhandled;
        self
    }
}

pub trait Automation {
    type State: Serialize + DeserializeOwned;

    fn descriptor(&self) -> AutomationDescriptor;

    fn initial_state(&self) -> Self::State;

    /// Advance the automation state machine for a single host-delivered event.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest chooses to reject the event or cannot
    /// produce a valid next state for it.
    fn reduce(
        &self,
        state: Self::State,
        event: AutomationEvent,
        ctx: GuestContext,
    ) -> Result<Decision<Self::State>, String>;
}

#[derive(Debug, Error)]
pub enum GuestSdkError {
    #[error("failed to serialize registration response: {0}")]
    RegistrationEncode(serde_json::Error),
    #[error("failed to serialize advance response: {0}")]
    AdvanceEncode(serde_json::Error),
    #[error("failed to decode advance request: {0}")]
    AdvanceDecode(serde_json::Error),
    #[error("failed to encode state as JSON: {0}")]
    StateEncode(serde_json::Error),
    #[error("failed to decode state from JSON: {0}")]
    StateDecode(serde_json::Error),
    #[error("automation returned an error: {0}")]
    Automation(String),
}

/// Build the serialized registration payload returned by the guest.
///
/// # Errors
///
/// Returns an error when the guest descriptor or initial state cannot be
/// serialized into the host registration envelope.
pub fn registration_payload<A>(automation: &A) -> Result<Vec<u8>, GuestSdkError>
where
    A: Automation,
{
    let initial_state =
        serde_json::to_value(automation.initial_state()).map_err(GuestSdkError::StateEncode)?;
    serde_json::to_vec(&RegistrationEnvelope::Ok(RegistrationResponse {
        descriptor: automation.descriptor(),
        initial_state,
    }))
    .map_err(GuestSdkError::RegistrationEncode)
}

/// Advance an automation from a raw host request payload and serialize the
/// response envelope expected by the control plane.
///
/// # Errors
///
/// Returns an error when the request cannot be decoded, the stored state cannot
/// be deserialized, the automation rejects the event, or the response cannot be
/// encoded back to JSON.
pub fn advance_payload<A>(automation: &A, request_bytes: &[u8]) -> Result<Vec<u8>, GuestSdkError>
where
    A: Automation,
{
    let request: AdvanceRequest =
        serde_json::from_slice(request_bytes).map_err(GuestSdkError::AdvanceDecode)?;
    let state =
        serde_json::from_value(request.state.clone()).map_err(GuestSdkError::StateDecode)?;
    let decision = automation
        .reduce(
            state,
            request.event,
            GuestContext {
                automation_id: request.automation_id,
                revision_id: request.revision_id,
                now: request.now,
                config: request.config,
            },
        )
        .map_err(GuestSdkError::Automation)?;

    let response = AdvanceEnvelope::Ok(AdvanceResponse {
        state: serde_json::to_value(decision.state).map_err(GuestSdkError::StateEncode)?,
        effects: decision.effects,
        status: decision.status,
        disposition: decision.disposition,
    });
    serde_json::to_vec(&response).map_err(GuestSdkError::AdvanceEncode)
}

/// Build a serialized registration error envelope.
///
/// # Panics
///
/// Panics if the error envelope cannot be serialized. That indicates a bug in
/// the stable protocol type rather than user automation logic.
pub fn error_registration_payload(message: impl Into<String>) -> Vec<u8> {
    serde_json::to_vec(&RegistrationEnvelope::Err(message.into()))
        .expect("registration error envelope should serialize")
}

/// Build a serialized advance error envelope.
///
/// # Panics
///
/// Panics if the error envelope cannot be serialized. That indicates a bug in
/// the stable protocol type rather than user automation logic.
pub fn error_advance_payload(message: impl Into<String>) -> Vec<u8> {
    serde_json::to_vec(&AdvanceEnvelope::Err(message.into()))
        .expect("advance error envelope should serialize")
}

/// Pack an owned byte buffer into the raw `(ptr,len)` handle used by the Wasm
/// guest ABI.
///
/// # Panics
///
/// Panics if the buffer length or guest pointer address does not fit in the
/// 32-bit Wasm ABI used by Mango automations.
#[must_use]
pub fn boxed_bytes_into_raw(bytes: Vec<u8>) -> u64 {
    let boxed = bytes.into_boxed_slice();
    let len =
        u32::try_from(boxed.len()).expect("boxed guest buffer length should fit in Wasm32 ABI");
    let raw = Box::into_raw(boxed).cast::<u8>();
    let ptr =
        u32::try_from(raw.addr()).expect("boxed guest buffer pointer should fit in Wasm32 ABI");
    (u64::from(len) << 32) | u64::from(ptr)
}

/// # Safety
///
/// The pointer and length must refer to a buffer previously allocated by
/// [`allocate_bytes`] or [`boxed_bytes_into_raw`] in this module.
pub unsafe fn free_boxed_bytes(ptr: u32, len: u32) {
    let slice = std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, len as usize);
    // SAFETY: The caller guarantees that `ptr,len` came from this allocator.
    unsafe {
        drop(Box::from_raw(slice));
    }
}

#[must_use]
/// Allocate a zeroed guest buffer and return its raw Wasm32 pointer.
///
/// # Panics
///
/// Panics if the allocated guest pointer address does not fit in the 32-bit
/// Wasm ABI used by Mango automations.
pub fn allocate_bytes(len: u32) -> u32 {
    let boxed = vec![0_u8; len as usize].into_boxed_slice();
    let raw = Box::into_raw(boxed).cast::<u8>();
    u32::try_from(raw.addr()).expect("allocated guest buffer pointer should fit in Wasm32 ABI")
}

#[macro_export]
macro_rules! export_automation {
    ($automation:expr) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn mango_automation_abi_version() -> u32 {
            $crate::AUTOMATION_ABI_VERSION
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn mango_automation_alloc(len: u32) -> u32 {
            $crate::allocate_bytes(len)
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn mango_automation_free(ptr: u32, len: u32) {
            // SAFETY: The host is required to free only buffers allocated by
            // `mango_automation_alloc` or returned by this guest.
            unsafe { $crate::free_boxed_bytes(ptr, len) };
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn mango_automation_register() -> u64 {
            let automation = $automation;
            let bytes = match $crate::registration_payload(&automation) {
                Ok(bytes) => bytes,
                Err(error) => $crate::error_registration_payload(error.to_string()),
            };
            $crate::boxed_bytes_into_raw(bytes)
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn mango_automation_advance(ptr: u32, len: u32) -> u64 {
            let automation = $automation;
            // SAFETY: The host promises to pass a valid pointer/length pair
            // into guest memory allocated by `mango_automation_alloc`.
            let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
            let response = match $crate::advance_payload(&automation, bytes) {
                Ok(bytes) => bytes,
                Err(error) => $crate::error_advance_payload(error.to_string()),
            };
            $crate::boxed_bytes_into_raw(response)
        }
    };
}

pub fn effect(effect_id: impl Into<String>, kind: EffectKind) -> EffectRequest {
    EffectRequest::new(effect_id, kind)
}

/// Serialize an arbitrary value into JSON for guest helpers and tests.
///
/// # Panics
///
/// Panics if the provided value cannot be serialized to JSON.
pub fn json(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("json helper should serialize value")
}
