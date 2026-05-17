//! Shared host/guest contract for Mango automations.
//!
//! This crate defines the stable boundary between:
//! - the guest automation artifact compiled to Wasm
//! - the control plane that registers, activates, and drives that artifact
//! - the pocket-universe simulator that replays the same contract in tests

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const AUTOMATION_ABI_VERSION: u32 = 1;
pub const AUTOMATION_SDK_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomationDescriptor {
    pub sdk_version: u32,
    pub automation_name: String,
    pub description: String,
    pub state_schema_version: u32,
    pub capabilities: Vec<Capability>,
}

impl AutomationDescriptor {
    #[must_use]
    pub fn new(
        automation_name: impl Into<String>,
        description: impl Into<String>,
        state_schema_version: u32,
        capabilities: Vec<Capability>,
    ) -> Self {
        Self {
            sdk_version: AUTOMATION_SDK_VERSION,
            automation_name: automation_name.into(),
            description: description.into(),
            state_schema_version,
            capabilities,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Capability {
    EmitNotifications,
    CallTools,
    FetchHttp,
    ReadProfile,
    RunCommand,
    RunInference,
    RunModel,
    ScheduleWakeups,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub descriptor: AutomationDescriptor,
    pub initial_state: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RegistrationEnvelope {
    Ok(RegistrationResponse),
    Err(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdvanceRequest {
    pub automation_id: String,
    pub revision_id: u64,
    pub now: i64,
    pub config: Value,
    pub state: Value,
    pub event: AutomationEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdvanceResponse {
    pub state: Value,
    pub effects: Vec<EffectRequest>,
    pub status: Option<String>,
    pub disposition: EventDisposition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdvanceEnvelope {
    Ok(AdvanceResponse),
    Err(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AutomationEvent {
    Activated {
        at: i64,
    },
    TriggerFired {
        trigger: String,
        payload: Value,
        at: i64,
    },
    WakeupFired {
        wakeup_id: String,
        at: i64,
    },
    UserSignal {
        signal: String,
        payload: Value,
        at: i64,
    },
    EffectCompleted {
        effect_id: String,
        result: EffectResult,
        at: i64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventDisposition {
    Unhandled,
    // TODO: This is the current single-automation flow-control contract.
    // Revisit it when multi-automation scatter-gather and concurrent baseline
    // chat fan-out are first-class so hosts can merge multiple outcomes
    // without overloading a binary handled/unhandled flag.
    #[default]
    Handled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRequest {
    pub effect_id: String,
    pub kind: EffectKind,
}

impl EffectRequest {
    #[must_use]
    pub fn new(effect_id: impl Into<String>, kind: EffectKind) -> Self {
        Self {
            effect_id: effect_id.into(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EffectKind {
    ScheduleWakeup {
        wakeup_id: String,
        at: i64,
    },
    CancelWakeup {
        wakeup_id: String,
    },
    EmitNotification {
        channel: String,
        title: String,
        body: String,
        metadata: Value,
    },
    FetchHttp {
        url: String,
    },
    ReadProfile {
        keys: Vec<String>,
    },
    CallTool {
        slug: String,
        input: Value,
    },
    RunCommand {
        program: String,
        args: Vec<String>,
    },
    RunInference {
        capability: String,
        input: Value,
    },
    RunModel {
        prompt: String,
        system: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EffectResult {
    Ok(Value),
    Err(String),
}
