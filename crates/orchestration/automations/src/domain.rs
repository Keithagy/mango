use std::{collections::BTreeMap, path::PathBuf};

use mango_automation_protocol::{
    AdvanceResponse, AutomationDescriptor, AutomationEvent, EffectKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RevisionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivationMode {
    ColdStart,
    PreserveState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisteredRevision {
    pub revision_id: RevisionId,
    pub artifact_path: PathBuf,
    pub artifact_sha256: String,
    pub registered_at: i64,
    pub descriptor: AutomationDescriptor,
    pub config: Value,
    pub initial_state: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledWakeup {
    pub wakeup_id: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedAutomation {
    pub automation_id: String,
    pub revisions: BTreeMap<RevisionId, RegisteredRevision>,
    pub active_revision_id: Option<RevisionId>,
    pub current_state: Option<Value>,
    pub scheduled_wakeups: BTreeMap<String, ScheduledWakeup>,
    pub last_status: Option<String>,
}

impl ManagedAutomation {
    #[must_use]
    pub fn new(automation_id: impl Into<String>) -> Self {
        Self {
            automation_id: automation_id.into(),
            revisions: BTreeMap::new(),
            active_revision_id: None,
            current_state: None,
            scheduled_wakeups: BTreeMap::new(),
            last_status: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub automations: BTreeMap<String, ManagedAutomation>,
    pub traces: Vec<TraceRecord>,
    pub next_revision_id: RevisionId,
}

impl ControlPlaneState {
    pub fn allocate_revision_id(&mut self) -> RevisionId {
        self.next_revision_id += 1;
        self.next_revision_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub at: i64,
    pub event: TraceEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TraceEvent {
    RevisionRegistered {
        automation_id: String,
        revision_id: RevisionId,
        artifact_sha256: String,
    },
    RevisionActivated {
        automation_id: String,
        revision_id: RevisionId,
        mode: ActivationMode,
    },
    AutomationDeactivated {
        automation_id: String,
        revision_id: RevisionId,
    },
    AutomationDeleted {
        automation_id: String,
    },
    EventSubmitted {
        automation_id: String,
        revision_id: RevisionId,
        event: AutomationEvent,
    },
    StateAdvanced {
        automation_id: String,
        revision_id: RevisionId,
        response: AdvanceResponse,
    },
    WakeupScheduled {
        automation_id: String,
        revision_id: RevisionId,
        wakeup_id: String,
        at: i64,
    },
    WakeupCancelled {
        automation_id: String,
        revision_id: RevisionId,
        wakeup_id: String,
    },
    WakeupDispatched {
        automation_id: String,
        revision_id: RevisionId,
        wakeup_id: String,
        at: i64,
    },
    EffectRequested {
        automation_id: String,
        revision_id: RevisionId,
        effect_id: String,
        effect_kind: String,
    },
    EffectHandled {
        automation_id: String,
        revision_id: RevisionId,
        effect_id: String,
        follow_up_events: usize,
    },
}

#[must_use]
pub fn effect_kind_label(kind: &EffectKind) -> &'static str {
    match kind {
        EffectKind::ScheduleWakeup { .. } => "schedule_wakeup",
        EffectKind::CancelWakeup { .. } => "cancel_wakeup",
        EffectKind::EmitNotification { .. } => "emit_notification",
        EffectKind::FetchHttp { .. } => "fetch_http",
        EffectKind::ReadProfile { .. } => "read_profile",
        EffectKind::RunCommand { .. } => "run_command",
        EffectKind::RunModel { .. } => "run_model",
    }
}
