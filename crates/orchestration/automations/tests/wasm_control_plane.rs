use std::{
    fmt::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use mango_automation_protocol::{
    AdvanceEnvelope, AdvanceResponse, AutomationDescriptor, Capability, EffectKind, EffectRequest,
    RegistrationEnvelope, RegistrationResponse,
};
use mango_automations::{
    ActivationMode, AutomationsControlPlane, AutomationsError, EffectHandler, EffectHandlerOutcome,
    ManualClock, MemoryControlPlaneStore, NoopEffectHandler, RegistrationRequest, TraceEvent,
};
use serde_json::json;
use tempfile::tempdir;

#[derive(Debug, Clone, Default)]
struct RecordingHandler {
    effects: Arc<Mutex<Vec<EffectRequest>>>,
}

impl RecordingHandler {
    fn effects(&self) -> Vec<EffectRequest> {
        self.effects.lock().expect("effects lock").clone()
    }
}

#[async_trait]
impl EffectHandler for RecordingHandler {
    async fn handle_effect(
        &self,
        _automation_id: &str,
        _revision_id: u64,
        effect: &EffectRequest,
        _now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        self.effects
            .lock()
            .expect("effects lock")
            .push(effect.clone());
        Ok(EffectHandlerOutcome::default())
    }
}

fn write_static_guest(
    dir: &Path,
    name: &str,
    registration: &RegistrationEnvelope,
    advance: &AdvanceEnvelope,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let register_bytes = serde_json::to_vec(registration)?;
    let advance_bytes = serde_json::to_vec(advance)?;
    let register_len = u32::try_from(register_bytes.len())?;
    let advance_len = u32::try_from(advance_bytes.len())?;
    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (func (export "mango_automation_abi_version") (result i32)
                i32.const 1)
            (func (export "mango_automation_alloc") (param i32) (result i32)
                i32.const 16384)
            (func (export "mango_automation_free") (param i32 i32))
            (func (export "mango_automation_register") (result i64)
                i64.const {})
            (func (export "mango_automation_advance") (param i32 i32) (result i64)
                i64.const {})
            (data (i32.const 0) "{}")
            (data (i32.const 4096) "{}")
        )"#,
        pack_handle(0, register_len),
        pack_handle(4096, advance_len),
        wat_bytes(&register_bytes),
        wat_bytes(&advance_bytes),
    );
    let wasm = wat::parse_str(wat)?;
    let path = dir.join(format!("{name}.wasm"));
    std::fs::write(&path, wasm)?;
    Ok(path)
}

fn pack_handle(ptr: u32, len: u32) -> u64 {
    (u64::from(len) << 32) | u64::from(ptr)
}

fn wat_bytes(bytes: &[u8]) -> String {
    let mut rendered = String::new();
    for byte in bytes {
        write!(&mut rendered, "\\{byte:02x}").expect("writing to a String should not fail");
    }
    rendered
}

#[tokio::test]
async fn revisions_must_be_registered_before_activation_and_activation_runs_guest_entrypoint()
-> Result<(), Box<dyn std::error::Error>> {
    let tempdir = tempdir()?;
    let artifact = write_static_guest(
        tempdir.path(),
        "scheduler",
        &RegistrationEnvelope::Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.scheduler",
                "static fixture scheduler",
                1,
                vec![Capability::ScheduleWakeups],
            ),
            initial_state: json!({ "phase": "registered" }),
        }),
        &AdvanceEnvelope::Ok(AdvanceResponse {
            state: json!({ "phase": "armed" }),
            effects: vec![EffectRequest::new(
                "wakeup-1",
                EffectKind::ScheduleWakeup {
                    wakeup_id: "tick".to_string(),
                    at: 120,
                },
            )],
            status: Some("armed".to_string()),
        }),
    )?;

    let clock = ManualClock::new(100);
    let control_plane = AutomationsControlPlane::new(
        MemoryControlPlaneStore::new(),
        NoopEffectHandler,
        clock.clone(),
    );

    let revision = control_plane.register_revision(&RegistrationRequest {
        automation_id: "fixture".to_string(),
        artifact_path: artifact,
        config: json!(null),
    })?;
    let before_activation = control_plane.automations()?;
    assert_eq!(
        before_activation["fixture"].active_revision_id, None,
        "registration should not implicitly activate a revision"
    );

    control_plane
        .activate_revision("fixture", revision.revision_id, ActivationMode::ColdStart)
        .await?;

    let after_activation = control_plane.automations()?;
    let automation = &after_activation["fixture"];
    assert_eq!(automation.active_revision_id, Some(revision.revision_id));
    assert_eq!(automation.current_state, Some(json!({ "phase": "armed" })));
    assert_eq!(automation.last_status.as_deref(), Some("armed"));
    assert!(automation.scheduled_wakeups.contains_key("tick"));

    let traces = control_plane.traces()?;
    assert!(
        traces
            .iter()
            .any(|trace| matches!(trace.event, TraceEvent::RevisionRegistered { .. }))
    );
    assert!(
        traces
            .iter()
            .any(|trace| matches!(trace.event, TraceEvent::RevisionActivated { .. }))
    );
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::WakeupScheduled { ref wakeup_id, .. } if wakeup_id == "tick"
    )));

    Ok(())
}

#[tokio::test]
async fn wakeups_and_effects_flow_through_control_plane_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let tempdir = tempdir()?;
    let artifact = write_static_guest(
        tempdir.path(),
        "notifier",
        &RegistrationEnvelope::Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.notifier",
                "static fixture notifier",
                1,
                vec![Capability::ScheduleWakeups, Capability::EmitNotifications],
            ),
            initial_state: json!({ "phase": "idle" }),
        }),
        &AdvanceEnvelope::Ok(AdvanceResponse {
            state: json!({ "phase": "notified" }),
            effects: vec![EffectRequest::new(
                "notify-1",
                EffectKind::EmitNotification {
                    channel: "demo".to_string(),
                    title: "Reminder".to_string(),
                    body: "Observe the control plane boundary".to_string(),
                    metadata: json!(null),
                },
            )],
            status: Some("notified".to_string()),
        }),
    )?;

    let clock = ManualClock::new(100);
    let handler = RecordingHandler::default();
    let control_plane = AutomationsControlPlane::new(
        MemoryControlPlaneStore::new(),
        handler.clone(),
        clock.clone(),
    );

    let revision = control_plane.register_revision(&RegistrationRequest {
        automation_id: "fixture".to_string(),
        artifact_path: artifact,
        config: json!(null),
    })?;
    control_plane
        .activate_revision("fixture", revision.revision_id, ActivationMode::ColdStart)
        .await?;

    let observed_effects = handler.effects();
    assert_eq!(observed_effects.len(), 1);
    assert!(matches!(
        observed_effects[0].kind,
        EffectKind::EmitNotification { .. }
    ));

    let traces = control_plane.traces()?;
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::EffectRequested { ref effect_id, .. } if effect_id == "notify-1"
    )));
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::EffectHandled { ref effect_id, follow_up_events, .. }
            if effect_id == "notify-1" && follow_up_events == 0
    )));

    Ok(())
}

#[tokio::test]
async fn preserve_state_rejects_incompatible_schema_versions()
-> Result<(), Box<dyn std::error::Error>> {
    let tempdir = tempdir()?;
    let first = write_static_guest(
        tempdir.path(),
        "schema-v1",
        &RegistrationEnvelope::Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.schema",
                "schema v1",
                1,
                vec![Capability::ScheduleWakeups],
            ),
            initial_state: json!({ "schema": 1 }),
        }),
        &AdvanceEnvelope::Ok(AdvanceResponse {
            state: json!({ "schema": 1, "armed": true }),
            effects: Vec::new(),
            status: Some("v1".to_string()),
        }),
    )?;
    let second = write_static_guest(
        tempdir.path(),
        "schema-v2",
        &RegistrationEnvelope::Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.schema",
                "schema v2",
                2,
                vec![Capability::ScheduleWakeups],
            ),
            initial_state: json!({ "schema": 2 }),
        }),
        &AdvanceEnvelope::Ok(AdvanceResponse {
            state: json!({ "schema": 2 }),
            effects: Vec::new(),
            status: Some("v2".to_string()),
        }),
    )?;

    let clock = ManualClock::new(100);
    let control_plane =
        AutomationsControlPlane::new(MemoryControlPlaneStore::new(), NoopEffectHandler, clock);
    let v1 = control_plane.register_revision(&RegistrationRequest {
        automation_id: "fixture".to_string(),
        artifact_path: first,
        config: json!(null),
    })?;
    control_plane
        .activate_revision("fixture", v1.revision_id, ActivationMode::ColdStart)
        .await?;
    let v2 = control_plane.register_revision(&RegistrationRequest {
        automation_id: "fixture".to_string(),
        artifact_path: second,
        config: json!(null),
    })?;

    let error = control_plane
        .activate_revision("fixture", v2.revision_id, ActivationMode::PreserveState)
        .await
        .expect_err("schema mismatch should reject preserve-state activation");
    assert!(matches!(
        error,
        AutomationsError::IncompatibleState { revision_id, .. } if revision_id == v2.revision_id
    ));

    Ok(())
}
