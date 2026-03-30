use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use mango_automation_protocol::{
    AdvanceRequest, AdvanceResponse, AutomationDescriptor, AutomationEvent, Capability, EffectKind,
    EffectRequest, RegistrationResponse,
};
use mango_automations::{
    ActivationMode, AutomationRuntime, AutomationsControlPlane, AutomationsError, EffectHandler,
    EffectHandlerOutcome, JsonFileControlPlaneStore, ManualClock, RegistrationRequest, TraceEvent,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

const START_TIME: i64 = 1_774_522_740;

#[derive(Debug, Clone, Default)]
struct PersistenceRuntime;

impl AutomationRuntime for PersistenceRuntime {
    fn register(&self, _artifact_path: &Path) -> Result<RegistrationResponse, AutomationsError> {
        Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.persistence",
                "restart-safe wakeup fixture",
                1,
                vec![Capability::ScheduleWakeups, Capability::EmitNotifications],
            ),
            initial_state: json!({
                "phase": "idle",
            }),
        })
    }

    fn advance(
        &self,
        _artifact_path: &Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError> {
        match &request.event {
            AutomationEvent::Activated { .. } => Ok(AdvanceResponse {
                state: json!({ "phase": "armed" }),
                effects: vec![EffectRequest::new(
                    "wake-once",
                    EffectKind::ScheduleWakeup {
                        wakeup_id: "once".to_string(),
                        at: request.now + 60,
                    },
                )],
                status: Some("armed".to_string()),
            }),
            AutomationEvent::WakeupFired { wakeup_id, .. } if wakeup_id == "once" => {
                Ok(AdvanceResponse {
                    state: json!({
                        "phase": "executed",
                    }),
                    effects: vec![EffectRequest::new(
                        "notify-once",
                        EffectKind::EmitNotification {
                            channel: "telegram".to_string(),
                            title: "Restart-safe".to_string(),
                            body: "delivery after restart".to_string(),
                            metadata: request.config.clone(),
                        },
                    )],
                    status: Some("executed".to_string()),
                })
            }
            _ => Ok(AdvanceResponse {
                state: request.state.clone(),
                effects: Vec::new(),
                status: Some("idle".to_string()),
            }),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingHandler {
    payloads: Arc<Mutex<Vec<Value>>>,
}

impl RecordingHandler {
    async fn payloads(&self) -> Vec<Value> {
        self.payloads.lock().await.clone()
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
        if let EffectKind::EmitNotification { metadata, .. } = &effect.kind {
            self.payloads.lock().await.push(metadata.clone());
        }
        Ok(EffectHandlerOutcome::default())
    }
}

#[tokio::test]
async fn registration_survives_restart_and_due_wakeup_executes()
-> Result<(), Box<dyn std::error::Error>> {
    let tempdir = tempfile::tempdir()?;
    let state_path = tempdir.path().join("automations.json");
    let artifact_path = tempdir.path().join("fixture.wasm");
    std::fs::write(&artifact_path, b"fixture")?;

    {
        let clock = ManualClock::new(START_TIME);
        let control_plane = AutomationsControlPlane::with_runtime(
            JsonFileControlPlaneStore::new(&state_path),
            PersistenceRuntime,
            RecordingHandler::default(),
            clock,
        );

        let revision = control_plane.register_revision(&RegistrationRequest {
            automation_id: "fixture".to_string(),
            artifact_path: artifact_path.clone(),
            config: json!({
                "chat_id": 77,
                "thread_id": 3,
            }),
        })?;
        control_plane
            .activate_revision("fixture", revision.revision_id, ActivationMode::ColdStart)
            .await?;
        assert_eq!(control_plane.automations()?.len(), 1);
    }

    let clock = ManualClock::new(START_TIME + 60);
    let handler = RecordingHandler::default();
    let control_plane = AutomationsControlPlane::with_runtime(
        JsonFileControlPlaneStore::new(&state_path),
        PersistenceRuntime,
        handler.clone(),
        clock,
    );

    control_plane.reconcile_due().await?;

    let automations = control_plane.automations()?;
    let automation = &automations["fixture"];
    assert_eq!(automation.active_revision_id, Some(1));
    assert_eq!(automation.last_status.as_deref(), Some("executed"));
    assert_eq!(
        automation.current_state,
        Some(json!({ "phase": "executed" }))
    );

    let payloads = handler.payloads().await;
    assert_eq!(payloads, vec![json!({ "chat_id": 77, "thread_id": 3 })]);

    let traces = control_plane.traces()?;
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::WakeupDispatched { ref wakeup_id, .. } if wakeup_id == "once"
    )));
    assert!(traces.iter().any(|trace| matches!(
        trace.event,
        TraceEvent::EffectHandled { ref effect_id, .. } if effect_id == "notify-once"
    )));

    Ok(())
}
