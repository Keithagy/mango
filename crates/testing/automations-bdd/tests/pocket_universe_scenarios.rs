use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mango_automation_protocol::{
    AdvanceRequest, AdvanceResponse, AutomationDescriptor, AutomationEvent, Capability, EffectKind,
    EffectRequest, RegistrationResponse,
};
use mango_automations::{
    ActivationMode, AutomationRuntime, AutomationsError, EffectHandler, EffectHandlerOutcome,
    PocketUniverse, RegistrationRequest, TraceEvent, TraceRecord,
};
use mango_automations_bdd::{
    AutomationsScenarioWorld, Scenario, ScenarioFailure, TimeDrivenScenarioWorld,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const START_TIME: i64 = 1_774_522_740;

fn summarize_trace(trace: &TraceRecord) -> String {
    match &trace.event {
        TraceEvent::WakeupScheduled { wakeup_id, at, .. } => {
            format!("wakeup_scheduled {wakeup_id} at {at}")
        }
        TraceEvent::EffectRequested {
            effect_id,
            effect_kind,
            ..
        } => format!("effect_requested {effect_id} {effect_kind}"),
        TraceEvent::EffectHandled {
            effect_id,
            follow_up_events,
            ..
        } => format!("effect_handled {effect_id} {follow_up_events}"),
        event => format!("{event:?}"),
    }
}

#[derive(Debug, Clone, Default)]
struct FixtureRuntime;

impl AutomationRuntime for FixtureRuntime {
    fn register(
        &self,
        _artifact_path: &std::path::Path,
    ) -> Result<RegistrationResponse, AutomationsError> {
        Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.loop",
                "generic loop fixture",
                1,
                vec![Capability::ScheduleWakeups, Capability::EmitNotifications],
            ),
            initial_state: json!({
                "armed": false,
                "notification_count": 0_u64,
            }),
        })
    }

    fn advance(
        &self,
        _artifact_path: &std::path::Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError> {
        let mut state = request.state.clone();
        let armed = state["armed"].as_bool().unwrap_or(false);
        let count = state["notification_count"].as_u64().unwrap_or(0);

        match &request.event {
            AutomationEvent::Activated { .. } => Ok(AdvanceResponse {
                state: json!({
                    "armed": true,
                    "notification_count": count,
                }),
                effects: vec![EffectRequest::new(
                    "wake-activate",
                    EffectKind::ScheduleWakeup {
                        wakeup_id: "pulse".to_string(),
                        at: request.now + 60,
                    },
                )],
                status: Some("armed".to_string()),
            }),
            AutomationEvent::WakeupFired { .. } if armed => {
                state["armed"] = json!(true);
                state["notification_count"] = json!(count + 1);
                Ok(AdvanceResponse {
                    state,
                    effects: vec![
                        EffectRequest::new(
                            format!("notify-{}", count + 1),
                            EffectKind::EmitNotification {
                                channel: "demo".to_string(),
                                title: "Pulse".to_string(),
                                body: format!("pulse {}", count + 1),
                                metadata: Value::Null,
                            },
                        ),
                        EffectRequest::new(
                            format!("wake-{}", count + 1),
                            EffectKind::ScheduleWakeup {
                                wakeup_id: "pulse".to_string(),
                                at: request.now + 60,
                            },
                        ),
                    ],
                    status: Some("pulsing".to_string()),
                })
            }
            AutomationEvent::UserSignal { signal, .. } if signal == "stop" => Ok(AdvanceResponse {
                state: json!({
                    "armed": false,
                    "notification_count": count,
                }),
                effects: Vec::new(),
                status: Some("stopped".to_string()),
            }),
            _ => Ok(AdvanceResponse {
                state,
                effects: Vec::new(),
                status: Some("idle".to_string()),
            }),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingHandler {
    notifications: Arc<Mutex<Vec<String>>>,
}

impl RecordingHandler {
    fn notifications(&self) -> Vec<String> {
        self.notifications
            .lock()
            .expect("notifications lock")
            .clone()
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
        if let EffectKind::EmitNotification { body, .. } = &effect.kind {
            self.notifications
                .lock()
                .expect("notifications lock")
                .push(body.clone());
        }
        Ok(EffectHandlerOutcome::default())
    }
}

#[derive(Debug)]
struct LoopWorld {
    _tempdir: TempDir,
    universe: PocketUniverse<FixtureRuntime, RecordingHandler>,
    handler: RecordingHandler,
}

impl LoopWorld {
    async fn new(initial_timestamp: i64) -> Result<Self, AutomationsError> {
        let tempdir =
            tempfile::tempdir().map_err(|error| AutomationsError::Io(error.to_string()))?;
        let artifact_path = tempdir.path().join("fixture.wasm");
        std::fs::write(&artifact_path, b"fixture")
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        let handler = RecordingHandler::default();
        let universe = PocketUniverse::new(initial_timestamp, FixtureRuntime, handler.clone());
        let revision = universe.register_revision(&RegistrationRequest {
            automation_id: "loop".to_string(),
            artifact_path,
            config: Value::Null,
        })?;
        universe
            .activate_revision("loop", revision.revision_id, ActivationMode::ColdStart)
            .await?;
        Ok(Self {
            _tempdir: tempdir,
            universe,
            handler,
        })
    }

    async fn stop(&self) -> Result<(), AutomationsError> {
        self.universe
            .submit_user_signal("loop", "stop", Value::Null)
            .await
    }

    fn notifications(&self) -> Vec<String> {
        self.handler.notifications()
    }
}

#[async_trait]
impl AutomationsScenarioWorld for LoopWorld {
    async fn traces(&mut self) -> Result<Vec<TraceRecord>, AutomationsError> {
        self.universe.traces()
    }
}

#[async_trait]
impl TimeDrivenScenarioWorld for LoopWorld {
    fn advance_time_by(&mut self, seconds: i64) {
        self.universe.advance_time_by(seconds);
    }

    async fn settle_automations(&mut self) -> Result<(), AutomationsError> {
        self.universe.settle().await.map(|_| ())
    }
}

#[tokio::test]
async fn pocket_universe_replays_control_plane_contracts() -> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "generic fixture loops until a stop signal is delivered",
        LoopWorld::new(START_TIME)
            .await
            .expect("world should initialize"),
    )
    .with_recent_trace_limit(12)
    .with_trace_summary(summarize_trace);

    scenario
        .when("time advances to the first scheduled wakeup")
        .advance_time_by_and_settle(60)
        .await?;

    scenario
        .then("a notification effect is observed")
        .expect_eventually(
            "an emitted notification effect",
            std::time::Duration::from_millis(50),
            |trace| {
                matches!(
                    trace.event,
                    TraceEvent::EffectHandled { ref effect_id, .. } if effect_id == "notify-1"
                )
            },
        )
        .await?;

    assert_eq!(
        scenario.world().notifications(),
        vec!["pulse 1".to_string()]
    );

    scenario
        .when("a stop signal is sent before the next wakeup")
        .perform(|world| {
            Box::pin(async move {
                world.stop().await?;
                world.advance_time_by_and_settle(60).await
            })
        })
        .await?;

    assert_eq!(
        scenario.world().notifications(),
        vec!["pulse 1".to_string()],
        "once stopped, the loop should not emit another notification"
    );

    Ok(())
}
