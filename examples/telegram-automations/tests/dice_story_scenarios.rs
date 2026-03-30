use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use mango_automation_control::{
    ActivationMode, AutomationRuntime, AutomationsError, EffectHandler, EffectHandlerOutcome,
    ManagedAutomation, PocketUniverse, RegistrationRequest, TraceEvent, TraceRecord,
};
use mango_automation_sdk::{
    AdvanceRequest, AdvanceResponse, AutomationDescriptor, AutomationEvent, Capability, EffectKind,
    EffectRequest, EffectResult, RegistrationResponse,
};
use mango_automations_bdd::{AutomationsScenarioWorld, Scenario, ScenarioFailure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Mutex;

const START_TIME: i64 = 1_774_522_740;
const TICK_WAKEUP_ID: &str = "tick";

fn summarize_trace(trace: &TraceRecord) -> String {
    match &trace.event {
        TraceEvent::WakeupDispatched { wakeup_id, at, .. } => {
            format!("wakeup_dispatched {wakeup_id} at {at}")
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

fn repeated_word_story(word: &str, word_count: usize) -> String {
    std::iter::repeat_n(word, word_count)
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}

fn deterministic_seed(automation_id: &str, fired_at: i64) -> u64 {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    let mut hasher = DefaultHasher::new();
    automation_id.hash(&mut hasher);
    fired_at.hash(&mut hasher);
    hasher.finish().max(1)
}

fn deterministic_roll(seed: u64, sides: u8) -> u8 {
    u8::try_from(seed % u64::from(sides)).unwrap_or(0) + 1
}

fn build_story_prompt(target_words: usize, roll: u8, previous_word_count: Option<usize>) -> String {
    match previous_word_count {
        Some(previous_word_count) => format!(
            "Your previous answer had {previous_word_count} words. Write exactly {target_words} words telling a short story about the number {roll}. Plain text only."
        ),
        None => format!(
            "Write exactly {target_words} words telling a short story about the number {roll}. Plain text only."
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TelegramTarget {
    chat_id: i64,
    thread_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TelegramPayload {
    chat_id: i64,
    thread_id: Option<i32>,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryConfig {
    target: TelegramTarget,
    period_seconds: u64,
    target_words: usize,
    max_llm_attempts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingRun {
    fired_at: i64,
    seed: u64,
    roll: Option<u8>,
    attempt: u8,
    previous_word_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RunSummary {
    fired_at: i64,
    status: String,
    seed: u64,
    roll: Option<u8>,
    attempts: u8,
    word_count: Option<usize>,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryState {
    next_fire_at: Option<i64>,
    pending_run: Option<PendingRun>,
    recent_runs: Vec<RunSummary>,
}

#[derive(Debug, Clone, Default)]
struct DiceStoryRuntime;

impl AutomationRuntime for DiceStoryRuntime {
    fn register(
        &self,
        _artifact_path: &std::path::Path,
    ) -> Result<RegistrationResponse, AutomationsError> {
        Ok(RegistrationResponse {
            descriptor: AutomationDescriptor::new(
                "fixture.dice_story",
                "dice story automation fixture",
                1,
                vec![
                    Capability::ScheduleWakeups,
                    Capability::RunCommand,
                    Capability::RunModel,
                    Capability::EmitNotifications,
                ],
            ),
            initial_state: json!({
                "next_fire_at": Value::Null,
                "pending_run": Value::Null,
                "recent_runs": [],
            }),
        })
    }

    fn advance(
        &self,
        _artifact_path: &std::path::Path,
        request: &AdvanceRequest,
    ) -> Result<AdvanceResponse, AutomationsError> {
        let config: DiceStoryConfig = serde_json::from_value(request.config.clone())
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        let state: DiceStoryState = serde_json::from_value(request.state.clone())
            .map_err(|error| AutomationsError::Guest(error.to_string()))?;
        advance_fixture(request, state, &config)
    }
}

fn advance_fixture(
    request: &AdvanceRequest,
    state: DiceStoryState,
    config: &DiceStoryConfig,
) -> Result<AdvanceResponse, AutomationsError> {
    match &request.event {
        AutomationEvent::Activated { at } => advance_activated(state, config, *at),
        AutomationEvent::WakeupFired { wakeup_id, at } if wakeup_id == TICK_WAKEUP_ID => {
            advance_wakeup(state, &request.automation_id, config, *at)
        }
        AutomationEvent::EffectCompleted {
            effect_id,
            result: EffectResult::Ok(payload),
            ..
        } if effect_id.starts_with("roll-") => advance_roll_completed(state, config, payload),
        AutomationEvent::EffectCompleted {
            effect_id,
            result: EffectResult::Ok(payload),
            ..
        } if effect_id.starts_with("model-") => advance_story_completed(state, config, payload),
        AutomationEvent::EffectCompleted {
            result: EffectResult::Err(message),
            ..
        } => advance_effect_failed(state, message),
        _ => idle_response(state),
    }
}

fn idle_response(state: DiceStoryState) -> Result<AdvanceResponse, AutomationsError> {
    response_with_state(state, Vec::new(), "idle")
}

fn response_with_state(
    state: DiceStoryState,
    effects: Vec<EffectRequest>,
    status: &str,
) -> Result<AdvanceResponse, AutomationsError> {
    Ok(AdvanceResponse {
        state: serde_json::to_value(state)
            .map_err(|error| AutomationsError::Guest(error.to_string()))?,
        effects,
        status: Some(status.to_string()),
    })
}

fn advance_activated(
    mut state: DiceStoryState,
    config: &DiceStoryConfig,
    at: i64,
) -> Result<AdvanceResponse, AutomationsError> {
    let next_fire_at = at + i64::try_from(config.period_seconds).unwrap_or(0);
    state.next_fire_at = Some(next_fire_at);
    response_with_state(state, vec![schedule_effect(next_fire_at)], "armed")
}

fn advance_wakeup(
    mut state: DiceStoryState,
    automation_id: &str,
    config: &DiceStoryConfig,
    at: i64,
) -> Result<AdvanceResponse, AutomationsError> {
    let seed = deterministic_seed(automation_id, at);
    let next_fire_at = at + i64::try_from(config.period_seconds).unwrap_or(0);
    state.next_fire_at = Some(next_fire_at);
    state.pending_run = Some(PendingRun {
        fired_at: at,
        seed,
        roll: None,
        attempt: 1,
        previous_word_count: None,
    });
    response_with_state(
        state,
        vec![
            schedule_effect(next_fire_at),
            EffectRequest::new(
                format!("roll-{at}"),
                EffectKind::RunCommand {
                    program: "fixture-dice".to_string(),
                    args: vec![seed.to_string()],
                },
            ),
        ],
        "rolling",
    )
}

fn advance_roll_completed(
    mut state: DiceStoryState,
    config: &DiceStoryConfig,
    payload: &Value,
) -> Result<AdvanceResponse, AutomationsError> {
    let Some(pending) = state.pending_run.as_mut() else {
        return idle_response(state);
    };
    let roll = payload
        .get("roll")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| AutomationsError::Guest("roll result missing".to_string()))?;
    pending.roll = Some(roll);
    let pending_snapshot = pending.clone();
    response_with_state(
        state,
        vec![model_effect(config, &pending_snapshot)],
        "drafting",
    )
}

fn advance_story_completed(
    mut state: DiceStoryState,
    config: &DiceStoryConfig,
    payload: &Value,
) -> Result<AdvanceResponse, AutomationsError> {
    let story = payload
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| AutomationsError::Guest("model result missing".to_string()))?
        .to_string();
    let word_count = count_words(&story);

    let Some(pending) = state.pending_run.as_mut() else {
        return idle_response(state);
    };
    let roll = pending
        .roll
        .ok_or_else(|| AutomationsError::Guest("pending roll missing".to_string()))?;

    if word_count == config.target_words {
        let fired_at = pending.fired_at;
        let seed = pending.seed;
        let attempts = pending.attempt;
        push_recent_run(
            &mut state.recent_runs,
            RunSummary {
                fired_at,
                status: "succeeded".to_string(),
                seed,
                roll: Some(roll),
                attempts,
                word_count: Some(word_count),
                error: None,
            },
        );
        state.pending_run = None;
        return response_with_state(
            state,
            vec![EffectRequest::new(
                format!("notify-{fired_at}"),
                EffectKind::EmitNotification {
                    channel: notification_channel(&config.target),
                    title: format!("Dice Story ({roll})"),
                    body: story,
                    metadata: Value::Null,
                },
            )],
            "succeeded",
        );
    }

    if pending.attempt < config.max_llm_attempts.max(1) {
        pending.attempt += 1;
        pending.previous_word_count = Some(word_count);
        let pending_snapshot = pending.clone();
        return response_with_state(
            state,
            vec![model_effect(config, &pending_snapshot)],
            "retrying",
        );
    }

    push_recent_run(
        &mut state.recent_runs,
        RunSummary {
            fired_at: pending.fired_at,
            status: "failed".to_string(),
            seed: pending.seed,
            roll: Some(roll),
            attempts: pending.attempt,
            word_count: Some(word_count),
            error: Some(format!(
                "story never satisfied {} words",
                config.target_words
            )),
        },
    );
    state.pending_run = None;
    response_with_state(state, Vec::new(), "failed")
}

fn advance_effect_failed(
    mut state: DiceStoryState,
    message: &str,
) -> Result<AdvanceResponse, AutomationsError> {
    if let Some(pending) = state.pending_run.take() {
        push_recent_run(
            &mut state.recent_runs,
            RunSummary {
                fired_at: pending.fired_at,
                status: "failed".to_string(),
                seed: pending.seed,
                roll: pending.roll,
                attempts: pending.attempt,
                word_count: pending.previous_word_count,
                error: Some(message.to_string()),
            },
        );
    }
    response_with_state(state, Vec::new(), "failed")
}

fn schedule_effect(next_fire_at: i64) -> EffectRequest {
    EffectRequest::new(
        format!("schedule-{next_fire_at}"),
        EffectKind::ScheduleWakeup {
            wakeup_id: TICK_WAKEUP_ID.to_string(),
            at: next_fire_at,
        },
    )
}

fn model_effect(config: &DiceStoryConfig, pending: &PendingRun) -> EffectRequest {
    let roll = pending
        .roll
        .expect("roll should be set before requesting model");
    EffectRequest::new(
        format!("model-{}-{}", pending.fired_at, pending.attempt),
        EffectKind::RunModel {
            prompt: build_story_prompt(config.target_words, roll, pending.previous_word_count),
            system: None,
        },
    )
}

fn notification_channel(target: &TelegramTarget) -> String {
    match target.thread_id {
        Some(thread_id) => format!("telegram:{}:{thread_id}", target.chat_id),
        None => format!("telegram:{}:-", target.chat_id),
    }
}

fn parse_notification_channel(channel: &str) -> TelegramPayload {
    let rest = channel.strip_prefix("telegram:").expect("telegram prefix");
    let mut parts = rest.split(':');
    let chat_id = parts
        .next()
        .expect("chat id")
        .parse()
        .expect("valid chat id");
    let thread_part = parts.next().expect("thread part");
    let thread_id = if thread_part == "-" {
        None
    } else {
        Some(thread_part.parse().expect("valid thread id"))
    };
    TelegramPayload {
        chat_id,
        thread_id,
        text: String::new(),
    }
}

fn push_recent_run(runs: &mut Vec<RunSummary>, summary: RunSummary) {
    runs.push(summary);
    if runs.len() > 8 {
        let overflow = runs.len() - 8;
        runs.drain(0..overflow);
    }
}

#[derive(Debug, Clone, Default)]
struct QueuedStoryModel {
    prompts: Arc<Mutex<Vec<String>>>,
    responses: Arc<Mutex<VecDeque<String>>>,
}

impl QueuedStoryModel {
    async fn push_response(&self, response: impl Into<String>) {
        self.responses.lock().await.push_back(response.into());
    }

    async fn prompts(&self) -> Vec<String> {
        self.prompts.lock().await.clone()
    }

    async fn complete(&self, prompt: String) -> std::result::Result<String, String> {
        self.prompts.lock().await.push(prompt);
        self.responses
            .lock()
            .await
            .pop_front()
            .ok_or_else(|| "no queued response available".to_string())
    }
}

#[derive(Debug, Clone, Default)]
struct DiceStoryEffectHandler {
    model: QueuedStoryModel,
    notifications: Arc<Mutex<Vec<TelegramPayload>>>,
}

impl DiceStoryEffectHandler {
    async fn notifications(&self) -> Vec<TelegramPayload> {
        self.notifications.lock().await.clone()
    }
}

#[async_trait]
impl EffectHandler for DiceStoryEffectHandler {
    async fn handle_effect(
        &self,
        _automation_id: &str,
        _revision_id: u64,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        match &effect.kind {
            EffectKind::RunCommand { args, .. } => {
                let seed = args
                    .first()
                    .and_then(|seed| seed.parse::<u64>().ok())
                    .ok_or_else(|| AutomationsError::Io("missing command seed".to_string()))?;
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![AutomationEvent::EffectCompleted {
                        effect_id: effect.effect_id.clone(),
                        result: EffectResult::Ok(json!({ "roll": deterministic_roll(seed, 6) })),
                        at: now,
                    }],
                })
            }
            EffectKind::RunModel { prompt, .. } => {
                let story = self
                    .model
                    .complete(prompt.clone())
                    .await
                    .map_err(AutomationsError::Io)?;
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![AutomationEvent::EffectCompleted {
                        effect_id: effect.effect_id.clone(),
                        result: EffectResult::Ok(json!({ "text": story })),
                        at: now,
                    }],
                })
            }
            EffectKind::EmitNotification { channel, body, .. } => {
                let mut payload = parse_notification_channel(channel);
                payload.text.clone_from(body);
                self.notifications.lock().await.push(payload);
                Ok(EffectHandlerOutcome::default())
            }
            other => Err(AutomationsError::Io(format!(
                "unexpected effect {other:?} in dice story test"
            ))),
        }
    }
}

#[derive(Debug)]
struct DiceStoryWorld {
    _tempdir: TempDir,
    universe: PocketUniverse<DiceStoryRuntime, DiceStoryEffectHandler>,
    handler: DiceStoryEffectHandler,
    next_automation_number: u64,
    artifact_path: std::path::PathBuf,
}

impl DiceStoryWorld {
    fn new(initial_timestamp: i64) -> Result<Self, AutomationsError> {
        let tempdir =
            tempfile::tempdir().map_err(|error| AutomationsError::Io(error.to_string()))?;
        let artifact_path = tempdir.path().join("fixture.wasm");
        std::fs::write(&artifact_path, b"fixture")
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        let handler = DiceStoryEffectHandler::default();
        let universe = PocketUniverse::new(initial_timestamp, DiceStoryRuntime, handler.clone());
        Ok(Self {
            _tempdir: tempdir,
            universe,
            handler,
            next_automation_number: 1,
            artifact_path,
        })
    }

    async fn queue_llm_response(&self, response: impl Into<String>) {
        self.handler.model.push_response(response).await;
    }

    async fn prompts(&self) -> Vec<String> {
        self.handler.model.prompts().await
    }

    async fn notifications(&self) -> Vec<TelegramPayload> {
        self.handler.notifications().await
    }

    async fn install_automation(
        &mut self,
        target: TelegramTarget,
        period_seconds: u64,
        target_words: usize,
        max_llm_attempts: u8,
    ) -> Result<u64, AutomationsError> {
        let automation_number = self.next_automation_number;
        self.next_automation_number += 1;
        let automation_id = automation_id_for_number(automation_number);
        let revision = self.universe.register_revision(&RegistrationRequest {
            automation_id: automation_id.clone(),
            artifact_path: self.artifact_path.clone(),
            config: serde_json::to_value(DiceStoryConfig {
                target,
                period_seconds,
                target_words,
                max_llm_attempts,
            })
            .map_err(|error| AutomationsError::Io(error.to_string()))?,
        })?;
        self.universe
            .activate_revision(
                &automation_id,
                revision.revision_id,
                ActivationMode::ColdStart,
            )
            .await?;
        Ok(automation_number)
    }

    async fn retarget_automation(
        &mut self,
        automation_number: u64,
        target: TelegramTarget,
    ) -> Result<(), AutomationsError> {
        let automation_id = automation_id_for_number(automation_number);
        let automations = self.universe.automations()?;
        let automation = automations
            .get(&automation_id)
            .ok_or_else(|| AutomationsError::AutomationNotFound(automation_id.clone()))?;
        let revision = automation
            .active_revision_id
            .and_then(|revision_id| automation.revisions.get(&revision_id))
            .or_else(|| automation.revisions.values().next_back())
            .ok_or_else(|| AutomationsError::RevisionNotFound {
                automation_id: automation_id.clone(),
                revision_id: 0,
            })?;
        let mut config: DiceStoryConfig = serde_json::from_value(revision.config.clone())
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        config.target = target;

        let next_revision = self.universe.register_revision(&RegistrationRequest {
            automation_id: automation_id.clone(),
            artifact_path: self.artifact_path.clone(),
            config: serde_json::to_value(config)
                .map_err(|error| AutomationsError::Io(error.to_string()))?,
        })?;
        self.universe
            .activate_revision(
                &automation_id,
                next_revision.revision_id,
                ActivationMode::PreserveState,
            )
            .await
    }

    fn advance_by(&self, seconds: i64) {
        self.universe.clock().advance_by(seconds);
    }

    async fn reconcile_due(&self) -> Result<usize, AutomationsError> {
        self.universe.reconcile_due().await
    }

    fn automation(&self, automation_number: u64) -> Result<ManagedAutomation, AutomationsError> {
        let automation_id = automation_id_for_number(automation_number);
        self.universe
            .automations()?
            .remove(&automation_id)
            .ok_or(AutomationsError::AutomationNotFound(automation_id))
    }
}

fn automation_id_for_number(automation_number: u64) -> String {
    format!("automation-{automation_number}")
}

#[async_trait]
impl AutomationsScenarioWorld for DiceStoryWorld {
    async fn traces(&mut self) -> Result<Vec<TraceRecord>, AutomationsError> {
        self.universe.traces()
    }
}

#[tokio::test]
async fn dice_story_automation_retries_until_the_story_passes_validation()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "dice story automation retries invalid llm output",
        DiceStoryWorld::new(START_TIME).expect("world should initialize"),
    )
    .with_recent_trace_limit(12)
    .with_trace_summary(summarize_trace);

    scenario
        .when("the llm is primed with one invalid story and one valid story")
        .perform(|world| {
            Box::pin(async move {
                world
                    .queue_llm_response(repeated_word_story("almost", 49))
                    .await;
                world
                    .queue_llm_response(repeated_word_story("victory", 50))
                    .await;
                Ok(())
            })
        })
        .await?;

    scenario
        .when("a dice story automation becomes due")
        .perform(|world| {
            Box::pin(async move {
                world
                    .install_automation(
                        TelegramTarget {
                            chat_id: 42,
                            thread_id: Some(7),
                        },
                        60,
                        50,
                        3,
                    )
                    .await?;
                world.advance_by(60);
                world.reconcile_due().await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("a notification is eventually delivered after the retry")
        .expect_eventually(
            "a handled notification effect",
            std::time::Duration::from_millis(50),
            |trace| {
                matches!(
                    trace.event,
                    TraceEvent::EffectHandled { ref effect_id, .. } if effect_id.starts_with("notify-")
                )
            },
        )
        .await?;

    let notifications = scenario.world().notifications().await;
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].chat_id, 42);
    assert_eq!(notifications[0].thread_id, Some(7));
    assert_eq!(count_words(&notifications[0].text), 50);

    let prompts = scenario.world().prompts().await;
    assert_eq!(prompts.len(), 2);
    assert!(prompts[1].contains("Your previous answer had 49 words."));

    let automation = scenario
        .world()
        .automation(1)
        .expect("automation should exist");
    let state: DiceStoryState = serde_json::from_value(
        automation
            .current_state
            .expect("automation state should be present"),
    )
    .expect("state should decode");
    assert_eq!(state.recent_runs.len(), 1);
    assert_eq!(state.recent_runs[0].status, "succeeded");
    assert_eq!(state.recent_runs[0].attempts, 2);

    Ok(())
}

#[tokio::test]
async fn reconciling_the_same_due_window_twice_does_not_duplicate_the_notification()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "reconcile is idempotent for the same due window",
        DiceStoryWorld::new(START_TIME).expect("world should initialize"),
    )
    .with_trace_summary(summarize_trace);

    scenario
        .when("a valid story is queued and the same wakeup window is reconciled twice")
        .perform(|world| {
            Box::pin(async move {
                world
                    .queue_llm_response(repeated_word_story("steady", 50))
                    .await;
                world
                    .install_automation(
                        TelegramTarget {
                            chat_id: 100,
                            thread_id: None,
                        },
                        60,
                        50,
                        2,
                    )
                    .await?;
                world.advance_by(60);
                world.reconcile_due().await?;
                world.reconcile_due().await?;
                Ok(())
            })
        })
        .await?;

    assert_eq!(scenario.world().notifications().await.len(), 1);

    let traces = scenario.world().traces().await.expect("traces should load");
    let dispatched = traces
        .into_iter()
        .filter(|trace| {
            matches!(
                trace.event,
                TraceEvent::WakeupDispatched { ref wakeup_id, .. } if wakeup_id == TICK_WAKEUP_ID
            )
        })
        .count();
    assert_eq!(dispatched, 1);

    Ok(())
}

#[tokio::test]
async fn exhausting_validation_retries_marks_the_run_failed_without_notification()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "validation exhaustion fails the run without notification",
        DiceStoryWorld::new(START_TIME).expect("world should initialize"),
    )
    .with_recent_trace_limit(12)
    .with_trace_summary(summarize_trace);

    scenario
        .when("only invalid stories are queued")
        .perform(|world| {
            Box::pin(async move {
                world
                    .queue_llm_response(repeated_word_story("short", 10))
                    .await;
                world
                    .queue_llm_response(repeated_word_story("still", 12))
                    .await;
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the automation becomes due")
        .perform(|world| {
            Box::pin(async move {
                world
                    .install_automation(
                        TelegramTarget {
                            chat_id: 9,
                            thread_id: None,
                        },
                        60,
                        50,
                        2,
                    )
                    .await?;
                world.advance_by(60);
                world.reconcile_due().await?;
                Ok(())
            })
        })
        .await?;

    let notifications = scenario.world().notifications().await;
    assert!(notifications.is_empty());

    let automation = scenario
        .world()
        .automation(1)
        .expect("automation should exist");
    let state: DiceStoryState = serde_json::from_value(
        automation
            .current_state
            .expect("automation state should be present"),
    )
    .expect("state should decode");
    assert_eq!(state.recent_runs.len(), 1);
    assert_eq!(state.recent_runs[0].status, "failed");

    Ok(())
}

#[tokio::test]
async fn activating_a_new_revision_with_preserved_state_retargets_future_notifications()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "config revisions preserve state while redirecting future notifications",
        DiceStoryWorld::new(START_TIME).expect("world should initialize"),
    )
    .with_recent_trace_limit(12)
    .with_trace_summary(summarize_trace);

    scenario
        .when("a first successful run completes on the original target")
        .perform(|world| {
            Box::pin(async move {
                world
                    .queue_llm_response(repeated_word_story("steady", 50))
                    .await;
                world
                    .install_automation(
                        TelegramTarget {
                            chat_id: 1,
                            thread_id: Some(10),
                        },
                        60,
                        50,
                        2,
                    )
                    .await?;
                world.advance_by(60);
                world.reconcile_due().await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .when("a preserved-state revision retargets the automation before the next run")
        .perform(|world| {
            Box::pin(async move {
                world
                    .queue_llm_response(repeated_word_story("steady", 50))
                    .await;
                world
                    .retarget_automation(
                        1,
                        TelegramTarget {
                            chat_id: 99,
                            thread_id: Some(20),
                        },
                    )
                    .await?;
                world.advance_by(60);
                world.reconcile_due().await?;
                Ok(())
            })
        })
        .await?;

    let notifications = scenario.world().notifications().await;
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].chat_id, 1);
    assert_eq!(notifications[0].thread_id, Some(10));
    assert_eq!(notifications[1].chat_id, 99);
    assert_eq!(notifications[1].thread_id, Some(20));

    let automation = scenario
        .world()
        .automation(1)
        .expect("automation should exist");
    let state: DiceStoryState = serde_json::from_value(
        automation
            .current_state
            .expect("automation state should be present"),
    )
    .expect("state should decode");
    assert_eq!(state.recent_runs.len(), 2);

    Ok(())
}
