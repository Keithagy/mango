use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use mango_automation_sdk as sdk;
use sdk::{
    Automation, AutomationDescriptor, AutomationEvent, Capability, Decision, EffectKind,
    EffectResult, export_automation,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TELEGRAM_CHANNEL: &str = "telegram";
const STORY_SYSTEM_PROMPT: &str = "You are the story-writing backend for a Mango Telegram automations example. Return plain text only, no title, no markdown, and obey the exact requested word count.";
const SCHEDULED_WAKEUP_ID: &str = "scheduled";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TelegramTarget {
    chat_id: i64,
    thread_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryAutomationConfig {
    target: TelegramTarget,
    node_executable: String,
    runner_path: String,
    script_path: String,
    period_seconds: u64,
    target_words: usize,
    max_llm_attempts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ActiveRun {
    run_id: u64,
    nominal_fire_at: i64,
    roll: Option<u8>,
    attempt: u8,
    previous_word_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RunSummary {
    run_id: u64,
    nominal_fire_at: i64,
    status: String,
    roll: Option<u8>,
    word_count: Option<usize>,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryState {
    next_run_id: u64,
    next_fire_at: Option<i64>,
    period_seconds_override: Option<u64>,
    active_run: Option<ActiveRun>,
    recent_runs: Vec<RunSummary>,
}

#[derive(Debug, Clone, Copy)]
struct DiceStoryAutomation;

impl Automation for DiceStoryAutomation {
    type State = DiceStoryState;

    fn descriptor(&self) -> AutomationDescriptor {
        AutomationDescriptor::new(
            "demo.telegram_dice_story",
            "Schedule a dice-story automation that runs a deterministic script, retries model output until the word count passes validation, and emits a Telegram notification.",
            1,
            vec![
                Capability::ScheduleWakeups,
                Capability::RunCommand,
                Capability::RunModel,
                Capability::EmitNotifications,
            ],
        )
    }

    fn initial_state(&self) -> Self::State {
        DiceStoryState {
            next_run_id: 0,
            next_fire_at: None,
            period_seconds_override: None,
            active_run: None,
            recent_runs: Vec::new(),
        }
    }

    fn reduce(
        &self,
        mut state: Self::State,
        event: AutomationEvent,
        ctx: sdk::GuestContext,
    ) -> Result<Decision<Self::State>, String> {
        let config = decode_config(&ctx.config)?;

        match event {
            AutomationEvent::Activated { at } => {
                let next_fire_at = add_seconds(at, effective_period_seconds(&state, &config))?;
                state.next_fire_at = Some(next_fire_at);
                Ok(schedule_next(state, next_fire_at, "armed"))
            }
            AutomationEvent::WakeupFired { wakeup_id, at } if wakeup_id == SCHEDULED_WAKEUP_ID => {
                let run_id = state.next_run_id + 1;
                state.next_run_id = run_id;
                let next_fire_at = add_seconds(at, effective_period_seconds(&state, &config))?;
                state.next_fire_at = Some(next_fire_at);
                state.active_run = Some(ActiveRun {
                    run_id,
                    nominal_fire_at: at,
                    roll: None,
                    attempt: 1,
                    previous_word_count: None,
                });

                let seed = deterministic_seed(&ctx.automation_id, at, run_id);
                let command_context = json!({
                    "automation_id": ctx.automation_id,
                    "run_id": run_id,
                    "nominal_fire_at": at,
                    "seed": seed,
                });
                let mut decision = schedule_next(state, next_fire_at, "rolling");
                decision.effects.insert(
                    0,
                    sdk::effect(
                        format!("roll-{run_id}"),
                        EffectKind::RunCommand {
                            program: config.node_executable,
                            args: vec![
                                config.runner_path,
                                config.script_path,
                                command_context.to_string(),
                            ],
                        },
                    ),
                );
                Ok(decision)
            }
            AutomationEvent::EffectCompleted {
                effect_id,
                result,
                ..
            } if effect_id.starts_with("roll-") => handle_roll_completed(state, result, config),
            AutomationEvent::EffectCompleted {
                effect_id,
                result,
                ..
            } if effect_id.starts_with("story-") => {
                handle_story_completed(state, effect_id, result, config)
            }
            AutomationEvent::UserSignal { signal, payload, at } if signal == "set_period" => {
                let seconds = payload
                    .get("seconds")
                    .and_then(Value::as_u64)
                    .filter(|seconds| *seconds > 0)
                    .ok_or_else(|| "set_period requires a positive `seconds` field".to_string())?;
                state.period_seconds_override = Some(seconds);
                let next_fire_at = add_seconds(at, seconds)?;
                state.next_fire_at = Some(next_fire_at);
                Ok(schedule_next(state, next_fire_at, format!("period={seconds}s")))
            }
            _ => Ok(Decision::new(state).with_status("idle")),
        }
    }
}

fn handle_roll_completed(
    mut state: DiceStoryState,
    result: EffectResult,
    config: DiceStoryAutomationConfig,
) -> Result<Decision<DiceStoryState>, String> {
    let mut run = state
        .active_run
        .clone()
        .ok_or_else(|| "received roll completion without an active run".to_string())?;
    match result {
        EffectResult::Ok(payload) => {
            let roll = payload
                .get("roll")
                .and_then(Value::as_u64)
                .and_then(|roll| u8::try_from(roll).ok())
                .ok_or_else(|| "roll command did not return a valid `roll`".to_string())?;
            run.roll = Some(roll);
            let prompt = story_prompt(roll, run.attempt, config.target_words, None);
            state.active_run = Some(run.clone());
            Ok(Decision::new(state)
                .with_effect(sdk::effect(
                    format!("story-{}-{}", run.run_id, run.attempt),
                    EffectKind::RunModel {
                        prompt,
                        system: Some(STORY_SYSTEM_PROMPT.to_string()),
                    },
                ))
                .with_status("drafting"))
        }
        EffectResult::Err(message) => {
            finalize_failed_run(&mut state, run, format!("roll failed: {message}"));
            Ok(Decision::new(state).with_status("failed"))
        }
    }
}

fn handle_story_completed(
    mut state: DiceStoryState,
    _effect_id: String,
    result: EffectResult,
    config: DiceStoryAutomationConfig,
) -> Result<Decision<DiceStoryState>, String> {
    let mut run = state
        .active_run
        .clone()
        .ok_or_else(|| "received story completion without an active run".to_string())?;
    let roll = run
        .roll
        .ok_or_else(|| "story completion arrived before the roll was stored".to_string())?;

    match result {
        EffectResult::Ok(payload) => {
            let story = payload
                .get("text")
                .and_then(Value::as_str)
                .map(normalize_story_text)
                .ok_or_else(|| "model response did not include `text`".to_string())?;
            let word_count = story.split_whitespace().count();

            if word_count == config.target_words {
                state.active_run = None;
                push_recent_run(
                    &mut state,
                    RunSummary {
                        run_id: run.run_id,
                        nominal_fire_at: run.nominal_fire_at,
                        status: "succeeded".to_string(),
                        roll: Some(roll),
                        word_count: Some(word_count),
                        error: None,
                    },
                );
                return Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        format!("notify-{}", run.run_id),
                        EffectKind::EmitNotification {
                            channel: TELEGRAM_CHANNEL.to_string(),
                            title: format!("Dice Story {}", run.run_id),
                            body: story,
                            metadata: json!(config.target),
                        },
                    ))
                    .with_status("succeeded"));
            }

            if run.attempt < config.max_llm_attempts.max(1) {
                run.attempt += 1;
                run.previous_word_count = Some(word_count);
                let prompt = story_prompt(
                    roll,
                    run.attempt,
                    config.target_words,
                    run.previous_word_count,
                );
                state.active_run = Some(run.clone());
                return Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        format!("story-{}-{}", run.run_id, run.attempt),
                        EffectKind::RunModel {
                            prompt,
                            system: Some(STORY_SYSTEM_PROMPT.to_string()),
                        },
                    ))
                    .with_status("retrying"));
            }

            finalize_failed_run(
                &mut state,
                run,
                format!(
                    "story never satisfied {} words; last count was {}",
                    config.target_words, word_count
                ),
            );
            Ok(Decision::new(state).with_status("failed"))
        }
        EffectResult::Err(message) => {
            finalize_failed_run(&mut state, run, format!("model failed: {message}"));
            Ok(Decision::new(state).with_status("failed"))
        }
    }
}

fn finalize_failed_run(state: &mut DiceStoryState, run: ActiveRun, error: String) {
    push_recent_run(
        state,
        RunSummary {
            run_id: run.run_id,
            nominal_fire_at: run.nominal_fire_at,
            status: "failed".to_string(),
            roll: run.roll,
            word_count: run.previous_word_count,
            error: Some(error),
        },
    );
    state.active_run = None;
}

fn schedule_next(
    state: DiceStoryState,
    next_fire_at: i64,
    status: impl Into<String>,
) -> Decision<DiceStoryState> {
    Decision::new(state)
        .with_effect(sdk::effect(
            format!("schedule-{next_fire_at}"),
            EffectKind::ScheduleWakeup {
                wakeup_id: SCHEDULED_WAKEUP_ID.to_string(),
                at: next_fire_at,
            },
        ))
        .with_status(status)
}

fn push_recent_run(state: &mut DiceStoryState, summary: RunSummary) {
    state.recent_runs.insert(0, summary);
    state.recent_runs.truncate(8);
}

fn effective_period_seconds(state: &DiceStoryState, config: &DiceStoryAutomationConfig) -> u64 {
    state
        .period_seconds_override
        .unwrap_or(config.period_seconds)
        .max(1)
}

fn decode_config(value: &Value) -> Result<DiceStoryAutomationConfig, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

fn add_seconds(start: i64, seconds: u64) -> Result<i64, String> {
    let seconds = i64::try_from(seconds).map_err(|error| error.to_string())?;
    start
        .checked_add(seconds)
        .ok_or_else(|| "scheduled wakeup overflowed i64".to_string())
}

fn story_prompt(
    roll: u8,
    attempt: u8,
    target_words: usize,
    previous_word_count: Option<usize>,
) -> String {
    match previous_word_count {
        Some(previous_word_count) => format!(
            "Attempt {attempt}. Your previous answer had {previous_word_count} words. Write exactly {target_words} words telling a short story about the number {roll}. Plain text only."
        ),
        None => format!(
            "Attempt {attempt}. Write exactly {target_words} words telling a short story about the number {roll}. Plain text only."
        ),
    }
}

fn normalize_story_text(story: &str) -> String {
    story.lines().map(str::trim).collect::<Vec<_>>().join(" ").trim().to_string()
}

fn deterministic_seed(automation_id: &str, nominal_fire_at: i64, run_id: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    automation_id.hash(&mut hasher);
    nominal_fire_at.hash(&mut hasher);
    run_id.hash(&mut hasher);
    hasher.finish().max(1)
}

export_automation!(DiceStoryAutomation);
