use mango_automation_sdk as sdk;
use sdk::{
    Automation, AutomationDescriptor, AutomationEvent, Capability, Decision, EffectKind,
    EffectResult, export_automation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DigestState {
    next_digest_at: i64,
}

#[derive(Debug, Clone, Copy)]
struct NewsDigestAutomation;

impl NewsDigestAutomation {
    fn next_daily_digest(after: i64) -> i64 {
        let day = 24 * 60 * 60;
        let current_day = after.div_euclid(day);
        (current_day + 1) * day + (9 * 60 * 60)
    }
}

impl Automation for NewsDigestAutomation {
    type State = DigestState;

    fn descriptor(&self) -> AutomationDescriptor {
        AutomationDescriptor::new(
            "demo.news_digest",
            "Read a curated source list, fetch a source, and draft a morning digest.",
            1,
            vec![
                Capability::ScheduleWakeups,
                Capability::ReadProfile,
                Capability::FetchHttp,
                Capability::RunModel,
                Capability::EmitNotifications,
            ],
        )
    }

    fn initial_state(&self) -> Self::State {
        DigestState { next_digest_at: 0 }
    }

    fn reduce(
        &self,
        mut state: Self::State,
        event: AutomationEvent,
        _ctx: sdk::GuestContext,
    ) -> Result<Decision<Self::State>, String> {
        match event {
            AutomationEvent::Activated { at } => {
                state.next_digest_at = Self::next_daily_digest(at);
                let next_digest_at = state.next_digest_at;
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "schedule-digest",
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "digest".to_string(),
                            at: next_digest_at,
                        },
                    ))
                    .with_status("scheduled"))
            }
            AutomationEvent::WakeupFired { wakeup_id, .. } if wakeup_id == "digest" => {
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "load-sources",
                        EffectKind::ReadProfile {
                            keys: vec!["favorite_news_sources".to_string()],
                        },
                    ))
                    .with_status("loading sources"))
            }
            AutomationEvent::EffectCompleted {
                effect_id,
                result: EffectResult::Ok(payload),
                ..
            } if effect_id == "load-sources" => {
                let sources = payload
                    .get("favorite_news_sources")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let selected_url = sources
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or("https://example.com")
                    .to_string();

                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "fetch-source",
                        EffectKind::FetchHttp { url: selected_url },
                    ))
                    .with_status("fetching source"))
            }
            AutomationEvent::EffectCompleted {
                effect_id,
                result: EffectResult::Ok(payload),
                ..
            } if effect_id == "fetch-source" => {
                let source_body = payload
                    .get("body")
                    .and_then(Value::as_str)
                    .unwrap_or("No source content was returned.");
                let prompt = format!(
                    "Summarize the following source into a concise daily digest with three bullets:\n\n{}",
                    source_body
                );
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "draft-digest",
                        EffectKind::RunModel {
                            prompt,
                            system: Some(
                                "You are a concise newsroom assistant. Return plain text only."
                                    .to_string(),
                            ),
                        },
                    ))
                    .with_status("drafting digest"))
            }
            AutomationEvent::EffectCompleted {
                effect_id,
                result: EffectResult::Ok(payload),
                at,
            } if effect_id == "draft-digest" => {
                let digest_text = payload
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("No digest text was returned.")
                    .to_string();
                state.next_digest_at = Self::next_daily_digest(at);
                let next_digest_at = state.next_digest_at;
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "deliver-digest",
                        EffectKind::EmitNotification {
                            channel: "demo".to_string(),
                            title: "Daily Digest".to_string(),
                            body: digest_text,
                            metadata: Value::Null,
                        },
                    ))
                    .with_effect(sdk::effect(
                        "schedule-next-digest",
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "digest".to_string(),
                            at: next_digest_at,
                        },
                    ))
                    .with_status("delivered"))
            }
            AutomationEvent::EffectCompleted {
                result: EffectResult::Err(message),
                ..
            } => Ok(Decision::new(state).with_status(format!("effect failed: {message}"))),
            _ => Ok(Decision::new(state).with_status("idle")),
        }
    }
}

export_automation!(NewsDigestAutomation);
