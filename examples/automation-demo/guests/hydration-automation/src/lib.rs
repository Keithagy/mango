use mango_automation_sdk as sdk;
use sdk::{
    Automation, AutomationDescriptor, AutomationEvent, Capability, Decision, EffectKind,
    export_automation,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReminderState {
    cycle: u64,
    pending_confirmation: bool,
    next_hourly_at: i64,
}

#[derive(Debug, Clone, Copy)]
struct HydrationReminder;

impl HydrationReminder {
    fn next_hour_boundary(after: i64) -> i64 {
        ((after / 3600) + 1) * 3600
    }
}

impl Automation for HydrationReminder {
    type State = ReminderState;

    fn descriptor(&self) -> AutomationDescriptor {
        AutomationDescriptor::new(
            "demo.hydration_reminder",
            "Remind the user every hour and keep pinging until a confirmation signal arrives.",
            1,
            vec![Capability::ScheduleWakeups, Capability::EmitNotifications],
        )
    }

    fn initial_state(&self) -> Self::State {
        ReminderState {
            cycle: 0,
            pending_confirmation: false,
            next_hourly_at: 0,
        }
    }

    fn reduce(
        &self,
        mut state: Self::State,
        event: AutomationEvent,
        _ctx: sdk::GuestContext,
    ) -> Result<Decision<Self::State>, String> {
        match event {
            AutomationEvent::Activated { at } => {
                state.next_hourly_at = Self::next_hour_boundary(at);
                let next_hourly_at = state.next_hourly_at;
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        "activate-hourly",
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "hourly".to_string(),
                            at: next_hourly_at,
                        },
                    ))
                    .with_status("armed"))
            }
            AutomationEvent::WakeupFired { wakeup_id, at } if wakeup_id == "hourly" => {
                state.cycle += 1;
                state.pending_confirmation = true;
                state.next_hourly_at = Self::next_hour_boundary(at);
                let cycle = state.cycle;
                let next_hourly_at = state.next_hourly_at;

                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        format!("notify-hourly-{cycle}"),
                        EffectKind::EmitNotification {
                            channel: "demo".to_string(),
                            title: format!("Hydration Cycle {cycle}"),
                            body: "Drink a glass of water, then confirm with `confirm_water`."
                                .to_string(),
                            metadata: serde_json::Value::Null,
                        },
                    ))
                    .with_effect(sdk::effect(
                        format!("schedule-ping-{cycle}"),
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "ping".to_string(),
                            at: at + 60,
                        },
                    ))
                    .with_effect(sdk::effect(
                        format!("schedule-hourly-{cycle}"),
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "hourly".to_string(),
                            at: next_hourly_at,
                        },
                    ))
                    .with_status("awaiting confirmation"))
            }
            AutomationEvent::WakeupFired { wakeup_id, at } if wakeup_id == "ping" => {
                if !state.pending_confirmation {
                    return Ok(Decision::new(state).with_status("idle"));
                }
                let cycle = state.cycle;

                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        format!("notify-ping-{cycle}-{at}"),
                        EffectKind::EmitNotification {
                            channel: "demo".to_string(),
                            title: format!("Hydration Cycle {cycle}"),
                            body: "Still waiting for confirmation. Reply with `confirm_water` once you have finished the glass.".to_string(),
                            metadata: serde_json::Value::Null,
                        },
                    ))
                    .with_effect(sdk::effect(
                        format!("schedule-next-ping-{cycle}-{at}"),
                        EffectKind::ScheduleWakeup {
                            wakeup_id: "ping".to_string(),
                            at: at + 60,
                        },
                    ))
                    .with_status("still waiting"))
            }
            AutomationEvent::UserSignal { signal, .. } if signal == "confirm_water" => {
                state.pending_confirmation = false;
                let cycle = state.cycle;
                Ok(Decision::new(state)
                    .with_effect(sdk::effect(
                        format!("cancel-ping-{cycle}"),
                        EffectKind::CancelWakeup {
                            wakeup_id: "ping".to_string(),
                        },
                    ))
                    .with_effect(sdk::effect(
                        format!("confirm-notify-{cycle}"),
                        EffectKind::EmitNotification {
                            channel: "demo".to_string(),
                            title: "Hydration Logged".to_string(),
                            body: "Confirmation received. The hourly reminder remains active."
                                .to_string(),
                            metadata: serde_json::Value::Null,
                        },
                    ))
                    .with_status("confirmed"))
            }
            _ => Ok(Decision::new(state).with_status("noop")),
        }
    }
}

export_automation!(HydrationReminder);
