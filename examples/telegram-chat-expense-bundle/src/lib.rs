use mango_automation_guest_sdk::{
    Automation, AutomationDescriptor, AutomationEvent, Capability, Decision, EffectKind,
    EffectResult, GuestContext, effect,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TEXT_TRIGGER: &str = "telegram.text_received";
const PHOTO_TRIGGER: &str = "telegram.photo_received";
const STORE_TOOL_SLUG: &str = "expense.markdown_store";
const RECEIPT_EXTRACTOR_CAPABILITY: &str = "expense.receipt_extractor";
const ROUTER_CAPABILITY: &str = "expense.router";
const LOAD_CONTEXT_EFFECT_ID: &str = "expense-store.list";
const EXTRACT_RECEIPT_EFFECT_ID: &str = "expense-receipt.extract";
const ROUTE_EFFECT_ID: &str = "expense-router.route";
const MUTATE_EFFECT_ID: &str = "expense-store.mutate";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExpenseAutomationState {
    pub pending_trigger: Option<TriggerInput>,
    pub pending_route: Option<PendingRouteContext>,
    pub pending_clarification: Option<PendingClarification>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingRouteContext {
    pub trigger: TriggerInput,
    pub expenses: Vec<StoredExpense>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerInput {
    Text(TextTrigger),
    Photo(PhotoTrigger),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextTrigger {
    pub text: String,
    pub username: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhotoTrigger {
    pub local_path: String,
    pub caption: Option<String>,
    pub username: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReceiptExtractionRequest {
    pub local_path: String,
    pub caption: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReceiptExtraction {
    pub local_path: String,
    pub caption: Option<String>,
    pub ocr_text: String,
    pub looks_like_expense: bool,
    pub merchant: Option<String>,
    pub amount: Option<String>,
    pub currency: Option<String>,
    pub spent_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteTrigger {
    Text(TextTrigger),
    Photo {
        photo: PhotoTrigger,
        extraction: ReceiptExtraction,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpenseDraft {
    pub merchant: Option<String>,
    pub amount: Option<String>,
    pub currency: Option<String>,
    pub spent_at: Option<String>,
    pub notes: Option<String>,
    pub source_image: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExpenseChanges {
    pub merchant: Option<String>,
    pub amount: Option<String>,
    pub currency: Option<String>,
    pub spent_at: Option<String>,
    pub notes: Option<String>,
    pub source_image: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredExpense {
    pub id: String,
    pub status: ExpenseStatus,
    pub merchant: String,
    pub amount: String,
    pub currency: String,
    pub spent_at: Option<String>,
    pub notes: Option<String>,
    pub source_image: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpenseStatus {
    Active,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpenseCandidate {
    pub id: String,
    pub merchant: String,
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingClarification {
    pub question: String,
    pub context: ClarificationContext,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClarificationContext {
    CompleteDraft {
        draft: ExpenseDraft,
        missing_fields: Vec<String>,
    },
    ChooseExpense {
        candidates: Vec<ExpenseCandidate>,
        intent: ClarifiedIntent,
    },
    ResolvePhotoUpdate {
        existing: ExpenseCandidate,
        draft: ExpenseDraft,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClarifiedIntent {
    Update { changes: ExpenseChanges },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteRequest {
    pub trigger: RouteTrigger,
    pub expenses: Vec<StoredExpense>,
    pub pending_clarification: Option<PendingClarification>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteDecision {
    Unhandled,
    Clarify {
        clarification: PendingClarification,
    },
    Reply {
        message: String,
        clear_clarification: bool,
    },
    Create {
        draft: ExpenseDraft,
    },
    Update {
        expense_id: String,
        changes: ExpenseChanges,
        clear_clarification: bool,
    },
    Delete {
        expense_id: String,
        clear_clarification: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExpenseStoreRequest {
    ListActive,
    Create {
        draft: ExpenseDraft,
    },
    Update {
        expense_id: String,
        changes: ExpenseChanges,
    },
    SoftDelete {
        expense_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExpenseStoreResponse {
    Expenses {
        expenses: Vec<StoredExpense>,
    },
    Mutated {
        expense: Box<StoredExpense>,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExpenseAutomation;

impl Automation for ExpenseAutomation {
    type State = ExpenseAutomationState;

    fn descriptor(&self) -> AutomationDescriptor {
        AutomationDescriptor::new(
            "expense.reports",
            "Maintains a markdown expense ledger from Telegram text and receipt photos.",
            1,
            vec![
                Capability::EmitNotifications,
                Capability::CallTools,
                Capability::RunInference,
            ],
        )
    }

    fn initial_state(&self) -> Self::State {
        Self::State::default()
    }

    fn reduce(
        &self,
        state: Self::State,
        event: AutomationEvent,
        _ctx: GuestContext,
    ) -> Result<Decision<Self::State>, String> {
        match event {
            AutomationEvent::Activated { .. } => Ok(Decision::new(state).with_status("ready")),
            AutomationEvent::TriggerFired {
                trigger, payload, ..
            } => handle_trigger_event(state, &trigger, payload),
            AutomationEvent::EffectCompleted {
                effect_id, result, ..
            } if effect_id == LOAD_CONTEXT_EFFECT_ID => handle_context_loaded(state, result),
            AutomationEvent::EffectCompleted {
                effect_id, result, ..
            } if effect_id == EXTRACT_RECEIPT_EFFECT_ID => handle_receipt_extracted(state, result),
            AutomationEvent::EffectCompleted {
                effect_id, result, ..
            } if effect_id == ROUTE_EFFECT_ID => handle_route_decision(state, result),
            AutomationEvent::EffectCompleted {
                effect_id, result, ..
            } if effect_id == MUTATE_EFFECT_ID => handle_mutation_completed(state, result),
            _ => Ok(Decision::new(state).with_status("idle")),
        }
    }
}

fn handle_trigger_event(
    mut state: ExpenseAutomationState,
    trigger_name: &str,
    payload: Value,
) -> Result<Decision<ExpenseAutomationState>, String> {
    let Some(trigger) = decode_trigger(trigger_name, payload)? else {
        return Ok(Decision::new(state).unhandled().with_status("fallthrough"));
    };

    state.pending_trigger = Some(trigger);
    Ok(Decision::new(state)
        .with_effect(effect(
            LOAD_CONTEXT_EFFECT_ID,
            EffectKind::CallTool {
                slug: STORE_TOOL_SLUG.to_string(),
                input: serde_json::to_value(ExpenseStoreRequest::ListActive)
                    .map_err(|error| error.to_string())?,
            },
        ))
        .with_status("loading_context")
        .unhandled())
}

fn handle_context_loaded(
    mut state: ExpenseAutomationState,
    result: EffectResult,
) -> Result<Decision<ExpenseAutomationState>, String> {
    let expenses = decode_store_expenses(result)?;
    let Some(trigger) = state.pending_trigger.clone() else {
        return Ok(Decision::new(state).with_status("idle"));
    };

    match trigger {
        TriggerInput::Text(text) => route_with_context(state, RouteTrigger::Text(text), expenses),
        TriggerInput::Photo(photo) => {
            state.pending_route = Some(PendingRouteContext {
                trigger: TriggerInput::Photo(photo.clone()),
                expenses,
            });
            Ok(Decision::new(state)
                .with_effect(effect(
                    EXTRACT_RECEIPT_EFFECT_ID,
                    EffectKind::RunInference {
                        capability: RECEIPT_EXTRACTOR_CAPABILITY.to_string(),
                        input: serde_json::to_value(ReceiptExtractionRequest {
                            local_path: photo.local_path,
                            caption: photo.caption,
                        })
                        .map_err(|error| error.to_string())?,
                    },
                ))
                .with_status("extracting_receipt")
                .unhandled())
        }
    }
}

fn handle_receipt_extracted(
    mut state: ExpenseAutomationState,
    result: EffectResult,
) -> Result<Decision<ExpenseAutomationState>, String> {
    let extraction = decode_receipt_extraction(result)?;
    let Some(PendingRouteContext {
        trigger: TriggerInput::Photo(photo),
        expenses,
    }) = state.pending_route.take()
    else {
        return Ok(Decision::new(state).with_status("idle"));
    };

    route_with_context(state, RouteTrigger::Photo { photo, extraction }, expenses)
}

fn route_with_context(
    state: ExpenseAutomationState,
    trigger: RouteTrigger,
    expenses: Vec<StoredExpense>,
) -> Result<Decision<ExpenseAutomationState>, String> {
    let pending_clarification = state.pending_clarification.clone();
    Ok(Decision::new(state)
        .with_effect(effect(
            ROUTE_EFFECT_ID,
            EffectKind::RunInference {
                capability: ROUTER_CAPABILITY.to_string(),
                input: serde_json::to_value(RouteRequest {
                    trigger,
                    expenses,
                    pending_clarification,
                })
                .map_err(|error| error.to_string())?,
            },
        ))
        .with_status("routing")
        .unhandled())
}

fn handle_route_decision(
    mut state: ExpenseAutomationState,
    result: EffectResult,
) -> Result<Decision<ExpenseAutomationState>, String> {
    state.pending_trigger = None;
    state.pending_route = None;
    match decode_route_decision(result)? {
        RouteDecision::Unhandled => Ok(Decision::new(state).unhandled().with_status("fallthrough")),
        RouteDecision::Clarify { clarification } => {
            let question = clarification.question.clone();
            state.pending_clarification = Some(clarification);
            Ok(notify(Decision::new(state).handled(), question)
                .with_status("awaiting_clarification"))
        }
        RouteDecision::Reply {
            message,
            clear_clarification,
        } => {
            if clear_clarification {
                state.pending_clarification = None;
            }
            Ok(notify(Decision::new(state).handled(), message).with_status("replied"))
        }
        RouteDecision::Create { draft } => {
            state.pending_clarification = None;
            mutate_store(state, ExpenseStoreRequest::Create { draft }, "creating")
        }
        RouteDecision::Update {
            expense_id,
            changes,
            clear_clarification,
        } => {
            if clear_clarification {
                state.pending_clarification = None;
            }
            mutate_store(
                state,
                ExpenseStoreRequest::Update {
                    expense_id,
                    changes,
                },
                "updating",
            )
        }
        RouteDecision::Delete {
            expense_id,
            clear_clarification,
        } => {
            if clear_clarification {
                state.pending_clarification = None;
            }
            mutate_store(
                state,
                ExpenseStoreRequest::SoftDelete { expense_id },
                "deleting",
            )
        }
    }
}

fn mutate_store(
    state: ExpenseAutomationState,
    request: ExpenseStoreRequest,
    status: &'static str,
) -> Result<Decision<ExpenseAutomationState>, String> {
    Ok(Decision::new(state)
        .with_effect(effect(
            MUTATE_EFFECT_ID,
            EffectKind::CallTool {
                slug: STORE_TOOL_SLUG.to_string(),
                input: serde_json::to_value(request).map_err(|error| error.to_string())?,
            },
        ))
        .with_status(status)
        .handled())
}

fn handle_mutation_completed(
    state: ExpenseAutomationState,
    result: EffectResult,
) -> Result<Decision<ExpenseAutomationState>, String> {
    let (message, expense) = decode_mutation_result(result)?;
    let _ = expense;
    Ok(notify(Decision::new(state).handled(), message).with_status("mutated"))
}

fn decode_trigger(trigger: &str, payload: Value) -> Result<Option<TriggerInput>, String> {
    match trigger {
        TEXT_TRIGGER => serde_json::from_value(payload)
            .map(TriggerInput::Text)
            .map(Some)
            .map_err(|error| error.to_string()),
        PHOTO_TRIGGER => serde_json::from_value(payload)
            .map(TriggerInput::Photo)
            .map(Some)
            .map_err(|error| error.to_string()),
        _ => Ok(None),
    }
}

fn decode_store_expenses(result: EffectResult) -> Result<Vec<StoredExpense>, String> {
    match result {
        EffectResult::Ok(value) => match serde_json::from_value::<ExpenseStoreResponse>(value)
            .map_err(|error| error.to_string())?
        {
            ExpenseStoreResponse::Expenses { expenses } => Ok(expenses),
            other @ ExpenseStoreResponse::Mutated { .. } => {
                Err(format!("expected expense context, got {other:?}"))
            }
        },
        EffectResult::Err(message) => Err(message),
    }
}

fn decode_receipt_extraction(result: EffectResult) -> Result<ReceiptExtraction, String> {
    match result {
        EffectResult::Ok(value) => serde_json::from_value(value).map_err(|error| error.to_string()),
        EffectResult::Err(message) => Err(message),
    }
}

fn decode_route_decision(result: EffectResult) -> Result<RouteDecision, String> {
    match result {
        EffectResult::Ok(value) => serde_json::from_value(value).map_err(|error| error.to_string()),
        EffectResult::Err(message) => Err(message),
    }
}

fn decode_mutation_result(result: EffectResult) -> Result<(String, Box<StoredExpense>), String> {
    match result {
        EffectResult::Ok(value) => match serde_json::from_value::<ExpenseStoreResponse>(value)
            .map_err(|error| error.to_string())?
        {
            ExpenseStoreResponse::Mutated { expense, message } => Ok((message, expense)),
            other @ ExpenseStoreResponse::Expenses { .. } => {
                Err(format!("expected mutation result, got {other:?}"))
            }
        },
        EffectResult::Err(message) => Err(message),
    }
}

fn notify(
    mut decision: Decision<ExpenseAutomationState>,
    body: String,
) -> Decision<ExpenseAutomationState> {
    decision.effects.push(effect(
        format!("notify-{}", decision.effects.len() + 1),
        EffectKind::EmitNotification {
            channel: "telegram.chat".to_string(),
            title: "Expense reports".to_string(),
            body,
            metadata: json!(null),
        },
    ));
    decision
}

#[cfg(target_arch = "wasm32")]
mango_automation_guest_sdk::export_automation!(ExpenseAutomation);
