use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

use mango_automations::{ProviderInvocation, ProviderInvocationResult};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use telegram_chat_expense_bundle::{
    ClarificationContext, ClarifiedIntent, ExpenseCandidate, ExpenseChanges, ExpenseDraft,
    ExpenseStatus, ExpenseStoreRequest, ExpenseStoreResponse, PendingClarification, PhotoTrigger,
    ReceiptExtraction, ReceiptExtractionRequest, RouteDecision, RouteRequest, RouteTrigger,
    StoredExpense, TextTrigger,
};

const STORE_SLUG: &str = "expense.markdown_store";
const RECEIPT_EXTRACTOR_SLUG: &str = "expense.receipt_extractor";
const ROUTER_SLUG: &str = "expense.router";

fn main() {
    let result = run();
    let response = match result {
        Ok(output) => ProviderInvocationResult::Ok { output },
        Err(message) => ProviderInvocationResult::Err { message },
    };

    let rendered = serde_json::to_vec(&response).expect("provider response should encode");
    std::io::stdout()
        .write_all(&rendered)
        .expect("provider response should write");
}

fn run() -> Result<Value, String> {
    let mut stdin = Vec::new();
    std::io::stdin()
        .read_to_end(&mut stdin)
        .map_err(|error| error.to_string())?;
    let invocation: ProviderInvocation =
        serde_json::from_slice(&stdin).map_err(|error| error.to_string())?;

    match invocation.slug.as_str() {
        STORE_SLUG => handle_store(invocation.config, invocation.input),
        RECEIPT_EXTRACTOR_SLUG => handle_receipt_extractor(invocation.config, invocation.input),
        ROUTER_SLUG => handle_router(invocation.config, invocation.input),
        other => Err(format!("unsupported provider slug `{other}`")),
    }
}

#[derive(Debug, Default, Deserialize)]
struct StoreConfig {
    state_root: Option<String>,
    #[serde(default)]
    host: ProviderHostConfig,
}

#[derive(Debug, Default, Deserialize)]
struct RouterConfig {
    ocr_executable: Option<String>,
    #[serde(default)]
    host: ProviderHostConfig,
}

#[derive(Debug, Default, Deserialize)]
struct ProviderHostConfig {
    state_root: Option<String>,
    ocr_executable: Option<String>,
}

fn handle_store(config: Value, input: Value) -> Result<Value, String> {
    let config: StoreConfig = serde_json::from_value(config).unwrap_or_default();
    let request: ExpenseStoreRequest =
        serde_json::from_value(input).map_err(|error| error.to_string())?;
    let directory = expense_directory(
        resolve_state_root(config.state_root.as_deref(), &config.host)
            .as_deref()
            .unwrap_or("./local/state/mango"),
    );
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;

    let response = match request {
        ExpenseStoreRequest::ListActive => ExpenseStoreResponse::Expenses {
            expenses: load_expenses(&directory)?
                .into_iter()
                .filter(|expense| expense.status == ExpenseStatus::Active)
                .collect(),
        },
        ExpenseStoreRequest::Create { draft } => {
            let expense = StoredExpense {
                id: next_expense_id(),
                status: ExpenseStatus::Active,
                merchant: draft
                    .merchant
                    .unwrap_or_else(|| "Unspecified merchant".to_string()),
                amount: draft.amount.unwrap_or_else(|| "0.00".to_string()),
                currency: draft.currency.unwrap_or_else(|| "SGD".to_string()),
                spent_at: draft.spent_at,
                notes: draft.notes,
                source_image: draft.source_image,
                created_at: now_stamp(),
                updated_at: now_stamp(),
            };
            write_expense(&directory, &expense)?;
            ExpenseStoreResponse::Mutated {
                message: format!(
                    "Saved expense {} for {} {} {}.",
                    expense.id, expense.merchant, expense.currency, expense.amount
                ),
                expense: Box::new(expense),
            }
        }
        ExpenseStoreRequest::Update {
            expense_id,
            changes,
        } => {
            let mut expense = read_expense(&directory, &expense_id)?;
            if let Some(merchant) = changes.merchant {
                expense.merchant = merchant;
            }
            if let Some(amount) = changes.amount {
                expense.amount = amount;
            }
            if let Some(currency) = changes.currency {
                expense.currency = currency;
            }
            if let Some(spent_at) = changes.spent_at {
                expense.spent_at = Some(spent_at);
            }
            if let Some(notes) = changes.notes {
                expense.notes = Some(notes);
            }
            if let Some(source_image) = changes.source_image {
                expense.source_image = Some(source_image);
            }
            expense.updated_at = now_stamp();
            write_expense(&directory, &expense)?;
            ExpenseStoreResponse::Mutated {
                message: format!(
                    "Updated expense {} to {} {} {}.",
                    expense.id, expense.merchant, expense.currency, expense.amount
                ),
                expense: Box::new(expense),
            }
        }
        ExpenseStoreRequest::SoftDelete { expense_id } => {
            let mut expense = read_expense(&directory, &expense_id)?;
            expense.status = ExpenseStatus::Deleted;
            expense.updated_at = now_stamp();
            write_expense(&directory, &expense)?;
            ExpenseStoreResponse::Mutated {
                message: format!(
                    "Soft-deleted expense {} for {} {} {}.",
                    expense.id, expense.merchant, expense.currency, expense.amount
                ),
                expense: Box::new(expense),
            }
        }
    };

    serde_json::to_value(response).map_err(|error| error.to_string())
}

fn handle_receipt_extractor(config: Value, input: Value) -> Result<Value, String> {
    let config: RouterConfig = serde_json::from_value(config).unwrap_or_default();
    let request: ReceiptExtractionRequest =
        serde_json::from_value(input).map_err(|error| error.to_string())?;
    let ocr_text = extract_ocr_text(
        &request.local_path,
        resolve_ocr_executable(config.ocr_executable.as_deref(), &config.host)
            .as_deref()
            .unwrap_or("tesseract"),
    );
    let combined = format!(
        "{}\n{}",
        request.caption.clone().unwrap_or_default(),
        ocr_text
    );
    let extraction = ReceiptExtraction {
        local_path: request.local_path,
        caption: request.caption,
        ocr_text: ocr_text.clone(),
        looks_like_expense: looks_like_expense_artifact(&combined),
        merchant: guess_merchant(&ocr_text).or_else(|| guess_merchant(&combined)),
        amount: guess_amount(&combined),
        currency: guess_currency(&combined).or_else(|| Some("SGD".to_string())),
        spent_at: guess_date(&combined),
    };
    serde_json::to_value(extraction).map_err(|error| error.to_string())
}

fn handle_router(_config: Value, input: Value) -> Result<Value, String> {
    let request: RouteRequest = serde_json::from_value(input).map_err(|error| error.to_string())?;

    let decision = match request.trigger.clone() {
        RouteTrigger::Photo { photo, extraction } => {
            route_photo(&photo, &extraction, &request.expenses)
        }
        RouteTrigger::Text(text) => route_text(
            &text,
            &request.expenses,
            request.pending_clarification.as_ref(),
        ),
    };

    serde_json::to_value(decision).map_err(|error| error.to_string())
}

fn route_photo(
    photo: &PhotoTrigger,
    extraction: &ReceiptExtraction,
    expenses: &[StoredExpense],
) -> RouteDecision {
    if !extraction.looks_like_expense {
        return RouteDecision::Unhandled;
    }

    let draft = ExpenseDraft {
        merchant: extraction.merchant.clone(),
        amount: extraction.amount.clone(),
        currency: extraction.currency.clone(),
        spent_at: extraction.spent_at.clone(),
        notes: (!extraction.ocr_text.trim().is_empty()).then_some(extraction.ocr_text.clone()),
        source_image: Some(photo.local_path.clone()),
    };

    if let Some(existing) = expenses.iter().find(|expense| {
        expense.status == ExpenseStatus::Active
            && (expense.source_image.as_deref() == Some(photo.local_path.as_str())
                || (extraction
                    .merchant
                    .as_ref()
                    .is_some_and(|name| expense.merchant.eq_ignore_ascii_case(name))
                    && extraction
                        .amount
                        .as_ref()
                        .is_some_and(|value| expense.amount == *value)))
    }) {
        return RouteDecision::Reply {
            message: format!(
                "That looks like a duplicate of {} ({} {} {}). I left the ledger unchanged.",
                existing.id, existing.merchant, existing.currency, existing.amount
            ),
            clear_clarification: false,
        };
    }

    if missing_draft_fields(&draft).is_empty() && extraction.ocr_text.trim().is_empty() {
        return RouteDecision::Clarify {
            clarification: PendingClarification {
                question:
                    "I only have your caption for that photo. Tell me the merchant and amount before I save it."
                        .to_string(),
                context: ClarificationContext::CompleteDraft {
                    draft: ExpenseDraft {
                        merchant: None,
                        amount: None,
                        currency: draft.currency.clone(),
                        spent_at: draft.spent_at.clone(),
                        notes: draft.notes.clone(),
                        source_image: draft.source_image.clone(),
                    },
                    missing_fields: vec!["the merchant".to_string(), "the amount".to_string()],
                },
            },
        };
    }

    if let Some(existing) = detect_photo_update_candidate(&draft, expenses) {
        return RouteDecision::Clarify {
            clarification: PendingClarification {
                question: format!(
                    "This looks like it might update {} ({} {} {}). Should I update that expense or save this as a new one?",
                    existing.id, existing.merchant, existing.currency, existing.amount
                ),
                context: ClarificationContext::ResolvePhotoUpdate {
                    existing: expense_candidate(existing),
                    draft,
                },
            },
        };
    }

    let missing_fields = missing_draft_fields(&draft);
    if missing_fields.is_empty() {
        RouteDecision::Create { draft }
    } else {
        RouteDecision::Clarify {
            clarification: PendingClarification {
                question: format!(
                    "I think that is an expense, but I still need {} before I save it.",
                    missing_fields.join(" and ")
                ),
                context: ClarificationContext::CompleteDraft {
                    draft,
                    missing_fields,
                },
            },
        }
    }
}

fn route_text(
    text: &TextTrigger,
    expenses: &[StoredExpense],
    pending_clarification: Option<&PendingClarification>,
) -> RouteDecision {
    let lower = text.text.to_ascii_lowercase();

    if let Some(decision) = pending_clarification
        .filter(|clarification| should_resolve_clarification_first(clarification, text))
        .and_then(|clarification| resolve_pending_clarification(clarification, text))
    {
        return decision;
    }

    if let Some(decision) = route_explicit_intent(&lower, &text.text, expenses, false) {
        return decision;
    }

    if let Some(decision) = pending_clarification
        .and_then(|clarification| resolve_pending_clarification(clarification, text))
    {
        return decision;
    }

    RouteDecision::Unhandled
}

fn route_explicit_intent(
    lower: &str,
    text: &str,
    expenses: &[StoredExpense],
    clear_clarification: bool,
) -> Option<RouteDecision> {
    let intent_signals = detect_intents(lower, text, expenses);
    match intent_signals.selection {
        IntentSelection::Mixed => Some(RouteDecision::Reply {
            message: "I heard more than one expense action in that message. Please ask me for one expense action at a time.".to_string(),
            clear_clarification: false,
        }),
        IntentSelection::Single(ExpenseIntent::Read) if intent_signals.has_negated_mutation => {
            Some(route_read(text, expenses))
        }
        IntentSelection::Single(_) | IntentSelection::None if intent_signals.has_negated_mutation => {
            Some(RouteDecision::Reply {
                message:
                    "I won't change the ledger from a negated instruction. Ask me to show, update, or delete explicitly."
                        .to_string(),
                clear_clarification: false,
            })
        }
        IntentSelection::Single(ExpenseIntent::Delete) => {
            Some(route_delete(text, expenses, clear_clarification))
        }
        IntentSelection::Single(ExpenseIntent::Update) => {
            Some(route_update(text, expenses, clear_clarification))
        }
        IntentSelection::Single(ExpenseIntent::Read) => Some(route_read(text, expenses)),
        IntentSelection::None => None,
    }
}

fn resolve_state_root(explicit: Option<&str>, host: &ProviderHostConfig) -> Option<String> {
    explicit
        .map(std::string::ToString::to_string)
        .or_else(|| host.state_root.clone())
}

fn resolve_ocr_executable(explicit: Option<&str>, host: &ProviderHostConfig) -> Option<String> {
    explicit
        .map(std::string::ToString::to_string)
        .or_else(|| host.ocr_executable.clone())
}

fn route_read(text: &str, expenses: &[StoredExpense]) -> RouteDecision {
    let matches = select_candidates(text, expenses);
    let selected = if matches.is_empty() {
        expenses
            .iter()
            .filter(|expense| expense.status == ExpenseStatus::Active)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        matches
    };

    if selected.is_empty() {
        return RouteDecision::Reply {
            message: "You do not have any active expenses yet.".to_string(),
            clear_clarification: false,
        };
    }

    let mut lines = vec!["Current active expenses:".to_string()];
    lines.extend(selected.iter().map(|expense| {
        format!(
            "- {}: {} {} {}{}",
            expense.id,
            expense.merchant,
            expense.currency,
            expense.amount,
            expense
                .spent_at
                .as_ref()
                .map(|date| format!(" on {date}"))
                .unwrap_or_default()
        )
    }));
    RouteDecision::Reply {
        message: lines.join("\n"),
        clear_clarification: false,
    }
}

fn route_delete(
    text: &str,
    expenses: &[StoredExpense],
    clear_clarification: bool,
) -> RouteDecision {
    let matches = select_candidates(text, expenses);
    match matches.as_slice() {
        [] => RouteDecision::Reply {
            message: "I could not find an active expense matching that delete request.".to_string(),
            clear_clarification: false,
        },
        [expense] => RouteDecision::Delete {
            expense_id: expense.id.clone(),
            clear_clarification,
        },
        _ => RouteDecision::Clarify {
            clarification: PendingClarification {
                question: format!(
                    "I found multiple matching expenses. Which one should I delete?\n{}",
                    render_candidates(&matches)
                ),
                context: ClarificationContext::ChooseExpense {
                    candidates: matches.iter().map(expense_candidate).collect(),
                    intent: ClarifiedIntent::Delete,
                },
            },
        },
    }
}

fn route_update(
    text: &str,
    expenses: &[StoredExpense],
    clear_clarification: bool,
) -> RouteDecision {
    let changes = parse_changes(text);
    if changes == ExpenseChanges::default() {
        return RouteDecision::Reply {
            message: "I heard an edit request, but I could not tell what should change."
                .to_string(),
            clear_clarification: false,
        };
    }

    let matches = select_candidates(text, expenses);
    match matches.as_slice() {
        [] => RouteDecision::Reply {
            message: "I could not find an active expense to update from that message.".to_string(),
            clear_clarification: false,
        },
        [expense] => RouteDecision::Update {
            expense_id: expense.id.clone(),
            changes,
            clear_clarification,
        },
        _ => RouteDecision::Clarify {
            clarification: PendingClarification {
                question: format!(
                    "I found multiple matching expenses. Which one should I update?\n{}",
                    render_candidates(&matches)
                ),
                context: ClarificationContext::ChooseExpense {
                    candidates: matches.iter().map(expense_candidate).collect(),
                    intent: ClarifiedIntent::Update { changes },
                },
            },
        },
    }
}

fn detect_photo_update_candidate<'a>(
    draft: &ExpenseDraft,
    expenses: &'a [StoredExpense],
) -> Option<&'a StoredExpense> {
    let merchant = draft.merchant.as_ref()?;
    let amount = draft.amount.as_ref()?;
    let mut matches = expenses
        .iter()
        .filter(|expense| expense.status == ExpenseStatus::Active)
        .filter(|expense| expense.merchant.eq_ignore_ascii_case(merchant))
        .filter(|expense| expense.amount != *amount)
        .filter(|expense| match (&draft.spent_at, &expense.spent_at) {
            (Some(draft_date), Some(expense_date)) => draft_date == expense_date,
            (Some(_), None) => false,
            _ => true,
        })
        .collect::<Vec<_>>();
    (matches.len() == 1).then(|| matches.remove(0))
}

fn resolve_pending_clarification(
    clarification: &PendingClarification,
    text: &TextTrigger,
) -> Option<RouteDecision> {
    match &clarification.context {
        ClarificationContext::CompleteDraft {
            draft,
            missing_fields,
        } => resolve_complete_draft_clarification(draft, missing_fields, text),
        ClarificationContext::ChooseExpense { candidates, intent } => {
            resolve_choose_expense_clarification(candidates, intent, text)
        }
        ClarificationContext::ResolvePhotoUpdate { existing, draft } => {
            resolve_photo_update_clarification(existing, draft, text)
        }
    }
}

fn resolve_complete_draft_clarification(
    draft: &ExpenseDraft,
    missing_fields: &[String],
    text: &TextTrigger,
) -> Option<RouteDecision> {
    if is_cancellation_answer(&text.text) {
        return Some(RouteDecision::Reply {
            message: "Okay, I won't save that expense.".to_string(),
            clear_clarification: true,
        });
    }

    let mut draft = draft.clone();
    let mut answered_any = false;
    if missing_fields
        .iter()
        .any(|field| field.to_ascii_lowercase().contains("amount"))
        && draft.amount.is_none()
        && looks_like_amount_answer(&text.text)
    {
        draft.amount = guess_amount(&text.text);
        answered_any |= draft.amount.is_some();
    }
    if missing_fields
        .iter()
        .any(|field| field.to_ascii_lowercase().contains("merchant"))
        && draft.merchant.is_none()
    {
        let candidate = text.text.trim();
        let can_use_direct_merchant = looks_like_merchant_answer(candidate)
            || (guess_amount(&text.text).is_some() && guess_merchant(candidate).is_some());
        if can_use_direct_merchant {
            draft.merchant = guess_merchant(candidate)
                .or_else(|| (!candidate.is_empty()).then_some(candidate.to_string()));
            answered_any |= draft.merchant.is_some();
        }
    }
    let missing_fields = missing_draft_fields(&draft);
    if missing_fields.is_empty() {
        Some(RouteDecision::Create { draft })
    } else if answered_any {
        Some(RouteDecision::Clarify {
            clarification: PendingClarification {
                question: format!(
                    "I still need {} before I can save that expense.",
                    missing_fields.join(" and ")
                ),
                context: ClarificationContext::CompleteDraft {
                    draft,
                    missing_fields,
                },
            },
        })
    } else if guess_amount(&text.text).is_some() {
        Some(RouteDecision::Reply {
            message: "I still need a direct amount answer before I can save that expense."
                .to_string(),
            clear_clarification: false,
        })
    } else {
        None
    }
}

fn resolve_choose_expense_clarification(
    candidates: &[ExpenseCandidate],
    intent: &ClarifiedIntent,
    text: &TextTrigger,
) -> Option<RouteDecision> {
    if is_cancellation_answer(&text.text) {
        return Some(RouteDecision::Reply {
            message: "Okay, I won't make that expense change.".to_string(),
            clear_clarification: true,
        });
    }
    let selected = pick_candidate(&text.text, candidates)?;
    Some(match intent {
        ClarifiedIntent::Delete => RouteDecision::Delete {
            expense_id: selected.id.clone(),
            clear_clarification: true,
        },
        ClarifiedIntent::Update { changes } => RouteDecision::Update {
            expense_id: selected.id.clone(),
            changes: changes.clone(),
            clear_clarification: true,
        },
    })
}

fn resolve_photo_update_clarification(
    existing: &ExpenseCandidate,
    draft: &ExpenseDraft,
    text: &TextTrigger,
) -> Option<RouteDecision> {
    if is_cancellation_answer(&text.text) {
        return Some(RouteDecision::Reply {
            message: "Okay, I won't save or update that expense.".to_string(),
            clear_clarification: true,
        });
    }
    if is_update_existing_answer(&text.text) {
        return Some(RouteDecision::Update {
            expense_id: existing.id.clone(),
            changes: ExpenseChanges {
                merchant: draft.merchant.clone(),
                amount: draft.amount.clone(),
                currency: draft.currency.clone(),
                spent_at: draft.spent_at.clone(),
                notes: draft.notes.clone(),
                source_image: draft.source_image.clone(),
            },
            clear_clarification: true,
        });
    }
    if is_save_new_answer(&text.text) {
        return Some(RouteDecision::Create {
            draft: draft.clone(),
        });
    }
    None
}

fn expense_directory(state_root: &str) -> PathBuf {
    PathBuf::from(state_root).join("expense-reports")
}

fn load_expenses(directory: &Path) -> Result<Vec<StoredExpense>, String> {
    let mut expenses = Vec::new();
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("md") {
            continue;
        }
        expenses.push(parse_expense(&path)?);
    }
    expenses.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(expenses)
}

fn read_expense(directory: &Path, expense_id: &str) -> Result<StoredExpense, String> {
    parse_expense(&directory.join(format!("{expense_id}.md")))
}

fn write_expense(directory: &Path, expense: &StoredExpense) -> Result<(), String> {
    let path = directory.join(format!("{}.md", expense.id));
    fs::write(path, render_expense(expense)).map_err(|error| error.to_string())
}

#[derive(Debug, Serialize, Deserialize)]
struct ExpenseFrontmatter {
    id: String,
    status: ExpenseStatus,
    merchant: String,
    amount: String,
    currency: String,
    spent_at: Option<String>,
    source_image: Option<String>,
    created_at: String,
    updated_at: String,
}

fn render_expense(expense: &StoredExpense) -> String {
    let frontmatter = ExpenseFrontmatter {
        id: expense.id.clone(),
        status: expense.status,
        merchant: expense.merchant.clone(),
        amount: expense.amount.clone(),
        currency: expense.currency.clone(),
        spent_at: expense.spent_at.clone(),
        source_image: expense.source_image.clone(),
        created_at: expense.created_at.clone(),
        updated_at: expense.updated_at.clone(),
    };
    let body = expense.notes.clone().unwrap_or_default();
    format!(
        "+++\n{}+++\n\n{}\n",
        toml::to_string(&frontmatter).expect("expense frontmatter should serialize"),
        body
    )
}

fn parse_expense(path: &Path) -> Result<StoredExpense, String> {
    let raw = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let remainder = raw
        .strip_prefix("+++\n")
        .ok_or_else(|| format!("{} is missing frontmatter", path.display()))?;
    let (frontmatter, body) = remainder
        .split_once("\n+++\n")
        .ok_or_else(|| format!("{} has invalid frontmatter", path.display()))?;
    let parsed: ExpenseFrontmatter =
        toml::from_str(frontmatter).map_err(|error| error.to_string())?;
    let notes = (!body.trim().is_empty()).then_some(body.trim().to_string());
    Ok(StoredExpense {
        id: parsed.id,
        status: parsed.status,
        merchant: parsed.merchant,
        amount: parsed.amount,
        currency: parsed.currency,
        spent_at: parsed.spent_at,
        notes,
        source_image: parsed.source_image,
        created_at: parsed.created_at,
        updated_at: parsed.updated_at,
    })
}

fn next_expense_id() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "expense-{}-{}",
        duration.as_secs(),
        duration.subsec_millis()
    )
}

fn now_stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn looks_like_expense_artifact(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    guess_amount(text).is_some()
        || [
            "receipt",
            "total",
            "subtotal",
            "tax",
            "visa",
            "mastercard",
            "thanks",
        ]
        .iter()
        .any(|token| lower.contains(token))
}

fn guess_amount(text: &str) -> Option<String> {
    let regex = Regex::new(r"(?i)(?:sgd|usd|eur|gbp|aud|cad|\$)?\s*([0-9]+(?:[.,][0-9]{2}))")
        .expect("amount regex");
    regex
        .captures_iter(text)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str().replace(',', ".")))
        .max_by(|left, right| {
            left.parse::<f64>()
                .unwrap_or_default()
                .partial_cmp(&right.parse::<f64>().unwrap_or_default())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn guess_currency(text: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("sgd") || lower.contains("s$") {
        Some("SGD".to_string())
    } else if lower.contains("usd") || lower.contains("us$") || lower.contains('$') {
        Some("USD".to_string())
    } else {
        None
    }
}

fn guess_merchant(text: &str) -> Option<String> {
    let ignored = [
        "receipt",
        "total",
        "subtotal",
        "tax",
        "card",
        "visa",
        "mastercard",
    ];
    text.lines()
        .map(str::trim)
        .find(|line| {
            !line.is_empty()
                && line.chars().any(char::is_alphabetic)
                && !looks_like_currency_amount_line(line)
                && !ignored
                    .iter()
                    .any(|ignored| line.to_ascii_lowercase().contains(ignored))
        })
        .map(std::string::ToString::to_string)
}

fn looks_like_currency_amount_line(line: &str) -> bool {
    let lower = line.trim().to_ascii_lowercase();
    guess_amount(&lower).is_some()
        && ["sgd", "usd", "eur", "gbp", "aud", "cad", "s$", "us$"]
            .iter()
            .any(|prefix| lower == *prefix || lower.starts_with(&format!("{prefix} ")))
}

fn guess_date(text: &str) -> Option<String> {
    let regex = Regex::new(r"\b(20[0-9]{2}[-/][0-9]{2}[-/][0-9]{2})\b").expect("date regex");
    regex
        .captures(text)
        .and_then(|capture| capture.get(1))
        .map(|value| value.as_str().replace('/', "-"))
}

fn looks_like_merchant_answer(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.ends_with('?') {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    let words = lower.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() || words.len() > 5 {
        return false;
    }

    let conversational_starters = [
        "tell", "show", "what", "why", "how", "can", "could", "would", "should", "please", "joke",
        "who", "hello", "hi", "hey", "thanks", "thank", "list", "read", "delete", "remove",
        "update", "change", "edit", "set", "good",
    ];
    if conversational_starters
        .iter()
        .any(|starter| words.first() == Some(starter))
    {
        return false;
    }

    trimmed.chars().any(char::is_alphabetic)
}

fn looks_like_amount_answer(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.ends_with('?') {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    let words = lower.split_whitespace().collect::<Vec<_>>();
    let conversational_starters = [
        "is", "what", "why", "how", "can", "could", "would", "should", "please", "tell", "show",
        "do", "does", "did", "not", "dont", "don't", "maybe",
    ];
    if conversational_starters
        .iter()
        .any(|starter| words.first() == Some(starter))
    {
        return false;
    }

    guess_amount(trimmed).is_some()
}

fn is_cancellation_answer(text: &str) -> bool {
    let lower = text.trim().to_ascii_lowercase();
    [
        "not an expense",
        "cancel",
        "never mind",
        "nevermind",
        "ignore it",
        "ignore this",
        "skip it",
    ]
    .iter()
    .any(|phrase| lower == *phrase || lower.starts_with(&format!("{phrase} ")))
}

fn is_update_existing_answer(text: &str) -> bool {
    let lower = text.trim().to_ascii_lowercase();
    [
        "update it",
        "update the existing one",
        "update existing",
        "replace the old one",
        "use the existing expense",
        "correct the existing expense",
        "fix the existing expense",
    ]
    .iter()
    .any(|phrase| lower == *phrase || lower.contains(phrase))
}

fn is_save_new_answer(text: &str) -> bool {
    let lower = text.trim().to_ascii_lowercase();
    [
        "save it as new",
        "save as new",
        "create a new one",
        "new expense",
        "keep both",
        "add a new expense",
    ]
    .iter()
    .any(|phrase| lower == *phrase || lower.contains(phrase))
}

#[derive(Debug, Clone, Copy)]
enum ExpenseIntent {
    Read,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy)]
enum IntentSelection {
    None,
    Single(ExpenseIntent),
    Mixed,
}

impl IntentSelection {
    fn include(self, enabled: bool, intent: ExpenseIntent) -> Self {
        if !enabled {
            return self;
        }
        match self {
            Self::None => Self::Single(intent),
            Self::Single(_) | Self::Mixed => Self::Mixed,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IntentSignals {
    selection: IntentSelection,
    has_negated_mutation: bool,
}

fn detect_intents(text: &str, raw_text: &str, expenses: &[StoredExpense]) -> IntentSignals {
    let read = is_read_intent(text, raw_text, expenses);
    let update = is_update_intent(text);
    let delete = is_delete_intent(text);
    let negated_update = is_negated_intent(text, &["update", "change", "edit", "set"]);
    let negated_delete = is_negated_intent(text, &["delete", "remove"]);

    IntentSignals {
        selection: IntentSelection::None
            .include(read, ExpenseIntent::Read)
            .include(update, ExpenseIntent::Update)
            .include(delete, ExpenseIntent::Delete),
        has_negated_mutation: negated_update || negated_delete,
    }
}

fn should_resolve_clarification_first(
    clarification: &PendingClarification,
    text: &TextTrigger,
) -> bool {
    match &clarification.context {
        ClarificationContext::CompleteDraft {
            draft,
            missing_fields,
        } => {
            let needs_amount = missing_fields
                .iter()
                .any(|field| field.to_ascii_lowercase().contains("amount"))
                && draft.amount.is_none();
            let needs_merchant = missing_fields
                .iter()
                .any(|field| field.to_ascii_lowercase().contains("merchant"))
                && draft.merchant.is_none();

            let can_answer_amount = !needs_amount || looks_like_amount_answer(&text.text);
            let can_answer_merchant = !needs_merchant
                || if needs_amount {
                    guess_merchant(&text.text).is_some()
                } else {
                    looks_like_merchant_answer(&text.text)
                };

            can_answer_amount && can_answer_merchant
        }
        ClarificationContext::ChooseExpense { .. } => false,
        ClarificationContext::ResolvePhotoUpdate { .. } => {
            is_cancellation_answer(&text.text)
                || is_update_existing_answer(&text.text)
                || is_save_new_answer(&text.text)
        }
    }
}

fn missing_draft_fields(draft: &ExpenseDraft) -> Vec<String> {
    let mut missing = Vec::new();
    if draft.merchant.as_ref().is_none_or(String::is_empty) {
        missing.push("the merchant".to_string());
    }
    if draft.amount.as_ref().is_none_or(String::is_empty) {
        missing.push("the amount".to_string());
    }
    missing
}

fn is_delete_intent(text: &str) -> bool {
    starts_with_command(text, &["delete", "remove"])
        || has_follow_on_command(text, &["delete", "remove"])
}

fn is_update_intent(text: &str) -> bool {
    starts_with_command(text, &["update", "change", "edit", "set"])
        || has_follow_on_command(text, &["update", "change", "edit", "set"])
}

fn is_read_intent(text: &str, raw_text: &str, expenses: &[StoredExpense]) -> bool {
    let mentions_expense_domain = ["expense", "expenses", "ledger", "spent", "receipt"]
        .iter()
        .any(|keyword| text.contains(keyword));
    let explicit_read_verb = starts_with_command(text, &["show", "list", "read"])
        || has_follow_on_command(text, &["show", "list", "read"]);
    if explicit_read_verb && mentions_expense_domain {
        return true;
    }

    if starts_with_command(text, &["show", "list", "read"]) {
        return !select_candidates(raw_text, expenses).is_empty();
    }

    if text.contains("what") && mentions_expense_domain {
        return true;
    }

    if starts_with_command(text, &["find", "lookup", "search"]) {
        return !select_candidates(raw_text, expenses).is_empty();
    }

    false
}

fn strip_polite_prefix(text: &str) -> &str {
    [
        "please ",
        "can you ",
        "could you ",
        "would you ",
        "will you ",
    ]
    .iter()
    .find_map(|prefix| text.strip_prefix(prefix))
    .unwrap_or(text)
}

fn starts_with_command(text: &str, verbs: &[&str]) -> bool {
    let stripped = strip_polite_prefix(text.trim());
    verbs.iter().any(|verb| {
        stripped == *verb
            || stripped.starts_with(&format!("{verb} "))
            || stripped.starts_with(&format!("{verb},"))
    })
}

fn has_follow_on_command(text: &str, verbs: &[&str]) -> bool {
    let connectors = [" and ", " then ", ", then ", ", and ", " just ", " also "];
    connectors.iter().any(|connector| {
        verbs.iter().any(|verb| {
            text.contains(&format!("{connector}{verb} "))
                || text.ends_with(&format!("{connector}{verb}"))
        })
    })
}

fn is_negated_intent(text: &str, verbs: &[&str]) -> bool {
    let stripped = strip_polite_prefix(text.trim());
    ["don't ", "dont ", "do not ", "not "]
        .iter()
        .filter_map(|prefix| stripped.strip_prefix(prefix))
        .any(|remainder| starts_with_command(remainder, verbs))
}

fn parse_changes(text: &str) -> ExpenseChanges {
    let amount = guess_amount(text);
    let lower = text.to_ascii_lowercase();
    let spent_at = guess_date(text);
    ExpenseChanges {
        amount,
        currency: guess_currency(text),
        spent_at,
        merchant: if lower.contains("merchant is ") {
            text.split_once("merchant is ")
                .map(|(_, merchant)| merchant.trim().to_string())
        } else {
            None
        },
        notes: None,
        source_image: None,
    }
}

fn select_candidates(query: &str, expenses: &[StoredExpense]) -> Vec<StoredExpense> {
    let tokens = query_tokens(query);
    let mut scored = expenses
        .iter()
        .filter(|expense| expense.status == ExpenseStatus::Active)
        .filter_map(|expense| {
            let score = score_expense(expense, &tokens);
            (score > 0).then_some((score, expense.clone()))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    scored.into_iter().map(|(_, expense)| expense).collect()
}

fn query_tokens(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| token.len() > 2)
        .filter(|token| {
            ![
                "the", "for", "and", "that", "this", "expense", "delete", "remove", "update",
                "change", "show", "list", "read", "what", "find", "set", "amount",
            ]
            .contains(&token.as_str())
        })
        .collect()
}

fn score_expense(expense: &StoredExpense, tokens: &[String]) -> usize {
    tokens
        .iter()
        .map(|token| {
            usize::from(expense.id.to_ascii_lowercase().contains(token))
                + usize::from(expense.merchant.to_ascii_lowercase().contains(token)) * 3
                + usize::from(
                    expense
                        .notes
                        .as_ref()
                        .is_some_and(|notes| notes.to_ascii_lowercase().contains(token)),
                )
        })
        .sum()
}

fn render_candidates(candidates: &[StoredExpense]) -> String {
    candidates
        .iter()
        .map(|expense| {
            format!(
                "- {}: {} {} {}",
                expense.id, expense.merchant, expense.currency, expense.amount
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn expense_candidate(expense: &StoredExpense) -> ExpenseCandidate {
    ExpenseCandidate {
        id: expense.id.clone(),
        merchant: expense.merchant.clone(),
        amount: expense.amount.clone(),
    }
}

fn pick_candidate<'a>(
    answer: &str,
    candidates: &'a [ExpenseCandidate],
) -> Option<&'a ExpenseCandidate> {
    let lower = answer.to_ascii_lowercase();
    let mut matches = candidates
        .iter()
        .filter(|candidate| {
            lower.contains(&candidate.id.to_ascii_lowercase())
                || lower.contains(&candidate.merchant.to_ascii_lowercase())
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        matches.pop()
    } else {
        None
    }
}

fn extract_ocr_text(path: &str, executable: &str) -> String {
    let Ok(mut child) = Command::new(executable)
        .arg(path)
        .arg("stdout")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    else {
        return String::new();
    };
    let Some(mut stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return String::new();
    };
    let Some(mut stderr) = child.stderr.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return String::new();
    };

    let timeout = Duration::from_millis(750);
    let poll_interval = Duration::from_millis(25);
    let started = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut rendered_stdout = String::new();
                let _ = stdout.read_to_string(&mut rendered_stdout);
                if !status.success() {
                    let mut rendered_stderr = String::new();
                    let _ = stderr.read_to_string(&mut rendered_stderr);
                    return String::new();
                }
                return rendered_stdout.trim().to_string();
            }
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return String::new();
            }
            Ok(None) => thread::sleep(poll_interval),
            Err(_) => return String::new(),
        }
    }
}
