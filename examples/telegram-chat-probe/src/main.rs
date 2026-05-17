use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use mango_telegram::{TelegramChatId, TelegramThreadId, TestTelegramActor};
use rand::{Rng, SeedableRng, prelude::SliceRandom, rngs::StdRng};
use serde::Deserialize;
use telegram_chat::testing::{
    ScriptedConversationBackend, TelegramChatHarness, TelegramChatHarnessBuilder,
};
use tracing::{info, warn};

const CONFIG_FILE_NAME: &str = "telegram-chat-probe.toml";
const BASELINE_PREFIX: &str = "baseline fallback:";
const WHITELIST_REPLY: &str = "sorry, you're not my customer";

const GENERAL_TEXTS: &[&str] = &[
    "hello there",
    "tell me a joke",
    "what are you up to",
    "summarize this day for me",
];
const INTRUDER_TEXTS: &[&str] = &["hello bot", "can you help me", "show my expenses"];
const NON_EXPENSE_CAPTIONS: &[&str] = &["cat", "dog", "sunset", "vacation"];

#[derive(Debug, Deserialize)]
struct RawConfig {
    verification: Option<RawVerificationConfig>,
}

#[derive(Debug, Deserialize)]
struct RawVerificationConfig {
    seed: Option<u64>,
    random_sessions: Option<usize>,
    max_turns: Option<usize>,
    reply_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct Config {
    seed: u64,
    random_sessions: usize,
    max_turns: usize,
    reply_timeout: Duration,
}

#[derive(Debug, Clone, Copy)]
enum SessionKind {
    Root,
    Threaded,
}

impl SessionKind {
    fn label(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Threaded => "threaded",
        }
    }

    fn thread_id(self) -> Option<TelegramThreadId> {
        match self {
            Self::Root => None,
            Self::Threaded => Some(TelegramThreadId(11)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplyKind {
    Baseline,
    Whitelist,
    Clarification,
    ExpenseRead,
    ExpenseSaved,
    ExpenseUpdated,
    ExpenseDeleted,
    ExpenseLookupMiss,
    Duplicate,
    AutomationUnavailable,
    Empty,
    Unknown,
}

#[derive(Debug, Clone)]
enum PlannedTurn {
    TrustedChat { text: String },
    IntruderChat { text: String },
    ExpensePhoto,
    NonExpensePhoto { caption: String },
    ClarificationAnswer { merchant: String, amount: String },
    ReadExpenses,
    UpdateExpense { merchant: String, amount: String },
    DeleteExpense { merchant: String },
}

impl PlannedTurn {
    fn label(&self) -> String {
        match self {
            Self::TrustedChat { text } => format!("trusted_chat({text})"),
            Self::IntruderChat { text } => format!("intruder_chat({text})"),
            Self::ExpensePhoto => "expense_photo(receipt)".to_string(),
            Self::NonExpensePhoto { caption } => format!("non_expense_photo({caption})"),
            Self::ClarificationAnswer { merchant, amount } => {
                format!("clarification_answer({merchant} {amount})")
            }
            Self::ReadExpenses => "read_expenses".to_string(),
            Self::UpdateExpense { merchant, amount } => {
                format!("update_expense({merchant} -> {amount})")
            }
            Self::DeleteExpense { merchant } => format!("delete_expense({merchant})"),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SessionModel {
    pending_clarification: bool,
    active_merchants: Vec<String>,
    next_merchant_id: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let config = load_config(probe_config_path().as_deref())?;
    run_probe(&config).await
}

async fn run_probe(config: &Config) -> Result<()> {
    info!(
        "telegram-chat probe starting: seed={} random_sessions={} max_turns={} reply_timeout_secs={}",
        config.seed,
        config.random_sessions,
        config.max_turns,
        config.reply_timeout.as_secs()
    );

    verify_scripted_surface(SessionKind::Root, 1, config).await?;
    verify_scripted_surface(SessionKind::Threaded, 2, config).await?;

    for index in 0..config.random_sessions {
        let kind = if index % 2 == 0 {
            SessionKind::Root
        } else {
            SessionKind::Threaded
        };
        let seed = config.seed.wrapping_add(index as u64);
        fuzz_session(kind, index + 100, seed, config).await?;
    }

    info!("telegram-chat probe passed");
    Ok(())
}

fn load_config(path: Option<&Path>) -> Result<Config> {
    let defaults = Config {
        seed: 7,
        random_sessions: 4,
        max_turns: 10,
        reply_timeout: Duration::from_secs(20),
    };

    let Some(path) = path else {
        warn!("{} not found; using built-in defaults", CONFIG_FILE_NAME);
        return Ok(defaults);
    };

    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read probe config {}", path.display()))?;
    let raw: RawConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse probe config {}", path.display()))?;
    let verification = raw.verification.unwrap_or(RawVerificationConfig {
        seed: None,
        random_sessions: None,
        max_turns: None,
        reply_timeout_secs: None,
    });

    Ok(Config {
        seed: verification.seed.unwrap_or(defaults.seed),
        random_sessions: verification
            .random_sessions
            .unwrap_or(defaults.random_sessions),
        max_turns: verification.max_turns.unwrap_or(defaults.max_turns),
        reply_timeout: Duration::from_secs(
            verification
                .reply_timeout_secs
                .unwrap_or(defaults.reply_timeout.as_secs()),
        ),
    })
}

fn probe_config_path() -> Option<PathBuf> {
    if let Some(path) = env::args_os().nth(1) {
        return Some(PathBuf::from(path));
    }

    let default = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(CONFIG_FILE_NAME);
    default.exists().then_some(default)
}

async fn verify_scripted_surface(
    kind: SessionKind,
    session_index: usize,
    config: &Config,
) -> Result<()> {
    let harness = build_harness(kind, session_index).await?;
    let intruder = intruder_actor_for(harness.actor());

    send_text_checked(
        &harness,
        kind,
        "scripted_trusted_baseline",
        &PlannedTurn::TrustedChat {
            text: "tell me a joke".to_string(),
        },
        config.reply_timeout,
        &intruder,
        ReplyKind::Baseline,
    )
    .await?;

    send_text_checked(
        &harness,
        kind,
        "scripted_intruder_rejection",
        &PlannedTurn::IntruderChat {
            text: "show my expenses".to_string(),
        },
        config.reply_timeout,
        &intruder,
        ReplyKind::Whitelist,
    )
    .await?;

    send_photo_checked(
        &harness,
        kind,
        "scripted_non_expense_photo",
        &PlannedTurn::NonExpensePhoto {
            caption: "cat".to_string(),
        },
        config.reply_timeout,
        ReplyKind::Baseline,
        0,
    )
    .await?;

    send_photo_checked(
        &harness,
        kind,
        "scripted_receipt_clarification",
        &PlannedTurn::ExpensePhoto,
        config.reply_timeout,
        ReplyKind::Clarification,
        1,
    )
    .await?;

    send_text_checked(
        &harness,
        kind,
        "scripted_read_during_clarification",
        &PlannedTurn::ReadExpenses,
        config.reply_timeout,
        &intruder,
        ReplyKind::ExpenseRead,
    )
    .await?;

    send_text_checked(
        &harness,
        kind,
        "scripted_complete_original_clarification",
        &PlannedTurn::ClarificationAnswer {
            merchant: "Probe Lunch".to_string(),
            amount: "12.50".to_string(),
        },
        config.reply_timeout,
        &intruder,
        ReplyKind::ExpenseSaved,
    )
    .await?;

    assert_markdown_parseable(&harness)
        .with_context(|| format!("{} scripted markdown parseability failed", kind.label()))?;

    info!("scripted probe passed for {}", kind.label());
    Ok(())
}

async fn fuzz_session(
    kind: SessionKind,
    session_index: usize,
    seed: u64,
    config: &Config,
) -> Result<()> {
    let harness = build_harness(kind, session_index).await?;
    let intruder = intruder_actor_for(harness.actor());
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = SessionModel::default();

    for step in 0..config.max_turns {
        let planned = plan_turn(&mut rng, &mut model);
        let reply = execute_turn(
            &harness,
            kind,
            &intruder,
            step,
            &planned,
            config.reply_timeout,
        )
        .await
        .with_context(|| format!("random fuzz failure seed={seed} step={step}"))?;
        apply_model(&mut model, &planned, &reply);
        assert_markdown_parseable(&harness)
            .with_context(|| format!("markdown parseability failed seed={seed} step={step}"))?;
    }

    info!("random probe passed for {} seed={seed}", kind.label());
    Ok(())
}

async fn build_harness(kind: SessionKind, session_index: usize) -> Result<TelegramChatHarness> {
    let session_offset = i64::try_from(session_index).context("session index exceeded i64")?;
    let actor = TestTelegramActor::new(
        TelegramChatId(7_000 + session_offset),
        kind.thread_id(),
        Some("trusted_customer".to_string()),
        format!("Trusted Customer {}", kind.label()),
    );
    TelegramChatHarnessBuilder::new()
        .with_actor(actor)
        .with_baseline_backend(ScriptedConversationBackend::new(|prompt| async move {
            format!("{BASELINE_PREFIX} {prompt}")
        }))
        .build()
        .await
        .map_err(|error| anyhow!(error))
}

fn intruder_actor_for(actor: &TestTelegramActor) -> TestTelegramActor {
    TestTelegramActor::new(
        actor.chat_id,
        actor.thread_id,
        Some("intruder".to_string()),
        "Intruder",
    )
}

fn plan_turn(rng: &mut StdRng, model: &mut SessionModel) -> PlannedTurn {
    if model.pending_clarification {
        plan_pending_clarification_turn(rng, model)
    } else if model.active_merchants.is_empty() {
        plan_empty_ledger_turn(rng)
    } else {
        plan_active_ledger_turn(rng, model)
    }
}

fn plan_pending_clarification_turn(rng: &mut StdRng, model: &mut SessionModel) -> PlannedTurn {
    match rng.gen_range(0..100) {
        0..=24 => {
            let merchant = next_merchant_name(model);
            PlannedTurn::ClarificationAnswer {
                merchant,
                amount: random_amount(rng),
            }
        }
        25..=44 => PlannedTurn::ReadExpenses,
        45..=59 if !model.active_merchants.is_empty() => PlannedTurn::UpdateExpense {
            merchant: random_active_merchant(rng, model),
            amount: random_amount(rng),
        },
        60..=74 if !model.active_merchants.is_empty() => PlannedTurn::DeleteExpense {
            merchant: random_active_merchant(rng, model),
        },
        75..=89 => PlannedTurn::TrustedChat {
            text: random_general_text(rng),
        },
        _ => PlannedTurn::IntruderChat {
            text: random_intruder_text(rng),
        },
    }
}

fn plan_empty_ledger_turn(rng: &mut StdRng) -> PlannedTurn {
    match rng.gen_range(0..100) {
        0..=24 => PlannedTurn::TrustedChat {
            text: random_general_text(rng),
        },
        25..=39 => PlannedTurn::IntruderChat {
            text: random_intruder_text(rng),
        },
        40..=59 => PlannedTurn::NonExpensePhoto {
            caption: random_non_expense_caption(rng),
        },
        60..=89 => PlannedTurn::ExpensePhoto,
        _ => PlannedTurn::ReadExpenses,
    }
}

fn plan_active_ledger_turn(rng: &mut StdRng, model: &SessionModel) -> PlannedTurn {
    match rng.gen_range(0..100) {
        0..=14 => PlannedTurn::TrustedChat {
            text: random_general_text(rng),
        },
        15..=24 => PlannedTurn::IntruderChat {
            text: random_intruder_text(rng),
        },
        25..=39 => PlannedTurn::ReadExpenses,
        40..=59 => PlannedTurn::UpdateExpense {
            merchant: random_active_merchant(rng, model),
            amount: random_amount(rng),
        },
        60..=74 => PlannedTurn::DeleteExpense {
            merchant: random_active_merchant(rng, model),
        },
        75..=84 => PlannedTurn::NonExpensePhoto {
            caption: random_non_expense_caption(rng),
        },
        _ => PlannedTurn::ExpensePhoto,
    }
}

fn next_merchant_name(model: &mut SessionModel) -> String {
    let merchant = format!("Probe Merchant {}", model.next_merchant_id);
    model.next_merchant_id += 1;
    merchant
}

fn random_amount(rng: &mut StdRng) -> String {
    format!("{}.{}", rng.gen_range(5..80), rng.gen_range(10..99))
}

fn random_active_merchant(rng: &mut StdRng, model: &SessionModel) -> String {
    model
        .active_merchants
        .choose(rng)
        .cloned()
        .unwrap_or_else(|| "Probe Merchant".to_string())
}

fn random_general_text(rng: &mut StdRng) -> String {
    random_text(rng, GENERAL_TEXTS, "hello there")
}

fn random_intruder_text(rng: &mut StdRng) -> String {
    random_text(rng, INTRUDER_TEXTS, "hello bot")
}

fn random_non_expense_caption(rng: &mut StdRng) -> String {
    random_text(rng, NON_EXPENSE_CAPTIONS, "cat")
}

fn random_text(rng: &mut StdRng, choices: &[&str], fallback: &str) -> String {
    choices.choose(rng).copied().unwrap_or(fallback).to_string()
}

async fn execute_turn(
    harness: &TelegramChatHarness,
    kind: SessionKind,
    intruder: &TestTelegramActor,
    step: usize,
    planned: &PlannedTurn,
    reply_timeout: Duration,
) -> Result<ReplyKind> {
    match planned {
        PlannedTurn::TrustedChat { .. }
        | PlannedTurn::IntruderChat { .. }
        | PlannedTurn::ReadExpenses
        | PlannedTurn::ClarificationAnswer { .. }
        | PlannedTurn::UpdateExpense { .. }
        | PlannedTurn::DeleteExpense { .. } => {
            send_text_checked(
                harness,
                kind,
                &format!("random_step_{step}"),
                planned,
                reply_timeout,
                intruder,
                expected_reply_kind(planned),
            )
            .await
        }
        PlannedTurn::ExpensePhoto | PlannedTurn::NonExpensePhoto { .. } => {
            send_photo_checked(
                harness,
                kind,
                &format!("random_step_{step}"),
                planned,
                reply_timeout,
                expected_reply_kind(planned),
                step,
            )
            .await
        }
    }
}

async fn send_text_checked(
    harness: &TelegramChatHarness,
    kind: SessionKind,
    label: &str,
    planned: &PlannedTurn,
    reply_timeout: Duration,
    intruder: &TestTelegramActor,
    expected: ReplyKind,
) -> Result<ReplyKind> {
    info!("{} {} sending {}", kind.label(), label, planned.label());
    match planned {
        PlannedTurn::TrustedChat { text } => harness.send_text(text).await?,
        PlannedTurn::IntruderChat { text } => harness.send_text_from(intruder, text).await?,
        PlannedTurn::ClarificationAnswer { merchant, amount } => {
            harness.send_text(format!("{merchant} {amount}")).await?;
        }
        PlannedTurn::ReadExpenses => harness.send_text("show my expenses").await?,
        PlannedTurn::UpdateExpense { merchant, amount } => {
            harness
                .send_text(format!("change {merchant} to {amount}"))
                .await?;
        }
        PlannedTurn::DeleteExpense { merchant } => {
            harness.send_text(format!("delete {merchant}")).await?;
        }
        PlannedTurn::ExpensePhoto | PlannedTurn::NonExpensePhoto { .. } => {
            bail!("text sender received a photo turn")
        }
    }

    check_reply(harness, kind, label, planned, reply_timeout, expected).await
}

async fn send_photo_checked(
    harness: &TelegramChatHarness,
    kind: SessionKind,
    label: &str,
    planned: &PlannedTurn,
    reply_timeout: Duration,
    expected: ReplyKind,
    step: usize,
) -> Result<ReplyKind> {
    info!("{} {} sending {}", kind.label(), label, planned.label());
    let caption = match planned {
        PlannedTurn::ExpensePhoto => Some("receipt".to_string()),
        PlannedTurn::NonExpensePhoto { caption } => Some(caption.clone()),
        _ => bail!("photo sender received a non-photo turn"),
    };
    let file_stem = match planned {
        PlannedTurn::ExpensePhoto => "receipt",
        PlannedTurn::NonExpensePhoto { .. } => "photo",
        _ => unreachable!(),
    };
    let file = harness
        .write_photo_fixture(
            format!("probe/{}/{}-{step}-{file_stem}.jpg", kind.label(), label),
            b"fixture-image",
        )
        .map_err(|error| anyhow!(error))?;
    match planned {
        PlannedTurn::ExpensePhoto | PlannedTurn::NonExpensePhoto { .. } => {
            harness.send_photo(file, caption).await?;
        }
        _ => unreachable!(),
    }

    check_reply(harness, kind, label, planned, reply_timeout, expected).await
}

async fn check_reply(
    harness: &TelegramChatHarness,
    kind: SessionKind,
    label: &str,
    planned: &PlannedTurn,
    reply_timeout: Duration,
    expected: ReplyKind,
) -> Result<ReplyKind> {
    let reply = match harness.recv_reply_with_timeout(reply_timeout).await {
        Ok(reply) => reply,
        Err(error) => {
            bail!(
                "{} {} timed out waiting for a reply after {}: {}\n{}",
                kind.label(),
                label,
                planned.label(),
                error,
                diagnostics(harness).await?
            );
        }
    };
    let actual = classify_reply(&reply);
    if actual != expected {
        bail!(
            "unexpected reply for {} {} turn={} expected={expected:?} actual={actual:?}\nreply={reply}\n{}",
            kind.label(),
            label,
            planned.label(),
            diagnostics(harness).await?
        );
    }
    Ok(actual)
}

fn expected_reply_kind(planned: &PlannedTurn) -> ReplyKind {
    match planned {
        PlannedTurn::TrustedChat { .. } | PlannedTurn::NonExpensePhoto { .. } => {
            ReplyKind::Baseline
        }
        PlannedTurn::IntruderChat { .. } => ReplyKind::Whitelist,
        PlannedTurn::ExpensePhoto => ReplyKind::Clarification,
        PlannedTurn::ClarificationAnswer { .. } => ReplyKind::ExpenseSaved,
        PlannedTurn::ReadExpenses => ReplyKind::ExpenseRead,
        PlannedTurn::UpdateExpense { .. } => ReplyKind::ExpenseUpdated,
        PlannedTurn::DeleteExpense { .. } => ReplyKind::ExpenseDeleted,
    }
}

fn classify_reply(reply: &str) -> ReplyKind {
    let trimmed = reply.trim();
    if trimmed.is_empty() {
        return ReplyKind::Empty;
    }
    if trimmed == WHITELIST_REPLY {
        return ReplyKind::Whitelist;
    }
    if trimmed.starts_with(BASELINE_PREFIX) {
        return ReplyKind::Baseline;
    }
    if trimmed.contains("I think that is an expense")
        || trimmed.contains("I still need")
        || trimmed.contains("Which one should I")
    {
        return ReplyKind::Clarification;
    }
    if trimmed.contains("Current active expenses")
        || trimmed.contains("You do not have any active expenses yet.")
    {
        return ReplyKind::ExpenseRead;
    }
    if trimmed.contains("Saved expense") {
        return ReplyKind::ExpenseSaved;
    }
    if trimmed.contains("Updated expense") {
        return ReplyKind::ExpenseUpdated;
    }
    if trimmed.contains("Soft-deleted expense") {
        return ReplyKind::ExpenseDeleted;
    }
    if trimmed.contains("I could not find an active expense")
        || trimmed.contains("I heard an edit request, but I could not tell what should change.")
    {
        return ReplyKind::ExpenseLookupMiss;
    }
    if trimmed.contains("duplicate") {
        return ReplyKind::Duplicate;
    }
    if trimmed.contains("sorry, I hit a problem while checking your expense automations") {
        return ReplyKind::AutomationUnavailable;
    }
    ReplyKind::Unknown
}

fn apply_model(model: &mut SessionModel, planned: &PlannedTurn, reply: &ReplyKind) {
    match (planned, reply) {
        (PlannedTurn::ExpensePhoto, ReplyKind::Clarification) => {
            model.pending_clarification = true;
        }
        (PlannedTurn::ClarificationAnswer { merchant, .. }, ReplyKind::ExpenseSaved) => {
            model.pending_clarification = false;
            if !model
                .active_merchants
                .iter()
                .any(|existing| existing == merchant)
            {
                model.active_merchants.push(merchant.clone());
            }
        }
        (PlannedTurn::DeleteExpense { merchant }, ReplyKind::ExpenseDeleted) => {
            model
                .active_merchants
                .retain(|existing| existing != merchant);
        }
        _ => {}
    }
}

fn assert_markdown_parseable(harness: &TelegramChatHarness) -> Result<()> {
    for path in harness
        .expense_markdown_files()
        .map_err(|error| anyhow!(error))?
    {
        let markdown = harness
            .read_markdown(&path)
            .map_err(|error| anyhow!(error))?;
        if !markdown.starts_with("+++\n") || !markdown.contains("\n+++\n") {
            bail!("{} is not valid frontmatter markdown", path.display());
        }
    }
    Ok(())
}

async fn diagnostics(harness: &TelegramChatHarness) -> Result<String> {
    let transcript = harness.transcript().await;
    let traces = harness.traces().map_err(|error| anyhow!(error))?;
    let markdown = harness
        .expense_markdown_files()
        .map_err(|error| anyhow!(error))?
        .into_iter()
        .map(|path| {
            let body = harness
                .read_markdown(&path)
                .unwrap_or_else(|error| error.to_string());
            format!("{}:\n{}", path.display(), body)
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    Ok(format!(
        "transcript={transcript:#?}\ntraces={traces:#?}\nmarkdown={markdown}"
    ))
}
