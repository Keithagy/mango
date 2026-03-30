use std::{
    collections::HashSet,
    fmt::Write as _,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mango_automation_control::{
    ActivationMode, AutomationsControlPlane, AutomationsError, EffectHandler, EffectHandlerOutcome,
    JsonFileControlPlaneStore, ManagedAutomation, RegisteredRevision, RegistrationRequest,
    SupervisorConfig, SupervisorHandle, SystemClock, WasmAutomationRuntime, spawn_supervisor,
};
use mango_automation_sdk::{AutomationEvent, EffectKind, EffectRequest, EffectResult};
use mango_shim_claude_agent::{ClaudeAgentBridge, ClaudeAgentConfig, ClaudeBridgeEvent};
use mango_telegram::{
    TelegramChatId, TelegramClient, TelegramInboundMessage, TelegramOutboundMessage,
    TelegramThreadId, TeloxideTelegramClient,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{error, info, warn};

const CONFIG_FILE_NAME: &str = "telegram-automations.toml";
const DEFAULT_STORY_SYSTEM_PROMPT: &str = "You are the story-writing backend for a Mango Telegram automation example. Return plain text only, no title, no markdown, and obey the exact requested word count.";
const GUEST_MANIFEST_PATH: &str =
    "examples/telegram-automations/guests/dice-story-automation/Cargo.toml";
const GUEST_ARTIFACT_PATH: &str = "examples/telegram-automations/guests/dice-story-automation/target/wasm32-unknown-unknown/debug/dice_story_automation.wasm";
const AUTOMATION_PREFIX: &str = "automation-";
const NOT_MY_CUSTOMER: &str = "sorry, you're not my customer";
const HELP_TEXT: &str = concat!(
    "/help - show commands\n",
    "/schedule_dice_story <period> - install a periodic dice-story automation for this chat\n",
    "/automations - list dice-story automations for this chat\n",
    "/automation_runs - list recent dice-story runs for this chat\n",
    "/set_period <automation_id> <period> - change an automation interval\n",
    "/pause_automation <automation_id> - deactivate an automation without deleting it\n",
    "/resume_automation <automation_id> - reactivate a paused automation\n",
    "/delete_automation <automation_id> - remove an automation\n",
    "\n",
    "Periods accept s, m, h, or d suffixes, for example 45s, 10m, or 2h."
);

type AppControlPlane = AutomationsControlPlane<
    JsonFileControlPlaneStore,
    WasmAutomationRuntime,
    TelegramEffectHandler,
    SystemClock,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct TimePeriod {
    seconds: u64,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    bot_token: Option<String>,
    bot_token_env: Option<String>,
    #[serde(default = "default_claude_executable")]
    claude_executable: String,
    claude_model: Option<String>,
    claude_system_prompt_append: Option<String>,
    claude_working_directory: Option<String>,
    #[serde(default = "default_node_executable")]
    node_executable: String,
    #[serde(default = "default_state_file")]
    state_file: String,
    #[serde(default = "default_script_path")]
    script_path: String,
    #[serde(default = "default_supervisor_poll_interval_ms")]
    supervisor_poll_interval_ms: u64,
    #[serde(default = "default_target_words")]
    target_words: usize,
    #[serde(default = "default_max_llm_attempts")]
    max_llm_attempts: u8,
    allowed_usernames: Vec<String>,
}

#[derive(Debug, Clone)]
struct ClaudeStoryBackendConfig {
    cwd: PathBuf,
    claude_executable: String,
    model: Option<String>,
    system_prompt: String,
}

#[derive(Debug, Clone, Copy)]
struct StoryGenerationConfig {
    target_words: usize,
    max_llm_attempts: u8,
}

#[derive(Debug, Clone)]
struct AppConfig {
    bot_token: String,
    claude: ClaudeStoryBackendConfig,
    node_executable: String,
    runner_path: PathBuf,
    state_path: PathBuf,
    script_path: PathBuf,
    allowed_usernames: UsernameWhitelist,
    supervisor_poll_interval: Duration,
    story: StoryGenerationConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TelegramTarget {
    chat_id: i64,
    thread_id: Option<i32>,
}

impl From<&TelegramInboundMessage> for TelegramTarget {
    fn from(message: &TelegramInboundMessage) -> Self {
        Self {
            chat_id: message.chat_id.0,
            thread_id: message.thread_id.map(|thread_id| thread_id.0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TelegramPayload {
    chat_id: i64,
    thread_id: Option<i32>,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryAutomationConfig {
    target: TelegramTarget,
    period_seconds: u64,
    target_words: usize,
    max_llm_attempts: u8,
    node_executable: String,
    runner_path: String,
    script_path: String,
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

#[derive(Debug, Clone)]
struct ClaudeStoryGenerator {
    backend: ClaudeStoryBackendConfig,
}

impl ClaudeStoryGenerator {
    fn new(backend: ClaudeStoryBackendConfig) -> Self {
        Self { backend }
    }

    async fn generate(
        &self,
        session_name: &str,
        prompt: &str,
        system_override: Option<String>,
    ) -> std::result::Result<String, String> {
        let mut config = ClaudeAgentConfig::new(
            self.backend.cwd.clone(),
            session_name.to_string(),
            self.backend.claude_executable.clone(),
        )
        .with_tools(Vec::<String>::new())
        .with_ephemeral_session()
        .with_one_shot_turns()
        .without_tool_use_hooks()
        .with_system_prompt_append(
            system_override.unwrap_or_else(|| self.backend.system_prompt.clone()),
        );

        if let Some(model) = &self.backend.model {
            config = config.with_model(model.clone());
        }

        let bridge = ClaudeAgentBridge::spawn(config)
            .map_err(|error| format!("failed to spawn Claude bridge: {error:#}"))?;
        let mut events = bridge.subscribe();
        bridge
            .send_user_text(prompt)
            .await
            .map_err(|error| format!("failed to send Claude prompt: {error:#}"))?;

        let result = self.collect_story(&mut events).await;
        close_bridge_quietly(&bridge).await;
        result
    }

    async fn collect_story(
        &self,
        events: &mut tokio::sync::broadcast::Receiver<ClaudeBridgeEvent>,
    ) -> std::result::Result<String, String> {
        let mut last_snapshot = String::new();

        loop {
            match events.recv().await {
                Ok(
                    ClaudeBridgeEvent::Ready { .. } | ClaudeBridgeEvent::ToolCallRequested { .. },
                ) => {}
                Ok(ClaudeBridgeEvent::SdkMessage { message }) => {
                    if let Some(story) = handle_claude_sdk_message(&message, &mut last_snapshot)
                        .map_err(|error| format!("failed to decode Claude message: {error:#}"))?
                    {
                        return Ok(story);
                    }
                }
                Ok(ClaudeBridgeEvent::BridgeError { message }) => {
                    return Err(format!("Claude bridge reported an error: {message}"));
                }
                Ok(ClaudeBridgeEvent::Stderr { line }) => {
                    warn!("claude bridge stderr: {line}");
                }
                Ok(ClaudeBridgeEvent::Exited { code }) => {
                    if !last_snapshot.trim().is_empty() {
                        return Ok(normalize_story_text(&last_snapshot));
                    }
                    return Err(format!(
                        "Claude bridge exited before returning a story{}",
                        format_exit_code(code)
                    ));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("claude bridge events lagged by {skipped}");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err("Claude bridge closed before returning a story".to_string());
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TelegramEffectHandler {
    client: TeloxideTelegramClient,
    story_generator: ClaudeStoryGenerator,
}

impl TelegramEffectHandler {
    fn new(client: TeloxideTelegramClient, story_generator: ClaudeStoryGenerator) -> Self {
        Self {
            client,
            story_generator,
        }
    }

    fn effect_completed(effect_id: &str, result: EffectResult, at: i64) -> AutomationEvent {
        AutomationEvent::EffectCompleted {
            effect_id: effect_id.to_string(),
            result,
            at,
        }
    }
}

#[async_trait]
impl EffectHandler for TelegramEffectHandler {
    async fn handle_effect(
        &self,
        automation_id: &str,
        revision_id: u64,
        effect: &EffectRequest,
        now: i64,
    ) -> Result<EffectHandlerOutcome, AutomationsError> {
        match &effect.kind {
            EffectKind::EmitNotification {
                channel,
                title,
                body,
                metadata: _,
            } => {
                let target = parse_telegram_channel(channel)
                    .map_err(|error| AutomationsError::Io(error.to_string()))?;
                self.client
                    .send_message(TelegramOutboundMessage {
                        chat_id: TelegramChatId(target.chat_id),
                        thread_id: target.thread_id.map(TelegramThreadId),
                        reply_to_message_id: None,
                        text: render_notification_text(title, body),
                    })
                    .await
                    .map_err(|error| AutomationsError::Io(error.to_string()))?;
                Ok(EffectHandlerOutcome::default())
            }
            EffectKind::RunCommand { program, args } => {
                let result =
                    run_command(program, args).map_or_else(EffectResult::Err, EffectResult::Ok);
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![Self::effect_completed(&effect.effect_id, result, now)],
                })
            }
            EffectKind::RunModel { prompt, system } => {
                let session_name = format!(
                    "telegram-automations-{automation_id}-{revision_id}-{}",
                    effect.effect_id
                );
                let result = self
                    .story_generator
                    .generate(&session_name, prompt, system.clone())
                    .await
                    .map_or_else(EffectResult::Err, |story| {
                        EffectResult::Ok(json!({ "text": story }))
                    });
                Ok(EffectHandlerOutcome {
                    follow_up_events: vec![Self::effect_completed(&effect.effect_id, result, now)],
                })
            }
            other => Err(AutomationsError::Io(format!(
                "telegram example does not implement effect {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
struct AutomationsBotApp {
    client: TeloxideTelegramClient,
    control_plane: AppControlPlane,
    guest_artifact_path: PathBuf,
    config: Arc<AppConfig>,
}

impl AutomationsBotApp {
    async fn handle_message(self: Arc<Self>, message: TelegramInboundMessage) -> Result<()> {
        if !self
            .config
            .allowed_usernames
            .contains(message.username.as_deref())
        {
            self.reply(&message, NOT_MY_CUSTOMER).await?;
            return Ok(());
        }

        let command = match parse_bot_command(&message.text) {
            Ok(command) => command,
            Err(error) => {
                self.reply(&message, format!("{error}\n\n{HELP_TEXT}"))
                    .await?;
                return Ok(());
            }
        };

        let response = match self.handle_command(&message, command).await {
            Ok(response) => response,
            Err(error) => format!("request failed: {error:#}"),
        };
        self.reply(&message, response).await
    }

    async fn handle_command(
        &self,
        message: &TelegramInboundMessage,
        command: BotCommand,
    ) -> Result<String> {
        let target = TelegramTarget::from(message);

        match command {
            BotCommand::Help => Ok(HELP_TEXT.to_string()),
            BotCommand::Automations => self.list_automations(&target),
            BotCommand::AutomationRuns => self.list_automation_runs(&target),
            BotCommand::ScheduleDiceStory { period } => {
                self.install_dice_story_automation(&target, period).await
            }
            BotCommand::SetPeriod {
                automation_number,
                period,
            } => self.set_period(&target, automation_number, period).await,
            BotCommand::PauseAutomation { automation_number } => {
                self.set_enabled(&target, automation_number, false).await
            }
            BotCommand::ResumeAutomation { automation_number } => {
                self.set_enabled(&target, automation_number, true).await
            }
            BotCommand::DeleteAutomation { automation_number } => {
                self.delete_automation(&target, automation_number)
            }
        }
    }

    async fn reply(&self, inbound: &TelegramInboundMessage, text: impl Into<String>) -> Result<()> {
        self.client
            .send_message(TelegramOutboundMessage {
                chat_id: inbound.chat_id,
                thread_id: inbound.thread_id,
                reply_to_message_id: Some(inbound.message_id),
                text: text.into(),
            })
            .await
            .context("failed to send telegram reply")
    }

    async fn install_dice_story_automation(
        &self,
        target: &TelegramTarget,
        period: TimePeriod,
    ) -> Result<String> {
        let automation_number = self.next_automation_number()?;
        let automation_id = automation_id_for_number(automation_number);
        let config = self.automation_config_for_target(target, period);
        let revision = self
            .control_plane
            .register_revision(&RegistrationRequest {
                automation_id: automation_id.clone(),
                artifact_path: self.guest_artifact_path.clone(),
                config: serde_json::to_value(config)
                    .context("failed to encode automation config")?,
            })
            .context("failed to register dice-story automation")?;
        self.control_plane
            .activate_revision(
                &automation_id,
                revision.revision_id,
                ActivationMode::ColdStart,
            )
            .await
            .context("failed to activate dice-story automation")?;

        let automation = self
            .automation_by_number(automation_number)?
            .context("installed automation disappeared before inspection")?;
        Ok(format!(
            "installed dice_story automation #{automation_number} every {}. next fire: {}",
            format_time_period(period),
            next_fire_label(&automation),
        ))
    }

    fn list_automations(&self, target: &TelegramTarget) -> Result<String> {
        let automations = self.automations_for_target(target)?;
        if automations.is_empty() {
            return Ok("no dice_story automations are installed for this chat".to_string());
        }

        let mut lines = vec!["automations for this chat:".to_string()];
        for (automation_number, automation) in automations {
            lines.push(format_automation_summary(automation_number, &automation));
        }
        Ok(lines.join("\n"))
    }

    fn list_automation_runs(&self, target: &TelegramTarget) -> Result<String> {
        let automations = self.automations_for_target(target)?;
        let mut runs = Vec::new();
        for (automation_number, automation) in automations {
            if let Some(state) = automation_state(&automation)? {
                for run in state.recent_runs {
                    runs.push((automation_number, run));
                }
            }
        }
        runs.sort_by_key(|(_, run)| run.fired_at);
        runs.reverse();

        if runs.is_empty() {
            return Ok("no dice_story runs have completed for this chat yet".to_string());
        }

        let mut lines = vec!["recent runs:".to_string()];
        for (automation_number, run) in runs.into_iter().take(5) {
            lines.push(format_run_summary(automation_number, &run));
        }
        Ok(lines.join("\n"))
    }

    async fn set_period(
        &self,
        target: &TelegramTarget,
        automation_number: u64,
        period: TimePeriod,
    ) -> Result<String> {
        let (automation_id, automation) =
            self.ensure_automation_in_scope(automation_number, target)?;
        let mut config =
            automation_config(&automation)?.context("automation config could not be decoded")?;
        config.period_seconds = period.seconds;

        let revision = self
            .control_plane
            .register_revision(&RegistrationRequest {
                automation_id: automation_id.clone(),
                artifact_path: self.guest_artifact_path.clone(),
                config: serde_json::to_value(config)
                    .context("failed to encode automation config")?,
            })
            .with_context(|| {
                format!("failed to register updated config for automation #{automation_number}")
            })?;
        self.control_plane
            .activate_revision(
                &automation_id,
                revision.revision_id,
                ActivationMode::PreserveState,
            )
            .await
            .with_context(|| {
                format!("failed to activate updated config for automation #{automation_number}")
            })?;

        let automation = self
            .automation_by_number(automation_number)?
            .context("automation disappeared after updating period")?;
        Ok(format!(
            "automation #{automation_number} now runs every {}. next fire: {}",
            format_time_period(period),
            next_fire_label(&automation),
        ))
    }

    async fn set_enabled(
        &self,
        target: &TelegramTarget,
        automation_number: u64,
        enabled: bool,
    ) -> Result<String> {
        let (automation_id, automation) =
            self.ensure_automation_in_scope(automation_number, target)?;
        if enabled {
            let revision = active_or_latest_revision(&automation)
                .context("paused automation has no registered revisions")?;
            self.control_plane
                .activate_revision(
                    &automation_id,
                    revision.revision_id,
                    ActivationMode::PreserveState,
                )
                .await
                .with_context(|| format!("failed to resume automation #{automation_number}"))?;
        } else if automation.active_revision_id.is_some() {
            self.control_plane
                .deactivate_automation(&automation_id)
                .with_context(|| format!("failed to pause automation #{automation_number}"))?;
        }

        let automation = self
            .automation_by_number(automation_number)?
            .context("automation disappeared after updating enabled state")?;
        Ok(format!(
            "automation #{automation_number} is now {}. next fire: {}",
            if automation.active_revision_id.is_some() {
                "enabled"
            } else {
                "paused"
            },
            next_fire_label(&automation),
        ))
    }

    fn delete_automation(&self, target: &TelegramTarget, automation_number: u64) -> Result<String> {
        let (automation_id, _) = self.ensure_automation_in_scope(automation_number, target)?;
        self.control_plane
            .delete_automation(&automation_id)
            .with_context(|| format!("failed to delete automation #{automation_number}"))?;
        Ok(format!("deleted automation #{automation_number}"))
    }

    fn ensure_automation_in_scope(
        &self,
        automation_number: u64,
        target: &TelegramTarget,
    ) -> Result<(String, ManagedAutomation)> {
        let automation_id = automation_id_for_number(automation_number);
        let automation = self
            .get_automation(&automation_id)?
            .ok_or_else(|| anyhow::anyhow!("automation #{automation_number} does not exist"))?;
        if automation_target(&automation)? != Some(target.clone()) {
            anyhow::bail!("automation #{automation_number} does not belong to this chat");
        }
        Ok((automation_id, automation))
    }

    fn automations_for_target(
        &self,
        target: &TelegramTarget,
    ) -> Result<Vec<(u64, ManagedAutomation)>> {
        let automations = self
            .control_plane
            .automations()
            .context("failed to load automations")?;
        let mut scoped_automations = Vec::new();
        for (automation_id, automation) in automations {
            let Some(automation_number) = automation_number_from_id(&automation_id) else {
                continue;
            };
            if automation_target(&automation)? == Some(target.clone()) {
                scoped_automations.push((automation_number, automation));
            }
        }
        scoped_automations.sort_by_key(|(automation_number, _)| *automation_number);
        Ok(scoped_automations)
    }

    fn automation_by_number(&self, automation_number: u64) -> Result<Option<ManagedAutomation>> {
        self.get_automation(&automation_id_for_number(automation_number))
    }

    fn get_automation(&self, automation_id: &str) -> Result<Option<ManagedAutomation>> {
        Ok(self
            .control_plane
            .automations()
            .context("failed to load automations")?
            .remove(automation_id))
    }

    fn next_automation_number(&self) -> Result<u64> {
        let automations = self
            .control_plane
            .automations()
            .context("failed to load automations")?;
        Ok(automations
            .keys()
            .filter_map(|automation_id| automation_number_from_id(automation_id))
            .max()
            .unwrap_or(0)
            + 1)
    }

    fn automation_config_for_target(
        &self,
        target: &TelegramTarget,
        period: TimePeriod,
    ) -> DiceStoryAutomationConfig {
        DiceStoryAutomationConfig {
            target: target.clone(),
            period_seconds: period.seconds,
            target_words: self.config.story.target_words,
            max_llm_attempts: self.config.story.max_llm_attempts,
            node_executable: self.config.node_executable.clone(),
            runner_path: self.config.runner_path.to_string_lossy().into_owned(),
            script_path: self.config.script_path.to_string_lossy().into_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BotCommand {
    Help,
    Automations,
    AutomationRuns,
    ScheduleDiceStory {
        period: TimePeriod,
    },
    SetPeriod {
        automation_number: u64,
        period: TimePeriod,
    },
    PauseAutomation {
        automation_number: u64,
    },
    ResumeAutomation {
        automation_number: u64,
    },
    DeleteAutomation {
        automation_number: u64,
    },
}

#[derive(Debug, Clone)]
struct UsernameWhitelist {
    usernames: Arc<HashSet<String>>,
}

impl UsernameWhitelist {
    fn from_usernames<I, S>(usernames: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            usernames: Arc::new(
                usernames
                    .into_iter()
                    .map(|username| normalize_username(username.as_ref()))
                    .filter(|username| !username.is_empty())
                    .collect(),
            ),
        }
    }

    fn contains(&self, username: Option<&str>) -> bool {
        username
            .map(normalize_username)
            .is_some_and(|username| self.usernames.contains(&username))
    }

    fn is_empty(&self) -> bool {
        self.usernames.is_empty()
    }

    fn len(&self) -> usize {
        self.usernames.len()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config_path = manifest_dir.join(CONFIG_FILE_NAME);
    let config = load_config(&config_path, &manifest_dir)?;
    let guest_artifact_path = build_guest_artifact(&manifest_dir)?;

    let client = TeloxideTelegramClient::connect(config.bot_token.clone())
        .await
        .context("failed to connect to Telegram")?;
    let control_plane = AutomationsControlPlane::new(
        JsonFileControlPlaneStore::new(config.state_path.clone()),
        TelegramEffectHandler::new(
            client.clone(),
            ClaudeStoryGenerator::new(config.claude.clone()),
        ),
        SystemClock,
    );
    let supervisor = spawn_supervisor(
        control_plane.clone(),
        SupervisorConfig {
            poll_interval: config.supervisor_poll_interval,
        },
    );
    let app = Arc::new(AutomationsBotApp {
        client: client.clone(),
        control_plane,
        guest_artifact_path,
        config: Arc::new(config),
    });

    info!(
        "telegram-automations started with {} allowed usernames, state={}, script={}, model={}",
        app.config.allowed_usernames.len(),
        app.config.state_path.display(),
        app.config.script_path.display(),
        app.config.claude.model.as_deref().unwrap_or("default"),
    );

    run_event_loop(app, supervisor).await
}

async fn run_event_loop(app: Arc<AutomationsBotApp>, supervisor: SupervisorHandle) -> Result<()> {
    loop {
        if supervisor.is_finished() {
            break;
        }

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl-c")?;
                info!("received ctrl-c, shutting down telegram-automations");
                break;
            }
            received = app.client.recv() => {
                match received {
                    Ok(Some(message)) => {
                        let app = app.clone();
                        tokio::spawn(async move {
                            if let Err(error) = app.handle_message(message).await {
                                error!("message handling failed: {error:#}");
                            }
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        error!("telegram receive failed: {error}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    supervisor
        .shutdown()
        .await
        .context("automations supervisor exited with an error")
}

fn load_config(path: &Path, manifest_dir: &Path) -> Result<AppConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let config: RawConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    let allowed_usernames = UsernameWhitelist::from_usernames(config.allowed_usernames);

    if allowed_usernames.is_empty() {
        anyhow::bail!(
            "{} must define at least one allowed username",
            path.display()
        );
    }

    Ok(AppConfig {
        bot_token: resolve_bot_token(path, config.bot_token, config.bot_token_env)?,
        claude: ClaudeStoryBackendConfig {
            cwd: resolve_claude_working_directory(path, config.claude_working_directory)?,
            claude_executable: config.claude_executable,
            model: config.claude_model.filter(|model| !model.trim().is_empty()),
            system_prompt: build_story_system_prompt(config.claude_system_prompt_append.as_deref()),
        },
        node_executable: config.node_executable,
        runner_path: manifest_dir.join("js/runner.mjs"),
        state_path: resolve_relative_path(path, &config.state_file),
        script_path: resolve_relative_path(path, &config.script_path),
        allowed_usernames,
        supervisor_poll_interval: Duration::from_millis(config.supervisor_poll_interval_ms),
        story: StoryGenerationConfig {
            target_words: config.target_words,
            max_llm_attempts: config.max_llm_attempts.max(1),
        },
    })
}

fn build_guest_artifact(manifest_dir: &Path) -> Result<PathBuf> {
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .context("failed to locate workspace root from example manifest directory")?;

    let status = std::process::Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg(GUEST_MANIFEST_PATH)
        .arg("--target")
        .arg("wasm32-unknown-unknown")
        .current_dir(workspace_root)
        .status()
        .context("failed to spawn cargo build for dice-story-automation guest")?;
    if !status.success() {
        anyhow::bail!("dice-story-automation build failed with status {status}");
    }

    let artifact_path = workspace_root.join(GUEST_ARTIFACT_PATH);
    if !artifact_path.exists() {
        anyhow::bail!(
            "guest artifact was not produced at expected path {}",
            artifact_path.display()
        );
    }
    Ok(artifact_path)
}

fn resolve_bot_token(
    path: &Path,
    bot_token: Option<String>,
    bot_token_env: Option<String>,
) -> Result<String> {
    if let Some(bot_token) = bot_token.filter(|token| !token.trim().is_empty()) {
        return Ok(bot_token.trim().to_owned());
    }

    if let Some(bot_token_env) = bot_token_env.filter(|name| !name.trim().is_empty()) {
        return std::env::var(&bot_token_env).with_context(|| {
            format!(
                "failed to read bot token from env var {} declared in {}",
                bot_token_env,
                path.display()
            )
        });
    }

    anyhow::bail!(
        "{} must define either bot_token or bot_token_env",
        path.display()
    );
}

fn resolve_claude_working_directory(path: &Path, configured: Option<String>) -> Result<PathBuf> {
    if let Some(configured) = configured.filter(|value| !value.trim().is_empty()) {
        return Ok(resolve_relative_path(path, configured.trim()));
    }

    std::env::current_dir().context("failed to resolve current working directory for Claude")
}

fn resolve_relative_path(path: &Path, configured: &str) -> PathBuf {
    let configured_path = PathBuf::from(configured);
    if configured_path.is_absolute() {
        return configured_path;
    }

    path.parent()
        .map_or_else(PathBuf::new, PathBuf::from)
        .join(configured_path)
}

fn parse_bot_command(text: &str) -> std::result::Result<BotCommand, String> {
    let mut parts = text.split_whitespace();
    let Some(raw_command) = parts.next() else {
        return Err("expected a slash command".to_string());
    };
    let command = normalize_command(raw_command);

    match command.as_str() {
        "/help" | "/start" => {
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::Help)
        }
        "/automations" => {
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::Automations)
        }
        "/automation_runs" => {
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::AutomationRuns)
        }
        "/schedule_dice_story" => {
            let period = parse_required_period(parts.next())?;
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::ScheduleDiceStory { period })
        }
        "/set_period" => {
            let automation_number = parse_required_automation_number(parts.next())?;
            let period = parse_required_period(parts.next())?;
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::SetPeriod {
                automation_number,
                period,
            })
        }
        "/pause_automation" => {
            let automation_number = parse_required_automation_number(parts.next())?;
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::PauseAutomation { automation_number })
        }
        "/resume_automation" => {
            let automation_number = parse_required_automation_number(parts.next())?;
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::ResumeAutomation { automation_number })
        }
        "/delete_automation" => {
            let automation_number = parse_required_automation_number(parts.next())?;
            ensure_no_extra_args(parts)?;
            Ok(BotCommand::DeleteAutomation { automation_number })
        }
        _ => Err(format!("unknown command {raw_command}")),
    }
}

fn normalize_command(command: &str) -> String {
    command
        .split('@')
        .next()
        .unwrap_or(command)
        .trim()
        .to_ascii_lowercase()
}

fn ensure_no_extra_args<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> std::result::Result<(), String> {
    if parts.next().is_some() {
        Err("too many command arguments".to_string())
    } else {
        Ok(())
    }
}

fn parse_required_automation_number(
    automation_number: Option<&str>,
) -> std::result::Result<u64, String> {
    let Some(automation_number) = automation_number else {
        return Err("missing automation id".to_string());
    };

    automation_number
        .parse()
        .map_err(|_| format!("invalid automation id {automation_number}"))
}

fn parse_required_period(period: Option<&str>) -> std::result::Result<TimePeriod, String> {
    let Some(period) = period else {
        return Err("missing period".to_string());
    };

    parse_period(period)
}

fn parse_period(input: &str) -> std::result::Result<TimePeriod, String> {
    let normalized = input.trim().to_ascii_lowercase();
    let digits_end = normalized.chars().take_while(char::is_ascii_digit).count();

    if digits_end == 0 {
        return Err(format!("invalid period {input}"));
    }

    let amount = normalized[..digits_end]
        .parse::<u64>()
        .map_err(|_| format!("invalid period {input}"))?;
    let multiplier = match &normalized[digits_end..] {
        "" | "s" => 1_u64,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => return Err(format!("unsupported period unit in {input}")),
    };
    let seconds = amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("period {input} overflowed"))?;

    if seconds == 0 {
        return Err("period must be greater than zero".to_string());
    }

    Ok(TimePeriod { seconds })
}

fn format_time_period(period: TimePeriod) -> String {
    if period.seconds.is_multiple_of(24 * 60 * 60) {
        format!("{}d", period.seconds / (24 * 60 * 60))
    } else if period.seconds.is_multiple_of(60 * 60) {
        format!("{}h", period.seconds / (60 * 60))
    } else if period.seconds.is_multiple_of(60) {
        format!("{}m", period.seconds / 60)
    } else {
        format!("{}s", period.seconds)
    }
}

fn format_timestamp(unix_timestamp: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_timestamp)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| unix_timestamp.to_string())
}

fn format_automation_summary(automation_number: u64, automation: &ManagedAutomation) -> String {
    let revision = active_or_latest_revision(automation);
    let period = revision
        .and_then(|revision| {
            serde_json::from_value::<DiceStoryAutomationConfig>(revision.config.clone()).ok()
        })
        .map_or_else(
            || "?".to_string(),
            |config| {
                format_time_period(TimePeriod {
                    seconds: config.period_seconds,
                })
            },
        );
    let status = automation
        .last_status
        .clone()
        .unwrap_or_else(|| "idle".to_string());

    format!(
        "#{} {} every {} next={} revision={} status={}",
        automation_number,
        if automation.active_revision_id.is_some() {
            "enabled"
        } else {
            "paused"
        },
        period,
        next_fire_label(automation),
        revision.map_or_else(
            || "-".to_string(),
            |revision| revision.revision_id.to_string()
        ),
        status,
    )
}

fn format_run_summary(automation_number: u64, run: &RunSummary) -> String {
    let mut line = format!(
        "automation #{} {} fire_at={}",
        automation_number,
        run.status,
        format_timestamp(run.fired_at),
    );

    if let Some(roll) = run.roll {
        let _ = write!(line, " roll={roll}");
    }
    if let Some(word_count) = run.word_count {
        let _ = write!(line, " words={word_count}");
    }
    let _ = write!(line, " attempts={}", run.attempts);
    if let Some(error) = &run.error {
        let _ = write!(line, " error={}", truncate(error, 72));
    }
    line
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn automation_id_for_number(automation_number: u64) -> String {
    format!("{AUTOMATION_PREFIX}{automation_number}")
}

fn automation_number_from_id(automation_id: &str) -> Option<u64> {
    automation_id
        .strip_prefix(AUTOMATION_PREFIX)
        .and_then(|suffix| suffix.parse().ok())
}

fn active_or_latest_revision(automation: &ManagedAutomation) -> Option<&RegisteredRevision> {
    automation
        .active_revision_id
        .and_then(|revision_id| automation.revisions.get(&revision_id))
        .or_else(|| automation.revisions.values().next_back())
}

fn automation_config(
    automation: &ManagedAutomation,
) -> Result<Option<DiceStoryAutomationConfig>, serde_json::Error> {
    active_or_latest_revision(automation)
        .map(|revision| serde_json::from_value(revision.config.clone()))
        .transpose()
}

fn automation_state(
    automation: &ManagedAutomation,
) -> Result<Option<DiceStoryState>, serde_json::Error> {
    automation
        .current_state
        .clone()
        .map(serde_json::from_value)
        .transpose()
}

fn automation_target(
    automation: &ManagedAutomation,
) -> Result<Option<TelegramTarget>, serde_json::Error> {
    Ok(automation_config(automation)?.map(|config| config.target))
}

fn next_fire_label(automation: &ManagedAutomation) -> String {
    if automation.active_revision_id.is_none() {
        return "not scheduled".to_string();
    }

    automation_state(automation)
        .ok()
        .flatten()
        .and_then(|state| state.next_fire_at)
        .map_or_else(|| "not scheduled".to_string(), format_timestamp)
}

fn render_notification_text(title: &str, body: &str) -> String {
    if title.trim().is_empty() {
        body.to_string()
    } else {
        format!("{title}\n\n{body}")
    }
}

fn parse_telegram_channel(channel: &str) -> Result<TelegramTarget> {
    let Some(rest) = channel.strip_prefix("telegram:") else {
        anyhow::bail!("unsupported notification channel {channel}");
    };
    let mut parts = rest.split(':');
    let chat_id = parts
        .next()
        .context("telegram channel is missing chat id")?
        .parse()
        .context("invalid telegram chat id")?;
    let thread_part = parts
        .next()
        .context("telegram channel is missing thread id")?;
    if parts.next().is_some() {
        anyhow::bail!("telegram channel has too many parts");
    }

    let thread_id = if thread_part == "-" {
        None
    } else {
        Some(thread_part.parse().context("invalid telegram thread id")?)
    };

    Ok(TelegramTarget { chat_id, thread_id })
}

fn run_command(program: &str, args: &[String]) -> std::result::Result<Value, String> {
    let output = std::process::Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("process exited with status {}", output.status)
        } else {
            stderr
        });
    }

    serde_json::from_slice(&output.stdout).or_else(|_| {
        let text = String::from_utf8(output.stdout).map_err(|error| error.to_string())?;
        Ok(json!({ "text": text }))
    })
}

fn normalize_username(username: &str) -> String {
    username.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn build_story_system_prompt(extra: Option<&str>) -> String {
    extra
        .filter(|prompt| !prompt.trim().is_empty())
        .map_or_else(
            || DEFAULT_STORY_SYSTEM_PROMPT.to_string(),
            |prompt| format!("{DEFAULT_STORY_SYSTEM_PROMPT}\n\n{prompt}"),
        )
}

fn handle_claude_sdk_message(
    message: &Value,
    last_snapshot: &mut String,
) -> Result<Option<String>> {
    match message.get("type").and_then(Value::as_str) {
        Some("stream_event") => {
            if let Some(delta) = extract_stream_text_delta(message) {
                last_snapshot.push_str(&delta);
            }
            Ok(None)
        }
        Some("assistant") => {
            if let Some(snapshot) = extract_text_snapshot(message) {
                *last_snapshot = snapshot;
            }
            Ok(None)
        }
        Some("result") => {
            let result_text = message
                .get("result")
                .and_then(Value::as_str)
                .map(normalize_story_text)
                .filter(|text| !text.is_empty())
                .or_else(|| {
                    (!last_snapshot.trim().is_empty()).then(|| normalize_story_text(last_snapshot))
                })
                .ok_or_else(|| anyhow::anyhow!("Claude returned an empty story"))?;

            if message
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let subtype = message
                    .get("subtype")
                    .and_then(Value::as_str)
                    .unwrap_or("claude_result_error");
                anyhow::bail!("{subtype}: {result_text}");
            }

            Ok(Some(result_text))
        }
        _ => Ok(None),
    }
}

fn extract_stream_text_delta(message: &Value) -> Option<String> {
    let event = message.get("event")?;
    if event.get("type").and_then(Value::as_str) != Some("content_block_delta") {
        return None;
    }

    let delta = event.get("delta")?;
    if delta.get("type").and_then(Value::as_str) != Some("text_delta") {
        return None;
    }

    delta.get("text").and_then(Value::as_str).map(str::to_owned)
}

fn extract_text_snapshot(message: &Value) -> Option<String> {
    if let Some(content) = message
        .get("message")
        .and_then(|value| value.get("content"))
        .or_else(|| message.get("content"))
    {
        let text = flatten_content(content);
        if !text.is_empty() {
            return Some(text);
        }
    }

    None
}

fn flatten_content(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items.iter().map(flatten_content_item).collect(),
        _ => String::new(),
    }
}

fn flatten_content_item(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Object(object) => object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

fn normalize_story_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn close_bridge_quietly(bridge: &ClaudeAgentBridge) {
    if let Err(error) = bridge.close().await {
        warn!("failed to close Claude bridge cleanly: {error:#}");
    }
}

fn format_exit_code(code: Option<i32>) -> String {
    code.map(|value| format!(" with code {value}"))
        .unwrap_or_default()
}

fn default_claude_executable() -> String {
    "claude".to_string()
}

fn default_node_executable() -> String {
    "node".to_string()
}

fn default_state_file() -> String {
    "telegram-automations-state.json".to_string()
}

fn default_script_path() -> String {
    "scripts/dice_story.js".to_string()
}

const fn default_supervisor_poll_interval_ms() -> u64 {
    500
}

const fn default_target_words() -> usize {
    50
}

const fn default_max_llm_attempts() -> u8 {
    3
}

#[cfg(test)]
mod tests {
    use super::{BotCommand, TimePeriod, normalize_command, parse_bot_command, parse_period};

    #[test]
    fn parse_period_accepts_seconds_minutes_hours_and_days() {
        assert_eq!(parse_period("45").unwrap(), TimePeriod { seconds: 45 });
        assert_eq!(parse_period("45s").unwrap(), TimePeriod { seconds: 45 });
        assert_eq!(parse_period("10m").unwrap(), TimePeriod { seconds: 600 });
        assert_eq!(parse_period("2h").unwrap(), TimePeriod { seconds: 7_200 });
        assert_eq!(parse_period("3d").unwrap(), TimePeriod { seconds: 259_200 });
    }

    #[test]
    fn parse_command_strips_bot_mentions() {
        let command = parse_bot_command("/schedule_dice_story@automationsbot 15m").unwrap();
        assert_eq!(
            command,
            BotCommand::ScheduleDiceStory {
                period: TimePeriod { seconds: 900 }
            }
        );
    }

    #[test]
    fn normalize_command_lowercases_the_name() {
        assert_eq!(normalize_command("/HELP@AutomationsBot"), "/help");
    }
}
