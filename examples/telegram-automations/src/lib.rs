use std::{
    collections::{BTreeMap, HashSet},
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
    JsonFileControlPlaneStore, ManagedAutomation, PocketUniverse,
    RegisteredRevision, RegistrationRequest, SupervisorConfig, SupervisorHandle, SystemClock,
    WasmAutomationRuntime, spawn_supervisor,
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

#[async_trait]
trait AutomationPlane: Clone + Send + Sync + 'static {
    fn register_revision(
        &self,
        request: &RegistrationRequest,
    ) -> Result<RegisteredRevision, AutomationsError>;

    async fn activate_revision(
        &self,
        automation_id: &str,
        revision_id: u64,
        mode: ActivationMode,
    ) -> Result<(), AutomationsError>;

    fn deactivate_automation(&self, automation_id: &str) -> Result<(), AutomationsError>;

    fn delete_automation(&self, automation_id: &str) -> Result<(), AutomationsError>;

    fn automations(&self) -> Result<BTreeMap<String, ManagedAutomation>, AutomationsError>;
}

#[async_trait]
impl AutomationPlane for AppControlPlane {
    fn register_revision(
        &self,
        request: &RegistrationRequest,
    ) -> Result<RegisteredRevision, AutomationsError> {
        AutomationsControlPlane::register_revision(self, request)
    }

    async fn activate_revision(
        &self,
        automation_id: &str,
        revision_id: u64,
        mode: ActivationMode,
    ) -> Result<(), AutomationsError> {
        AutomationsControlPlane::activate_revision(self, automation_id, revision_id, mode).await
    }

    fn deactivate_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        AutomationsControlPlane::deactivate_automation(self, automation_id)
    }

    fn delete_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        AutomationsControlPlane::delete_automation(self, automation_id)
    }

    fn automations(&self) -> Result<BTreeMap<String, ManagedAutomation>, AutomationsError> {
        AutomationsControlPlane::automations(self)
    }

}

#[async_trait]
impl AutomationPlane for PocketUniverse<WasmAutomationRuntime, TelegramEffectHandler> {
    fn register_revision(
        &self,
        request: &RegistrationRequest,
    ) -> Result<RegisteredRevision, AutomationsError> {
        PocketUniverse::register_revision(self, request)
    }

    async fn activate_revision(
        &self,
        automation_id: &str,
        revision_id: u64,
        mode: ActivationMode,
    ) -> Result<(), AutomationsError> {
        PocketUniverse::activate_revision(self, automation_id, revision_id, mode).await
    }

    fn deactivate_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        PocketUniverse::deactivate_automation(self, automation_id)
    }

    fn delete_automation(&self, automation_id: &str) -> Result<(), AutomationsError> {
        PocketUniverse::delete_automation(self, automation_id)
    }

    fn automations(&self) -> Result<BTreeMap<String, ManagedAutomation>, AutomationsError> {
        PocketUniverse::automations(self)
    }

}

#[derive(Debug, thiserror::Error)]
enum AppTelegramClientError {
    #[error("{0}")]
    Live(#[from] mango_telegram::TeloxideTelegramError),
    #[cfg(test)]
    #[error("{0}")]
    Test(#[from] mango_telegram::TestTelegramError),
}

#[derive(Debug, Clone)]
enum AppTelegramClient {
    Live(TeloxideTelegramClient),
    #[cfg(test)]
    Test(mango_telegram::TestTelegramClient),
}

#[async_trait]
impl TelegramClient for AppTelegramClient {
    type Error = AppTelegramClientError;

    async fn recv(&self) -> Result<Option<TelegramInboundMessage>, Self::Error> {
        match self {
            Self::Live(client) => client.recv().await.map_err(AppTelegramClientError::from),
            #[cfg(test)]
            Self::Test(client) => client.recv().await.map_err(AppTelegramClientError::from),
        }
    }

    async fn send_message(&self, message: TelegramOutboundMessage) -> Result<(), Self::Error> {
        match self {
            Self::Live(client) => client
                .send_message(message)
                .await
                .map_err(AppTelegramClientError::from),
            #[cfg(test)]
            Self::Test(client) => client
                .send_message(message)
                .await
                .map_err(AppTelegramClientError::from),
        }
    }
}

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
    node_executable: String,
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
struct RunSummary {
    #[serde(alias = "fired_at")]
    nominal_fire_at: i64,
    status: String,
    #[serde(default)]
    run_id: Option<u64>,
    roll: Option<u8>,
    #[serde(default)]
    attempts: Option<u8>,
    word_count: Option<usize>,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiceStoryState {
    next_fire_at: Option<i64>,
    #[serde(default)]
    recent_runs: Vec<RunSummary>,
}

#[derive(Debug, Clone)]
struct ClaudeStoryGenerator {
    backend: ClaudeStoryBackendConfig,
}

#[async_trait]
trait StoryGenerator: Send + Sync {
    async fn generate(
        &self,
        session_name: &str,
        prompt: &str,
        system_override: Option<String>,
    ) -> std::result::Result<String, String>;
}

#[async_trait]
trait CommandRunner: Send + Sync {
    async fn run_command(
        &self,
        program: &str,
        args: &[String],
    ) -> std::result::Result<Value, String>;
}

impl ClaudeStoryGenerator {
    fn new(backend: ClaudeStoryBackendConfig) -> Self {
        Self { backend }
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

#[async_trait]
impl StoryGenerator for ClaudeStoryGenerator {
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
        .with_node_executable(self.backend.node_executable.clone())
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
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessCommandRunner;

#[async_trait]
impl CommandRunner for ProcessCommandRunner {
    async fn run_command(
        &self,
        program: &str,
        args: &[String],
    ) -> std::result::Result<Value, String> {
        run_command(program, args)
    }
}

#[derive(Clone)]
struct TelegramEffectHandler {
    client: AppTelegramClient,
    story_generator: Arc<dyn StoryGenerator>,
    command_runner: Arc<dyn CommandRunner>,
}

impl TelegramEffectHandler {
    fn new(
        client: AppTelegramClient,
        story_generator: Arc<dyn StoryGenerator>,
        command_runner: Arc<dyn CommandRunner>,
    ) -> Self {
        Self {
            client,
            story_generator,
            command_runner,
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
                metadata,
            } => {
                let target = notification_target(channel, metadata)
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
                let result = self
                    .command_runner
                    .run_command(program, args)
                    .await
                    .map_or_else(EffectResult::Err, EffectResult::Ok);
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

struct AutomationsBotApp<P> {
    client: AppTelegramClient,
    control_plane: P,
    guest_artifact_path: PathBuf,
    config: Arc<AppConfig>,
}

impl<P> AutomationsBotApp<P>
where
    P: AutomationPlane,
{
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
        runs.sort_by_key(|(_, run)| run.nominal_fire_at);
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

/// Run the shipped `telegram-automations` example binary.
///
/// # Errors
///
/// Returns an error when the example config cannot be loaded, the Wasm guest
/// cannot be built, Telegram connectivity fails, or the supervisor loop exits
/// with an error.
pub async fn run_binary() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config_path = manifest_dir.join(CONFIG_FILE_NAME);
    let config = load_config(&config_path, &manifest_dir)?;
    let guest_artifact_path = build_guest_artifact(&manifest_dir)?;

    let client = AppTelegramClient::Live(
        TeloxideTelegramClient::connect(config.bot_token.clone())
            .await
            .context("failed to connect to Telegram")?,
    );
    let control_plane = AutomationsControlPlane::new(
        JsonFileControlPlaneStore::new(config.state_path.clone()),
        TelegramEffectHandler::new(
            client.clone(),
            Arc::new(ClaudeStoryGenerator::new(config.claude.clone())),
            Arc::new(ProcessCommandRunner),
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

async fn run_event_loop<P>(
    app: Arc<AutomationsBotApp<P>>,
    supervisor: SupervisorHandle,
) -> Result<()>
where
    P: AutomationPlane,
{
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
            node_executable: config.node_executable.clone(),
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
        format_timestamp(run.nominal_fire_at),
    );

    if let Some(run_id) = run.run_id {
        let _ = write!(line, " run={run_id}");
    }
    if let Some(roll) = run.roll {
        let _ = write!(line, " roll={roll}");
    }
    if let Some(word_count) = run.word_count {
        let _ = write!(line, " words={word_count}");
    }
    if let Some(attempts) = run.attempts {
        let _ = write!(line, " attempts={attempts}");
    }
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

fn notification_target(channel: &str, metadata: &Value) -> Result<TelegramTarget> {
    if channel == "telegram" {
        return serde_json::from_value(metadata.clone())
            .context("telegram notification metadata did not contain a target");
    }

    parse_telegram_channel(channel)
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
    use std::fs;

    use super::{
        BotCommand, DiceStoryState, RunSummary, TelegramTarget, TimePeriod,
        default_supervisor_poll_interval_ms, format_run_summary, load_config, normalize_command,
        notification_target, parse_bot_command, parse_period,
    };
    use serde_json::json;
    use tempfile::tempdir;

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

    #[test]
    fn notification_target_accepts_metadata_for_bare_telegram_channel() {
        let target = notification_target(
            "telegram",
            &json!({
                "chat_id": 42,
                "thread_id": 7,
            }),
        )
        .unwrap();

        assert_eq!(
            target,
            TelegramTarget {
                chat_id: 42,
                thread_id: Some(7),
            }
        );
    }

    #[test]
    fn dice_story_state_decodes_the_current_guest_run_history_schema() {
        let state: DiceStoryState = serde_json::from_value(json!({
            "next_fire_at": 1234,
            "active_run": null,
            "recent_runs": [
                {
                    "run_id": 3,
                    "nominal_fire_at": 1111,
                    "status": "succeeded",
                    "roll": 4,
                    "word_count": 50,
                    "error": null
                }
            ]
        }))
        .unwrap();

        assert_eq!(state.next_fire_at, Some(1234));
        assert_eq!(state.recent_runs.len(), 1);
        assert_eq!(
            state.recent_runs[0],
            RunSummary {
                nominal_fire_at: 1111,
                status: "succeeded".to_string(),
                run_id: Some(3),
                roll: Some(4),
                attempts: None,
                word_count: Some(50),
                error: None,
            }
        );
        assert!(format_run_summary(1, &state.recent_runs[0]).contains("run=3"));
    }

    #[test]
    fn load_config_threads_node_runtime_into_both_host_command_paths() {
        let tempdir = tempdir().unwrap();
        let config_path = tempdir.path().join("telegram-automations.toml");
        fs::write(
            &config_path,
            format!(
                concat!(
                    "bot_token = \"test-token\"\n",
                    "claude_executable = \"/opt/bin/claude\"\n",
                    "claude_model = \"haiku\"\n",
                    "node_executable = \"/opt/bin/bun\"\n",
                    "state_file = \"state.json\"\n",
                    "script_path = \"scripts/dice_story.js\"\n",
                    "supervisor_poll_interval_ms = {}\n",
                    "target_words = 50\n",
                    "max_llm_attempts = 3\n",
                    "allowed_usernames = [\"keithisms\"]\n"
                ),
                default_supervisor_poll_interval_ms()
            ),
        )
        .unwrap();

        let config = load_config(&config_path, tempdir.path()).unwrap();

        assert_eq!(config.node_executable, "/opt/bin/bun");
        assert_eq!(config.claude.node_executable, "/opt/bin/bun");
        assert_eq!(config.claude.claude_executable, "/opt/bin/claude");
    }
}

#[cfg(test)]
mod bdd_scenarios {
    use std::{
        collections::VecDeque,
        path::{Path, PathBuf},
        sync::{Arc, OnceLock},
        time::Duration,
    };

    use mango_automations_bdd::{
        AutomationsScenarioWorld, Scenario, ScenarioFailure, TimeDrivenScenarioWorld,
    };
    use mango_telegram::{
        TelegramChatId, TelegramOutboundMessage, TelegramThreadId, TestTelegramActor,
        TestTelegramClientConfig, TestTelegramDriver, telegram_test_client_with_config,
    };
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::*;

    const START_TIME: i64 = 1_775_004_000;
    const WAIT_FOR_REPLY: Duration = Duration::from_millis(100);
    type TestAutomationPlane = PocketUniverse<WasmAutomationRuntime, TelegramEffectHandler>;

    fn summarize_trace(trace: &mango_automation_control::TraceRecord) -> String {
        match &trace.event {
            mango_automation_control::TraceEvent::RevisionRegistered {
                automation_id,
                revision_id,
                ..
            } => format!("revision_registered {automation_id} rev={revision_id}"),
            mango_automation_control::TraceEvent::WakeupDispatched {
                automation_id,
                wakeup_id,
                at,
                ..
            } => format!("wakeup_dispatched {automation_id} {wakeup_id} at {at}"),
            mango_automation_control::TraceEvent::EffectRequested {
                automation_id,
                effect_id,
                effect_kind,
                ..
            } => format!("effect_requested {automation_id} {effect_id} {effect_kind}"),
            mango_automation_control::TraceEvent::EffectHandled {
                automation_id,
                effect_id,
                follow_up_events,
                ..
            } => {
                format!("effect_handled {automation_id} {effect_id} follow_ups={follow_up_events}")
            }
            event => format!("{event:?}"),
        }
    }

    fn repeated_word_story(word: &str, word_count: usize) -> String {
        std::iter::repeat_n(word, word_count)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn guest_artifact_path() -> Result<PathBuf, AutomationsError> {
        static ARTIFACT: OnceLock<std::result::Result<PathBuf, String>> = OnceLock::new();
        ARTIFACT
            .get_or_init(|| {
                build_guest_artifact(Path::new(env!("CARGO_MANIFEST_DIR")))
                    .map_err(|error| error.to_string())
            })
            .clone()
            .map_err(AutomationsError::Io)
    }

    #[derive(Debug, Clone, Default)]
    struct ScriptedStoryGenerator {
        prompts: Arc<Mutex<Vec<String>>>,
        responses: Arc<Mutex<VecDeque<std::result::Result<String, String>>>>,
    }

    impl ScriptedStoryGenerator {
        async fn push_story(&self, story: impl Into<String>) {
            self.responses.lock().await.push_back(Ok(story.into()));
        }

        async fn prompts(&self) -> Vec<String> {
            self.prompts.lock().await.clone()
        }
    }

    #[async_trait]
    impl StoryGenerator for ScriptedStoryGenerator {
        async fn generate(
            &self,
            _session_name: &str,
            prompt: &str,
            _system_override: Option<String>,
        ) -> std::result::Result<String, String> {
            self.prompts.lock().await.push(prompt.to_string());
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Err("no scripted story response queued".to_string()))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CommandInvocation {
        program: String,
        args: Vec<String>,
    }

    #[derive(Debug, Clone, Default)]
    struct DeterministicDiceCommandRunner {
        scripted_results: Arc<Mutex<VecDeque<std::result::Result<Value, String>>>>,
        invocations: Arc<Mutex<Vec<CommandInvocation>>>,
    }

    impl DeterministicDiceCommandRunner {
        async fn push_result(&self, result: std::result::Result<Value, String>) {
            self.scripted_results.lock().await.push_back(result);
        }

        async fn invocations(&self) -> Vec<CommandInvocation> {
            self.invocations.lock().await.clone()
        }
    }

    #[async_trait]
    impl CommandRunner for DeterministicDiceCommandRunner {
        async fn run_command(
            &self,
            program: &str,
            args: &[String],
        ) -> std::result::Result<Value, String> {
            self.invocations.lock().await.push(CommandInvocation {
                program: program.to_string(),
                args: args.to_vec(),
            });

            if let Some(result) = self.scripted_results.lock().await.pop_front() {
                return result;
            }

            let context = args
                .get(2)
                .ok_or_else(|| {
                    "expected runner to receive context json as the third arg".to_string()
                })
                .and_then(|value| {
                    serde_json::from_str::<Value>(value)
                        .map_err(|error| format!("failed to decode runner context: {error}"))
                })?;
            let seed = context
                .get("seed")
                .and_then(Value::as_u64)
                .ok_or_else(|| "runner context was missing a numeric seed".to_string())?;
            let roll = (seed % 6) + 1;
            Ok(json!({ "roll": roll }))
        }
    }

    struct AppWorld {
        _tempdir: TempDir,
        universe: TestAutomationPlane,
        driver: TestTelegramDriver,
        story_generator: ScriptedStoryGenerator,
        command_runner: DeterministicDiceCommandRunner,
        app: Arc<AutomationsBotApp<TestAutomationPlane>>,
        customer: TestTelegramActor,
        intruder: TestTelegramActor,
    }

    impl AppWorld {
        fn new() -> Result<Self, AutomationsError> {
            let tempdir =
                tempfile::tempdir().map_err(|error| AutomationsError::Io(error.to_string()))?;
            let state_path = tempdir.path().join("telegram-automations-state.json");
            let guest_artifact_path = guest_artifact_path()?;
            let (driver, client) = telegram_test_client_with_config(TestTelegramClientConfig {
                capacity: 32,
                first_message_id: 10_000,
            });
            let customer = TestTelegramActor::new(
                TelegramChatId(42),
                Some(TelegramThreadId(7)),
                Some("trusted_customer".to_string()),
                "Trusted Customer",
            );
            let intruder = TestTelegramActor::new(
                TelegramChatId(42),
                Some(TelegramThreadId(7)),
                Some("intruder".to_string()),
                "Intruder",
            );
            let story_generator = ScriptedStoryGenerator::default();
            let command_runner = DeterministicDiceCommandRunner::default();
            let app_config = AppConfig {
                bot_token: "test-bot-token".to_string(),
                claude: ClaudeStoryBackendConfig {
                    cwd: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                    node_executable: "/opt/bin/bun".to_string(),
                    claude_executable: "claude".to_string(),
                    model: Some("haiku".to_string()),
                    system_prompt: build_story_system_prompt(None),
                },
                node_executable: "/opt/bin/bun".to_string(),
                runner_path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("js/runner.mjs"),
                state_path: state_path.clone(),
                script_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("scripts/dice_story.js"),
                allowed_usernames: UsernameWhitelist::from_usernames(["trusted_customer"]),
                supervisor_poll_interval: Duration::from_millis(10),
                story: StoryGenerationConfig {
                    target_words: 5,
                    max_llm_attempts: 3,
                },
            };
            let app_client = AppTelegramClient::Test(client.clone());
            let universe = PocketUniverse::new(
                START_TIME,
                WasmAutomationRuntime::new(),
                TelegramEffectHandler::new(
                    app_client.clone(),
                    Arc::new(story_generator.clone()),
                    Arc::new(command_runner.clone()),
                ),
            );
            let app = Arc::new(AutomationsBotApp {
                client: app_client,
                control_plane: universe.clone(),
                guest_artifact_path,
                config: Arc::new(app_config),
            });

            Ok(Self {
                _tempdir: tempdir,
                universe,
                driver,
                story_generator,
                command_runner,
                app,
                customer,
                intruder,
            })
        }

        async fn send_and_process(
            &self,
            actor: &TestTelegramActor,
            text: impl Into<String>,
        ) -> Result<(), AutomationsError> {
            self.driver
                .send_text(actor, text)
                .await
                .map_err(|error| AutomationsError::Io(error.to_string()))?;
            let Some(message) = self
                .app
                .client
                .recv()
                .await
                .map_err(|error| AutomationsError::Io(error.to_string()))?
            else {
                return Err(AutomationsError::Io(
                    "test telegram client closed before delivering inbound".to_string(),
                ));
            };

            self.app
                .clone()
                .handle_message(message)
                .await
                .map_err(|error| AutomationsError::Io(error.to_string()))
        }

        async fn next_outbound(&self) -> Result<TelegramOutboundMessage, AutomationsError> {
            self.driver
                .recv_outbound(WAIT_FOR_REPLY)
                .await
                .map_err(|error| AutomationsError::Io(error.to_string()))
        }

        async fn queue_story(&self, story: impl Into<String>) {
            self.story_generator.push_story(story).await;
        }

        async fn prompts(&self) -> Vec<String> {
            self.story_generator.prompts().await
        }

        async fn queue_command_result(&self, result: std::result::Result<Value, String>) {
            self.command_runner.push_result(result).await;
        }

        async fn command_invocations(&self) -> Vec<CommandInvocation> {
            self.command_runner.invocations().await
        }

        fn automation_count(&self) -> Result<usize, AutomationsError> {
            self.universe.automations().map(|automations| automations.len())
        }
    }

    struct PocketWorld {
        _tempdir: TempDir,
        universe: TestAutomationPlane,
        driver: TestTelegramDriver,
        story_generator: ScriptedStoryGenerator,
        command_runner: DeterministicDiceCommandRunner,
        guest_artifact_path: PathBuf,
        target: TelegramTarget,
    }

    impl PocketWorld {
        fn new() -> Result<Self, AutomationsError> {
            let tempdir =
                tempfile::tempdir().map_err(|error| AutomationsError::Io(error.to_string()))?;
            let guest_artifact_path = guest_artifact_path()?;
            let (driver, client) = telegram_test_client_with_config(TestTelegramClientConfig {
                capacity: 32,
                first_message_id: 20_000,
            });
            let story_generator = ScriptedStoryGenerator::default();
            let command_runner = DeterministicDiceCommandRunner::default();
            let target = TelegramTarget {
                chat_id: 42,
                thread_id: Some(7),
            };
            let universe = mango_automation_control::PocketUniverse::new(
                START_TIME,
                WasmAutomationRuntime::new(),
                TelegramEffectHandler::new(
                    AppTelegramClient::Test(client),
                    Arc::new(story_generator.clone()),
                    Arc::new(command_runner.clone()),
                ),
            );

            Ok(Self {
                _tempdir: tempdir,
                universe,
                driver,
                story_generator,
                command_runner,
                guest_artifact_path,
                target,
            })
        }

        async fn install(&self, period: TimePeriod) -> Result<u64, AutomationsError> {
            let revision = self.universe.register_revision(&RegistrationRequest {
                automation_id: automation_id_for_number(1),
                artifact_path: self.guest_artifact_path.clone(),
                config: serde_json::to_value(self.automation_config(period))
                    .map_err(|error| AutomationsError::Io(error.to_string()))?,
            })?;
            self.universe
                .activate_revision("automation-1", revision.revision_id, ActivationMode::ColdStart)
                .await?;
            Ok(revision.revision_id)
        }

        fn automation_config(&self, period: TimePeriod) -> DiceStoryAutomationConfig {
            DiceStoryAutomationConfig {
                target: self.target.clone(),
                period_seconds: period.seconds,
                target_words: 50,
                max_llm_attempts: 3,
                node_executable: "/opt/bin/bun".to_string(),
                runner_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("js/runner.mjs")
                    .to_string_lossy()
                    .into_owned(),
                script_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("scripts/dice_story.js")
                    .to_string_lossy()
                    .into_owned(),
            }
        }

        async fn next_outbound(&self) -> Result<TelegramOutboundMessage, AutomationsError> {
            self.driver
                .recv_outbound(WAIT_FOR_REPLY)
                .await
                .map_err(|error| AutomationsError::Io(error.to_string()))
        }

        async fn queue_story(&self, story: impl Into<String>) {
            self.story_generator.push_story(story).await;
        }

        async fn queue_command_result(&self, result: std::result::Result<Value, String>) {
            self.command_runner.push_result(result).await;
        }

        async fn command_invocations(&self) -> Vec<CommandInvocation> {
            self.command_runner.invocations().await
        }

        async fn prompts(&self) -> Vec<String> {
            self.story_generator.prompts().await
        }

        fn automation(&self) -> Result<ManagedAutomation, AutomationsError> {
            self.universe
                .automations()?
                .remove("automation-1")
                .ok_or_else(|| AutomationsError::Io("automation-1 was not found".to_string()))
        }

        fn dice_story_state(&self) -> Result<DiceStoryState, AutomationsError> {
            automation_state(&self.automation()?)
                .map_err(|error| AutomationsError::Io(error.to_string()))?
                .ok_or_else(|| AutomationsError::Io("automation state was missing".to_string()))
        }

        fn delete(&self) -> Result<(), AutomationsError> {
            self.universe.delete_automation("automation-1")
        }
    }

    fn pocket_scenario(name: &str) -> Scenario<PocketWorld> {
        Scenario::new(name, PocketWorld::new().expect("world should initialize"))
            .with_recent_trace_limit(14)
            .with_trace_summary(summarize_trace)
    }

    async fn install_pocket_demo(
        scenario: &mut Scenario<PocketWorld>,
        period: TimePeriod,
    ) -> Result<(), ScenarioFailure> {
        scenario
            .when("a dice-story automation is registered and activated in the pocket universe")
            .perform(|world| {
                Box::pin(async move {
                    world.queue_command_result(Ok(json!({ "roll": 5 }))).await;
                    world.queue_story(repeated_word_story("coins", 50)).await;
                    let revision_id = world.install(period).await?;
                    assert_eq!(revision_id, 1);
                    Ok(())
                })
            })
            .await
    }

    async fn expect_first_pocket_wakeup(
        scenario: &mut Scenario<PocketWorld>,
        at: i64,
    ) -> Result<(), ScenarioFailure> {
        scenario
            .then("the activation schedules the first wakeup")
            .expect_eventually(
                "an activation wakeup schedule trace",
                Duration::from_millis(50),
                move |trace| {
                    matches!(
                        trace.event,
                        mango_automation_control::TraceEvent::WakeupScheduled {
                            ref wakeup_id,
                            at: trace_at,
                            ..
                        } if wakeup_id == "scheduled" && trace_at == at
                    )
                },
            )
            .await
    }

    async fn reconcile_first_pocket_run(
        scenario: &mut Scenario<PocketWorld>,
    ) -> Result<(), ScenarioFailure> {
        scenario
            .when("the first wakeup becomes due and the pocket universe reconciles it")
            .advance_time_by_and_settle(15)
            .await?;

        scenario
            .then("the run requests the configured command runtime and completes with a notification")
            .expect_eventually(
                "a notification effect for run 1",
                Duration::from_millis(50),
                |trace| {
                    matches!(
                        trace.event,
                        mango_automation_control::TraceEvent::EffectHandled {
                            ref effect_id,
                            ..
                        } if effect_id == "notify-1"
                    )
                },
            )
            .await
    }

    async fn assert_successful_pocket_run(scenario: &mut Scenario<PocketWorld>) {
        let automation = scenario
            .world()
            .automation()
            .expect("automation should exist after reconciliation");
        assert_eq!(automation.active_revision_id, Some(1));
        assert_eq!(automation.last_status.as_deref(), Some("succeeded"));

        let notification = scenario
            .world()
            .next_outbound()
            .await
            .expect("notification should be captured");
        assert_eq!(notification.chat_id, TelegramChatId(42));
        assert_eq!(notification.thread_id, Some(TelegramThreadId(7)));
        assert!(notification.text.contains("Dice Story 1"));

        let command_invocations = scenario.world().command_invocations().await;
        assert_eq!(command_invocations.len(), 1);
        assert_eq!(command_invocations[0].program, "/opt/bin/bun");
        assert!(
            command_invocations[0].args[0].ends_with("js/runner.mjs"),
            "expected runner path in {:?}",
            command_invocations[0].args
        );
        assert!(
            command_invocations[0].args[1].ends_with("scripts/dice_story.js"),
            "expected script path in {:?}",
            command_invocations[0].args
        );
        assert_eq!(scenario.world().prompts().await.len(), 1);

        let state = scenario
            .world()
            .dice_story_state()
            .expect("state should decode");
        assert_eq!(state.recent_runs.len(), 1);
        assert_eq!(state.recent_runs[0].status, "succeeded");
        assert_eq!(state.recent_runs[0].run_id, Some(1));
        assert_eq!(state.recent_runs[0].roll, Some(5));
        assert_eq!(state.recent_runs[0].word_count, Some(50));
        assert_eq!(state.next_fire_at, Some(START_TIME + 30));
    }

    async fn pause_resume_delete_pocket_demo(
        scenario: &mut Scenario<PocketWorld>,
    ) -> Result<(), ScenarioFailure> {
        scenario
            .when("the automation is paused, resumed, and deleted inside the pocket universe")
            .perform(|world| {
                Box::pin(async move {
                    world.universe.deactivate_automation("automation-1")?;
                    world
                        .universe
                        .activate_revision("automation-1", 1, ActivationMode::PreserveState)
                        .await?;
                    world.delete()?;
                    Ok(())
                })
            })
            .await
    }

    #[async_trait]
    impl AutomationsScenarioWorld for AppWorld {
        async fn traces(
            &mut self,
        ) -> Result<Vec<mango_automation_control::TraceRecord>, AutomationsError> {
            self.universe.traces()
        }
    }

    #[async_trait]
    impl AutomationsScenarioWorld for PocketWorld {
        async fn traces(
            &mut self,
        ) -> Result<Vec<mango_automation_control::TraceRecord>, AutomationsError> {
            self.universe.traces()
        }
    }

    #[async_trait]
    impl TimeDrivenScenarioWorld for AppWorld {
        fn advance_time_by(&mut self, seconds: i64) {
            self.universe.advance_time_by(seconds);
        }

        async fn settle_automations(&mut self) -> Result<(), AutomationsError> {
            self.universe.settle().await.map(|_| ())
        }
    }

    #[async_trait]
    impl TimeDrivenScenarioWorld for PocketWorld {
        fn advance_time_by(&mut self, seconds: i64) {
            self.universe.advance_time_by(seconds);
        }

        async fn settle_automations(&mut self) -> Result<(), AutomationsError> {
            self.universe.settle().await.map(|_| ())
        }
    }

    #[tokio::test]
    async fn unauthorized_sender_is_rejected_without_installing_any_automations()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "unauthorized telegram sender is rejected without mutating control-plane state",
            AppWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(8)
        .with_trace_summary(summarize_trace);

        scenario
            .when("an intruder sends a message")
            .perform(|world| {
                Box::pin(async move { world.send_and_process(&world.intruder, "hi").await })
            })
            .await?;

        let outbound = scenario
            .world()
            .next_outbound()
            .await
            .expect("reply should be captured");
        assert_eq!(outbound.chat_id, TelegramChatId(42));
        assert_eq!(outbound.thread_id, Some(TelegramThreadId(7)));
        assert_eq!(outbound.text, NOT_MY_CUSTOMER);
        assert_eq!(
            scenario
                .world()
                .automation_count()
                .expect("count should load"),
            0
        );

        Ok(())
    }

    #[tokio::test]
    async fn scheduling_and_listing_automations_uses_the_telegram_command_surface()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "telegram commands expose the same start and listing flow used in the live probe",
            AppWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(10)
        .with_trace_summary(summarize_trace);

        scenario
            .when("a customer asks for help before any automations exist")
            .perform(|world| {
                Box::pin(async move { world.send_and_process(&world.customer, "/start").await })
            })
            .await?;

        let help_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("help reply should be captured");
        assert_eq!(help_reply.text, HELP_TEXT);

        scenario
            .when("the customer asks for automations before any exist")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/automations")
                        .await
                })
            })
            .await?;

        let empty_listing = scenario
            .world()
            .next_outbound()
            .await
            .expect("empty listing should be captured");
        assert_eq!(
            empty_listing.text,
            "no dice_story automations are installed for this chat"
        );

        scenario
            .when("the customer schedules a dice story automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/schedule_dice_story 15s")
                        .await
                })
            })
            .await?;

        let install_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("install reply should be captured");
        assert!(
            install_reply
                .text
                .contains("installed dice_story automation #1 every 15s")
        );

        scenario
            .when("the customer asks for the installed automations")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/automations")
                        .await
                })
            })
            .await?;

        let listing = scenario
            .world()
            .next_outbound()
            .await
            .expect("listing reply should be captured");
        assert!(listing.text.contains("automations for this chat:"));
        assert!(listing.text.contains("#1 enabled every 15s"));
        assert!(listing.text.contains("status=armed"));

        Ok(())
    }

    #[tokio::test]
    async fn due_runs_deliver_notifications_and_automation_runs_can_be_queried_repeatedly()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "a due dice-story automation completes and its run history stays queryable",
            AppWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(12)
        .with_trace_summary(summarize_trace);

        scenario
            .when("the customer schedules an automation and the model needs one retry")
            .perform(|world| {
                Box::pin(async move {
                    world.queue_story(repeated_word_story("almost", 4)).await;
                    world.queue_story(repeated_word_story("victory", 5)).await;
                    world
                        .send_and_process(&world.customer, "/schedule_dice_story 1s")
                        .await
                })
            })
            .await?;
        let _ = scenario
            .world()
            .next_outbound()
            .await
            .expect("install reply");

        scenario
            .when("the automation becomes due and is reconciled")
            .advance_time_by_and_settle(1)
            .await?;

        let notification = scenario
            .world()
            .next_outbound()
            .await
            .expect("notification should be captured");
        assert_eq!(notification.chat_id, TelegramChatId(42));
        assert_eq!(notification.thread_id, Some(TelegramThreadId(7)));
        assert!(notification.text.contains("Dice Story 1"));
        assert_eq!(scenario.world().prompts().await.len(), 2);
        let command_invocations = scenario.world().command_invocations().await;
        assert_eq!(command_invocations.len(), 1);
        assert_eq!(command_invocations[0].program, "/opt/bin/bun");
        assert!(
            command_invocations[0].args[0].ends_with("js/runner.mjs"),
            "expected runner path in {:?}",
            command_invocations[0].args
        );
        assert!(
            command_invocations[0].args[1].ends_with("scripts/dice_story.js"),
            "expected script path in {:?}",
            command_invocations[0].args
        );

        scenario
            .when("the customer asks for recent runs twice")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/automation_runs")
                        .await?;
                    world
                        .send_and_process(&world.customer, "/automation_runs")
                        .await?;
                    Ok(())
                })
            })
            .await?;

        let first_runs_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("first runs reply should be captured");
        let second_runs_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("second runs reply should be captured");
        assert!(first_runs_reply.text.contains("recent runs:"));
        assert!(first_runs_reply.text.contains("automation #1 succeeded"));
        assert!(first_runs_reply.text.contains("run=1"));
        assert!(second_runs_reply.text.contains("automation #1 succeeded"));

        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_commands_retime_pause_resume_and_delete_the_automation()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "the telegram command surface manages automation lifecycle transitions",
            AppWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(14)
        .with_trace_summary(summarize_trace);

        scenario
            .when("the customer schedules an automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/schedule_dice_story 30s")
                        .await
                })
            })
            .await?;
        let _ = scenario
            .world()
            .next_outbound()
            .await
            .expect("install reply");

        scenario
            .when("the customer changes the period")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/set_period 1 2m")
                        .await
                })
            })
            .await?;
        let period_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("period reply should be captured");
        assert!(
            period_reply
                .text
                .contains("automation #1 now runs every 2m")
        );

        scenario
            .when("the customer pauses the automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/pause_automation 1")
                        .await
                })
            })
            .await?;
        let pause_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("pause reply should be captured");
        assert!(pause_reply.text.contains("automation #1 is now paused"));

        scenario
            .when("the customer resumes the automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/resume_automation 1")
                        .await
                })
            })
            .await?;
        let resume_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("resume reply should be captured");
        assert!(resume_reply.text.contains("automation #1 is now enabled"));

        scenario
            .when("the customer deletes the automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/delete_automation 1")
                        .await
                })
            })
            .await?;
        let delete_reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("delete reply should be captured");
        assert_eq!(delete_reply.text, "deleted automation #1");
        assert_eq!(
            scenario
                .world()
                .automation_count()
                .expect("count should load"),
            0
        );

        Ok(())
    }

    #[tokio::test]
    async fn failed_external_tool_runs_are_reported_via_automation_runs()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "a failing external dice tool is preserved in recent automation runs",
            AppWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(12)
        .with_trace_summary(summarize_trace);

        scenario
            .when("the customer schedules an automation")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/schedule_dice_story 1s")
                        .await
                })
            })
            .await?;
        let _ = scenario
            .world()
            .next_outbound()
            .await
            .expect("install reply");

        scenario
            .when("the external dice tool fails when the automation fires")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .queue_command_result(Err("node executable missing".to_string()))
                        .await;
                    world.advance_time_by_and_settle(1).await
                })
            })
            .await?;

        scenario
            .when("the customer asks for recent runs")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .send_and_process(&world.customer, "/automation_runs")
                        .await
                })
            })
            .await?;
        let reply = scenario
            .world()
            .next_outbound()
            .await
            .expect("runs reply should be captured");
        assert!(reply.text.contains("recent runs:"));
        assert!(reply.text.contains("automation #1 failed"));
        assert!(reply.text.contains("roll failed: node executable missing"));

        Ok(())
    }

    #[tokio::test]
    async fn pocket_universe_replays_the_live_happy_path_for_dice_story()
    -> Result<(), ScenarioFailure> {
        let mut scenario = pocket_scenario(
            "the pocket universe can replay the live dice-story control-plane flow",
        );

        install_pocket_demo(&mut scenario, TimePeriod { seconds: 15 }).await?;
        expect_first_pocket_wakeup(&mut scenario, START_TIME + 15).await?;
        reconcile_first_pocket_run(&mut scenario).await?;
        assert_successful_pocket_run(&mut scenario).await;
        pause_resume_delete_pocket_demo(&mut scenario).await?;

        assert!(
            scenario
                .world()
                .universe
                .automations()
                .expect("snapshot should load")
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn pocket_universe_preserves_failed_external_runs_for_inspection()
    -> Result<(), ScenarioFailure> {
        let mut scenario = Scenario::new(
            "the pocket universe preserves failed command runs in automation state",
            PocketWorld::new().expect("world should initialize"),
        )
        .with_recent_trace_limit(12)
        .with_trace_summary(summarize_trace);

        scenario
            .when("the guest is activated and its external dice command fails")
            .perform(|world| {
                Box::pin(async move {
                    world
                        .queue_command_result(Err("node executable missing".to_string()))
                        .await;
                    world.install(TimePeriod { seconds: 1 }).await?;
                    world.advance_time_by_and_settle(1).await
                })
            })
            .await?;

        let state = scenario
            .world()
            .dice_story_state()
            .expect("state should decode");
        assert_eq!(state.recent_runs.len(), 1);
        assert_eq!(state.recent_runs[0].status, "failed");
        assert_eq!(
            state.recent_runs[0].error.as_deref(),
            Some("roll failed: node executable missing")
        );

        Ok(())
    }
}
