use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use mango_telegram::{
    TelegramChatId, TelegramClient, TelegramInboxSender, TelegramSurface, TelegramThreadId,
    TeloxideTelegramClient, telegram_inbox,
};
use serde::Deserialize;
use telegram_chat::{ClaudeConversationConfig, UsernameWhitelist, spawn_chat_runtime};
use tokio::time::sleep;
use tracing::{error, info};

const CONFIG_FILE_NAME: &str = "telegram-chat.toml";

#[derive(Debug, Deserialize)]
struct RawConfig {
    bot_token: Option<String>,
    bot_token_env: Option<String>,
    #[serde(default = "default_claude_executable")]
    claude_executable: String,
    claude_model: Option<String>,
    claude_system_prompt_append: Option<String>,
    claude_working_directory: Option<String>,
    allowed_usernames: Vec<String>,
    #[serde(default = "default_bus_capacity")]
    bus_capacity: usize,
    #[serde(default = "default_inbox_capacity")]
    inbox_capacity: usize,
}

#[derive(Debug)]
struct AppConfig {
    bot_token: String,
    claude: ClaudeConversationConfig,
    allowed_usernames: UsernameWhitelist,
    bus_capacity: usize,
    inbox_capacity: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(CONFIG_FILE_NAME);
    let config = load_config(&config_path)?;
    let client = TeloxideTelegramClient::connect(config.bot_token.clone())
        .await
        .context("failed to connect to Telegram")?;

    info!(
        "telegram-chat started with {} allowed usernames using Claude executable {}",
        config.allowed_usernames.len(),
        config.claude.claude_executable
    );

    let mut sessions: HashMap<(TelegramChatId, Option<TelegramThreadId>), TelegramInboxSender> =
        HashMap::new();

    loop {
        let message = match client.recv().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(error) => {
                error!("telegram receive failed: {error}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let key = (message.chat_id, message.thread_id);

        if let Some(sender) = sessions.get(&key)
            && sender.send(message.clone()).await.is_ok()
        {
            continue;
        }

        let (sender, inbox) = telegram_inbox(config.inbox_capacity);
        drop(spawn_chat_runtime(
            client.clone(),
            TelegramSurface::from(&message),
            inbox,
            config.bus_capacity,
            config.allowed_usernames.clone(),
            &config.claude,
        ));

        sender
            .send(message)
            .await
            .context("newly spawned session closed before first message")?;
        sessions.insert(key, sender);
    }

    Ok(())
}

fn load_config(path: &Path) -> Result<AppConfig> {
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
        claude: ClaudeConversationConfig {
            cwd: resolve_claude_working_directory(path, config.claude_working_directory)?,
            claude_executable: config.claude_executable,
            model: config.claude_model.filter(|model| !model.trim().is_empty()),
            system_prompt_append: config
                .claude_system_prompt_append
                .filter(|prompt| !prompt.trim().is_empty()),
        },
        allowed_usernames,
        bus_capacity: config.bus_capacity,
        inbox_capacity: config.inbox_capacity,
    })
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
        let configured_path = PathBuf::from(configured);
        if configured_path.is_absolute() {
            return Ok(configured_path);
        }

        let base_dir = path.parent().map_or_else(PathBuf::new, PathBuf::from);
        return Ok(base_dir.join(configured_path));
    }

    std::env::current_dir().context("failed to resolve current working directory for Claude")
}

const fn default_bus_capacity() -> usize {
    1024
}

const fn default_inbox_capacity() -> usize {
    32
}

fn default_claude_executable() -> String {
    "claude".to_string()
}
