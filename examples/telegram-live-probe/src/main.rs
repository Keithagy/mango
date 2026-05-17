use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use grammers_client::{
    Client, SenderPool, SignInError,
    client::{UpdateStream, UpdatesConfiguration},
    peer::Peer,
    session::storages::SqliteSession,
    update::Update,
};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::{info, warn};

const CONFIG_FILE_NAME: &str = "telegram-live-probe.toml";

#[derive(Debug, Deserialize)]
struct RawConfig {
    demo: RawDemoConfig,
    probe_user: RawProbeUserConfig,
    verification: Option<RawVerificationConfig>,
}

#[derive(Debug, Deserialize)]
struct RawDemoConfig {
    config_path: String,
    chat_id: Option<i64>,
    thread_id: Option<i32>,
    bot_username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawProbeUserConfig {
    api_id: i32,
    api_hash: Option<String>,
    api_hash_env: Option<String>,
    session_file: String,
    phone: Option<String>,
    phone_env: Option<String>,
    login_code: Option<String>,
    login_code_env: Option<String>,
    password: Option<String>,
    password_env: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawVerificationConfig {
    schedule_period: Option<String>,
    reply_timeout_secs: Option<u64>,
    notification_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct Config {
    demo: DemoConfig,
    probe_user: ProbeUserConfig,
    verification: VerificationConfig,
}

#[derive(Debug, Clone)]
struct DemoConfig {
    config_path: PathBuf,
    chat_id: Option<i64>,
    thread_id: Option<i32>,
    bot_username: String,
}

#[derive(Debug, Clone)]
struct ProbeUserConfig {
    api_id: i32,
    api_hash: Option<String>,
    session_file: PathBuf,
    phone: Option<String>,
    login_code: Option<String>,
    login_code_env: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Clone)]
struct VerificationConfig {
    schedule_period: String,
    reply_timeout: Duration,
    notification_timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct RawTelegramAutomationsConfig {
    bot_token: Option<String>,
    bot_token_env: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramBotGetMeResponse {
    ok: bool,
    result: Option<TelegramBotIdentity>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramBotIdentity {
    username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedMessage {
    text: String,
}

struct ProbeConnection {
    client: Client,
    updates: UpdateStream,
    shutdown_handle: grammers_client::sender::SenderPoolFatHandle,
    runner_task: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let config_path = probe_config_path();
    let config = load_config(&config_path).await?;
    Box::pin(run_probe(config)).await
}

async fn run_probe(config: Config) -> Result<()> {
    if config.demo.thread_id.is_some() {
        anyhow::bail!(
            "forum-topic verification is not implemented yet for the MTProto probe; expected thread_id = null"
        );
    }

    let mut connection = connect_probe_user(&config.probe_user).await?;
    let target = resolve_target_peer(&connection.client, &config.demo).await?;
    info!(
        "telegram-live-probe connected: target={} bot_username={}",
        target.name().unwrap_or("<unknown>"),
        config.demo.bot_username
    );

    Box::pin(verify_telegram_automations(
        &config,
        &mut connection,
        &target,
    ))
    .await?;
    shutdown(connection).await;
    Ok(())
}

async fn load_config(path: &Path) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read probe config {}", path.display()))?;
    let raw: RawConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse probe config {}", path.display()))?;

    let demo_config_path = resolve_relative_path(path, &raw.demo.config_path);
    let bot_token = resolve_demo_bot_token(&demo_config_path)?;
    let bot_username = if let Some(bot_username) = raw
        .demo
        .bot_username
        .filter(|username| !username.trim().is_empty())
    {
        bot_username.trim().trim_start_matches('@').to_string()
    } else {
        fetch_bot_username(&bot_token, &demo_config_path).await?
    };

    let probe_user = ProbeUserConfig {
        api_id: raw.probe_user.api_id,
        api_hash: resolve_optional_secret(
            raw.probe_user.api_hash,
            raw.probe_user.api_hash_env.as_deref(),
        )?,
        session_file: resolve_relative_path(path, &raw.probe_user.session_file),
        phone: resolve_optional_secret(raw.probe_user.phone, raw.probe_user.phone_env.as_deref())?,
        login_code: resolve_optional_secret(
            raw.probe_user.login_code,
            raw.probe_user.login_code_env.as_deref(),
        )?,
        login_code_env: raw.probe_user.login_code_env,
        password: resolve_optional_secret(
            raw.probe_user.password,
            raw.probe_user.password_env.as_deref(),
        )?,
    };

    Ok(Config {
        demo: DemoConfig {
            config_path: demo_config_path,
            chat_id: raw.demo.chat_id,
            thread_id: raw.demo.thread_id,
            bot_username,
        },
        probe_user,
        verification: VerificationConfig {
            schedule_period: raw
                .verification
                .as_ref()
                .and_then(|verification| verification.schedule_period.clone())
                .filter(|period| !period.trim().is_empty())
                .unwrap_or_else(|| "15s".to_string()),
            reply_timeout: Duration::from_secs(
                raw.verification
                    .as_ref()
                    .and_then(|verification| verification.reply_timeout_secs)
                    .unwrap_or(20),
            ),
            notification_timeout: Duration::from_secs(
                raw.verification
                    .as_ref()
                    .and_then(|verification| verification.notification_timeout_secs)
                    .unwrap_or(120),
            ),
        },
    })
}

async fn fetch_bot_username(bot_token: &str, demo_config_path: &Path) -> Result<String> {
    let url = format!("https://api.telegram.org/bot{bot_token}/getMe");
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call Telegram getMe")?;
    let status = response.status();
    let payload = response
        .json::<TelegramBotGetMeResponse>()
        .await
        .context("failed to decode Telegram getMe response")?;
    if !status.is_success() || !payload.ok {
        anyhow::bail!(
            "failed to derive bot username from {}: {}",
            demo_config_path.display(),
            payload
                .description
                .unwrap_or_else(|| format!("http status {status}"))
        );
    }

    payload
        .result
        .and_then(|result| result.username)
        .filter(|username| !username.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("Telegram getMe did not return a bot username"))
}

fn resolve_demo_bot_token(demo_config_path: &Path) -> Result<String> {
    let raw = fs::read_to_string(demo_config_path)
        .with_context(|| format!("failed to read demo config {}", demo_config_path.display()))?;
    let raw: RawTelegramAutomationsConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse demo config {}", demo_config_path.display()))?;
    resolve_bot_token(
        demo_config_path,
        raw.bot_token,
        raw.bot_token_env.as_deref(),
    )
}

fn resolve_bot_token(
    path: &Path,
    bot_token: Option<String>,
    bot_token_env: Option<&str>,
) -> Result<String> {
    if let Some(bot_token) = bot_token.filter(|token| !token.trim().is_empty()) {
        return Ok(bot_token.trim().to_owned());
    }

    if let Some(env_name) = bot_token_env.filter(|name| !name.trim().is_empty()) {
        return std::env::var(env_name).with_context(|| {
            format!(
                "failed to read bot token from env var {env_name} declared in {}",
                path.display()
            )
        });
    }

    anyhow::bail!(
        "{} must define either bot_token or bot_token_env",
        path.display()
    );
}

fn resolve_optional_secret(
    value: Option<String>,
    env_name: Option<&str>,
) -> Result<Option<String>> {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        return Ok(Some(value.trim().to_owned()));
    }

    if let Some(env_name) = env_name.filter(|name| !name.trim().is_empty()) {
        return std::env::var(env_name)
            .map(Some)
            .with_context(|| format!("failed to read secret from env var {env_name}"));
    }

    Ok(None)
}

async fn connect_probe_user(config: &ProbeUserConfig) -> Result<ProbeConnection> {
    let session = Arc::new(
        SqliteSession::open(&config.session_file)
            .await
            .with_context(|| {
                format!(
                    "failed to open probe session file {}",
                    config.session_file.display()
                )
            })?,
    );
    let SenderPool {
        runner,
        updates,
        handle,
    } = SenderPool::new(Arc::clone(&session), config.api_id);
    let client = Client::new(handle.clone());
    let runner_task = tokio::spawn(runner.run());

    if !client
        .is_authorized()
        .await
        .context("failed to query Telegram authorization state")?
    {
        authorize_probe_user(&client, config).await?;
    }

    // Prime peer cache so future peer resolution and updates work with the persisted session.
    let mut dialogs = client.iter_dialogs();
    while dialogs
        .next()
        .await
        .context("failed to iterate Telegram dialogs")?
        .is_some()
    {}

    let updates = client
        .stream_updates(
            updates,
            UpdatesConfiguration {
                catch_up: false,
                ..Default::default()
            },
        )
        .await;

    Ok(ProbeConnection {
        client,
        updates,
        shutdown_handle: handle,
        runner_task,
    })
}

async fn authorize_probe_user(client: &Client, config: &ProbeUserConfig) -> Result<()> {
    let phone = match &config.phone {
        Some(phone) => phone.clone(),
        None => prompt("Enter the probe user's phone number in international format: ")?,
    };
    let api_hash = config
        .api_hash
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "api_hash is required for first-time probe login when the session file is not yet authorized"
            )
        })?;

    let login_token = client
        .request_login_code(&phone, api_hash)
        .await
        .context("failed to request Telegram login code")?;
    let code = match &config.login_code {
        Some(code) => code.clone(),
        None => match config
            .login_code_env
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            Some(env_name) => std::env::var(env_name).with_context(|| {
                format!("failed to read Telegram login code from env var {env_name}")
            })?,
            None => prompt("Enter the login code sent by Telegram: ")?,
        },
    };

    match client.sign_in(&login_token, code.trim()).await {
        Ok(_) => {
            info!("probe user signed in successfully");
            Ok(())
        }
        Err(SignInError::PasswordRequired(password_token)) => {
            let password = match &config.password {
                Some(password) => password.clone(),
                None => prompt("Enter the Telegram 2FA password for the probe user: ")?,
            };
            client
                .check_password(password_token, password.trim())
                .await
                .context("failed to complete Telegram 2FA login")?;
            info!("probe user signed in with 2FA");
            Ok(())
        }
        Err(error) => Err(anyhow::anyhow!(
            "failed to sign into Telegram probe user: {error}"
        )),
    }
}

async fn resolve_target_peer(client: &Client, demo: &DemoConfig) -> Result<Peer> {
    if let Some(chat_id) = demo.chat_id
        && chat_id < 0
    {
        return find_dialog_by_bot_api_chat_id(client, chat_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "could not find a Telegram dialog with bot-api chat id {chat_id}; make sure the probe user has joined that chat"
                )
            });
    }

    client
        .resolve_username(&demo.bot_username)
        .await
        .with_context(|| format!("failed to resolve @{}", demo.bot_username))?
        .ok_or_else(|| anyhow::anyhow!("Telegram username @{} was not found", demo.bot_username))
}

async fn find_dialog_by_bot_api_chat_id(client: &Client, chat_id: i64) -> Result<Option<Peer>> {
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs
        .next()
        .await
        .context("failed to iterate Telegram dialogs while resolving target chat")?
    {
        let peer = dialog.peer();
        if peer.id().bot_api_dialog_id() == chat_id {
            return Ok(Some(peer.clone()));
        }
    }
    Ok(None)
}

async fn verify_telegram_automations(
    config: &Config,
    connection: &mut ProbeConnection,
    target: &Peer,
) -> Result<()> {
    info!(
        "running live telegram-automations verification against {} using demo config {}",
        target.name().unwrap_or("<unknown>"),
        config.demo.config_path.display()
    );

    let start_reply = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/start",
        config.verification.reply_timeout,
        |text| !text.trim().is_empty(),
    ))
    .await?;
    info!("start reply: {}", start_reply.text);

    Box::pin(cleanup_existing_automations(config, connection, target)).await?;

    let empty_listing = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/automations",
        config.verification.reply_timeout,
        |text| text.contains("no dice_story automations are installed for this chat"),
    ))
    .await?;
    info!("empty listing verified: {}", empty_listing.text);

    let install_reply = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        &format!(
            "/schedule_dice_story {}",
            config.verification.schedule_period
        ),
        config.verification.reply_timeout,
        |text| text.contains("installed dice_story automation #1"),
    ))
    .await?;
    info!("install reply: {}", install_reply.text);

    let listing = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/automations",
        config.verification.reply_timeout,
        |text| {
            text.contains("automations for this chat:")
                && text.contains("#1 enabled")
                && text.contains("status=armed")
        },
    ))
    .await?;
    info!("listing verified: {}", listing.text);

    let notification = boxed(wait_for_message(
        connection,
        target,
        &config.demo.bot_username,
        config.verification.notification_timeout,
        |text| text.contains("Dice Story 1"),
    ))
    .await?;
    info!("notification received: {}", notification.text);

    Box::pin(verify_run_history(config, connection, target)).await?;

    Box::pin(verify_lifecycle(config, connection, target)).await?;
    info!("live telegram-automations verification completed successfully");
    Ok(())
}

async fn verify_run_history(
    config: &Config,
    connection: &mut ProbeConnection,
    target: &Peer,
) -> Result<()> {
    let runs_first = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/automation_runs",
        config.verification.reply_timeout,
        |text| text.contains("recent runs:") && text.contains("automation #1"),
    ))
    .await?;
    info!("first runs listing: {}", runs_first.text);

    let runs_second = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/automation_runs",
        config.verification.reply_timeout,
        |text| text.contains("recent runs:") && text.contains("automation #1"),
    ))
    .await?;
    info!("second runs listing: {}", runs_second.text);
    Ok(())
}

async fn verify_lifecycle(
    config: &Config,
    connection: &mut ProbeConnection,
    target: &Peer,
) -> Result<()> {
    let pause_reply = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/pause_automation 1",
        config.verification.reply_timeout,
        |text| text.contains("automation #1 is now paused"),
    ))
    .await?;
    info!("pause reply: {}", pause_reply.text);

    let resume_reply = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/resume_automation 1",
        config.verification.reply_timeout,
        |text| text.contains("automation #1 is now enabled"),
    ))
    .await?;
    info!("resume reply: {}", resume_reply.text);

    let delete_reply = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/delete_automation 1",
        config.verification.reply_timeout,
        |text| text.trim() == "deleted automation #1",
    ))
    .await?;
    info!("delete reply: {}", delete_reply.text);
    Ok(())
}

async fn cleanup_existing_automations(
    config: &Config,
    connection: &mut ProbeConnection,
    target: &Peer,
) -> Result<()> {
    let listing = boxed(send_command_and_wait_for_text(
        config,
        connection,
        target,
        "/automations",
        config.verification.reply_timeout,
        |text| {
            text.contains("automations for this chat:")
                || text.contains("no dice_story automations are installed for this chat")
        },
    ))
    .await?;
    let mut automation_numbers = parse_automation_numbers(&listing.text);
    automation_numbers.sort_unstable();
    automation_numbers.reverse();
    for automation_number in automation_numbers {
        let reply = boxed(send_command_and_wait_for_text(
            config,
            connection,
            target,
            &format!("/delete_automation {automation_number}"),
            config.verification.reply_timeout,
            |text| text.trim() == format!("deleted automation #{automation_number}"),
        ))
        .await?;
        info!(
            "deleted pre-existing automation #{automation_number}: {}",
            reply.text
        );
    }
    Ok(())
}

async fn send_command_and_wait_for_text(
    config: &Config,
    connection: &mut ProbeConnection,
    target: &Peer,
    command: &str,
    timeout: Duration,
    predicate: impl Fn(&str) -> bool,
) -> Result<ObservedMessage> {
    info!("sending probe command: {command}");
    boxed(send_text(&connection.client, target, command)).await?;
    let message = wait_for_message(
        connection,
        target,
        &config.demo.bot_username,
        timeout,
        predicate,
    )
    .await?;
    Ok(message)
}

async fn send_text(client: &Client, target: &Peer, text: &str) -> Result<()> {
    let peer_ref = target.to_ref().await.ok_or_else(|| {
        anyhow::anyhow!("resolved target peer is missing a usable peer reference")
    })?;
    boxed(client.send_message(peer_ref, text))
        .await
        .with_context(|| format!("failed to send Telegram probe message: {text}"))?;
    Ok(())
}

async fn wait_for_message(
    connection: &mut ProbeConnection,
    target: &Peer,
    bot_username: &str,
    timeout: Duration,
    predicate: impl Fn(&str) -> bool,
) -> Result<ObservedMessage> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| anyhow::anyhow!("timed out waiting for a matching Telegram message"))?;
        let update = tokio::time::timeout(remaining, connection.updates.next())
            .await
            .context("timed out waiting for a Telegram update")?
            .context("failed to receive Telegram update")?;
        if let Some(message) = observed_message(&update, target, bot_username) {
            if predicate(&message.text) {
                return Ok(message);
            }
            warn!("ignoring non-matching bot message: {}", message.text);
        }
    }
}

fn observed_message(update: &Update, target: &Peer, bot_username: &str) -> Option<ObservedMessage> {
    let Update::NewMessage(message) = update else {
        return None;
    };
    let sender_username = message.sender().and_then(|sender| sender.username());
    info!(
        "probe observed message: outgoing={} peer_id={} sender_id={:?} sender_username={:?} text={}",
        message.outgoing(),
        message.peer_id(),
        message.sender_id(),
        sender_username,
        message.text()
    );
    if message.outgoing() || message.peer_id() != target.id() {
        return None;
    }

    let sender_matches = message
        .sender_id()
        .is_some_and(|sender_id| sender_id == target.id())
        || sender_username.is_some_and(|username| username.eq_ignore_ascii_case(bot_username));
    if !sender_matches {
        return None;
    }

    Some(ObservedMessage {
        text: message.text().to_string(),
    })
}

async fn shutdown(connection: ProbeConnection) {
    connection.updates.sync_update_state().await;
    connection.shutdown_handle.quit();
    let _ = connection.runner_task.await;
}

fn parse_automation_numbers(text: &str) -> Vec<u64> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let number = trimmed.strip_prefix('#')?;
            number
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .collect()
}

fn probe_config_path() -> PathBuf {
    match std::env::args().nth(1) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(CONFIG_FILE_NAME),
    }
}

fn resolve_relative_path(config_path: &Path, value: &str) -> PathBuf {
    let value_path = PathBuf::from(value);
    if value_path.is_absolute() {
        return value_path;
    }

    config_path
        .parent()
        .map_or_else(PathBuf::new, PathBuf::from)
        .join(value_path)
}

fn prompt(label: &str) -> Result<String> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(label.as_bytes())?;
    stdout.flush()?;

    let stdin = io::stdin();
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    Ok(line.trim().to_string())
}

fn boxed<F>(future: F) -> Pin<Box<F>>
where
    F: std::future::Future,
{
    Box::pin(future)
}

#[cfg(test)]
mod tests {
    use super::parse_automation_numbers;

    #[test]
    fn parse_automation_numbers_reads_listing_lines() {
        let text = "automations for this chat:\n#12 enabled every 15s next=...\n#7 paused";
        assert_eq!(parse_automation_numbers(text), vec![12, 7]);
    }

    #[test]
    fn parse_automation_numbers_ignores_non_listing_text() {
        assert!(parse_automation_numbers("no dice_story automations are installed").is_empty());
    }
}
