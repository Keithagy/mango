use std::{env, time::Duration};

use async_trait::async_trait;
use reqwest::Client;
use teloxide::{
    Bot,
    payloads::{GetUpdatesSetters, SendMessageSetters},
    prelude::Requester,
    requests::Request,
    types::{AllowedUpdate, ChatId, Message, MessageId, ReplyParameters, ThreadId, UpdateKind},
};
use tokio::{
    sync::{Mutex, mpsc},
    time::sleep,
};

const TELEGRAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TELEGRAM_HTTP_TIMEOUT: Duration = Duration::from_secs(35);
const TELEGRAM_POLL_TIMEOUT_SECONDS: u32 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TelegramChatId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TelegramThreadId(pub i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TelegramMessageId(pub i32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramInboundMessage {
    pub chat_id: TelegramChatId,
    pub thread_id: Option<TelegramThreadId>,
    pub message_id: TelegramMessageId,
    pub username: Option<String>,
    pub display_name: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramOutboundMessage {
    pub chat_id: TelegramChatId,
    pub thread_id: Option<TelegramThreadId>,
    pub reply_to_message_id: Option<TelegramMessageId>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramSurface {
    pub chat_id: TelegramChatId,
    pub thread_id: Option<TelegramThreadId>,
    pub username: Option<String>,
    pub display_name: String,
}

impl From<&TelegramInboundMessage> for TelegramSurface {
    fn from(value: &TelegramInboundMessage) -> Self {
        Self {
            chat_id: value.chat_id,
            thread_id: value.thread_id,
            username: value.username.clone(),
            display_name: value.display_name.clone(),
        }
    }
}

impl From<TelegramInboundMessage> for TelegramSurface {
    fn from(value: TelegramInboundMessage) -> Self {
        Self::from(&value)
    }
}

pub trait TelegramSessionSurface {
    fn telegram_chat_id(&self) -> TelegramChatId;

    fn telegram_thread_id(&self) -> Option<TelegramThreadId> {
        None
    }
}

impl TelegramSessionSurface for TelegramSurface {
    fn telegram_chat_id(&self) -> TelegramChatId {
        self.chat_id
    }

    fn telegram_thread_id(&self) -> Option<TelegramThreadId> {
        self.thread_id
    }
}

#[async_trait]
pub trait TelegramClient: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn recv(&self) -> Result<Option<TelegramInboundMessage>, Self::Error>;
    async fn send_message(&self, message: TelegramOutboundMessage) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum TeloxideTelegramError {
    #[error("telegram client configuration failed: {0}")]
    Configuration(String),
    #[error("telegram startup validation failed: {0}")]
    Connect(#[source] teloxide::RequestError),
    #[error("telegram polling failed: {0}")]
    Polling(#[source] teloxide::RequestError),
    #[error("telegram send failed: {0}")]
    Send(#[source] teloxide::RequestError),
}

#[derive(Debug, Clone)]
pub struct TeloxideTelegramClient {
    bot: Bot,
    inbox: std::sync::Arc<
        Mutex<mpsc::Receiver<Result<TelegramInboundMessage, TeloxideTelegramError>>>,
    >,
}

impl TeloxideTelegramClient {
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self::from_bot(build_bot(normalize_bot_token(token)))
    }

    /// Build a client from a token and validate it against Telegram before
    /// starting the polling loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the token is empty, malformed, or Telegram rejects
    /// the initial `getMe` validation request.
    pub async fn connect(token: impl Into<String>) -> Result<Self, TeloxideTelegramError> {
        let token = normalize_bot_token(token);
        validate_bot_token(&token)?;

        let bot = build_bot(token);
        bot.get_me()
            .send()
            .await
            .map_err(TeloxideTelegramError::Connect)?;

        Ok(Self::from_bot(bot))
    }

    /// Build a client from a token stored in an environment variable.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment variable is not present or cannot
    /// be read.
    pub fn from_env_var(name: &str) -> Result<Self, env::VarError> {
        env::var(name).map(Self::new)
    }

    #[must_use]
    pub fn from_bot(bot: Bot) -> Self {
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(run_polling(bot.clone(), tx));
        Self {
            bot,
            inbox: std::sync::Arc::new(Mutex::new(rx)),
        }
    }
}

#[async_trait]
impl TelegramClient for TeloxideTelegramClient {
    type Error = TeloxideTelegramError;

    async fn recv(&self) -> Result<Option<TelegramInboundMessage>, Self::Error> {
        match self.inbox.lock().await.recv().await {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }

    async fn send_message(&self, message: TelegramOutboundMessage) -> Result<(), Self::Error> {
        let mut request = self
            .bot
            .send_message(ChatId(message.chat_id.0), message.text);

        if let Some(thread_id) = message.thread_id {
            request = request.message_thread_id(ThreadId(MessageId(thread_id.0)));
        }

        if let Some(reply_to_message_id) = message.reply_to_message_id {
            request =
                request.reply_parameters(ReplyParameters::new(MessageId(reply_to_message_id.0)));
        }

        request
            .await
            .map(|_| ())
            .map_err(TeloxideTelegramError::Send)
    }
}

async fn run_polling(
    bot: Bot,
    tx: mpsc::Sender<Result<TelegramInboundMessage, TeloxideTelegramError>>,
) {
    let mut offset = 0_i32;

    loop {
        let result = bot
            .get_updates()
            .offset(offset)
            .limit(100)
            .timeout(TELEGRAM_POLL_TIMEOUT_SECONDS)
            .allowed_updates(vec![AllowedUpdate::Message])
            .send()
            .await;

        match result {
            Ok(updates) => {
                for update in updates {
                    offset = next_offset(update.id.0);
                    if let Some(message) = inbound_message(update.kind)
                        && tx.send(Ok(message)).await.is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                if tx
                    .send(Err(TeloxideTelegramError::Polling(error)))
                    .await
                    .is_err()
                {
                    return;
                }

                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn inbound_message(kind: UpdateKind) -> Option<TelegramInboundMessage> {
    match kind {
        UpdateKind::Message(message) => inbound_from_message(&message),
        _ => None,
    }
}

fn inbound_from_message(message: &Message) -> Option<TelegramInboundMessage> {
    let text = message.text()?.to_string();
    let username = message.from.as_ref().and_then(|user| user.username.clone());
    let display_name = message
        .from
        .as_ref()
        .map(display_name)
        .or_else(|| username.clone())
        .unwrap_or_else(|| "telegram user".to_string());

    Some(TelegramInboundMessage {
        chat_id: TelegramChatId(message.chat.id.0),
        thread_id: message
            .thread_id
            .map(|thread_id| TelegramThreadId(thread_id.0.0)),
        message_id: TelegramMessageId(message.id.0),
        username,
        display_name,
        text,
    })
}

fn display_name(user: &teloxide::types::User) -> String {
    match user.last_name.as_deref() {
        Some(last_name) if !last_name.is_empty() => {
            format!("{} {last_name}", user.first_name)
        }
        _ => user.first_name.clone(),
    }
}

fn next_offset(update_id: u32) -> i32 {
    i32::try_from(update_id)
        .unwrap_or(i32::MAX)
        .saturating_add(1)
}

fn build_bot(token: String) -> Bot {
    let client = Client::builder()
        .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .timeout(TELEGRAM_HTTP_TIMEOUT)
        .tcp_nodelay(true)
        .build()
        .expect("telegram reqwest client creation failed");

    Bot::with_client(token, client)
}

fn normalize_bot_token(token: impl Into<String>) -> String {
    token.into().trim().to_owned()
}

fn validate_bot_token(token: &str) -> Result<(), TeloxideTelegramError> {
    if token.is_empty() {
        return Err(TeloxideTelegramError::Configuration(
            "bot token is empty".to_string(),
        ));
    }

    let Some((bot_id, secret)) = token.split_once(':') else {
        return Err(TeloxideTelegramError::Configuration(
            "bot token must contain a ':' separator".to_string(),
        ));
    };

    if bot_id.is_empty() || !bot_id.chars().all(|character| character.is_ascii_digit()) {
        return Err(TeloxideTelegramError::Configuration(
            "bot token id must be ASCII digits".to_string(),
        ));
    }

    if secret.is_empty() || secret.chars().any(char::is_whitespace) {
        return Err(TeloxideTelegramError::Configuration(
            "bot token secret must be non-empty and contain no whitespace".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        TELEGRAM_HTTP_TIMEOUT, TELEGRAM_POLL_TIMEOUT_SECONDS, normalize_bot_token,
        validate_bot_token,
    };

    #[test]
    fn http_timeout_exceeds_poll_timeout() {
        assert!(
            TELEGRAM_HTTP_TIMEOUT
                > std::time::Duration::from_secs(u64::from(TELEGRAM_POLL_TIMEOUT_SECONDS,))
        );
    }

    #[test]
    fn normalize_bot_token_trims_whitespace() {
        assert_eq!(
            normalize_bot_token("  123456:abc_def-ghi  "),
            "123456:abc_def-ghi"
        );
    }

    #[test]
    fn validate_bot_token_accepts_basic_telegram_shape() {
        assert!(validate_bot_token("123456:abc_def-ghi").is_ok());
    }

    #[test]
    fn validate_bot_token_rejects_invalid_shapes() {
        assert!(validate_bot_token("").is_err());
        assert!(validate_bot_token("missing-colon").is_err());
        assert!(validate_bot_token("abc:def").is_err());
        assert!(validate_bot_token("123456:").is_err());
        assert!(validate_bot_token("123456:bad secret").is_err());
    }
}
