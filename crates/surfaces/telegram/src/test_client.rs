use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, mpsc},
    time::{Duration, timeout},
};

use crate::{
    TelegramChatId, TelegramClient, TelegramInboundMessage, TelegramMessageId,
    TelegramOutboundMessage, TelegramPhotoAttachment, TelegramThreadId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestTelegramClientConfig {
    pub capacity: usize,
    pub first_message_id: i32,
}

impl Default for TestTelegramClientConfig {
    fn default() -> Self {
        Self {
            capacity: 128,
            first_message_id: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestTelegramActor {
    pub chat_id: TelegramChatId,
    pub thread_id: Option<TelegramThreadId>,
    pub username: Option<String>,
    pub display_name: String,
}

impl TestTelegramActor {
    #[must_use]
    pub fn new(
        chat_id: TelegramChatId,
        thread_id: Option<TelegramThreadId>,
        username: Option<String>,
        display_name: impl Into<String>,
    ) -> Self {
        Self {
            chat_id,
            thread_id,
            username,
            display_name: display_name.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TestTelegramError {
    #[error("test telegram inbox is closed")]
    InboxClosed,
    #[error("timed out waiting for a test telegram outbound message")]
    ReceiveTimedOut,
}

#[derive(Debug, Clone)]
pub struct TestTelegramClient {
    inbox: Arc<Mutex<mpsc::Receiver<TelegramInboundMessage>>>,
    outbound: mpsc::Sender<TelegramOutboundMessage>,
    transcript: Arc<Mutex<Vec<TelegramOutboundMessage>>>,
}

#[derive(Debug, Clone)]
pub struct TestTelegramDriver {
    inbound: mpsc::Sender<TelegramInboundMessage>,
    outbound: Arc<Mutex<mpsc::Receiver<TelegramOutboundMessage>>>,
    transcript: Arc<Mutex<Vec<TelegramOutboundMessage>>>,
    next_message_id: Arc<AtomicI32>,
}

#[must_use]
pub fn telegram_test_client(capacity: usize) -> (TestTelegramDriver, TestTelegramClient) {
    telegram_test_client_with_config(TestTelegramClientConfig {
        capacity,
        ..TestTelegramClientConfig::default()
    })
}

#[must_use]
pub fn telegram_test_client_with_config(
    config: TestTelegramClientConfig,
) -> (TestTelegramDriver, TestTelegramClient) {
    let (inbound_tx, inbound_rx) = mpsc::channel(config.capacity);
    let (outbound_tx, outbound_rx) = mpsc::channel(config.capacity);
    let transcript = Arc::new(Mutex::new(Vec::new()));

    (
        TestTelegramDriver {
            inbound: inbound_tx,
            outbound: Arc::new(Mutex::new(outbound_rx)),
            transcript: Arc::clone(&transcript),
            next_message_id: Arc::new(AtomicI32::new(config.first_message_id)),
        },
        TestTelegramClient {
            inbox: Arc::new(Mutex::new(inbound_rx)),
            outbound: outbound_tx,
            transcript,
        },
    )
}

impl TestTelegramDriver {
    /// Send a text message from a scripted actor into the test client inbox.
    ///
    /// # Errors
    ///
    /// Returns an error if the test client has already been dropped.
    pub async fn send_text(
        &self,
        actor: &TestTelegramActor,
        text: impl Into<String>,
    ) -> Result<(), TestTelegramError> {
        let message_id = TelegramMessageId(self.next_message_id.fetch_add(1, Ordering::SeqCst));
        self.inbound
            .send(TelegramInboundMessage {
                chat_id: actor.chat_id,
                thread_id: actor.thread_id,
                message_id,
                username: actor.username.clone(),
                display_name: actor.display_name.clone(),
                text: text.into(),
                caption: None,
                photo: None,
            })
            .await
            .map_err(|_| TestTelegramError::InboxClosed)
    }

    /// Send a photo message from a scripted actor into the test client inbox.
    ///
    /// # Errors
    ///
    /// Returns an error if the test client has already been dropped.
    pub async fn send_photo(
        &self,
        actor: &TestTelegramActor,
        local_path: impl Into<PathBuf>,
        caption: Option<String>,
    ) -> Result<(), TestTelegramError> {
        let message_id = TelegramMessageId(self.next_message_id.fetch_add(1, Ordering::SeqCst));
        self.inbound
            .send(TelegramInboundMessage {
                chat_id: actor.chat_id,
                thread_id: actor.thread_id,
                message_id,
                username: actor.username.clone(),
                display_name: actor.display_name.clone(),
                text: caption.clone().unwrap_or_default(),
                caption,
                photo: Some(TelegramPhotoAttachment {
                    local_path: local_path.into(),
                }),
            })
            .await
            .map_err(|_| TestTelegramError::InboxClosed)
    }

    /// Wait for the next outbound Telegram message emitted by the app.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout expires before a message is available.
    pub async fn recv_outbound(
        &self,
        wait_for: Duration,
    ) -> Result<TelegramOutboundMessage, TestTelegramError> {
        timeout(wait_for, self.outbound.lock().await.recv())
            .await
            .map_err(|_| TestTelegramError::ReceiveTimedOut)?
            .ok_or(TestTelegramError::InboxClosed)
    }

    pub async fn transcript(&self) -> Vec<TelegramOutboundMessage> {
        self.transcript.lock().await.clone()
    }
}

#[async_trait]
impl TelegramClient for TestTelegramClient {
    type Error = TestTelegramError;

    async fn recv(&self) -> Result<Option<TelegramInboundMessage>, Self::Error> {
        Ok(self.inbox.lock().await.recv().await)
    }

    async fn send_message(&self, message: TelegramOutboundMessage) -> Result<(), Self::Error> {
        self.transcript.lock().await.push(message.clone());
        self.outbound
            .send(message)
            .await
            .map_err(|_| TestTelegramError::InboxClosed)
    }
}
