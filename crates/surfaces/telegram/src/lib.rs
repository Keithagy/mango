//! Telegram surface adapters for Mango runtimes.

mod client;
mod surface;
mod test_client;

pub use client::{
    TelegramChatId, TelegramClient, TelegramInboundMessage, TelegramMessageId,
    TelegramOutboundMessage, TelegramPhotoAttachment, TelegramSessionSurface, TelegramSurface,
    TelegramThreadId, TeloxideTelegramClient, TeloxideTelegramError,
};
pub use surface::{
    DisplayTelegramTextMapper, PlainTextTelegramInputMapper, TelegramEgress, TelegramInbox,
    TelegramInboxSender, TelegramIngress, TelegramIngressMapper, TelegramInputTurn,
    TelegramTextMapper, telegram_inbox,
};
pub use test_client::{
    TestTelegramActor, TestTelegramClient, TestTelegramClientConfig, TestTelegramDriver,
    TestTelegramError, telegram_test_client, telegram_test_client_with_config,
};
