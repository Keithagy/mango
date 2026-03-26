//! Telegram surface adapters for Mango runtimes.

mod client;
mod surface;

pub use client::{
    TelegramChatId, TelegramClient, TelegramInboundMessage, TelegramMessageId,
    TelegramOutboundMessage, TelegramSessionSurface, TelegramSurface, TelegramThreadId,
    TeloxideTelegramClient, TeloxideTelegramError,
};
pub use surface::{
    DisplayTelegramTextMapper, PlainTextTelegramInputMapper, TelegramEgress, TelegramInbox,
    TelegramInboxSender, TelegramIngress, TelegramIngressMapper, TelegramInputTurn,
    TelegramTextMapper, telegram_inbox,
};
