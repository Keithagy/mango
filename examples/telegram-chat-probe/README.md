# Telegram Chat Probe

This example is a black-box probe for `telegram-chat`.

It exercises the app entirely through the public `telegram_chat::testing`
harness, not through internals. The probe runs a small scripted verification
suite plus seeded random sessions that mix:

- normal conversational text
- receipt-like photos
- non-expense photos
- clarification follow-ups
- read/update/delete expense commands
- unauthorized turns in the same chat/thread
- root-chat and forum-thread sessions

On failure it prints the failing seed, turn label, transcript, traces, and
persisted markdown state so the behavior is reproducible.

## Running

```sh
cargo run -p telegram-chat-probe
```

To override the defaults with a local config:

```sh
cargo run -p telegram-chat-probe -- path/to/telegram-chat-probe.toml
```
