# Telegram Automations Demo

This example has two verification paths:

- Deterministic BDD tests use the in-process Telegram test client from `mango-telegram`.
- Live Telegram verification runs the real `telegram-automations` binary against Telegram.
  The dedicated MTProto probe for that path lives in `../telegram-live-probe/`.

## Deterministic Test Infra

The example crate's BDD scenarios use a configurable in-process Telegram client plus scripted story and command backends on top of the `PocketUniverse` simulator. That path verifies:

- the `/start` help flow and empty `/automations` listing from a clean chat
- username allowlisting
- `/schedule_dice_story`, `/automations`, `/automation_runs`
- due wakeups and notification delivery
- model retry behavior
- lifecycle commands: set period, pause, resume, delete
- failed external-tool runs surfacing through `/automation_runs`

The example crate also includes direct pocket-universe scenarios for the real dice-story Wasm guest so simulator coverage does not depend only on the Telegram command surface.

Those tests do not require:

- a Telegram bot token
- Claude
- Node.js

## Live Telegram Verification Inputs

To run the real binary against Telegram and let me drive it from the outside, I need:

1. `telegram-automations.toml` or equivalent values for:
   - `bot_token` or `bot_token_env`
   - `allowed_usernames`
   - `state_file`
   - `claude_executable`
   - `claude_model`
   - `claude_working_directory`
   - `node_executable`
   - `script_path`
2. A dedicated Telegram chat for the demo:
   - `chat_id`
   - optional `thread_id`
3. A non-bot test actor username to put in `allowed_usernames`.

Important: Telegram bots do not receive updates from other bots, so a fully automated live probe cannot be another Bot API bot. A real black-box probe would need either:

- a human-operated test account, or
- an MTProto-backed user client with its own Telegram credentials

If you want me to automate that live path, I need these additional user-client credentials:

- `api_id`
- `api_hash`
- session material for the test user account
- the username of that test user account

A template for those inputs lives in `../telegram-live-probe/telegram-live-probe.example.toml`.
The bot token should stay in `telegram-automations.toml` or its env var, not be copied into the probe config.
