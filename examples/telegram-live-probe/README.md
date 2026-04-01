# Telegram Live Probe

This example is a black-box probe for `telegram-automations`.

It logs in as a real Telegram user account over MTProto, derives the bot username from the
`telegram-automations` demo config, sends Telegram commands, waits for bot replies, and verifies
that the live demo behaves end to end.

## What it verifies

- `/automations` on a clean chat
- `/schedule_dice_story`
- `/automations` after install
- background notification delivery from the automation
- repeated `/automation_runs`
- `/pause_automation`, `/resume_automation`, `/delete_automation`

## Config

Copy `telegram-live-probe.example.toml` to `telegram-live-probe.toml` and fill in the probe-user
credentials.

Important:

- The probe uses a real Telegram user account, not another bot.
- The first run may prompt for the Telegram login code and 2FA password to create the local
  session file.
- Subsequent runs reuse the persisted session file.
- If you prefer, `api_hash`, `phone`, `login_code`, and `password` can all live directly in the
  local untracked TOML file instead of environment variables.

## Running

```sh
cargo run -p telegram-live-probe
```

To point the probe at a different config file:

```sh
cargo run -p telegram-live-probe -- path/to/telegram-live-probe.toml
```
