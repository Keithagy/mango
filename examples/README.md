# Examples

These crates are the public example surface for Mango.

- `example-support` under `examples/example-support/` holds the repeated scaffolding that would otherwise drown the examples:
  the in-memory bus adapter, session/subscription helpers, shared runtime startup, and common
  worker-error publishing.
- `browser-chat` is the smallest browser chat example.
- `code-agent` shows Mango-owned tool execution around a coding agent.
- `browser-debate` shows orchestration across multiple concurrent inference workers.
- `telegram-automations` shows an interactive Telegram bot that manages versioned Wasm automations, runs deterministic JS through host-mediated effects, validates a real LLM refinement step, and delivers the result back over Telegram. Its example-level BDD coverage is wired through the pocket universe.
- `telegram-live-probe` is the MTProto-backed black-box test client for driving `telegram-automations` from a real user account.
- `telegram-chat` shows a Telegram surface backed by generic Mango ingress and egress workers.

Suggested reading order:

1. `examples/example-support/src/lib.rs`
2. one example `src/main.rs` to see runtime assembly
3. that example's `src/lib.rs` to inspect the schema and worker logic

If a detail is repeated across examples, the default move is to push it into
`example-support` instead of re-explaining it in each example.
