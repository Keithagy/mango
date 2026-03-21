# Examples

These crates are the public proof-of-concept surface for Mango.

- `mango-example-support` holds the repeated scaffolding that would otherwise drown the examples:
  the in-memory bus adapter, session/subscription helpers, shared runtime startup, and common
  worker-error publishing.
- `mango-poc` is the smallest browser chat example.
- `mango-code-agent` shows Mango-owned tool execution around a coding agent.
- `mango-debate-poc` shows orchestration across multiple concurrent inference workers.

Suggested reading order:

1. `examples/mango-example-support/src/lib.rs`
2. one example `src/main.rs` to see runtime assembly
3. that example's `src/lib.rs` to inspect the schema and worker logic

If a detail is repeated across examples, the default move is to push it into
`mango-example-support` instead of re-explaining it in each proof of concept.
