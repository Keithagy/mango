# automation-demo

This example keeps application intent at the edge of the repository.

- Core crates define the generic SDK, Wasm control plane, and pocket-universe simulator.
- App-specific automations live under `guests/` as standalone Rust projects.
- The demo binary compiles those guests to Wasm, registers them explicitly into the pocket universe,
  activates them, and drives them through simulated world events.

Nothing in the core crates knows about hydration reminders or news digests.

Boundary map:

- `crates/kernel/automation-sdk`: public guest-facing SDK surface
- `crates/orchestration/automation-control`: host-side control-plane surface
- `crates/testing/automation-sim`: pocket-universe entrypoint
- `examples/automation-demo/guests/hydration-automation`: reminder-until-confirmed demo guest
- `examples/automation-demo/guests/news-digest-automation`: fetch/profile/model demo guest

Run the demo from the workspace root:

```bash
cargo run -p automation-demo
```

The demo host will:

1. Compile the hydration guest to `wasm32-unknown-unknown`
2. Register the resulting Wasm artifact into the pocket universe
3. Activate that specific revision
4. Advance simulated time until reminder notifications fire
5. Inject a confirmation signal and settle the simulated world again
