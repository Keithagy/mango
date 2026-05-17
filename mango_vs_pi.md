# Mango vs Pi: Architectural Comparison

A thorough comparison of three systems in the agent ecosystem:

- **Pi's extension system** (`pi-mono/packages/coding-agent/src/core/extensions/`) — an interactive coding agent with a hot-reloadable plugin architecture
- **Mango's automation runtime** (`mango/crates/`) — a Rust monorepo implementing Wasm-sandboxed, event-driven automations with a pure-reducer state machine
- **Mom** (`pi-mono/packages/mom/`) — a self-managing Slack bot powered by LLM tool use, with Docker-based isolation

---

## Part 1: Pi's Extension System

### Extension Contract

Every Pi extension is a factory function (`types.ts:1291`):

```typescript
type ExtensionFactory = (pi: ExtensionAPI) => void | Promise<void>;
```

The `ExtensionAPI` (`types.ts:1069-1228`) exposes registration methods:
- `registerTool(tool)` — register a tool the LLM can call
- `registerCommand(name, opts)` — register a `/slash` command
- `registerShortcut(key, opts)` — keyboard shortcut
- `registerFlag(name, opts)` — CLI flag
- `registerMessageRenderer(type, renderer)` — custom UI rendering
- `registerProvider(name, config)` — LLM provider
- `on(event, handler)` — subscribe to lifecycle events

### Tool Definition

A `ToolDefinition` (`types.ts:369-405`) contains:
- `name` / `label` / `description` — identity and LLM prompting
- `parameters` — TypeBox JSON schema
- `execute(toolCallId, params, signal, onUpdate, ctx)` — the implementation
- `renderCall?` / `renderResult?` — optional custom TUI rendering
- `promptSnippet?` / `promptGuidelines?` — inject text into the system prompt

### Hot-Reload Mechanism

The key is at `loader.ts:292-304`:

```typescript
async function loadExtensionModule(extensionPath: string) {
    const jiti = createJiti(import.meta.url, {
        moduleCache: false,  // <-- fresh evaluation every time
        ...(isBunBinary
            ? { virtualModules: VIRTUAL_MODULES, tryNative: false }
            : { alias: getAliases() }),
    });
    const module = await jiti.import(extensionPath, { default: true });
    return typeof module === "function" ? module : undefined;
}
```

Uses jiti (a just-in-time TypeScript transpiler) with `moduleCache: false`. Each `/reload` creates a fresh jiti instance that re-reads and re-evaluates source from disk. Not file-watching — on-demand re-import with cache busting.

### Two-Phase Initialization

Extensions go through a two-phase init (`runner.ts:243-313`):

1. **Load phase**: `createExtensionRuntime()` creates a runtime object with throwing stubs for actions. The factory runs and registers tools/commands/handlers, but can't invoke session actions yet. Provider registrations are queued.
2. **Bind phase**: `bindCore()` replaces stubs with real implementations, flushes queued provider registrations. All extension APIs reference the same shared runtime object, so the swap is transparent.

### Event System

The `ExtensionRunner` (`runner.ts`) drives events through extension handlers:
- `emitToolCall()` — intercept/modify tool calls before execution
- `emitToolResult()` — intercept/modify tool results
- `emitContext()` — modify messages/context before sending to LLM
- `emitInput()` — transform user input
- `emitUserBash()` — hook into user shell commands

Handlers stored per-extension in `Map<string, HandlerFn[]>`, invoked in registration order.

### Context-Engineering Pathways

Pi extensions can shape what the LLM sees through six distinct mechanisms:

1. **`context` event** (`types.ts:540-543`): Rewrite the full message array before each LLM call
2. **`before_agent_start` event**: Inject messages or replace/append the system prompt per-turn
3. **`before_provider_request` event**: Modify the raw API payload
4. **`tool_call` event**: Mutate tool arguments or block execution
5. **`tool_result` event**: Modify tool output before it returns to context
6. **`promptSnippet` / `promptGuidelines`** on `ToolDefinition`: Inject text into system prompt sections

### Proactive/Autonomous Capabilities

Extensions can trigger LLM inference autonomously:

```typescript
pi.sendMessage(
    { customType: "alert", content: "Tests failing" },
    { triggerTurn: true }
);
```

Combined with `fs.watch()`, `setInterval()`, or any Node.js async primitive, extensions can be fully autonomous actors. Example: `file-trigger.ts` watches a file and triggers the agent when it changes.

### Security Model

Extensions run in the same process with full ambient authority. The `ExtensionAPI` is a convenience interface, not a security boundary. An extension can `import('child_process')` and execute arbitrary code at load time. This is the standard trust model for plugin systems in interpreted languages.

### Assessment: Quality of Context-Engineering Documentation

**Score: 7/10.** Precise contracts, excellent examples (70+ in `examples/extensions/`), but missing the architectural narrative.

**Strengths:**
- Type signatures are self-documenting (discriminated unions, generic overloads on `pi.on()`)
- Consistent naming convention (`session_before_switch` → `SessionBeforeSwitchEvent` → `SessionBeforeSwitchResult`)
- `ToolCallEvent`'s mutability contract explicitly documented (line 735)
- `promptSnippet` / `promptGuidelines` clearly convey intent

**Weaknesses:**
- No conceptual map of the context-engineering pipeline and event ordering
- The `context` event's power is drastically understated (one-line JSDoc for the most powerful hook)
- `before_agent_start` conflates two unrelated capabilities (message injection + system prompt replacement)
- `sendMessage` vs `sendUserMessage` vs `appendEntry` poorly differentiated
- `deliverAs` options (`"steer" | "followUp" | "nextTurn"`) never semantically defined
- Observation vs. mutation distinction is implicit (inferred from `ExtensionHandler<E>` vs `ExtensionHandler<E, R>`)
- Three different handler composition models undocumented (early-return for `tool_call`, accumulation for `tool_result`, chaining for `context`)
- `before_provider_request` payload typed as `unknown` in both directions

---

## Part 2: Mango's Architecture

### Workspace Structure

```
crates/
├── kernel/
│   ├── core/              # Domain contracts (traits, types, no deps)
│   ├── runtime-support/   # Runtime helpers, ID generation
│   └── automation-sdk/    # Public guest-facing SDK (re-exports)
├── bridges/
│   ├── claude-agent/      # Process-based bridge to Claude Code
│   └── codex/             # Another bridge
├── orchestration/
│   ├── automations/       # Core control plane (Wasm runtime, effect handler)
│   ├── automation-protocol/  # Stable host/guest contract
│   └── automation-sdk/    # Guest SDK implementation
├── substrate/
│   └── inmemory-bus/      # Event bus implementation
└── surfaces/
    └── telegram/          # UI surface
```

### Schema-Driven Type System

`AgentSchema` (`kernel/core/src/agent/schema.rs:6-112`) is a god-trait parameterizing the entire domain vocabulary:

```rust
pub trait AgentSchema: AgentSchemaIds {
    type Ids: AgentIds;
    type Surface: Clone + Debug;
    type InputKind: Clone + Debug;
    type Input: Clone + Debug;
    type Directive: Clone + Debug;
    type Output: Clone + Debug;
    type ToolData: Clone + Debug;
    type Status: Clone + Debug;
    type CancellationDetail: Clone + Debug;
    type CompletionDetail: Clone + Debug;
    type EngineId: Clone + Debug + Eq + Hash;
    type ToolName: Clone + Debug + Eq + Hash;
    // ... factory methods for all ID types
}
```

Every event, worker, and bus is generic over `S: AgentSchema`, ensuring type safety across the entire system.

### Runtime Assembly

`AgentRuntime` (`runtime.rs:26-62`) composes four boundaries:

```rust
pub trait AgentRuntime {
    type Schema: AgentSchema;
    type Substrate: RuntimeSubstrate<Self::Schema>;   // Event bus + control worker
    type Surface: RuntimeSurface<...>;                // Ingress, egress, presentation
    type Bridge: RuntimeBridge<...>;                  // Inference + tools workers
    type Lifecycle: LifecycleEventHooks<...>;         // Startup + session futures
}
```

Workers are trait-based (`BusWorker<S, B>`, `SessionWorker<S, B>`) with subscription-filtered event streams.

### Event-Driven Architecture

Events are structured as domain records (`substrate.rs:60-76`):

```rust
pub struct Event<S: AgentSchema> {
    pub id: S::EventId,
    pub stream: StreamKey<S>,           // Global, Session, Thread, Worker
    pub visibility: EventVisibility,    // Internal, UserVisible, Both
    pub occurred_at: SystemTime,
    pub payload: EventPayload<S>,       // Interaction | Execution | Presentation | Error
}
```

Execution events (`execution.rs`) have rich lifecycle tracking:
- `ControlEvent` — Requested, CancelRequested
- `InferenceEvent` — Started, Output, Completed, Cancelled, Failed
- `ToolEvent` — Requested, Started, Progress, Succeeded, Failed, Cancelled

The `EventBus` trait (`substrate.rs:162-183`) is transport-agnostic with pub/sub semantics. The `InMemoryAgentBus` uses Tokio broadcast channels with subscription filtering at poll time.

### Wasm Automation System

#### Protocol (Stable ABI)

`automation-protocol/src/lib.rs` defines the host/guest boundary:

```rust
pub struct AdvanceRequest {
    pub automation_id: String,
    pub revision_id: u64,
    pub now: i64,
    pub config: Value,
    pub state: Value,
    pub event: AutomationEvent,
}

pub struct AdvanceResponse {
    pub state: Value,
    pub effects: Vec<EffectRequest>,
    pub status: Option<String>,
    pub disposition: EventDisposition,
}
```

Events the guest can receive:
```rust
pub enum AutomationEvent {
    Activated { at },
    TriggerFired { trigger, payload, at },
    WakeupFired { wakeup_id, at },
    UserSignal { signal, payload, at },
    EffectCompleted { effect_id, result, at },
}
```

Effects the guest can request:
```rust
pub enum EffectKind {
    ScheduleWakeup { wakeup_id, at },
    CancelWakeup { wakeup_id },
    EmitNotification { channel, title, body, metadata },
    FetchHttp { url },
    ReadProfile { keys },
    CallTool { slug, input },
    RunCommand { program, args },
    RunInference { capability, input },
    RunModel { prompt, system },
}
```

#### Guest SDK

The `Automation` trait (`automation-sdk/src/lib.rs:70-89`) is the guest contract:

```rust
pub trait Automation {
    type State: Serialize + DeserializeOwned;
    fn descriptor(&self) -> AutomationDescriptor;
    fn initial_state(&self) -> Self::State;
    fn reduce(&self, state: Self::State, event: AutomationEvent, ctx: GuestContext)
        -> Result<Decision<Self::State>, String>;
}
```

`Decision` carries the new state, requested effects, status, and disposition. The `export_automation!` macro generates the Wasm ABI glue (memory allocation, C-ABI exports).

#### Capability Enforcement

Each automation declares required capabilities in its `AutomationDescriptor`:

```rust
pub enum Capability {
    EmitNotifications, CallTools, FetchHttp, ReadProfile,
    RunCommand, RunInference, RunModel, ScheduleWakeups,
}
```

The control plane checks capabilities before executing effects (`control_plane.rs:648-689`):

```rust
fn ensure_capability(&self, automation_id, revision_id, effect) -> Result<(), AutomationsError> {
    let required = capability_for_effect(effect);
    if revision.descriptor.capabilities.contains(&required) {
        return Ok(());
    }
    Err(AutomationsError::MissingCapability { ... })
}
```

#### Control Plane

`AutomationsControlPlane<B, R, H, C>` (`control_plane.rs:98-132`) manages the full lifecycle:

1. `register_revision()` — load artifact, compute SHA256, persist descriptor
2. `activate_revision()` — ColdStart or PreserveState (validates schema version compatibility)
3. `submit_event()` — event loop with VecDeque: advance guest → persist → apply effects → follow-up events
4. `reconcile_due()` — dispatch all wakeups where `at <= now`
5. `deactivate_automation()` / `delete_automation()` — lifecycle cleanup

Every state transition is recorded in a structured trace log (`TraceEvent` variants: `RevisionRegistered`, `RevisionActivated`, `EventSubmitted`, `StateAdvanced`, `WakeupScheduled`, `EffectRequested`, `EffectHandled`, etc.).

#### Wasm Runtime

`WasmAutomationRuntime` (`guest.rs:34-103`) uses Wasmtime:
- Fresh instance per invocation (no persistent Wasm state)
- Guest must export: `mango_automation_abi_version`, `mango_automation_register`, `mango_automation_advance`, `mango_automation_alloc`, `mango_automation_free`, `memory`
- ABI version check on load
- Data exchange via guest memory with packed `(ptr, len)` handles

---

## Part 3: Mom

### What It Is

Mom is a self-managing Slack bot powered by Claude Sonnet. It connects to Slack via Socket Mode, receives messages, and responds using an LLM agent with five tools (bash, read, write, edit, attach). It runs tool execution in Docker containers for isolation.

Core philosophy from README: "Minimal by design. She builds her own tools without pre-built assumptions."

### Architecture

- **Per-channel state**: Each Slack channel gets its own directory with `log.jsonl`, `context.jsonl`, `MEMORY.md`, `attachments/`, `scratch/`, `skills/`
- **Dual-file context**: `log.jsonl` (append-only source of truth, all messages) + `context.jsonl` (what the LLM sees, auto-synced + compacted)
- **Docker isolation**: `DockerExecutor` wraps commands as `docker exec <container> sh -c <cmd>`
- **Self-managing**: The agent installs packages, configures credentials, creates skills, and schedules events autonomously

### Event System

```typescript
type MomEvent = ImmediateEvent | OneShotEvent | PeriodicEvent
```

- **Immediate**: fires on file creation (webhooks, external signals)
- **One-shot**: fires at an ISO 8601 timestamp, then deleted
- **Periodic**: fires on a cron schedule, persists until deleted

`EventsWatcher` watches `data/events/` for JSON files with 100ms debounce. The agent itself can create event files, making the system self-programming.

### Skills System

Skills are agent-created CLI tools:
- Stored in `/workspace/skills/` (global) or `/workspace/<channel>/skills/` (channel-specific)
- Each has a `SKILL.md` with YAML frontmatter + usage docs + scripts
- Discovered by scanning directories, loaded into system prompt as summaries
- Agent reads full SKILL.md when it decides to use a skill

### Context Engineering

Mom has a layered context management strategy:
- `log.jsonl` as infinite grep-able history beyond context window
- `context.jsonl` as bounded LLM context (auto-compacted via SessionManager)
- `MEMORY.md` as persistent agent-managed working memory
- ~1000-2000 line system prompt with Slack formatting rules, channel/user ID mappings, workspace layout, skills documentation, event system instructions, memory instructions

---

## Part 4: Comparative Analysis

### Pi vs Mango: Extension System vs Automation Runtime

These solve fundamentally different problems. Pi is an interactive coding agent where the user sits at a terminal. Mango manages long-lived, event-driven automations running autonomously.

#### Plugin Isolation: Trust vs. Containment

**Pi**: Extensions run in the host process with full ambient authority. `ExtensionAPI` is a convention, not a boundary.

**Mango**: Automations run in Wasm sandboxes with declared capabilities. The guest exports a pure function and can only interact through `EffectKind` variants the host handles.

```
Pi:    extension ──[shared address space]── host
Mango: guest ──[Wasm ABI boundary]── control plane ──[capability check]── effect handler
```

#### State Model: Imperative Mutation vs Pure Reducers

**Pi**: Extensions mutate shared state imperatively (`pi.registerTool()`, `pi.sendMessage()`, `pi.setActiveTools()`). Two-phase init exists because imperative style creates ordering dependencies.

**Mango**: Pure state machines. `reduce(state, event, ctx) → Decision`. State serialized as JSON, passed in and returned out. The automation is stateless between calls. Every transition is traced.

Testing implication: Mango automations are trivially testable (feed state + event, assert output). Pi extensions require mocking the entire ExtensionAPI surface.

#### Event Taxonomy: Pipeline Hooks vs Domain Events

**Pi** events are agent pipeline hooks — points in the LLM inference/tool-execution loop:

```
input → before_agent_start → context → before_provider_request → [LLM] →
tool_call → [execute] → tool_result → turn_end → agent_end
```

Implementation-coupled: extension authors need to understand the agent's internal pipeline.

**Mango** events are domain-level stimuli:

```rust
AutomationEvent::Activated | TriggerFired | WakeupFired | UserSignal | EffectCompleted
```

Implementation-decoupled: the automation doesn't know how the host dispatches internally. The kernel's own rich event taxonomy (`ExecutionEvent`, `InferenceEvent`, `ToolEvent`) is hidden behind the protocol boundary.

Pi's approach is more powerful but harder to reason about. Mango's is constrained but self-documenting.

#### Context Engineering

**Pi**: Deep, granular control — six distinct pathways for shaping LLM context (messages, system prompt, provider payload, tool I/O, prompt injection via tool definitions). Essentially middleware for LLM inference.

**Mango**: No direct context engineering. `RunModel { prompt, system }` is a black box from the guest's perspective. The `ClaudeAgentConfig` bridge has `system_prompt_append`, but that's host-level configuration.

#### Composition Model

**Pi**: Handler chaining with heterogeneous semantics. `tool_call` early-returns on block. `tool_result` accumulates mutations. `context` chains (each handler sees previous handler's output). Three different models, undocumented in types.

**Mango**: Single consistent model — event loop with effect queue. Events produce effects, effects produce follow-up events, loop until queue empty.

#### Type Safety

**Pi**: Structural typing (TS discriminated unions, TypeBox schemas, generic overloads). Some boundaries loose: `before_provider_request` payload is `unknown`, `ToolExecutionUpdateEvent.partialResult` is `any`.

**Mango**: Parametric typing. `AgentSchema` defines ~15 associated types. Every event, worker, and bus is generic over `S: AgentSchema`. Compiler enforces that a `ToolEvent<S>` carrying `S::ToolData` can't be confused with `InferenceEvent<S>` carrying `S::Output`. Rigorous but intimidating — generic bounds cascade through every type.

#### Hot Reload

**Pi**: jiti `moduleCache: false` — re-transpile TypeScript on `/reload`. Simple, zero-config.

**Mango**: Revision-based deployment. Register new `.wasm` artifact, activate with optional state preservation (validates `state_schema_version` compatibility), old revisions preserved for rollback. Fresh Wasm instance per `advance` call — no persistent sandbox state.

#### Auditability

**Pi**: No built-in audit trail. Extension side effects invisible to host.

**Mango**: Complete trace log. Every state transition recorded: `RevisionRegistered`, `StateAdvanced`, `WakeupScheduled`, `EffectRequested`, `EffectHandled`. Audit trail complete by construction because all effects go through the control plane.

### Mom vs Mango: LLM Agent vs Compiled State Machine

Mom and Mango solve the same fundamental problem — autonomous, event-driven agent orchestration without a human at the keyboard. The comparison reveals what each values.

#### State Machine: Implicit vs Explicit

**Mango**: Explicit pure state machine. `reduce(state, event) → Decision`. State is serialized JSON, owned by the control plane. Every transition traced.

**Mom**: Implicit state machine. State scattered across `log.jsonl`, `context.jsonl`, `MEMORY.md`, `ChannelState` in memory, and the Docker container filesystem. The "state machine" is emergent behavior of interacting systems.

Mango's approach is more testable and reproducible. Mom's is more pragmatic — the LLM manages complexity. But Mom can't snapshot/restore a channel's full state without reconstructing from multiple files.

#### Isolation: Docker vs Wasm

**Mom**: Docker containers for tool execution. Coarse-grained — full root inside container. Protects host but not credentials inside container.

**Mango**: Wasm modules for automation. Fine-grained — guest can only request effects through declared capabilities. Fresh instance per call, no persistent sandbox state.

Not comparable strategies — different threat models. Docker isolates a general-purpose Linux userspace. Wasm isolates a pure computation that delegates all side effects to the host.

Could Mom benefit from Mango-style capability declarations? Currently tools are hardcoded and the agent has unrestricted access. A capability check before tool execution would let operators scope instances.

#### Event/Scheduling

**Mom**: Filesystem-based. JSON files in `data/events/`. The agent can create event files (via bash/write), making the system self-programming. External systems can also write files.

**Mango**: Effect-based. `ScheduleWakeup` is a declared capability. Wakeups are managed state. Every schedule/cancel is traced.

Mom's approach is more accessible (human-readable, creatable from anywhere). Mango's is more principled (declared capability, managed state, traced). Mom has an elegant property Mango lacks: the agent can create entirely new event types by writing arbitrary JSON. Mango's `AutomationEvent` enum is closed.

#### Tool Execution

**Mom**: LLM-selected. Five hardcoded tools wrapping an `Executor` abstraction. The LLM chooses which tool to call at inference time (non-deterministic, expensive, creative).

**Mango**: Code-selected. `EffectKind` variants emitted by `reduce` (deterministic, cheap, rigid). Effects composable — `CallTool` can produce follow-up events.

Mom can adapt to novel situations through creative tool use ("set up monitoring for staging"). Mango automations need everything pre-programmed.

#### Context Engineering

**Mom**: Deep investment. Layered strategy with log + context + memory + skills + crafted system prompt. Auto-compaction when context exceeds 200k tokens. Agent can grep log.jsonl for infinite history.

**Mango**: Minimal. `RunModel { prompt, system }` delegates to host. No token management, no compaction, no memory system.

Mom needs deep context engineering because it delegates decision-making to the LLM. Mango's automations are deterministic code that doesn't need it.

#### Self-Modification

**Mom**: Core feature. Agent writes its own memory, creates skills, installs tools, schedules events, manages credentials. "She builds her own tools without pre-built assumptions."

**Mango**: Impossible by design. Behavior fully defined at compile time. Can't install new capabilities, create new event types, or modify its own code.

This maps to a real choice: do you want your autonomous agent to be a creative problem solver or a reliable state machine? Mom chooses the former; Mango the latter. For a daily news digest, Mango is clearly better (deterministic, traceable). For "set up monitoring for staging," Mom is clearly better (she can figure it out).

### Cross-Cutting Observations

1. **They're complementary, not competing.** Pi is a plugin system for extending an interactive agent. Mango is an orchestration layer for managing autonomous state machines. Mom is an LLM-driven autonomous agent. The Claude bridge in Mango (`bridges/claude-agent`) literally spawns a process that could be running Pi — they could sit at different layers of the same stack.

2. **Mango's `reduce` pattern is what Pi's extension system would look like if it prioritized testability and sandboxing over developer ergonomics.** The tradeoff is real — Pi's `pi.on("tool_call", handler)` is immediately intuitive; Mango's `match event { ... }` requires understanding the event-loop-over-effects pattern first.

3. **Pi's context-engineering power comes with documentation debt.** Six distinct pathways, three composition models, no pipeline diagram. Mango avoids this by not exposing context to guests — at the cost of expressiveness.

4. **Mango's `Capability` enum is what a Wasm-based Pi would need.** If Pi ever sandboxed extensions, `capability_for_effect` — a closed enum matching effect kinds to declared capabilities — is exactly the right design. 13 lines of code (`control_plane.rs:676-689`).

5. **Both Pi and Mango have a schema/vocabulary layer, at different altitudes.** Pi's `ToolDefinition` is concrete: "things the LLM can call." Mango's `AgentSchema` is abstract: "all the nouns in the agent lifecycle." Pi's is practical for a single runtime. Mango's pays off when you need multiple runtime configurations.

6. **Mom is what happens when you push agent autonomy to the extreme.** By giving the LLM full bash access, self-modification, and self-scheduling, Mom achieves emergence at the cost of predictability. Mango achieves predictability at the cost of emergence. The security sections of both projects' documentation reveal how conscious each is of this tradeoff.

### What Each Could Learn from the Others

#### Pi from Mango
- Capability declarations for extensions (especially if Wasm sandboxing is ever considered)
- Structured audit trail for extension side effects
- Revision-based deployment with rollback

#### Mango from Pi
- Richer context-engineering hooks (if automations gain LLM-driven decision making)
- Lower barrier to entry (Pi's "drop a .ts file" vs Mango's "compile a Rust crate to Wasm")
- Example-driven documentation (Pi's 70+ example extensions are its best documentation)

#### Mom from Mango
- Capability declarations per channel/instance (restrict which tools are available)
- Structured trace logging (beyond Slack threads and log files)
- Testable state transitions (extract pure functions from impure sync logic)
- Revision-based deployment with state preservation

#### Mango from Mom
- Self-modifying capabilities (agents that extend their own trigger surface)
- Grep-able infinite history (beyond the single JSON state blob)
- Skills as emergent tooling (lightweight plugins the agent itself creates)
- Context-aware error recovery (LLM reasoning about failures vs code-level error handling)

---

## Summary Table

| Dimension | Pi (Extensions) | Mango (Automations) | Mom (Slack Bot) |
|---|---|---|---|
| **Primary purpose** | Interactive coding agent plugin | Autonomous orchestration | Self-managing Slack agent |
| **Language** | TypeScript | Rust | TypeScript |
| **Plugin isolation** | None (shared process) | Wasm sandbox + capabilities | Docker container |
| **State model** | Imperative mutation | Pure reducer `(state, event) → Decision` | Scattered (log, context, memory, fs) |
| **Determinism** | N/A (user-driven) | Deterministic (code selects effects) | Non-deterministic (LLM selects tools) |
| **Context engineering** | Deep (6 pathways) | Minimal (prompt passthrough) | Deep (log + context + memory + skills) |
| **Event model** | Pipeline hooks | Domain stimuli (closed enum) | File-based (open, self-programmable) |
| **Scheduling** | Ad-hoc (`setInterval`) | First-class (`ScheduleWakeup`) | File-based (immediate/one-shot/periodic) |
| **Hot reload** | jiti `moduleCache: false` | Revision-based artifact replacement | Process restart |
| **Auditability** | None | Complete trace log | Partial (Slack threads + logs) |
| **Self-modification** | Extensions can do anything | Impossible by design | Core feature |
| **Testability** | Requires mocking | Trivial (pure function) | Requires mocking LLM + fs |
| **Developer barrier** | Low (drop a .ts file) | High (Rust → Wasm → control plane) | Low (env vars + Docker) |
| **Trust model** | Trust the code | Trust the interface | Trust the container |
| **Composition** | 3 handler chaining models | Single event-loop model | LLM-driven tool composition |
