# Telegram Chat Automation Vision

## Purpose

`telegram-chat` should remain a normal conversational Telegram chat app while also supporting dynamically registered automations.

The goal is not to bolt one special-case expense workflow onto the app. The goal is to shape the example toward a general automation architecture in which app behavior can be extended by installing automation bundles without recompiling the host to understand their app-specific contracts.

This document captures the intended architecture and requirements as discussed before implementation shortcuts and drift.

## Product Shape

- The app remains a normal chat app with a baseline conversational backend.
- Automations are additive, not a replacement for chat.
- An inbound user turn may be handled by an automation, may fall through to baseline chat, and in the future may participate in richer multi-handler or scatter-gather flows.
- For the immediate scope, a handled automation response is the user-visible reply for that turn; an unhandled automation turn falls through to the baseline chat backend.
- Pending clarification state in an automation must not cause unrelated turns to be stolen from baseline chat. If no automation handler matches the current turn, the turn falls through and the clarification remains pending.

## Architectural Principles

### 1. The host must not need recompilation to understand a new automation

- New automations must be installable without teaching the host any app-specific typed contract.
- The host must remain generic and type-erased with respect to automation-specific schemas.
- The host may normalize surface-specific ingress into generic trigger payloads and may ferry observations back to the surface, but it must not hardcode business-specific logic for one automation.

### 2. The transport protocol belongs in the core runtime

- The transport boundary between automation components is a core runtime concern, not an app concern.
- Core runtime types should carry generic trigger/tool/inference transport data.
- App-level typed contracts belong only inside the automation bundle and its owned providers/backends, not in the host-facing interface.

### 3. An automation is a bundle, not just a Wasm guest

An automation should be treated as a bundle containing:

- the automation guest itself
- its declared trigger subscriptions
- the tools it depends on
- the inference capabilities it depends on

The bundle should be dynamically registerable and testable as a unit.

### 4. Wasm guests must not perform unintermediated external effects

- Wasm guests may request tools and inference through the runtime.
- Wasm guests must not directly perform arbitrary file I/O, subprocess spawning, network calls, or unmanaged side effects.
- All external interactions must stay mediated through the control plane/runtime so they remain observable, backpressured, sandboxable, and testable.

## Bundle Model

Each automation bundle should declare, in runtime-readable metadata:

- its Wasm artifact
- its trigger subscriptions
- its required tool bindings
- its required inference capability bindings

The core runtime should be able to parse this metadata dynamically and register the bundle at runtime.

The bundle itself owns its typed schemas and business semantics. Outside the bundle boundary, the host only sees generic slugs and opaque structured payloads.

## Registration Model

- The expense example automation should be compiled separately into Wasm and registered through the automations control plane.
- It must not be statically compiled into the chat host as an in-process hardcoded behavior.
- For the example app, one default auto-registration at startup is acceptable.
- If compile-time build automation is practical, `build.rs` is acceptable. Otherwise startup registration is acceptable.
- The important requirement is architectural separation between the host app and the automation bundle.

## Trigger Model

- Trigger subscriptions are declared by the bundle itself.
- The core runtime dynamically loads those subscriptions and dispatches normalized ingress events to matching bundles.
- The host should not hardcode which automation wants which trigger.

For this example, the relevant trigger family includes at least:

- normalized text turns
- normalized photo turns

The design should be extensible to future triggers such as stickers or other media.

## Tool and Inference Model

### Dynamic injection

- Tools and inference backends must be dynamically injectable in the same general way as automations.
- A bundle may rely on bundle-specific tools and bundle-specific inference capabilities without requiring host recompilation.
- The runtime should mount these dependencies by bundle declaration and runtime registration.

### Typed schemas scoped to the bundle

- The library user may define typed request/response schemas for their own tools and inference capabilities.
- Those typed schemas are a concern only within the automation bundle and its owned providers/backends.
- The host and core dispatch path should remain type-erased and transport-oriented.

### Model capability routing

- Inference requests should be routed by declared capability identifiers.
- The bundle should express its needed inference capabilities declaratively.
- Receipt extraction and other model-mediated decisions should be expressed as inference capabilities, not as host-specific hardcoded logic.

## Control Plane and Dispatch

- The automations control plane remains the authority for bundle registration, activation, state progression, and effect mediation.
- The generic registration-and-dispatch concern should live in the core automations library, not in `telegram-chat`.
- `telegram-chat` should be only a thin adapter that:
  - normalizes Telegram ingress into generic trigger payloads
  - derives session scope keys
  - maps emitted observations back into user-visible replies
  - chooses which bundles to auto-install by configuration

## Session Scope

- Automation instances should be scoped per Telegram chat/thread.
- State must not bleed across chats or threads.
- The shared backing store may live under one configurable state root, but automation state and dispatch identity are per chat/thread surface.

## Expense Automation Requirements

The example bundle should prove out a realistic, stateful automation by managing an expense-report datastore.

### Domain behavior

The automation must support CRUD over expense reports:

- create new expenses from receipt photos or clarified user input
- read expenses through free-form conversational requests
- update existing expenses through free-form conversational edit requests
- delete expenses through free-form conversational requests

### Multi-turn statefulness

- The automation must maintain pending clarification state.
- It must ask follow-up questions before writing or mutating persistent data when confidence is insufficient.
- This is a key requirement: the example must demonstrate a multi-turn, stateful automation invocation, not just one-shot command handling.

### Agentic interpretation

Photo-triggered expense handling must be agentic rather than rule-only:

- determine whether the uploaded image is an expense at all
- determine whether it is a new expense, a duplicate, or a change to an existing one
- determine whether available evidence is sufficient to act
- ask clarifying questions when confidence is insufficient

### Actual extraction

- Receipt handling must include actual extraction, not just treating every photo as an expense.
- Extraction should be modeled through bundle-declared inference capabilities mediated by the runtime.

### Ingress normalization

- Photo ingress should be normalized to a persisted local file path before automation dispatch.
- The automation should operate on that normalized path rather than opaque Telegram file IDs.

## Persistence and Storage

- The default persistence model should be a human-inspectable pile of markdown under `./local/state/mango/`.
- The location must be configurable and dependency-injectable.
- The storage backend should be abstract enough that a library user can swap in another backend, such as a database, while preserving the same behavioral contract.
- For this example, soft delete is preferred over hard delete for safety and testability.
- A simple shape such as one file per expense with a stable ID and machine-readable frontmatter is acceptable.

## Testing Requirements

### App-level BDD coverage

- The app-level code must be well covered with end-to-end BDD tests.
- These tests must use only facilities exposed to the app-level library user.
- A library user must be able to write behavioral tests for their automation interactions without dropping into library internals.

### Public test surface

The public testing surface should allow a library user to:

- drive normalized text turns
- drive normalized photo turns
- inject or swap datastore backends
- inject or swap tool backends
- inject or swap inference backends
- inspect emitted replies
- inspect resulting persistent state
- exercise multi-turn clarification flows

### Pocket-universe compatibility

- Bundles, tools, and inference dependencies should all be testable against the pocket-universe style test engine and the public app-level harness.
- The testability story should reinforce the same dynamic registration architecture used at runtime.

## Flow-Control Semantics

For the immediate scope:

- a handled automation turn produces the final user-visible reply for that turn
- an unhandled automation turn falls through to baseline chat

This is explicitly understood as an intermediate step. The future architecture likely needs richer flow-control and scatter-gather semantics, including:

- multiple automations triggered by a single input
- automation plus baseline chat both contributing to one turn
- overlapping or nested automations

The current work should shape toward that future rather than baking in assumptions that prevent it.

## Non-Goals For This Phase

- A stable final public API
- A fully solved scatter-gather semantics model
- A globally fixed set of tools or model capabilities compiled into the host
- A host that understands bundle-specific typed business contracts

## Success Criteria

This vision is satisfied when:

- `telegram-chat` remains a normal conversational app
- the expense automation is a separate Wasm bundle registered through the control plane
- the host stays generic and transport-oriented
- bundle-declared triggers/tools/inference are dynamically mounted
- expense CRUD works through normal text and photo turns
- multi-turn clarification is real and stateful
- storage is human-inspectable and dependency-injectable
- app-level BDD tests cover behavior end to end using only public test facilities
- the architecture points cleanly toward multiple dynamic automation bundles in the future
