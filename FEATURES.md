Favn Roadmap & Feature Status

## Current Version

**Current release: v0.1.0**

Favn is in early development.

The product direction is now explicitly locked:

- Favn is **asset-first**
- v1 and v2 focus on excellent **ETL/ELT orchestration**
- assets are the only public executable node type in v1
- orchestration concerns (pipelines, schedules, polling, triggers) stay **outside** the function-level DSL
- assets are expected to **materialize externally**
- Favn tracks orchestration state, lineage, freshness, runs, and materialization history
- Favn does **not** pass data directly between assets as a public model

This document tracks both **direction** and **progress**.

---

## Product Focus

Favn v1 should be great at:

- defining ETL/ELT assets in plain Elixir
- connecting assets through dependency declarations
- executing asset graphs deterministically
- scheduling and polling asset/pipeline execution
- skipping unnecessary work through freshness rules
- tracking runs and materializations
- giving a clear graph and documentation view of the codebase

Favn v1 is **not** trying to solve:

- AI agent orchestration
- generic workflow automation
- human-task orchestration
- broad connector marketplace
- reusable external pipeline marketplace

Those may come later, but they are not the product goal of v1.

---

## Core Design Principles

### 1) Assets first

There is only one public executable node type in v1:

- `@asset`

### 2) Small business-focused function DSL

Function attributes should only describe the asset itself.

### 3) Orchestration config stays outside function attributes

Pipelines, schedules, polling, and other triggers belong to orchestration/configuration, not to the business function DSL.

### 4) Built-in Elixir docs are first-class

Favn should use:

- `@moduledoc`
- `@doc`
- `@spec`

as the main documentation source for catalog and UI.

### 5) No direct data passing between assets

Dependencies express graph relationships and execution ordering.

Assets are expected to read from and write to external systems/storage as part of their own business logic.

### 6) Assets use runtime context

Assets use a single function shape:

```elixir
def my_asset(ctx)
```

Runtime information (trigger context, pipeline config, execution metadata, etc.) is provided through `ctx`.

---

## Function-Level DSL (v1)

These are the function attributes we focus on in v1:

- `@asset`
- `@depends`
- `@uses`
- `@freshness`
- `@meta`

### `@asset`

Marks a public function as a Favn asset with a name.

### `@depends`

Declares upstream asset dependencies.

Purpose:

- execution ordering
- lineage
- documentation
- freshness reasoning

This does not imply direct value passing between assets.

### `@uses`

Declares which external integrations or Elixir functions an asset uses.

Used for:

- lineage
- UI mapping
- documentation
- impact analysis

### `@freshness`

Declares how fresh the latest successful materialization must be for Favn to skip rerun.

### `@meta`

Holds non-execution metadata for catalog, filtering, ownership, and UI.

---

## Explicitly Out of Scope for v1 DSL

The following are intentionally not function attributes in v1:

- jobs
- schedules
- polling definitions
- webhook definitions
- API trigger definitions
- event trigger definitions
- checks
- observers
- metrics
- pipeline installation/config
- source DSL
- artifact/ref dependency modes
- data passing configuration between assets

---

## Runtime Contract Direction

The v1 asset function contract should be:

```elixir
@asset "My Asset"
def my_asset(ctx) do
  ...
end
```

`ctx` should eventually carry runtime information such as:

- run information
- trigger information
- freshness/attempt context
- pipeline-installed configuration
- partition/runtime-window context (when applicable)

---

## Roadmap

## v0.1.0 — Foundation

**Status: Released**

This release proves the core architecture and programming model.

### Features

- [x] Asset DSL (`@asset`) for defining business logic
- [x] Asset discovery and registry
- [x] Dependency graph construction (DAG)
- [x] Deterministic planning from dependencies
- [x] Local synchronous execution
- [x] Run model with outputs
- [x] Storage abstraction (in-memory)
- [x] Run event emission (PubSub)
- [x] Public API (`Favn` module)
- [x] Host application integration (supervision tree)
- [x] Basic tests across core modules

---

## v0.2.0 — Runtime Foundation

**Status: In Progress**

Turns Favn into a real execution engine with durable runtime state and concurrency.

### Features

- [x] Asynchronous run execution
- [x] Parallel execution with bounded concurrency
- [x] Run + step state machine
- [x] Retry mechanism
- [x] Cancellation support
- [x] Timeout handling
- [x] SQLite storage adapter
- [x] Stable event schema (runs + steps)
- [x] Internal runtime telemetry foundation
- [x] Coordinator / executor separation
- [ ] Remove current in-memory-output assumptions from the execution model
- [ ] Align runtime model with asset-first external materialization direction
- [ ] Simplify asset execution contract toward `def asset(ctx)`

---

## v0.3.0 — Asset DSL Refactor

**Status: Planned**

Refines authoring around the locked v1 asset model.

### Features

- [ ] Support single asset function shape: `def asset(ctx)`
- [ ] Remove requirement for arity-2 assets
- [ ] Remove requirement for `%Favn.Asset.Output{}`
- [ ] Replace `depends_on` option with repeatable `@depends`
- [ ] Add `@uses`
- [ ] Add `@freshness`
- [ ] Add `@meta`
- [ ] Remove `kind` from the public authoring DSL
- [ ] Keep built-in Elixir docs as the primary documentation source
- [ ] Update graph/catalog extraction from the new DSL

---

## v0.4.0 — Triggers and Orchestration Layer

**Status: Planned**

Introduces orchestration config outside the business DSL.

### Features

- [ ] Manual run trigger
- [ ] API-triggered execution
- [ ] Cron / schedule trigger
- [ ] Polling trigger
- [ ] Polling state / cursor tracking
- [ ] Freshness-aware skip behavior
- [ ] Initial orchestration layer outside function attributes
- [ ] Initial pipeline/configuration definition model
- [ ] Make pipeline/config available through `ctx`
- [ ] Stable operator actions: run, cancel, rerun

---

## v0.5.0 — Single-Node Asset Production Readiness

**Status: Planned**

Makes Favn production-usable for asset-based orchestration on a single node.

### Features

- [ ] Postgres storage adapter
- [ ] Queueing and admission control
- [ ] Concurrency controls
- [ ] Run deduplication / run keys
- [ ] Materialization metadata/history tracking
- [ ] Asset freshness state tracking
- [ ] Improved failure recovery
- [ ] Asset catalog foundation
- [ ] Graph and run inspection foundation
- [ ] Stronger testing around retries, reruns, and recovery

---

## v0.6.0 — Pipeline Composition Foundation

**Status: Planned**

Makes larger asset projects easier to install and configure.

### Features

- [ ] Graph composition across modules
- [ ] Pipeline-level asset selection
- [ ] Pipeline-level configuration/bindings
- [ ] Reusable asset graph installation inside an app
- [ ] Support accessing pipeline-installed configuration through `ctx`
- [ ] Keep pipeline model outside the function attribute DSL
- [ ] Preserve asset graph clarity in UI and docs

---

## v1.0.0 — Stable Asset-Oriented Orchestrator

**Status: Planned**

First stable release focused on asset-based ETL/ELT orchestration.

### Features

#### Authoring

- [ ] Stable asset DSL: `@asset`, `@depends`, `@uses`, `@freshness`, `@meta`
- [ ] Stable single asset function contract: `def asset(ctx)`

#### Execution

- [ ] Deterministic dependency-based execution
- [ ] Manual/API run initiation
- [ ] Schedule trigger
- [ ] Polling trigger
- [ ] Retry, timeout, cancellation, and rerun support
- [ ] Freshness-aware skip logic
- [ ] Stable run and step lifecycle

#### Data / materialization model

- [ ] Asset-first external materialization model
- [ ] Materialization metadata/history tracking
- [ ] No direct data passing between assets
- [ ] Runtime context support for orchestration-installed config

#### Orchestration layer

- [ ] Pipeline/configuration layer outside function DSL
- [ ] Pipeline/config accessible through `ctx`
- [ ] Graph composition support for larger applications

#### Operator experience

- [ ] Asset graph/catalog view
- [ ] Run history
- [ ] Materialization history
- [ ] Freshness visibility
- [ ] Failure/retry visibility
- [ ] Generated documentation from code + metadata

#### Storage/runtime

- [ ] Built-in storage: memory, SQLite, Postgres
- [ ] Strong single-node production story

---

## v2.0.0 — Asset Platform Expansion

**Status: Planned**

Deepens the asset-first model without changing the product focus.

### Features

- [ ] Multi-asset support
- [ ] Richer graph composition
- [ ] Better pipeline installation/reuse model
- [ ] More advanced orchestration config through pipelines
- [ ] Webhook trigger
- [ ] Event trigger
- [ ] Richer API trigger model
- [ ] Source/external asset observation model
- [ ] Asset checks
- [ ] Better partition/runtime-window support through orchestration context
- [ ] Distributed multi-node execution
- [ ] Resource-aware placement and scheduling
- [ ] Stronger plugin seams for storage, triggers, and execution
- [ ] `favn_view` more mature operator product
- [ ] `favn_demo` onboarding project

---

## Companion Projects

### `favn_view`

- [ ] Alpha after v1 execution + graph foundations are stable
- [ ] Usable asset catalog/operator UI during v1.x
- [ ] More complete operator experience in v2

### `favn_demo`

- [ ] Initial demo project after core v1 direction is stable
- [ ] Better onboarding/demo project in v2

---

## Notes

- This roadmap is intentionally narrower than earlier versions.
- v1 and v2 are focused on making Favn excellent at asset-based ETL/ELT orchestration.
- General workflow automation and AI-agent orchestration are not part of the current product goal.
- Priority remains: correct architecture > fast feature delivery.