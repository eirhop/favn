Favn Roadmap & Feature Status

## Current Version

**Current release: v0.2.0**

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
- `@meta`
- `@depends`
`@uses` and `@freshness` are intentionally deferred for later iterations.

### `@asset`

Marks a public function as a Favn asset with a name.

### `@depends`

Declares upstream asset dependencies.

Purpose:

- execution ordering
- lineage
- documentation
- lineage reasoning

Shape in v0.2: one dependency per attribute declaration. For multiple dependencies, repeat `@depends`.

This does not imply direct value passing between assets.

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
- [x] Initial run model
- [x] Storage abstraction (in-memory)
- [x] Run event emission (PubSub)
- [x] Public API (`Favn` module)
- [x] Host application integration (supervision tree)
- [x] Basic tests across core modules

---

## v0.2.0 — Runtime Foundation + DSL Contract Refactor

**Status: Released**

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
- [x] Remove current in-memory-output assumptions from the execution model
- [x] Align runtime model with asset-first external materialization direction
- [x] Simplify asset execution contract toward `def asset(ctx)`

---

## v0.2 DSL decisions (approved April 4, 2026)

These decisions are part of the remaining v0.2 scope and should be treated as source-of-truth:

### Approved DSL decisions (April 4, 2026)

The following DSL decisions are approved and should be treated as source-of-truth for the remaining v0.2 refactor:

- Canonical attribute order in examples and docs:
  1. `@asset "name"`
  2. `@meta ...`
  3. `@doc ...`
  4. `@depends ...` (repeatable, single entry each)
  5. `@spec ...`
  6. `def asset(ctx) do ... end`
- `@depends` supports only single-entry declarations; repeat the attribute for additional dependencies.
- `@uses` is out of scope for this PR and deferred to later pipeline design work.
- Missing `@doc` or `@spec` should not emit warnings/errors.
- Asset returns may be `:ok`, `{:ok, map()}`, or `{:error, reason}`.

### Remaining v0.2 refactor PR slice (current PR scope)

This refactor PR should focus on contract cleanup only (no new orchestration features):

- [x] Refactor `@asset` option usage by moving dependency declarations to repeatable `@depends` and non-execution metadata to `@meta`.
- [x] Remove direct value-passing assumptions from asset execution and runtime expectations.
- [x] Require/normalize single-arity asset functions (`def asset(ctx)`).
- [x] Remove `%Favn.Asset.Output{}` as a required public return wrapper.
- [x] Update docs (`lib/favn.ex`, `README.md`, and examples) to the approved DSL and ordering.
- [x] Keep roadmap and feature status aligned with these decisions.

---

## v0.3.0 — Initial Orchestration Layer

**Status: In Progress**

Builds the first orchestration layer on top of the finalized v0.2 asset/runtime contract.

### v0.3 decisions (approved April 6, 2026)

The following decisions are source-of-truth for the first v0.3 pipeline foundation PR:

- Pipeline is a composition/orchestration layer, not a second planning system.
- Pipeline selection/resolution must hand final target refs to the existing dependency planner/runtime.
- Initial public pipeline DSL is intentionally tiny and user-friendly:
  - `asset`
  - `assets`
  - `select`
  - `deps`
  - `schedule`
  - `partition`
  - `source`
  - `outputs`
- Selection authoring supports shorthand (`asset` / `assets`) and flexible selection (`select do ... end`).
- A pipeline definition must use either shorthand selection or `select`, but not both.
- First v0.3 foundation PR scope is code-defined pipelines + manual pipeline execution + pipeline-aware `ctx`.
- Scheduler/polling/API triggers and DB-managed installations are deferred from this first PR.
- Initial v0.3 pipeline DSL starts with `asset`, `assets`, and `select` for selection.
- A later version may introduce `where` for richer asset filtering/querying.
- `where` is a filtering layer only and does not replace dependency-based graph planning.

### Features

- [x] `Favn.run_pipeline/2` manual pipeline trigger
- [x] `Favn.plan_pipeline/2` pipeline planning API
- [x] Initial code-defined tiny pipeline DSL (`asset`, `assets`, `select`, `deps`)
- [x] Deterministic pipeline selector resolution (ref/module/tag/category)
- [x] Initial orchestration layer outside function attributes
- [x] Initial pipeline configuration definition model
- [x] Reusable schedule trigger DSL (`Favn.Triggers.Schedules`) with repeated top-level `schedule` declarations
- [x] Pipeline schedule clause supports explicit schedule refs (`{Module, :name}`) and inline schedule keywords
- [x] Remove temporary atom-only schedule references from pipeline DSL
- [x] Resolve pipeline schedules into normalized trigger schedule structs in `ctx.pipeline.schedule`
- [x] Configurable scheduler default timezone (`config :favn, scheduler: [default_timezone: ...]`)
- [x] Validate cron expressions and timezone identifiers during schedule authoring
- [x] Make pipeline/config/trigger context available through `ctx`
- [x] Internal module namespacing for asset internals (`Favn.Assets.Registry`, `Favn.Assets.GraphIndex`, `Favn.Assets.Planner`)
- [x] Stable operator actions: run, cancel, rerun
- [x] Basic partition/runtime-window slot in `ctx` (initially `nil` for manual runs)

### Deferred after first v0.3 pipeline foundation PR

- [ ] API-triggered execution
- [ ] Cron/schedule runtime engine
- [ ] Schedule missed-run runtime semantics (`missed: :one | :all`)
- [ ] Schedule overlap runtime semantics (`overlap: :allow | :queue_one`)
- [ ] Schedule `catchup_limit` option
- [ ] Polling trigger runtime
- [ ] Polling state/cursor tracking
- [ ] Backfills
- [ ] DB-managed pipeline installations/runtime overrides
- [ ] Rich `where` clause filtering/query DSL
- [ ] Source/output runtime binding behaviors
- [ ] Multi-output runtime fan-out behavior

---

## v0.4.0 — Freshness, Pipeline Composition, and Installed Context

**Status: Planned**

Expands the orchestration layer with freshness, pipeline composition, and installed runtime configuration.

### Features

- [ ] Freshness-aware skip behavior
- [ ] Richer freshness behavior
- [ ] Introduce `@uses` after pipeline/runtime integration design is finalized
- [ ] Graph composition across modules
- [ ] Pipeline-level asset selection
- [ ] Pipeline-level configuration/bindings
- [ ] Reusable named orchestration components (`schedule`, `partition`, `source`, `output`) declared inline or by reference
- [ ] Pipeline references to reusable orchestration component definitions
- [ ] Future `where` clause for richer asset filtering/querying (filtering layer only; planner remains dependency-graph based)
- [ ] Reusable asset graph installation inside an app
- [ ] Code-defined pipeline templates + DB-managed mutable installations/runtime config
- [ ] Support accessing pipeline-installed configuration through `ctx`
- [ ] Keep pipeline model outside the function attribute DSL
- [ ] Preserve asset graph clarity in UI and docs

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
