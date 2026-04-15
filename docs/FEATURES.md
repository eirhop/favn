# Favn Roadmap & Feature Status

## Current Version

**Current release: v0.4.0**

## Current Focus

Favn `v0.5.0` is now a refactor release, not a hardening pass on the current monolith.

- [x] v0.5 roadmap replaced with the umbrella refactor plan in `docs/REFACTOR.md`
- [x] Phase 0 umbrella app list defined
- [x] Phase 0 dependency direction rules defined
- [x] Phase 0 app responsibilities narrowed around authoring/core, orchestrator, runner, and view boundaries
- [x] Phase 0 migration rules and legacy deletion criteria defined

Near-term priority is to execute the refactor in the locked migration order:

- umbrella creation and legacy isolation
- public DSL/domain/compiler extraction into `favn` and `favn_core`
- manifest generation and manifest version pinning
- separate runner boundary
- separate orchestrator boundary
- storage adapters for memory, SQLite, and Postgres
- DuckDB as the first optional runner plugin
- separate view runtime and local developer tooling

## Terminology

- **Asset** — a Favn executable unit
- **Source** — an external relation declared via `Favn.Source`, used in lineage but not executed
- **Relation** — warehouse identity: connection, catalog, schema, name
- **Connection** — reusable backend configuration used by assets and pipelines
- **Adapter** — implementation for a backend such as DuckDB, Snowflake, or Databricks

---

## v0.1.0 — Foundation

**Status: Released**

- [x] Asset DSL (`@asset`)
- [x] Asset discovery and registry
- [x] Dependency graph construction
- [x] Deterministic planning from dependencies
- [x] Local synchronous execution
- [x] Initial run model
- [x] In-memory storage
- [x] Run event emission
- [x] Public `Favn` API
- [x] Host application integration
- [x] Core test coverage

---

## v0.2.0 — Runtime Foundation

**Status: Released**

- [x] Asynchronous run execution
- [x] Parallel execution with bounded concurrency
- [x] Run and step state machine
- [x] Retry support
- [x] Cancellation support
- [x] Timeout handling
- [x] SQLite storage adapter
- [x] Stable run and step event schema
- [x] Coordinator and executor separation
- [x] Asset execution contract aligned around `def asset(ctx)`
- [x] Runtime model aligned with external materialization direction
- [x] Internal assets namespacing cleanup

---

## v0.3.0 — Pipelines, Scheduler, and Windowing

**Status: Released**

Goal: make Favn usable for scheduled and windowed asset execution.

- [x] Documentation sync for window-aware DSL examples across README and `Favn` moduledoc
- [x] Working pipeline execution
  - Plan: `docs/WORKING_PIPELINE_EXECUTION_PLAN.md`
  - Scope: manual `run_pipeline/2`, deterministic `plan_pipeline/2`, anchor-window runs, `backfill_pipeline/2`, and pipeline rerun/cancel/timeout/retry/resume semantics
- [x] Working scheduler trigger runtime
  - Plan: `docs/WORKING_SCHEDULER_TRIGGER_RUNTIME_PLAN.md`
  - Scope: scheduler runtime engine startup, pipeline schedule discovery, active/inactive schedule handling, missed/overlap policy enforcement, trigger metadata submission through `run_pipeline/2`, and single-node persisted scheduler state recovery
- [x] Pipeline `window` clause
- [x] Asset-level `@window`
- [x] Runtime window domain primitives (`Favn.Window.*`)
- [x] `ctx.window`
- [x] `ctx.pipeline.anchor_window`
- [x] Runtime/coordinator step identity keyed as `{asset_ref, window_key}` (current scaffold uses `nil` window keys)
- [x] Plan-level `target_node_keys` scaffold for node-key target completion semantics
- [x] Plan-level `node_stages` scaffold for node-key stage iteration in runtime restore paths
- [x] Window-aware planner identity `{asset_ref, window_key}` (v1 anchor-window expansion)
- [x] Window-aware execution and retry semantics (node-key runtime state/coordinator)
- [x] Node-key keyed persisted rerun resume state for window-expanded runs
- [x] SQLite persisted window state foundation (`run_node_results` + `window_latest_results`)
- [x] Backfill API and range expansion
- [x] Public backfill APIs (`Favn.backfill_asset/2`, `Favn.backfill_pipeline/2`)
- [x] Planner anchor-range expansion via `Anchor.expand_range/4` with cross-anchor node-key dedupe
- [x] Backfill provenance persisted on runs (`run.backfill`, `pipeline.backfill_range`, `pipeline.anchor_ranges`)
- [x] Freshness policy checks over persisted node/window run state
- [x] Initial snapshot persistence failure cleanup (terminate spawned coordinator on submit failure)
- [x] Cron day-of-month/day-of-week semantics aligned with standard OR behavior
- [x] Freshness scan correctness no longer defaults to capped run history
- [x] SQLite scheduler datetime parsing hardened for malformed persisted values
- [x] Release metadata/version docs aligned to v0.3.0
- [x] SQL-readiness asset compiler seam for future SQL frontend integration
- [x] Manual run support with explicit anchor windows
- [x] Replace `partition` direction with runtime windowing

---

## v0.4.0 — SQL Foundation and DuckDB

**Status: Released**

Goal: first complete SQL workflow on top of the shared runtime window model.

- [x] `Favn.Connection` behaviour and reusable connection model
  - [x] Connection foundation architecture design doc (`docs/CONNECTION_FOUNDATION_ARCHITECTURE.md`)
- [x] `Favn.SQL.Adapter` behaviour
  - [x] Adapter architecture design doc (`docs/SQL_ADAPTER_ARCHITECTURE.md`)
- [x] DuckDB/duckdbex architecture recommendation and implementation request scope (`docs/sql_adapter_scope.md`)
- [x] DuckDB adapter foundation (duckdbex-backed connect/query/introspection/materialization baseline)
  - [x] Appender-backed table writes preserve normal `WritePlan` create semantics
  - [x] Appender lifecycle cleanup is explicit on failure paths
- [x] Phase 1 shared relation foundation
  - [x] `Favn.RelationRef`
  - [x] `%Favn.Asset{relation: ...}`
  - [x] `Favn.Namespace` config inheritance for `connection` / `catalog` / `schema` via grouped `relation:` key
  - [x] `Favn.Assets` support for `@relation`
  - [x] runtime exposure through `ctx.asset.relation`
  - [x] relation normalization and validation (`database`/`table` aliases included)
  - [x] relation uniqueness checks
  - [x] relation index in the registry catalog
- [x] Phase 2 single-asset Elixir DSL (`Favn.Asset`)
  - [x] one-module-per-asset `def asset(ctx)` convention
  - [x] `@doc`, `@meta`, `@depends`, `@window`, `@relation`
  - [x] relation default inheritance via `Favn.Namespace`
  - [x] canonical compile output as one `%Favn.Asset{ref: {Module, :asset}}`
  - [x] module dependency shorthand normalization (`@depends Some.AssetModule`)
  - [x] `@relation true` relation-name inference from module leaf (`Macro.underscore/1`)
  - [x] module convenience lookup via `Favn.get_asset(module)` for single-asset modules
- [x] Phase 3 single-asset SQL DSL (`Favn.SQLAsset`)
  - [x] one-module-per-asset SQL DSL with `query do ... end` and a real `~SQL""" ... """` sigil
  - [x] `@doc`, `@meta`, `@depends`, `@window`, `@materialized`, `@relation`
  - [x] relation default inheritance via `Favn.Namespace`
  - [x] canonical compile output as one `%Favn.Asset{ref: {Module, :asset}}`
  - [x] module convenience lookup via `Favn.get_asset(module)` for single-asset modules
  - [x] explicit runtime guard until SQL execution lands
- [x] Phase 3 follow-up: finalize SQL authoring DSL before runtime integration
  - [x] `Favn.SQL` reusable SQL modules via `defsql ... do ... end`
  - [x] keep `~SQL` as the only SQL body syntax for both `query` and `defsql`
  - [x] one `@name` placeholder/input model across asset queries and reusable SQL
  - [x] reserved runtime SQL inputs such as `@window_start` / `@window_end`
  - [x] param-driven SQL inputs for non-reserved `@name` placeholders
  - [x] expression SQL macros and relation SQL macros from day one
  - [x] direct asset references in relation position, e.g. `from MyApp.Raw.Sales.Orders`
  - [x] scanner-based ordered SQL IR instead of plain-string-only storage
  - [x] compile-time normalization and validation of reusable SQL definitions/calls
  - [x] duplicate visible reusable SQL definition detection across local/imported definitions
  - [x] reusable SQL cycle detection
  - [x] deferred symbolic asset refs for not-yet-compiled modules
- [x] DuckDB adapter hardening for runtime execution
  - [x] Explicit DuckDB runtime ownership/cleanup rules for db/conn/result/appender handles
  - [x] DuckDB error normalization and rollback-failure surfacing via `%Favn.SQL.Error{}`
  - [x] Transactional hardening for multi-step writes aligned with DuckDB transaction semantics
  - [x] Concurrency semantics aligned with DuckDB optimistic conflict behavior (no blanket write lock)
  - [x] Internal duckdbex client boundary for later first-party package extraction
- [x] Typed external relational sources
  - [x] Unified `relation` DSL naming
  - [x] `Favn.Source` DSL for external relations
  - [x] `@relation` in `Favn.Asset` (renamed from `@produces`)
  - [x] `@relation` in `Favn.SQLAsset` (with inference by default)
  - [x] `Favn.Namespace` grouped relation defaults: `relation: [connection: ..., catalog: ..., schema: ...]`
  - [x] Relation-based dependency inference with registered sources
  - [x] `%Favn.Asset{type: :source}` for non-materializing sources
  - [x] Removed `@produces` legacy naming
- [x] Phase 4a runtime integration for SQL assets
  - [x] SQL execution through shared runtime
  - [x] SQL helper APIs (`Favn.render/2`, `Favn.preview/2`, `Favn.explain/2`, `Favn.materialize/2`)
  - [x] strict render result struct with final SQL, canonical bound params, and diagnostics metadata
  - [x] `render/2` remains backend-free and session-free; it resolves compiled metadata but does not hit adapters
  - [x] runtime SQL input resolution and parameter binding from explicit `params` plus normalized runtime inputs
  - [x] renderer always emits one backend-neutral positional binding model; adapters may rewrite placeholders if required
  - [x] render-time deferred asset-ref resolution and fully inlined `defsql` expansion from the SQL IR
  - [x] cross-connection direct asset refs fail during render with clear diagnostics
  - [x] materialization planning for `:view` and `:table`
  - [x] `preview/2` returns both canonical rendered SQL and the actual executed preview statement
- [x] Phase 4b incremental SQL materialization
  - [x] incremental planning layer that extends render output into backend-agnostic `WritePlan`s
  - [x] first safe incremental strategies: `:append` and `:delete_insert`
  - [x] explicit runtime guard for unsupported incremental strategies such as `:merge` and current `:replace`
  - [x] window-aware incremental execution and explicit lookback handling
- [x] Generated multi-asset Elixir DSL for repetitive extraction assets (`Favn.MultiAsset`)
- [x] SQL file support
  - [x] `query file: "..."` for SQL asset main queries
  - [x] `defsql ..., file: "..."` for reusable SQL definitions
  - [x] compile-time-only file loading with project-local path validation
  - [x] `.sql` file diagnostics and Mix recompilation via external resource tracking
- [x] Phase 5 relation-based SQL inference
  - [x] dependency inference from typed SQL relation references
  - [x] SQL asset dependency planning on the shared window-aware runtime model
  - [x] additive explicit + inferred dependency merge with provenance metadata
  - [x] unmanaged external relation warnings and ambiguous ownership errors
  - [x] follow-up hardening: nested/subquery `WITH` CTE alias exclusion tests for relation inference
- [x] Documentation refactor for public DSL discoverability and AI-agent routing
- [x] Registered source/external dependency model (typed relation identities)

---

## v0.5.0 — Umbrella Refactor

**Status: In Progress**

Goal: turn Favn into a manifest-first product with separate authoring, runner, orchestrator, and view boundaries.

Pre-refactor groundwork already completed in the legacy runtime and to be carried forward during migration:

- [x] PostgreSQL storage foundation architecture plan (`docs/POSTGRES_STORAGE_FOUNDATION_PLAN.md`)
- [x] PostgreSQL storage adapter implementation

- [x] Phase 0: freeze and reframe
  - [x] old v0.5 roadmap marked as replaced
  - [x] umbrella app list locked
  - [x] dependency direction rules locked
  - [x] app role boundaries locked (`favn_core` compiler/manifest, orchestrator-owned storage/APIs, runner-owned execution plugins, view-through-orchestrator)
  - [x] migration rules locked
  - [x] legacy deletion criteria defined
- [x] Phase 1: create umbrella and isolate `favn_legacy`
- [ ] Phase 2: move public DSL and domain/compiler foundation into `favn` and `favn_core`, including manifest model/generation foundations
  - [x] public `Favn.*` namespace ownership established under `apps/favn/lib`
  - [x] user business code can compile against `favn` for migrated authoring/manifest paths
  - [x] compile-time DSL usage through `favn` is established
  - [x] initial manifest generation foundation exists without starting runtime services
  - [x] unsupported migrated runtime SQL paths fail deterministically with `{:error, :runtime_not_available}`
  - [x] canonical domain/compiler foundation physically re-centered into `favn_core`
  - [x] post-Phase-2 re-thinning: move internal-only compiler/manifest/planning machinery back into `favn_core` while keeping `favn` as thin public surface
  - [ ] remove temporary migration seams once runner/runtime boundary ownership is finalized
- [x] Phase 3: implement persisted manifest schema and manifest version pinning
  - [x] planning docs created: `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md`
  - [x] implementation checklist created: `docs/refactor/PHASE_3_TODO.md`
  - [x] initial `favn_core` Phase 3 foundation modules added (`manifest/build`, `manifest/serializer`, `manifest/identity`, `manifest/compatibility`, `manifest/version`, runner contracts)
  - [x] first ownership re-centering slice completed for pure value objects (`Favn.Ref`, `Favn.RelationRef`, `Favn.Timezone`, `Favn.Diagnostic`) from `favn` to `favn_core`
  - [x] second ownership re-centering slice completed for window and schedule internals (`Favn.Window.*` structs/helpers and `Favn.Triggers.Schedule`) from `favn` to `favn_core`
  - [x] third ownership re-centering slice completed for asset/compiler/connection internals (`Favn.Asset.Dependency`, `Favn.Asset.RelationInput`, `Favn.Asset.RelationResolver`, `Favn.DSL.Compiler`, `Favn.Connection.Definition`) from `favn` to `favn_core`
  - [x] canonical runtime manifest schema locked
  - [x] manifest build output split from canonical persisted manifest payload
  - [x] manifest version id/hash strategy implemented
  - [x] canonical serializer and hashing implemented
  - [x] manifest/runner compatibility validation implemented
  - [x] shared runner/orchestrator contract DTOs introduced in `favn_core`
  - [x] contract lock tests added before Phase 4 runner implementation
- [ ] Phase 4: build the runner boundary in `favn_runner`
- [ ] Phase 5: build the orchestrator boundary in `favn_orchestrator`
- [ ] Phase 6: add `favn_storage_sqlite` and `favn_storage_postgres`
- [ ] Phase 7: move DuckDB into `favn_duckdb`
- [ ] Phase 8: add `favn_view`
- [ ] Phase 9: ship developer tooling and packaging flows
- [ ] Phase 10: cut over and delete legacy runtime paths

Detailed migration planning for the current refactor slices lives in:

- `docs/refactor/PHASE_2_MIGRATION_PLAN.md`
- `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md`
- `docs/refactor/PHASE_3_TODO.md`

Deferred until after the refactor unless needed to establish the new boundaries:

- [ ] Queueing and admission control
- [ ] Concurrency controls
- [ ] Run deduplication / run keys
- [ ] Improved failure recovery
- [ ] Materialization history tracking
- [ ] Asset and window state inspection
- [ ] Asset dependency provenance and relation-lineage inspection APIs
- [ ] Better rerun and replay ergonomics
- [ ] Stronger test coverage for runtime, scheduler, windowing, and SQL execution
- [ ] Operator-facing graph and run inspection foundation
- [ ] DuckLake connection and snapshot foundation
- [ ] Advanced multi-node scheduling and resource-aware placement

---

## v1.0.0 — Stable ETL/ELT Orchestrator

**Status: Planned**

Goal: stable, ergonomic, production-usable orchestration on top of the refactored product architecture.

- [ ] Stable asset authoring model
- [ ] Stable pipeline authoring model
- [ ] Stable scheduler trigger
- [ ] Stable runtime windowing and backfills
- [ ] Stable connection and adapter architecture
- [ ] Stable SQL asset authoring model
- [ ] Stable materialization model
- [ ] Stable dependency-driven execution for Elixir and SQL assets
- [ ] Stable single-node packaging
- [ ] Stable split deployment packaging
- [ ] Built-in storage options: memory, SQLite, Postgres
- [ ] Clear documentation and graph-oriented developer experience
- [ ] DuckLake advanced capabilities
  - [ ] Change-feed and row-lineage support (`rowid`, snapshot diffs)
  - [ ] Schema-evolution-aware runtime behavior
  - [ ] Optional macro publishing from `defsql`
  - [ ] Maintenance operations (snapshot cleanup, compaction helpers)

---

## Beyond v1

Later, but not part of the immediate priority:

- [ ] More SQL adapters
- [ ] Polling trigger
- [ ] API trigger
- [ ] Distributed multi-node execution
- [ ] Resource-aware scheduling
- [ ] `favn_demo`
