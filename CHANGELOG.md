# Changelog

All notable changes to this project are documented in this file.

## [0.4.0] - 2026-04-13

### Changed

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

## [0.3.0] - 2026-04-08

### Changed
Goal: make Favn usable for scheduled and windowed asset execution.

- [x] Documentation sync for window-aware DSL examples across README and `Favn` moduledoc
- [x] Working pipeline execution
  - Plan: `WORKING_PIPELINE_EXECUTION_PLAN.md`
  - Scope: manual `run_pipeline/2`, deterministic `plan_pipeline/2`, anchor-window runs, `backfill_pipeline/2`, and pipeline rerun/cancel/timeout/retry/resume semantics
- [x] Working scheduler trigger runtime
  - Plan: `WORKING_SCHEDULER_TRIGGER_RUNTIME_PLAN.md`
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

## [0.2.0] - 2026-03-27

### Changed
- Refactor `@asset` option usage by moving dependency declarations to repeatable `@depends` and non-execution metadata to `@meta`.
- Remove direct value-passing assumptions from asset execution and runtime expectations.
- Require/normalize single-arity asset functions (`def asset(ctx)`).
- Remove `%Favn.Asset.Output{}` as a required public return wrapper.

### Added
Turned Favn into a real execution engine with durable runtime state and concurrency.

- Asynchronous run execution
- Parallel execution with bounded concurrency
- Run + step state machine
- Retry mechanism
- Cancellation support
- Timeout handling
- SQLite storage adapter
- Stable event schema (runs + steps)
- Internal runtime telemetry foundation
- Coordinator / executor separation
- Remove current in-memory-output assumptions from the execution model
- Align runtime model with asset-first external materialization direction
- Simplify asset execution contract toward `def asset(ctx)`

## [0.1.1] - 2026-03-27

### Changed
- Full renaming of repo from Flux to Favn

## [0.1.0] - 2026-03-27

### Added

- Asset authoring DSL via `use Flux.Assets` and `@asset` declarations with compile-time metadata capture.
- Registry-backed public asset discovery APIs (`list_assets/0`, `list_assets/1`, `get_asset/1`).
- DAG-backed dependency graph inspection APIs (`upstream_assets/2`, `downstream_assets/2`, `dependency_graph/2`).
- Deterministic run planner with target normalization, deduplication, and stage grouping.
- Synchronous runtime runner with canonical output/error contracts.
- Storage facade with normalized public error contract for run retrieval/listing.
- Run events API with run-topic subscribe/unsubscribe and lifecycle notifications.
- Host app startup integration via configured asset modules, graph indexing, PubSub, and storage adapter configuration.
- Baseline CI workflow for compilation and test coverage of the public API surface.
- Installation documentation for this first release recommends pinning the public git tag (`v0.1.0`) when adding Flux as a dependency.
- Test environment restoration isolates storage adapter configuration across tests/doctests for repeatable public API validation.
