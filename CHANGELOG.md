# Changelog

All notable changes to this project are documented in this file.

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
