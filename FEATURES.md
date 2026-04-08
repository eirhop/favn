# Favn Roadmap & Feature Status

## Current Version

**Current release: v0.3.0**

## Current Focus

Favn is an asset-first orchestrator for ETL/ELT workloads.

Near-term priority is a practical path to real usage:

- working pipelines
- working scheduler trigger
- runtime windowing and backfills
- reusable connection and adapter foundations
- SQL assets with materializations, dependencies, and windowing
- DuckDB as the first SQL backend

## Terminology

- **Asset** — a Favn executable unit
- **Source** — an external relation Favn reads from but does not build
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

**Status: Release candidate**

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

---

## v0.4.0 — SQL Foundation and DuckDB

**Status: Planned**

Goal: first complete SQL workflow on top of the shared runtime window model.

- [ ] `Favn.Connection` behaviour and reusable connection model
- [ ] `Favn.SQL.Adapter` behaviour
- [ ] DuckDB adapter
- [ ] Typed source identities
- [ ] `Favn.SQL` / `Favn.SQLAssets` authoring model
- [ ] Multi-asset SQL modules
- [ ] Inline SQL support
- [ ] SQL file support
- [ ] Dependency inference from typed SQL references
- [ ] Materialization strategies: `:view`, `:table`, `:incremental`
- [ ] SQL asset windowing
- [ ] SQL asset dependency planning on the shared window-aware runtime model
- [ ] Window-aware incremental execution
- [ ] Lookback handling for incremental SQL assets

---

## v0.5.0 — Hardening and Production Readiness

**Status: Planned**

Goal: make single-node production usage reliable and inspectable.

- [ ] Postgres storage adapter
- [ ] Queueing and admission control
- [ ] Concurrency controls
- [ ] Run deduplication / run keys
- [ ] Improved failure recovery
- [ ] Materialization history tracking
- [ ] Asset and window state inspection
- [ ] Better rerun and replay ergonomics
- [ ] Stronger test coverage for runtime, scheduler, windowing, and SQL execution
- [ ] Operator-facing graph and run inspection foundation

---

## v1.0.0 — Stable ETL/ELT Orchestrator

**Status: Planned**

Goal: stable, ergonomic, production-usable orchestration for Elixir and SQL assets.

- [ ] Stable asset authoring model
- [ ] Stable pipeline authoring model
- [ ] Stable scheduler trigger
- [ ] Stable runtime windowing and backfills
- [ ] Stable connection and adapter architecture
- [ ] Stable SQL asset authoring model
- [ ] Stable materialization model
- [ ] Stable dependency-driven execution for Elixir and SQL assets
- [ ] Strong single-node production story
- [ ] Built-in storage options: memory, SQLite, Postgres
- [ ] Clear documentation and graph-oriented developer experience

---

## Beyond v1

Later, but not part of the immediate priority:

- [ ] More SQL adapters
- [ ] Polling trigger
- [ ] API trigger
- [ ] Distributed multi-node execution
- [ ] Resource-aware scheduling
- [ ] `favn_view`
- [ ] `favn_demo`
