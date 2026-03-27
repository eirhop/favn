# Flux Feature Roadmap

## Versioning model

Flux is planned in three stages:

* **v0.1** — testable core release
* **v0.2** — complete core usable in development
* **v0.3** — production-grade release

This document is intended to be implementation-oriented. Each version includes:

* a goal description
* a feature list with status
* a scope description for each feature
* a task checklist that AI agents can execute against

Status values used in this document:

* **done** — implemented and expected to ship in that version
* **planned** — intended for that version but not yet implemented
* **candidate** — under consideration for that version
* **deferred** — intentionally moved to a later version

---

## v0.1 — Testable core release

### Goal

Ship a small but coherent core library that can be installed, exercised, and tested in host applications. The focus of v0.1 is correctness of the public API and the core runtime model, not production guarantees.

### Features

#### 1. Asset authoring DSL

**Status:** done

**Scope**

Provide a way to define business-oriented assets as normal Elixir functions with Flux metadata attached through `use Flux.Assets` and `@asset` annotations.

**Todo list**

* [ ] Verify the DSL supports asset declaration on normal functions.
* [ ] Verify metadata capture for docs, dependencies, kind, tags, source file, and source line.
* [ ] Verify both local and cross-module dependency references are supported.
* [ ] Add or confirm tests for compile-time metadata extraction.
* [ ] Add or confirm tests for invalid asset declarations.

#### 2. Asset registry and discovery

**Status:** done

**Scope**

Expose a stable public surface for listing configured assets and fetching asset metadata by canonical reference.

**Todo list**

* [ ] Verify `Flux.list_assets/0` returns the configured global asset catalog.
* [ ] Verify `Flux.list_assets/1` returns only assets for a target module.
* [ ] Verify `Flux.get_asset/1` returns canonical metadata.
* [ ] Verify unknown modules return `:not_asset_module`.
* [ ] Verify missing assets return `:asset_not_found`.
* [ ] Add regression tests for duplicate or conflicting asset references.

#### 3. Dependency graph model and inspection

**Status:** done

**Scope**

Model dependencies as a DAG and expose read APIs for upstream/downstream inspection and filtered subgraph generation.

**Todo list**

* [ ] Verify DAG validation rejects cycles.
* [ ] Verify shared upstream dependencies are supported.
* [ ] Verify `Flux.upstream_assets/2` returns deterministic results.
* [ ] Verify `Flux.downstream_assets/2` returns deterministic results.
* [ ] Verify `Flux.dependency_graph/2` respects filters for tags, kinds, modules, names, direction, transitive, and include_target.
* [ ] Add tests for empty and single-node graphs.

#### 4. Deterministic run planner

**Status:** done

**Scope**

Build a deterministic execution plan from one or more targets, with deduplicated nodes, topological ordering, and stage grouping.

**Todo list**

* [ ] Verify targets are normalized, deduplicated, and sorted.
* [ ] Verify the topological order is deterministic.
* [ ] Verify stage assignment is deterministic.
* [ ] Verify nodes run at most once per plan.
* [ ] Verify `dependencies: :all` and `dependencies: :none` semantics.
* [ ] Add regression tests for multi-target planning.

#### 5. Synchronous run execution

**Status:** done

**Scope**

Provide a first execution runtime that runs planned work synchronously and returns a structured run result.

**Todo list**

* [ ] Verify assets are invoked as `asset(ctx, deps)`.
* [ ] Verify dependency outputs are passed using canonical refs.
* [ ] Verify success and error return contracts are enforced.
* [ ] Verify execution follows deterministic planner order.
* [ ] Verify failures stop execution in a predictable way.
* [ ] Add tests for dependency mode behavior.

#### 6. Run store and run inspection

**Status:** done

**Scope**

Persist run metadata through the configured storage adapter and expose `get_run/1` and `list_runs/1` as stable public APIs.

**Todo list**

* [ ] Verify run records are created for started runs.
* [ ] Verify final statuses are persisted.
* [ ] Verify `Flux.get_run/1` returns expected run state.
* [ ] Verify `Flux.list_runs/1` supports status filtering and limit.
* [ ] Verify storage-layer failures are normalized into public errors.
* [ ] Add regression tests for invalid options.

#### 7. Run events

**Status:** done

**Scope**

Expose live run event subscription as a best-effort observability mechanism for active runs.

**Todo list**

* [ ] Verify `Flux.subscribe_run/1` subscribes successfully.
* [ ] Verify `Flux.unsubscribe_run/1` unsubscribes successfully.
* [ ] Verify run lifecycle events are emitted in expected order.
* [ ] Verify event delivery is observability-only and not part of run correctness.
* [ ] Add tests for event payload consistency.

#### 8. Host application integration

**Status:** done

**Scope**

Support use of Flux as an OTP dependency configured through host app application config.

**Todo list**

* [ ] Verify startup loads configured asset modules.
* [ ] Verify the global graph index is built during boot.
* [ ] Verify pubsub server configuration works.
* [ ] Verify storage adapter configuration works.
* [ ] Verify docs clearly describe startup and configuration lifecycle.

#### 9. Release hygiene for first public tag

**Status:** candidate

**Scope**

Ensure the repository is ready for a public `v0.1.0` release from a packaging and maintainability perspective.

**Todo list**

* [ ] Verify `LICENSE` file exists and matches package metadata.
* [ ] Add release notes or `CHANGELOG.md` entry for `v0.1.0`.
* [ ] Verify README installation instructions match actual release workflow.
* [ ] Verify package metadata in `mix.exs` is complete.
* [ ] Verify CI covers the public API surface at a minimum.

---

## v0.2 — Complete core usable in development

### Goal

Expand Flux from a testable runtime into a complete development-grade core. v0.2 should allow a developer to define assets, schedule runs, execute work asynchronously with controlled parallelism, inspect results across restarts, and iterate locally with confidence.

### Features

#### 1. Durable storage via SQLite adapter

**Status:** planned

**Scope**

Add a durable local storage adapter backed by SQLite so run history and execution state survive node restarts and can be queried during development.

**Todo list**

* [ ] Define the storage adapter behavior contract more explicitly if needed.
* [ ] Design the SQLite schema for runs, run steps, statuses, timestamps, errors, and stored metadata.
* [ ] Implement a SQLite storage adapter module.
* [ ] Add migrations or schema bootstrap logic.
* [ ] Persist run creation, updates, and final state transitions.
* [ ] Persist per-step execution records.
* [ ] Implement `get_run/1` support on top of SQLite.
* [ ] Implement `list_runs/1` support on top of SQLite.
* [ ] Add tests for restart persistence.
* [ ] Add tests for storage errors and corrupted-state handling.

#### 2. Richer run and step inspection

**Status:** planned

**Scope**

Expand the stored and returned run model so developers can inspect execution in detail, including step-level progress and outcomes.

**Todo list**

* [ ] Define the canonical step record shape.
* [ ] Add per-step status fields such as queued, running, ok, error, skipped, cancelled.
* [ ] Add per-step start and finish timestamps.
* [ ] Add per-step duration fields or derived timing support.
* [ ] Add step error capture with normalized structure.
* [ ] Decide whether outputs, output metadata, or output references are persisted.
* [ ] Update `Flux.Run` to reflect the expanded model.
* [ ] Add tests for partial and completed run inspection.

#### 3. Asynchronous execution

**Status:** planned

**Scope**

Allow runs to execute asynchronously so the caller can start a run and inspect it while work continues in the background.

**Todo list**

* [ ] Define async runtime semantics for `Flux.run/2` or introduce a dedicated async API if needed.
* [ ] Decide whether sync execution remains the default or becomes an option.
* [ ] Implement supervised async run processes.
* [ ] Ensure run state transitions are persisted during async execution.
* [ ] Ensure event emission works during async execution.
* [ ] Add tests for concurrent run starts.
* [ ] Add tests for shutdown/restart behavior during active runs.

#### 4. Parallel stage execution with bounded concurrency

**Status:** planned

**Scope**

Execute independent assets in parallel while respecting dependency stages and a configurable concurrency limit.

**Todo list**

* [ ] Define the concurrency model for stage execution.
* [ ] Add a `max_concurrency` runtime option or equivalent configuration.
* [ ] Execute assets within the same stage concurrently.
* [ ] Preserve deterministic semantics for stage ordering even when step completion order varies.
* [ ] Define failure behavior when one parallel step fails.
* [ ] Add tests for parallel execution correctness.
* [ ] Add tests for bounded concurrency under load.
* [ ] Add benchmark coverage for representative development workloads.

#### 5. Cron-based scheduler

**Status:** planned

**Scope**

Add a scheduler that can trigger runs from cron expressions on a local node. This is intended for development and single-node usage, not distributed or exactly-once scheduling guarantees.

**Todo list**

* [ ] Define a schedule model for mapping cron entries to target assets and run options.
* [ ] Choose whether schedules are configured statically, dynamically, or both.
* [ ] Implement cron parsing and next-run computation.
* [ ] Implement a scheduler supervisor/process tree.
* [ ] Trigger runs through the normal runtime APIs rather than bypassing them.
* [ ] Persist schedule definitions if required by the chosen design.
* [ ] Emit schedule-triggered run metadata.
* [ ] Add tests for cron matching and missed tick behavior.
* [ ] Add tests for app boot with valid and invalid schedules.

#### 6. Improved run events

**Status:** planned

**Scope**

Expand the run event stream so it reflects both run-level and step-level progress with a stable payload schema useful for development tooling.

**Todo list**

* [ ] Define canonical event types.
* [ ] Add step-started, step-completed, step-failed events.
* [ ] Add run-started, run-completed, run-failed, run-cancelled events.
* [ ] Define a versioned event payload shape.
* [ ] Ensure events include enough metadata for UI inspection.
* [ ] Document ordering expectations and best-effort semantics.
* [ ] Add regression tests for payload consistency.

#### 7. Run cancellation

**Status:** candidate

**Scope**

Allow a user or host app to cancel a queued or active run and mark unfinished work accordingly.

**Todo list**

* [ ] Define public API for cancellation.
* [ ] Define whether cancellation is best-effort or hard-stop.
* [ ] Define state transitions for running, queued, and downstream steps.
* [ ] Ensure cancellation updates storage consistently.
* [ ] Ensure cancellation emits correct run and step events.
* [ ] Add tests for cancelling during synchronous and asynchronous execution.

#### 8. Basic retry support

**Status:** candidate

**Scope**

Support limited retry behavior suitable for development workflows without introducing full production-grade orchestration policies.

**Todo list**

* [ ] Decide whether retries are configured per run, per asset, or both.
* [ ] Define retryable failure semantics.
* [ ] Add retry counters and attempt tracking to run-step records.
* [ ] Implement bounded retry execution.
* [ ] Ensure retries are reflected in events and persisted state.
* [ ] Add tests for deterministic retry behavior.

#### 9. Hex packaging and installability

**Status:** candidate

**Scope**

Reduce adoption friction by publishing Flux on Hex and tightening package metadata, docs, and release process.

**Todo list**

* [ ] Verify package metadata is complete for Hex publishing.
* [ ] Verify docs generation and published docs flow.
* [ ] Add release checklist for version bumps and changelog updates.
* [ ] Publish an initial Hex package when the project is ready.
* [ ] Update README installation instructions after publish.

---

## v0.3 — Production-grade release

### Goal

Make Flux operationally trustworthy in real production environments. v0.3 should focus on correctness under failure, stronger execution and scheduling guarantees, better observability, and operational control.

### Features

#### 1. Production-grade durable storage

**Status:** planned

**Scope**

Strengthen the persistence model beyond local development durability so run state is robust under operational failure scenarios.

**Todo list**

* [ ] Evaluate whether SQLite remains sufficient for production use cases.
* [ ] Define production storage requirements for consistency, concurrency, backup, and recovery.
* [ ] Add or design a production-ready storage adapter strategy.
* [ ] Verify migration and upgrade story for stored run data.
* [ ] Add failure-injection tests for storage outages and partial writes.

#### 2. Stronger scheduling guarantees

**Status:** planned

**Scope**

Evolve scheduling from local-node development convenience into a production-safe subsystem with clearer guarantees.

**Todo list**

* [ ] Define desired scheduling guarantees.
* [ ] Decide whether clustered scheduling is in scope.
* [ ] Design leader election, locking, or other coordination if multi-node scheduling is required.
* [ ] Add protection against duplicate triggers.
* [ ] Add recovery behavior for scheduler downtime or process crashes.
* [ ] Add tests for coordination and failover behavior.

#### 3. Operationally safe concurrency and resource control

**Status:** planned

**Scope**

Make parallel execution controllable and safe in production through explicit resource limits and isolation strategies.

**Todo list**

* [ ] Add queueing or admission control for active runs.
* [ ] Add workload-level concurrency controls.
* [ ] Add per-run or per-asset execution limits if needed.
* [ ] Define backpressure behavior.
* [ ] Add tests for overload and starvation scenarios.

#### 4. Stronger observability and telemetry

**Status:** planned

**Scope**

Provide production-ready visibility into system behavior through telemetry, metrics, and structured diagnostic hooks.

**Todo list**

* [ ] Define telemetry event coverage.
* [ ] Emit metrics for run counts, durations, failures, queue depth, and retries.
* [ ] Add hooks for tracing or structured logging integration.
* [ ] Document telemetry contracts.
* [ ] Add tests for telemetry emission and event compatibility.

#### 5. Stronger event delivery semantics

**Status:** planned

**Scope**

Improve run event delivery guarantees beyond best-effort pubsub where production use cases require it.

**Todo list**

* [ ] Define which event guarantees are required.
* [ ] Decide whether durable event persistence is needed.
* [ ] Define replay behavior for missed subscribers if in scope.
* [ ] Evaluate transport options if pubsub alone is insufficient.
* [ ] Add tests for reconnect and event-loss scenarios.

#### 6. Operational controls and policy surface

**Status:** candidate

**Scope**

Add the controls needed by operators to manage live systems safely and predictably.

**Todo list**

* [ ] Add pause/resume semantics if needed.
* [ ] Add manual re-run controls with explicit scope.
* [ ] Add policy controls for retries, cancellation, and concurrency.
* [ ] Define administrative APIs and authorization boundaries if relevant.
* [ ] Add tests for operator control flows.

---

## Explicitly out of scope for this roadmap

The following item is intentionally not included here because it should live separately:

* a standalone example repository such as `flux-demo` for a fully workable cloneable app

That repository can support adoption and documentation, but it should not be treated as a core library roadmap item.
