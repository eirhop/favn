# Phase 5 Orchestrator Boundary Plan

## Status

Planned on branch `feature/phase-5-orchestrator-plan`.

Phase 4 delivered the first real runner seam on `main`:

- `favn_runner` owns manifest registration, worker execution, and runner-side connection/SQL runtime slices
- same-node execution can already cross the runner boundary through `FavnRunner`
- `Favn.Run.Context` and `Favn.Run.AssetResult` now live in `favn_core`

What still remains in `favn_legacy` is the control plane:

- run admission and lifecycle management
- step readiness, retry, cancellation, and timeout policy
- run snapshot projection and event publication
- scheduler runtime and scheduler cursor persistence
- storage facade and concrete adapters

`apps/favn_orchestrator` is still only a scaffold, so Phase 5 is where the product actually gains a real control plane.

## Recommendation Summary

Phase 5 should make five architectural moves together:

1. Make `favn_orchestrator` the system of record for persisted manifest versions, active manifest selection, runs, run events, and scheduler cursors.
2. Add manifest-native planning helpers in `favn_core` so orchestrator can resolve asset runs and pipeline runs from persisted `%Favn.Manifest{}` data without loading user modules directly.
3. Introduce a shared runner-client behaviour in `favn_core`; orchestrator talks to a configured runner client module, and same-node mode can point that at `FavnRunner` without adding a compile-time dependency from `favn_orchestrator` to `favn_runner`.
4. Keep the orchestrator runtime small and boring: one admission manager, one run server per run, one storage boundary, one scheduler runtime, and one memory adapter in Phase 5.
5. Preserve current user-visible run behavior where practical, but move ownership into orchestrator-owned modules and persisted manifest data instead of legacy runtime discovery.

The most important recommendation is to introduce an explicit active-manifest policy now.

The orchestrator must persist many manifest versions, but scheduler and default manual runs need one deterministic answer to “which version should new runs use right now?”.

Recommended rule:

- all runs persist their pinned `manifest_version_id` and `manifest_content_hash`
- manual runs may supply an explicit manifest version, otherwise they default to the active manifest version
- scheduler always evaluates and submits against the active manifest version
- changing the active manifest version affects only future runs, never in-flight runs

## Current Reality On `main`

The Phase 5 design should fit the code that already exists:

- `FavnRunner` currently exposes `register_manifest/1`, `submit_work/2`, `await_result/2`, `cancel_work/2`, and `run/2`
- `%Favn.Contracts.RunnerWork{}` is already pinned by `manifest_version_id` and `manifest_content_hash`
- `Favn.Assets.Planner` can already plan from an explicit `%Favn.Assets.GraphIndex{}` rather than loaded app config
- `Favn.Pipeline.Resolver` is already explicit-input driven, but it still resolves from compile-time pipeline definitions rather than persisted manifest pipeline descriptors
- `apps/favn/lib/favn.ex` still points runtime helpers at legacy storage/manager paths when available

That means Phase 5 does not need to invent the execution seam, but it does need to add the missing persisted-manifest planning and control-plane ownership around it.

## Phase 5 Architecture Decisions

### 1. Orchestrator owns persisted manifest versions and activation

The orchestrator should own manifest persistence and active-version selection.

Recommended persisted concerns:

- store immutable `%Favn.Manifest.Version{}` records by `manifest_version_id`
- deduplicate or verify by `content_hash`
- store one active manifest pointer for the current runtime environment
- expose fetch/list helpers for operators and later view APIs

Recommended rule:

- Phase 5 should keep `%Favn.Manifest.Version{}` as the persistence contract accepted by orchestrator
- orchestrator does not need to re-open Phase 3 serialization or hashing rules
- future tooling may decide where version IDs are allocated, but the orchestrator boundary should already assume the version envelope is the persisted unit

### 2. Add manifest-native planning in `favn_core`

The orchestrator must not call authoring-time compilation or registry APIs to plan runs.

Recommended new pure helpers in `favn_core`:

- `Favn.Manifest.Index`
  - build lookup maps from `%Favn.Manifest{}`
  - build a `%Favn.Assets.GraphIndex{}` from persisted manifest assets
  - expose pipeline and schedule lookup helpers
- `Favn.Manifest.PipelineResolver`
  - resolve one `%Favn.Manifest.Pipeline{}` to target refs and pipeline context using manifest assets and manifest schedules only

Recommended planning flow:

1. fetch pinned `%Favn.Manifest.Version{}` from orchestrator storage
2. build or load derived `%Favn.Manifest.Index{}`
3. resolve either explicit asset refs or a persisted pipeline descriptor
4. call `Favn.Assets.Planner.plan/2` with `graph_index:` from the manifest index
5. persist the resulting `%Favn.Plan{}` inside orchestrator-owned run state

Important rule:

- Phase 5 orchestrator code must not call `Favn.list_assets/0`, `Favn.plan_asset_run/2`, or runtime module discovery APIs for normal operation

### 3. Add a shared runner-client behaviour in `favn_core`

`favn_orchestrator` cannot depend on `favn_runner`, but it still needs a stable contract to dispatch work.

Recommended new shared behaviour:

- `Favn.Contracts.RunnerClient`
  - `register_manifest(version, opts \\ [])`
  - `submit_work(work, opts \\ [])`
  - `await_result(execution_id, timeout, opts \\ [])`
  - `cancel_work(execution_id, reason, opts \\ [])`

Recommended Phase 5 rule:

- `favn_orchestrator` reads the configured runner client module from app config
- same-node mode can point that setting at `FavnRunner`
- future remote clients can implement the same behaviour without changing orchestrator lifecycle code

This keeps the dependency rule intact:

- shared runner protocol contract stays in `favn_core`
- orchestrator does not take a compile-time dependency on runner implementation
- runner remains execution-only

### 4. Keep run coordination in the orchestrator, not the runner

The runner still executes one concrete node at a time. The orchestrator owns everything around it.

Recommended ownership split:

- orchestrator owns admission, planning, dependency readiness, dispatch order, retry policy, timeout policy, cancellation policy, and persistence
- runner owns manifest lookup, context building, asset invocation, runner-side runtime values, and execution result normalization

Recommended orchestrator work unit:

- one `%Favn.Contracts.RunnerWork{}` per concrete `%Favn.Plan.node_key()`

The orchestrator should populate `RunnerWork.metadata` with enough control-plane context to correlate local dispatch state cleanly:

- `:node_key`
- `:stage`
- `:attempt`
- `:max_attempts`

### 5. Introduce an orchestrator-owned internal run state

Phase 5 should not persist raw internal GenServer state directly.

Recommended internal struct:

- `FavnOrchestrator.RunState`

Recommended fields:

- run identity and manifest identity
- resolved `%Favn.Plan{}`
- target refs and target node keys
- trigger / pipeline context / params
- retry policy and timeout policy
- run lifecycle timestamps and terminal reason
- per-node step state keyed by `%Favn.Plan.node_key()`
- in-flight runner execution map keyed by runner execution id
- event sequence counter

Recommended public projection target:

- keep `%Favn.Run{}` as the persisted/operator-facing run snapshot

`%Favn.Run{}` is a control-plane record, so it should move to orchestrator ownership in Phase 5 rather than into `favn_core`.

### 6. Keep the event model append-only, but do not over-rotate into full event sourcing

Phase 5 needs durable run/event history for operators and later view APIs, but it does not need a full event-sourced architecture rewrite.

Recommended model:

- orchestrator appends one canonical run or step event per accepted lifecycle transition
- orchestrator projects `%Favn.Run{}` snapshots from internal state after each accepted transition
- storage persists both the latest run snapshot and the append-only event log
- PubSub fanout happens from orchestrator after the write succeeds

Recommended rule:

- raw runner events are transport facts, not the authoritative operator event history
- operator-visible `run_created`, `run_started`, `step_started`, `step_finished`, `run_finished`, `run_failed`, `run_cancelled`, and `run_timed_out` events should come from orchestrator transitions

This keeps Phase 5 aligned with the future `favn_view` boundary without forcing the runner to become an event-history service.

### 7. Storage contract should be orchestrator-owned and stronger than the legacy adapter

Phase 5 needs a new storage boundary in `favn_orchestrator`, with one in-memory implementation now and app-specific adapters later in Phase 6.

Recommended behaviour:

- one authoritative low-level storage adapter contract, now served as `Favn.Storage.Adapter`

Recommended callback groups:

1. Manifest versions
   - `put_manifest_version/2`
   - `get_manifest_version/2`
   - `list_manifest_versions/2`
   - `set_active_manifest_version/2`
   - `get_active_manifest_version/1`
2. Run snapshots
   - `put_run/2`
   - `get_run/2`
   - `list_runs/2`
3. Run event history
   - `append_run_event/2`
   - `list_run_events/2`
4. Scheduler state
   - `put_scheduler_state/2`
   - `get_scheduler_state/3`

Required invariants:

- manifest version writes are immutable once accepted
- run writes are monotonic by `event_seq`
- same `event_seq` + same snapshot hash is idempotent success
- same `event_seq` + different snapshot hash is `{:error, :conflicting_snapshot}`
- lower `event_seq` is `{:error, :stale_write}`
- run events are append-only and unique by `{run_id, sequence}`
- scheduler state is keyed by `{pipeline_module, schedule_id}` and carries optimistic version semantics

Recommended Phase 5 implementation:

- one in-memory adapter under `apps/favn_orchestrator/lib/favn_orchestrator/storage/adapter/memory.ex`
- no SQLite/Postgres app work yet

### 8. Scheduler should read the active manifest, not discover loaded pipeline modules

The legacy scheduler discovers schedules from loaded modules. The orchestrator scheduler must instead derive schedule entries from persisted manifest data.

Recommended rule:

- scheduler loads the active manifest version
- scheduler resolves schedule entries from persisted `%Favn.Manifest.Pipeline{}` and `%Favn.Manifest.Schedule{}` data
- scheduler persists one cursor row per `{pipeline_module, schedule_id}` stream
- scheduler resets or reconciles state when the schedule fingerprint changes

Recommended carry-forward behavior from legacy:

- `missed: :all | :skip | :one`
- `overlap: :allow | :forbid | :queue_one`
- anchor-window derivation for windowed pipelines

Recommended new responsibility:

- scheduler-triggered runs must always persist the manifest version they were created against

### 9. Rerun, retry, timeout, and cancellation stay manifest-pinned

Phase 5 should preserve the reproducibility rule already implied by Phase 3.

Recommended rules:

- retries always use the same pinned manifest version as the original run
- reruns default to the original run's pinned manifest version, not the currently active manifest version
- cancellation closes admission immediately and forwards cancellation to all in-flight runner executions
- run-level timeout is owned by orchestrator; the runner only cancels concrete work items it is told to stop

This is one of the key reasons the orchestrator, not the runner, owns retry and run lifecycle policy.

### 10. Preserve public `Favn.*` runtime facades where they still make sense

Phase 5 should move ownership, not delete every stable name.

Recommended preserved `Favn.*` modules moved into the orchestrator owner app:

- `Favn.Run`
- `Favn.Storage`
- `Favn.Scheduler`
- `Favn.Scheduler.State`

Recommended new app-internal modules under `FavnOrchestrator.*`:

- `FavnOrchestrator` facade
- `FavnOrchestrator.RunManager`
- `FavnOrchestrator.RunServer`
- `FavnOrchestrator.RunState`
- `FavnOrchestrator.Projector`
- `FavnOrchestrator.RunnerDispatch`
- `FavnOrchestrator.ManifestStore`
- `FavnOrchestrator.Storage`
- `FavnOrchestrator.Scheduler.Runtime`

This matches the Phase 4 pattern where shared/public contracts kept `Favn.*` names while app-owned services use `FavnRunner.*`.

## Recommended Runtime Shape

### Application children

Recommended children under `FavnOrchestrator.Application`:

- orchestrator storage process(es) required by the configured adapter
- `Registry` for run process lookup / local subscriptions if needed
- `DynamicSupervisor` for per-run servers
- `FavnOrchestrator.RunManager`
- `FavnOrchestrator.ManifestStore` or equivalent manifest service facade
- `Phoenix.PubSub` only if Phase 5 keeps orchestrator-owned local event fanout in-app
- `FavnOrchestrator.Scheduler.Runtime` when scheduler is enabled

Do not add:

- a queue service
- a second execution engine
- storage-adapter-specific logic in the supervisor tree

### Run execution flow

Recommended Phase 5 run flow:

1. fetch explicit or active manifest version
2. resolve targets and build `%Favn.Plan{}` from manifest-native planning helpers
3. create internal `%FavnOrchestrator.RunState{}` and initial `%Favn.Run{}` snapshot
4. persist initial snapshot and `run_created` event
5. start per-run server under the orchestrator run supervisor
6. dispatch ready nodes one by one through the configured runner client
7. await results in orchestrator-owned dispatch tasks or processes
8. apply transitions, emit events, and persist updated `%Favn.Run{}` snapshots after each accepted result
9. when all targets succeed or a terminal policy fires, persist final snapshot and terminal event

### Scheduler flow

Recommended scheduler flow:

1. fetch active manifest version
2. derive schedule entries and fingerprints from the manifest
3. load persisted scheduler cursor state for each schedule stream
4. evaluate due occurrences according to cron + missed/overlap policy
5. submit pipeline runs pinned to the active manifest version
6. persist updated scheduler cursor state

## Legacy Slice Map

### Move or rewrite in Phase 5

- `apps/favn_legacy/lib/favn/run.ex`
- `apps/favn_legacy/lib/favn/storage.ex`
- `apps/favn_legacy/lib/favn/storage/adapter.ex`
- `apps/favn_legacy/lib/favn/runtime/manager.ex`
- `apps/favn_legacy/lib/favn/runtime/coordinator.ex`
- `apps/favn_legacy/lib/favn/runtime/state.ex`
- `apps/favn_legacy/lib/favn/runtime/projector.ex`
- `apps/favn_legacy/lib/favn/runtime/events.ex`
- `apps/favn_legacy/lib/favn/runtime/transitions/run.ex`
- `apps/favn_legacy/lib/favn/runtime/transitions/step.ex`
- `apps/favn_legacy/lib/favn/runtime/step_state.ex`
- `apps/favn_legacy/lib/favn/scheduler.ex`
- `apps/favn_legacy/lib/favn/scheduler/runtime.ex`
- `apps/favn_legacy/lib/favn/scheduler/state.ex`
- `apps/favn_legacy/lib/favn/scheduler/storage.ex`

### Leave in legacy for Phase 6+

- concrete SQLite adapter files
- concrete Postgres adapter files
- storage-specific Ecto repos and migrations

### Leave for later phases

- runner plugin extraction (`favn_duckdb`)
- view/UI runtime
- packaging and install/dev tooling

## File Plan

### In `apps/favn_core/lib/favn/`

- `contracts/runner_client.ex`
- `manifest/index.ex`
- `manifest/pipeline_resolver.ex`

### In `apps/favn_orchestrator/lib/favn/`

- `run.ex`
- `storage.ex`
- `scheduler.ex`
- `scheduler/state.ex`

### In `apps/favn_orchestrator/lib/favn_orchestrator/`

- `manifest_store.ex`
- `storage.ex`
- `storage/adapter.ex`
- `storage/adapter/memory.ex`
- `run_manager.ex`
- `run_server.ex`
- `run_state.ex`
- `projector.ex`
- `runner_dispatch.ex`
- `scheduler/runtime.ex`
- `scheduler/manifest_entries.ex`

The exact file count can stay smaller if some modules naturally collapse together, but this should be the conceptual split.

## Implementation Order

Recommended Phase 5 work order:

1. Add `Favn.Contracts.RunnerClient` plus manifest-native planning helpers in `favn_core`.
2. Move `Favn.Run` and `Favn.Scheduler.State` into orchestrator ownership.
3. Build the orchestrator storage adapter contract and in-memory adapter.
   The later cleanup should leave only one authoritative low-level behaviour rather than parallel public/internal variants.
4. Implement manifest persistence plus active-manifest selection.
5. Implement manual asset-run submission and run snapshot persistence.
6. Implement pipeline-run resolution from persisted manifest data.
7. Implement retry, timeout, cancellation, and rerun against pinned manifest versions.
8. Implement scheduler runtime from active manifest data.
9. Switch public runtime-facing `Favn` facade helpers from legacy to orchestrator-owned paths.
10. Update docs and roadmap status.

## Testing Plan

### `apps/favn_core/test`

- manifest index build tests
- manifest pipeline resolver tests
- runner client behaviour contract tests if helper coverage is useful

### `apps/favn_orchestrator/test`

- manifest persistence and active-manifest selection
- run submission for asset refs against explicit manifest versions
- pipeline submission from manifest descriptors
- unknown manifest / unknown asset / unknown pipeline failures
- run projection and event-sequence monotonic persistence
- retry policy, cancellation, timeout, and rerun behavior
- scheduler evaluation using persisted manifest schedules
- memory adapter conflict/idempotency behavior

### Cross-app integration

- same-node orchestrator-to-runner integration using configured runner client `FavnRunner`
- verify orchestrator registers manifest versions with runner before first dispatch
- verify run snapshots stay pinned when active manifest changes mid-flight

## Documentation Updates Required In Phase 5

Phase 5 implementation should update at least:

- `README.md`
- `docs/REFACTOR.md`
- `docs/FEATURES.md`
- `docs/structure/`
- `apps/favn/lib/favn.ex` moduledoc and public runtime docs that currently still describe transitional behavior

The docs must make these points explicit:

- orchestrator is now the persisted control plane
- manual runs and scheduler runs are manifest-version pinned
- scheduler reads persisted manifest data, not loaded pipeline modules
- same-node execution still crosses the runner boundary through a configured runner client

## Explicit Out-Of-Scope List

Do not implement these as part of Phase 5:

- SQLite/Postgres adapter apps (`favn_storage_sqlite`, `favn_storage_postgres`)
- distributed queueing or lease-based claims
- remote transport polish beyond what is needed for the shared runner-client contract
- view/UI work
- packaging/install tooling
- plugin extraction into `favn_duckdb`
- rewriting Phase 4 runner execution ownership back into the orchestrator
- making SQL manifest execution a blocker for the orchestrator boundary; Phase 5 should stay asset-type agnostic and let the runner return its current capability errors where needed

Phase 5 is successful when `favn_orchestrator` can persist manifest versions, choose an active manifest, create and track runs against pinned manifest data, schedule from persisted schedules, and dispatch work through the runner boundary without loading user business modules directly.
