# Phase 5 TODO

## Status

Checklist for implementing the Phase 5 orchestrator boundary plan defined in `docs/refactor/PHASE_5_ORCHESTRATOR_BOUNDARY_PLAN.md`.

Phase 5 implementation is complete. This checklist records the delivered orchestrator boundary. Storage-adapter apps, UI work, and packaging remain later phases.

## Core Planning And Shared Contracts

- [x] Add `Favn.Contracts.RunnerClient` in `favn_core`.
- [x] Add `Favn.Manifest.Index` in `favn_core`.
- [x] Add `Favn.Manifest.PipelineResolver` in `favn_core`.
- [x] Ensure orchestrator planning can build `%Favn.Assets.GraphIndex{}` from persisted manifest assets without runtime module discovery.
- [x] Add focused `apps/favn_core/test` coverage for manifest-native planning helpers.

## Orchestrator-Owned Public Contracts

- [x] Move `Favn.Run` contract ownership out of `favn_legacy` and serve it from orchestrator-owned files.
- [x] Move `Favn.Scheduler.State` contract ownership out of `favn_legacy` and serve it from orchestrator-owned files.
- [x] Keep `Favn.Scheduler` as a preserved `Favn.*` facade under the orchestrator owner app.
- [x] Keep `Favn.Storage` as a preserved `Favn.*` facade under the orchestrator owner app.
- [x] Align `Favn.Storage` adapter validation/defaults and `Favn.Storage.Adapter` behaviour with `FavnOrchestrator.Storage` contract shape rather than legacy adapter callback assumptions.

## Orchestrator App Skeleton

- [x] Replace scaffold `FavnOrchestrator.hello/0` API with real orchestrator entrypoints.
- [x] Add `FavnOrchestrator.RunManager`.
- [x] Add per-run server supervision.
- [x] Add `FavnOrchestrator.RunState` and `FavnOrchestrator.Projector`.
- [x] Keep `FavnOrchestrator.Application` limited to control-plane-owned children only.

## Storage Boundary

- [x] Add `FavnOrchestrator.Storage.Adapter` behaviour.
- [x] Add an in-memory orchestrator adapter.
- [x] Support manifest version persistence and active-manifest selection.
- [x] Support monotonic run snapshot persistence with conflict detection.
- [x] Support append-only run event storage.
- [x] Support versioned scheduler-state persistence keyed by `{pipeline_module, schedule_id}`.

## Manifest Registry And Activation

- [x] Implement manifest version registration in `favn_orchestrator`.
- [x] Implement fetch/list helpers for persisted manifest versions.
- [x] Implement active-manifest selection and lookup.
- [x] Ensure changing the active manifest affects future runs only.

## Run Submission And Lifecycle

- [x] Implement explicit-manifest asset-run submission.
- [x] Implement default-to-active-manifest run submission when no manifest version is supplied.
- [x] Build `%Favn.Plan{}` from persisted manifest data, not authoring-time helpers.
- [x] Persist initial run snapshot and `run_created` event before dispatch begins.
- [x] Dispatch ready nodes through the configured runner client.
- [x] Await runner results and apply orchestrator-owned state transitions.
- [x] Persist updated run snapshots after each accepted transition.
- [x] Emit orchestrator-owned run and step events.
- [x] Support multi-target pipeline submission in one orchestrator run plan.
- [x] Execute pipeline stage groups (`node_stages`) in stage-parallel mode when retry policy allows single-attempt execution.
- [x] Project public run reads with terminal result aggregation per ref while keeping attempt history in events and internal result payloads.

## Retry, Timeout, Cancellation, And Rerun

- [x] Recreate retry policy in orchestrator ownership.
- [x] Recreate run timeout handling in orchestrator ownership.
- [x] Recreate run cancellation handling in orchestrator ownership.
- [x] Forward cancellation to in-flight runner executions.
- [x] Keep retries and reruns pinned to the source run's manifest version by default.
- [x] On timeout and partial stage-submit failures, best-effort cancel in-flight runner execution IDs before continuing retries or terminal transitions.
- [x] Replay pipeline reruns from the original pipeline target selection/dependency mode rather than collapsing to a single `asset_ref`.

## Pipeline And Scheduler Support

- [x] Implement pipeline-run resolution from persisted manifest pipeline descriptors.
- [x] Implement scheduler runtime from active manifest data rather than loaded module discovery.
- [x] Preserve current cron missed/overlap behavior.
- [x] Preserve window-anchor derivation for scheduled windowed pipelines.
- [x] Persist scheduler cursor updates through the new orchestrator storage boundary.

## Public Facade Cutover

- [x] Update `apps/favn/lib/favn.ex` runtime helpers to delegate to orchestrator-owned paths instead of legacy manager/storage modules.
- [x] Keep deterministic `{:error, :runtime_not_available}` behavior where runtime services are not started.
- [x] Update user-facing docs and examples to reflect manifest-pinned orchestration.

## Scheduler Facade

- [x] Add public scheduler facade methods backed by orchestrator ownership (`Favn.Scheduler`, `Favn.reload_scheduler/0`, `Favn.tick_scheduler/0`, `Favn.list_scheduled_pipelines/0`).

## Tests

- [x] Add `apps/favn_orchestrator/test/manifest_store_test.exs`.
- [x] Add `apps/favn_orchestrator/test/storage/memory_adapter_test.exs`.
- [x] Add `apps/favn_orchestrator/test/run_manager_test.exs`.
- [x] Add `apps/favn_orchestrator/test/run_server_test.exs`.
- [x] Add `apps/favn_orchestrator/test/projector_test.exs`.
- [x] Add `apps/favn_orchestrator/test/scheduler/runtime_test.exs`.
- [x] Add same-node orchestrator-to-runner integration coverage.
- [x] Keep SQLite/Postgres adapter tests out of Phase 5 until Phase 6.

## Docs Updates

- [x] Update `README.md` once orchestrator owns real control-plane paths.
- [x] Update `docs/REFACTOR.md` Phase 5 status as slices land.
- [x] Update `docs/FEATURES.md` checkboxes as orchestrator slices complete.
- [x] Update `docs/lib_structure.md` with new orchestrator-owned modules.
- [x] Update `docs/test_structure.md` with new orchestrator test layout.

## Verification

- [x] Run `mix format`.
- [x] Run `mix compile --warnings-as-errors`.
- [x] Run `mix test`.
- [x] Run `mix credo --strict`.
- [x] Run `mix dialyzer`.
- [x] Run `mix xref graph --format stats --label compile-connected`.

## Explicit Out Of Scope For Later Phases

- SQLite/Postgres adapter apps remain Phase 6.
- View/UI work remains later-phase scope.
- Packaging/tooling flows remain later-phase scope.
- Runner remains execution-only rather than becoming a control-plane service.
- Distributed queueing, claims, and lease management remain later-phase scope.
