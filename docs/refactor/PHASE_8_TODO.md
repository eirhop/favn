# Phase 8 TODO

## Status

Checklist for implementing `docs/refactor/PHASE_8_VIEW_PROTOTYPE_PLAN.md`.

This list is intentionally detailed and execution-oriented. `docs/FEATURES.md` remains the high-level roadmap only.

## Orchestrator Event Boundary

- [x] Add orchestrator-owned PubSub child startup and config defaults.
- [x] Add `FavnOrchestrator.Events` with run/global topic helpers and subscribe/unsubscribe wrappers.
- [x] Add `%FavnOrchestrator.RunEvent{}` typed operator event struct.
- [x] Add `FavnOrchestrator.TransitionWriter` for authoritative transition persistence + post-write broadcast.
- [x] Add `FavnOrchestrator.subscribe_run/1`.
- [x] Add `FavnOrchestrator.unsubscribe_run/1`.
- [x] Add `FavnOrchestrator.subscribe_runs/0`.
- [x] Add `FavnOrchestrator.unsubscribe_runs/0`.
- [x] Extend `FavnOrchestrator.list_run_events/1` to `list_run_events/2` with optional filters (`after_sequence`, `limit`).

## Authoritative Transition Persistence

- [x] Add storage facade API for authoritative transition writes (single call for snapshot + event append semantics).
- [x] Implement memory adapter transition-write path with existing monotonic/idempotent guarantees.
- [x] Implement sqlite adapter transition-write path in one DB transaction.
- [x] Implement postgres adapter transition-write path in one DB transaction.
- [x] Normalize event codec read/write paths around `%FavnOrchestrator.RunEvent{}` boundary values.
- [x] Ensure stale/conflict errors remain explicit and deterministic.

## Run Lifecycle Wiring

- [x] Replace `RunManager` run-created/cancel transition calls to use transition writer.
- [x] Replace `RunServer` transition persistence calls to use transition writer.
- [x] Ensure all run lifecycle transitions produce one canonical operator event per accepted transition.
- [x] Verify global + run-scoped publication for each accepted transition.
- [x] Verify external cancel race handling still behaves correctly under new transition writer path.

## Scheduler and Manifest Snapshot APIs

- [x] Add stable scheduler inspection DTO/API in `favn_orchestrator` for prototype page use.
- [x] Keep scheduler updates snapshot-only in Phase 8 (no scheduler subscriptions/topics).
- [x] Keep manifest pages snapshot-only in Phase 8 (no manifest subscriptions/topics).

## Phoenix LiveView Foundation (`favn_view`)

- [x] Add Phoenix and LiveView dependencies to `apps/favn_view/mix.exs`.
- [x] Add endpoint/pubsub/telemetry supervision in `FavnView.Application`.
- [x] Add `FavnViewWeb` base module, endpoint, router, layouts, and core components.
- [x] Add baseline app config files needed for local runtime (`config/config.exs` and `config/dev.exs` updates as needed).

## View Contexts and Presenters

- [x] Add `FavnView.Runs` thin context wrapping orchestrator run APIs and subscriptions.
- [x] Add `FavnView.Manifests` thin context wrapping orchestrator manifest APIs.
- [x] Add `FavnView.Scheduler` thin context wrapping orchestrator scheduler APIs.
- [x] Add stable presenter helpers for UI-facing map shaping (avoid raw LiveView mapping logic spread).

## LiveView Screens

- [x] Dashboard LiveView with active manifest, recent runs, basic counters, and submit forms.
- [x] Runs index LiveView with live updates via `subscribe_runs/0`.
- [x] Run detail LiveView with live timeline via `subscribe_run/1`.
- [x] Manifest list LiveView.
- [x] Manifest detail LiveView.
- [x] Scheduler inspection LiveView.

## Operator Actions

- [x] Wire asset-run submission action.
- [x] Wire pipeline-run submission action.
- [x] Wire cancel action on running runs.
- [x] Wire rerun action for terminal runs.

## Orchestrator Tests

- [x] Add tests for run/global topic subscription and unsubscribe behavior.
- [x] Add tests for publication-after-persistence guarantee.
- [x] Add tests for event sequence monotonicity across success/failure/retry/cancel flows.
- [x] Add tests for transition-write idempotency and stale/conflict behavior.
- [x] Add tests for snapshot + event history consistency under reconnect/bootstrap flow.

## LiveView Tests

- [x] Add dashboard render + submit action tests.
- [x] Add runs index live-update tests on global events.
- [x] Add run detail timeline live-update tests on run events.
- [x] Add active-subscription cancel/rerun tests.
- [x] Add scheduler/manifest snapshot page tests.

## Docs and Roadmap Hygiene

- [x] Keep `docs/FEATURES.md` as high-level roadmap only; do not add detailed implementation checklist there.
- [x] Keep this TODO file updated as slices land.
- [x] Keep `README.md` and phase docs aligned as public operator-facing behavior evolves.
- [x] Update `docs/lib_structure.md` and `docs/test_structure.md` as Phase 8 code files and tests are added.

## Verification Gate (Per AGENTS.md)

- [x] Run `mix format`.
- [x] Run `mix compile --warnings-as-errors`.
- [x] Run `mix test`.
- [x] Run `mix credo --strict`.
- [x] Run `mix dialyzer`.
- [x] Run `mix xref graph --format stats --label compile-connected`.

## Explicit Non-Goals For This Phase

- [ ] auth/RBAC.
- [ ] remote transport API.
- [ ] polling-based update path.
- [ ] graph visualization/rich DAG UI.
- [ ] runner log streaming.
- [ ] manifest/schedule editing.
- [ ] production hardening beyond architectural correctness for this boundary slice.
