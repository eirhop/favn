# Phase 8 View Prototype + Orchestrator Live Events Plan

## Status

Completed.

Implemented in this phase:

- orchestrator-owned PubSub topics and subscription APIs for run-scoped and global run streams
- typed `%FavnOrchestrator.RunEvent{}` envelope plus authoritative transition writer path
- normalized storage event codec support for public run-event fields
- Phoenix LiveView foundation in `favn_view`
- dashboard, run list/detail, manifest list/detail, and scheduler inspection screens
- thin view-side contexts and presenter layer for stable UI-facing values
- lifecycle/pubsub/bootstrap coverage for success, retry, cancel, timeout, and external-cancel race paths

This phase establishes a real `favn_view` Phoenix LiveView runtime and restores operator-facing live updates through orchestrator-owned event subscriptions.

## Context

Phase 5 already moved run lifecycle ownership into `favn_orchestrator` and established persisted run snapshots plus append-only run event history.

What Phase 8 needed to add on top of pre-implementation `main`:

- a real `apps/favn_view` Phoenix LiveView app
- an orchestrator-owned PubSub boundary for operator live updates
- public orchestrator subscription APIs for run-scoped and global run streams

Historical reference:

- PR #8 (Flux) is a useful reference for Phoenix PubSub subscription ergonomics, topic naming style, and message shape.
- the runner-owned publication model from PR #8 must not be reused directly; operator-visible stream ownership now belongs to orchestrator transitions.

## Goal

Ship a minimum useful LiveView operator prototype for v1 and lock the long-term boundary between `favn_view` and `favn_orchestrator`.

## Architectural Decisions

### 1) Ownership split

- `favn_orchestrator` owns operator APIs, run/event persistence, transition acceptance, and live event publication.
- `favn_view` owns LiveView pages/components and invokes orchestrator APIs directly in-process.
- `favn_view` must not call runner modules, storage adapters, `favn_core`, plugins, or user business code.

### 2) Live transport for v1

- same-BEAM direct calls from LiveView to `FavnOrchestrator` are the primary v1 boundary.
- no HTTP/REST/GraphQL product boundary design in this phase.
- no polling-based update path.

### 3) PubSub placement

- add orchestrator-owned PubSub server in `favn_orchestrator` (`FavnOrchestrator.PubSub`).
- keep `favn_view` Phoenix PubSub (`FavnView.PubSub`) for LiveView internals.
- operator stream subscriptions in `favn_view` must go through orchestrator APIs, not direct topic strings.

### 4) Topic model for Phase 8

Ship both:

- run-scoped topic: `favn:orchestrator:runs:<run_id>`
- global runs topic: `favn:orchestrator:runs`

Do not add scheduler or manifest live topics in this phase. Keep scheduler and manifest pages snapshot-driven.

### 5) Event authority and publication timing

- operator-visible run lifecycle events are orchestrator-owned.
- authoritative stream reflects accepted orchestrator transitions.
- publish only after accepted transition persistence succeeds.
- publication should happen in a dedicated orchestrator transition writer/publisher path, not ad hoc in runner-facing code.

## Public API Additions (`FavnOrchestrator`)

### Subscription surface

- `subscribe_run(run_id)`
- `unsubscribe_run(run_id)`
- `subscribe_runs()`
- `unsubscribe_runs()`

Recommended signatures:

- `@spec subscribe_run(String.t()) :: :ok | {:error, term()}`
- `@spec unsubscribe_run(String.t()) :: :ok`
- `@spec subscribe_runs() :: :ok | {:error, term()}`
- `@spec unsubscribe_runs() :: :ok`

### Event read APIs

- evolve `list_run_events/1` to `list_run_events/2` with optional filters (`after_sequence`, `limit`).

### Snapshot APIs needed by the prototype

- keep existing run/manifest/scheduler snapshot reads.
- provide one stable scheduler inspection API that returns orchestrator-owned scheduler entry plus cursor state for each pipeline schedule stream.

## Event Envelope Spec

Introduce typed operator events at the orchestrator boundary.

Recommended public struct:

- `%FavnOrchestrator.RunEvent{}`

Recommended fields:

- `schema_version :: pos_integer()`
- `run_id :: String.t()`
- `sequence :: pos_integer()`
- `event_type :: atom()`
- `entity :: :run | :step`
- `occurred_at :: DateTime.t()`
- `status :: atom() | nil`
- `manifest_version_id :: String.t() | nil`
- `manifest_content_hash :: String.t() | nil`
- `asset_ref :: Favn.Ref.t() | nil`
- `stage :: non_neg_integer() | nil`
- `data :: map()`

Message shape delivered over PubSub:

- `{:favn_run_event, %FavnOrchestrator.RunEvent{}}`

Versioning policy:

- start at `schema_version: 1`.
- additive optional fields keep the version.
- breaking shape/semantic changes bump schema version.

## Snapshot + Stream Consumption Model

### Run detail LiveView

1. subscribe to run topic with `subscribe_run(run_id)`.
2. fetch snapshot (`get_run/1`) and persisted event history (`list_run_events/2`).
3. render from snapshot + timeline.
4. on each event, dedupe by `{run_id, sequence}`.
5. append event to timeline and refresh authoritative run snapshot.

### Run list/dashboard LiveView

1. subscribe to global runs topic with `subscribe_runs/0`.
2. fetch initial list via `list_runs/1`.
3. on event, dedupe and refresh affected run snapshot via `get_run/1`.

This keeps snapshots authoritative while preserving event-driven UX.

## Publication Flow Design

For each accepted transition:

1. compute next `RunState` with incremented `event_seq`.
2. build canonical `%FavnOrchestrator.RunEvent{}` for that sequence.
3. persist run snapshot and append run event as one authoritative transition write.
4. only after write success, broadcast to:
   - run topic
   - global runs topic

Required invariants:

- run snapshots stay monotonic by `event_seq`.
- event history stays append-only and unique by `{run_id, sequence}`.
- stale/conflicting writes return explicit errors.
- broadcasts never represent transitions that failed persistence.

## Module / File Plan

## `favn_orchestrator`

Add:

- `apps/favn_orchestrator/lib/favn_orchestrator/run_event.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/events.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/transition_writer.ex`

Change:

- `apps/favn_orchestrator/lib/favn_orchestrator.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/application.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/projector.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_manager.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_server.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/storage.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/storage/run_event_codec.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/storage/adapter/memory.ex`
- corresponding sqlite/postgres adapter write paths to support authoritative transition writes

## `favn_view` (Phoenix LiveView)

Add/replace scaffold with:

- `apps/favn_view/lib/favn_view/application.ex` children for endpoint + telemetry + pubsub
- `apps/favn_view/lib/favn_view_web.ex`
- `apps/favn_view/lib/favn_view/endpoint.ex`
- `apps/favn_view/lib/favn_view_web/router.ex`
- `apps/favn_view/lib/favn_view_web/layouts.ex`
- `apps/favn_view/lib/favn_view_web/core_components.ex`
- `apps/favn_view/lib/favn_view/runs.ex`
- `apps/favn_view/lib/favn_view/manifests.ex`
- `apps/favn_view/lib/favn_view/scheduler.ex`
- `apps/favn_view/lib/favn_view/presenters/run_presenter.ex`
- `apps/favn_view/lib/favn_view/presenters/manifest_presenter.ex`
- `apps/favn_view/lib/favn_view/presenters/scheduler_presenter.ex`
- `apps/favn_view/lib/favn_view_web/live/dashboard_live.ex`
- `apps/favn_view/lib/favn_view_web/live/runs/index_live.ex`
- `apps/favn_view/lib/favn_view_web/live/runs/show_live.ex`
- `apps/favn_view/lib/favn_view_web/live/manifests/index_live.ex`
- `apps/favn_view/lib/favn_view_web/live/manifests/show_live.ex`
- `apps/favn_view/lib/favn_view_web/live/scheduler/index_live.ex`

## Phase 8 Prototype Scope (UI)

Minimum useful operator prototype:

- dashboard (active manifest + recent runs + submit controls)
- run list (filter + recent status)
- run detail (live timeline + actions)
- manifest list/detail (snapshot-driven)
- scheduler inspection (snapshot-driven)
- submit asset run
- submit pipeline run
- cancel run
- rerun run

Intentionally not in scope:

- auth/RBAC
- transport API design for remote deployments
- polling fallback
- graph/DAG rich visualization
- runner log streaming
- editing manifests/schedules

## Testing Strategy

### Orchestrator

- PubSub/topic behavior for run-scoped and global subscriptions
- publication-after-persistence guarantee tests
- event sequencing monotonicity tests
- idempotency/stale/conflict semantics tests for transition writes
- snapshot + event history consistency tests

### View (LiveView)

- run list receives global run events and updates correctly
- run detail receives run-scoped events and appends timeline
- cancel/rerun actions work while subscriptions are active
- submit forms trigger orchestrator calls and render resulting state
- scheduler and manifest inspection pages stay snapshot-only and do not poll

## Acceptance Criteria

Phase 8 slice is done when:

1. `favn_view` runs as a Phoenix LiveView app in the umbrella.
2. `favn_view` depends only on `favn_orchestrator`.
3. `FavnOrchestrator` exposes run-scoped and global subscription APIs.
4. operator live events are orchestrator-owned and emitted only after accepted persisted transitions.
5. run list and run detail update live via event subscriptions (no polling).
6. dashboard/manifests/scheduler inspection/submit/cancel/rerun prototype flows are usable for operator testing.
7. tests cover event sequencing, snapshot-stream consistency, and active subscription control flows.

## Risks and Tradeoffs

- Broadcasting after persistence can still lose transient live delivery on subscriber disconnects; snapshot + persisted event history remains the recovery source.
- Refetching snapshots on event receipt is intentionally simple and stable, but less optimized than maintaining a full view-side reducer.
- Deferring scheduler/manifest live topics keeps Phase 8 focused and reduces churn in event contracts.
