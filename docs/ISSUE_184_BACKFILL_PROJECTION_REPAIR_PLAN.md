# Issue 184 Backfill Projection Repair Plan

> Status: planned for issue #184.
> Scope: add operator/developer repair tooling for derived operational-backfill read models without mutating authoritative run history.

## Issue Summary

Issue #184 asks for an explicit repair or replay path for operational-backfill projections when projection code changes, projection writes fail, or persisted read models drift from authoritative run records.

The derived models in scope are:

- coverage baselines
- backfill-window ledger rows
- latest asset/window state

Authoritative run snapshots and run events must remain the source of truth. Repair must not append run events, rewrite run snapshots, or synthesize lifecycle transitions.

## Current Baseline

Implemented foundations already present:

- `FavnOrchestrator.TransitionWriter` persists run transitions first, then best-effort projects derived backfill state.
- `FavnOrchestrator.Backfill.CoverageProjector` derives coverage baselines from successful run metadata.
- `FavnOrchestrator.Backfill.Projector` derives backfill-window status and asset/window state from backfill child run transitions.
- `FavnOrchestrator.BackfillManager` creates pending backfill-window rows during submission and records parent/child lineage.
- `FavnOrchestrator.Storage` and `Favn.Storage.Adapter` expose read/write callbacks for the three backfill read models across memory, SQLite, and Postgres adapters.
- The private HTTP API and `mix favn.backfill` can submit, inspect, and rerun operational backfills.

Important gaps to close:

- Existing projectors assume the pending backfill-window row already exists, so deleting the read model prevents simple transition replay from rebuilding the ledger.
- The storage contract has upsert/list callbacks for read models but no explicit scoped reset or replace operation.
- There is no public orchestrator maintenance entrypoint, HTTP command, or local Mix task for projection repair.
- Projection repair behavior for missing historical metadata is not documented or surfaced to operators.
- Reusing `Backfill.Projector` directly would also recalculate and persist parent backfill status transitions, which violates the repair constraint not to mutate authoritative run history.

## Design Decisions

### Authoritative Replay Source

Use persisted `RunState` snapshots as the primary repair source and persisted run events only as supporting evidence when needed.

Run snapshots carry the latest durable facts required to rebuild derived state:

- parent backfill run metadata, including pipeline target and coverage-baseline reference
- child backfill run trigger lineage, including `backfill_run_id` and `window_key`
- child pipeline metadata, including resolved anchor/window context when present
- terminal child result payloads, including asset results and per-asset metadata
- successful run coverage metadata for coverage-baseline projection

Run events remain important for audit and future replay precision, but the first repair path should not require reconstructing every historical intermediate snapshot from events. The repair output is current read-model state, not historical event playback.

### Repair Semantics

Add a new orchestrator-owned repair module, tentatively `FavnOrchestrator.Backfill.Repair`, with a pure planning phase and an apply phase.

The planning phase should:

- list candidate runs from storage
- identify parent backfill runs by `submit_kind: :backfill_pipeline`
- identify child backfill runs by `trigger.kind == :backfill`, `trigger.backfill_run_id`, and `trigger.window_key`
- derive coverage baselines from successful run coverage metadata
- derive backfill-window rows from child run lineage and anchor metadata
- derive latest asset/window state from terminal child run asset results
- produce a summary with projected counts, skipped counts, and reasons

The apply phase should:

- optionally clear only the derived rows in the requested repair scope
- upsert rebuilt coverage baselines, backfill windows, and asset/window states
- never call `TransitionWriter.persist_transition/3`
- never mutate `RunState` snapshots or append run events
- return a structured report suitable for CLI/API output and tests

### Scope And Filtering

Support a small first set of filters:

- `backfill_run_id` for one parent backfill repair
- `pipeline_module` for pipeline-scoped repair
- no filter for full derived-state repair in local/dev maintenance

Default behavior should be dry-run for operator-facing tooling. The apply path should require an explicit `--apply` flag in the Mix task and a distinct HTTP request field such as `{"apply": true}`.

### Missing Or Invalid Historical Metadata

Repair should be explicit and conservative.

If a run lacks enough metadata to rebuild a read-model row, skip that row and include a reason in the report. Do not fabricate window boundaries, pipeline modules, asset refs, or coverage identities.

Expected skip reasons:

- missing backfill child trigger
- missing pipeline module
- missing anchor/window metadata
- invalid or unsupported window kind
- missing terminal result for asset/window state
- invalid asset result ref or status
- missing required coverage metadata
- raw coverage source identity rejected

This preserves the invariant that repaired read models are derived only from persisted authoritative data.

### Storage Contract

Extend `Favn.Storage.Adapter` and `FavnOrchestrator.Storage` with scoped read-model maintenance callbacks rather than broad table-specific deletes embedded in repair code.

Recommended minimal callback:

```elixir
@callback replace_backfill_read_models(
            scope :: keyword(),
            coverage_baselines :: [CoverageBaseline.t()],
            backfill_windows :: [BackfillWindow.t()],
            asset_window_states :: [AssetWindowState.t()],
            adapter_opts()
          ) :: :ok | {:error, error()}
```

The adapter implementation should clear only rows matching the provided scope and then insert the rebuilt rows in one adapter-owned operation where possible.

Scope behavior:

- `backfill_run_id` clears windows for that parent and asset/window states whose `latest_parent_run_id` is that parent.
- `pipeline_module` clears coverage baselines, windows, and asset/window states for that pipeline.
- empty scope clears all three derived read-model tables.

If this proves too coarse for coverage baselines scoped by one `backfill_run_id`, keep coverage repair additive for parent-scoped repair and document that stale unrelated coverage rows require pipeline or full repair.

### Operator Surface

Add a local command under the existing backfill task:

```sh
mix favn.backfill repair --backfill-run-id RUN_ID
mix favn.backfill repair --pipeline-module MyApp.Pipelines.Daily
mix favn.backfill repair --apply --pipeline-module MyApp.Pipelines.Daily
```

The local command should call a private orchestrator HTTP maintenance endpoint so the operator workflow matches existing local tooling.

Recommended endpoint:

```http
POST /api/orchestrator/v1/backfills/projections/repair
```

Request body:

```json
{
  "apply": false,
  "backfill_run_id": "run_backfill_123",
  "pipeline_module": "MyApp.Pipelines.Daily"
}
```

Response body should include the structured repair report, including dry-run/apply mode, counts, and skip reasons.

Use operator authorization, not viewer authorization. Add audit records when `apply` is true.

## Boundary Design

### `favn_orchestrator`

Owns repair semantics and private maintenance API.

- Add `FavnOrchestrator.repair_backfill_projections/1` as the internal facade.
- Add `FavnOrchestrator.Backfill.Repair` for planning and applying repair.
- Add pure helper functions for deriving read-model structs from `RunState` values.
- Keep repair independent from `TransitionWriter` to avoid mutating run history.
- Add API route, contract schema, DTOs, auth handling, and audit logging.

### Storage Adapters

Own scoped replacement semantics.

- Add the storage callback and facade function.
- Implement memory, SQLite, and Postgres behavior consistently.
- Prefer adapter-owned transactions for SQLite/Postgres replacement.
- Keep filters and ordering behavior for existing list callbacks unchanged.

### `favn_local`

Owns local developer/operator workflow behind the Mix task.

- Add an orchestrator client function for the repair endpoint.
- Add `Favn.Dev.Backfill.repair_projections/1`.
- Normalize CLI filters and render the repair report clearly.

### `favn`

Owns the public Mix task entrypoint.

- Add `mix favn.backfill repair` parsing.
- Document dry-run default and `--apply` behavior in task docs.
- Keep diagnostics user-facing and specific.

## Implementation Plan

1. Add focused tests that describe repair from a deleted read model and from drifted read models using memory storage.
2. Add pure repair planning in `FavnOrchestrator.Backfill.Repair`, returning a report without writes.
3. Add scoped storage replacement to the storage facade and memory adapter; make the initial tests pass.
4. Implement SQLite and Postgres scoped replacement with adapter-owned transactional behavior where available.
5. Add orchestrator facade and private HTTP repair endpoint with operator auth, validation, DTOs, and audit on apply.
6. Extend `Favn.Dev.OrchestratorClient`, `Favn.Dev.Backfill`, and `mix favn.backfill` with dry-run/apply repair commands.
7. Add tests for invalid filters, dry-run no-op behavior, apply behavior, skipped metadata reporting, and no mutation of run events/snapshots.
8. Update `docs/FEATURES.md`, `README.md`, `Favn.AI` breadcrumbs, relevant moduledocs, and task docs only when the repair workflow is implemented.

## Test Plan

Narrow tests first:

- `mix test apps/favn_orchestrator/test/backfill_repair_test.exs`
- `mix test apps/favn_orchestrator/test/api/router_test.exs`
- `mix test apps/favn_local/test`
- `mix test apps/favn/test/mix_tasks/public_tasks_test.exs`
- adapter-specific SQLite/Postgres tests for scoped replacement

Before finishing implementation, run the full Elixir gate:

```sh
mix format
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix xref graph --format stats --label compile-connected
```

## Acceptance Mapping

- Documented repair/replay workflow: add `mix favn.backfill repair` docs, `README.md`, `docs/FEATURES.md`, `Favn.AI` breadcrumbs, and relevant moduledocs after implementation.
- Tests cover replay after read-model deletion or projection drift: add orchestrator repair tests plus storage replacement tests.
- Repair does not mutate authoritative run history: assert run snapshots and run events are unchanged before and after apply.
