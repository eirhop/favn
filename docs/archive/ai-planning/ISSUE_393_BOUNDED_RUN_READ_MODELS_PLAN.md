# Issue 393 Bounded Run Read Models Plan

Issue: #393

Status: implemented in this branch.

## Goal

Make operator-facing run read paths scale with the requested group, page, and
cursor instead of total persisted run history. The orchestrator should own run
group semantics, lifecycle status, health, progress, and summary totals. The
view should keep only visual labels, component formatting, and interaction
state.

This follows `docs/archive/ai-planning/refactor_review_standard.md`: expose explicit storage and
read-model contracts where broad scans currently hide real runtime contracts.

## Current Baseline

The affected paths are already behind the public `FavnOrchestrator` facade, but
the contracts underneath are too broad:

- `FavnOrchestrator.RunReadModel.list_execution_groups/1` calls
  `Storage.list_runs(limit: scan_limit)`, groups in memory, then filters and
  limits after grouping.
- `FavnOrchestrator.RunReadModel.get_execution_group_detail/2` and
  `list_execution_group_events/2` call `Storage.list_runs()` and find one group
  in memory.
- `FavnOrchestrator.list_run_stream_events/2` calls `list_run_events/2`, loading
  all events for a run before applying `after_sequence` and `limit`.
- `FavnView.RunsListLive` fetches `FavnOrchestrator.list_execution_groups(limit:
  100)` and applies search, status, trigger, target, window, and sort filters in
  the LiveView after the hard limit.
- SQLite and Postgres persist `favn_runs` with status, event sequence, update
  sequence, timestamps, and the encoded run blob. They do not currently expose
  root/parent/group columns needed for indexed group reads.
- `favn_run_events` has `{run_id, sequence}` and `global_sequence` indexes, so
  run-scoped event cursor reads can be made bounded without changing the event
  model.

## Architectural Decision

Add explicit orchestrator storage contracts for run group membership and
run-event cursors, then keep orchestration semantics in `RunReadModel`.

Do not make storage return component-shaped data. Storage should return
persisted facts and bounded pages. `RunReadModel` should turn those facts into
operator semantic read models. `favn_view` should call only the public
`FavnOrchestrator` facade and convert semantic fields into labels, tones, and
page assigns.

Use offset pagination for execution group lists in V1 because it aligns with the
existing `FavnOrchestrator.Page` contract and current operator list behavior.
Use sequence cursors for run events because event replay already has stable
monotonic per-run and global sequences.

## Storage Contract V1

Extend `Favn.Storage.Adapter` and `FavnOrchestrator.Storage` with these bounded
callbacks:

```elixir
list_execution_group_runs(group_id, opts) :: {:ok, [RunState.t()]} | {:error, term()}
list_execution_group_run_ids(group_id, opts) :: {:ok, [String.t()]} | {:error, term()}
list_execution_group_events(group_id, opts) :: {:ok, [map()]} | {:error, term()}
list_run_events(run_id, run_event_opts, opts) :: {:ok, [map()]} | {:error, term()}
list_execution_groups(group_opts, opts) :: {:ok, Page.t(map())} | {:error, term()}
```

Contract rules:

- `group_id` identifies the root execution group id.
- Group membership is any run whose persisted group id is `group_id`, including
  the root run itself.
- `list_execution_group_runs/2` returns the root plus children in deterministic
  group order without scanning unrelated runs.
- `list_execution_group_run_ids/2` exists so grouped event reads can avoid
  decoding full run snapshots when only run ids are needed.
- `list_execution_group_events/2` accepts `after_global_sequence`, `limit`, and
  optionally `after_sequence` only if the cursor is explicitly run-scoped. The
  default should be chronological ascending and bounded.
- `list_run_events/3` accepts `after_sequence` and `limit`, validates them before
  adapter work, and applies both predicates in storage.
- `list_execution_groups/2` applies filters, sort, limit, and offset before page
  slicing. It returns persisted grouping facts plus summary columns, not UI
  strings.

Keep `Storage.list_runs/1` for generic legacy/internal reads, but stop using it
for group detail, group events, SSE replay, and operator run list pages.

## Persistence Shape

Add denormalized run-group query columns to `favn_runs` for SQLite and Postgres:

```text
root_execution_group_id text not null
parent_run_id text null
root_run_id text null
submit_kind text null
asset_ref_text text null
target_refs_text text null
window_key text null
started_at utc_datetime_usec null
finished_at utc_datetime_usec null
```

The exact column set should stay minimal, but the adapter must be able to filter
and sort execution group lists before decoding arbitrary blobs. At minimum, the
first implementation needs `root_execution_group_id`, `parent_run_id`,
`root_run_id`, `status`, `updated_seq`, `inserted_at`, and `updated_at`.

Recommended indexes:

- `favn_runs(root_execution_group_id, updated_seq)` for group detail and group
  run-id lookup.
- `favn_runs(root_execution_group_id, run_id)` for deterministic membership.
- `favn_runs(status, updated_seq)` remains for status-filtered generic run reads.
- `favn_run_events(run_id, sequence)` already exists and should back
  run-scoped cursor reads.
- `favn_run_events(global_sequence)` already exists and should back global and
  grouped chronological replay.

Migration/backfill rules:

- New columns are derived from `RunState` at `put_run/2` and
  `persist_run_transition/3` time.
- Existing rows must be backfilled by decoding `run_blob` once during migration
  where practical. If SQLite migration code cannot safely decode Elixir terms,
  add adapter startup repair or lazy update on first write/read, but document the
  temporary fallback explicitly.
- Memory adapter stores the same derived facts in state or computes them from
  `RunState` without changing semantics.

## Run Read Model Contract

Update `FavnOrchestrator.RunReadModel` to use bounded storage APIs:

- `list_execution_groups/1` delegates list filters and pagination to storage,
  then computes semantic fields from returned group facts and any required
  bounded child/window summaries.
- `get_execution_group_detail/2` calls `Storage.list_execution_group_runs/1` for
  the requested group only.
- `list_execution_group_events/2` calls `Storage.list_execution_group_events/2`
  or uses `list_execution_group_run_ids/1` plus run-event cursor queries.
- `list_execution_group_asset_attempts/2`, `list_execution_group_windows/2`, and
  `list_execution_group_timeline/2` reuse the bounded detail path rather than
  calling a full run scan.
- The public summary shape should add orchestrator-owned semantic fields so the
  LiveViews do not reconstruct them:
  `status`, `health`, `active?`, `progress`, `failure_count`, `summary_totals`,
  and `last_activity_at`.
- Keep visual concerns out of this module. No CSS tones, copy labels, or
  component-specific flags.

## Public Facade And API Contract

Keep `FavnOrchestrator` as the only backend dependency for `favn_view`:

- Change `FavnOrchestrator.list_execution_groups/1` to return
  `{:ok, %Page{items: groups}}` or add a new `page_execution_groups/1` facade and
  remove legacy list usage from the UI in the same issue.
- Add `FavnOrchestrator.list_execution_group_events/2` cursor options for
  grouped replay where needed.
- Change `FavnOrchestrator.list_run_stream_events/2` to call storage with
  `after_sequence` and `limit` instead of loading all events.
- Add or update HTTP endpoints only where the browser/operator API needs them.
  API routes should parse filters and pagination, then delegate to facade
  functions. They should not query storage directly.
- For SSE, keep cursor-invalid behavior: when replay needs more than
  `@sse_replay_limit`, return `410 cursor_expired` rather than falling back to an
  unbounded scan.

## Operator Filter Contract

Normalize run-list filters at the orchestrator boundary, not in the LiveView:

```elixir
[
  search: String.t() | nil,
  status: atom() | nil,
  trigger_type: atom() | nil,
  target_asset: String.t() | nil,
  window: :has_window | :no_window | nil,
  only_failed: boolean(),
  only_running: boolean(),
  only_incomplete: boolean(),
  sort: :started_desc | :failed_first | :running_first | :status_priority,
  limit: pos_integer(),
  offset: non_neg_integer()
]
```

Storage applies cheap persisted filters and stable ordering first. `RunReadModel`
applies semantic filters that require derived group totals only after it has a
bounded candidate set, or storage gets additional summary columns if correctness
requires exact filter-before-limit behavior for that semantic field.

For #393, exact filter-before-limit behavior is required for the operator list.
If a filter cannot be applied correctly without persisted group summaries, add
the smallest persisted summary/read-model table rather than over-fetching and
guessing.

## View Plan

`FavnView.RunsListLive` should become a thin paginated client of the orchestrator
read model:

- Keep filter form state and mode state in the LiveView.
- On filter changes, call `FavnOrchestrator.list_execution_groups/1` with
  normalized filters and reset `offset` to `0`.
- Render `page.items`; use `page.has_more?` and `page.next_offset` for incremental
  loading or page controls.
- Remove client-side filtering and sorting for execution groups.
- Keep `filter_options/1` either sourced from a bounded orchestrator facet API or
  limited to options present on the current page. Do not scan all groups in the
  LiveView to populate dropdowns.

`FavnView.RunDetailLive` should use a single bounded group detail call:

- Add a facade helper if needed to resolve a requested run id to its execution
  group id without loading all detail first.
- Stop calling `get_run_detail/1` only to derive the group id when storage can
  expose the persisted group id.
- Keep detail transformation, labels, tones, and component assigns in the view.

## Implementation Tasks

- [x] Add run group query metadata derivation helper in `favn_orchestrator` close
  to `RunState`/storage codec ownership.
- [x] Add SQLite and Postgres migrations for persisted run group columns and
  indexes.
- [x] Update SQLite/Postgres `put_run` and `persist_run_transition` writes to
  populate group query columns.
- [x] Add storage behaviour callbacks and `FavnOrchestrator.Storage` facade
  functions for group runs, group run ids, group events, run-event cursors, and
  paged execution group lists.
- [x] Implement the new callbacks in memory, SQLite, and Postgres adapters.
- [x] Update `FavnOrchestrator.list_run_stream_events/2` to use bounded storage
  cursor reads.
- [x] Update `RunReadModel` group detail/event/list functions to stop calling
  broad `Storage.list_runs/0` paths.
- [x] Move operator run-list filter and sort normalization into
  `FavnOrchestrator`/`RunReadModel` and return a page.
- [x] Update `FavnOrchestrator.API.Router` event/list parsing and response shapes
  where needed.
- [x] Update `FavnView.RunsListLive` and `FavnView.RunDetailLive` to consume the
  new facade contracts only.
- [x] Capture implementation status and verification notes in this plan document.

## Testing Plan

Add the smallest tests at the owning layers:

- Shared adapter contract tests for `list_execution_group_runs/2`, group run-id
  lookup, paged execution group filtering/sorting, and `list_run_events/3` with
  `after_sequence` plus `limit`.
- SQLite and Postgres migration/backfill coverage for new query columns where
  practical.
- `RunReadModel` tests proving group detail, group events, status, health,
  active state, progress, failures, and totals do not depend on unrelated runs.
- SSE replay tests proving run-scoped replay is cursor-bounded and still returns
  `cursor_expired` for unreplayable gaps.
- LiveView regressions proving filters are sent through the facade and results
  reflect filter-before-limit behavior.

Focused checks during implementation:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser test/integration/storage_adapter_contract_test.exs test/run_read_model_test.exs test/events_test.exs test/api/router_test.exs
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser test/favn_view/page_live_test.exs
```

Full gate before finishing code changes:

```bash
mix format
mix compile --warnings-as-errors
mix test
```

Focused verification run for this branch:

```bash
mix format
mix compile --warnings-as-errors
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --exclude acceptance --exclude slow --exclude browser test/integration/storage_adapter_contract_test.exs test/run_read_model_test.exs test/events_test.exs test/api/router_test.exs
MIX_ENV=test mix do --app favn_view cmd mix test --exclude acceptance --exclude slow --exclude browser test/favn_view/page_live_test.exs
```

## Risks And Follow-Up Decisions

- Adding persisted run group columns is a schema change, but it is the cleanest
  way to guarantee filter-before-limit behavior without hidden scans.
- Persisted group summary tables may be needed if exact `only_failed`,
  `only_running`, or health filtering cannot be implemented from indexed run
  columns plus bounded window reads. Prefer adding that explicit read model over
  scan-limit heuristics.
- Existing private pre-v1 contracts may be changed directly. Remove deprecated
  UI/list paths in the implementation rather than keeping compatibility shims.
