# Issue 394 Freshness And Backfill Persistence Plan

Issue: #394

Status: planned.

## Goal

Make run startup and backfill projection scale with the explicit runtime inputs
instead of total persisted freshness or window cardinality.

This follows `docs/refactor_review_standard.md`: expose the storage contracts
that already exist implicitly, keep persisted truth behind the orchestrator
storage boundary, and protect the behavior with adapter-level tests.

## Current Baseline

- `FavnOrchestrator.RunServer.Execution.initial_freshness_context/2` calls
  `Storage.list_asset_freshness_states/1` page by page and filters each page
  against the current plan in memory.
- Startup latency therefore scales with all persisted freshness rows, not with
  the planned run graph.
- `FavnOrchestrator.Backfill.Projector` updates a child window with
  `Storage.put_backfill_window/1`, updates asset/window rows separately, then
  calls `reproject_parent/1`.
- `reproject_parent/1` pages all windows for the backfill after each child
  transition to compute parent status and counts.
- `TransitionWriter.persist_transition/3` intentionally writes the authoritative
  run snapshot/event first and invokes derived projectors afterward, so the new
  contract should keep derived backfill truth in storage without leaking adapter
  details into transition writing.

## Architectural Decision

Add explicit orchestrator storage contracts for bounded freshness lookup and
backfill child projection. Keep freshness and backfill semantics in
`favn_orchestrator`; adapters only implement indexed lookup, transactions, and
durable aggregate maintenance.

Do not make storage return UI-shaped progress. Storage should return persisted
facts such as freshness rows, backfill-window rows, and aggregate counts.
`Backfill.Projector` should decide whether the parent run needs a transition.
`favn_view` should continue to read backend state only through the public
`FavnOrchestrator` facade.

## Freshness Lookup Contract

Extend `Favn.Storage.Adapter` and `FavnOrchestrator.Storage` with a required
bounded lookup:

```elixir
@type freshness_state_key :: {module(), atom(), String.t()}

get_asset_freshness_states_by_keys([freshness_state_key()], adapter_opts()) ::
  {:ok, %{freshness_state_key() => AssetFreshnessState.t()}} | {:error, term()}
```

Contract rules:

- Keys are exact `{asset_ref_module, asset_ref_name, freshness_key}` tuples.
- Duplicate keys are collapsed by the facade before adapter work.
- An empty key list returns `{:ok, %{}}`.
- Missing keys are omitted from the returned map, not returned as errors.
- Returned states must be decoded exactly like `get_asset_freshness_state/4` and
  `list_asset_freshness_states/2`.
- Ordering is not part of the contract; callers should use the key map.

Implementation rules:

- Memory lookup reads the existing freshness-state map by key.
- SQLite and Postgres apply the key predicate in SQL and should chunk internally
  if needed to avoid parameter limits.
- Keep `list_asset_freshness_states/1` for operator/repair listing, but remove
  it from run startup.

## Runtime Freshness Plan

Change `RunServer.Execution` to derive the exact planned freshness keys before
classification:

- Build `assets_by_ref`, `refresh_policy`, and one `now` timestamp in
  `initial_freshness_context/2`.
- Add a pure helper in `FavnOrchestrator.Freshness.Decider`, for example
  `planned_lookup_keys(plan, opts)`, that uses the same policy/key logic as
  `decide_many/3`.
- Include one key per planned node for the freshness policy active at `now`.
- Load prior states with `Storage.get_asset_freshness_states_by_keys/1`.
- Index loaded states with the existing `index_freshness_states/1` behavior so
  decisions can still read by `{ref, freshness_key}` and by
  `latest_success_node_key`.
- Pass the context timestamp into every stage decision so calendar-period keys do
  not drift between startup lookup and classification.

Non-obvious behavior to preserve:

- If bounded lookup fails, keep the current conservative behavior of treating
  prior states as empty only if that is already intentional. Prefer surfacing a
  storage error if the current silent fallback is not required for compatibility.
- Forced refresh modes may still perform the bounded lookup because downstream
  freshness decisions and result metadata can need prior upstream versions.

## Backfill Aggregate Contract

Add a small orchestrator-owned struct, such as
`FavnOrchestrator.Backfill.Progress`, to represent persisted aggregate truth:

```elixir
%FavnOrchestrator.Backfill.Progress{
  backfill_run_id: String.t(),
  total_count: non_neg_integer(),
  pending_count: non_neg_integer(),
  running_count: non_neg_integer(),
  ok_count: non_neg_integer(),
  partial_count: non_neg_integer(),
  error_count: non_neg_integer(),
  cancelled_count: non_neg_integer(),
  timed_out_count: non_neg_integer(),
  status: BackfillWindow.status(),
  updated_at: DateTime.t()
}
```

Extend `Favn.Storage.Adapter` and `FavnOrchestrator.Storage` with named
backfill-progress operations:

```elixir
apply_backfill_child_projection(
  BackfillWindow.t(),
  [AssetWindowState.t()],
  adapter_opts()
) :: {:ok, Backfill.Progress.t()} | {:error, term()}

get_backfill_progress(String.t(), adapter_opts()) ::
  {:ok, Backfill.Progress.t()} | {:error, term()}

rebuild_backfill_progress(String.t(), adapter_opts()) ::
  {:ok, Backfill.Progress.t()} | {:error, term()}
```

Contract rules:

- `apply_backfill_child_projection/3` upserts the child window, upserts all
  derived asset/window states, updates the aggregate counts for the parent
  backfill, and returns the new aggregate in one storage operation.
- The operation computes count deltas from the previously persisted window
  status, so repeated writes with the same status are idempotent for counts.
- If an aggregate row is missing, the operation repairs it from window rows in
  the same transaction before returning.
- `rebuild_backfill_progress/2` recomputes aggregate truth from
  `favn_backfill_windows` and is the explicit repair path for migrations,
  manual repair, and consistency checks.
- Parent status is derived from counts with the current `parent_status/1` rules:
  any pending/running window means `:running`; all `:ok` means `:ok`; all
  `:cancelled` means `:cancelled`; all `:timed_out` means `:timed_out`; any
  success among terminal mixed results means `:partial`; otherwise `:error`.

## Persistence Shape

Add a derived aggregate table for SQLite and Postgres:

```text
favn_backfill_progress
  backfill_run_id text primary key
  total_count integer not null
  pending_count integer not null
  running_count integer not null
  ok_count integer not null
  partial_count integer not null
  error_count integer not null
  cancelled_count integer not null
  timed_out_count integer not null
  status text not null
  record_payload text not null
  updated_at timestamp/text not null
```

Recommended indexes:

- Keep the existing unique window index on
  `(backfill_run_id, pipeline_module, window_key)` for child projection.
- Add or verify an index on `favn_backfill_windows(backfill_run_id, status)` for
  aggregate rebuild.
- Keep freshness lookup backed by the existing unique freshness index on
  `(asset_ref_module, asset_ref_name, freshness_key)`.

Migration/backfill rules:

- Existing deployments should rebuild `favn_backfill_progress` from
  `favn_backfill_windows` during migration where practical.
- If migration-time decoding is not practical, adapter startup or first access
  should call the same `rebuild_backfill_progress/2` path and document the lazy
  repair behavior.
- `replace_backfill_read_models/4` should replace aggregate rows consistently
  with windows, either by accepting progress rows or by rebuilding progress after
  replacement inside the same adapter transaction.

## Projector Flow

Change `FavnOrchestrator.Backfill.Projector` as follows:

- Keep child context extraction and window/asset-state domain construction in
  the projector.
- Replace separate `Storage.put_backfill_window/1`,
  `Storage.put_asset_window_state/1`, and `list_all_backfill_windows/1` calls
  with `Storage.apply_backfill_child_projection/2` for terminal child
  transitions.
- Use the returned `Backfill.Progress` to decide whether the parent run needs a
  status transition and to populate parent event data/result counts.
- For running child transitions, either use the same operation with an empty
  asset-state list or add a separate `apply_backfill_window_progress/1` only if
  the distinction keeps the contract clearer.
- Keep parent `TransitionWriter.persist_transition/3` outside the adapter
  transaction. The aggregate row becomes the immediately consistent persisted
  progress truth; the parent run event remains the authoritative lifecycle event
  and can be retried idempotently.
- Change `reproject_parent/1` to read `Storage.get_backfill_progress/1` or call
  `Storage.rebuild_backfill_progress/1`, not page all windows.

Change `FavnOrchestrator.BackfillManager` where pending windows are created:

- Prefer a small bulk storage operation for initial pending windows and progress
  initialization if the adapter code stays simpler.
- Otherwise keep `put_backfill_window/1` for setup and call
  `Storage.rebuild_backfill_progress/1` once after all pending rows are written.
- Do not make every setup `put_backfill_window/1` silently mutate progress unless
  that behavior is documented as part of the storage contract.

## Adapter Implementation Notes

Memory adapter:

- Store `backfill_progress` next to `backfill_windows`.
- Update the window map, asset-window map, and progress map in one GenServer call.
- Rebuild by grouping current in-memory windows by `backfill_run_id`.

SQLite adapter:

- Use the existing repo transaction helper around child projection.
- SQLite write transactions serialize concurrent writers, so recomputing or
  delta-updating the single aggregate row inside the transaction is sufficient.
- Use dynamic `OR` groups or chunked `IN` predicates for freshness-key lookup.

Postgres adapter:

- Use one transaction for child projection.
- Lock the affected aggregate row or perform atomic count updates from the old
  window status to avoid lost updates under concurrent child completions.
- Use a multi-key predicate or an `unnest`/`VALUES` join for freshness lookup,
  with chunking if adapter parameter handling requires it.

## Landing Slices

1. Add adapter contract tests for bounded freshness lookup and missing-key
   behavior.
2. Add `get_asset_freshness_states_by_keys/1` to the storage facade, behavior,
   memory adapter, SQLite adapter, and Postgres adapter.
3. Change `RunServer.Execution` to derive planned lookup keys and remove startup
   use of `list_asset_freshness_states/1`.
4. Add `Backfill.Progress` and storage contract tests for progress rebuild from
   window rows.
5. Add SQLite/Postgres migrations and memory state for `favn_backfill_progress`.
6. Implement `get_backfill_progress/1` and `rebuild_backfill_progress/1` across
   adapters.
7. Implement `apply_backfill_child_projection/2` across adapters with atomic
   child-window, asset-window, and aggregate updates.
8. Change `Backfill.Projector` to consume returned progress instead of scanning
   all windows after each child transition.
9. Initialize progress during backfill window creation or immediately after the
   pending windows are written.
10. Wire `Backfill.Repair` and `replace_backfill_read_models/4` through the same
    progress rebuild path.
11. Update docs and module docs for the new freshness and backfill storage
    contracts.

## Tests

- Adapter contract: multi-key freshness lookup returns only requested rows.
- Adapter contract: duplicate and empty freshness-key requests are deterministic.
- Adapter contract: missing freshness keys are omitted without failing the whole
  lookup.
- Run startup: a run with unrelated persisted freshness states only queries the
  planned keys and still makes the same skip/run decisions.
- Backfill projector: ordered child transitions update parent counts/status
  without listing all windows.
- Backfill projector: concurrent child completions preserve correct counts and
  terminal parent status for memory and SQL adapters where feasible.
- Backfill projector: repeated/idempotent child projection does not double-count.
- Backfill repair: aggregate rebuild from window rows produces the same status
  and counts as the current full-window scan logic.
- Migration/adapter: existing windows can be repaired into aggregate rows.

## Risks And Tradeoffs

- SQLite and Postgres need different SQL mechanics for multi-key lookup and
  concurrent aggregate updates; keep the behavior contract identical and hide the
  mechanics inside adapters.
- Aggregate rows are derived truth and can drift if future code writes windows
  outside the named operations. Keep setup/repair paths explicit and tests around
  every storage write that affects windows.
- Parent run status still lives in the authoritative run snapshot/event stream,
  so there is a short window where progress is updated and the parent transition
  publish fails. The aggregate repair path and idempotent parent transition
  should make this recoverable without making storage append run events.
- Calendar-period freshness keys depend on `now`; using one context timestamp
  avoids intra-run drift but means long-running pipeline stages use the run-start
  period for initial prior-state lookup.

## Non-Goals

- Do not move freshness policy, staleness, or backfill status rules into storage
  adapters.
- Do not expose storage adapters or repos through `favn_view`.
- Do not remove generic list APIs needed for operator pages and repair workflows.
- Do not solve distributed backfill coordination beyond preserving correct
  single-database transactional behavior.
