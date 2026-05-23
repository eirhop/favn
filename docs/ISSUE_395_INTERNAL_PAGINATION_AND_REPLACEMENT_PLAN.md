# Issue 395 Internal Pagination And Replacement Scope Plan

Issue: #395

Status: planned.

## Goal

Make internal persisted-truth traversal stable under concurrent writes and make
backfill read-model replacement scopes explicit, safe, and consistent across
memory, SQLite, and Postgres adapters.

This follows `docs/refactor_review_standard.md`: expose storage contracts that
already exist implicitly, keep persisted truth behind the orchestrator storage
boundary, and protect behavior with adapter-level contract tests.

## Current Baseline

- `FavnOrchestrator.Page` is offset-only and is used by public operator reads and
  internal scans.
- Public/API-facing list operations such as backfill windows, coverage baselines,
  asset-window states, and asset freshness expose `:limit` and `:offset`. These
  are acceptable UI-facing contracts and should not be replaced globally.
- Internal full-walk helpers still use offset pagination:
  `FavnOrchestrator.Backfill.Projector.list_all_backfill_windows/1`,
  `FavnOrchestrator.RunReadModel` backfill-window hydration, and
  `FavnOrchestrator.Freshness.Query` upstream freshness inspection.
- Some full-walk helpers accumulate with `acc ++ items`, which makes repeated
  page accumulation grow quadratically with row count.
- SQL adapters order list pages deterministically, but they still use
  `LIMIT/OFFSET`, so mutable tables can skip or duplicate rows between pages.
- `replace_backfill_read_models/4` accepts a keyword scope where `[]` means
  destructive replacement of all derived rows.
- SQL adapter deletion silently filters unsupported non-empty scope keys out of
  `delete_scoped/4`; an unsupported scope can therefore delete nothing while the
  replacement rows are still inserted.
- `favn_orchestrator` is the owner of the read-model contract. `favn_view` should
  continue using public orchestrator facades and should not learn about cursor
  scan internals.
- Issue #394 has already added bounded freshness lookup and backfill progress
  contracts on `main`; this issue should extend those contracts rather than
  reintroducing broad scans into run startup or parent progress computation.

## Architectural Decision

Keep offset pagination for public operator pages and introduce separate internal
cursor/keyset scan contracts under `FavnOrchestrator.Storage` and
`Favn.Storage.Adapter`.

Replace keyword replacement scopes with a tagged contract:

```elixir
@type read_model_replacement_scope ::
        :all
        | {:backfill_run, String.t()}
        | {:pipeline, module()}
```

The facade should normalize and validate replacement scopes before adapter work.
Adapters should reject unsupported scopes consistently with
`{:error, {:unsupported_replacement_scope, scope}}` and must not partially mutate
state after detecting an unsupported scope.

Do not extend `FavnOrchestrator.Page` into a hybrid offset/cursor type. A separate
cursor page keeps UI pagination and internal repair/traversal semantics explicit.

## Cursor Scan Contract

Add an orchestrator-owned cursor page, for example:

```elixir
defmodule FavnOrchestrator.CursorPage do
  @type cursor :: map() | nil

  @type t(item) :: %__MODULE__{
          items: [item],
          limit: pos_integer(),
          after_cursor: cursor(),
          has_more?: boolean(),
          next_cursor: cursor()
        }
end
```

Cursor rules:

- Cursors are adapter-neutral maps built from the deterministic order columns of
  the last returned item.
- Cursors are internal terms passed through the storage facade, not public HTTP
  tokens.
- `limit` should reuse the existing `Page.max_limit/0` cap unless a separate max
  is needed later.
- Empty result pages return `next_cursor: nil` and `has_more?: false`.
- A page fetches `limit + 1` rows to compute `has_more?`, returns only `limit`
  rows, and sets `next_cursor` from the last returned row.
- Keyset traversal is not a snapshot isolation guarantee. Inserts that sort
  before the current cursor may be missed until the next scan, but original rows
  should not be skipped or duplicated because an earlier page changed size.

Add scan callbacks only for internal traversal paths that need them now:

```elixir
@callback scan_backfill_windows(filter_opts(), keyword(), adapter_opts()) ::
            {:ok, CursorPage.t(BackfillWindow.t())} | {:error, error()}

@callback scan_asset_freshness_states(filter_opts(), keyword(), adapter_opts()) ::
            {:ok, CursorPage.t(AssetFreshnessState.t())} | {:error, error()}
```

Scan option rules:

- Supported scan options are `:limit` and `:after`.
- `:after` is `nil` or a cursor previously returned by the same scan contract.
- Unknown scan options return `{:error, :invalid_cursor_pagination}` or a tagged
  unsupported option error before adapter work.
- Filter support should match the corresponding list contract where practical,
  but scan callbacks must reject unsupported filters rather than silently ignoring
  them.

## Backfill Window Cursor

Use the existing deterministic order:

```text
window_start_at ASC,
backfill_run_id ASC,
pipeline_module ASC,
window_key ASC
```

Cursor shape:

```elixir
%{
  kind: :backfill_window,
  window_start_at: DateTime.t(),
  backfill_run_id: String.t(),
  pipeline_module: module(),
  window_key: String.t()
}
```

SQL keyset predicate:

```text
(window_start_at, backfill_run_id, pipeline_module, window_key) >
  (:window_start_at, :backfill_run_id, :pipeline_module, :window_key)
```

SQLite can express the comparison as equivalent `OR` clauses if row-value
comparison support is not desirable for adapter portability.

Memory implementation should sort with the same tuple and drop rows until the
cursor tuple is passed. This remains in-memory but preserves adapter semantics.

## Asset Freshness Cursor

Use the existing deterministic order:

```text
updated_at DESC,
asset_ref_module ASC,
asset_ref_name ASC,
freshness_key ASC
```

Cursor shape:

```elixir
%{
  kind: :asset_freshness_state,
  updated_at: DateTime.t(),
  asset_ref_module: module(),
  asset_ref_name: atom(),
  freshness_key: String.t()
}
```

SQL keyset predicate must account for the descending leading column:

```text
updated_at < :updated_at
OR (updated_at = :updated_at AND asset_ref_module > :asset_ref_module)
OR (updated_at = :updated_at AND asset_ref_module = :asset_ref_module AND asset_ref_name > :asset_ref_name)
OR (updated_at = :updated_at AND asset_ref_module = :asset_ref_module AND asset_ref_name = :asset_ref_name AND freshness_key > :freshness_key)
```

This scan is a stability improvement for freshness inspection. It is not a
replacement for exact-key freshness lookups added by issue #394.

## Internal Call-Site Changes

Change full-walk helpers to consume cursor scans and accumulate with prepend plus
final reverse instead of repeated list append.

Targeted changes:

- Replace `Backfill.Projector.list_all_backfill_windows/1` internals with
  `Storage.scan_backfill_windows/2`.
- Replace `RunReadModel.list_all_backfill_windows/3` internals with
  `Storage.scan_backfill_windows/2`.
- Prefer `Storage.get_backfill_progress/1` for backfill progress summaries where
  aggregate counts are enough, and scan windows only for detail/failure views that
  require row-level data.
- Replace `Freshness.Query.fetch_current_upstream_states/3` with
  `Storage.scan_asset_freshness_states/2` while keeping early termination once all
  requested upstream node keys have been found.
- Keep public `FavnOrchestrator.list_backfill_windows/1`,
  `list_coverage_baselines/1`, `list_asset_window_states/1`, and
  `list_asset_freshness/1` offset-based for UI/API compatibility.

`FavnOrchestrator.RunServer.Execution` already uses
`Storage.get_asset_freshness_states_by_keys/1` after issue #394. It should remain
on bounded exact-key lookup and should not move to cursor scans.

## Replacement Scope Contract

Change `Favn.Storage.Adapter.replace_backfill_read_models/5` and
`FavnOrchestrator.Storage.replace_backfill_read_models/4` to accept
`read_model_replacement_scope()` instead of keyword filters.

Scope semantics:

- `:all` deletes all coverage baselines, backfill windows, asset-window states,
  and backfill progress rows before inserting replacement rows and rebuilding
  progress.
- `{:backfill_run, id}` deletes backfill windows and progress for `id`, deletes
  asset-window states whose `latest_parent_run_id` is `id`, and deletes coverage
  baselines whose `created_by_run_id` is `id`.
- `{:pipeline, module}` deletes coverage baselines, backfill windows, and
  asset-window states whose `pipeline_module` matches `module`.
- Pipeline-scoped replacement should rebuild progress only for affected
  `backfill_run_id`s rather than deleting all progress. Affected ids are the
  union of ids from deleted windows and inserted replacement windows.
- Replacement rows are still upserted using their natural keys after scoped
  deletion.
- Adapters must validate the scope before starting destructive work.
- Adapters must perform scoped deletion, replacement insertion, and progress
  cleanup/rebuild in one transaction or one memory-server call.

`Backfill.Repair` should continue allowing a dry-run plan without a scope, but
`apply: true` should require an explicit replacement scope. For full replacement,
callers should pass `scope: :all`; an omitted apply scope should return a clear
error such as `{:error, :replacement_scope_required}`.

## Adapter Implementation Notes

Memory:

- Add shared cursor tuple builders for backfill windows and asset freshness.
- Reject unsupported replacement scope terms before mutating GenServer state.
- Rebuild only affected backfill progress rows for scoped replacement.

SQLite:

- Add keyset WHERE builders alongside the existing offset list builders; do not
  change public offset list SQL.
- Add `latest_parent_run_id` to the asset-window-state replacement predicate if
  needed for `{:backfill_run, id}` parity.
- Validate replacement scope before `repo.transact/1` or as the first transaction
  step before any `DELETE`.
- Replace the current empty-scope `DELETE FROM table` behavior with explicit
  `:all` handling.

Postgres:

- Add keyset WHERE builders alongside `build_select_query/4`; keep public offset
  list SQL unchanged.
- Add missing replacement predicate support for `created_by_run_id`,
  `latest_parent_run_id`, or other fields required by the tagged scopes.
- Remove the silent unsupported-scope no-op path from `delete_scoped/4`.
- Rebuild only affected progress rows for scoped replacement.

## Landing Slices

1. Add `FavnOrchestrator.CursorPage` and storage facade validation for cursor scan
   options, with focused unit tests.
2. Add `scan_backfill_windows/3` to the adapter behaviour, memory adapter, SQLite,
   and Postgres, plus adapter contract tests for tie-break ordering and
   mutation-between-pages behavior.
3. Migrate backfill-window internal full walks to cursor scans and fix list
   accumulation to prepend/reverse.
4. Add `scan_asset_freshness_states/3` across adapters with contract tests for
   descending `updated_at` tie-breaks.
5. Migrate `Freshness.Query` upstream freshness inspection to cursor scans while
   preserving early termination.
6. Introduce the tagged `read_model_replacement_scope` type in the storage
   behaviour/facade and update `Backfill.Repair` scope normalization.
7. Update memory, SQLite, and Postgres replacement implementations to validate
   scopes, reject unsupported scopes, and rebuild only affected progress rows.
8. Add adapter contract tests for `:all`, `{:pipeline, module}`,
   `{:backfill_run, id}`, unsupported scope rejection, and stale-row removal.
9. Update docs and module docs to distinguish public offset pages from internal
   cursor scans and to document destructive `:all` replacement as opt-in.

## Tests

- Cursor page validation rejects invalid limits, invalid cursors, and unknown scan
  options.
- Backfill-window scan returns deterministic pages with
  `{window_start_at, backfill_run_id, pipeline_module, window_key}` tie-breaks.
- Backfill-window scan does not skip or duplicate already-visible rows when a row
  is inserted or deleted between page reads.
- Asset-freshness scan returns deterministic pages with `updated_at DESC` plus
  stable tie-breakers.
- Internal full-walk helpers return the same rows as the offset list APIs for a
  static dataset.
- `Backfill.Repair.repair(apply: true)` without an explicit scope fails before
  mutation.
- `replace_backfill_read_models(:all, ...)` removes stale rows from all derived
  read-model tables and rebuilds progress.
- `replace_backfill_read_models({:pipeline, module}, ...)` removes only matching
  pipeline rows and preserves other pipelines.
- `replace_backfill_read_models({:backfill_run, id}, ...)` removes only rows tied
  to the parent backfill run and preserves other backfills in the same pipeline.
- Unsupported replacement scopes return the same tagged error across memory,
  SQLite, and Postgres, with no partial mutation.
- Progress rows are removed or rebuilt for affected backfills, including when a
  replacement scope removes the last window for a backfill.

## Risks And Tradeoffs

- Cursor scans are internal terms, not public tokens. If they later become public,
  they will need versioned encoding and validation.
- Keyset scans over mutable tables provide stable traversal relative to the cursor,
  not a database snapshot. This is the right tradeoff for repair and inspection
  paths that can be rerun.
- Pipeline-scoped progress rebuild is more complex than deleting all progress, but
  it avoids unrelated progress churn and makes scoped repair semantics precise.
- `{:backfill_run, id}` scope depends on `latest_parent_run_id` for
  asset-window-state cleanup and `created_by_run_id` for coverage-baseline cleanup;
  adapter indexes may need follow-up tuning if this becomes hot.

## Non-Goals

- Do not replace UI/API offset pagination in this issue.
- Do not introduce public HTTP cursor tokens.
- Do not move storage or repair semantics out of `favn_orchestrator`.
- Do not rework run startup freshness lookup; issue #394 already moved that path
  to bounded exact-key lookup.
- Do not add snapshot isolation requirements for repair scans.
