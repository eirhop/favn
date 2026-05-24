# N+1 And Storage Scalability Review

Date: 2026-05-24

Priority: P2 - performance and scalability

Scope reviewed:

- `apps/favn_orchestrator`
- `apps/favn_storage_postgres`
- `apps/favn_storage_sqlite`
- `apps/favn_view`
- `apps/favn_sql_runtime`
- `apps/favn_duckdb`
- `apps/favn_duckdb_adbc`

Structure docs read before inspection:

- `docs/structure/README.md`
- `docs/structure/favn_orchestrator.md`
- `docs/structure/favn_storage_postgres.md`
- `docs/structure/favn_storage_sqlite.md`
- `docs/structure/favn_view.md`
- `docs/structure/favn_sql_runtime.md`
- `docs/structure/favn_duckdb.md`
- `docs/structure/favn_duckdb_adbc.md`

## Severe Scalability Issues

### Active catalogue and detail APIs load all manifest runs

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator.ex:391`
- `apps/favn_orchestrator/lib/favn_orchestrator.ex:407`
- `apps/favn_orchestrator/lib/favn_orchestrator.ex:424`
- `apps/favn_orchestrator/lib/favn_orchestrator.ex:445`
- `apps/favn_orchestrator/lib/favn_orchestrator.ex:1904`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:2712`
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex:3468`

Call chain:

`FavnView.AssetCatalogueLive`, `FavnView.PipelinesLive`, `FavnView.AssetDetailLive`, or `FavnView.PipelineDetailLive` call `FavnOrchestrator.active_*`, which calls `catalogue_runs/1`, which calls `list_runs(manifest_version_id: manifest_version_id)`, which reaches `Storage.list_runs/1` and the adapter `list_runs_query/1`.

Current data flow:

The orchestrator loads every persisted run for the active manifest, then derives latest asset and pipeline status in memory. Asset detail also loads first-page freshness/window state with `limit: Page.max_limit()` and then filters target data in memory.

Expected query count as data grows:

The query count is small, but the result size is O(all runs for the manifest). As run history grows, one UI request can return and decode the full manifest run history. Asset detail can also become incomplete when relevant freshness/window rows exceed the 500-row page cap.

Smallest boundary-safe fix:

Add orchestrator read-model APIs for exact UI needs and storage primitives behind them: `latest_runs_by_asset_refs/2`, `latest_runs_by_pipeline_targets/2`, and cursor-paged `list_target_runs/2`. Keep `favn_view` on the public orchestrator facade and avoid direct storage access.

### Run detail loads every backfill window

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:972`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1307`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1326`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1342`

Call chain:

`FavnView.RunDetailLive.load_run/2` calls `FavnOrchestrator.get_operator_run_detail/2`, which calls `RunReadModel.operator_run_detail/2`, which calls `execution_group_window_summaries/1`, `detail_backfill_windows/1`, or `backfill_failures/2`, which calls `list_all_backfill_windows/3`.

Current data flow:

The operator detail page recursively scans every backfill-window row for the group using `Storage.scan_backfill_windows([backfill_run_id: ...], limit: Page.max_limit())`, then builds window summaries, progress, and failure data in memory.

Expected query count as data grows:

One detail response performs O(total windows / 500) storage scans and builds an O(total windows) in-memory/render payload. Large backfills can make a single run detail page fetch and render the entire backfill ledger.

Smallest boundary-safe fix:

Split operator detail into bounded read models. Use persisted `backfill_progress` and execution-group summary for counts, add cursor/keyset `list_execution_group_windows(group_id, limit, cursor, filters)`, and return only limited failure samples by default.

### Staleness explanation scans all freshness rows

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator/freshness/query.ex:67`
- `apps/favn_orchestrator/lib/favn_orchestrator/freshness/query.ex:84`
- `apps/favn_orchestrator/lib/favn_orchestrator/freshness/query.ex:90`

Call chain:

`FavnOrchestrator.explain_asset_staleness/2` calls `Freshness.Query.current_upstream_states/1`, which calls `Storage.scan_asset_freshness_states([], limit: Page.max_limit())` until all requested upstream node keys are found or the table is exhausted.

Current data flow:

The API looks keyed because callers pass `upstream_node_keys`, but storage is scanned broadly and filtered in memory against `latest_success_node_key`.

Expected query count as data grows:

Worst-case query count is O(total freshness rows / 500) per explanation, with decode cost proportional to scanned rows.

Smallest boundary-safe fix:

Add a storage API keyed by planned node key, such as `get_asset_freshness_states_by_node_keys/1`, backed by queryable encoded/hash node-key columns and indexes. Reuse the execution-time batch lookup pattern in `apps/favn_orchestrator/lib/favn_orchestrator/run_server/execution.ex:2052`.

## Likely N+1s

### Operator run detail queries events once per run in a group

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:735`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:868`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:874`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:880`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1139`

Call chain:

`RunDetailLive.load_run/2` calls `get_operator_run_detail(run_id, include: [:events])`, which loads the execution group and calls `execution_group_asset_attempts/2`, which loops through `group.runs` and calls `run_events(run.id, :operator)` for each run.

Current data flow:

Each child run gets its own `Storage.list_run_events(run_id, limit: 200, order: :desc)` query so step summaries can be merged with event-derived state.

Expected query count as data grows:

For N child runs, detail performs approximately `1 get_run + 1 list_execution_group_runs + N list_run_events + 2 list_execution_group_events`, plus backfill-window scans. Large backfills make the event portion grow linearly with child count.

Smallest boundary-safe fix:

Use `Storage.list_execution_group_events(group_id, limit/order/window)` once and group events by `run_id`, or introduce a persisted/batched asset-attempt read model for operator detail.

### Backfill failure samples fetch child run and events per failed window

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1356`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1366`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex:1383`

Call chain:

`backfill_failures/2` filters failed windows, takes the first 10, maps through `backfill_failure/1`, then calls `child_failure_context/1`, which performs `Storage.get_run/1` and `Storage.list_run_events/1` per sampled failed child.

Current data flow:

The N+1 is bounded to the 10-failure detail sample, but each sampled failure can add two storage calls.

Expected query count as data grows:

The current cap limits this to about 20 extra queries per detail response, independent of total failures. It is tolerable but unnecessary.

Smallest boundary-safe fix:

Persist `asset_ref` and a failure summary directly in the backfill-window read model, or batch child run/event retrieval for sampled windows.

### Expanded run-list rows refresh one group detail at a time

Files:

- `apps/favn_view/lib/favn_view/runs_list_live.ex:284`
- `apps/favn_view/lib/favn_view/runs_list_live.ex:298`

Call chain:

`refresh_runs/1` calls `refresh_expanded_details/1`, which reduces over `expanded_group_ids` and calls `ensure_group_detail/2`, which calls `FavnOrchestrator.get_execution_group_detail/1` per expanded group.

Current data flow:

The UI stays boundary-safe, but it compensates for missing batch read-model shape by repeatedly calling a full orchestrator detail API.

Expected query count as data grows:

If E groups are expanded, every refresh performs E detail reads. Each detail read can include per-run event queries and broad window scans.

Smallest boundary-safe fix:

Refresh only the group identified by the incoming run event, or add an orchestrator batch read-model API for expanded group details.

## Broad Scans And Unbounded Reads

### `list_runs` has no default pagination

Files:

- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:2712`
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex:3468`

Current data flow:

`list_runs_query/1` only adds `LIMIT` when callers pass `:limit`. Several operator catalogue/detail paths pass only `manifest_version_id`.

Expected query count as data grows:

One query can return all run snapshots for a manifest, with decode and memory costs proportional to history size.

Smallest boundary-safe fix:

Keep raw unbounded scans internal and explicit. Operator APIs should use bounded read-model calls and cursor/keyset scans.

### Execution-group summary rebuild is an implicit full-table maintenance path

Files:

- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1949`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1960`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1974`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1987`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1998`

Call chain:

`RunReadModel.page_execution_groups/1` can call `rebuild_empty_execution_group_summary_page/3`, which calls `Storage.rebuild_execution_group_summaries/0`, which scans all group ids and rebuilds each group summary.

Current data flow:

The rebuild path selects every distinct `root_execution_group_id`, then fetches runs and windows per group and upserts one summary per group.

Expected query count as data grows:

Rebuild is O(groups) plus about three queries per group. It can be triggered by a UI overview read if the summary table is empty.

Smallest boundary-safe fix:

Keep summary maintenance on write paths. Make full rebuild an explicit chunked maintenance task with cursor progress instead of an implicit UI read fallback.

### Text-search filters scan execution-group summaries

Files:

- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:2786`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:2792`
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex:3541`
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex:3547`

Current data flow:

`target_asset` and `search` filters operate over `target_refs_text` with substring matching.

Expected query count as data grows:

Filtered operator lists can become O(summary rows), even though the summary table is smaller than raw runs.

Smallest boundary-safe fix:

Accept short-term for P2 if filtered lists are shallow. If this becomes common, add a normalized `execution_group_targets` projection or indexed searchable projection.

## Missing Indexes

### Manifest run history index

Files:

- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:27`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:40`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:25`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:38`

Risk:

Catalogue/detail run reads filter by `manifest_version_id` and order by `updated_seq DESC, run_id DESC`, but existing foundation indexes only cover `status, updated_seq` and later execution-group columns.

Smallest fix:

Add `favn_runs(manifest_version_id, updated_seq DESC, run_id)` for Postgres and the equivalent SQLite index.

### Backfill-window scan index

Files:

- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:104`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:108`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:102`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:106`

Risk:

`list_all_backfill_windows/3` scans by `backfill_run_id` and cursor-orders by `window_start_at`, `backfill_run_id`, `pipeline_module`, and `window_key`. Existing indexes do not match that scan shape.

Smallest fix:

Add `favn_backfill_windows(backfill_run_id, window_start_at, pipeline_module, window_key)`.

### Asset-window detail index

Files:

- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:111`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/create_foundation.ex:134`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:109`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/create_foundation.ex:132`

Risk:

Asset detail loads asset-window states by `manifest_version_id`, then filters the target asset and timeline range in memory. Current indexes cover unique asset/window key and pipeline/status shapes, not the detail read shape.

Smallest fix:

Add a storage API that takes asset ref plus window range, backed by an index like `favn_asset_window_states(manifest_version_id, asset_ref_module, asset_ref_name, window_kind, window_start_at)`.

### Freshness node-key index

Files:

- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_asset_freshness_state.ex:7`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_asset_freshness_state.ex:7`

Risk:

`latest_success_node_key` is embedded in the payload/read model, but `explain_asset_staleness/2` needs to look it up by key. Without a queryable encoded/hash node-key column, that API falls back to full scans.

Smallest fix:

Add an encoded or hashed `latest_success_node_key` column plus index and expose keyed lookup through storage.

## Acceptable For Now

### View boundaries are respected

`favn_view` reviewed paths call `FavnOrchestrator` facade functions rather than storage adapters, repos, scheduler internals, or runner internals directly.

### Log reads are bounded and indexed

Files:

- `apps/favn_view/lib/favn_view/logs_live_support.ex:11`
- `apps/favn_view/lib/favn_view/logs_live_support.ex:121`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_log_entries.ex:33`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_log_entries.ex:31`

The UI loads a bounded initial log set and replays by cursor. Storage has indexes for global sequence and common filters including run id, run id plus asset step id, asset ref, level, source, stream, and runner execution id.

### SQL session pooling exists for DuckDB and DuckDB ADBC

Files:

- `apps/favn_sql_runtime/lib/favn/sql/client.ex:454`
- `apps/favn_sql_runtime/lib/favn/sql/session_pool.ex:1`
- `apps/favn_duckdb/lib/favn/sql/adapter/duckdb.ex:246`
- `apps/favn_duckdb_adbc/lib/favn/sql/adapter/duckdb/adbc.ex:222`

DuckDB and ADBC adapters are poolable, and the SQL client keys reuse by connection/config, required catalog set, and adapter fingerprint. Reviewed paths did not show a repeated connection setup loop that bypasses this pool.

### Execution-time freshness lookup is batched

Files:

- `apps/favn_orchestrator/lib/favn_orchestrator/run_server/execution.ex:2052`
- `apps/favn_orchestrator/lib/favn_orchestrator/storage.ex:532`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex:1085`
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex:1100`

The hot runner freshness path computes planned lookup keys and uses `get_asset_freshness_states_by_keys/1`, so it avoids one storage query per asset.
