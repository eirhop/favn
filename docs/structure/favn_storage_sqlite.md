# favn_storage_sqlite

Purpose: SQLite implementation of the orchestrator storage adapter, including
schema migrations, command idempotency records, JSON-safe run/event/backfill,
freshness, materialization-claim, and target-status DTO persistence, and
canonical payload persistence. High-growth operator reads use indexed log cursor
scans, persisted execution-group summaries, and persisted target-status rows
rather than repeated full run aggregation.

Code:
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/`
- Auth/session/audit schema migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_auth_state.ex`
- Command idempotency schema migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_idempotency_records.ex`
- Run-event global sequence migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_run_event_global_sequence.ex`
- Asset freshness state migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_asset_freshness_state.ex`
- Materialization claim migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_materialization_claims.ex`
- Execution-group summaries and log cursor indexes: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_execution_group_summaries.ex`
- Target status projection migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_target_statuses.ex`
- Pipeline target run-history query metadata migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_run_pipeline_query_column.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/diagnostics.ex`

Tests:
- `apps/favn_storage_sqlite/test/`
- SQLite readiness diagnostics: `apps/favn_storage_sqlite/test/sqlite_readiness_test.exs`
- Stopped-backend control-plane restore verification, including auth/session/audit and command-idempotency state: `apps/favn_storage_sqlite/test/sqlite_control_plane_restore_test.exs`
- Auth/session/audit and command-idempotency storage restart coverage: `apps/favn_storage_sqlite/test/sqlite_storage_test.exs`
- Single-node bootstrap acceptance verification: `apps/favn_storage_sqlite/test/sqlite_single_node_bootstrap_acceptance_test.exs`

Use when changing SQLite persistence, migrations, adapter lifecycle, readiness
diagnostics, local SQLite storage semantics, materialization claim acquisition,
or single-node bootstrap persistence acceptance coverage.
