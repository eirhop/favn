# favn_storage_postgres

Purpose: Postgres implementation of the orchestrator storage adapter, including
managed/external repo modes, schema readiness, migrations, JSON-safe
run/event/backfill, freshness, materialization-claim, and target-status DTO
persistence, and canonical payload persistence. High-growth operator reads use
indexed log cursor scans, persisted execution-group summaries, and persisted
target-status rows rather than repeated full run aggregation.

Code:
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/`
- Run-event global sequence migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_run_event_global_sequence.ex`
- Asset freshness state migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_asset_freshness_state.ex`
- Materialization claim migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_materialization_claims.ex`
- Execution-group summaries and log cursor indexes: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_execution_group_summaries.ex`
- Target status projection migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_target_statuses.ex`
- Pipeline target run-history query metadata migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_run_pipeline_query_column.ex`

Tests:
- `apps/favn_storage_postgres/test/`
- live Postgres coverage is opt-in through configured environment variables

Use when changing Postgres persistence, migrations, readiness checks,
transaction semantics, materialization claim acquisition, or production-oriented
storage behavior.
