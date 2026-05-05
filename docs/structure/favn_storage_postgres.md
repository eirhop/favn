# favn_storage_postgres

Purpose: Postgres implementation of the orchestrator storage adapter, including
managed/external repo modes, schema readiness, migrations, JSON-safe
run/event/backfill DTO persistence, and canonical payload persistence.

Code:
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex`
- `apps/favn_storage_postgres/lib/favn_storage_postgres/`
- Run-event global sequence migration: `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_run_event_global_sequence.ex`

Tests:
- `apps/favn_storage_postgres/test/`
- live Postgres coverage is opt-in through configured environment variables

Use when changing Postgres persistence, migrations, readiness checks, transaction
semantics, or production-oriented storage behavior.
