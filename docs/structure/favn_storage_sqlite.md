# favn_storage_sqlite

Purpose: SQLite implementation of the orchestrator storage adapter, including
schema migrations, command idempotency records, and canonical payload persistence.

Code:
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/`
- Auth/session/audit schema migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_auth_state.ex`
- Command idempotency schema migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_idempotency_records.ex`
- Run-event global sequence migration: `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_run_event_global_sequence.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/diagnostics.ex`

Tests:
- `apps/favn_storage_sqlite/test/`
- SQLite readiness diagnostics: `apps/favn_storage_sqlite/test/sqlite_readiness_test.exs`
- Stopped-backend control-plane restore verification: `apps/favn_storage_sqlite/test/sqlite_control_plane_restore_test.exs`
- Auth/session/audit and command-idempotency storage restart coverage: `apps/favn_storage_sqlite/test/sqlite_storage_test.exs`
- Single-node bootstrap acceptance verification: `apps/favn_storage_sqlite/test/sqlite_single_node_bootstrap_acceptance_test.exs`

Use when changing SQLite persistence, migrations, adapter lifecycle, readiness
diagnostics, local SQLite storage semantics, or single-node bootstrap persistence
acceptance coverage.
