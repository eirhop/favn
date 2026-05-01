# favn_storage_sqlite

Purpose: SQLite implementation of the orchestrator storage adapter, including
schema migrations and canonical payload persistence.

Code:
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/diagnostics.ex`

Tests:
- `apps/favn_storage_sqlite/test/`
- SQLite readiness diagnostics: `apps/favn_storage_sqlite/test/sqlite_readiness_test.exs`
- Stopped-backend control-plane restore verification: `apps/favn_storage_sqlite/test/sqlite_control_plane_restore_test.exs`

Use when changing SQLite persistence, migrations, adapter lifecycle, readiness
diagnostics, or local SQLite storage semantics.
