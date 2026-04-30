# favn_storage_sqlite

Purpose: SQLite implementation of the orchestrator storage adapter, including
schema migrations and canonical payload persistence.

Code:
- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
- `apps/favn_storage_sqlite/lib/favn_storage_sqlite/`

Tests:
- `apps/favn_storage_sqlite/test/`

Use when changing SQLite persistence, migrations, adapter lifecycle, or local
SQLite storage semantics.
