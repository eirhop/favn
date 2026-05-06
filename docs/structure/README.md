# Structure Docs

This directory maps code and tests by product area. Use these files only when
you need ownership or test-location guidance for a specific app.

- `favn.md`: public package facade and public Mix tasks
- `favn_authoring.md`: authoring DSL implementation and doc lookup internals
- `favn_core.md`: compiler, manifest, planning, windows, and shared contracts
- `favn_local.md`: local developer tooling and packaging implementation
- `favn_orchestrator.md`: control plane, HTTP API, runs, auth, storage contracts
- `favn_runner.md`: execution runtime and runner-owned inspection
- `favn_sql_runtime.md`: shared SQL connection/client/admission runtime
- `favn_duckdb.md`: DuckDB adapter and runner plugin runtime
- `favn_duckdb_adbc.md`: preferred DuckDB ADBC adapter and runner plugin runtime
- `favn_storage_sqlite.md`: SQLite storage adapter
- `favn_storage_postgres.md`: Postgres storage adapter
- `favn_test_support.md`: shared test fixtures and helpers
- `favn_web.md`: separate SvelteKit web/BFF workspace
