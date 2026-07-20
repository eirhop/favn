# Application Structure

Use these pages to find ownership and test locations for an umbrella application.

- `favn.md`: public DSL surface
- `favn_core.md`: compiler, domain, and manifest logic
- `favn_runner.md`: execution runtime
- `favn_orchestrator.md`: control plane and persistence contracts
- `favn_storage_postgres.md`: PostgreSQL control-plane implementation
- `favn_local.md`: developer tooling and deployment builders
- `favn_view.md`: Phoenix/LiveView boundary
- `favn_sql_runtime.md`: SQL session and execution boundary
- `favn_duckdb.md` and `favn_duckdb_adbc.md`: DuckDB integrations
- `favn_test_support.md`: shared test fixtures and helpers

Current persistence implementation is documented under `docs/storage/postgresql/`;
the rationale and invariants are in
`docs/architecture/postgresql-control-plane-storage-v2.md`. SQLite storage was
removed during the Storage V2 reset and has no current application boundary.
