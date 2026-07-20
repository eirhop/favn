# Storage

Favn separates control-plane persistence from customer data-plane storage.

- `favn_orchestrator` owns persistence semantics and defines small capability
  contracts for domain commands and bounded queries.
- `favn_storage_postgres` implements those contracts for PostgreSQL 18.
- `favn_view`, public DSL code, and runners never access Ecto repos or schemas
  directly.
- Customer blobs, DuckLake metadata, warehouse tables, credentials, and SQL
  sessions are not stored in the Favn control-plane database.

The removed memory adapter and frozen SQLite prototype are not runtime
alternatives. Full development and integration testing use PostgreSQL so the
same constraints, transactions, query plans, and concurrency behavior are
exercised before production.

Production release status and the remaining non-storage gates are tracked in
[`../production/README.md`](../production/README.md).

## PostgreSQL documentation

- [Architecture and implementation](postgresql/architecture.md)
- [Data model and ER diagrams](postgresql/data-model.md)
- [Testing strategy](postgresql/testing.md)
- [Operator runbook](../production/postgresql_operator_runbook.md)
- [Detailed design decision record](../architecture/postgresql-control-plane-storage-v2.md)
- [Application ownership map](../structure/favn_storage_postgres.md)

## Contract rules

- Commands express atomic business changes, not generic CRUD.
- High-cardinality reads require finite limits and stable cursors.
- Workspace-owned operations require an explicit workspace context.
- Cross-workspace reads require an explicit platform context.
- Unknown write outcomes are resolved with the original command identity.
- Notifications and caches improve latency only; PostgreSQL remains authoritative.
