# Control-Plane Persistence

Reader: Favn contributors changing orchestrator persistence.

Documentation type: reference.

Favn has one production control-plane backend: PostgreSQL 18 in the
`favn_storage_postgres` application. It is internal infrastructure. User code and
`favn_view` call the public orchestrator facade and never call persistence modules.

This boundary does not own DuckDB/DuckLake data, warehouse data, SQL sessions,
customer blob storage, or secrets. Those remain runner/plugin/data-plane concerns.

## Contract shape

The old 97-callback mega-adapter has been removed. The orchestrator defines eleven
small capability contracts under `FavnOrchestrator.Persistence`:

- registry and immutable manifest/deployment catalog;
- runs, authoritative snapshots, events, and publication outbox;
- run ownership and fencing;
- scheduler claims and cursors;
- execution admission;
- materialization claims;
- backfills;
- identity, sessions, membership, and audit;
- logs;
- bounded operator reads;
- maintenance and projection lifecycle.

Contracts expose atomic domain commands and bounded queries, not table CRUD.
Commands return consistent typed results. Queries require an explicit workspace or
platform context and a finite limit/cursor where cardinality can grow.

`FavnOrchestrator.Persistence.Backend` is the composition boundary that supplies
these stores. `FavnStoragePostgres.Backend` is the production implementation; no
module implements all operations.

## Correctness rules

- Every tenant-owned row has a non-null workspace key. Related rows use composite
  workspace foreign keys where PostgreSQL can enforce ownership.
- Authoritative state and its outbox record commit in one transaction.
- Projectors consume sequenced publications and update repairable read models.
- Identity/sequence allocation is not treated as transaction commit order.
- Ownership, scheduling, admission, and materialization coordination use expiring
  claims and monotonically increasing fencing tokens.
- Reads are index-backed and bounded. Elixir-side full-table filtering and
  delimiter-encoded membership are forbidden.
- Cache correctness cannot depend on PubSub or invalidation. PostgreSQL authority
  and versioned persisted projections remain the source of truth.
- Unknown write outcomes are resolved with the original command/idempotency
  identity; callers must not manufacture a new identity.

## Runtime and migrations

Runtime nodes own one Ecto repo and validate the exact `favn_control` schema at
startup. They never migrate automatically. A separately authorized migration job
runs `mix favn.postgres.migrate`, then grants the least-privilege runtime role.

Development and CI use live PostgreSQL too. There is no memory or SQLite fallback.
See `docs/production/postgresql_operator_runbook.md` for setup, TLS, roles,
connection budgets, backups, restore drills, retention, monitoring, and incidents.

## Testing expectations

- Pure domain behavior: deterministic unit tests without a persistence backend.
- Store behavior: PostgreSQL integration tests using SQL sandbox isolation where
  possible.
- Coordination: separate connections/processes and real competing transactions.
- Scale: bounded query-count assertions plus `EXPLAIN` plan contracts at realistic
  cardinality.
- Operations: migration, runtime privilege, restore, missing-row projection backfill, and
  multi-node/failure-injection coverage.
- Tenancy: positive and negative cross-workspace tests for every public read and
  mutation path.

## Related documents

- `docs/architecture/postgresql-control-plane-storage-v2.md`
- `docs/structure/favn_storage_postgres.md`
- `docs/production/postgresql_operator_runbook.md`
- `docs/report/storage_architecture_data_model_quality_audit.md`
