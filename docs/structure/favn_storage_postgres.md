# favn_storage_postgres

Purpose: the production, development, and integration-test implementation of
Favn's PostgreSQL 18 control-plane persistence.

## Ownership

- `FavnStoragePostgres.Backend` composes the capability stores required by
  `FavnOrchestrator.Persistence.Backend`.
- `FavnStoragePostgres.BackendSupervisor` owns the repo, notification listener,
  publisher, projectors, and bounded maintenance workers.
- Capability stores live under `registry/`, `runs/`, `run_ownership/`,
  `scheduler/`, `admission/`, `materialization/`, `backfills/`, `identity/`,
  `logs/`, `operator_reads/`, and `maintenance/`.
- Ecto schemas live under `schemas/`. Ordinary typed queries use Ecto;
  concurrency-critical commands may use focused SQL.
- Reset-baseline migrations live under `migrations/` and create the dedicated
  `favn_control` schema.
- Mix tasks under `lib/mix/` own migration, workspace provisioning, runtime grants,
  restore verification, and bounded operational commands.

The application does not define product lifecycle decisions. Those remain in
`favn_orchestrator`; this application implements transactional persistence and
database-enforced invariants.

## Runtime contract

- PostgreSQL major 18 is required.
- Runtime startup validates the exact schema and fails closed.
- Production requires verified TLS and a least-privilege runtime role.
- Runtime nodes never migrate at boot.
- Tenant access requires explicit workspace/platform context.
- PostgreSQL `NOTIFY` and local PubSub are wake-up optimizations only; durable
  publications and cursors provide replay correctness.

## Tests

`apps/favn_storage_postgres/test/storage_v2/` covers schema authority,
transactions, idempotency, fencing, concurrency, workspace isolation, bounded
reads, query plans, privileges, migrations, and restore behavior. Live PostgreSQL
is mandatory; tests do not substitute memory or SQLite behavior.

Use this app when changing PostgreSQL configuration, migrations, schemas, queries,
transactions, outbox/projectors, encryption-at-rest for runtime-input pins,
maintenance, or database operations.

See:

- `docs/architecture/postgresql-control-plane-storage-v2.md`
- `docs/adapters/storage-adapters.md`
- `docs/production/postgresql_operator_runbook.md`
