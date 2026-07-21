# `apps/favn_storage_postgres`

Production, development, and integration-test implementation of Favn's
PostgreSQL 18 control-plane persistence.

## Ownership

- `FavnStoragePostgres.Backend` composes the eleven capability stores required
  by `FavnOrchestrator.Persistence.Backend`.
- `FavnStoragePostgres.BackendSupervisor` owns the Ecto repo, notification
  listener, outbox sequencer, projectors, and bounded maintenance workers.
- Capability stores own registry, runs, run ownership, scheduling, admission,
  materialization, backfills, identity, logs, operator reads, and maintenance.
- Ecto schemas live under `schemas/`; concurrency-critical mutations use focused
  SQL where row locks, `SKIP LOCKED`, or database-time lease checks are required.
- Reset-baseline migrations create the dedicated `favn_control` schema.
- `FavnStoragePostgres.Release` owns release-safe migration, schema/grant
  verification, workspace provisioning, key inventory/compaction, restore
  verification, and upgrade preflight behavior. Mix tasks are development
  wrappers around that module.

The app implements transactional persistence and database invariants. Product
lifecycle decisions remain in `favn_orchestrator`.

## Runtime contract

- PostgreSQL major 18 is required.
- Runtime nodes validate the exact schema and never migrate at boot.
- Production requires verified TLS and a least-privilege runtime role.
- Every tenant operation carries an explicit workspace or platform context.
- PostgreSQL `NOTIFY` is only a wake-up optimization; durable outbox publications
  and cursors provide replay correctness.
- Current manifest rows store a database-validated `required_runner_release_id`.
  Historical pre-contract rows retain `NULL` for audit display and cannot be
  activated.

## Development commands

```bash
mix favn.postgres.migrate
mix favn.postgres.verify_schema
mix favn.postgres.grant_runtime --role favn_runtime
mix favn.postgres.provision_workspace --id CUSTOMER --slug CUSTOMER --name "Customer"
mix favn.postgres.preflight_upgrade
mix favn.postgres.runtime_input_key_inventory
mix favn.postgres.compact_runtime_input_keys --version 1
mix favn.postgres.verify_restore
```

Production one-off containers call the same operations without Mix, for example:

```bash
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.migrate())'
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.verify_schema())'
```

All release functions return `{:ok, %{status: :ok, ...}}` or a redacted
`{:error, %{status: :error, code: ...}}`. Migration and grant operations reject
the configured restricted runtime role.

Use `scripts/postgres/setup` for the local container. See the production runbook
before configuring TLS, roles, connection budgets, backups, restore drills,
retention, or monitoring.

## Tests

`test/storage_v2/` covers schema authority, transactions, idempotency, fencing,
concurrency, workspace isolation, bounded reads, query plans, privileges,
migrations, and restore behavior against live PostgreSQL.

Related documentation:

- `docs/architecture/postgresql-control-plane-storage-v2.md`
- `docs/adapters/storage-adapters.md`
- `docs/production/postgresql_operator_runbook.md`
