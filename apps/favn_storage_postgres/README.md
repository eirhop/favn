# `apps/favn_storage_postgres`

Production, development, and integration-test implementation of Favn's
PostgreSQL 18 control-plane persistence.

## Ownership

- `FavnStoragePostgres.Backend` composes the seventeen capability stores required
  by `FavnOrchestrator.Persistence.Backend`.
- `FavnStoragePostgres.BackendSupervisor` owns the Ecto repo, notification
  listener, outbox sequencer, projectors, and bounded maintenance workers.
- Capability stores own registry, runs, run ownership, scheduling, admission,
  materialization, backfills, identity, logs, operator reads, and maintenance.
- Ecto schemas live under `schemas/`; concurrency-critical mutations use focused
  SQL where row locks, `SKIP LOCKED`, or database-time lease checks are required.
- The bootstrap workflow creates the dedicated migrator-owned `favn_control`
  schema before reset-baseline migrations run.
- `FavnStoragePostgres.Release` owns the composed database Jobs plus local/internal
  migration, schema/grant verification, workspace provisioning,
  key inventory/compaction, and restore verification.

The app implements transactional persistence and database invariants. Product
lifecycle decisions remain in `favn_orchestrator`.

## Runtime contract

- PostgreSQL major 18 is required.
- Runtime nodes validate the exact schema and never migrate at boot.
- Production requires verified TLS and a least-privilege runtime role.
- Authentication is explicit: password deployments use the existing URL;
  Azure deployments use managed identity through an independently supervised
  `favn_azure` cache for every new Repo and notification connection.
- Expected managed-identity failures fail closed through connection backoff.
  Existing healthy sessions remain open, and Favn never retries a database
  write whose outcome may be unknown.
- Every tenant operation carries an explicit workspace or platform context.
- PostgreSQL `NOTIFY` is only a wake-up optimization; durable outbox publications
  and cursors provide replay correctness.
- Manifest rows store a database-validated map from logical runner pool to exact
  runner release ID. The schema is reset-only and does not retain pre-contract
  manifest rows.

## Development commands

Set `FAVN_DATABASE_URL` to the restricted runtime connection and
`FAVN_DATABASE_MIGRATOR_URL` to the elevated development connection. The normal
schema update is one explicit command:

```bash
mix favn.postgres.upgrade
```

The individual development wrappers remain available for diagnosis and
maintenance:

```bash
mix favn.postgres.migrate
mix favn.postgres.verify_schema
mix favn.postgres.grant_runtime --role favn_runtime
mix favn.postgres.provision_workspace --id CUSTOMER --slug CUSTOMER --name "Customer"
mix favn.postgres.runtime_input_key_inventory
mix favn.postgres.compact_runtime_input_keys --version 1
mix favn.postgres.verify_restore
```

Production database lifecycle containers use the fixed wrapper and composed
workflow:

```bash
/app/bin/favn_control_plane_ops status
/app/bin/favn_control_plane_ops bootstrap
/app/bin/favn_control_plane_ops upgrade
```

The wrapper emits one redacted JSON result and stable exit code. See
[`docs/production/postgresql_bootstrap.md`](../../docs/production/postgresql_bootstrap.md)
for profiles, lifecycle, cleanup, and failure handling. Normal application
startup never invokes migration.

Key inventory and compaction parse `FAVN_RUNTIME_INPUT_PIN_KEYS` and
`FAVN_RUNTIME_INPUT_PIN_KEY_VERSION` directly in the one-off release process;
they never rely on a previously started control-plane application.

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
