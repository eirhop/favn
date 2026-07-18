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
- The registry stores compact manifest indexes in `manifest_versions`, immutable
  SQL execution artifacts in `execution_packages`, and their exact asset bindings
  in `manifest_execution_packages`. Publication uploads missing packages first;
  manifest registration rejects dangling or asset-mismatched references.
- `manifest_versions` stores non-null scalar catalogue counts and a bounded atom
  inventory, so runtime-state and run decoding do not repeatedly scan or transfer
  the full JSONB release.
- `run_plans` stores one bounded immutable plan per planned run. `runs.snapshot`
  stores only mutable state plus the plan hash, keeping every transition below its
  independent 4 MiB boundary.
- Immutable deployment-target rows include bounded, fingerprinted JSONB catalogue
  descriptors. Customer catalogue reads use those indexed rows and do not decode a
  full manifest.
- Reset-baseline migrations live under `migrations/` and create the dedicated
  `favn_control` schema.
- Mix tasks under `lib/mix/` own migration, workspace provisioning, runtime grants,
  restore verification, and bounded operational commands.
- Platform maintenance can remove old execution packages only when no manifest link
  references them; the global content-addressed registry cannot be purged by
  workspace.

The application does not define product lifecycle decisions. Those remain in
`favn_orchestrator`; this application implements transactional persistence and
database-enforced invariants.

## Runtime contract

- PostgreSQL major 18 is required.
- Runtime startup validates the exact schema and fails closed.
- Production requires verified TLS and a least-privilege runtime role.
- Runtime nodes never migrate at boot.
- Tenant access requires explicit workspace/platform context.
- The storage-owned node-local manifest cache contains only decoded compact immutable
  releases and is bounded by entry and byte budgets. The orchestrator and runner own
  their separate bounded compiled lookup caches. Execution packages are fetched by SHA-256
  primary key for one admitted runtime asset, with indexed joins proving its pinned
  workspace deployment and target authorization, and are not cached as manifest
  state.
- Runtime-input pins carry the exact execution-package hash and resolver identity,
  preventing a pin from replaying against changed SQL execution content.
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

- `docs/storage/postgresql/architecture.md`
- `docs/storage/postgresql/data-model.md`
- `docs/storage/postgresql/testing.md`
- `docs/architecture/postgresql-control-plane-storage-v2.md`
- `docs/adapters/storage-adapters.md`
- `docs/production/postgresql_operator_runbook.md`
