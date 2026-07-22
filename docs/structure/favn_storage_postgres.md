# favn_storage_postgres

Purpose: the production, development, and integration-test implementation of
Favn's PostgreSQL 18 control-plane persistence.

## Ownership

- `FavnStoragePostgres.Backend` composes the capability stores required by
  `FavnOrchestrator.Persistence.Backend`.
- `FavnStoragePostgres.BackendSupervisor` owns the repo, notification listener,
  publisher, projectors, and bounded maintenance workers.
- Capability stores live under `registry/`, `runs/`, `run_ownership/`,
  `scheduler/`, `admission/`, `target_generations/`, `materialization/`,
  `backfills/`, `identity/`,
  `resource_circuits/`, `logs/`, `operator_reads/`, and `maintenance/`.
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
- `asset_target_generations` owns immutable physical-generation identity and
  `asset_target_bindings` selects the active generation for ordinary writes and
  current-evidence reads. Initial writes remain `building` until authoritative
  physical reconciliation records an activation and fingerprint. Rebuild
  operations, actions, windows, and target locks have
  normalized, bounded authority tables for later lifecycle transitions.
- Materialization ledgers pin both the optional physical target generation and the
  required evidence generation. Window and freshness projections use that evidence
  generation in their primary identity.
- `asset_attempt_overviews` projects exact run, asset-step, and effective runtime
  window identities from authoritative run events. It supports bounded operator
  overview reads without loading run snapshots, plans, or event payloads and is
  repairable through projection maintenance.
- Immutable deployment-target rows include bounded, fingerprinted JSONB catalogue
  descriptors. Customer catalogue reads use those indexed rows and do not decode a
  full manifest.
- Reset-baseline migrations live under `migrations/` and create the dedicated
  `favn_control` schema.
- `FavnStoragePostgres.Release` owns migration, exact-schema verification,
  workspace provisioning, runtime grants, restore verification, key inventory,
  explicit key compaction, and upgrade preflight behavior. Mix tasks under
  `lib/mix/` are thin development wrappers.
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
- The production loader supplies a frozen Repo configuration. Runtime storage
  code does not select a backend or reread database/key environment variables.
  Verified TLS uses an explicit CA bundle when configured and otherwise uses the
  Erlang system trust store; plaintext is rejected in production.
- Runtime nodes never migrate at boot.
- `manifest_versions.required_runner_release_id` is non-null and format-checked
  for current manifest schemas. It is null only on historical pre-contract rows,
  which remain available to bounded audit reads but cannot be activated.
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
- `resource_circuits` stores workspace-scoped closed/open/half-open state and an
  exclusive expiring probe token. `resource_circuit_outcomes` makes terminal
  updates idempotent by run/node/attempt/resource. `resource_recovery_candidates`
  durably claims linked recovery work without changing the source run.

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
