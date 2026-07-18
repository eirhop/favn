# Favn Features

This file records implemented behavior. Forward-looking work belongs in
`docs/ROADMAP.md`; normative PostgreSQL decisions live in
`docs/architecture/postgresql-control-plane-storage-v2.md`.

Maturity labels:

- **solid**: covered at its owning boundary and intended to remain.
- **prototype**: implemented, but its public or operational contract may change.
- **internal**: runtime implementation, not a consumer API.

## Product Boundary

- `favn` is the public manifest-first DSL and Mix-task surface.
- `favn_core` owns shared domain, compiler, manifest, and contract types.
- `favn_runner` executes compiled work; SQL/data-plane integrations remain plugin-owned.
- `favn_orchestrator` owns control-plane use cases, scheduling, coordination, auth,
  private APIs, and the public backend facade used by `favn_view`.
- `favn_storage_postgres` is the only control-plane persistence backend.
- `favn_local` owns the project-local developer loop and build/bootstrap tooling.
- `favn_view` is a thin Phoenix/LiveView boundary and calls only the public
  orchestrator facade.

Favn is private pre-v1 software. Breaking legacy forms are removed rather than
deprecated when a cleaner contract is accepted.

## Authoring And Compilation

- Asset, multi-asset, SQL-asset, namespace, pipeline, schedule, window, freshness,
  retry, settings, and runtime-config DSLs are implemented. **State: solid.**
- Discovery and explicit module lists compile business code into a deterministic,
  versioned manifest with graph metadata and runner contract identity. **State: solid.**
- Manifest publication separates compact catalogue/planning indexes from immutable,
  content-addressed SQL execution packages. Large generated SQL stays in PostgreSQL
  and only the selected asset package is fetched at runtime. **State: solid.**
- Typed references, relation catalogs, resource requirements, and explicit
  connection modules cross the manifest/runtime boundary; arbitrary functions and
  generic config bags do not. **State: solid.**
- Runtime-input resolvers select immutable inputs before execution. Sensitive pins
  are encrypted with the configured runtime-input key and are never copied into
  generic metadata, events, logs, telemetry, or errors. **State: prototype.**

## Planning And Execution

- Asset and pipeline target planning supports dependency selection, refresh modes,
  windowed execution, stages, retries, replay input modes, and bounded execution
  admission. **State: prototype.**
- Runner work is pinned to a manifest version and explicit execution identity, with
  the selected SQL asset's verified execution package attached before preflight.
  Ownership and fencing tokens prevent a stale orchestrator from committing after
  losing a lease. **State: solid internal contract.**
- DuckDB and DuckDB ADBC adapters support bounded queries, typed configuration,
  required catalogs/resources, session scripts, and runner-local exclusive session
  pooling. **State: prototype.**
- Azure credential integration provides cached Azure CLI and managed-identity
  tokens without moving credential material into the control plane. **State: prototype.**

## Orchestration And Operations

- Run submit/read/cancel/retry, backfill planning/execution, schedules, logs,
  materialization claims, admission, manifest publication/deployment, lineage, and
  operator read models are implemented behind explicit workspace authority.
  **State: prototype.**
- Mutating HTTP commands use scoped idempotency keys. PostgreSQL atomically commits
  the request fingerprint, domain mutation, audit entry, outbox event, and replay
  result. Exact retries replay; conflicting input is rejected. **State: solid.**
- Password auth uses Argon2id. Actors, workspace memberships, credentials, opaque
  session-token hashes, revocation, access versions, and audit records are durable
  PostgreSQL state. Browser cookies contain the selected workspace and opaque token;
  browser-safe scopes exclude credential and token material. **State: prototype.**
- SSE replay reads durable publication/run cursors. PubSub and PostgreSQL `NOTIFY`
  are latency wake-ups only and never correctness authorities. **State: prototype.**
- Diagnostics and readiness are redacted. Runtime readiness requires a reachable
  database, the exact supported schema, critical constraints/indexes, and valid
  runtime-input encryption keys. **State: solid internal contract.**

## PostgreSQL Storage V2

- The former 97-callback adapter, memory backend, SQLite backend, and legacy
  PostgreSQL schema were removed. Capability-specific behaviours now group atomic
  commands and bounded queries by domain. **State: implemented.**
- The `favn_control` schema stores platform manifest releases and hard-separated
  workspace state. Customer access always carries an explicit workspace context;
  platform operations require a distinct platform context. **State: implemented.**
- Compact manifests reference immutable SHA-256 execution packages through normalized
  manifest/asset associations. Package-first publication, one-query missing-hash
  negotiation, primary-key runtime reads, and package-bound encrypted input pins
  avoid full-manifest SQL loading and unbounded package scans. **State: implemented.**
- Workspace deployments materialize an exact immutable target catalog containing
  common targets plus customer-specific grants and dependency closure. Customer
  reads expose only customer-visible targets. **State: implemented.**
- Authoritative writes use database constraints, optimistic versions, leases with
  fencing, `FOR UPDATE SKIP LOCKED`, advisory serialization where appropriate, a
  commit-safe ordered outbox, and idempotent projectors. **State: implemented.**
- High-growth reads are keyset-paginated and bounded. Operator screens read compact
  projections instead of scanning runs/events or issuing per-row follow-up queries.
  Performance-contract tests assert query counts and reviewed query plans.
  **State: implemented.**
- Runtime nodes validate but never apply migrations. Separate Mix tasks cover
  migration, workspace provisioning, least-privilege grants, restore verification,
  missing-row projection backfill, and bounded retention of disposable operational
  records. Online repair of corrupt current projection rows is explicitly deferred
  until shadow generations and atomic cutover exist.
  Canonical history and published outbox events remain indefinite until safe
  referential/SSE watermarks are implemented. **State: implemented.**
- Live-PostgreSQL tests cover authority, tenant isolation, idempotency, competing
  connections, fencing, claim concurrency, query plans, restore checks, and facade
  use cases. **State: implemented.**

See `docs/structure/favn_storage_postgres.md` for the code/data map and
`docs/production/postgresql_operator_runbook.md` for operations.

## Local Development And Packaging

- `mix favn.init`, `doctor`, `install`, `dev`, `run`, `backfill`, `runs`, `logs`,
  `inspect`, `query`, `diagnostics`, `reload`, `status`, `stop`, and `reset` form the
  local developer loop. **State: solid but private-dev.**
- Local development uses PostgreSQL and one explicit local workspace. Memory and
  SQLite are not supported storage modes. **State: implemented.**
- `mix favn.build.runner` builds project-local runner output. `build.web` and
  `build.orchestrator` remain metadata-oriented. `build.single` creates an
  operational, project-local, non-relocatable PostgreSQL backend launcher.
  **State: prototype.**
- `mix favn.bootstrap.single` publishes missing execution packages followed by the
  compact manifest index, logs into an explicit workspace, deploys the selected
  release, registers it with the runner, and verifies the workspace active manifest.
  **State: prototype.**

## Web Shell

- Phoenix/LiveView provides authenticated asset, lineage, pipeline, schedule, run,
  log, and readiness surfaces. It depends only on the public orchestrator facade.
  **State: prototype.**
- Run/log live topics are workspace-scoped. Cross-node PostgreSQL publication
  notifications trigger bounded durable refreshes; payload data is reauthorized and
  reread from PostgreSQL. **State: prototype.**

## Current Caveats

- The repository is still pre-v1; the package/release distribution model is not
  finalized.
- The single-node launcher is not a relocatable OTP release, and split web/
  orchestrator build outputs are not yet deployable releases.
- PostgreSQL is intentionally mandatory. A smaller SQLite developer adapter may be
  evaluated only after production operation proves a real need.
- Analytics data remains runner/plugin-owned: customer blob accounts, DuckLake
  metadata databases, warehouses, and key-vault credentials are not stored in the
  Favn control-plane database.

## Verification

The root fast, acceptance, and slow suites are explicit CI tiers. PostgreSQL tests
require `FAVN_DATABASE_URL`; CI starts an ephemeral PostgreSQL service rather than
using a shared cloud database. The slow PostgreSQL tier starts three independent
BEAM VMs and repository pools to verify database-authoritative partitioning,
failover, and fencing. Security advisories are checked with `mix hex.audit`.
