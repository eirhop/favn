# Favn Features

This file records current capability and limits. Forward work belongs in
[`ROADMAP.md`](ROADMAP.md); production readiness is summarized in
[`production/README.md`](production/README.md).

Favn is private pre-v1 software. PostgreSQL 18 is the only control-plane database.

## Authoring and execution

- The `favn` package provides manifest-first asset, SQL-asset, pipeline, schedule,
  window, coverage, freshness, retry, settings, and runtime-input DSLs.
- Compilation produces a deterministic schema-11 manifest bound to the exact
  verified runner release, with graph metadata, compact catalogue/planning indexes,
  content-addressed SQL execution packages, environment-resolved timezones and
  coverage, provenance, and desired SQL target descriptors.
- SQL output contracts validate ordered columns/types, lineage, grain, uniqueness,
  nullability, and up to 16 ordered conditional row-count claims.
- Planning supports asset and pipeline targets, dependency selection, refresh
  modes, windows, stages, retries, replay, and bounded admission.
- Packaged runners self-verify their baked descriptor against runtime target and
  versions, selected BEAM digests, and application version/lock fingerprints in
  packaged `.app` files before startup. Stamped application and configured
  plugin inventories must exactly match the descriptor; plugin callbacks may
  select only descriptor-fingerprinted applications and supervised child roots.
  Runner
  work, inspection, results, and events carry the exact verified release id in
  addition to the manifest and execution-package identity. The server discards
  events that do not exactly match stored work and replaces mismatched results
  with bounded errors. Ownership leases and fencing prevent stale executors
  from committing.
- The control plane reaches the runner only through a statically configured
  distributed-BEAM node. Connection, diagnostics, and RPC calls are bounded;
  readiness requires connected, ready diagnostics with a valid release ID.
- DuckDB and DuckDB ADBC support bounded queries, typed configuration, catalog
  requirements, session scripts, and runner-local exclusive sessions.

Authoring and manifest contracts are comparatively mature. Planning, execution,
runtime inputs, and SQL integrations remain pre-v1 and may change.

## Control plane

- `favn_orchestrator` owns manifests, deployments, runs, events, schedules, logs,
  backfills, admission, circuits, auth, audit, idempotency, and operator read models.
- View and Orchestrator run in one control-plane BEAM. One boot loader validates
  their environment together before supervision starts; PostgreSQL is the fixed
  production backend, deployment settings are runtime-only, forwarded headers are
  accepted only from private allowlisted proxies, and diagnostics redact secrets.
- Every customer operation carries explicit workspace authority. Platform actions
  use a separate authority boundary.
- Mutating HTTP commands atomically commit idempotency state, domain mutation,
  audit, outbox, and replay result.
- Password auth uses Argon2id. Actors, memberships, credential hashes, session-token
  hashes, revocation, access versions, and audit records are durable PostgreSQL data.
- SSE and cross-node notifications use durable cursors; PubSub and PostgreSQL
  `NOTIFY` are wake-ups, never correctness authorities.
- Resource circuits, recovery candidates, schedule occurrences, execution
  ownership, claims, leases, and fencing are durable coordination state.
- Both runtime BEAMs expose monotonic lifecycle state and reject new mutation or
  execution admission while draining. Readiness flips before bounded shutdown;
  admitted work may settle until the configured deadline, after which ordinary
  durable cancellation/result paths preserve honest recovery state.
- The production control plane is an immutable Linux amd64 OTP release containing
  only View, Orchestrator, PostgreSQL storage, Core, and runtime dependencies. It
  runs as non-root, supports a read-only root filesystem, and has fixed health and
  release-operation entrypoints.

These capabilities are implemented and tested, but several operator workflows and
high-volume asynchronous submission paths remain unfinished.

## PostgreSQL Storage V2

- `favn_storage_postgres` is the only persistence backend. The generic mega-adapter,
  memory backend, SQLite backend, and legacy PostgreSQL schema were removed.
- Capability-specific stores group atomic commands and bounded queries by domain.
- The `favn_control` schema separates platform manifests from workspace data and
  enforces exact schema, constraint, index, identifier, and payload requirements.
- High-growth reads use keyset pagination and bounded projections. Manifest runtime
  reads fetch compact indexes and selected immutable execution packages.
- Separate tasks own migration, runtime grants, workspace provisioning, restore
  verification, projection backfill, and bounded retention. Runtime nodes never
  migrate automatically.
- Live PostgreSQL suites cover tenancy, idempotency, concurrency, fencing, claims,
  query plans, restore mechanics, and multi-node database authority.

Implementation details live in [`storage/postgresql/`](storage/postgresql/); the
operator contract is [`production/postgresql_operator_runbook.md`](production/postgresql_operator_runbook.md).

## Local development and packaging

- `mix favn.init`, `doctor`, `install`, `dev`, `run`, `backfill`, `runs`, `logs`,
  `inspect`, `query`, `diagnostics`, `reload`, `status`, `stop`, and `reset` provide
  the private local developer loop against PostgreSQL.
- `build.runner` creates an immutable, relocatable customer runner OCI context
  keyed by the deterministic runner release ID; Favn does not push it.
- Repository maintainers use `build.control_plane` for the deterministic,
  integrity-checked official image context and optional unpublished local
  candidate. Consumer projects can use `FAVN_CHECKOUT` with
  `mix favn.maintainer.dev` to exercise one coherent local framework checkout by
  exact local image ID. Pull-request CI qualifies unpublished candidates; an
  explicit workflow dispatch publishes only the exact current green `main`
  revision to GHCR, records immutable digest aliases, scans the image, and
  attaches provenance and SPDX SBOM attestations. Ordinary merges publish no
  image.
- `build.manifest` allows manifest-only deployment only after exact runner
  fingerprint alignment. `publish` stages content-addressed artifacts and
  `activate` selects one exact version for one workspace using a service token
  read only from the environment.
- `mix favn.install` resolves the version-matched prebuilt control plane to an
  immutable digest and keeps install state independent of deployment topology.
  `mix favn.init --target compose` scaffolds non-overwriting local and
  single-host consumer templates. `mix favn.dev` applies explicit
  command/config/default Compose selection, validates versioned role labels and
  immutable images, and runs PostgreSQL, the control plane, and the
  customer-built runner without owning extra consumer services or resources.
- Customer runner contexts separate stable dependency inputs from mutable
  application/release inputs. BuildKit can reuse dependency compilation after
  ordinary customer-code edits while every executable change still produces a
  new immutable runner image; no live container receives copied source or BEAMs.
- Local sample DuckDB paths are runtime references with host defaults below
  `.favn/data` and container paths below `/var/lib/favn/data`. Stop and reset
  preserve the consumer Compose file, containers, networks, volumes, services,
  and data.

## Operator web UI

- Authenticated LiveView routes cover assets, pipelines, schedules, runs, logs,
  lineage, login/logout, and health through the public orchestrator facade.
- Workspace-scoped live updates reread durable state after notification.
- Asset and run detail distinguish requested anchors from exact effective runtime
  windows and use compact projections; event payloads load only on the Events view.
- The UI remains a prototype: some asset-detail modes are placeholders, mutation
  audit is incomplete, actor/session/audit administration is absent, and there is
  no production browser acceptance suite.

## Production limits

- The first supported topology is one control-plane node and one runner node on
  a trusted private network. Multi-node control-plane/runner scaling is deferred.
- Secrets are environment-only and rotate through an operator-controlled restart.
- PostgreSQL production-size restore, provider PITR, failover/load evidence,
  dashboards, and alert wiring remain release gates.
- PostgreSQL backup does not recover DuckDB files, DuckLake metadata, object
  storage, warehouses, source systems, or external secret stores.
- Scheduler occurrences are durable, but submission still runs synchronously in
  the scheduler tick. The general durable asynchronous submission queue is not
  implemented.
- SQL adapter-native cancellation and broader DuckDB/DuckLake failure-injection
  coverage remain incomplete.

CI runs fast, acceptance, and slow suites against PostgreSQL and enforces Hex and
dependency advisory audits. Documentation-link, ExDoc, and stale-document checks
are not yet automated CI gates.
