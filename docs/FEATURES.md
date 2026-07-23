# Favn Features

This file records current capability and limits. Forward work belongs in
[`ROADMAP.md`](ROADMAP.md); production readiness is summarized in
[`production/README.md`](production/README.md).

Favn is private pre-v1 software. PostgreSQL 18 is the only control-plane database.

## Authoring and execution

- The `favn` package provides manifest-first asset, SQL-asset, pipeline, schedule,
  window, coverage, freshness, retry, settings, and runtime-input DSLs.
- Compilation produces a deterministic schema-12 manifest bound to the exact
  verified runner release, with graph metadata, compact catalogue/planning indexes,
  content-addressed SQL execution packages, environment-resolved timezones and
  coverage, provenance, and desired SQL target descriptors.
- SQL output contracts validate ordered columns/types, lineage, grain, uniqueness,
  nullability, and up to 16 ordered conditional row-count claims.
- Planning supports asset and pipeline targets, dependency selection, refresh
  modes, stages, retries, replay, and bounded admission. Scheduled window
  selections apply pipeline lookback once; manual and backfill selections stay
  exact, and runs persist requested, expansion, and effective anchors.
- Customer-built runners validate and advertise an operator-supplied immutable
  release ID together with the running Favn version, runner contract, Elixir,
  OTP, and target. Favn validates compatibility and exact manifest alignment but
  does not inspect customer source or dependency provenance. Runner work,
  inspection, results, and events carry the exact release ID in addition to the
  manifest and execution-package identity. The server discards
  events that do not exactly match stored work and replaces mismatched results
  with bounded errors. Ownership leases and fencing prevent stale executors
  from committing.
- The control plane reaches the runner only through a statically configured
  distributed-BEAM node. Connection, diagnostics, and RPC calls are bounded;
  readiness requires connected, ready diagnostics with a valid release ID.
- DuckDB and DuckDB ADBC support bounded queries, typed configuration, catalog
  requirements, session scripts, and runner-local exclusive sessions.
- DuckLake SQL tables support structured declarative physical partitioning with
  identity, year, month, day, hour, and bucket keys. The declared current spec
  is operator-visible; historical layout evolution remains DuckLake-owned, and
  a full rebuild is the explicit whole-table rewrite.

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
- Asset coverage is evaluated against bounded canonical expected windows and
  successful evidence from only the active semantic or physical generation.
  Catalogue/API reads distinguish complete, incomplete, and explicit unknown
  states; operators can review and manually submit an immutable exact-gap
  backfill plan, with stale selections rejected before mutation.
- Manifest activation inspects persisted SQL relations through the runner and
  records desired, active-generation, and physical compatibility per target.
  Incompatible, drifted, and ownership-unknown targets reject ordinary writes
  on affected dependency paths; compatible and unrelated paths remain runnable.
- Operators can plan, approve, inspect, cancel, safely retry, and reconcile
  immutable generation rebuilds. Rebuilds use isolated candidates, frozen work
  items, sorted target locks, fenced recovery, physical validation, marker-based
  activation reconciliation, topological downstream repair, and explicit cleanup.
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

- `mix favn.init`, `doctor`, `install`, `dev`, `run`, `backfill`, `rebuild`, `runs`, `logs`,
  `inspect`, `query`, `diagnostics`, `reload`, `status`, `stop`, and `reset` provide
  the private local developer loop against PostgreSQL.
- `mix favn.init` creates the non-overwriting documented local Compose and
  editable customer Dockerfile/release templates. Optional versioned native
  dependencies are explicit includes such as `duckdb-adbc@1.5.4`.
- Repository maintainers use `build.control_plane` for the deterministic,
  integrity-checked official image context and optional unpublished local
  candidate. Consumer projects can use `FAVN_CHECKOUT` with
  `mix favn.maintainer.dev` to exercise one coherent local framework checkout by
  exact local image ID. Pull-request CI qualifies unpublished candidates; an
  explicit workflow dispatch publishes only the exact current green `main`
  revision to GHCR, records immutable digest aliases, scans the image, and
  attaches provenance and SPDX SBOM attestations. Ordinary merges publish no
  image.
- `build.manifest --runner-release-id ID` binds content-addressed manifests to
  an explicit operator-owned runner identity. `publish` stages artifacts and
  `activate` selects one exact version for one workspace using a service token
  read only from the environment.
- `mix favn.install` resolves the version-matched prebuilt control plane to an
  immutable digest and keeps install state independent of deployment topology.
  The target-specific init forms remain available for comparison copies and
  single-host consumer templates. `mix favn.dev` applies explicit
  command/config/default Compose selection, automatically invokes the local
  customer Dockerfile unless an image is selected, validates versioned role
  labels, pins the inspected runner by local image ID, and runs PostgreSQL, the
  control plane, and runner with Compose `--no-build`.
- Local sample DuckDB paths are runtime references with host defaults below
  `.data` and container paths below `/var/lib/favn/data`. Stop and reset
  preserve the consumer Compose file, containers, networks, volumes, services,
  and data.

## Operator web UI

- Authenticated LiveView routes cover assets, pipelines, schedules, runs, rebuilds, logs,
  lineage, login/logout, and health through the public orchestrator facade.
- Workspace-scoped live updates reread durable state after notification.
- Asset and run detail distinguish requested anchors from exact effective runtime
  windows and use compact projections; event payloads load only on the Events view.
- The asset catalogue and detail page show persisted target compatibility apart
  from health, freshness, and coverage. Blocking states include a stable reason,
  bounded structured diff, active generation, and desired/physical fingerprints.
- Rebuild pages enforce plan/review/start separation, page bounded operation and
  item histories, show progress and unknown outcomes, and render only
  server-authorized cancellation, retry, and reconciliation actions.
- The UI remains a prototype: some asset-detail modes are placeholders, mutation
  audit outside the rebuild workflow is incomplete, actor/session/audit
  administration is absent, and there is
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
