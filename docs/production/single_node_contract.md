# Single-Node Production Contract

This document is the source of truth for Favn's first production deployment
target. It resolves issue #245 and gives follow-up production issues a concrete
contract to implement against.

The contract is intentionally narrow: Favn v1 production targets one backend
node first, with SQLite control-plane persistence on durable attached storage
and production-grade DuckDB execution. Postgres and distributed execution are
follow-up production modes, not part of the first v1 single-node contract.

The package and user-facing API support boundary for this runtime target is
documented separately in `docs/production/public_api_boundary.md`.

## Current Implementation Status

This is an architecture and documentation contract. The current
`mix favn.build.single` output includes a verified project-local backend-only
SQLite launcher with generated `bin/start` and `bin/stop` scripts. It depends on
the recorded installed runtime source root rather than a self-contained or
relocatable runtime closure. Full release packaging and backup automation remain
follow-up implementation work. `favn_web` now has an explicit Node production
service path documented in
`docs/production/web_service.md`, but it is not yet bundled into
`mix favn.build.single`.

Phase 1 runtime config validation, backend control-plane bootstrap, live
single-node first-run verification, explicit web service startup/readiness, and
SQLite-backed auth/session/audit persistence have landed, while full operator
runbooks and production web hardening remain follow-up implementation work.

Follow-up issues must treat this document as the product contract they are
making real.

Phase 1 bootstrap is intentionally backend/control-plane scoped. `mix
favn.bootstrap.single` verifies orchestrator service-token auth through
`/api/orchestrator/v1/bootstrap/service-token`, reads and verifies manifest JSON,
registers the manifest, activates it by default, and asks the orchestrator to
register the persisted manifest with the local runner through
`/api/orchestrator/v1/manifests/:manifest_version_id/runner/register`. It
verifies active-manifest selection through the service-auth-only
`/api/orchestrator/v1/bootstrap/active-manifest` endpoint. The implemented SQLite
acceptance verification covers manifest persistence, active-manifest selection,
scheduler state, runner registration, and restart survival. Focused storage
coverage verifies SQLite restart survival for actors, credential hashes,
session-token hashes, revocations, and redacted audit entries. Live generated
artifact coverage verifies first-admin login, manifest-pinned run submission,
run history, auth/session state, diagnostics, and restart survival against fresh
SQLite storage.

## Supported V1 Topology

The supported v1 production topology is one backend node:

- The backend node runs the orchestrator, runner, and scheduler.
- The orchestrator owns the control plane, private HTTP API, manifests, runs,
  events, scheduler state, auth/audit control-plane data, and storage adapter.
- The runner executes manifest-pinned work and owns SQL asset execution,
  runner plugins, cancellation, timeouts, crash reporting, and safe relation
  inspection.
- The scheduler runs once on the backend node and submits scheduled work through
  the orchestrator boundary.
- Control-plane persistence uses SQLite on durable block storage attached to
  that one backend node.
- DuckDB-backed execution is in scope for v1 production and must be hardened as
  part of the production promise.
- The web service may be co-located on the backend host or deployed separately,
  but it must call the documented orchestrator API boundary and must not bypass
  the backend through direct storage or BEAM access.

The first production target is not a split runner pool. There is exactly one
production backend node writing the SQLite control-plane database.

## Process Model

The production backend may be implemented as one release with supervised
components or as tightly managed local services inside one production artifact,
but the externally supported model is one backend node. Implementation work must
preserve these boundaries:

- `favn_orchestrator` is the system of record and the only control-plane writer.
- `favn_runner` is the execution runtime. The orchestrator submits work through
  `Favn.Contracts.RunnerClient` rather than discovering asset code or running
  work directly.
- `favn_duckdb` is a runner plugin and SQL adapter, not a control-plane storage
  layer.
- `favn_storage_sqlite` is the first production control-plane storage adapter.
- `favn_local` remains local tooling and packaging implementation. Production
  runtime behavior must not depend on local-dev-only assumptions such as loopback
  dev service wrappers, trusted local-dev context, or `FAVN_DEV_*` names.

The scheduler is part of the backend control plane. It must not run in multiple
active instances against the same SQLite database.

## Web Deployment Model

`favn_web` may be deployed in either supported placement:

- Co-located with the backend on the same host.
- Deployed as a separate web service that reaches the backend over the private
  orchestrator HTTP API.

In both placements, web must use the documented API boundary:

- Web calls the orchestrator HTTP API, currently under `/api/orchestrator/v1`.
- Web authenticates service-to-service calls with a configured service token.
- Web forwards actor/session context through documented headers only after the
  orchestrator has authenticated the browser session.
- Web must not connect to the SQLite database, mutate control-plane files, call
  runner internals directly, or rely on local-dev Distributed Erlang plumbing.

Public browser exposure belongs to `favn_web`. The orchestrator API is private
backend infrastructure and should be reachable only from trusted web/backend
network paths in production.

## Storage Contract

The first v1 production storage mode is SQLite for control-plane persistence.

SQLite stores the control-plane state needed to operate the backend, including:

- Manifest versions and active-manifest selection.
- Run snapshots and run events.
- Scheduler cursors/state.
- Operational backfill read models.
- Durable auth actors, credential hashes, session-token hashes, revocation
  timestamps, and redacted audit entries.
- Command idempotency records with hashed keys, request fingerprints, normalized
  redacted responses, status, resource identity, and retention timestamps.

In-memory storage is not a production mode. Postgres support exists as adapter
work today, but Postgres production mode is planned after SQLite single-node
production works and is not part of the first v1 single-node contract.

## SQLite Attached-Storage Requirements

SQLite is supported only on durable storage attached to one backend node.

Supported:

- A local disk or durable block volume attached to the one backend node.
- One backend process model with one active writer topology.
- A configured absolute database path, with parent directories created and
  writable before backend startup.
- Backups taken with SQLite-safe online backup or quiesced filesystem snapshot
  procedures that preserve database consistency.

Unsupported:

- Multiple backend nodes writing to the same SQLite database.
- Shared network filesystems such as NFS, SMB, or distributed POSIX-like mounts
  for the SQLite control-plane database.
- Object-storage mounts such as S3/GCS/FUSE-backed paths for the SQLite database.
- Active/passive or active/active HA orchestrators against one SQLite file.
- Manually editing SQLite rows outside Favn migrations or documented recovery
  procedures.

Production implementations must fail fast or report clear readiness failures
when the configured SQLite path is missing, unwritable, on an unsupported class
of storage that can be detected safely, or not schema-ready.

## DuckDB Production Contract

DuckDB is part of the v1 production promise. It is runner/plugin-owned
data-plane infrastructure, not control-plane persistence. `favn_duckdb` is the
supported `duckdbex`-backed plugin for bundled local/in-memory DuckDB execution.
`favn_duckdb_adbc` is the supported ADBC-backed plugin for deployments that need
explicit DuckDB shared-library/driver control.

Production DuckDB behavior covers or must preserve:

- Manifest-pinned SQL asset execution through the runner-owned materialization
  planner and shared SQL runtime client.
- Local-file DuckDB database paths on durable attached storage when local files
  are used; production paths must be explicit and production-safe rather than
  implicit local-dev state. Production connection schemas can include DuckDB's
  production storage fields to reject memory databases, relative paths, missing
  parent directories, and unwritable parent directories before opening DuckDB.
- Conservative admission/concurrency behavior for local-file DuckDB connections;
  local files default to single-admitted SQL sessions against the same database
  path unless the connection explicitly configures another safe policy. Admission
  timeouts are retryable structured SQL errors with the blocked scope and timeout.
- Bounded normal query results returned to Elixir by row count and converted
  result byte size. Large outputs must be written by explicit DuckDB SQL such as
  `COPY (...) TO '/path/file.parquet' (FORMAT parquet)` rather than hidden
  adapter-created result files; command statements such as `COPY` use
  `Favn.SQLClient.execute/3`, not the bounded `query/3` row-return path.
- Production diagnostics for the ADBC path must load the configured
  driver, connect, run bootstrap, ping DuckDB, and report the actual DuckDB
  version with driver paths and secrets redacted.
- Separate-process DuckDB execution as the recommended production placement when
  using the implemented DuckDB plugin modes, so DuckDB handles live in a
  supervised worker process instead of the asset worker process.
- Bootstrap extension install/load accepts any valid DuckDB extension identifier;
  malformed identifiers are rejected during bootstrap config validation.
- Clear diagnostics for connection, bootstrap, extension, materialization,
  appender, cancellation, timeout, and crash failures, with secret values
  redacted from logs, API/UI payloads, and structured error details. Separate
  process worker unavailability is reported as retryable, while worker-call
  timeouts are reported as unknown-outcome failures because the DuckDB operation
  may still be running in the worker.
- Documented backup/restore expectations for local DuckDB files or external
  DuckLake-style storage used by named connections.
- No general production SQL editor as part of the v1 promise. Relation
  inspection remains curated and runner-owned.

DuckDB data-plane storage is separate from SQLite control-plane persistence. A
production deployment may use SQLite for Favn control-plane state and DuckDB for
asset data at the same time. SQLite control-plane backup does not include local
DuckDB database files, DuckLake data paths, object storage, or external source
systems; operators must back up and restore those data-plane systems separately.

## Scheduler Behavior

The v1 scheduler contract is single-backend scheduling:

- One scheduler runtime is active for the backend node.
- The scheduler reads the active manifest from orchestrator storage.
- Schedule state is persisted through the configured control-plane storage.
- Scheduled runs are submitted through the same orchestrator run-submission path
  as operator-triggered runs.
- Cron schedules, missed-run policies, overlap policies, and scheduled window
  policies must follow the implemented manifest/runtime semantics.
- Scheduler state must survive backend restarts when SQLite storage is used.

The v1 contract does not include distributed scheduler leadership, cross-node
deduplication, HA failover, or multi-node catch-up coordination.

## Runner/Orchestrator Boundary

The orchestrator/runner boundary is required even when both components run on
one backend node:

- Orchestrator owns manifests, run lifecycle, API commands, state persistence,
  scheduler state, and audit/control-plane concerns.
- Runner owns execution, manifest registration for execution, context building,
  plugin loading, SQL materialization planning, cancellation, timeout handling,
  crash reporting, and relation inspection.
- Orchestrator dispatches work through the runner client contract and records
  normalized outcomes.
- Runner execution must be manifest-pinned and must not depend on ad hoc
  orchestrator-side module discovery.

The v1 single-node contract may use local calls or same-host RPC internally, but
implementation must not require remote runner nodes or a multi-node runner pool.

## Runtime Config, Env Vars, And Secrets

Production implementation issues must define and validate runtime configuration
using production names, not local-dev-only `FAVN_DEV_*` names.

At minimum, the production single-node runtime needs:

- `FAVN_STORAGE=sqlite` for the first production mode.
- `FAVN_SQLITE_PATH` pointing to the durable attached-storage SQLite database;
  it must be absolute.
- `FAVN_SQLITE_MIGRATION_MODE` as `manual` or `auto`, defaulting to `manual`.
- `FAVN_SQLITE_BUSY_TIMEOUT_MS`, defaulting to `5000`, as a positive integer.
- `FAVN_SQLITE_POOL_SIZE`, defaulting to `1`; Phase 1 accepts only `1`.
- `FAVN_ORCHESTRATOR_API_BIND_HOST`, defaulting to `127.0.0.1`, as an IPv4
  address.
- `FAVN_ORCHESTRATOR_API_PORT`, defaulting to `4101`, in `1..65535`.
- `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS`, required as comma-separated
  `service_identity:token` entries. Each service identity must be nonblank and
  unique; each token must be at least 32 characters and not contain weak
  placeholder fragments such as `replace`, `change`, `placeholder`, `example`,
  `secret`, `password`, `test`, `token`, or `todo`.
- `FAVN_ORCHESTRATOR_AUTH_SESSION_TTL`, defaulting to `43200`, as a positive
  integer absolute session TTL in seconds.
- `FAVN_SCHEDULER_ENABLED`, defaulting to `true`, as a boolean.
- `FAVN_SCHEDULER_TICK_MS`, defaulting to `15000`, minimum `100`.
- `FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES`, defaulting to `1000`, as a
  positive integer.
- `FAVN_RUNNER_MODE`, defaulting to `local`; Phase 1 accepts only the local
  single-node runner mode.
- `FAVN_WEB_ORCHESTRATOR_BASE_URL`, required by `favn_web`, as an absolute
  `http://` or `https://` URL without embedded credentials.
- `FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN`, required by `favn_web`, at least 32
  characters, for web-to-orchestrator service auth.
- `FAVN_WEB_PUBLIC_ORIGIN`, required by `favn_web`, as the exact browser-facing
  origin for unsafe request `Origin`/`Referer` validation. Production web origins
  must use `https://` except for local-only `localhost`, `127.0.0.1`, or `::1`
  smoke-test origins.
- `FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN`, required by first-run bootstrap
  tooling unless `--service-token` is passed, at least 32 characters, and present
  as the token value of one `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` entry.
- Durable first-admin/browser-login setup, durable sessions, actors,
  credentials, and audit logs. Password credentials are stored as encoded
  Argon2id hash strings, accepted passwords must be 15 to 1,024 characters,
  session revocation is durable, and audit records include stable service
  identities for service-authenticated requests.
- Runtime config values required by authored assets and named SQL connections,
  expressed through manifest-safe refs such as `env!/1` and `secret_env!/1`.
- DuckDB connection paths, bootstrap secrets, extension settings, and external
  storage credentials required by named DuckDB connections.

Production code must redact secrets in logs, diagnostics, persisted manifests,
and API/UI payloads. Missing required runtime config must produce clear errors;
SQL connection config preflight before planned runs start is a follow-up
production requirement.

## Persistence Paths

The production path contract is intentionally explicit:

- `FAVN_SQLITE_PATH` is the control-plane SQLite database path and must live on
  durable attached storage.
- DuckDB local database paths in named connections must live on durable storage
  when those connections contain production asset data. Production DuckDB paths
  must be explicit connection config, not inferred from local-dev state.
- Runtime logs, crash dumps, and diagnostics must have documented paths or
  stdout/stderr behavior suitable for the chosen release packaging.
- Build metadata paths under `.favn/dist` are not production persistence paths.
- Local development state such as `.favn/runtime.json` and `FAVN_DEV_*` env values
  are not production contracts.

Ownership is split by app boundary. `favn_orchestrator` validates and applies
orchestrator API, service-token, SQLite storage, scheduler, and local-runner
client config before supervised runtime traffic starts. `favn_runner` validates
runner mode before runner supervision starts. `favn_web` validates web-to-
orchestrator URL/token and public web origin before handling production web
requests. Local-dev-only `FAVN_DEV_*` names are not accepted by this production
contract.

Postgres production config validation is explicitly deferred to the later
Postgres production-mode issue. `FAVN_STORAGE=postgres` is not a valid first
single-node production runtime mode.

## Backup And Restore Expectations

SQLite-first production requires backup, migration, and restore guarantees for
SQLite before Postgres production mode is introduced.

Minimum expectations for the SQLite phase:

- Document how operators take consistent backups of the SQLite control-plane
  database.
- Document whether the backend must be stopped, paused, or can remain online
  during backup.
- Document how to restore the SQLite database onto a fresh backend node with the
  same release version.
- Document how migrations interact with backup and rollback expectations.
- Document what state is not covered by the control-plane backup, especially
  DuckDB asset data, external source systems, logs, and object/external storage.
- Document separate backup and restore procedures for runner/plugin-owned DuckDB
  data-plane files or external DuckLake-style storage used by named connections.
- Verify restore with realistic manifests, runs, scheduler state, auth/audit
  state after durable auth lands, and operational backfill state.

Postgres backup/restore is a follow-up production-mode concern and should not be
used to block SQLite-first v1 readiness.

## Migration And Schema Readiness

Production startup must have explicit schema-readiness behavior:

- SQLite migrations must be deterministic and versioned.
- Production startup must either run approved migrations automatically or fail
  with a clear `schema_not_ready` style error and documented operator action.
- Readiness checks must confirm the SQLite schema required by the running
  release is present before serving production traffic.
- Migration failures must not leave the backend silently serving with a partial
  or incompatible control-plane schema.
- Pre-closeout BEAM-term SQL payload rows are not upgrade-compatible and remain
  unsupported; persisted state must be reset, recreated, or migrated only through
  a documented production migration path.

The SQLite adapter has schema-readiness diagnostics that distinguish empty,
ready, missing, upgrade-required, newer-than-release, and inconsistent schemas,
and it has stopped-backend restore verification coverage for current
control-plane state. The orchestrator exposes those diagnostics through
readiness and the service-authenticated operator diagnostics endpoint; migration
commands and full backup/restore runbooks remain separate production-hardening
work.

## Health, Readiness, And Diagnostics

The production runtime must expose enough health, readiness, and diagnostics for
operators to trust the single-node deployment.

Required expectations:

- Liveness indicates that the process is running.
- Readiness fails when the orchestrator API cannot serve, SQLite is unavailable
  or not schema-ready, required secrets/config are missing, the scheduler cannot
  load required state, or the runner boundary is unavailable.
- Diagnostics identify active manifest, storage mode, SQLite path readiness,
  scheduler status, runner availability, recent run failures, and migration
  readiness without leaking secrets.
- Web readiness must include its ability to reach the orchestrator API through
  the configured service boundary.
- DuckDB diagnostics must identify connection/bootstrap/materialization failures,
  bootstrap step/kind, unsupported allow-list values, and adapter details where
  safe, without exposing secret values.

The orchestrator exposes unauthenticated `/api/orchestrator/v1/health/live` and
`/api/orchestrator/v1/health/ready` endpoints. Liveness returns `200` when the
process can serve the route. Readiness returns `200` only when all aggregated
checks pass and `503` with redacted check diagnostics otherwise. Readiness
delegates storage checks through the storage adapter boundary and isolates check
raises/exits/throws so degraded dependencies produce structured `503`
diagnostics rather than unhandled `500` responses. Detailed diagnostics are
available through service-authenticated `GET /api/orchestrator/v1/diagnostics`
and the local `mix favn.diagnostics` wrapper; the detailed report includes
active manifest, storage/schema readiness, scheduler, runner, redacted
data-plane connection summaries, in-flight runs, and recent failed-run
summaries.

The web service protects `/api/web/v1/health/live` and
`/api/web/v1/health/ready` with the same hook-level web-session gate as other
BFF routes. Web liveness is process-only and does not call the orchestrator. Web
readiness verifies web config and calls orchestrator readiness through the
configured API boundary with a bounded timeout, returning `503` with redacted
diagnostics when the web config is invalid, the orchestrator is unreachable,
times out, or reports not-ready. Unauthenticated health requests return the same
minimal JSON `401` envelope as other protected BFF routes.

## Explicitly Unsupported In V1

The first v1 single-node production contract explicitly does not support:

- Distributed execution.
- Multi-node runner pools.
- HA orchestration.
- Multiple backend nodes writing to the same SQLite database.
- Shared SQLite on network filesystems or object-storage mounts.
- Postgres as the first production control-plane mode.
- Split control-plane writes across orchestrator instances.
- Scheduler leadership election or cross-node scheduler coordination.
- Production deployment that depends on local-dev `mix favn.dev` service wrappers.
- Release packaging, backup scripts, auth/session persistence, web hardening, or
  full operator runbooks being considered complete by this document alone.

These are unsupported unless a later production contract explicitly supersedes
this document.

## Follow-Up Production Modes

Production modes must land in this order:

- First: SQLite single-node production on durable attached storage.
- Next: Postgres production mode after SQLite single-node works and has clear
  backup, migration, restore, readiness, and verification guarantees.
- Later: distributed/multi-node execution after Postgres support is production
  ready.

Distributed execution should build on the Postgres production mode, not on shared
SQLite.

## How Follow-Up Issues Use This Contract

Follow-up production issues should cite this document and implement one bounded
slice of the contract without reopening the core topology decision.

- Release/build issues implement the one-backend-node topology.
- Runtime config issues define production env vars and startup validation for the
  paths and secrets named here.
- Storage issues implement SQLite-first migration, backup, restore, and readiness
  guarantees.
- DuckDB issues harden DuckDB as part of the production promise.
- Web issues preserve the web-to-orchestrator API boundary for co-located and
  separate deployments.
- Observability issues expose health/readiness/diagnostics for this topology.
- Postgres issues should be scoped as the next production mode.
- Distributed runner/orchestration issues should move after Postgres production
  mode unless explicitly limited to design notes.

Issue #262 should be updated after this contract is merged so its phase order
matches SQLite first, DuckDB hardening as required v1 work, Postgres next, and
distributed execution later.
