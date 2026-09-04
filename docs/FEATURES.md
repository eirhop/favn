# Favn Features

This file records current capability and limits. Forward work belongs in
[`ROADMAP.md`](ROADMAP.md); production readiness is summarized in
[`production/README.md`](production/README.md).

Favn is private pre-v1 software. PostgreSQL 18 is the only control-plane database.

## Authoring and execution

- The `favn` package provides manifest-first asset, SQL-asset, pipeline, schedule,
  window, coverage, freshness, retry, settings, and runtime-input DSLs.
- Compilation produces a deterministic schema-19 manifest whose user-defined
  runner pools are bound to exact verified runner releases and whose referenced
  execution-pool defaults are strictly validated, with graph metadata,
  compact catalogue/planning indexes, content-addressed SQL execution packages,
  environment-resolved timezones and coverage, provenance, and desired SQL
  target descriptors.
- Production manifest builds also produce one deterministic `.tar.gz` and
  SHA-256. Any authorized HTTPS client can upload it unchanged; users do not
  select compression or package batching and do not rebuild the control-plane
  image for a manifest-only release.
- SQL output contracts validate ordered columns/types, lineage, grain, uniqueness,
  nullability, and up to 16 ordered conditional row-count claims.
- Planning supports asset and pipeline targets, dependency selection, refresh
  modes, stages, retries, replay, and bounded admission. Scheduled window
  selections apply pipeline lookback once; a windowed manual run defaults to
  one latest complete availability-aware window; explicit manual and backfill
  selections stay exact; and runs persist requested, expansion, and effective
  anchors.
  Adjacent scheduled or operator-selected windows may execute as one range while
  coverage remains at the authored grain. Rebuilds combine by default and can
  explicitly activate an empty replacement for later backfill.
- Customer-built runners validate and advertise an operator-supplied logical
  pool and immutable release ID together with the running Favn version, runner
  protocol, Elixir, OTP, and target. Favn validates exact task alignment but
  does not inspect customer source or dependency provenance. Runner tasks,
  inspection, results, and events carry the exact pool/release and assignment
  identity. Atomic claims, leases, and fencing reject stale executors.
- Runners initiate distributed-BEAM connections to the control plane, register,
  and pull one compatible durable task at a time. Zero runners is a ready
  control-plane state. An authenticated numeric demand endpoint lets external
  infrastructure scale each exact pool/release partition from zero to N;
  elastic runners exit after their configured idle grace.
- DuckDB and DuckDB ADBC support bounded queries, typed configuration, catalog
  requirements, session scripts, and runner-local exclusive sessions.
- DuckDB/DuckLake SQL assets can atomically replace complete single- or
  composite-key groups selected by explicit incremental and full scope queries.
  The runner validates the staged scope and candidate before one checked
  delete-and-insert transaction; other adapters and direct interactive SQL
  operations reject this strategy explicitly.
- DuckLake SQL tables support structured declarative physical partitioning with
  identity, year, month, day, hour, and bucket keys. The declared current spec
  is operator-visible; historical layout evolution remains DuckLake-owned, and
  a full rebuild is the explicit whole-table rewrite.

Authoring and manifest contracts are comparatively mature. Planning, execution,
runtime inputs, and SQL integrations remain pre-v1 and may change.

## Control plane

- `favn_orchestrator` owns manifests, deployments, runs, events, schedules, logs,
  backfills, admission, circuits, auth, audit, idempotency, and operator read models.
- View and Orchestrator are separate OTP releases in one immutable image. View
  can scale `0..1`; Orchestrator stays at exactly one replica. Each validates
  only its role-owned environment; PostgreSQL is the fixed
  production backend, deployment settings are runtime-only, forwarded headers are
  accepted only from private allowlisted proxies, and diagnostics redact secrets.
- Every customer operation carries explicit workspace authority. Platform actions
  use a separate authority boundary.
- Mutating HTTP commands atomically commit idempotency state, domain mutation,
  audit, outbox, and replay result.
- Lifecycle CLI commands show persisted run target identities and bounded,
  redacted errors with stable codes and recovery guidance instead of dumping
  internal response payloads. Failed backfills can be inspected directly by
  backfill ID, including bounded failed-window admission reasons when no child
  run was created.
- Password auth uses Argon2id. Actors, memberships, credential hashes, session-token
  hashes, revocation, access versions, and audit records are durable PostgreSQL data.
- SSE and cross-node notifications use durable cursors; PubSub and PostgreSQL
  `NOTIFY` are wake-ups, never correctness authorities.
- Resource circuits, recovery candidates, schedule occurrences, execution
  ownership, claims, leases, and fencing are durable coordination state.
- Authored schedules are inactive when first published in every workspace.
  Operators can list, preview, activate, and deactivate them explicitly; enabling
  starts at the next due occurrence and disabling does not cancel accepted runs.
- Asset coverage is evaluated against bounded canonical expected windows and
  successful evidence from the durable non-persisted binding or only the active
  physical generation.
  Catalogue/API reads distinguish complete, incomplete, and explicit unknown
  states; operators can review and manually submit an immutable exact-gap
  backfill plan, with stale selections rejected before mutation. Gap plans
  default to one child run per window; operators may combine a contiguous
  selection into one child run, and that choice is frozen into the reviewed plan.
- Manifest activation inspects persisted SQL relations through the runner and
  records desired, active-generation, and physical compatibility per target.
  Inspection demand is durable before runner availability is checked, so an
  exact pool/release can cold-start from zero through the runner-demand endpoint.
  Incompatible, drifted, and ownership-unknown targets reject ordinary writes
  on affected dependency paths; compatible and unrelated paths remain runnable.
- The private manifest-deployment API authenticates, checks replay/conflict, and
  acquires distributed upload admission before reading the archive. It validates
  gzip/tar incrementally without mounted volumes, stores packages in bounded
  batches, atomically accepts a durable operation, and activates it through
  recoverable claims and a workspace-wide fence. Status distinguishes
  `accepted`, `activating`, `succeeded`, `needs_attention`, `failed`, and
  `unknown` outcomes.
- Manifest activation requires explicit approval for non-empty execution-pool
  defaults, stores the effective policy and provenance in the immutable workspace
  deployment, and atomically applies matching PostgreSQL capacity scopes.
  Existing operator overrides survive later manifest deployments; removed-pool
  overrides become inactive and do not silently revive.
- Operators can plan, approve, inspect, cancel, safely retry, and reconcile
  immutable generation rebuilds. Rebuilds use isolated candidates, frozen work
  items, sorted target locks, fenced recovery, physical validation, marker-based
  activation reconciliation, topological downstream repair, and explicit cleanup.
- Operators can recover an interrupted initial generation from an immutable,
  evidence-backed plan. Recovery requires the original Favn generation,
  successful materialization, historical descriptor, fresh physical fingerprint,
  and exact pre-existing table-bound marker; arbitrary, replaced, or unbound
  relations cannot be adopted.
- Orchestrator exposes monotonic lifecycle state and rejects new mutation or
  execution admission while draining. View shutdown is independent. Readiness
  flips before bounded Orchestrator shutdown;
  admitted work may settle until the configured deadline, after which ordinary
  durable cancellation/result paths preserve honest recovery state.
- The production control plane is one immutable Linux amd64 image containing
  separate View and Orchestrator releases plus PostgreSQL storage, Core, Azure
  credential support, and runtime dependencies. It
  runs as non-root, supports a read-only root filesystem, and has fixed health and
  release-operation entrypoints.

These capabilities are implemented and tested. A production-shaped HTTP
boundary qualification now covers route catalogue drift, baseline
authentication failures, proxy behavior, browser navigation, durable-state
fingerprints, and network isolation. The remaining issue #578 phases, manual
accessibility, and production-provider qualification are unfinished.

## PostgreSQL Storage V2

- `favn_storage_postgres` is the only persistence backend. The generic mega-adapter,
  memory backend, SQLite backend, and legacy PostgreSQL schema were removed.
- Capability-specific stores group atomic commands and bounded queries by domain.
- The `favn_control` schema separates platform manifests from workspace data and
  enforces exact schema, constraint, index, identifier, and payload requirements.
- High-growth reads use keyset pagination and bounded projections. Manifest runtime
  reads fetch compact indexes and selected immutable execution packages.
- One idempotent `bootstrap` Job owns Favn-specific role/database setup,
  migrations, grants, atomic initial workspace/administrator provisioning, and
  fresh runtime verification. Read-only `status` and least-privilege `upgrade` use the same
  immutable image. Runtime nodes never migrate automatically.
- PostgreSQL supports explicit password and Azure managed-identity modes. Azure
  mode obtains fresh-enough per-connection tokens through its own bounded cache,
  covers notifications and one-off release operations, suppresses ambient
  PostgreSQL password defaults, maps exact Entra object IDs through the Azure
  provider adapter, and preserves connection backoff on provider failure.
- The migrator owns only `favn_control` and runs with `NOCREATEROLE`; bootstrap
  administrator authority is isolated from the normal control plane and normal
  upgrades. Password deployments use the same common role policy without Azure.
- Live PostgreSQL suites cover tenancy, idempotency, concurrency, fencing, claims,
  query plans, restore mechanics, and multi-node database authority.

Implementation details live in [`storage/postgresql/`](storage/postgresql/); the
operator contract is [`production/postgresql_operator_runbook.md`](production/postgresql_operator_runbook.md).

## Local development and packaging

- `mix favn.dev`, `reload`, `stop`, and `doctor` provide a Docker-free source
  loop. View and Orchestrator run in the current BEAM and one child runner BEAM
  uses the consumer's compiled code.
- `mix favn.postgres.upgrade` explicitly applies migrations and runtime grants
  with the development migrator identity, then verifies the exact schema with
  the restricted runtime identity. Startup never invokes it.
- A second `iex -S mix` session can use the public `Favn` facade to submit,
  list, inspect, cancel, and diagnose runs through the same authenticated
  Orchestrator HTTP boundary without starting another runner.
- Developers supply PostgreSQL and load environment variables themselves.
  Startup verifies the schema and workspace but never starts, migrates,
  provisions, resets, or deletes PostgreSQL.
- `mix favn.init --target deployment` copies one non-overwriting,
  customer-owned Compose and runner-image example. PostgreSQL is external.
- Repository maintainers build `rel/control_plane/Dockerfile` directly from the
  repository root. CI validates and scans that image and publishes immutable
  commit digests with provenance and SBOM data.
- Repeated `build.manifest --runner-release POOL=ID` options bind every effective
  logical pool to an explicit operator-owned runner identity and emit a
  transport-ready archive. The private manifest-deployment API is the preferred
  production automation boundary; `publish` and `activate` remain available for
  interactive and local workflows.
- Images remain deployment artifacts: the reusable control plane is Favn-owned,
  while each runner image and its native dependencies are customer-owned.

## Operator web UI

- Authenticated LiveView routes cover assets, pipelines, schedules, runs, rebuilds, recovery, logs,
  lineage, login/logout, and health through the public orchestrator facade.
- Workspace-scoped live updates reread durable state after notification.
- Asset and run detail distinguish requested anchors from exact effective runtime
  windows and use compact projections; event payloads load only on the Events view.
- Run detail shows a failed run's stable error code and a bounded message; nested
  reasons and runner payloads stay out of the page.
- Asset detail has five working sub-pages: an overview, run history with per-run
  detail, a coverage calendar for a windowed asset, documentation, and diagnostics.
  Each loads only what it renders. One dialog submits a run, prefilled from the
  period the orchestrator reports the asset is due for.
- The asset catalogue and detail page show persisted target compatibility apart
  from health, freshness, and coverage. Diagnostics names the verdict and the fix in
  operator words; the stable reason, bounded structured diff, active generation, and
  desired/physical fingerprints stay reachable behind disclosures.
- Rebuild pages enforce plan/review/start separation, page bounded operation and
  item histories, show progress and unknown outcomes, and render only
  server-authorized cancellation, retry, and reconciliation actions.
- The UI remains a prototype: visual and manual accessibility qualification
  remains. Actor, session, audit, and credential administration are
  implemented. A Playwright/axe HTTP-boundary
  suite now checks anonymous and administrator navigation for every catalogued
  browser GET route, CSRF rejection for every browser mutation route, secure
  cookies, session revocation, HTTPS/proxy headers, and serious or critical
  automated WCAG 2.2 findings. The complete role/workspace and Entra browser
  matrix remains in #579.

## Production limits

- The first supported topology is one control-plane node, PostgreSQL, and zero
  to N runners across arbitrary user-defined pools on a trusted private network.
  Multi-control-plane availability is deferred.
- Secrets are environment-only and rotate through an operator-controlled restart.
- Azure Container Apps deployments can use single-tenant Microsoft Entra Easy
  Auth. Favn persists an exact tenant/object-ID-to-actor link and continues to
  own actor status, workspace membership, roles, opaque sessions, revocation,
  workspace switching, and redacted audit. Native OIDC, other proxy assertion
  formats, and provider group/role authorization remain out of scope.
- Azure Database for PostgreSQL can use separate user-assigned managed identities
  and PostgreSQL roles for runtime and migration, without a database URL/password
  secret. Operators own the Azure deployment and its live qualification.
- The local production-shaped HTTP-boundary phase catalogues every browser and
  private API route and fails on catalogue or required-evidence drift. It is not
  the complete #578 release verdict. Role/workspace isolation, abuse pressure,
  runner/PostgreSQL transport and authority, OCI/secret inspection, Azure
  ingress, managed Entra, WAF/rate-limiting policy, and manual accessibility
  still require their documented phase or target-environment evidence.
- PostgreSQL production-size restore, provider PITR, failover/load evidence,
  dashboards, and alert wiring remain release gates.
- PostgreSQL backup does not recover DuckDB files, DuckLake metadata, object
  storage, warehouses, source systems, or external secret stores.
- API, operator, scheduler, backfill, rebuild, recovery, and child-run requests
  use one durable asynchronous submission queue. Planning is bounded outside
  producer and scheduler processes; accepted intent survives a control-plane
  restart and scheduler occurrence completion is atomic with enqueue.
- Durable runner-task queues and elastic multi-runner execution are implemented.
  Pipeline tasks retain bounded task-local continuation data and reference one
  shared, fenced freshness checkpoint per run; immutable manifest
  definitions are reconstructed only by the orchestrator.
  Live managed-platform scale, restore/load, and cold-start evidence remain
  production release gates.
- The operator UI shows durable pre-admission run states and provides a Runners
  page focused on runner health: live presence, capacity per pool and release,
  and a durable runner session history recording when each runner registered,
  what it completed, and whether it shut down, crashed, or is presumed dead
  after a control-plane restart, with date and state filters, busy/idle window
  totals, and workspace-scoped failed-task detail per session.
- SQL adapter-native cancellation and broader DuckDB/DuckLake failure-injection
  coverage remain incomplete.

CI runs fast, acceptance, and slow suites against PostgreSQL and enforces Hex and
dependency advisory audits. Documentation-link, ExDoc, and stale-document checks
are not yet automated CI gates.
