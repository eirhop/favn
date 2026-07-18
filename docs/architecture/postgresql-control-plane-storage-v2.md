# PostgreSQL Control-Plane Storage V2 Architecture

Status: implemented baseline and normative evolution guide
Accepted direction: PostgreSQL first; SQLite deferred; consultant-operated customer workspaces
Drafted: 2026-07-16
Reader: Favn contributors implementing or reviewing control-plane persistence
Documentation type: normative architecture and implementation guide

For a shorter guide to the implemented code, use
`docs/storage/postgresql/architecture.md`. The table catalog and Mermaid ER
diagrams are in `docs/storage/postgresql/data-model.md`.

## Purpose

This document defines the implemented PostgreSQL Storage V2 baseline and the
remaining gates before Favn can be considered production-ready. It is the source
of truth for:
module ownership, persistence contracts, data model, transactions, multi-node
coordination, projections, caching, migrations, operations, tests, and production
acceptance.

The capability contracts, PostgreSQL schema, and workspace model described here
are implemented. The removed `Favn.Storage.Adapter` behaviour, memory backend,
SQLite backend, and legacy PostgreSQL adapter remain historical migration inputs
only and must not be restored for compatibility.

The key words **must**, **must not**, **should**, and **may** are normative.

The evidence baseline is
`docs/report/storage_architecture_data_model_quality_audit.md`, including its
concurrency races, approximately `O(N^2)` execution-group rebuild path, unbounded
detail reads, incomplete PostgreSQL feature coverage, and adapter-duplication
findings.

## Decision summary

1. PostgreSQL is the only initial Storage V2 backend for production, development,
   tests, and CI.
2. PostgreSQL 18, kept on the latest available minor release, is the reference
   engine. The design requires no optional PostgreSQL extensions.
3. Favn owns one Ecto repository and one dedicated PostgreSQL schema named
   `favn_control`.
4. One shared control plane may host approximately 5–10 customer companies. Each
   company is one customer workspace initially; the model supports more than one
   workspace per company later without adding an organization hierarchy now.
5. One project repository produces a platform-global immutable manifest release
   containing a compact asset/pipeline catalog. Large SQL execution trees are
   immutable, content-addressed execution packages stored separately in PostgreSQL.
   Each workspace deployment materializes the exact common, explicitly selected,
   and dependency targets that workspace may execute and the subset customer users
   may see.
6. Customer analytics data is physically separated outside Favn: each workspace has
   a dedicated blob-storage account and DuckLake PostgreSQL metadata store. Shared
   Favn PostgreSQL contains control-plane state only.
7. Favn team members use explicit platform-authorized read paths for bounded
   cross-workspace monitoring. Customer reads always require one `WorkspaceContext`
   and can return only that workspace's authorized targets and runs.
8. The 97-callback mega-adapter is replaced by small capability stores. No concrete
   module implements the entire persistence surface.
9. Contracts represent atomic domain commands and bounded queries, not table CRUD.
10. Run snapshots plus append-only run events remain the authoritative run model.
   Favn does not become a fully event-sourced system.
11. Every authoritative mutation that affects asynchronous work also writes a durable
   outbox event in the same transaction.
12. PostgreSQL identity/sequence values are never treated as commit order. A
   post-commit sequencer assigns transactional publication IDs before projectors or
   live replay advance a durable cursor.
13. Operator summaries are compact, versioned, repairable projections. A run
   transition must never scan its execution group or sibling windows.
14. Multi-node ownership uses expiring leases and monotonically increasing fencing
   tokens. Process registration, PubSub, and node memory are never correctness
   authorities.
15. PostgreSQL tables and indexes are designed from typed access patterns. Broad
    scans followed by Elixir filtering and delimiter-based membership columns are
    forbidden.
16. Cache correctness never depends on invalidation. The first cache layers are
    normalized indexes and persisted read models; node-local ETS is limited to safe,
    versioned data.
17. Existing private-development storage data is reset rather than migrated into
    V2. If that decision changes, data migration must be designed as a separate
    project before implementation continues.
18. SQLite may be reconsidered after PostgreSQL passes every production gate. It
    must adapt to the accepted semantics; it must not weaken this design.
19. The full memory storage backend is removed after tests have moved to pure unit
    tests, focused fakes, and PostgreSQL integration tests.
20. Execution packages are uploaded before the compact manifest index that references
    them. Runtime fetches only the selected asset's package by its SHA-256 primary key;
    catalogue and planning reads never load generated SQL for unrelated assets.
21. One immutable execution plan is stored in `run_plans` when a run is submitted.
    Mutable run snapshots keep only its hash and never rewrite or re-hash the full plan
    during transitions.
22. An active run holds a runner-local manifest lease. Cache eviction skips leased
    releases. Lease acquisition preflights the complete planned SQL scope exactly once;
    per-work preparation obtains only its handle, selected asset, and package relations
    in one atomic cache call and never rescans the plan.
23. `RunManager` is a small in-memory coordinator only. Submission persistence,
    cancellation writes, recovery reads, manifest loads, and crash terminalization run
    in caller or supervised worker processes so PostgreSQL latency cannot serialize the
    node-wide manager mailbox.
24. Pipeline retry recovery uses one compact stage bitset checkpoint event. Mutable
    snapshots contain only scalar checkpoint identity/timing; the checkpoint is the
    authoritative replacement for per-node pipeline retry-scheduled events.

## Superseded direction

This decision supersedes the SQLite-first production direction in:

- `docs/refactor/PHASE_6_STORAGE_ADAPTER_PLAN.md`;
- the former SQLite version of `docs/production/single_node_contract.md`;
- SQLite-first production statements in `README.md`, `docs/FEATURES.md`, and
  `docs/ROADMAP.md`.

Archived/refactor documents may still describe prototype behavior, but they are no
longer implementation direction or a release promise.

## Owner decision record

The owner accepted all original non-tenancy decisions on 2026-07-16 and clarified
that Favn is a consultant-operated data-platform service for approximately 5–10
salmon-production companies, not a public self-service SaaS. The storage model uses
customer workspaces as explicit operating and isolation boundaries without adding
SaaS billing, plan, or generalized provisioning abstractions.

| Decision | Benefit | Cost or rejected alternative |
| --- | --- | --- |
| Reset private-development storage at cutover | One clean baseline and no compatibility code | Existing private-dev state is discarded; a data migration would be a separate project. |
| Require PostgreSQL 18 in dev/test/CI | One engine contract and production-relevant evidence | Local development needs PostgreSQL; no database-free full runtime. |
| One Favn-owned repo and `favn_control` schema | Predictable lifecycle, pool, migrations, telemetry, and permissions | Embedders cannot substitute an arbitrary host repo. |
| Capability stores across 11 modules | Small invariant boundaries and typed use cases | More named functions than generic CRUD; generic `execute/query` is rejected as hidden complexity. |
| Post-commit publication sequencer | No cursor holes from out-of-order transaction commits without serializing every run write | Adds a small observable delay and one batched singleton sequencing point. |
| Remove the full memory backend and freeze SQLite | PostgreSQL behavior becomes the only integration truth | Some tests need focused fakes or a real database; SQLite convenience waits. |
| No automatic production migration | Safe controlled rollout with a least-privilege runtime role | Deployment must run and observe a separate migration job. |
| Customer-workspace model in one shared database/schema | Fits a small consultant-operated fleet and avoids a dangerous ownership-key/FK/index retrofit | Application scoping, composite constraints, and negative isolation tests are required; database/schema-per-customer is not the baseline. |
| Global immutable manifest releases plus workspace deployments | One analytics model can be rolled out to several companies without copying manifest content | Customer-specific configuration must be modeled at deployment boundaries and cannot mutate the shared release. |
| Compact manifest indexes plus content-addressed execution packages | Very large generated SQL no longer inflates every catalogue, planning, cache, or runtime registration read | Publication has an explicit package-first protocol and runtime execution requires one indexed package fetch. |
| Exact immutable deployment target catalog | Common and customer-specific assets coexist in one repository without runtime ambiguity or code-level customer IDs | A new common asset appears only after a new workspace deployment; deployment must resolve and validate dependency closure. |
| Hard-separated customer data planes | Blob data and DuckLake metadata cannot mix even though Favn monitoring is shared | Each workspace needs dedicated infrastructure and credential rotation. |
| Logical credential names plus one secret-store endpoint per workspace | The same authored code runs everywhere and secret rotation does not rebuild manifests | Runner resource/session caches must include workspace and resolved endpoint identity. |
| Ecto for repo/schema/query composition, explicit SQL for concurrency-critical paths | Typed ordinary code without hiding PostgreSQL semantics | The implementation will intentionally retain focused SQL. |
| Conservative initial retention | Canonical history and SSE replay remain complete | Operators must monitor growth until safe history watermarks are implemented. |

Changes to this record require updating the affected invariants before implementation
rather than carrying an informal exception into code.

## Assumptions and non-goals

### Assumptions

- A production deployment may run multiple orchestrator nodes against one
  PostgreSQL primary.
- Every node can use PostgreSQL during development and test. A database-free full
  runtime is not required.
- Run IDs and manifest IDs remain stable prefixed strings because they are already
  operator-visible. Internal row IDs use PostgreSQL `bigint` identity columns;
  publication order uses the separate transactional counter.
- One Favn control plane is expected to host approximately 5–10 customer companies.
  One company maps to one workspace initially. Consultants and service identities may
  be members of several workspaces.
- Customers use substantially the same source applications and analytics data model.
  Immutable manifest releases are platform-global; credentials, connection mappings,
  configuration, execution state, and data-plane namespaces are workspace-specific.
- The Favn team owns one project repository containing both shared and
  customer-specific assets. Customer identifiers are deployment configuration, not
  authored into reusable asset code merely to control availability.
- Every workspace has a dedicated blob-storage account, DuckLake PostgreSQL metadata
  store, and hosted Key Vault or equivalent secret-store endpoint. Logical secret
  and connection names are standardized across workspaces.
- Customers reach Favn through its authenticated UI/API, never with direct access to
  the shared control-plane database. Platform consultants require separately granted
  cross-workspace access.
- The database is a trusted control-plane service reached through a private network
  with TLS in production.
- The active PostgreSQL major version is upgraded deliberately; CI always covers
  the declared production major.

### Non-goals

- Cross-database SQL generation or lowest-common-denominator behavior.
- A generic repository abstraction over arbitrary databases.
- Full event sourcing or replaying all control-plane truth solely from an event log.
- Read-replica routing in the first implementation.
- Redis or another distributed cache in the first implementation.
- Public self-service signup, billing, plans, customer-managed provisioning, or a
  generic organization hierarchy.
- Database-per-workspace, schema-per-workspace, or sharding as the baseline.
- Combining customer raw data merely because customers share an analytics model.
- Automatic production migrations during application startup.
- Compatibility with existing private-development SQLite/PostgreSQL rows.
- Persisting user asset data, DuckDB tables, DuckLake catalogs, or SQL session state.

## Production quality gates

Storage V2 is not complete because it compiles or passes CRUD tests. It must satisfy
these properties:

| Property | Required result |
| --- | --- |
| Atomicity | Accepted run transitions atomically update the snapshot, append the event, and append required outbox records. |
| Fencing | A worker that lost ownership cannot mutate the run, renew its work, or complete a reused claim. |
| Idempotency | Retrying a command with the same identity and content returns the committed result; conflicting reuse is rejected. |
| Bounded work | Transition cost is independent of sibling run/window count. All high-growth reads declare page and payload limits. |
| Multi-node safety | Schedulers, projectors, recovery, admission, and dispatch can run on multiple nodes without duplicate authoritative effects. |
| Workspace isolation | Workspace-owned commands, queries, relationships, projections, caches, and events cannot read or reference another workspace. |
| Repairability | Derived projections can be rebuilt from authoritative tables without stopping ordinary writes. |
| Operability | Exact schema readiness, migration status, projection lag, pool pressure, lock contention, retention, backup, and restore are observable. |
| Security | No raw session tokens, passwords, idempotency keys, secrets, or unsafe Erlang terms are persisted. |
| Evidence | PostgreSQL integration, concurrency, plan, load, restart, and restore tests are mandatory in CI or explicit production acceptance slices. |

## Scale envelope and budgets

The implementation must be tested at or above the following initial envelope. These
are engineering acceptance inputs, not promised customer quotas.

| Dimension | Acceptance cardinality |
| --- | ---: |
| Manifest versions | 10,000 |
| Runs | 1,000,000 |
| Run events | 20,000,000 |
| Runs in one execution group | 10,000 |
| Windows in one backfill | 100,000 |
| Targets referenced by one run | 10,000 |
| Log entries | 100,000,000 before retention |
| Concurrent run-transition writers | 100 |
| Concurrent child completions in one group | 100 |
| Orchestrator nodes | 3 minimum acceptance topology |

Hard query/work budgets:

- Creating or transitioning one run must be `O(1)` in runs, events, siblings, and
  windows. Work may be `O(T)` only for the run's own `T` target memberships, and
  those writes must be bulk operations.
- Acquiring execution admission may be `O(S)` for the requested scope count, with a
  configured maximum of eight scopes per lease.
- Interactive pages default to 50 records and permit at most 200.
- Internal cursor scans permit at most 500 records per batch.
- Log ingestion uses bulk inserts in chunks of 100–1,000 rows; it must not allocate
  a sequence and issue an insert per entry.
- Projection workers process configurable batches, initially 250 events, with
  transactions short enough to remain below the lock timeout.
- The outbox sequencer processes committed rows in batches, initially up to 1,000,
  and must sustain at least twice the measured peak authoritative mutation rate.
- No interactive response may contain every child, window, event, or log record for
  a high-cardinality aggregate.
- At 10,000 group children, a child transition must execute the same query shape and
  touch the same number of rows as it does at 10 children.

Reference-environment latency gates must be recorded with the benchmark hardware.
The initial targets are p95 below 50 ms for run commits, p95 below 100 ms for
bounded operator pages, and no more than 10 ms p95 pool queue time under the
declared concurrency test. Hardware-independent row and query budgets remain the
stronger acceptance criteria.

## Ownership and application boundaries

```text
public/API/CLI/view caller
          |
          v
FavnOrchestrator public/use-case facade
          | resolves actor/service -> WorkspaceContext
          |
          v
orchestrator domain owner
          |
          v
FavnOrchestrator.Persistence capability facade + typed command/query
          |
          v
FavnStoragePostgres capability implementation
          |
          v
FavnStoragePostgres.Repo -> PostgreSQL favn_control schema
```

### `favn_orchestrator` owns

- Persistence behaviours and typed command/query/result contracts.
- Run, scheduler, admission, backfill, freshness, auth, and idempotency semantics.
- Workspace-context resolution, authorization, and explicit platform-versus-workspace
  command semantics.
- Canonical JSON-safe payload shapes and projection transformations.
- Public bounded operator DTOs and error contracts.
- The decision to start repair, recovery, retention, or reconciliation work.

### `favn_storage_postgres` owns

- `FavnStoragePostgres.Repo` and its supervision.
- Ecto schemas, changesets used for database rows, queries, migrations, and SQL.
- PostgreSQL transactions, row locks, `SKIP LOCKED`, conflict clauses, bulk writes,
  readiness queries, and SQLSTATE mapping.
- Durable outbox/projector cursor mechanics and database-backed worker leases.
- Calling orchestrator-owned pure projection transformations.
- PostgreSQL-specific telemetry measurements and redacted diagnostics.

### Boundary rules

- `favn_view`, `favn_runner`, and public `favn` code must not use the repo, Ecto row
  schemas, or store implementations.
- Ecto schemas never cross into orchestrator public or operator-facing return types.
- Adapter code must not decide scheduling, retry, cancellation, freshness, or
  authorization policy.
- The orchestrator must not assemble SQL, depend on Postgrex structs, or inspect
  database constraints.
- All backend modules are internal. Only orchestrator persistence facades are called
  by control-plane code.
- Every workspace-owned public/use-case command and query receives a validated
  `%FavnOrchestrator.WorkspaceContext{workspace_id: ...}`. Storage never infers workspace from
  a process dictionary, connection session, application environment, ID prefix, or
  caller-supplied arbitrary metadata.
- Compile-time dependency points inward: `favn_storage_postgres` depends on the
  orchestrator-owned contracts; `favn_orchestrator` does not depend on the concrete
  PostgreSQL app. The release/local composition root includes both, configures the
  backend module, and starts returned child specs. This avoids a circular umbrella
  dependency while preserving the boundary.

## Repository and runtime topology

### Repository ownership

Storage V2 uses one Favn-owned `FavnStoragePostgres.Repo`. The current
`repo_mode: :managed | :external` split is removed.

An embedding application may point the Favn repo at the same PostgreSQL server or
database as another repo, but Favn keeps its own pool, configuration, telemetry,
schema, and migration ownership. This prevents host-repo lifecycle and pool settings
from silently changing Favn correctness.

### PostgreSQL namespace

All objects live in a dedicated `favn_control` schema. Runtime connections set:

```sql
SET search_path TO pg_catalog, favn_control;
```

Ecto schemas still set `@schema_prefix "favn_control"`; correctness does not rely
only on `search_path`. The repo sets `migration_default_prefix: "favn_control"`, so
`schema_migrations` is owned with the application schema rather than silently
landing in `public`.

No extensions are required. Case-insensitive usernames use a normalized column and
unique index rather than `citext`. Application-generated IDs avoid `pgcrypto`.

### Engine support

- Reference and production major: PostgreSQL 18.
- Production and CI must use the latest PostgreSQL 18 minor release.
- A major upgrade requires a compatibility test run, restored production-size
  snapshot, query-plan review, and documented rollback/restore path.
- Azure Database for PostgreSQL Flexible Server is a supported deployment target,
  but no Azure-only SQL is allowed in the persistence model.

### Connection configuration

Production configuration must support a secret-backed URL and explicit overrides:

```elixir
config :favn_storage_postgres, FavnStoragePostgres.Repo,
  url: System.fetch_env!("FAVN_DATABASE_URL"),
  pool_size: 15,
  queue_target: 50,
  queue_interval: 1_000,
  timeout: 15_000,
  ssl: FavnStoragePostgres.Config.verified_tls_options!()
```

The TLS helper is target API, not a placeholder for `ssl: true`: production options
must verify the certificate chain and database hostname from configured trust
material. Local development may explicitly select a separate non-production TLS
mode.

The real runtime loader must validate redacted normalized configuration. It must not
log URLs, usernames with embedded secrets, passwords, certificates, or token values.

Every connection sets bounded server-side defaults appropriate to its role:

- `application_name` with service and instance identity;
- `statement_timeout`;
- `lock_timeout` shorter than the normal command timeout;
- `idle_in_transaction_session_timeout`;
- UTC session timezone.

Bounded online maintenance commands run in checked-out transactions with local
maintenance timeouts; offline restore verification checks out one connection with
an explicit restore timeout. These session-local settings do not change global
normal-request timeouts. A distinct maintenance pool is unnecessary at the initial
5–10 workspace scale and can be added later if observed pool pressure warrants it.

### Pool budgeting

Pool size is a deployment budget, not a per-node performance knob:

```text
(orchestrator nodes × runtime pool size)
+ migration connections
+ direct notification connections
+ operational headroom
<= PostgreSQL max connection budget
```

Production diagnostics must report pool size, checkout queue latency, timeout count,
and database connection budget inputs without exposing credentials.

PgBouncer transaction pooling may be introduced later. If used, prepared statements
must be configured compatibly and the optional `LISTEN` connection must bypass the
transaction pool. Storage correctness must not use session advisory locks.

## Customer-workspace foundation

This is a small, consultant-operated multi-customer platform, not a generalized SaaS.
The baseline is one Favn deployment, one PostgreSQL database, one `favn_control`
schema, and approximately 5–10 customer workspaces. Workspace scope is still a hard
correctness boundary: retrofitting it into every key, relationship, query, cache,
cursor, and projection after production data exists would be a high-risk migration.

### Required in the first baseline

- Add an authoritative `workspaces` table. Provisioning may remain an explicit
  consultant/operator command; no self-service workflow is required.
- Require explicit `WorkspaceContext` at orchestrator use-case boundaries. Internal
  platform maintenance uses a separate typed platform context, never a magic workspace
  ID.
- Add non-null `workspace_id` to every workspace-owned authoritative row, coordination
  row, outbox event, projection, log batch/entry, audit entry, and idempotency record.
- Include `workspace_id` in every workspace-owned uniqueness constraint, relationship,
  query predicate, keyset cursor, and cache key. Workspace-owned parents use or expose
  composite `(workspace_id, id)` identity and child foreign keys reference both columns
  so PostgreSQL rejects cross-workspace relationships.
- Put `workspace_id` first in indexes whose access pattern always starts from workspace
  scope. Deliberately global maintenance/publication indexes are documented
  exceptions.
- Store immutable, canonical manifest versions globally because they are shared
  analytics releases. A `workspace_deployments` row binds one manifest version to
  one workspace and a fingerprint/version of workspace-specific non-secret
  configuration. Secrets are referenced, never embedded in the manifest or row.
- Keep one runtime-state row per workspace pointing to its active deployment.
  Schedules, runs, backfills, materializations, target projections, credentials,
  connection mappings, and data-plane namespaces are workspace-local. Every run pins
  both the deployment and manifest release it executed.
- Model operator identity globally, with workspace membership and workspace-specific roles
  in `auth_workspace_memberships`; do not duplicate credentials per company or put one
  workspace directly on an actor.
- Make projectors, repair, retention, and caches preserve workspace scope even when a
  worker processes batches for several workspaces.
- Add negative tests proving workspace A cannot read, mutate, reference, replay, cache,
  or infer workspace B state.

Consultants and service identities may belong to several workspaces. APIs and UI
flows must nevertheless select one explicit workspace for ordinary operations.
Cross-workspace reporting or maintenance is a separately authorized platform
operation, never an omitted predicate.

### Shared repository and target availability

One repository and one manifest release contain the full platform catalog. The
manifest records a stable distribution default for each target:

- `common`: selected for every new workspace deployment by default; or
- `selective`: unavailable until the deployment explicitly selects the target or a
  stable authored bundle that contains it.

These labels are release metadata, not authorization by themselves. The deployment
command resolves common targets, explicit workspace selections, pipeline members,
and transitive dependencies into exact immutable rows in
`workspace_deployment_targets`. It validates every reference and dependency before
opening the transaction, then bulk-inserts the resolved catalog. A missing target,
invalid pipeline, or incomplete dependency closure rejects the entire deployment.
Each row also stores a bounded JSONB presentation descriptor derived during that
same planning pass. Catalogue and active-release reads therefore use the indexed,
immutable deployment rows and never fetch or decode a large manifest merely to
render target labels and capabilities. The descriptor is fingerprinted as part of
the target catalog and is not an execution authority by itself.

Row existence means the target may participate in execution. Each row separately
records whether it is customer-visible. A dependency may therefore execute without
appearing as a customer-selectable top-level asset. Common and explicitly selected
targets are visible by default; deployment policy may make an internal target hidden.

There is no runtime `all common assets` wildcard. A deployment pins an exact target
set, so adding a common asset to a later release does not silently change an active
customer. Schedules are created only for authorized deployment targets. Run creation
and persisted run targets use composite foreign keys to the deployment catalog, so a
buggy planner cannot execute an asset that was not selected for that workspace.

Customer asset/catalog APIs query indexed `workspace_deployment_targets` rows with
`customer_visible = true`; they do not deserialize the full manifest and filter it in
Elixir. Platform views may inspect hidden and visible targets through explicit
`PlatformContext` reads.

### Data-plane binding and credential resolution

Favn PostgreSQL is the shared control plane: manifests, deployments, schedules,
runs, events, projections, logs, and authorization. It does not store or serve
customer analytics tables and is not the DuckLake metadata database.

Each workspace deployment contains a typed, canonical, non-secret resource binding:

- dedicated blob-storage account/container endpoints;
- dedicated DuckLake PostgreSQL metadata-store identity;
- dedicated Key Vault or equivalent secret-store URL;
- standardized logical connection and secret names expected by authored code; and
- any workspace-specific catalog/database names and safe connection options.

Connection strings, passwords, access keys, and resolved tokens are never stored in
the manifest, deployment row, run snapshot, event, log, or Favn database. Authored
assets refer only to stable logical resources. At execution, the orchestrator sends
the pinned workspace/deployment resource descriptor; the runner resolves the same
logical secret names against that workspace's validated secret-store URL. Secret
names normally resolve the vault's current active version, so rotation inside the
customer vault requires no manifest change; resolved values and tokens still obey
bounded expiry/refresh rules. Changing an endpoint or logical mapping creates a new
immutable deployment.

Secret-store and data-plane URLs cannot come from customer run parameters. Deployment
validation restricts their schemes/hosts and canonicalizes them before fingerprinting
to prevent endpoint injection. Credential providers use workload/managed identity
where available and return redacted typed failures.

Every SQL session-pool and credential-cache key begins with `workspace_id` and also
includes deployment/resource identity, endpoint configuration, required catalogs,
and adapter/provider fingerprint. A physical DuckDB/ADBC session may attach resources
for exactly one workspace and must never be reused across workspaces, even when
logical secret names or other configuration happen to match.

### Customer and platform visibility

Customer actors receive only workspace memberships. All customer catalog, run,
event, log, freshness, and backfill APIs construct `WorkspaceContext` from the
authenticated membership; request parameters cannot choose a broader scope. A
workspace-scoped cursor is rejected under every other workspace.

Activating a workspace deployment requires both workspace-admin authority and an
independent `platform_operator` or `platform_admin` grant. A customer administrator
cannot switch the platform-authored manifest release alone, while a platform grant
cannot omit the target workspace context.

Favn consultants who need fleet monitoring receive an explicit active grant in
`auth_platform_grants`. That grant resolves to `PlatformContext` and permits only
named, bounded cross-workspace read/maintenance use cases. Platform access is not
implemented by omitting a workspace predicate from a customer function. Platform
pages return `workspace_id` with every row and use global keyset cursors/indexes.
Platform mutations still select and authorize one workspace explicitly.

`favn_view` calls these use cases only through the public orchestrator facade. It
does not select scope, join workspace results, or query persistence directly.

### Deliberately omitted SaaS machinery

- Public signup, invitations, billing, plans, entitlement engines, and customer-owned
  administration.
- Dynamic database/schema placement, sharding, and a customer database fleet
  registry.
- Inheritance/template frameworks for customer analytics models. Shared manifests
  plus explicit deployments are sufficient until real variation proves otherwise.
- Elaborate plan-based quotas. Platform-global and per-workspace capacity limits are
  retained only to protect the service from noisy workloads.

### Isolation posture

The production baseline is shared database/shared schema with explicit workspace
context, workspace-leading indexes, composite workspace foreign keys, least-privilege
database roles, and adversarial isolation tests. This is proportionate for a service
operated only by trusted consultants and platform services.

Customers never receive PostgreSQL credentials; their access is through authenticated
orchestrator use cases. PostgreSQL row-level security remains an optional later
defense-in-depth layer rather than a baseline requirement because bounded
cross-workspace platform reads are a core use case. It is not a substitute for
scoped contracts, authorization, or composite constraints. If RLS is introduced,
connection-pool tests must prove session context is reset between checkouts.

If a contract, regulation, or data-residency requirement demands physical isolation,
run a separately deployed Favn control plane for that customer. Do not burden the
shared baseline with dynamic database-per-workspace routing. Schema-per-workspace is
not a target.

The common analytics model does not authorize mixing raw or customer-derived data.
Credentials, object-store prefixes, warehouse/database schemas, catalogs, and other
data-plane namespaces remain workspace-specific. Any cross-company benchmark or
aggregate is an explicit curated data product with its own authorization and data
agreement.

```text
one repository -> global manifest release (complete catalog)
          |
          +---- deployment A + exact target catalog ---- runtime/runs/schedules A
          |                 |
          |                 +---- Key Vault A -> Blob A + DuckLake metadata PG A
          |
          +---- deployment B + exact target catalog ---- runtime/runs/schedules B
                            |
                            +---- Key Vault B -> Blob B + DuckLake metadata PG B

customer identity -------- membership A only
consultant identity ------- explicit platform grant
```

This deliberately has no organization/template/inheritance layer. If one company
later needs two independently isolated operating environments, create two workspaces;
add a parent organization only when a concrete cross-workspace policy requires it.

## Persistence contract architecture

### Root backend contract

`FavnOrchestrator.Persistence.Backend` replaces adapter-wide CRUD. It has only
lifecycle responsibilities:

```elixir
@callback child_specs(keyword()) ::
  {:ok, [Supervisor.child_spec()]} | {:error, Persistence.Error.t()}

@callback stores() :: FavnOrchestrator.Persistence.Stores.t()

@callback readiness(keyword()) ::
  {:ok, FavnOrchestrator.Persistence.Readiness.t()} | {:error, Persistence.Error.t()}

@callback diagnostics(keyword()) ::
  {:ok, FavnOrchestrator.Persistence.Diagnostics.t()} | {:error, Persistence.Error.t()}
```

`Stores` is an explicit struct containing every required capability module. Startup
validates it once. There are no optional product-critical callbacks and no
`function_exported?/3` capability discovery in request paths.

The initial and only backend is `FavnStoragePostgres.Backend`.

The operation inventory below currently names 72 functions across 11 cohesive
capabilities. That is intentionally visible review surface, not one adapter with 72
callbacks. Collapsing them behind generic `execute/1`, `query/1`, or CRUD functions
would reduce the apparent count while hiding types and invariants, so it is not an
acceptable optimization. Each capability can be implemented, tested, and changed
independently; the count must fall when features or redundant access paths are
deleted, not by obscuring them.

### Contract design rules

- A store operation names an atomic domain outcome or bounded access pattern.
- Every argument and result uses a typed struct or a small explicit scalar.
- Every command/query includes either `WorkspaceContext` or an explicitly authorized
  `PlatformContext`; no operation has an unscoped overload or a default workspace
  inside persistence. Mutations are always workspace-scoped.
- Store queries include their limit, cursor, sort, and filter budget.
- Commands include expected versions, fencing tokens, and idempotency identity when
  required.
- API idempotency is committed in the same transaction as a database-local command;
  callers never perform a reserve call and a mutation call as two transactions.
- No `put_*` operation exposes an arbitrary mutable row.
- No separate `append_run_event` permits a caller to bypass snapshot/event atomicity.
- No `list_all`, unbounded default, generic map query, or radically option-dependent
  return shape is allowed.
- Maintenance and repair are separate from interactive stores.
- A behaviour should normally contain no more than twelve cohesive operations. The
  target is understandable invariants, not an artificially low callback count.

### Required capability stores

The following operation names are the target semantic surface. Exact arities may
change while typed command structs are implemented, but no lower-level operation may
be added without documenting its invariant here.

#### Registry store

| Operation | Contract |
| --- | --- |
| `register_execution_packages/1` | Platform-authorized, bounded, idempotent insertion of verified immutable packages by SHA-256 content hash plus a deduplicated batch audit; the same hash with different canonical content is a conflict. |
| `missing_execution_package_hashes/1` | Return the missing subset of one bounded, deduplicated platform-authorized hash batch with one indexed query. |
| `register_manifest/1` | Idempotently insert a platform-global immutable canonical manifest release by ID and content hash; reject either identity with different content. |
| `get_manifest/1` | Fetch a global release by typed selector `ById` or `ByContentHash`; return a decoded manifest or `:not_found`. |
| `get_execution_package/1` | Fetch and verify one immutable package by SHA-256 primary key for a workspace-authorized runtime path. It must not scan a manifest or package collection. |
| `deploy_manifest/1` | Resolve and validate common/selected/dependency targets, then atomically create an immutable workspace deployment and exact target catalog, activate it, increment workspace runtime revision, synchronize authorized schedule cursors, append audit/outbox events, and return runtime state. |
| `get_runtime_state/1` | Return active deployment, manifest identity, and runtime revision for the command workspace. |
| `get_deployment_targets/1` | Return the exact immutable target catalog for one workspace deployment, optionally restricted to customer-visible grants. |

Manifest listing is an operator read-store query, not registry mutation API.

#### Run store

| Operation | Contract |
| --- | --- |
| `create_run/1` | Atomically insert the initial snapshot, target memberships, first run event, and outbox event. Same command/content replays; conflicting identity fails. |
| `commit_transition/1` | Lock/guard the run, validate owner fence and expected sequence, insert the event, update the snapshot, append outbox, and return the fully persisted event/global IDs. |
| `request_cancellation/1` | Lock the run and atomically persist an operator cancellation request, canonical event, outbox event, and API idempotency result before external runner cancellation. |
| `get_run/1` | Fetch one canonical run snapshot. |
| `page_runs/1` | Load a bounded authoritative run-snapshot page only for internal detail reconstruction. Customer callers cannot construct platform scope. |
| `page_run_summaries/1` | Return compact relational run summaries through a bounded workspace/platform keyset query; authoritative snapshots are never selected. |
| `page_events/1` | Page workspace run/group events or platform-wide events through an explicit typed scope and event cursor. |
| `pin_runtime_inputs/1` | Bulk insert immutable protected input pins; exact duplicates replay and conflicts reject the whole command. |
| `get_runtime_inputs/1` | Batch-fetch exact run/node keys or all pins for one bounded run. |

There is no general `put_run`, separate event append, unbounded group load, or raw
snapshot replacement. A future import tool must use a separate offline contract.

#### Run ownership and runner-execution store

| Operation | Contract |
| --- | --- |
| `claim_run/1` | Acquire or take over an expired run ownership lease and return a new monotonically increasing fencing token. |
| `claim_recovery_batch/1` | Atomically claim a bounded set of currently available/expired ownership rows by eligibility, without a permanent identity cursor. |
| `renew_run/1` | Renew only when run, owner, and fencing token still match. |
| `release_run/1` | Idempotently release only the matching ownership generation. |
| `record_dispatch/1` | Persist runner dispatch intent before the external call, with a unique dispatch identity. |
| `advance_execution/1` | Apply a guarded runner-execution lifecycle transition, including cancellation outcome. |
| `page_executions/1` | Bounded active recovery pages by run, owner, or workspace; historical pages require an exact `run_id`. |

Every active run transition issued by a run worker carries the run ownership fencing
token. A stale process receives `:fenced`, even if its BEAM process is still alive.

#### Scheduler store

| Operation | Contract |
| --- | --- |
| `claim_due_schedules/1` | Claim a bounded set of due cursor rows with `FOR UPDATE SKIP LOCKED`, lease expiry, and fencing generation. |
| `commit_evaluation/1` | If the schedule fence matches, atomically insert deterministic occurrence intents and advance the cursor. |
| `claim_occurrences/1` | Claim a bounded queue of persisted, undispatched occurrence intents. |
| `complete_occurrence/1` | Link the idempotently created run or record a bounded dispatch failure using the occurrence fence. |

Schedule calculation remains orchestrator logic. Storage owns claiming, deduplication,
and atomic cursor/occurrence persistence.

#### Execution admission store

| Operation | Contract |
| --- | --- |
| `admit/1` | Lock requested capacity scopes in canonical order, validate limits, create the lease/scopes, remove the matching waiter, and return the lease; otherwise persist/return the blocking scope. |
| `renew_lease/1` | Renew only the matching active lease identity and owner generation. |
| `release_lease/1` | Idempotently release one lease and decrement capacity counters exactly once. |
| `release_run_leases/1` | Release matching active leases for one run in a bounded transaction and return freed scopes. |
| `expire/1` | Claim and expire bounded batches of overdue leases/waiters, preserving exact counter updates. |

#### Materialization store

| Operation | Contract |
| --- | --- |
| `claim/1` | Return an existing successful materialization, an active competing claim, or a new/reclaimed claim with fencing generation. |
| `renew/1` | Heartbeat only the matching active claim generation. |
| `finish/1` | Fence the claimant, mark the claim terminal, insert an immutable successful materialization when applicable, and append outbox in one transaction. |
| `get_many/1` | Batch-fetch exact claim/materialization identities used by planning and recovery. |

Claims are coordination state. Successful materializations are a separate immutable
ledger and remain repair authority after claims are purged.

#### Backfill store

| Operation | Contract |
| --- | --- |
| `start_plan/1` | Idempotently insert a planning header with expected window/batch counts and canonical plan hash. |
| `append_plan_batch/1` | Insert one deterministic bounded batch and its immutable batch receipt; exact replay succeeds and conflicting batch content fails. |
| `activate_plan/1` | Lock the header, verify every immutable batch/count/hash, move the backfill to dispatchable state, and append outbox. |
| `claim_windows/1` | Claim a bounded queue of dispatchable windows with fencing and `SKIP LOCKED`. |
| `transition_window/1` | Guard window version/fence, update one window, and append outbox; no group/window-wide aggregation. |
| `get_backfill/1` | Fetch the authoritative header plus compact projected progress watermark. |
| `page_windows/1` | Keyset-page windows by backfill/status/range. |

Backfill completion and progress are driven by an ordered projector/finalizer. Child
workers never lock or rebuild a common full-group summary.

#### Operator read store

| Operation | Contract |
| --- | --- |
| `page_manifests/1` | Bounded manifest history. |
| `page_execution_groups/1` | Compact group overview page. |
| `get_execution_group/1` | One overview plus first bounded child/window/failure pages and independent continuation cursors. |
| `page_group_runs/1` | Bounded group children/attempts. |
| `page_group_windows/1` | Bounded group windows. |
| `get_target_statuses/1` | Batch exact target IDs for one manifest/kind. |
| `page_target_runs/1` | Bounded target history ordered by run submission. |
| `get_freshness_many/1` | Batch exact freshness identities or latest-success node identities. |

The operator facade may compose these results, but it must preserve independent
cursors and must not recursively consume all pages.

#### Log store

| Operation | Contract |
| --- | --- |
| `append_batch/1` | Normalize/redact, atomically insert one deduplicated batch, its outbox event, and bulk entries, then return committed entries in input order. |
| `page/1` | Keyset-page one typed filter with a maximum result size. |
| `purge/1` | Delete one bounded retention batch and return the last processed cursor/count. |

#### Identity store

| Operation | Contract |
| --- | --- |
| `create_actor/1` | Atomically create global actor, initial credential, and membership in the command workspace; normalized username is unique. |
| `get_actor/1` | Fetch global identity plus current membership by typed ID or normalized username selector under one workspace. |
| `page_actors/1` | Bounded membership/actor list for one workspace. |
| `set_access/1` | Atomically change one workspace membership or platform grant through a typed access command and append audit; platform grants require existing platform authority. |
| `change_password/1` | Lock actor/credential, replace the global hash, revoke active sessions when requested, and append workspace audit. |
| `create_session/1` | Persist only a token hash with absolute expiry and append audit. |
| `get_session/1` | Resolve by session ID or token hash and return only active/explicit status. |
| `revoke_sessions/1` | Revoke one session or all actor sessions idempotently and append audit. |
| `page_audit/1` | Keyset-page redacted audit entries. |

#### Shared command idempotency

Idempotency is a transaction component used by capability-store commands, not a
separate behaviour that callers invoke before and after a mutation. Every supported
API command carries a typed idempotency context. The PostgreSQL implementation
conflict-checks or inserts its record, performs the domain mutation, and stores the
bounded replay result in one `Repo.transaction/2`.

External workflows instead persist a deterministic intent/outbox record and return
an accepted result. A worker performs the external effect outside the transaction
and resolves it by deterministic identity. Favn never claims exactly-once external
effects and never keeps a database transaction open around a network call.

#### Maintenance store

| Operation | Contract |
| --- | --- |
| `backfill_missing_projection/1` | Replay one scoped, bounded publication batch to fill missing projection rows without overwriting rows at an equal or newer publication. |
| `reconcile/1` | Check and repair explicitly named authoritative/derived invariants in bounded batches. |
| `purge/1` | Execute one configured retention batch. |

Maintenance operations never run implicitly from interactive reads.

The initial release deliberately does not claim online repair of corrupted
projection values. That requires shadow projection generations plus an atomic
workspace cutover; replaying into live tables is only safe for missing rows because
publication guards prevent regression while projectors continue to run.

## Shared command and result conventions

All commands have explicit structs under
`FavnOrchestrator.Persistence.Commands`. Common fields are:

- `workspace_context`: validated workspace identity and authorization scope for every
  workspace-owned command;
- `command_id`: stable retry identity where an expected sequence is insufficient;
- optional typed `idempotency` context for supported external API commands;
- `occurred_at`: domain time supplied once by the orchestrator;
- `expected_version` or `expected_sequence`;
- `owner_id` and `fencing_token` for owner-bound work;
- canonical DTO payloads;
- bounded trace metadata with no secrets.

All committed write results include the committed database identity/version needed
by callers. Callers must not re-read a just-written row merely to obtain its event
ID, sequence, or cursor.

Domain occurrence times are normalized command inputs. Lease, claim, session-expiry
comparison, and takeover decisions use PostgreSQL `clock_timestamp()` plus bounded
durations so node clock skew cannot grant authority. Tests control policy durations,
not the database clock through application timestamps.

Example transition result:

```elixir
%FavnOrchestrator.Persistence.Results.RunCommitted{
  run: canonical_run,
  event: canonical_event,
  event_id: 12_345,
  outbox_event_id: 54_321,
  replayed?: false
}
```

## Error contract and retry policy

No Postgrex, DBConnection, Ecto changeset, SQL text, connection details, or arbitrary
exception escapes the persistence boundary.

```elixir
%FavnOrchestrator.Persistence.Error{
  operation: :commit_run_transition,
  kind: :conflict,
  outcome: :not_applied,
  retryable?: false,
  constraint: :run_event_sequence,
  details: %{}
}
```

Allowed `kind` values:

- `:invalid`;
- `:not_found`;
- `:conflict`;
- `:fenced`;
- `:capacity_exceeded`;
- `:timeout`;
- `:unavailable`;
- `:schema_not_ready`;
- `:corrupt_data`;
- `:unknown_outcome`;
- `:internal`.

Retry rules:

- PostgreSQL serialization failure (`40001`) and deadlock (`40P01`) may be retried
  with bounded jitter only when the whole transaction is database-local and
  idempotent. Maximum automatic attempts: three.
- Unique/check/foreign-key violations are mapped to domain results and are not
  generically retried.
- Lock timeout returns a retryable contention result only when no external effect
  occurred.
- A lost connection around commit is `:unknown_outcome`. Resolve it through command
  identity, event sequence/hash, occurrence identity, or idempotency record before
  retrying.
- No transaction remains open during runner calls, password hashing, network calls,
  sleeps, user input, or expensive manifest planning.

The default transaction isolation is `READ COMMITTED` with explicit row locks and
conditional updates. A specific command may use `SERIALIZABLE` only when its
invariant is simpler and its bounded retry behavior is tested. Isolation level is
not a substitute for unique constraints, fencing, or deterministic lock order.

### Canonical lock order

Transactions lock only rows required by their invariant. When a command spans the
listed row classes, it uses this capability order:

| Capability | Lock order |
| --- | --- |
| Idempotent API command | Idempotency record, then the capability order below |
| Manifest deployment | Workspace `workspace_runtime_state`, then same-workspace schedule cursors by natural key |
| Run transition | `runs`, then `run_ownerships`, then exact runner-execution row when required |
| Admission | Capacity scopes by canonical `(workspace_id, kind, key)`, then waiter; release locks lease first, then its scope rows in the same canonical order |
| Materialization | Exact claim row, then immutable success insert |
| Backfill planning | Backfill header, then batch receipt, then deterministic window rows |
| Window transition | Exact window only; it never locks overview/header rows |
| Projection batch | Projector cursor, then projection rows by primary key |
| Outbox sequencing | Publication singleton, then currently visible outbox rows by `outbox_event_id` |
| Identity mutation | Actor, membership by workspace, credential, then sessions by session ID |

No command may acquire these in reverse merely because an Ecto pipeline makes it
convenient. A new cross-capability transaction must document its combined order and
add a competing-connection deadlock test before merge.

## Data-model conventions

### Workspace scope

`workspaces` is the root control-plane ownership table. It contains a stable
operator-visible `workspace_id`, unique normalized slug, display name, lifecycle
status, version, and timestamps. Workspace deletion is a deliberate retention/export
workflow, never an unbounded cascade.

Workspace-owned operator-visible IDs are stable strings but are identified by
`(workspace_id, id)`; global uniqueness is not assumed. Every child relationship
carries `workspace_id` and uses a composite foreign key. This is intentional: two
companies may use the same authored/deterministic ID, and a buggy query cannot
attach workspace A's run to workspace B's deployment. Global immutable manifest
releases are a deliberate exception because the same analytics release is deployable
to several workspaces. Other deliberately platform-global internal identities such
as publication/event/log row IDs use a global scalar key.

Platform-global tables are limited and named explicitly: `workspaces`,
`manifest_versions`, global actor credentials/sessions,
`outbox_publication_state`, global projector ownership, and Ecto migration metadata.
Mixed-scope tables require an explicit scope-kind check or are split; nullable
`workspace_id` is not used as an undocumented shortcut.

### Names and types

- Tables are plural `snake_case` under `favn_control`.
- Operator-visible IDs remain non-empty `text` values with documented
  `octet_length` check constraints; unconstrained attacker-controlled text is not an
  identifier type.
- Internal row identity uses `bigint GENERATED BY DEFAULT AS IDENTITY`; identity
  allocation is not commit order and is never a durable tail/replay watermark.
- Cross-transaction publication order uses the transactional counter described in
  “Durable outbox and projections.”
- All timestamps are `timestamptz(6)` and application values are UTC.
- Hashes and token hashes are `bytea`; rendered hex belongs at API/log boundaries.
- Statuses are `text` plus `CHECK` constraints, not PostgreSQL enum types, so status
  evolution uses ordinary migrations.
- Money-like or precise numeric values use appropriate numeric types, never floats.
- JSON data is `jsonb`, JSON-safe, canonical at the codec boundary, and accompanied
  by a positive `payload_version` where it is a reconstructable record.
- Queryable fields are columns. JSONB is not a replacement for modeled access paths.
- No BEAM term serialization is durable.
- No raw module atom is reconstructed from storage. Text identities are resolved
  through the pinned manifest/known-atom boundary.

### Common mutable-row fields

Mutable authoritative rows use:

- `version bigint NOT NULL` for optimistic concurrency where applicable;
- `inserted_at timestamptz(6) NOT NULL`;
- `updated_at timestamptz(6) NOT NULL`;
- a database constraint on legal status/value ranges.

Database defaults are limited to mechanical values such as identity columns and
`inserted_at`; domain timestamps and statuses come from normalized commands.

### Foreign-key policy

- Authoritative identity relationships use foreign keys.
- High-cardinality child deletion does not rely on a single huge cascading
  transaction. Retention removes children in bounded batches before the parent.
- Append-only logs may carry logical run IDs without a foreign key so independent
  retention cannot trigger mass updates.
- Read-model references may be nullable or logical when authoritative retention can
  outlive/precede projection retention. This exception must be documented per table.
- Constraints are not omitted merely to make fixtures easier.

## Schema catalogue

### Core authority

| Table | Purpose and key |
| --- | --- |
| `workspaces` | Root customer-company/control-plane ownership identity. PK `workspace_id`; unique normalized slug. |
| `manifest_versions` | Platform-global immutable compact analytics indexes. PK `manifest_version_id`; unique `content_hash`. |
| `execution_packages` | Platform-global immutable SQL execution artifacts. SHA-256 `content_hash` PK plus verified asset identity and canonical payload. |
| `manifest_execution_packages` | Exact manifest-index/package association and asset identity. PK `(manifest_version_id, package_hash)`; unique `(manifest_version_id, asset_module, asset_name)`. |
| `workspace_deployments` | Immutable binding of a workspace, manifest release, and workspace configuration fingerprint. PK `(workspace_id, deployment_id)`. |
| `workspace_deployment_targets` | Exact immutable target catalog authorized for a deployment, including customer visibility and selection source. Composite PK. |
| `workspace_runtime_state` | One row per workspace containing active deployment and monotonic runtime revision. |
| `runs` | Workspace-owned current authoritative run snapshot. PK `(workspace_id, run_id)`. |
| `run_targets` | Normalized immutable run/asset/pipeline memberships. Composite PK. |
| `run_events` | Append-only canonical run events with globally unique `event_id` and per-run sequence. |
| `runtime_input_pins` | Immutable protected runtime input decisions per run/node key; key-version lookup is indexed for readiness-safe retirement. |
| `runtime_input_key_versions` | Compact inventory of key versions referenced by persisted pins; never stores key material. Unreferenced versions are removed only through the guarded operator compaction. |
| `run_ownerships` | Current expiring owner and fencing generation for active runs. |
| `runner_executions` | Durable runner dispatch/attempt/cancellation lifecycle. |

### Scheduling and execution coordination

| Table | Purpose and key |
| --- | --- |
| `schedule_cursors` | Per-manifest schedule evaluation state, claim lease, and fence. |
| `schedule_occurrences` | Durable unique due-work intents and dispatch state. |
| `capacity_scopes` | Explicit platform-global or workspace-owned configured limit and active lease count per scope. |
| `execution_leases` | Active/terminal capacity leases. |
| `execution_lease_scopes` | Lease-to-capacity-scope membership. |
| `admission_waiters` | Durable priority queue for blocked steps. |
| `materialization_claims` | Expiring fenced ownership of logical materialization identity. |
| `materializations` | Immutable successful materialization ledger. |

### Backfill and coverage authority

| Table | Purpose and key |
| --- | --- |
| `coverage_baselines` | Immutable/guarded coverage evidence owned by a manifest/pipeline. |
| `backfills` | Authoritative requested backfill identity, scope, range, and lifecycle. |
| `backfill_plan_batches` | Immutable receipts for resumable bounded window-plan insertion. |
| `backfill_windows` | Authoritative planned window and attempt/dispatch state. |

### Durable event delivery and repairable projections

| Table | Purpose and key |
| --- | --- |
| `outbox_events` | Workspace-owned durable events written with authoritative mutations and later assigned commit-safe publication order. |
| `outbox_publication_state` | Singleton transactional publication counter and sequencer lease. |
| `projection_cursors` | Per-projector cursor, worker lease, and generation. |
| `projection_failures` | Bounded redacted evidence for a blocked projection event. |
| `execution_group_overviews` | Compact group counters/status; never child ID arrays. |
| `backfill_overviews` | Compact per-status window counts and completion progress. |
| `target_statuses` | Workspace-owned current asset/pipeline status guarded by global source publication. |
| `asset_window_states` | Latest manifest/asset/window materialization projection. |
| `asset_freshness_states` | Exact manifest/asset/freshness projection and latest-success lookup. |

### Logs, identity, and API safety

| Table | Purpose and key |
| --- | --- |
| `log_batches` | One workspace-owned committed/outbox-linked identity per bounded producer batch. |
| `log_entries` | Workspace-owned redacted append-only operational logs with batch position and row identity. |
| `auth_actors` | Global operator identity, normalized username, and status. |
| `auth_credentials` | Password-hash record, one current row per actor. |
| `auth_sessions` | Opaque-session token hashes, expiry, and revocation. |
| `auth_workspace_memberships` | Workspace-specific actor membership, roles, and status. PK `(workspace_id, actor_id)`. |
| `auth_platform_grants` | Explicit consultant/platform roles and status. PK `actor_id`; never implied by workspace membership. |
| `auth_audit_entries` | Workspace-owned redacted append-only security/command audit stream. |
| `auth_platform_audit_entries` | Platform-scoped redacted audit for grant changes and cross-workspace operations. |
| `idempotency_records` | Workspace-owned hashed external command identity, fingerprint, state, and bounded replay response. |

## Core table design

### `manifest_versions`

Required columns:

- `manifest_version_id text PRIMARY KEY`;
- `content_hash bytea NOT NULL`;
- `schema_version integer NOT NULL CHECK (schema_version > 0)`;
- `runner_contract_version integer NOT NULL CHECK (runner_contract_version > 0)`;
- `payload_version smallint NOT NULL CHECK (payload_version > 0)`;
- `manifest jsonb NOT NULL`, containing the compact manifest index rather than SQL
  execution trees;
- `inserted_at timestamptz(6) NOT NULL`.

Unique `content_hash` supports idempotent registration.

Registration canonicalizes and validates before opening a transaction. Reusing the
same ID or content hash with different canonical bytes is a conflict. Manifest rows
are never updated. A manifest contains no workspace credentials, secret references,
or mutable customer configuration. Every SQL asset contains only its verified
execution-package hash. Registration rejects a manifest unless every referenced
package already exists and matches the indexed asset identity, then writes the
manifest and all package associations in one transaction.

### `execution_packages`

Required columns:

- `content_hash bytea PRIMARY KEY`, exactly 32 bytes of SHA-256 content identity;
- `asset_module text NOT NULL`;
- `asset_name text NOT NULL`;
- `runtime_input_resolver text`, when the package declares one;
- `payload jsonb NOT NULL` containing the canonical execution-package document;
- `first_linked_at timestamptz(6)`, set once when a manifest first references the
  package;
- `inserted_at timestamptz(6) NOT NULL`.

The application verifies schema version, canonical serialization, asset identity,
and content hash before insertion. A package is at most 4 MiB encoded and one command
is at most 32 MiB aggregate encoded content. Public upload requests contain at most
100 packages; the store accepts at most 1,000 packages within that byte budget for
trusted internal batching and inserts in chunks of 100. `ON CONFLICT DO NOTHING` is
followed by verification of every addressed row, so an exact replay succeeds and a
hash/content mismatch fails the transaction. Packages are immutable and global
because authored execution code is shared platform release content, never customer
data or credentials.

Package-first publication can leave an unreferenced row when a client uploads and
never completes manifest registration. Manifest publication validates identities in
bounded, payload-free `FOR KEY SHARE` batches before inserting links, closing the
retention race without loading generated SQL. Platform maintenance scans the partial
`(inserted_at, content_hash) WHERE first_linked_at IS NULL` index and deletes only
packages older than an explicit grace-period cutoff that still have no
`manifest_execution_packages` row, in bounded locked batches. A manifest link and
its foreign key therefore protect every reachable package; workspace-scoped package
purge is forbidden because packages are platform-global and may be shared.

### `manifest_execution_packages`

Required columns:

- `manifest_version_id text NOT NULL REFERENCES manifest_versions`;
- `package_hash bytea NOT NULL REFERENCES execution_packages(content_hash)`;
- `asset_module text NOT NULL`;
- `asset_name text NOT NULL`.

The primary key is `(manifest_version_id, package_hash)`. A unique index on
`(manifest_version_id, asset_module, asset_name)` prevents one indexed asset from
resolving to several packages; an index on `package_hash` supports reverse integrity
and retention work. This normalized association is also the bounded authority used
when runtime-input pins validate the exact selected asset package and resolver.
Association insertion is chunked to stay below PostgreSQL bind-parameter limits and
the transaction verifies the persisted link count before commit.

### Publication and runtime read path

Publication first asks for missing hashes, uploads only missing packages, and then
registers the compact manifest index. The manifest transaction fails closed if a
referenced package is absent or belongs to a different asset. A runtime work item is
pinned to a deployment, manifest, and asset; the orchestrator resolves the asset's
hash through a prebuilt compact index and performs one package-primary-key read joined
to the exact workspace deployment and authorized target before handing work to the
runner. Wide stages attach packages only after admission, one work item at a time,
rather than retaining every stage package in one process. The runner re-verifies the
package hash and asset identity. No execution-package collection is loaded into the
manifest cache, operator catalogue, or run snapshot.

### `workspace_deployments`

Required columns:

- `workspace_id text NOT NULL REFERENCES workspaces(workspace_id)`;
- `deployment_id text NOT NULL`;
- `manifest_version_id text NOT NULL REFERENCES manifest_versions(manifest_version_id)`;
- `configuration jsonb NOT NULL`;
- `configuration_fingerprint bytea NOT NULL`;
- `target_catalog_fingerprint bytea NOT NULL`;
- `configuration_version integer NOT NULL CHECK (configuration_version > 0)`;
- `deployed_by_actor_id text NULL`;
- `inserted_at timestamptz(6) NOT NULL`.

The primary key is `(workspace_id, deployment_id)`. A unique constraint on
`(workspace_id, manifest_version_id, configuration_fingerprint,
target_catalog_fingerprint)` makes an identical deployment retry idempotent. Unique
`(workspace_id, deployment_id,
manifest_version_id)` supports consistency-preserving run foreign keys.
`configuration` is a validated, versioned, canonical,
non-secret document. It may contain stable secret references; credentials and secret
values remain in the designated secret provider. Its fingerprint covers the exact
target selection inputs, logical resource mapping, secret-store URL, blob endpoint,
and DuckLake metadata-store identity. `target_catalog_fingerprint` separately hashes
the sorted resolved target rows for cache and integrity checks. Deployments are
immutable.

### `workspace_deployment_targets`

Required columns:

- `workspace_id text NOT NULL`;
- `deployment_id text NOT NULL`;
- `target_kind text NOT NULL CHECK (target_kind IN ('asset', 'pipeline'))`;
- `target_id text NOT NULL`;
- `selection_source text NOT NULL CHECK (selection_source IN
  ('common', 'explicit', 'dependency'))`;
- `customer_visible boolean NOT NULL`;
- `descriptor jsonb NOT NULL`, constrained to a JSON object no larger than 256 KiB;
- `inserted_at timestamptz(6) NOT NULL`.

The primary key is `(workspace_id, deployment_id, target_kind, target_id)`, with a
composite deployment foreign key. Deployment planning bulk-inserts this exact set in
the deployment transaction. Pipeline rows are allowed only when all resolved asset
members and dependencies have asset rows. Run targets and schedule cursors reference
the appropriate deployment-target identity. `descriptor` contains only bounded,
browser-safe presentation data; the authoritative target identity remains the row
key. The customer catalog index is:

```sql
CREATE INDEX workspace_deployment_targets_customer_idx
  ON favn_control.workspace_deployment_targets
  (workspace_id, deployment_id, target_kind, target_id)
  WHERE customer_visible;
```

This table is authorization and reproducibility evidence, not a mutable entitlement
system. Changing availability produces a new deployment.

### `workspace_runtime_state`

Required columns:

- `workspace_id text PRIMARY KEY REFERENCES workspaces(workspace_id)`;
- `active_deployment_id text` with composite
  `(workspace_id, active_deployment_id)` FK to `workspace_deployments`;
- `revision bigint NOT NULL CHECK (revision >= 0)`;
- `activated_by_actor_id text NULL`;
- `activated_at timestamptz(6) NULL`;
- `updated_at timestamptz(6) NOT NULL`.

This replaces an open-ended key/value runtime-settings table and the global
singleton assumption. The active manifest is resolved through the active deployment.
New workspace runtime state needs an explicit column/table and contract; truly
platform-global state needs a separately named table.

### `runs`

Required relational columns:

- `workspace_id text NOT NULL REFERENCES workspaces(workspace_id)`;
- `run_id text NOT NULL`;
- `deployment_id text NOT NULL`;
- `manifest_version_id text NOT NULL`;
- `root_execution_group_id text NOT NULL`;
- `parent_run_id text NULL`;
- `rerun_of_run_id text NULL`;
- `submit_kind text NOT NULL CHECK (...)`;
- `trigger_type text NOT NULL CHECK (...)`;
- `status text NOT NULL CHECK (...)`;
- `event_sequence integer NOT NULL CHECK (event_sequence > 0)`;
- `submitted_event_id bigint NOT NULL`;
- `latest_event_id bigint NOT NULL`;
- `snapshot_version smallint NOT NULL CHECK (snapshot_version > 0)`;
- `snapshot_hash bytea NOT NULL`;
- `snapshot jsonb NOT NULL`;
- `inserted_at`, `updated_at`, and nullable `terminal_at`.

The primary key is `(workspace_id, run_id)`. A composite foreign key from
`(workspace_id, deployment_id)` to `workspace_deployments` prevents cross-workspace
deployment use. `manifest_version_id` is pinned redundantly for queryability and must
match the deployment through a composite uniqueness/FK constraint. Required workspace
foreign keys cover the root group, parent, rerun source, submitted event, and latest
event identities. The manifest release FK is global by design. Circular run/event
and root-group constraints are `DEFERRABLE INITIALLY DEFERRED`.

Required indexes:

```sql
CREATE INDEX runs_recent_idx
  ON favn_control.runs (workspace_id, latest_event_id DESC, run_id DESC);

CREATE INDEX runs_platform_recent_idx
  ON favn_control.runs (latest_event_id DESC, workspace_id, run_id DESC);

CREATE INDEX runs_manifest_recent_idx
  ON favn_control.runs
  (workspace_id, manifest_version_id, latest_event_id DESC, run_id DESC);

CREATE INDEX runs_group_children_idx
  ON favn_control.runs
  (workspace_id, root_execution_group_id, submitted_event_id DESC, run_id DESC);

CREATE INDEX runs_active_idx
  ON favn_control.runs (workspace_id, status, run_id)
  WHERE status IN ('pending', 'running');

CREATE INDEX runs_workspace_status_recent_idx
  ON favn_control.runs
  (workspace_id, status, latest_event_id DESC, run_id DESC);

CREATE INDEX runs_platform_status_recent_idx
  ON favn_control.runs
  (status, latest_event_id DESC, workspace_id, run_id DESC);

CREATE INDEX runs_parent_idx ON favn_control.runs (workspace_id, parent_run_id)
  WHERE parent_run_id IS NOT NULL;
```

`runs_recent_idx` serves customer/workspace history. `runs_platform_recent_idx`
serves the bounded cross-workspace consultant overview; customer query plans must
never select it through an unscoped query contract.
The two status/recent indexes serve the same keyset ordering when `PageRuns` includes
a status filter. `runs_active_idx` remains the smaller operational lookup for active
run coordination and is not a history-page index.

The circular run/event constraints are added after both tables exist and are
deferrable. Root rows set `root_execution_group_id = run_id`; the root reference is
also deferred so the self-reference is validated at commit.

No index is added to `snapshot`; query features must first get an explicit relational
access path.

### `run_targets`

Required columns:

- `workspace_id` FK;
- `run_id` FK;
- `deployment_id` FK;
- `manifest_version_id` FK;
- `target_kind` (`asset` or `pipeline`);
- deterministic `target_id`;
- `target_module`, nullable `target_name`;
- `is_primary boolean NOT NULL`;
- `submitted_event_id bigint NOT NULL REFERENCES run_events DEFERRABLE INITIALLY
  DEFERRED`;
- PK `(workspace_id, run_id, target_kind, target_id)`.

Asset memberships include every node in the immutable execution plan, not only the
asset refs selected by the submitter. The run snapshot retains the submitted
`target_refs`, while `is_primary` marks its primary asset. This distinction lets
upstream dependencies participate in materialization lineage and runtime-input
pinning without making them customer-selected assets.

`(workspace_id, deployment_id, target_kind, target_id)` has a composite foreign key
to `workspace_deployment_targets`. This is the database-level guard that prevents a
run from executing an asset outside the workspace's pinned deployment catalog.

Target history index:

```sql
CREATE INDEX run_targets_history_idx
  ON favn_control.run_targets
  (workspace_id, deployment_id, target_kind, target_id,
   submitted_event_id DESC, run_id DESC)
  INCLUDE (is_primary);
```

Target history is ordered by run submission, not by a delimiter-based text field or
mutable activity time. Current status is joined from `runs`.

### `run_events`

Required columns:

- `workspace_id text NOT NULL REFERENCES workspaces(workspace_id)`;
- `event_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY` (identity only,
  never a commit-order replay cursor);
- `run_id text NOT NULL` with composite `(workspace_id, run_id)` FK to `runs`;
- `sequence integer NOT NULL CHECK (sequence > 0)`;
- `event_type text NOT NULL`;
- `entity_type text NOT NULL CHECK (entity_type IN ('run', 'step'))`;
- nullable `asset_step_id`, `status`, and `stage`;
- `occurred_at timestamptz(6) NOT NULL`;
- `payload_version smallint NOT NULL`;
- `event jsonb NOT NULL`;
- `event_hash bytea NOT NULL`;
- `outbox_event_id bigint NOT NULL UNIQUE` with composite
  `(workspace_id, outbox_event_id)` FK to `outbox_events`;
- unique `(workspace_id, event_id)` for composite references;
- unique `(workspace_id, run_id, sequence)`.

Required indexes:

```sql
CREATE INDEX run_events_run_cursor_idx
  ON favn_control.run_events (workspace_id, run_id, sequence);

CREATE INDEX run_events_type_cursor_idx
  ON favn_control.run_events (workspace_id, run_id, event_type, sequence);

CREATE INDEX run_events_step_cursor_idx
  ON favn_control.run_events (workspace_id, run_id, asset_step_id, sequence)
  WHERE asset_step_id IS NOT NULL;
```

Run-scoped history uses `(run_id, sequence)`. Durable global/group tailing and SSE
replay use the outbox `publication_id`, then join the referenced run event. A raw
identity value must not be exposed as a tail cursor: PostgreSQL can allocate ID 10,
commit ID 11 first, and make ID 10 visible later.

`create_run` preallocates its event and outbox row identities, constructs both
canonical records, then inserts outbox, run, initial available ownership, targets,
and event in one transaction. Identity gaps after rollback are expected and
harmless because these identities are not replay watermarks.

### Run transition algorithm

`commit_transition` executes one short transaction:

1. Lock the run row `FOR UPDATE`.
2. For owner-bound work, lock the matching `run_ownerships` row `FOR UPDATE` and
   validate owner, generation, and `expires_at > clock_timestamp()`. Every
   code path uses this run-then-ownership lock order.
3. Assert the command's expected sequence/version/from-status. The orchestrator has
   already validated domain transition policy; persistence enforces the atomic
   precondition and structural constraints.
4. If the requested sequence is already stored, compare snapshot and event hashes.
   Return the persisted result only for an exact replay; reject conflicting content.
5. Preallocate non-ordering event/outbox identities and insert the outbox event.
6. Insert the run event using its event identity and the outbox identity.
7. Update the run row with `WHERE event_sequence = expected_sequence` while the
   ownership lock still protects the validated fence.
8. Commit and return the canonical snapshot, persisted event, IDs, and replay flag.
9. Wake the outbox sequencer only after commit. Public live events are broadcast
   after publication sequencing; a wake-up failure never rolls back persisted truth.

The transaction never reads sibling runs/windows and never updates a group summary
or target status directly.

### `runtime_input_pins`

Key `(workspace_id, run_id, node_key_hash)`. Store `payload_fingerprint`, the exact
`execution_package_hash` foreign key, `resolver_module`, `encryption_key_version`,
protected `payload bytea`, and timestamps. The run's selected asset must be linked to
that package by `manifest_execution_packages`, and the package must declare the same
resolver. Exact replay compares the package and resolver as well as the payload
fingerprint, so a changed SQL package cannot silently reuse an old resolution. Bulk
pinning is all-or-conflict. Encryption keys live outside PostgreSQL and support
rotation by key version. PostgreSQL retains only a compact used-version inventory;
readiness fails when the external keyring omits a referenced version. A fingerprint
over protected or low-entropy input is a
versioned keyed HMAC, not a plain dictionary-attackable hash.

## Multi-node execution ownership

### Instance identity

Each orchestrator boot has a unique `instance_id` containing a stable deployment
identity plus random boot identity. It is diagnostic, not authority. Authority comes
from database fencing tokens.

### `run_ownerships`

One row per workspace-owned active/recoverable run, created atomically with the run:

- PK `(workspace_id, run_id)` with composite FK to `runs`;
- `owner_instance_id text`;
- `generation bigint NOT NULL`;
- `status text`;
- `claimed_at`, `heartbeat_at`, `expires_at`;
- `version bigint`.

Initial status is `available` with generation zero. Exact claim locks the row and
increments `generation`. Recovery workers atomically claim bounded batches where
status is available or an owned lease is expired, ordered by eligibility time and
run ID, using `FOR UPDATE SKIP LOCKED`. They do not retain an identity cursor, so a
late commit cannot fall permanently behind a watermark.

Takeover is allowed only after expiry or an explicit safe release. Every owner-bound
run write includes the generation. Heartbeat never extends a mismatched generation.
The required partial eligibility index covers
`(workspace_id, status, expires_at, run_id)` for available/owned rows. Terminalization
removes the row from recovery eligibility; retention later purges it.

### `runner_executions`

This is an attempt/dispatch ledger, not the current run snapshot. Required identities
include `execution_id`, unique `dispatch_id`, run, step, attempt, run-owner
generation, runner reference, status, deadline, cancellation outcome, and lifecycle
timestamps. Runner metadata is canonical bounded JSONB.

Persist dispatch intent before calling the runner. A crash between runner acceptance
and the acknowledgement update is an explicit uncertain execution recovered by
dispatch identity; it is not silently resubmitted.

Automatic run recovery is fail-closed. A fresh run with no execution-ledger row and
a run at an explicit durable retry checkpoint may resume. If an execution may have
been accepted, or completed history exists without a durable continuation position,
recovery attempts bounded cleanup and terminalizes the run as
`uncertain_runner_recovery`; it never submits the work again merely because a
restarted runner reports that the execution ID is absent.

Default run-specific recovery pages filter `terminal_at IS NULL` and use the partial
`runner_executions_run_active_page_idx` on
`(workspace_id, run_id, runner_execution_id)`. Explicit history pages use the full
`runner_executions_run_page_idx`; owner and workspace active pages have corresponding
partial keyset indexes. Performance contracts capture the actual Ecto page queries
and require PostgreSQL to select these indexes with mostly-terminal history.

## Distributed scheduler

### `schedule_cursors`

Natural key
`(workspace_id, deployment_id, pipeline_target_id, schedule_id)`. Store schedule
fingerprint, activation state, next due time, last evaluated occurrence, cursor
version, claim owner/generation/expiry, and updated time.

Manifest deployment receives normalized schedule rows from orchestrator logic and
atomically:

- activates matching rows;
- marks changed fingerprints `needs_review` according to product policy;
- retires rows absent from the new manifest;
- updates `workspace_runtime_state`;
- emits outbox/audit events.

### `schedule_occurrences`

Unique identity is derived from deployment, schedule identity, and scheduled time.
Rows represent persisted intent with statuses such as `pending`, `dispatching`,
`dispatched`, and `failed`. Store claim generation/expiry, deterministic run command
identity, linked run ID, attempts, and bounded error evidence.

Due evaluation and dispatch are separate:

```text
claim due cursor
  -> orchestrator computes missed-run/overlap/window policy
  -> commit unique occurrence intent + advance cursor
  -> claim pending occurrence
  -> idempotently create run
  -> link run and complete occurrence
```

Crash at any arrow is recoverable. More than one scheduler node may run; row claims
and occurrence uniqueness prevent duplicate authoritative runs.

## Execution admission

### Capacity model

`capacity_scopes` uses PK `(workspace_id, scope_kind, scope_key)` and stores
`capacity_limit`, `active_count`, and version. Legal kinds are explicit
(`workspace_global`, `pool`, `run` or the final domain names accepted by the
orchestrator). A future platform-wide limit is a separately named platform scope,
not a workspace row with nullable/magic identity.

Admission locks every requested scope row in the same canonical
`(workspace_id, kind, key)` order, checks limits, increments counters, inserts the lease
and scope memberships, and deletes the matching waiter in one transaction. This
deterministic order is mandatory to reduce deadlocks.

When a configured limit decreases below the active count, current work continues and
new admission blocks until the count falls below the new limit.

### Lease lifecycle

`execution_leases` stores workspace ID, immutable lease identity, run/step/attempt,
owner generation, active/terminal status, and expiry. `execution_lease_scopes` has
workspace-preserving FK memberships to every capacity scope.

Release/expiry locks the lease first. Only an active lease can decrement counts, and
it does so exactly once. Expiry and cleanup claim bounded batches with `SKIP LOCKED`.
Counter reconciliation compares active lease memberships to `active_count`; repair
is explicit and observable.

### Waiter queue

`admission_waiters` stores workspace ID, deterministic waiter identity,
run/step/stage/attempt, priority, enqueue time, blocked scope columns, requested
scopes in bounded canonical JSONB, deadline, wake generation, and claim lease
fields.

Waiter selection uses `(priority DESC, inserted_at, waiter_id)` and
`FOR UPDATE SKIP LOCKED`. `SKIP LOCKED` is used only for queue consumers, never for a
general consistent read.

## Materialization and freshness

### Separate claim from fact

`materialization_claims` is expiring coordination state. `materializations` is the
immutable ledger of successful logical materializations. This prevents purging a
stale lease record from deleting the evidence used to rebuild freshness.

`materializations` includes:

- workspace ID;
- generated `materialization_id`;
- workspace-scoped unique logical `claim_key`/identity hash;
- manifest and asset target identities;
- freshness key/version and input fingerprint;
- run, step, runner execution, and source event identities;
- bounded output/row evidence;
- completion timestamp and payload version.

Finishing a claim verifies generation, updates the claim, inserts the immutable
ledger row for success, and writes outbox in one transaction. A conflicting success
for the same logical identity returns the existing materialization if content is
identical and rejects otherwise.

### Freshness projection

`asset_freshness_states` is keyed by
`(workspace_id, deployment_id, asset_target_id, freshness_key)` and stores latest
attempt, latest success, current version/fingerprint, status, and
`source_publication_id`.

It also stores a normalized/hash form of `latest_success_node_key` with an index for
exact batch lookup. No freshness API may scan every asset state and filter in Elixir.

Freshness is eventually projected from the materialization ledger. Runtime
correctness does not assume zero projection lag: materialization claiming checks the
authoritative claim/materialization identity before executing duplicate work.

## Backfill model

### `backfills`

One row per accepted backfill with run/group identity, manifest, target, requested
range, window kind/timezone, refresh policy, status, expected window count, expected
batch count, canonical plan hash, inserted window count, creator, command identity,
timestamps, and version. Legal states distinguish `planning` from dispatchable work.

### `backfill_plan_batches`

Window planning is resumable so a 100,000-window request does not require one large
transaction. The orchestrator divides the canonical plan into batches of at most
500 windows. Each immutable receipt stores `(backfill_id, batch_number)` as its
workspace-local identity plus row count, first/last window identity, and a SHA-256 hash
of the canonical batch. The PK is `(workspace_id, backfill_id, batch_number)`.

`append_plan_batch` locks the planning header, inserts the receipt and windows in
one transaction, and increments `inserted_window_count` only for a new exact batch.
An exact batch replay is success; the same batch number with different content is a
conflict. Batches and windows cannot change after insertion.

`activate_plan` locks the header, loads at most the declared bounded batch-receipt
set, verifies contiguous batch numbers, exact total count, and the overall hash of
the ordered receipt hashes, then marks the backfill dispatchable and writes outbox.
Planning crashes resume from receipts. Stale incomplete plans are visible in
diagnostics and removed only by explicit retention/reconciliation.

### `backfill_windows`

Each window has a deterministic ID and unique
`(workspace_id, backfill_id, pipeline_target_id, window_key)`. Store exact start/end
timestamps, status, attempt count, latest child run, last success, dispatch claim
generation and expiry, error summary, timestamps, and version.

Required indexes:

```sql
CREATE INDEX backfill_windows_page_idx
  ON favn_control.backfill_windows
  (workspace_id, backfill_id, window_start_at, window_id);

CREATE INDEX backfill_windows_status_page_idx
  ON favn_control.backfill_windows
  (workspace_id, backfill_id, status, window_start_at, window_id);

CREATE INDEX backfill_windows_dispatch_idx
  ON favn_control.backfill_windows
  (workspace_id, status, window_start_at, window_id)
  WHERE status IN ('pending', 'retry_pending');
```

Window transitions update one row and append outbox. Progress/finalization workers
consume those events in order, maintain `backfill_overviews`, and terminalize the
parent when no nonterminal window remains. A partial index supporting the existence
check is required. This makes child transition latency independent of total windows
and avoids a hot synchronous progress row.

`asset_window_states` is keyed by workspace, deployment, asset target, and window identity
and guarded by `source_publication_id`. It is rebuilt from authoritative
window/materialization facts.

## Durable outbox and projections

### Why an outbox

Derived state and notifications must not be updated by an application-side
read/compare/write after the authoritative transaction. That creates stale-write
races and makes a committed transition depend on expensive projection work.

`outbox_events` provides durable, ordered, at-least-once delivery:

- `workspace_id text NOT NULL REFERENCES workspaces(workspace_id)`;
- `outbox_event_id bigint identity PRIMARY KEY` as an internal row identity;
- nullable `publication_id bigint CHECK (publication_id > 0)` assigned uniquely only
  after the creating transaction is visible;
- `topic`, `event_type`, aggregate type/id;
- `payload_version`, bounded `payload jsonb`;
- business `occurred_at` plus database-generated `inserted_at`;
- correlation/command IDs with no secrets.

Required unique `(workspace_id, outbox_event_id)` supports workspace-preserving child
foreign keys while `outbox_event_id` remains the global internal identity.

Every event is inserted in the same transaction as its authoritative mutation.
Required indexes cover unsequenced rows and `(topic, publication_id)` for published
rows.

```sql
CREATE INDEX outbox_unsequenced_idx
  ON favn_control.outbox_events (outbox_event_id)
  WHERE publication_id IS NULL;

CREATE UNIQUE INDEX outbox_publication_idx
  ON favn_control.outbox_events (publication_id)
  WHERE publication_id IS NOT NULL;

CREATE INDEX outbox_topic_publication_idx
  ON favn_control.outbox_events (topic, publication_id)
  WHERE publication_id IS NOT NULL;

CREATE INDEX outbox_workspace_topic_publication_idx
  ON favn_control.outbox_events (workspace_id, topic, publication_id)
  WHERE publication_id IS NOT NULL;
```

### Commit-safe publication sequencing

A PostgreSQL identity/sequence value is not commit order. Transaction A can allocate
10, transaction B can allocate and commit 11, and A can commit later. Advancing a
projector or SSE cursor to 11 would then skip 10 permanently. Storage V2 therefore
never tails `event_id`, `log_id`, or `outbox_event_id` directly.

`outbox_publication_state` has exactly one row containing the latest transactional
publication ID plus sequencer owner, lease generation/expiry, and updated time. Any
node may take over an expired lease. The initial release retains published outbox
rows indefinitely, so it does not expose a retained-history watermark or
expired-cursor state yet. A sequencing batch:

1. locks this singleton row and validates the worker generation;
2. selects and locks up to 1,000 currently visible unsequenced outbox rows in
   `outbox_event_id` order;
3. assigns a contiguous range of `publication_id` values while advancing the
   ordinary row-backed counter;
4. commits row assignments, counter, and lease renewal together;
5. publishes wake-ups only after commit.

Only already committed input rows are visible to the sequencing transaction. The
identity orders rows that are visible together; it is not itself exposed as a replay
cursor. Mutations of one aggregate serialize before inserting their outbox rows, so
their identities preserve aggregate-version order even when application business
timestamps move backwards. The counter update is transactional rather than a
PostgreSQL sequence, so rollback publishes neither IDs nor a cursor advance. A row
whose creating transaction commits late simply receives a later publication ID. The
singleton is a batched publication serialization point, not a lock in authoritative
run transactions; its throughput and lag are explicit production gates.

The sequencer may use one lease owner at a time. `SKIP LOCKED` is unnecessary for
publication ordering and must not be used to let multiple sequencers assign ranges
concurrently. Raw outbox identities remain internal diagnostics, never public replay
cursors.

### Projector ownership

Each projector has one `projection_cursors` row containing last publication ID,
worker lease owner, lease generation/expiry, status, and updated time. Any node may
claim an expired projector. Only the matching generation may apply a batch and
advance the cursor.

Initial projectors are single ordered consumers per projection type. This is
intentional: it gives deterministic cross-run target/group ordering and permits
efficient bulk reduction. Before partitioning a projector, its partition key and
cross-key invariants must be documented and load evidence must show the need.

### Batch transaction

For each claimed projector:

1. Read at most the configured batch after its publication cursor for its topics.
2. Batch-load all required existing projection rows and memberships.
3. Reduce ordered events through orchestrator-owned pure projection functions.
4. Bulk upsert changed rows with `source_publication_id` guards.
5. Advance the cursor and renew the worker lease in the same transaction.
6. Commit, then publish local/distributed UI notifications.

At-least-once reprocessing is safe because the cursor transaction and global
`source_publication_id` guards make older/equal evidence a no-op.

Every event and projection key carries workspace ID. A global projector may batch events
for several workspaces, but reduction, existing-row loads, conflict targets, and
upserts group by workspace; no reducer state is shared across workspace keys.

### Failure behavior

A malformed/internal event records a redacted `projection_failures` row and blocks
that projector rather than silently skipping state. Readiness becomes degraded, not
dead. An operator can retry after a fix or start a scoped rebuild. Failure records
are bounded and never include raw secrets/payloads.

### Projection lag

Diagnostics expose event-count and wall-clock lag per projector. Initial thresholds:

- healthy: below 1,000 events and 5 seconds;
- degraded: above either threshold for two consecutive checks;
- not ready for operator traffic: blocked projector or lag beyond a configured hard
  threshold;
- authoritative command readiness remains separate so UI lag does not unnecessarily
  stop safe run terminalization.

### Repair

Each projection defines:

- authoritative source tables;
- stable rebuild ordering/cursor;
- scoped delete/upsert semantics;
- online rebuild strategy;
- verification query comparing counts/watermarks.

Rebuilds write to a generation or shadow table where replacement would expose
partial results. The rebuilt generation records the publication high-watermark from
its consistent source snapshot; committed-but-unsequenced events receive later
publication IDs and replay safely. A global rebuild is never launched from a read
request.

## Compact operator read models

### `execution_group_overviews`

One row per workspace/root group containing only:

- root/group IDs and manifest;
- root/current status and trigger type;
- child count and per-status counts;
- active/failure flags;
- first/last activity IDs/timestamps;
- total/terminal/failed window counts when applicable;
- bounded first/latest failure identity, not a failure list;
- `source_publication_id` and projection generation.

It must not contain all child IDs, all targets, all windows, attempts, or event
timelines. Child/window/failure collections have separate cursor queries.

### `backfill_overviews`

Contains per-status window counters, remaining count, completion state, and watermark.
It is repairable from `backfill_windows`.

### `target_statuses`

Natural key `(workspace_id, deployment_id, target_kind, target_id)`. Store current
run lifecycle status, latest run/event identity and time, and globally comparable
`source_publication_id`. Detailed success, failure, materialization, and freshness
facts remain in their owning authoritative tables and dedicated projections.

Upserts use:

```sql
ON CONFLICT (...) DO UPDATE
SET ...
WHERE target_statuses.source_publication_id < EXCLUDED.source_publication_id
```

The projector processes run events in publication order and batch-loads run/event
context. It bulk-upserts a run's affected targets only when the run lifecycle status
changes; step events that leave the run status unchanged do not rewrite every target.
Lifecycle fan-out is one set-based statement, with no per-target get-plus-put loop.

## Query and pagination rules

- Keyset pagination is mandatory for runs, events, logs, windows, groups, targets,
  audits, manifests, recovery, retention, and repairs.
- Cursor order always has a unique stable tie-breaker.
- Every workspace-owned cursor encodes/authenticates workspace scope and is rejected when
  presented under a different `WorkspaceContext`.
- Historical page cursors may use row identity and are snapshot-relative under
  concurrent inserts. Any tail, replay, projector, or reconnect cursor uses
  `publication_id` (plus an item offset for a published batch), never a PostgreSQL
  identity sequence.
- Cursors are versioned opaque Base64url DTOs and are strictly validated. Public/API
  cursors should be authenticated to prevent arbitrary query amplification.
- Offset pagination is allowed only for explicitly shallow static lists, not
  high-growth control-plane history.
- Interactive default/max page sizes are 50/200. Internal scan maximum is 500.
- Total counts are returned only from a maintained compact projection or an explicit
  bounded count endpoint. Pages do not run `COUNT(*)` over large histories by
  default.
- Query structs enumerate legal filters/sorts. Unknown keys are rejected.
- Every index must name the corresponding `WHERE`, join, ordering, and cursor access
  pattern in its migration/module documentation.
- Every high-cardinality query gets a representative `EXPLAIN (ANALYZE, BUFFERS)`
  acceptance case.

## Log storage

`log_batches` stores one producer batch identity, producer/sequence deduplication
key scoped by workspace, content fingerprint, entry count, and `outbox_event_id`.
`append_batch` inserts the batch, one outbox event, and all `log_entries` in one
transaction. Exact producer batch replay returns the existing rows; conflicting
content is rejected.

`log_entries` uses a row-identity primary key, `(log_batch_id, batch_offset)` unique
position, canonical redacted columns, and bounded JSONB metadata. Historical pages
use ordinary keyset indexes. Required indexes initially include:

- `(workspace_id, run_id, log_id)`;
- `(workspace_id, run_id, asset_step_id, log_id)`;
- `(workspace_id, runner_execution_id, log_id)`;
- `(workspace_id, level, log_id)` only if a real operator query uses it;
- BRIN on `occurred_at` after representative plans demonstrate benefit.

Producer deduplication uses a unique partial key when both producer identity and
sequence are present. Bulk append uses `insert_all`/parameterized bulk SQL.

Live log replay uses the batch outbox `publication_id` plus `batch_offset`, encoded
as one order-preserving public `global_sequence`. It never uses `log_id` as a durable
tail cursor, so concurrent batches cannot create a late
commit hole. Outbox replay retention therefore defines the maximum reconnect age;
an older cursor receives an explicit cursor-expired response and the client resumes
with a bounded historical query.

Start unpartitioned. Native time partitioning is introduced only when measured table
size, purge/vacuum cost, or backup behavior crosses a documented threshold. A
partitioning design must preserve global cursor and producer-deduplication semantics;
it is not a cosmetic migration.

## Authentication, sessions, audit, and idempotency

### Actors and credentials

`auth_actors` stores global actor ID, display username, `normalized_username`, display
name, status, version, and timestamps. Unique normalized username is enforced by
B-tree.

`auth_workspace_memberships` stores `(workspace_id, actor_id)`, validated workspace-specific
role array, membership status, version, and timestamps. Authorization checks actor
status and membership status/roles for the resolved workspace. Roles do not live on the
global actor row.

`auth_platform_grants` stores one optional explicit grant per actor with validated
platform roles, status, version, grantor, and timestamps. Only an active actor with
an active grant can resolve `PlatformContext`. Workspace membership never implies a
platform grant, including membership in every current workspace.

`auth_credentials` stores actor FK, Argon2id encoded hash, algorithm/version metadata,
changed time, and version. Password hashing occurs before the transaction. Plaintext
passwords never enter persistence commands, logs, metadata, or audit.

### Sessions

`auth_sessions` stores session ID, SHA-256 token hash, actor, issued/expires/revoked
times, revocation reason class, and optional last-seen time. Only the newly generated
raw token is returned once before storage; lookups hash before querying.

Indexes cover unique token hash, active actor sessions, and expiry cleanup. Session
validity is checked against actor status and absolute expiry on every authenticated
boundary or through a short safe request-local lookup, not a long-lived authority
cache. Workspace authorization additionally checks current membership; a valid global
session does not grant access to every workspace.

### Audit

`auth_audit_entries` is append-only and supports bounded identity-keyset history
pages; it is not a live replay stream. It stores non-null workspace ID, principal
kind/id, action, outcome, target type/id, request/correlation ID, source IP class
when approved, redacted metadata, and occurred time. It never stores passwords,
tokens, idempotency keys, connection strings, raw errors, or unbounded request
bodies.

`auth_platform_audit_entries` is a separate append-only stream for platform grant
changes and cross-workspace operations. It stores no nullable/magic workspace ID;
affected workspace IDs, when any, are a bounded validated list in redacted metadata.
Customer contexts cannot query this stream.

### Idempotency

The unique key is
`(workspace_id, operation, principal_kind, principal_id, key_hash)`. Store request
fingerprint, state, reservation generation, bounded response status/body, resource
identity, and expiry.

Raw idempotency keys are high-entropy caller values and are stored only as a digest.
An operation fingerprint that can cover passwords, tokens, or other low-entropy
secrets uses an operation-specific nonsecret canonical form or a versioned keyed
HMAC; it is never a plain reusable secret digest.

For a database-local command the idempotency row and domain mutation share one
transaction:

1. insert the keyed record or lock the existing record;
2. reject a different request fingerprint;
3. return the bounded stored result for a completed exact replay;
4. for a new record, perform the capability mutation and outbox/audit writes;
5. store the replay DTO/resource identity and commit everything together.

A concurrent exact request waits on the unique row and observes either the complete
result or the first transaction's rollback. No committed `in_progress` hole exists
for database-only work. A lost connection around commit is resolved by repeating
this transaction and reading the same record.

A committed `in_progress` state is allowed only for an explicitly modeled external
workflow. It references a durable deterministic intent, has a lease/generation and
expiry, returns `202 Accepted`, and is reconciled rather than blindly replaying an
external effect. Expired ownership can be replaced only inside a guarded transaction.
A cleanup worker purges all expired terminal records, not only keys that are reused.

## Caching strategy

### Cache hierarchy

1. PostgreSQL constraints and indexes.
2. Compact persisted read models.
3. Request-scoped batching/deduplication.
4. Node-local ETS for explicitly safe versioned objects.
5. A distributed cache only after production evidence justifies its failure modes.

### Initial immutable-manifest caches

The implementation has three explicitly bounded node-local caches with distinct
ownership:

- PostgreSQL storage caches decoded immutable compact releases by version ID and
  content hash, bounded by entry and estimated decoded-term bytes.
- The orchestrator caches compiled `Favn.Manifest.Index` values by version ID,
  content hash, and index-format version, bounded by entry and byte budgets. A value
  larger than the budget is compiled and served without retention.
- Each runner compiles a registered release once into exact asset and SQL-relation
  lookup maps. Per-work preparation receives a small manifest handle, one selected
  asset, and only the relation metadata referenced by that execution package. A
  pipeline therefore does not scan or copy the complete manifest for every asset.
  Active runs acquire expiring, idempotent leases; eviction skips leased entries and
  rejects new registration when every cache slot is protected.

Immutable content needs no invalidation. All three caches expose bounds and pressure
counters; the runner and orchestrator caches additionally emit eviction/oversize
telemetry. A manifest registry with unbounded versions must not create unbounded BEAM
memory.

Execution packages are deliberately not in this cache. Runtime loads one package by
primary key for the selected SQL asset and attaches it only to that work item. A
package cache may be considered later only from measured repeated-fetch evidence and
must have a byte budget, content-hash identity, and telemetry; it must never turn
package collections back into per-manifest resident memory.

Completed runner executions retain only a deterministic work fingerprint for exact
submission replay. They do not retain `RunnerWork`, its execution package, or the
full manifest. This makes completed-execution retention independent of SQL package
size. Completed results, events, and logs are bounded per execution and by an
aggregate byte budget as well as count. Oversized results are compacted in the worker
before they enter the central runner mailbox, compacted again at the retention
boundary, reported in diagnostics, and old completed executions are evicted
oldest-first. Exact replay lookup occurs before manifest resolution, so retained
completed work remains replayable after its lease is released and its manifest is
later evicted. Active leased entries cannot be evicted.

The mutable `runs.snapshot` payload is capped at 4 MiB. A run's immutable plan is
written once to `run_plans` with its SHA-256 identity and a 64 MiB bound, then joined
only by execution/detail reads that require it. Transitions update the compact
snapshot without serializing the plan, so transition write cost and snapshot size do
not grow with plan size.

Each orchestrator node also applies a conservative byte budget to decoded plans held
by active `RunServer` processes. The default is 512 MiB and
`FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES` may set 64 MiB through 8 GiB. The
estimator charges four times the external-term size to account for decoded BEAM term
overhead. A newly submitted run is durable before capacity admission; when the node
budget is occupied it remains `pending`, emits pressure telemetry, and the bounded
recovery loop retries it. A single plan larger than the node budget is rejected
before persistence, avoiding a run that can never execute. Capacity is released when
the monitored run process exits, including crashes.

Persisted run snapshots retain at most 128 detailed node and asset results and carry
exact node counts plus an explicit truncation marker. Per-stage success/failure state
is tracked separately while executing, so bounding operator detail cannot change
dependency or freshness correctness. Automatic remaining-work retry is rejected for
a truncated historical result because a partial success list is not safe retry
evidence.

A wide-stage retry writes one authoritative `pipeline_retry_checkpointed` event in
place of per-node retry-scheduled events. Its bitset identifies retry nodes by
position in the pinned plan stage, using roughly one bit per planned node. The run
snapshot stores only the checkpoint sequence, stage, next attempt, and absolute retry
time. Recovery reads the single exact checkpoint event through the bounded run-event
keyset query, validates it against the pinned stage, reconstructs the retry set in
memory, and fails closed when the event is missing or corrupt. Persistence is one
transaction per retry decision while in-memory encoding remains linear in the retry
set.

The active-deployment pointer may use a very short versioned cache keyed by workspace.
Reads include the workspace runtime revision, and deployment publishes invalidation
after commit. A missed notification expires safely or is detected through revision
checks.

The exact immutable deployment-target catalog may be cached only by
`(workspace_id, deployment_id, target_catalog_fingerprint)`. Customer-visible and
execution-authorized lookups remain separate typed results; a target row from one
deployment cannot authorize another.

Runner credential/token caches are runner-owned, bounded, and expiry-aware. Their
keys include `workspace_id`, secret-store URL, logical credential name, provider
fingerprint, and relevant resource endpoint. Neither a common secret name nor an
identical token value permits a cross-workspace cache entry or SQL session.

### Forbidden authority caches

Do not cache as authoritative:

- mutable run state;
- ownership/fencing tokens;
- capacity counts, leases, or waiters;
- scheduler claims/occurrences;
- materialization claims;
- idempotency reservations;
- password/session validity;
- projection cursor ownership.

Terminal run DTOs or operator pages may receive measured short-lived caches later,
keyed by workspace plus immutable revision/event ID. Cache hit ratio, size, eviction,
stale read, and load latency must be observable before expansion.

### Cross-node invalidation

PostgreSQL `NOTIFY` may wake the sequencer after an outbox insert commits and wake
change-feed/projector/cache listeners after publication. Payloads contain only a
bounded internal wake-up/publication identity. The durable outbox/publication cursor
remains truth because notifications are transient and may be missed.

When PgBouncer transaction pooling is used, `LISTEN` uses a dedicated direct
Postgrex notification connection or is disabled in favor of polling.

## Notifications, PubSub, and SSE

- Persist first, commit, then broadcast.
- Local/distributed PubSub transports live updates but does not define durable event
  order.
- Global SSE resume uses commit-safe publication cursors from PostgreSQL. A
  run-scoped stream may use its serialized `(run_id, sequence)`; live log cursors use
  `(publication_id, batch_offset)`.
- The initial release retains global/log and run events indefinitely. Before any
  history purge is enabled, the publication state and SSE contract must gain an
  atomic oldest-retained watermark and explicit expired-cursor response.
- Every node clears versioned caches on boot and reconciles from PostgreSQL before
  serving cache-dependent reads.
- If BEAM clustering/PubSub is unavailable, each node's published-outbox change-feed
  listener still publishes locally. Missed notifications are recovered by
  publication-cursor polling.

## Migrations and schema readiness

### Pre-v1 baseline

Storage V2 starts from one canonical PostgreSQL baseline migration. Existing custom
migration lists and historical `create_if_not_exists` migrations are removed after
the reset cutover.

Versioned `Ecto.Migration` modules live under
`apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/` and are applied
in order by `FavnStoragePostgres.StorageV2.Migrations`. The schema owner runs them
as a deployment step. The runtime role has no DDL privileges and never
auto-migrates on application startup.

Development uses the repository scripts for container lifecycle and the migration
task for an existing PostgreSQL service:

```text
scripts/postgres/setup
mix favn.postgres.migrate
scripts/postgres/reset   # development/test only, explicit confirmation
```

### Migration rules

- Migrations are immutable after merge.
- Use explicit create/alter operations; do not mask malformed schemas with broad
  `create_if_not_exists`.
- Set migration lock and statement timeouts intentionally.
- Any large backfill is resumable application work, not one unbounded migration
  transaction.
- After the first supported production release, use expand/backfill/contract across
  compatible releases.
- Destructive changes require verified backup/restore and deployment sequencing.
- Index creation on high-cardinality live tables uses `CONCURRENTLY` in a migration
  that is safe outside a transaction when required.

### Readiness

Readiness must verify:

- database connectivity and expected server major;
- exact expected migration versions, including rejection of future versions;
- required schema, tables, columns, types, nullability, constraints, and critical
  indexes;
- runtime role permissions;
- required projector cursor rows and blocked/lag state;
- no migration currently in an incompatible partial state.

Liveness only states that the BEAM/application is alive. Readiness states whether it
can safely accept its class of traffic. Diagnostics expose redacted detail without
turning raw database errors into API output.

## Retention and maintenance

The initial production release implements bounded purge only for operational data
whose deletion cannot create gaps in canonical history or live replay. Operators
configure and run those jobs explicitly:

| Data | Default retention |
| --- | --- |
| Logs | 14 days |
| Completed idempotency records | 7 days after expiry/completion |
| Expired/revoked sessions | 30 days |
| Terminal materialization claims | 7 days after terminal state |
| Projection failure evidence | 30 days after resolution |
| Execution leases, waiters, and ownership rows | Indefinite in the initial release |
| Auth audit | Indefinite in the initial release |
| Runs/events/backfills/materializations | Indefinite in the initial release |
| Outbox events | Indefinite in the initial release |
| Manifests | Indefinite in the initial release |

Canonical retention remains a product/operator decision, but it must not be enabled
until referential deletion order and an SSE `oldest_retained_publication_id`
watermark are implemented together. Until then, database-size and table/index-growth
alerts are mandatory; retaining history is safer than creating a silent replay gap.

Purge workers:

- delete keyset-selected bounded batches;
- use `SKIP LOCKED` only when multiple purge workers are intentionally supported;
- pause under pool/replica/lock pressure;
- emit rows/bytes/duration/watermark telemetry;
- preserve parent/child FK order;
- never hold one transaction for an entire history.

Vacuum, analyze, table/index bloat, sequence consumption, database size, and oldest
retained cursor are operational metrics. Autovacuum tuning is workload evidence, not
a hard-coded application migration.

## Backup, restore, and disaster recovery

The PostgreSQL service owns physical backup/PITR. Favn owns application verification:

- documented recovery-point and recovery-time objectives;
- automated PITR/restore into an isolated database;
- schema/readiness validation after restore;
- manifest, active runtime state, run/event, auth, idempotency, ownership, and
  projection consistency checks;
- projector restart/rebuild validation;
- a controlled promotion procedure;
- periodic restore drills recorded as production evidence.

The control-plane backup does not include runner-owned DuckDB/DuckLake data. Run
records may reference data-plane systems restored to a different point; operator
runbooks must state that cross-system consistency limitation.

Read replicas are not used by initial application queries. They may be added later
only to explicitly stale-tolerant histories with replica-lag telemetry and primary
fallback. Commands, ownership, scheduler, auth, idempotency, freshness decisions,
and read-after-write flows always use the primary.

## Security and database roles

Use separate roles:

- schema owner/migrator: owns `favn_control`, used only by deployment migrations;
- runtime role: connect, usage, DML, and sequence privileges only;
- optional observer role: read-only access to approved diagnostic views, not raw
  credential/session payloads.

The runtime role has no `CREATE` privilege on the database, `public`, or
`favn_control`, has no inherited role memberships, and cannot create shadow objects.
Schema ownership remains with the migrator role. The runtime may change its
session-local search path, as any ordinary PostgreSQL login can, but that grants no
additional object authority.

Production requires TLS certificate verification, private network access, secret
rotation, and redacted configuration diagnostics. The application must not disable
TLS verification merely because TLS is enabled.

Runtime input pins use application-level authenticated encryption when they may
contain protected values. Hashes, encrypted bytes, and key version are stored;
encryption keys are not. Each record uses a unique nonce and associated data binding
the run, node key, payload version, and key version; decryption fails closed on any
mismatch. Rotation adds a new current key while retaining historical keys. Retirement
is allowed only after readiness no longer reports the version as referenced, following
an explicit pin purge or re-encryption operation.

No generic SQL console is exposed through the orchestrator. Operational queries use
curated diagnostics/views with bounded outputs.

## Observability

Every store operation emits semantic telemetry independent of Ecto query telemetry:

```text
[:favn, :persistence, :operation, :start | :stop | :exception]
```

Measurements include duration, checkout time, query count, rows returned/changed,
retry count, lock wait when available, and batch size. Metadata includes operation,
store, result/error class, and instance—not IDs or secrets with unbounded
cardinality.

Raw workspace IDs are not metric labels. Workspace-specific investigation uses
access-controlled structured traces/logs or bounded operator queries; aggregate
metrics may use bounded plan/tier classes only after those product concepts exist.

Required dashboards/alerts:

- operation latency/error/unknown-outcome rate;
- pool checkout queue and timeout rate;
- database connections versus budget;
- deadlock, serialization retry, lock timeout, and long transaction rate;
- run/scheduler/claim/lease fencing failures;
- admission queue depth/age by bounded scope kind;
- unsequenced outbox depth/age and publication-sequencer lease/throughput;
- projector lag/block/failure/rebuild progress;
- outbox growth and oldest unconsumed event;
- log/run/event table growth and retention lag;
- autovacuum/analyze age and table/index bloat;
- readiness and migration incompatibility;
- backup age and last successful restore drill.

Telemetry handlers must not execute database work synchronously.

## Testing architecture

### Unit tests

Pure state transitions, command validation, codecs, cursor encoding, error mapping,
and projection reducers run without a database. Focused fakes implement one
capability when a unit test needs an error/result seam. There is no full memory
backend conformance suite.

### PostgreSQL integration tests

Every store operation has PostgreSQL integration tests against PostgreSQL 18. Use
`Ecto.Adapters.SQL.Sandbox` for isolated transactional tests. Tests involving child
processes use explicit allowances or shared mode and finish all database work before
the owner exits.

True concurrency/locking/crash tests use multiple real connections and isolated test
schemas/databases rather than forcing competing workers through one sandbox
transaction.

PostgreSQL is a mandatory CI service. Tests never silently pass because a database
URL is missing.

### Local and CI PostgreSQL topology

Developers may supply `FAVN_DATABASE_URL` for an existing local PostgreSQL 18
instance. The repository also provides a pinned container definition and setup task
so local installation is optional and every contributor can reproduce CI.

Tests require `FAVN_DATABASE_URL`. Local runs must point it at a disposable test
database; CI supplies a job-local PostgreSQL service container and never a production
URL. The production acceptance harness separately creates a temporary least-privilege
runtime role, grants the exact runtime privileges, and starts the compiled production
artifact with that role. Migration and privilege tests intentionally retain migrator
credentials where their subject requires them.

Normal pull-request CI does **not** use a cloud database or repository secret. On a
Linux GitHub-hosted runner it starts the official PostgreSQL service container,
waits on `pg_isready`, creates ephemeral test roles/databases, runs migrations, and
destroys the service with the job. The image is pinned to the declared PostgreSQL 18
minor and digest (18.4 when this guide was drafted); dependency automation proposes
minor/digest updates, which run the full database suite before merge.

CI is divided by evidence, not by backend:

1. Database-free unit tests cover pure domain transitions, validation, codecs,
   cursor signing, and projection reducers.
2. PostgreSQL integration tests run every capability operation against the service
   container. Ordinary isolated cases use SQL Sandbox.
3. Concurrency/unknown-outcome tests use multiple actual pool connections and
   committed isolated fixtures or a dedicated non-sandbox database slice. They must
   not fake contention through one shared sandbox connection.
4. The PostgreSQL slow slice starts three separate BEAM VMs, each with its own pool,
   against the same service container. It exercises recovery and schedule partitioning,
   admission and materialization exclusion, publication sequencing, projector
   failover, node loss, lease takeover, and stale-write fencing.
5. Scale/`EXPLAIN (ANALYZE, BUFFERS)` tests run in a scheduled or explicit heavy CI
   slice with fixed seed cardinalities and recorded runner characteristics.

A managed Azure PostgreSQL environment is a separate nightly/pre-release/production
acceptance gate, not per-PR CI. It proves verified TLS, migrator/runtime role
permissions, connection budgets, provider configuration, HA/failover behavior,
PITR/restore, and production-shape query plans. It may use an ephemeral
infrastructure-as-code server or a locked-down reusable non-production server. No
release is approved until this gate and a restore drill pass, but ordinary
contributors do not need cloud credentials.

### Required concurrency cases

- Two creators reuse the same run ID with same and conflicting content.
- Two transitions race for the same expected sequence.
- A lower internal outbox identity commits after a higher identity; publication and
  replay expose both without a cursor hole, and repeated delivery remains safe.
- Concurrent identical API commands commit one domain mutation and one replay
  result; conflicting fingerprints cannot enter the mutation.
- A stale run owner attempts transition/heartbeat after takeover.
- Scheduler nodes claim the same due row and occurrence.
- Admission requests lock overlapping scopes in different input orders.
- Lease release races expiry and runs exactly one counter decrement.
- Two materialization claimants race; stale generation cannot finish.
- Two backfill workers claim/transition the same window.
- Projector dies before and after projection update/cursor advancement.
- Older target/group evidence commits after newer evidence and cannot regress state.
- Connection loss around commit resolves through command identity.

### Required workspace-boundary cases

- One global manifest release can be deployed to two workspaces without duplicating
  its content. Idempotency keys, target identities, and schedule identities remain
  independent within each workspace deployment.
- A common target is materialized into both deployment catalogs, while a selective
  target appears only in the chosen workspace. A run or schedule for the absent
  target is rejected by both the orchestrator contract and PostgreSQL foreign key.
- Dependency closure is complete, deterministic, and bulk-persisted; dependency-only
  targets execute but do not appear in customer-visible catalog reads.
- Workspace A cannot fetch workspace B by presenting a globally unique object ID.
- Composite foreign keys reject cross-workspace deployment/run/event/backfill,
  materialization, membership, and projection relationships.
- Cursor authentication rejects reuse under another workspace.
- Deployment/operator caches never return a workspace-owned value populated under
  another workspace; global manifest cache entries are safely shared and immutable.
- Global projectors process interleaved workspace events without sharing reducer keys.
- Logs, audit, SSE replay, repair, retention, and diagnostics preserve workspace scope.
- Customer actors cannot construct `PlatformContext`, page platform audit, or access
  cross-workspace run/log cursors. An explicitly granted consultant can page the
  bounded platform overview and every returned row identifies its workspace.
- Workspace A's runner context resolves only Key Vault A, Blob A, and DuckLake
  metadata PostgreSQL A. Pool/cache keys and adversarial fixtures prove no session,
  token, endpoint, or catalog reuse with workspace B.
- Scoped-query, composite-constraint, cache, cursor, and privileged-maintenance
  isolation tests pass with at least two workspaces. If RLS is enabled later, its
  connection-pool context reset receives a separate adversarial test suite.

### Query-plan and scale tests

Seed at least the declared scale envelope. For every high-cardinality query:

- capture `EXPLAIN (ANALYZE, BUFFERS)` on representative selective and worst-case
  inputs;
- assert bounded returned rows and query count;
- review scans, sorts, heap fetches, temporary spill, and buffer reads;
- avoid brittle exact cost assertions while failing on known sequential-scan or
  unbounded-sort regressions;
- verify transition work is invariant between small and 10k-child groups.

### Multi-node acceptance

Run at least three orchestrator BEAM nodes against one PostgreSQL primary and verify:

- scheduling deduplication;
- owner takeover/fencing;
- admission and materialization exclusion;
- projection failover and cursor continuity;
- publication-sequencer takeover without a skipped or duplicated durable cursor;
- local UI notification recovery from persisted cursors;
- node crash during dispatch/commit/projector batch;
- restart recovery without global unbounded scans.

The storage-authority portion is automated by
`apps/favn_storage_postgres/test/storage_v2/concurrency_authority_test.exs` in the
`:slow` CI slice using three OTP peer nodes and independent repository pools. Full
release-topology notification and dispatch crash testing remains a pre-release
deployment gate because it requires runnable split release artifacts and real runner
integrations rather than storage-only peers.

### Migration and restore tests

- empty database to current schema;
- exact readiness;
- missing column/index/constraint detection;
- future migration rejection;
- deployment with migration job then old/new compatible runtime where applicable;
- restored production-shape snapshot and missing-row projection backfill;
- backup/restore drill artifact verification.

## Development experience

PostgreSQL is the default local control plane. The repository should provide:

- a pinned PostgreSQL 18 minor/digest container definition shared with CI;
- one command to start PostgreSQL and wait for health;
- one setup/migrate command;
- one explicit destructive reset command;
- sensible local credentials excluded from production paths;
- deterministic test database creation and cleanup;
- diagnostics that distinguish server unavailable, database missing, auth failure,
  schema missing, upgrade required, and future schema.

`mix favn.dev` should use PostgreSQL by default once cutover completes. A developer
may run an existing local PostgreSQL service instead of the provided container by
setting the documented URL. Local setup creates separate migrator and runtime roles
where permission behavior is under test; test credentials are never accepted by the
production configuration path.

## Implementation module layout

Target shape (names may change only with equivalent ownership):

```text
apps/favn_orchestrator/lib/favn_orchestrator/persistence/
  backend.ex
  stores.ex
  error.ex
  registry_store.ex
  run_store.ex
  run_ownership_store.ex
  scheduler_store.ex
  admission_store.ex
  materialization_store.ex
  backfill_store.ex
  operator_read_store.ex
  log_store.ex
  identity_store.ex
  idempotency.ex
  maintenance_store.ex
  commands/
  queries/
  results/
  projections/

apps/favn_storage_postgres/lib/favn_storage_postgres/
  backend.ex
  repo.ex
  schemas/
  registry/
  runs/
  scheduling/
  coordination/
  materializations/
  backfills/
  projections/
  outbox/
  logs/
  identity/
  idempotency/
  maintenance/
  telemetry.ex
  error_mapper.ex
```

Modules are split by capability/invariant, not into generic `Queries`, `Helpers`, or
`Utils` dumping grounds. A large SQL statement may have a focused module named after
the command it implements.

Ecto schemas model row types and simple queries. Explicit parameterized SQL is
preferred when it makes locking, atomic conditional updates, `SKIP LOCKED`, bulk
upserts, or query plans clearer. The goal is typed maintainability, not eliminating
SQL.

## Implementation sequence

### Phase 0: freeze decisions and evidence

- Record this accepted architecture and reset/no-migration decision.
- Add mandatory PostgreSQL 18 dev/CI infrastructure.
- Convert current behavior tests into explicit use-case characterization tests.
- Establish benchmark seeds, query counters, and reference environment.

Exit: the new contracts and production gates are reviewable before persistence code
is written.

### Phase 1: foundation and core authority

- Add the root backend and capability facades.
- Add Favn-owned repo, configuration, error type, telemetry, canonical baseline
  migration, exact readiness, and diagnostics.
- Implement workspaces/context resolution, composite workspace constraints, global
  compact manifests, content-addressed execution packages, immutable workspace
  deployments/target catalogs/resource bindings, and per-workspace runtime state.
- Implement run creation/transition/events/targets/pins and outbox.
- Implement commit-safe outbox publication sequencing before any projector or live
  stream consumes a global cursor.
- Add bounded run/event queries and a byte-bounded compact-manifest ETS cache;
  execution packages remain database-on-demand.

Exit: run commits are atomic, fenced contract inputs exist, no projection work is in
the transition, and PostgreSQL tests are mandatory.

### Phase 2: multi-node ownership and scheduling

- Implement run ownership fencing and runner-execution ledger.
- Refactor run workers/recovery to use fences.
- Implement schedule cursor claims and occurrence intents.
- Maintain the three-node scheduling, ownership, admission, materialization,
  sequencing, projection-failover, and crash test in the PostgreSQL slow slice.

Exit: stale nodes cannot write and scheduled runs are exactly-once authoritative
effects under retries/crashes.

### Phase 3: admission and materialization

- Implement capacity scopes, leases, waiters, deterministic locking, and expiry.
- Implement materialization claims plus immutable materialization ledger.
- Add reconciliation and concurrency/load tests.

Exit: cross-node capacity and materialization exclusion are database-proven.

### Phase 4: backfills and projections

- Implement backfill headers/windows and distributed window claiming.
- Implement outbox projectors/cursors/failures.
- Implement compact group, progress, target, freshness, and asset-window projections.
- Replace unbounded operator detail with independently paged reads.

Exit: 100k-window backfills and 10k-child groups stay within work/query budgets.

### Phase 5: identity, idempotency, logs, and operations

- Implement global actors/credentials/sessions, workspace memberships, workspace audit,
  explicit platform grants/audit, and workspace-scoped API idempotency.
- Prove database-local idempotency records/results commit atomically with their
  capability mutations and model external intents separately.
- Implement bulk logs and retention.
- Implement backup/restore verification, maintenance, observability, and operator
  diagnostics.

Exit: every current control-plane feature has complete PostgreSQL persistence and an
operational lifecycle.

### Phase 6: cutover and deletion

- Make PostgreSQL the default/required production and development backend.
- Remove `Favn.Storage.Adapter`, the old PostgreSQL adapter/migrations, memory
  backend, and SQLite from the active runtime/test contract.
- Delete compatibility functions, optional callback fallbacks, stale docs, and
  obsolete tests in the same change series.
- Run full multi-node/load/restore acceptance.

Exit: one production storage architecture exists and no deprecated path can be
selected.

### Phase 7: optional SQLite evaluation

Only after every production gate passes, decide whether SQLite convenience is worth
a second implementation. The evaluation must quantify maintenance cost and state
which capabilities can be faithfully supported. It may be a reduced local product
mode; it must never be presented as evidence for PostgreSQL concurrency correctness.

## Review checklist for every implementation change

- Which capability and invariant owns this change?
- Is the operation atomic at the correct use-case boundary?
- Does it require an expected version or fencing token?
- What happens on exact retry, conflicting retry, timeout, crash, and unknown commit?
- Is workspace context explicit in the command/query, SQL predicate, relationship,
  uniqueness constraint, cursor, projection, and cache key?
- Does the pinned deployment authorize every planned/run target, and is customer
  visibility distinct from dependency execution?
- Can any endpoint, credential, catalog, or pooled session cross a workspace boundary?
- Is a platform-wide read explicit, bounded, audited, and authorized by
  `PlatformContext` rather than an omitted workspace predicate?
- Is every list/page bounded with a stable cursor?
- Does query work depend on aggregate history or sibling count?
- Which concrete access pattern justifies each new index?
- Does JSON duplicate a queryable field or hide a relationship?
- Is projection state repairable, globally versioned, and outside hot transitions?
- Are locks acquired in canonical order and held only during database work?
- Is a PostgreSQL constraint enforcing the invariant across nodes?
- Are errors normalized and secrets redacted?
- What telemetry and production diagnostic proves behavior?
- Does the test use real competing PostgreSQL connections when concurrency matters?
- Have stale memory/SQLite/old-adapter forms been deleted rather than deprecated?

## Final production acceptance

PostgreSQL Storage V2 is production-ready only when all are true:

- PostgreSQL is required in development and mandatory in CI.
- Pull-request CI provisions PostgreSQL without cloud credentials; managed-service
  acceptance is separately automated and required before release.
- The mega-adapter and full memory backend are gone.
- Auth, sessions, audit, and idempotency have full PostgreSQL implementations.
- Three-node scheduling, ownership, admission, materialization, projection, crash,
  and recovery acceptance passes.
- Stale fences and projection evidence are proven unable to regress state.
- Publication sequencing is proven not to skip late-committing identity values, and
  every live/projector cursor uses publication order.
- Concurrent API retries are atomic with their database mutations; no split
  reserve/complete crash window remains.
- Run transition query/row work is unchanged at 10 and 10,000 siblings.
- Every interactive high-growth read is cursor bounded and plan reviewed.
- Projection lag, pool pressure, lock contention, retention, and unknown outcomes are
  observable with alerts.
- The declared retention profile completes within its maintenance budget.
- Exact schema readiness detects missing, malformed, old, and future schemas.
- Migration, PITR/restore, and missing-row projection backfill drills succeed on production-shape
  data.
- PostgreSQL configuration, TLS, role permissions, connection budget, HA, backup,
  and upgrade policy are documented in the operator runbook.
- No code or documentation still claims SQLite is the production release target.
- Every workspace-owned table/access path is scoped from the baseline, and
  cross-workspace negative tests pass with at least two workspaces.
- One global manifest release can be deployed independently to two workspaces while
  materializing different exact target catalogs and preserving workspace-specific
  configuration and all operational/data-plane isolation.
- Customer UI/API users can see only customer-visible assets and runs in their active
  workspace; platform consultants require explicit grants and every cross-workspace
  result is bounded, attributed, and audited.
- Runner acceptance proves dedicated Key Vault, blob-storage, and DuckLake metadata
  endpoints, credential caches, and SQL session pools never cross workspaces.

Customers access the control plane only through authenticated Favn UI/API use cases;
direct PostgreSQL access is unsupported. Re-evaluate RLS before granting direct
database access or running less-trusted third-party code inside the control-plane
trust boundary. A contractual physical-isolation requirement is served by a separate
Favn deployment, not dynamic database routing in this baseline.

## Official implementation references

- [PostgreSQL versioning policy](https://www.postgresql.org/support/versioning/)
- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- [PostgreSQL explicit locking and deadlocks](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL row security policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [PostgreSQL `SELECT` locking and `SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html)
- [PostgreSQL sequence functions and transaction caveats](https://www.postgresql.org/docs/current/functions-sequence.html)
- [PostgreSQL `NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html)
- [Ecto SQL Sandbox](https://hexdocs.pm/ecto_sql/Ecto.Adapters.SQL.Sandbox.html)
- [GitHub Actions PostgreSQL service containers](https://docs.github.com/en/actions/tutorials/use-containerized-services/create-postgresql-service-containers)
- [PostgreSQL official container image](https://hub.docker.com/_/postgres)
- [Azure Database for PostgreSQL release notes](https://learn.microsoft.com/en-us/azure/postgresql/release-notes/release-notes)
- [Azure PostgreSQL Flexible Server infrastructure reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.dbforpostgresql/flexibleservers)
