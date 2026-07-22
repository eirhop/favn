# ADR: PostgreSQL Control-Plane Storage V2

- **Status:** accepted and implemented
- **Accepted:** 2026-07-16
- **Cutover:** [PR #491](https://github.com/eirhop/favn/pull/491)
- **Decision owner:** Favn project owner
- **Supersedes:** the SQLite-first production direction and generic storage-adapter architecture

Current implementation details live in
[`../storage/postgresql/`](../storage/postgresql/). Production procedures live in
the [`PostgreSQL operator runbook`](../production/postgresql_operator_runbook.md).
This record explains why the architecture exists and which decisions must remain
true as the implementation evolves.

## Context

Favn originally supported a generic 97-callback storage adapter, memory storage,
SQLite, and a partial PostgreSQL implementation. That design created several
sources of persistence semantics and made production behavior difficult to prove.
The pre-cutover audit also found incomplete PostgreSQL coverage, concurrency races,
unbounded operator reads, duplicated adapter logic, and transition work that grew
with sibling runs.

Favn is a consultant-operated data-platform service for an initial fleet of about
5–10 customer companies, not a public self-service SaaS. Customers use authenticated
Favn UI/API operations. They do not receive control-plane database access. Customer
analytics data and credentials require stronger physical separation than shared
control-plane metadata.

The project was private pre-v1 at cutover, so existing development data could be
discarded. This allowed one clean storage contract instead of a compatibility
migration between prototype architectures.

## Decision

### One PostgreSQL baseline

- PostgreSQL 18 is the only control-plane database for production, development,
  tests, and CI. It is kept on a supported minor release.
- Favn owns one Ecto repository and one dedicated `favn_control` schema.
- Runtime nodes validate the exact expected schema but never migrate at startup.
  Deployment runs migrations separately with a privileged migrator identity; the
  application uses a least-privilege runtime identity.
- The design requires no optional PostgreSQL extensions.
- Existing private-development persistence was reset at cutover. Any future data
  migration is a separately designed project, not compatibility code in the stores.

### Explicit workspace isolation

- One customer company maps to one workspace initially. The schema permits more
  workspaces later without introducing organization, billing, or plan abstractions.
- Every customer command and query carries an explicit validated workspace context.
  Cross-workspace consultant operations require a separate authorized platform
  context and bounded, attributable reads.
- Workspace-owned relationships use workspace-aware keys and constraints so the
  database rejects cross-workspace references.
- Platform-global immutable manifest releases and execution packages may be shared.
  Each workspace deployment persists the exact common, customer-specific, and
  dependency targets that workspace may execute and the subset customers may see.
- Customer business data, blob accounts, DuckLake metadata databases, warehouses,
  secret stores, and SQL sessions remain outside the shared control-plane database
  and are isolated per customer.

PostgreSQL row-level security is not part of the initial trust model because
customers never access the database and platform operations intentionally span
workspaces. Direct database access or less-trusted control-plane code requires a
new security decision. Contractual physical control-plane isolation requires a
separate Favn deployment.

### Capability contracts, not a database abstraction

- `favn_orchestrator` owns domain commands, bounded queries, contexts, results,
  errors, authorization, and projection transformations.
- `favn_storage_postgres` implements those contracts and owns Ecto schemas,
  migrations, SQL, transactions, locks, constraints, and database diagnostics.
- Persistence is divided into focused capability stores. No module implements the
  former mega-adapter, and no generic `execute/query` escape hatch is exposed.
- Contracts represent atomic use cases and bounded reads, not table CRUD.
- Ecto is used for ordinary typed composition. Focused parameterized SQL remains
  appropriate where locks, conditional updates, `SKIP LOCKED`, bulk operations, or
  query plans must be explicit.

The dependency points inward: the PostgreSQL app depends on orchestrator-owned
contracts. The orchestrator does not compile against the concrete PostgreSQL app;
the deployment composition root selects and starts the backend.

### Durable authority and coordination

- Run snapshots plus append-only events are authoritative. Favn is not fully event
  sourced.
- An authoritative mutation and every required outbox event commit in one database
  transaction. API command identity and result commit with the mutation so exact
  retries replay and conflicting reuse is rejected.
- Database sequences identify rows; they do not prove transaction commit order. A
  durable post-commit sequencer assigns publication order before projectors or live
  replay advance persisted cursors.
- Multi-node ownership, execution, scheduling, admission, materialization,
  projection, and recovery use database constraints, leases, claims, and monotonically
  increasing fencing tokens. Node memory, process registration, PubSub, and
  notifications are never correctness authorities.
- Locks use a deterministic order and remain inside short database transactions.
  Stale owners cannot renew, complete, or mutate work after losing a fence.
- Operator summaries are versioned repairable projections. Hot run transitions do
  not scan execution-group siblings, backfill windows, or projection histories.

### Bounded manifests, execution, and reads

- Compact manifest indexes are stored separately from immutable SHA-256-addressed
  SQL execution packages. Packages are uploaded before a manifest that references
  them; runtime fetches only the selected authorized package.
- A run stores one immutable execution plan. Mutable snapshots retain its identity
  and hash rather than rewriting the plan on every transition.
- Active execution leases the pinned runner manifest. Complete SQL preflight occurs
  once for the planned scope; individual work does not rescan the full plan.
- `RunManager` is only a bounded in-memory coordinator. Database I/O and crash
  terminalization run in caller or supervised worker processes so one slow query
  cannot serialize its mailbox.
- Pipeline retry recovery persists one compact stage checkpoint rather than one
  mutable snapshot field or retry-scheduled event per node.
- Growing histories use stable keyset cursors and declared page, batch, and payload
  limits. Queryable lifecycle fields remain typed scalar columns; JSONB is reserved
  for bounded versioned values read as a unit.
- Cache correctness never depends on invalidation. Node-local caches contain only
  bounded, versioned accelerator state. PostgreSQL `NOTIFY` and local PubSub are
  wake-ups; durable rows and cursors provide replay correctness.

## Required invariants

Future storage changes must preserve these properties:

| Property | Invariant |
| --- | --- |
| Atomicity | State, audit/idempotency outcome, and required outbox effects commit at the owning use-case boundary. |
| Fencing | A stale owner cannot renew, mutate, or complete reused work. |
| Retry safety | Exact command retries return the committed result; conflicting identity reuse is rejected; unknown outcomes are resolved by rereading with the original identity. |
| Bounded work | Hot-transition cost does not grow with siblings, windows, or history; all high-growth reads and maintenance operations are bounded. |
| Workspace isolation | Context, predicates, relationships, uniqueness, cursors, projections, and cache keys retain workspace ownership. |
| Repairability | Derived projections can be rebuilt from authoritative rows without stopping ordinary writes. |
| Durable replay | Notifications may be lost without losing authoritative events or advancing a cursor past unprocessed work. |
| Schema authority | Runtime accepts exactly the supported migration set, constraints, indexes, identifier shapes, and payload versions, and rejects old or future schemas. |
| Secret safety | Raw passwords, session tokens, idempotency keys, runtime secrets, and unsafe Erlang terms are never persisted. |
| Operational evidence | Concurrency, query plans, privileges, migration, restore, and failure behavior are tested against real PostgreSQL. |

## Design envelope

These cardinalities guided the schema and query design. They are engineering test
inputs, not customer quotas:

| Dimension | Design cardinality |
| --- | ---: |
| Manifest versions | 10,000 |
| Runs | 1,000,000 |
| Run events | 20,000,000 |
| Runs in one execution group | 10,000 |
| Windows in one backfill | 100,000 |
| Targets in one run | 10,000 |
| Log entries before retention | 100,000,000 |
| Concurrent run-transition writers | 100 |
| Concurrent child completions in one group | 100 |

One run transition must remain `O(1)` in aggregate history and sibling count. Work
may be `O(T)` only for that run's own bounded target set and must use bulk
operations. Interactive pages return at most 200 rows; internal scans normally
process at most 500. Multi-node database coordination must remain safe even though
the first supported release target is one control-plane node, one separate runner
node, and PostgreSQL.

## Consequences

### Benefits

- Development and CI exercise the same transaction, constraint, query-plan, and
  concurrency semantics used in production.
- One persistence contract eliminates compatibility branches and makes failures,
  retries, and unknown outcomes explicit.
- Database-enforced workspace ownership, claims, leases, and fences remain correct
  across process and application-node failure.
- Shared immutable manifests avoid copying common analytics definitions while exact
  deployments and separate data planes preserve customer boundaries.
- Bounded queries and repairable projections support predictable operator reads
  without putting derived work into authoritative transitions.

### Costs

- A full local runtime requires PostgreSQL; database-free integration behavior is
  intentionally unavailable.
- Deployment requires an explicit migration job, runtime grants, monitoring,
  backup, restore, and capacity management.
- Focused capability contracts and concurrency SQL create more named modules than
  generic CRUD.
- Conservative retention keeps canonical history until safe referential and SSE
  watermarks are designed and proven.
- Application workspace scoping and negative isolation tests remain mandatory
  because the baseline does not use database/schema-per-customer or RLS.

## Rejected or deferred alternatives

- **SQLite as a production or equivalence backend:** rejected for the initial
  release because it cannot prove PostgreSQL concurrency behavior and would restore
  two sources of semantics. A smaller development-only mode may be reconsidered
  only from measured need and must adapt to these contracts.
- **Full memory persistence:** removed. Pure domain tests use values or focused
  fakes; persistence behavior uses PostgreSQL.
- **Generic repository or mega-adapter:** rejected because atomic domain commands,
  locking, errors, and boundedness become implicit or optional.
- **Database-per-workspace, schema-per-workspace, or sharding:** unnecessary for the
  initial small fleet and would complicate platform-wide operation. A separate
  deployment remains available for physical-isolation requirements.
- **Full event sourcing:** rejected. Snapshots and authoritative relational state
  remain necessary; events support history, replay, and projections.
- **Redis, read replicas, or authoritative node caches:** deferred until measured
  scale requires them. None may become necessary for correctness.
- **Automatic runtime migrations:** rejected because production rollout requires
  observable, privileged, independently reversible migration work.
- **Migrating prototype data:** rejected at the pre-v1 cutover. Compatibility with
  removed rows and callbacks would permanently weaken the new baseline.

## Migration outcome

PR #491 completed the architectural cutover:

- PostgreSQL became mandatory for the control plane and local integration paths;
- capability-specific orchestrator contracts and PostgreSQL stores replaced the
  generic adapter;
- the memory and SQLite backends, legacy PostgreSQL schema, compatibility callbacks,
  and obsolete tests were removed;
- the `favn_control` schema, exact readiness, migration/grant/provisioning tasks,
  workspace model, durable coordination, outbox/projections, bounded reads, and
  live-PostgreSQL tests became the baseline.

The storage architecture is implemented, but implementation does not by itself
prove a supportable production release. Release artifacts, managed-provider
restore/PITR/load evidence, telemetry, operator workflows, asynchronous submission,
and data-plane recovery are tracked in [`../ROADMAP.md`](../ROADMAP.md) and
[`../production/README.md`](../production/README.md).

## Evolution rules

A change that weakens or replaces one of these decisions requires a new or
superseding ADR. Ordinary implementation changes update the owning current document:

- [`../storage/postgresql/architecture.md`](../storage/postgresql/architecture.md)
  for runtime ownership, write/read paths, caches, and recovery;
- [`../storage/postgresql/data-model.md`](../storage/postgresql/data-model.md) for
  tables, relationships, and modeling rules;
- [`../storage/postgresql/testing.md`](../storage/postgresql/testing.md) for test
  tiers and PostgreSQL setup;
- [`../structure/favn_storage_postgres.md`](../structure/favn_storage_postgres.md)
  for code ownership and locations;
- [`../production/postgresql_operator_runbook.md`](../production/postgresql_operator_runbook.md)
  for configuration, migrations, privileges, backup, restore, monitoring, incidents,
  and upgrades.
