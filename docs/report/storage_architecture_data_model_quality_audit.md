# Storage Architecture, Data Model, and Production Quality Audit

Date: 2026-07-16
Audited commit: `69937d5afd4d581e7d6bae7a7a445c9c884678d7`
Audit worktree: `/home/eirik/code/favn-storage-audit`
Audit branch: `codex/storage-audit`

## Post-audit architecture decision

The SQLite-first assumption used during this audit has been superseded.
PostgreSQL is now the sole initial Storage V2 backend for production,
development, tests, and CI. The normative implementation design is
`docs/architecture/postgresql-control-plane-storage-v2.md`. This report remains
the evidence for the redesign: its correctness, complexity, contract-size, and
operational findings still apply to the legacy implementations.

## Executive verdict

Favn's storage code is not uniformly "AI slop." Its core persistence semantics are
better than that description suggests: the application boundary is explicit, run
snapshot and event writes are guarded and transactional, persisted payloads go
through shared codecs, coordination writes take concurrency seriously, and several
large reads use cursor APIs.

The implementation is nevertheless below the production bar expected for a
high-volume or multi-node control plane. The main problems are structural rather
than cosmetic:

1. Two derived projections can lose newer state under concurrent writers.
2. Execution-group maintenance repeatedly reads and rebuilds the whole group. A
   backfill with `N` children can therefore cause approximately `O(N^2)` total work.
3. Operator detail reads have bounded event slices but still load every child and
   backfill window, so the response is not actually bounded.
4. PostgreSQL is incomplete as a deployable Favn backend: production environment
   configuration cannot select it, authentication persistence is stubbed,
   idempotency persistence is absent, and live tests are opt-in.
5. SQLite and PostgreSQL each contain a multi-thousand-line raw-SQL adapter with a
   large amount of parallel implementation. This makes semantic drift likely and
   raises the cost of every storage change.
6. Retention, query-plan checks, load budgets, and mandatory PostgreSQL CI are
   missing. Append-heavy tables can grow indefinitely without an operational
   contract.

Adapter readiness is therefore:

| Adapter | Intended role | Assessment |
| --- | --- | --- |
| Memory | Deterministic local/test semantic implementation | Appropriate for tests and local use; not a production store. A single GenServer serializes all work and large scans block every storage call. |
| SQLite | Durable single-node Phase 1 store | The most complete backend. Reasonable for the documented single-node envelope, but large execution groups/backfills and unbounded retention are material risks. |
| PostgreSQL | Multi-connection/multi-node durable store | Useful implementation work exists, but it is not production-ready or feature-complete in the current product configuration. |

The recommended response is not a wholesale rewrite. First protect correctness and
make high-cardinality operations bounded, then split the adapters behind the current
public boundary. Replacing all raw SQL with Ecto schemas would create risk without
solving the important concurrency and complexity problems.

## Assumptions, scope, and method

This is an analysis-only change. No storage behavior, schema, or public API was
modified.

The audit originally assumed:

- SQLite remains the current production Phase 1 target.
- PostgreSQL is intended to become a real production adapter, not merely an
  experimental reference.
- Execution groups and backfills may eventually contain thousands or more children
  and windows.
- Run events and logs are long-lived unless Favn defines an explicit retention
  policy.
- Breaking changes are acceptable because Favn is private pre-v1.

The review traced the public orchestrator facade, adapter behaviour, memory state,
SQLite and PostgreSQL implementations, migrations, projection/read-model code,
runtime configuration, tests, and existing storage documentation. Findings about
algorithmic complexity and races follow directly from code paths. Findings about
actual latency and database planner choices require runtime validation.

No orchestrator runtime, Tidewave server, database fixture, or Elixir toolchain was
available in the audit shell. Consequently, this report does not claim measured
throughput and does not include `EXPLAIN (ANALYZE, BUFFERS)` or SQLite query-plan
results. Those measurements are required before selecting final index changes.

The earlier `docs/report/n_plus_one_storage_scalability_review.md` was also checked.
Some of its issues have been fixed: catalogue target status loading is batched and
execution-group step events are fetched with group queries rather than one query per
child. This report records the remaining and newly identified system-level risks.

## What production-quality storage means for Favn

Generic style advice is not a useful quality bar for an orchestrator. In this
project, good storage code should satisfy all of the following.

### Correct authority and consistency

- Every record is classified as authoritative, coordination state, or a repairable
  projection.
- An accepted run transition atomically writes the run snapshot and its event.
- Stale writers cannot overwrite newer authoritative or projected state.
- Retry and idempotency semantics are explicit at every mutation boundary.
- Database-enforced constraints support the invariants that must survive multiple
  BEAM nodes.

### Predictable scale

- Request and transition paths have bounded row counts and payload sizes.
- Pagination is keyset/cursor based on high-growth tables.
- A single child transition is not proportional to the total number of siblings.
- Indexes start with equality filters and then match cursor/order columns.
- Maintenance scans are explicit, chunked, resumable, observable, and never hidden
  inside normal read requests.

### Adapter integrity

- Memory, SQLite, and PostgreSQL implement the same required semantics or advertise
  an explicit, intentional capability difference.
- Dialect-specific locking and SQL remain local to the adapter.
- Shared codecs, invariants, and conformance tests prevent semantic drift.
- Production readiness never depends on a test silently skipping because an
  environment variable is absent.

### Operability

- Schema readiness detects missing, incompatible, and future schemas.
- Retention and cleanup exist for append-only and expiring records.
- Semantic storage operations expose duration, row count, contention, and error
  telemetry.
- Backup, restore, migration, and repair procedures are documented and tested.

### Maintainability

- Modules follow storage capabilities and invariants, not arbitrary line-count
  splits.
- Public contracts are typed and small enough to reason about.
- Simple CRUD does not require fragile positional row decoding in a 5,000-line
  module, while concurrency-sensitive SQL stays explicit.
- A change to one capability does not require editing three giant parallel files.

## Current architecture

The public boundary is sound:

```text
Favn / orchestrator callers
          |
          v
FavnOrchestrator.Storage facade
          |
          v
Favn.Storage.Adapter contract
    /          |           \
 Memory      SQLite     PostgreSQL
                  \       /
              Ecto SQL repos
```

`FavnOrchestrator.Storage` is the only facade callers should use. It delegates to a
configured adapter, supplies adapter options, and provides optional-callback
fallbacks. The inspected UI-facing read paths respect the orchestrator boundary.

The contract is very broad: `Favn.Storage.Adapter` has 97 callback declarations,
many optional, and the facade is 1,015 lines. The concrete SQL adapters are 5,121
lines for SQLite and 4,641 lines for PostgreSQL. This is not automatically wrong,
but it is now too large to make parity and invariants easy to verify.

### Persistence style

SQLite and PostgreSQL use Ecto repos for connection management, transactions, and
migrations, but the adapters issue raw `Ecto.Adapters.SQL.query/3` statements rather
than Ecto schemas and queries. Rows combine:

- relational columns used for filters, ordering, optimistic guards, and indexes;
- canonical JSON/blob payloads decoded by shared orchestrator codecs.

This hybrid is a defensible design for event snapshots and guarded writes. The
problem is its universal use: routine CRUD, dynamic query assembly, parameter
positioning, row decoding, locks, and projection maintenance all live in each giant
adapter. The SQLite/PostgreSQL adapter diff changes roughly 3,930 lines, which is a
strong drift signal.

## Data model and ownership

SQLite expects 25 tables. PostgreSQL implements the following 19 common tables but
does not implement SQLite's authentication and idempotency tables.

| Responsibility | Tables | Classification |
| --- | --- | --- |
| Manifest registry | `favn_manifest_versions`, `favn_runtime_settings` | Authoritative immutable versions plus active selection |
| Run history | `favn_runs`, `favn_run_events` | Authoritative snapshot plus append-only event history |
| Scheduling | `favn_scheduler_cursors` | Authoritative coordination checkpoint |
| Backfill/coverage | `favn_pipeline_coverage_baselines`, `favn_backfill_windows`, `favn_backfill_progress`, `favn_asset_window_states` | Operational state and repairable read models |
| Freshness | `favn_asset_freshness_states` | Derived state/read model |
| Logs | `favn_log_entries` | Append-only operational record; not reconstructable from run events |
| Execution coordination | `favn_execution_leases`, `favn_execution_lease_scopes`, `favn_execution_admission_waiters`, `favn_materialization_claims`, `favn_execution_ownerships` | Ephemeral but correctness-sensitive coordination state |
| Operator projections | `favn_execution_group_summaries`, `favn_target_statuses` | Repairable derived read models |
| Runtime reproducibility | `favn_runtime_input_pins` | Authoritative input decision for a run |
| SQLite-only security/API | `favn_auth_actors`, `favn_auth_credentials`, `favn_auth_sessions`, `favn_auth_audits`, `favn_idempotency_records` | Authoritative security and request-deduplication state |
| SQLite sequence support | `favn_counters` | Monotonic sequence allocator |

PostgreSQL replaces the SQLite counter table with three database sequences for run
writes, globally ordered run events, and globally ordered logs.

The snapshot-plus-payload design is mostly coherent. Queryable fields are projected
into columns while the canonical DTO preserves richer state and compatibility.
However, this is a dual-write model: columns and payload can diverge. Legacy query
metadata repair exists, but invariant validation should be centralized and tested
for every adapter.

Only lease scopes have a physical foreign key. Runs are not constrained to existing
manifests and events are not constrained to existing runs. That may simplify repair
and deletion, but it also permits authoritative orphan records. A missing joined
manifest can make run decoding fail rather than isolate one bad row. Core authority
relationships should either gain foreign keys or have an explicit, transactional
validation and lifecycle policy. Derived/repairable projections can remain looser
when that is intentional.

## What is already good

- Run transitions use expected event sequence/status guards and write the run
  snapshot and event in one transaction.
- Global event and log sequences provide deterministic cursor order.
- Scheduler updates use optimistic versions.
- Lease, admission, ownership, and materialization-claim paths use transactions and
  database locking where cross-writer correctness requires it.
- Shared codecs avoid persisting arbitrary Erlang terms, enforce JSON-safe DTOs,
  redact sensitive values, and avoid unsafe persisted atom creation.
- Read models are treated as repairable projections, and projection failures are
  recorded instead of being silently treated as authoritative success.
- Logs, freshness states, and backfill windows have cursor APIs with bounded page
  sizes.
- Several replacement/upsert paths deduplicate by key and write batches.
- SQLite's single-node constraint, pool-size limit, readiness modes, path handling,
  backup, and restore expectations are clearly documented.
- The memory adapter has an explicit state struct and useful indexes, making it a
  strong semantic test implementation.
- Shared adapter-contract tests cover many run, cursor, coordination, and codec
  invariants. SQLite in particular has substantial behavioural coverage.
- Two previously important N+1 paths have been improved: target statuses for the
  catalogue are batched, and operator step-event loading now uses two group queries
  rather than one query per child.

These are foundations worth preserving during refactoring.

## Critical findings

### C1. Target-status projection can overwrite newer evidence

Evidence:

- `target_status/projector.ex:28` projects every referenced asset and pipeline after
  a transition.
- `target_status/projector.ex:95` reads the existing row, compares evidence in the
  application, then upserts.
- `target_status/projector.ex:219` performs the "latest evidence" decision outside
  the database write.
- SQLite `sqlite.ex:2367` and PostgreSQL `postgres.ex:1603` use unconditional
  `ON CONFLICT ... DO UPDATE` upserts.

Two workers can both read the same target status. The newer transition can commit
first, after which the older worker can unconditionally overwrite it. SQLite's
production pool size of one reduces simultaneous SQL execution but does not make
the separate read and write atomic. PostgreSQL and multi-node operation make the
race more likely.

The current `updated_seq` is a run-local event sequence and cannot order events from
different runs.

Recommendation:

- Give projection evidence a globally comparable, deterministic version, such as a
  run write sequence or global event sequence with a stable tie-breaker.
- Implement compare-and-upsert in the database so stale evidence cannot update the
  row.
- Batch current-state lookup and upsert for all affected targets.
- Add a two-writer regression test that deliberately commits older evidence last.

### C2. Execution-group summaries have both a lost-update race and quadratic work

Evidence:

- SQLite `sqlite.ex:3698` and PostgreSQL `postgres.ex:2357` load and decode every run
  and every backfill window for a group, rebuild the full summary, calculate
  activity, and upsert it.
- SQLite `sqlite.ex:3729` and PostgreSQL `postgres.ex:2386` overwrite the projection
  without guarding the calculated `activity_seq`.
- Normal run persistence invokes refresh; SQLite `sqlite.ex:1065` and PostgreSQL
  `postgres.ex:1086` can invoke it again from the backfill child projection.
- The summary payload includes all `child_run_ids`, so even a paged summary row grows
  with the group.

A stale full-group rebuild can commit after a newer one and regress counts/status.
Separately, refreshing after each of `N` child transitions repeatedly reads up to
`N` siblings. Total work over the group is approximately `O(N^2)`, with repeated
payload decode and allocation. Some child paths refresh the same group twice.

This is the highest-confidence large-backfill bottleneck in the current design.

Recommendation:

- Make the overview projection compact: root identity/status, aggregate counters,
  last activity, and bounded failure/current-work samples only.
- Remove full child ID lists from the summary payload; children already have their
  own indexed/paged relation through run query columns.
- Update counters/deltas atomically with a monotonic projection version, or serialize
  projection application per group. Guard every upsert against stale activity.
- Keep a full rebuild as an explicit, chunked repair operation, not the normal
  transition algorithm.
- Ensure a child transition schedules or performs one projection update, not two.
- Test concurrent distinct-child transitions and groups with at least 10k synthetic
  children.

### C3. PostgreSQL is not a complete production backend

Evidence:

- `production_runtime_config.ex:169` accepts only `FAVN_STORAGE=sqlite`.
- PostgreSQL authentication callbacks at `postgres.ex:1397-1442` return
  `{:error, :auth_persistence_not_supported}`.
- PostgreSQL does not implement the optional idempotency callbacks or their table.
- PostgreSQL integration tests run only when `FAVN_POSTGRES_TEST_URL` is supplied;
  otherwise the live contract is skipped.
- PostgreSQL readiness checks missing table/version names but, unlike SQLite, do not
  reject migration versions newer than the running release.

Calling this adapter production-ready would create a false contract: it cannot be
selected through the production environment path and lacks security/API state used
by SQLite.

Recommendation:

- Decide explicitly whether PostgreSQL is an upcoming product backend or a parked
  experiment.
- If upcoming, make auth and idempotency required capabilities, wire production
  configuration, run the shared contract against a real PostgreSQL service in CI,
  and add upgrade/downgrade/readiness tests.
- Until those gates pass, document it as incomplete and prevent ambiguous partial
  activation.

## High-priority scalability and operational findings

### H1. Operator detail is not bounded

`run_read_model.ex:493` loads all runs in an execution group.
`run_read_model.ex:1105` recursively cursor-scans every backfill window. The returned
detail then constructs every child, window, attempt, and timeline in memory. Event
subsets have limits, but the complete response is still `O(children + windows +
selected events)` in database rows, heap, serialization time, and response size.

Split this contract into a compact group summary plus independently cursor-paged
children, windows, attempts, and failures. Counts should come from the compact
projection. A detail page may compose the first bounded page; it must not imply that
one request returns all history.

### H2. Several paths have fan-out or scan amplification

The detailed query-shape table below distinguishes classic N+1 from other scaling
failures. The important point is that a constant query count is not enough if every
query reads an unbounded group.

| Path | Current shape | Scale | Assessment |
| --- | --- | --- | --- |
| Execution-group refresh | Several queries that load all group runs/windows per child transition | About `O(N^2)` total rows over `N` children | Critical; redesign projection |
| Target-status transition projection | One read and one write per referenced asset/pipeline | `2T` queries for `T` targets | Hot bounded-N+1; batch and guard |
| Backfill failure details | For up to 10 failed windows, one child run read plus one event read | Up to 20 extra queries | Bounded N+1; batch or persist compact failure context |
| Operator step events | Two group queries using per-run/latest-per-step windows | Constant query count, potentially large ranking input | Prior N+1 fixed; profile very large groups |
| Operator detail group/windows | Constant number of cursor loops, but consumes every row | `O(N + W)` rows and response | Unbounded materialization; page the contract |
| Freshness upstream explanation | Pages every freshness row in batches of 500, then filters in Elixir | `ceil(S / 500)` queries and `O(S)` rows | Add exact indexed lookup by latest-success node key |
| Target-status full rebuild | For each asset/pipeline, filters and sorts the full manifest run list | Approximately `O((A + P) * R log R)` | Build indexes in one pass or aggregate in SQL |
| Group-summary rebuild | Enumerates groups, then several full-group queries per group | At least `O(G)` queries plus all child rows | Explicit chunked maintenance only |
| Log batch persistence | Transaction loops entries; sequence allocation and insert per row | About `2L` queries for `L` logs, plus conflict reads | Allocate sequence blocks and bulk insert chunks |
| Recovery | Loads all active runs and repairs individually | Proportional to active set | Cursor/chunk and instrument startup budget |

The group event queries use window functions across matching history. Existing event
indexes help filtering, but ranking can still become expensive for huge groups. A
current-step projection or separately paged event view should be considered only
after query plans show it is needed.

### H3. A full repair is hidden inside a normal read

`run_read_model.ex:280-329` can invoke
`Storage.rebuild_execution_group_summaries/0` when a summary page is empty. A read
request must not trigger a global repair. Besides unpredictable latency, concurrent
readers can start duplicate work.

Return an explicit projection-not-ready/empty state and run an observable,
single-owner, chunked repair job. Repairs need progress, cancellation/restart, and a
bounded database budget.

### H4. Important indexes do not match the query shape

The schema has many useful indexes, especially for status scans, group membership,
events, logs, leases, and claims. The following mismatches deserve measured query
plans:

- Manifest run history/rebuilds filter `manifest_version_id` and order by
  `updated_seq, run_id`, but there is no general composite index in that order.
- Asset target history uses delimiter-based `LIKE` against `target_refs_text`.
  A normal B-tree cannot efficiently find membership. Introduce a normalized
  `run_targets` projection keyed by
  `(manifest_version_id, target_kind, target_id, updated_seq, run_id)`.
- Backfill cursor scans order by window start and identity while commonly filtering
  by `backfill_run_id`; the leading columns of existing indexes do not fully match
  that filter/order combination.
- Freshness cursor ordering starts with `updated_at` and identity fields, while the
  available indexes lead with status or manifest alone.
- Asset-window detail fetches all matching pages and uses an identity/order shape
  not fully represented by existing composites.
- Offset pagination still exists on some operator history. Deep pages become
  progressively more expensive and should move to keyset cursors on high-growth
  data.
- Summary target search is substring text search over denormalized payload text. If
  it becomes a primary filter, use normalized associations rather than broad text
  scans.

Do not add all proposed indexes blindly. Capture representative SQLite
`EXPLAIN QUERY PLAN` and PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` results at realistic
cardinality, because unnecessary indexes also increase write cost and database size.

### H5. Append-only and expiring data have no lifecycle policy

Run events, logs, runs, auth audits, ownership history, pins, backfill state,
sessions, and idempotency rows can grow indefinitely. Idempotency records have
`expires_at`, but expired rows are replaced only when the same key is reused; there
is no general purge. PostgreSQL will also need vacuum/bloat expectations, and SQLite
needs file-size/checkpoint/vacuum expectations.

Define per-record retention, archive, and legal/audit requirements before adding a
generic cleanup job. Cleanup must preserve cursor semantics and run reproducibility.
Then implement bounded, resumable purges with telemetry and database-specific
maintenance guidance.

### H6. Public run listing is unbounded by default

`FavnOrchestrator.Storage.list_runs/1` forwards options without a default limit, and
SQL applies a limit only when one is supplied. The memory adapter similarly scans
and sorts. Some repair/recovery callers intentionally need scans, but the public
shape should not expose an accidental full-table read.

Give public listing a conservative default and maximum cursor page. Add a separate
internal `scan_runs` contract for chunked maintenance. The bounded run-event API
already demonstrates the desired pattern.

### H7. Transition broadcasting incurs an avoidable read

After a transition is persisted, `TransitionWriter` reads the event back to obtain
its global sequence before broadcasting it. Since Favn is pre-v1, change the adapter
transition result to return the fully persisted event (or at least its allocated
global sequence). This removes one round-trip from every successful transition and
makes the transaction result more explicit.

## Migration and schema-readiness findings

The migration history contains duplicate intent. Both foundation migrations create
backfill structures, while later add-backfill migrations repeat them with
`create_if_not_exists`. SQLite and PostgreSQL also have visible historical index
drift around coverage and asset-window state.

`create_if_not_exists` is useful for bootstrap tolerance but can hide a table with
the right name and the wrong shape. Current readiness is primarily based on object
names and migration versions, not required columns, constraints, and critical
indexes. PostgreSQL additionally does not report a schema newer than the running
release as incompatible.

Because Favn is private pre-v1, the simplest clean option is:

1. Decide whether existing development databases must be preserved.
2. If not, squash each backend to a canonical baseline and delete superseded
   migrations.
3. If they must be preserved, add explicit corrective migrations and never rewrite
   applied history.
4. Generate or test a schema contract covering columns, nullability, uniqueness,
   foreign keys, and critical indexes for both SQL backends.
5. Detect missing and future migration versions consistently.

Do not keep accumulating idempotent migrations merely to avoid making the pre-v1
data-compatibility decision.

## Memory adapter assessment

The memory adapter is a useful semantic implementation. Its state is divided into
typed maps and supporting indexes for group membership, backfill run lookup, log
deduplication, and coordination entities. Domain-specific helper modules are already
more readable than the SQL adapters.

It must remain explicitly non-production:

- one GenServer serializes all reads and writes;
- list operations scan/sort maps and projections rebuild whole groups;
- a large scan executes inside the server and blocks unrelated storage operations;
- logs, events, runs, and read models grow until process restart;
- process loss loses all state.

Conformance tests must avoid accidentally specifying memory-only ordering or atomic
behaviour that the SQL stores do not guarantee. Conversely, concurrency tests must
run against real SQL because GenServer serialization can conceal database races.

## Code-quality and refactoring opportunities

### Split by capability, not by helper type

Keep `FavnOrchestrator.Storage` as the public facade, but divide the internal adapter
contract and implementations into cohesive capabilities:

- manifests and runtime settings;
- runs and events;
- execution-group and target read models;
- scheduling and coordination;
- backfill and freshness;
- logs;
- auth and idempotency;
- lifecycle/readiness.

The concrete adapter can remain a thin delegator so callers do not change at once.
Avoid generic `Helpers`, `Queries`, or `Utils` dumping grounds; each module should
name an invariant or storage capability.

### Make capabilities explicit

Ninety-seven callbacks plus optional `function_exported?` checks obscure whether a
backend is usable. Product-critical features such as auth and idempotency should not
be accidental optional callbacks. Either split capability behaviours or expose a
typed `capabilities/0` value validated during startup. Unsupported product modes
should fail before serving traffic.

### Use typed query modules selectively

Keep raw SQL for guarded run/event writes, locks, compare-and-upsert projections,
window queries, and true bulk operations. For simple CRUD and ordinary reads,
internal Ecto schemas/query modules or typed row modules can remove positional
decoders and duplicated dynamic parameter assembly.

Do not build a clever cross-dialect SQL generator. Share only semantically identical
metadata, codecs, invariant checks, and pure transformations; keep PostgreSQL locks,
sequences, conflict clauses, and SQLite-specific transaction details visible.

### Normalize errors and telemetry

The facade should expose a stable storage error taxonomy rather than leaking a mix
of Exqlite, Postgrex, and adapter-specific tuples. Add semantic operation telemetry
with adapter, operation, duration, result class, contention/retry, and row count.
Ecto query telemetry is useful but cannot answer "which Favn operation is slow?"
without reconstructing it from raw SQL.

### Strengthen tests around production risks

Add:

- mandatory PostgreSQL adapter-contract CI;
- concurrent stale-writer tests for target and group projections;
- query-count assertions for transition, detail, and bulk-log paths;
- large synthetic execution-group/backfill tests;
- checked query plans for the documented hot queries;
- migration upgrade, future-version, and schema-shape tests;
- retention/cleanup and cursor-continuity tests;
- parity tests for capability declarations and error shapes.

The current suite is strong on many semantic cases but does not establish a
production scale envelope.

## Recommended sequence before broad cleanup

### Phase 0: establish evidence

- Choose expected 50th/95th/maximum sizes for groups, windows, targets per run,
  events per run, and logs per day.
- Seed representative SQLite and PostgreSQL datasets.
- Record operation-level query counts, latency, rows read, payload size, and query
  plans for transition, group list/detail, target catalogue, recovery, and log ingest.

### Phase 1: correctness gates

- Add monotonic compare-and-upsert for target statuses.
- Guard and redesign execution-group projection updates.
- Declare PostgreSQL's capability/readiness status and enforce it at startup.
- Decide authoritative foreign-key and deletion semantics.

### Phase 2: bound high-cardinality work

- Replace full-group transition refresh with compact incremental projection updates.
- Page operator children/windows/attempts/failures.
- Remove automatic global repair from reads.
- Batch target projection, failure context, and log persistence.
- Add exact freshness lookup and normalized run-target association.

### Phase 3: simplify without changing semantics

- Split behaviours/facade/adapters by capability.
- Introduce typed row/query modules for simple operations.
- Centralize shared codecs, query metadata, invariant validation, and error mapping.
- Canonicalize or correct migrations and add schema contract tests.

### Phase 4: operationalize

- Add retention and chunked cleanup.
- Add semantic telemetry, dashboards, and alerts.
- Make PostgreSQL CI and scale-regression budgets mandatory.
- Document the PostgreSQL data model and production topology if it becomes
  supported.

This order avoids a large aesthetic refactor that preserves the worst algorithms or
changes concurrency behaviour accidentally.

## Acceptance criteria for a production-quality result

Before calling storage production-ready at meaningful scale, the following should
be demonstrably true:

- An older concurrent projection write cannot replace newer target/group state.
- A child transition has a fixed query/row budget independent of sibling count.
- No operator HTTP/LiveView request materializes an unbounded group or history.
- A 10k-child synthetic group remains within agreed transition and detail-page
  budgets.
- Log batch cost grows in bulk chunks, not two round-trips per entry.
- All high-growth list APIs use bounded keyset pagination.
- Every append-heavy/expiring table has an explicit lifecycle policy.
- PostgreSQL runs the same required contract in CI and can be selected only when all
  required capabilities are present.
- SQLite and PostgreSQL hot queries have reviewed plans against representative data.
- Schema readiness detects missing, malformed, and future schemas.
- The public facade and each capability module are small enough that their
  concurrency and return-shape invariants can be reviewed locally.

## Decisions resolved after the audit

The owner decisions that were open when this audit was written are now resolved by
`docs/architecture/postgresql-control-plane-storage-v2.md`:

- PostgreSQL is implemented first and is mandatory for production, development,
  tests, and CI; SQLite is deferred.
- The architecture defines explicit acceptance cardinalities and fixed query/work
  budgets for groups, windows, events, logs, and other high-growth paths.
- Private pre-v1 database state is reset at cutover; compatibility migration is not
  part of Storage V2.
- Production requires an explicit retention profile. The architecture records
  reviewable initial defaults without hard-coding them as the production promise.
- Interactive operator reads are bounded and cursor-paged. Any complete export is a
  separate asynchronous product capability, not an unbounded page mode.

The normative architecture, rather than this evidence report, governs the
implementation sequence and acceptance gates.
