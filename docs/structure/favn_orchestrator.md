# favn_orchestrator

Purpose: the control plane and public backend facade for manifests, deployments,
runs, schedules, events, identity, authorization, audit, idempotency, backfills,
freshness, and execution coordination.

## Boundaries

- `FavnOrchestrator` is the public facade used by `favn_view` and same-BEAM
  operator clients.
- Private HTTP routes live under `FavnOrchestrator.API` and expose versioned,
  JSON-safe DTOs.
- Persistence contracts live under `FavnOrchestrator.Persistence`; concrete
  PostgreSQL modules live in `favn_storage_postgres`.
- `FavnOrchestrator.WorkspaceContext` scopes customer operations.
  `FavnOrchestrator.OperatorContext` carries authenticated actor and workspace or
  platform authority. Callers must not infer workspace from a run identifier.
- Domain lifecycle and policy remain here. Stores implement atomic commands and
  bounded queries; they do not decide scheduling, retries, repair policy, or user
  authorization.

## Main areas

- `Manifests`, `ExecutionPackages`, `ManifestStore`, `ManifestIndexCache`, and
  deployment planning own
  immutable compact global releases, package-first publication, on-demand runtime
  package attachment, bounded compiled indexes, and exact workspace deployment
  catalogs. Publication is runner-independent and leaves a manifest staged.
  Activation loads that immutable version, requires an explicitly ready runner
  reporting the exact `required_runner_release_id`, verifies or registers the
  manifest in the runner cache, and only then commits the deployment pointer.
  Runner outage returns a service-unavailable result; a release mismatch or
  conflicting runner cache entry returns a conflict without changing the active
  deployment. `Manifests` emits bounded publication and activation telemetry.
  Runner diagnostic events include latency, status, manifest id when known, and
  the required/actual release ids on mismatch. Rejected activation audit entries
  contain only actor/service identity, stable reason codes, idempotency metadata,
  and relevant release ids; selection and configuration are not copied into them.
- `Runs`, `RunManager`, `RunServer`, and `TransitionWriter` own submission,
  execution, retry, cancellation, snapshots, events, and durable publication.
  Submission derives the runner release only from the selected immutable manifest;
  caller options cannot override it. The run's workspace, deployment, manifest id,
  manifest content hash, and runner release id are one immutable identity. Dispatch,
  relation inspection, runner results, recovery, events, diagnostics, and compact
  operator summaries preserve or verify that identity.
  `RunManager` coordinates only bounded in-memory admission and process tracking;
  PostgreSQL work happens in callers or supervised recovery workers so one slow
  database operation cannot block unrelated manager calls.
- `RunOwnership`, `ExecutionAdmission`, `MaterializationClaims`, and scheduler
  runtime modules own fenced distributed coordination.
- `RunnerClient.BeamNode` is the sole production runner transport. It connects
  to one validated static long node name, performs bounded `:erpc` calls, and
  never loads or calls `favn_runner` inside the control-plane BEAM. Readiness
  accepts only connected runner diagnostics with a ready verified release ID.
- `ResourceCircuits` resolves configured execution-pool and SQL-connection
  resources before ordinary capacity admission. It records only explicit runner
  resource outcomes, while `ResourceRecovery` submits linked targeted runs for
  durable, unexpired, safe candidates after an exclusive probe succeeds. Its
  supervised bounded sweep resumes pending candidates after restart, and
  deterministic recovery run ids make uncertain submission replay idempotent.
- Run recovery discovers active workspace identities from PostgreSQL rather than
  treating a static node environment list as persistence authority.
- `Backfills` and `BackfillDispatcher` own range expansion, parent/child state,
  dispatch, and compensation.
- `Identity` and `Auth` own accounts, memberships, sessions, service identities,
  policy enforcement, and audit intent.
- `Operator.Catalogue`, `Operator.Lineage`, `Operator.Schedules`, `Logs`, and the
  facade expose bounded read models to thin clients. Asset catalogue detail
  decodes freshness keys structurally and projects run anchors, exact coverage,
  and aggregated calendar freshness separately; only anchor and exact-window
  projections carry submission intent. `AssetRunContext` binds a manifest-pinned
  asset to one selecting pipeline policy and timezone. Catalogue reads and operator
  commands share its stable id, reject forged or stale contexts, and surface
  multi-pipeline ambiguity instead of depending on manifest order.
- `RunReadModel` keeps requested backfill anchors distinct from exact effective
  asset windows. Its default operator detail path expands compact relational
  projections; the events view is the explicit bounded snapshot/event path.
- `RuntimeInputPins` owns encrypted resolve/pin/replay behavior. Raw resolved
  credentials never enter generic run metadata. Pins are bound to the selected
  asset's exact execution-package hash and resolver.
- `Readiness`, `Diagnostics`, and persistence maintenance expose safe operational
  state without bypassing the public boundary.

Run snapshots and append-only events are authoritative for run state. Compact
operator projections are versioned, repairable, and updated through the durable
outbox. PubSub and PostgreSQL notifications only reduce refresh latency.
Current snapshots require the runner release binding and use storage format v3.
Historical terminal v2 snapshots remain readable with a nil release id for audit.
A non-terminal v2 snapshot cannot be recovered or dispatched and returns the stable
`legacy_runner_release_unbound` reason.
Catalogue and planning paths share the byte- and entry-bounded compiled manifest
index cache; generated SQL execution
trees are loaded only after execution admission for the selected runtime asset and
attached to `RunnerWork` before runner preflight. The per-run `Manifest.Index` is
reduced to the planned asset subset and provides constant-time lookup; wide stages
never retain all packages at once. Wide-stage admission yields after a small
node/time batch so ownership renewal and cancellation messages cannot be starved.
Wide-stage retry persistence writes one authoritative compact stage-bitset checkpoint
instead of per-node retry-scheduled transitions, keeping the database work constant
per retry decision.
Terminal node failure does not stop independent stage siblings. Downstream nodes
with incomplete required upstreams are durably blocked. Resource-open blocking is
a terminal node result in the source run; opt-in recovery always creates another
linked run and never rewrites that source result.
Run-list APIs select compact relational summaries without the authoritative JSON
snapshot. Operator detail reconstruction caps full child snapshots at four and marks
`child_run_details_truncated?` when more relational child rows exist.
Asset assurance projects every ordered SQL contract row-count claim with its exact
or bounded constraint, condition, violation policy, stable claim identity, and
latest bounded check result; `favn_view` does not reconstruct this state.

## Tests

- Pure orchestration policy and facade tests:
  `apps/favn_orchestrator/test/`.
- PostgreSQL transaction, concurrency, authority, tenancy, and query tests:
  `apps/favn_storage_postgres/test/storage_v2/`.
- Product-level one-node workflow:
  `apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs`.

Use this app when changing lifecycle semantics, persistence contracts, workspace
authorization, private API behavior, live-event DTOs, backfills, scheduling,
admission, ownership, retries, cancellation, readiness, or operator read models.
