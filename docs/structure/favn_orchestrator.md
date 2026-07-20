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
  catalogs.
- `Runs`, `RunManager`, `RunServer`, and `TransitionWriter` own submission,
  execution, retry, cancellation, snapshots, events, and durable publication.
  `RunManager` coordinates only bounded in-memory admission and process tracking;
  PostgreSQL work happens in callers or supervised recovery workers so one slow
  database operation cannot block unrelated manager calls.
- `RunOwnership`, `ExecutionAdmission`, `MaterializationClaims`, and scheduler
  runtime modules own fenced distributed coordination.
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
  projections carry submission intent.
- `RuntimeInputPins` owns encrypted resolve/pin/replay behavior. Raw resolved
  credentials never enter generic run metadata. Pins are bound to the selected
  asset's exact execution-package hash and resolver.
- `Readiness`, `Diagnostics`, and persistence maintenance expose safe operational
  state without bypassing the public boundary.

Run snapshots and append-only events are authoritative for run state. Compact
operator projections are versioned, repairable, and updated through the durable
outbox. PubSub and PostgreSQL notifications only reduce refresh latency.
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
Run-list APIs select compact relational summaries without the authoritative JSON
snapshot. Operator detail reconstruction caps full child snapshots at four and marks
`child_run_details_truncated?` when more relational child rows exist.

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
