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

- `Manifests`, `ManifestStore`, and deployment planning own immutable global
  releases and exact workspace deployment catalogs.
- `Runs`, `RunManager`, `RunServer`, and `TransitionWriter` own submission,
  execution, retry, cancellation, snapshots, events, and durable publication.
- `RunOwnership`, `ExecutionAdmission`, `MaterializationClaims`, and scheduler
  runtime modules own fenced distributed coordination.
- `Backfills` and `BackfillDispatcher` own range expansion, parent/child state,
  dispatch, and compensation.
- `Identity` and `Auth` own accounts, memberships, sessions, service identities,
  policy enforcement, and audit intent.
- `Operator.Catalogue`, `Operator.Lineage`, `Operator.Schedules`, `Logs`, and the
  facade expose bounded read models to thin clients.
- `RuntimeInputPins` owns encrypted resolve/pin/replay behavior. Raw resolved
  credentials never enter generic run metadata.
- `Readiness`, `Diagnostics`, and persistence maintenance expose safe operational
  state without bypassing the public boundary.

Run snapshots and append-only events are authoritative for run state. Compact
operator projections are versioned, repairable, and updated through the durable
outbox. PubSub and PostgreSQL notifications only reduce refresh latency.

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
