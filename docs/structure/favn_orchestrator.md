# favn_orchestrator

Purpose: the always-on control plane for manifests, deployments, durable run
submission, DAG coordination, schedules, runner demand, identity, audit, and
operator APIs.

## Boundaries

- `FavnOrchestrator` is the public same-BEAM facade used by `favn_view` and
  local/operator workflows.
- Private HTTP routes expose versioned JSON-safe control-plane and scaler
  contracts.
- Persistence behaviours live under `FavnOrchestrator.Persistence`; concrete
  PostgreSQL implementations live in `favn_storage_postgres`.
- Runners are disposable workers. PostgreSQL remains the authority for queued
  work, claims, leases, fencing, results, and downstream DAG readiness.
- Favn exposes demand and runner protocols. It does not provision or terminate
  infrastructure.

## Submission and DAG coordination

`RunSubmissions` durably records accepted intent before planning. Bounded
workers claim submissions with workspace-fair concurrency, pin the active
manifest, plan the DAG, create the run, and enqueue only asset attempts that
are runnable now. Scheduler, API, backfill, rebuild, recovery, and child-run
producers all use this authority.

`RunManager`, `RunServer`, and `TransitionWriter` coordinate one admitted run.
The run pins the deployment, manifest ID, manifest content hash, and the
complete pool-to-release map. Each planned asset selects its explicit
`runner_pool` (or the configured default), and the resulting task pins the one
matching release.

A task result becomes visible durably before its downstream DAG nodes are
made runnable. Runner loss is handled from task lease and result state, not
from process-monitor state alone. Safe failures may retry under policy;
unknown outcomes require reconciliation and are not blindly replayed.

`RunServer` keeps one shared freshness context for the active pipeline run and
persists its compact mutable form as a fenced run checkpoint. Runner tasks
carry only task-local settlement facts and a checkpoint reference. Recovery
loads that checkpoint once, rebuilds immutable asset definitions from the
pinned manifest, and rejects task continuations that name a different
checkpoint.

`RunManager` initially admits a run from a conservative decoded-plan estimate,
then resizes that same node-wide reservation from the measured retained
execution state after checkpoint recovery. The plan, compact manifest
projection, and shared freshness context therefore remain inside one global
active-run memory budget instead of relying only on the per-checkpoint size
limit.

## Runner capacity and registry

`RunnerTasks` owns the durable task facade. `RunnerRegistry` is a process-local
registry of connected dynamic BEAM runners for the single control plane. A
runner registers its instance, boot, pool, release, slot count, capabilities,
and BEAM PID. Monitors remove disconnected sessions; runners re-register after
either side restarts.

The registry is intentionally not durable. PostgreSQL already contains the
work and assignment authority. Registration only answers which live process
may be notified or assigned work now.

The orchestrator exposes exact demand for each pool/release partition:

```text
GET /internal/runner-demand/<pool>/<release-id>
```

The scalar is bounded and cheap to poll. Infrastructure starts enough jobs to
cover demand. The orchestrator never calls Azure, Kubernetes, KEDA, ECS,
Nomad, or a VM provider.

After a runner finishes a task, it claims another. With no compatible work, an
elastic runner follows the configured idle grace and self-exits; a resident
runner waits. Exact release-drain reads protect manifest/image rollback and
overlap.

## Manifest lifecycle

`Manifests`, `ExecutionPackages`, `ManifestStore`, and
`ManifestIndexCache` own immutable publication and workspace activation.
Manifest versions carry an arbitrary logical pool-to-release map. Publication
and activation do not require a live runner, so the control plane remains
ready at zero runners.

Physical relation inspection and generation operations are durable runner
tasks. They use the same exact pool/release selection, claim, fencing, result,
and acknowledgement protocol as asset attempts.

## Other authorities

- `RunOwnership` and `ExecutionAdmission` own fenced distributed coordination.
- `TargetGenerations` resolves persisted assets to physical generations and
  non-persisted assets to durable workspace evidence bindings. Claims and reads
  pin that identity; `MaterializationClaims` and rebuild modules own
  physical-generation mutation and activation.
- Scheduler modules own durable occurrence intent and overlap policy.
- Runtime-input pins, audit, auth, idempotency, logs, and projections are
  explicit persistence boundaries.
- Readiness checks configuration, lifecycle, API, scheduler, PostgreSQL, and
  queue/counter consistency. Zero connected runners is healthy.

## Main code

- `apps/favn_orchestrator/lib/favn_orchestrator/run_submissions.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_manager.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/run_server.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/runner_tasks.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/runner_registry.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/api/runner_capacity_router.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/manifests.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/production_runtime_config.ex`

Tests live under `apps/favn_orchestrator/test/`. Cross-node protocol and
PostgreSQL authority tests additionally live in `apps/favn_runner/test/` and
`apps/favn_storage_postgres/test/`.
