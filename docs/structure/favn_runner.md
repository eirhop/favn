# favn_runner

Purpose: disposable execution runtime for durably queued Favn tasks.

## Runtime model

Every runner starts as a hidden dynamic distributed-BEAM node, connects
outbound to the configured control plane, and registers:

- a unique runner instance and boot identity;
- one operator-defined logical runner pool;
- one immutable runner release;
- one execution slot in the first elastic implementation; and
- the task kinds and capabilities it can execute.

The orchestrator sends only a lightweight availability notification. The
runner atomically claims the exact next compatible task from PostgreSQL before
executing it. It never receives correctness-critical work solely through a
message or process mailbox.

The runner acknowledges an assignment generation, prepares the pinned
manifest and execution package, runs the task, persists bounded logs and the
terminal result, and waits for the orchestrator to acknowledge that result.
Expired assignments are fenced. A stale runner cannot overwrite a newer
attempt or result.

## Lifecycle modes

The pool policy is owned by the control plane:

- `resident` runners wait indefinitely for more work;
- `elastic` runners wait for `idle_grace_ms`, make one final atomic claim, and
  exit when no compatible work remains.

Self-exit is the scale-down contract. Favn never selects an infrastructure
instance to terminate. A service, Job, VM supervisor, Kubernetes controller,
or other infrastructure layer observes the process exit and removes its
container or node.

One elastic runner has one slot. This keeps job count equal to desired
concurrency and avoids adding local slot scheduling before measurements show
that it is useful.

## Exact release and pool binding

`FAVN_RUNNER_RELEASE_ID` is baked into the customer runner image. The runtime
also requires `FAVN_RUNNER_POOL`. A runner claims only tasks whose logical pool
and immutable release both match its registration.

Pool names are arbitrary operator-defined names such as `duckdb`,
`pure_elixir`, `gpu`, or `high_memory`; they are not fixed size classes.
Manifest versions carry the complete pool-to-release map. Each queued asset
attempt carries the one exact pool and release selected for that asset.

Multiple manifest and runner releases may overlap safely because each demand,
claim, assignment, result, and drain decision is partitioned by that exact
pair. Rollback means restoring the previous manifest and matching runner image;
new work then appears on that release partition while already-claimed work
keeps its original immutable identity.

## Preparation and execution

`FavnRunner.ManifestStore` owns the bounded immutable manifest cache and
artifact preparation. It verifies the requested pool release before resolving
an asset and its execution package.

`FavnRunner.TaskExecutor` owns one claimed task process. It verifies execution
package identity before starting `FavnRunner.Worker`, persists logs and a
terminal result through the control plane, and retains unacknowledged terminal
results in the bounded `FavnRunner.TaskResultBuffer`.

`FavnRunner.Worker` and the SQL runtime own asset execution, runtime input
resolution, relation inspection, generation operations, checks, contracts,
materialization planning, and bounded result/error normalization. Unknown
write outcomes remain unknown; they are never reclassified as safe retries.

Runner-local caches, SQL sessions, credentials, and plugin state are
disposable. Durable work, fencing, runtime-input pins, results, and downstream
DAG readiness remain PostgreSQL authority.

## Networking and production configuration

Production uses mutual-TLS distributed BEAM plus a high-entropy cookie.
Runners use `FAVN_RUNNER_NODE_HOST_ALIAS` to form an
`undefined@<host-alias>` dynamic node and `FAVN_CONTROL_PLANE_NODE` as the
single stable outbound destination. They do not require inbound distribution
listeners.

The runner can therefore run as a Container Apps Job, Kubernetes Job, ECS
task, Nomad batch allocation, VM process, or resident service without changing
Favn core.

See:

- `apps/favn_runner/lib/favn_runner/runner_agent.ex`
- `apps/favn_runner/lib/favn_runner/task_executor.ex`
- `apps/favn_runner/lib/favn_runner/task_result_buffer.ex`
- `apps/favn_runner/lib/favn_runner/manifest_store.ex`
- `apps/favn_runner/lib/favn_runner/worker.ex`
- `apps/favn_runner/lib/favn_runner/shutdown.ex`
- `docs/production/elastic_runners.md`

Tests live under `apps/favn_runner/test/`. Direct execution helpers are
test-only; production always exercises the durable claim, assignment, result,
and acknowledgement protocol.
