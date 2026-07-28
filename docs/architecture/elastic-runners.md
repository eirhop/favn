# One control plane with elastic runners implementation plan

Reader: Favn contributors implementing or reviewing runner distribution.

Documentation type: accepted architecture and implementation plan.

Status: implemented; qualification evidence is recorded in
`elastic-runners-implementation-log.md`

Last researched: 2026-07-26

## Purpose

Document the completed breaking-change migration from one statically addressed
runner to one control-plane node coordinating zero to many resident or
self-terminating runner nodes. The design must preserve PostgreSQL as the
durable correctness authority, use distributed BEAM communication after a
runner starts, and leave infrastructure provisioning outside Favn.

This plan covers runner elasticity and dynamic runner membership for one control
plane. Multi-control-plane availability and rolling mixed-version clusters remain
separate work.

## Decision summary

Implement the multi-runner design directly. Do not build a separate 0-to-1
transport first. A maximum of one runner is only a deployment setting of the
same architecture.

Use these boundaries:

- authors select an arbitrary configured `runner_pool` such as `:duckdb`,
  `:pure_elixir`, `:gpu`, or `:private_network`;
- deployment infrastructure maps each logical pool and runner release to an
  image, plugins, CPU, memory, accelerators, network policy, maximum job count,
  and price;
- Favn persists runner tasks and exposes an authenticated numeric demand metric;
- infrastructure observes that metric and starts runner processes;
- a runner initiates one outbound distributed-BEAM connection, registers, and
  asks Favn for a compatible task;
- PostgreSQL atomically claims and fences every task;
- BEAM messages wake runners and carry task protocol messages after connection;
- elastic runners exit themselves after a control-plane-selected idle grace;
- resident runners wait indefinitely;
- infrastructure treats process exit as job completion and removes the compute.

Pool names have no built-in size or capability meaning. Favn validates and
matches them exactly but does not interpret them. The first version is
deliberately cost-aware through explicit pool selection, admission-aware demand,
scale to zero, and bounded idle reuse. It does not try to predict future work or
choose cloud SKUs. Those optimizations need measured runtime and price data and
should be added only after this contract is operating.

`execution_pool` and `runner_pool` remain separate:

- `execution_pool` limits access to a shared resource such as an API;
- `runner_pool` selects the interchangeable runner environment and deployment
  policy that executes an asset.

Do not overload or rename `execution_pool`.

## Implementation sequence

This is the architectural sequence. Section 21 translates it into detailed
implementation tasks grouped under seven review phases. The tasks govern build
order and focused verification; the phase boundaries govern independent
review, commit, and push order.

1. Freeze the new terminology, ownership boundaries, and non-goals.
2. Complete issue #525's durable run-submission queue and asynchronous
   orchestration handoff.
3. Add an explicit runner-pool authoring and manifest contract.
4. Add a durable, typed runner-task queue and atomic claim commands.
5. Refactor run execution to enqueue runnable DAG nodes instead of calling a
   static runner.
6. Move runner-local manifest installation and runtime-input preparation to task
   assignment.
7. Add dynamic BEAM runner registration, availability, monitoring, and wake-up.
8. Add runner pull, lease renewal, result delivery, cancellation, and recovery.
9. Move inspection and target-generation operations onto the same typed task
   lifecycle.
10. Add resident and elastic runner lifecycle modes with bounded idle grace.
11. Add admission-aware capacity-demand projections and an authenticated read
    endpoint.
12. Make manifest activation, inspection, readiness, and reconciliation work
    with zero or many compatible runners.
13. Remove the static single-runner transport, configuration, health,
    replacement, cache-reconciliation, log-bridge, and execution-ledger paths.
14. Update local development and production deployment artifacts around the new
    runner contract.
15. Add concurrency, crash-boundary, scale, security, and deployment acceptance
    coverage.
16. Update canonical public, architecture, production, operator, feature, and
    roadmap documentation.
17. Deliver in independently reviewable vertical slices while keeping the final
    contract free of legacy compatibility branches.

Issue #525 and elastic runners are one implementation program and one production
gate, delivered in reviewable slices. They still require two explicit durable
queue contracts and must not be merged into one generic lifecycle:

```text
submission queue (#525)
        |
        v
plan pipeline DAG
        |
        v
runnable asset attempt
        |
        v
runner-task queue (this plan)
        |
        v
runner pool and runner process
```

The submission queue protects accepted run requests before planning. The
runner-task queue protects individual data-plane operations after planning.
The orchestrator consumes submissions; runners consume runner tasks. A single
submission can produce many tasks, and only runner tasks create runner demand.

## Assumptions and non-goals

### Assumptions

- PostgreSQL 18 remains mandatory and is the only durable coordination
  authority.
- The first supported control-plane topology is exactly one control-plane BEAM
  node.
- A runner has one execution slot in the first version.
- Every task is pinned to one logical runner pool and one exact immutable runner
  release ID.
- Pool names are arbitrary bounded identifiers with no predefined vocabulary.
- Source pool names are atoms, while manifest/runtime pool names are opaque
  strings. Runtime names are 1-63 bytes, begin with an ASCII letter or digit,
  and then contain only ASCII letters, digits, `.`, `_`, or `-`. This is a
  provider-neutral URL/config/metric-safe identity rule, not a fixed pool
  vocabulary, and runtime input is never converted into a new atom.
- Each logical pool has exactly one configured lifecycle mode and idle-grace
  policy in version one. Every old/new release of that pool inherits it;
  resident and elastic runners are not mixed within a pool.
- Different pools may use different images, plugins, native dependencies,
  resources, accelerators, or network boundaries. Any executable-image
  difference has its own exact runner release ID.
- Each manifest publication contains a bounded `runner_releases` map from every
  effective runner-pool name to one exact immutable runner release ID. A run
  snapshot freezes that map, and every task copies its exact pool/release pair
  from the snapshot when the task is created.
- A manifest with no executable assets or pipelines has no effective runner
  pools and therefore carries an empty `runner_releases` map. Every manifest
  with executable work must contain every effective pool exactly once.
- Runner images and infrastructure remain customer-owned.
- Breaking schema, configuration, API, DSL, and operational changes are allowed.
- Azure Container Apps Jobs and Kubernetes/KEDA are reference deployments, not
  runtime dependencies.
- Under normal available platform capacity, the supported reference deployment
  must start P95 cold work within five minutes from the runner task becoming
  durably runnable to its `Started` acknowledgement being persisted.

### Non-goals for the first version

- multiple active control-plane nodes;
- Favn provisioning or deleting cloud resources;
- automatic cloud SKU selection or live cloud-price lookup;
- predictive prewarming, a prewarm horizon, or DAG runtime forecasting;
- fallback from one named pool to any other pool;
- multiple concurrent task slots inside one runner;
- mixed runner protocol versions;
- durable history of every transient runner presence heartbeat;
- exact-once external side effects, which cannot be guaranteed across process,
  network, and infrastructure failures.

The protocols and storage identities must leave room for later multi-control-plane
ownership, multiple slots, and smarter wait policies, but no speculative
abstraction should be added until one of those features is implemented.

## Current codebase findings

### Work is already one asset-node attempt

The unit to distribute is not a whole pipeline. It is one planned asset node,
window, and attempt:

- `RunServer.Execution.StepAttemptLifecycle.build_work/2` builds one
  `%Favn.Contracts.RunnerWork{}` with one `asset_ref`, one `asset_step_id`, one
  stage, and one attempt;
- `RunServer.Execution.StageAdmission` walks the runnable node keys and submits
  them independently;
- `FavnRunner.Worker` executes one source, Elixir, or SQL asset;
- the control plane owns the DAG, stages, retries, pipeline `max_concurrency`,
  execution-pool admission, materialization claims, and target generations.

This is the correct boundary for horizontal distribution. The planner must
continue to expose only currently runnable nodes. Downstream blocked nodes are
known to the DAG but are not immediate compute demand.

### The present transport is statically one-to-one

The current production path assumes one exact runner node:

- `FavnOrchestrator.RunnerClient.BeamNode` reads one `:runner_node` and uses
  bounded `:erpc` calls;
- `FavnOrchestrator.ProductionRuntimeConfig` requires `FAVN_RUNNER_NODE` and
  converts that configured name to an atom;
- `FavnRunner.ProductionRuntimeConfig` requires one runner name and one expected
  control-plane name;
- `docs/structure/favn_orchestrator.md`,
  `docs/structure/favn_runner.md`, and
  `docs/production/deployment_topology.md` document one control plane and one
  statically addressed runner;
- generated Compose and runner templates contain one fixed runner service.

Adding a list of node names to `BeamNode` would not solve queue durability,
assignment fencing, runner startup, scale down, or recovery and must not be used
as an intermediate architecture.

### The static client is broader than asset execution

`Favn.Contracts.RunnerClient` and `RunnerClient.BeamNode` currently expose all
of these remote operations:

- register, ensure, acquire, renew, and release a runner-local manifest;
- submit work, await result, and cancel work;
- resolve runtime inputs;
- subscribe and unsubscribe execution logs;
- inspect a relation;
- inspect, initialize, activate, reconcile, and discard target generations;
- read runner diagnostics.

Scale to zero is incomplete unless every operation that requires customer code,
plugins, connections, or adapters can select a compatible dynamic runner.
Therefore this plan uses one typed runner-task lifecycle, not an asset-only
queue plus a hidden static RPC fallback.

### Admission already exists but dispatch is immediate

`RunServer.Execution.StageAdmission` currently:

1. acquires pipeline and execution-pool capacity;
2. acquires resource-circuit permits;
3. acquires a materialization claim;
4. attaches the execution package;
5. resolves and persists runtime inputs by calling the runner;
6. persists a `RunExecutionOwnership` dispatch intent;
7. immediately calls `RunnerDispatch.submit_work/4`;
8. starts a task that waits on the runner result.

The capacity and materialization contracts should be retained. The immediate
remote call and runner-local result wait should be removed.

Only tasks that have passed the existing orchestration admission gates should
contribute runnable compute demand. Otherwise a stage with 100 nodes and
`max_concurrency 2` could incorrectly start 100 runners.

### The existing runner execution ledger is not a work queue

`runner_executions` records a dispatch intent and later execution status, but it
does not provide:

- a pool/release claim index;
- an atomic unassigned-to-assigned transition;
- an assignment lease and generation;
- a demand counter;
- a durable typed payload/result codec;
- runner registration identity;
- recovery of work that has never reached a runner.

`RunExecutionOwnership` and `runner_executions` should therefore be replaced,
not extended into two overlapping lifecycle models.

### Runner-local state assumes a long-lived singleton

`FavnRunner.Server` retains queued, running, and completed executions and serves
`await_result`. `FavnRunner.ManifestStore` is an in-memory cache whose leases
are currently acquired for an entire run from the one runner.
`ActiveManifestReconciler` repopulates that singleton cache and readiness fails
unless all active manifests are registered there.

An elastic runner starts with an empty cache and may disappear after one task.
Manifest installation must become assignment-local. The durable task and result
must live in PostgreSQL, not in runner retention.

### Persistence boundaries are suitable for a new capability

`FavnOrchestrator.Persistence.Stores` validates explicit capability stores at
startup. Add a `runner_tasks` capability instead of placing new task operations
inside `RunOwnershipStore`. Keep run ownership and runner task ownership as
separate fenced domains.

Storage V2 is an exact, reset-only baseline with an explicit migration registry,
required tables, columns, indexes, constraints, and definition fingerprint.
The queue change must update all of those inventories and the restore verifier.

## Refactoring opportunities

| Current shape | Replacement | Benefit |
| --- | --- | --- |
| Static wide `RunnerClient` RPC | Typed `RunnerTasks` | All data-plane work scales to zero |
| `runner_executions` ledger | Fenced `runner_tasks` queue | One recovery lifecycle |
| Monolithic runner server | Connection, agent, executor, buffer | Explicit failure ownership |
| Run-wide manifest leases | Assignment-local leases | No cache prepopulation |
| Runner-side result retention | PostgreSQL push-with-ack | Safe runner exit |
| One global claim coordinator | Concurrent claims, pool/release wakes | No global I/O bottleneck |
| `RunWorkSet` state | `ActiveTaskSet` identities | Work and assignment are separate |
| Direct operation RPCs | Typed tasks and continuations | Restart-safe scale to zero |
| Destructive replacement | Release coexistence and drain | Pinned work is uninterrupted |
| Runner-dependent readiness | Queue/configuration readiness | Zero runners is healthy |

These are replacements, not abstraction layers over the old design. The final
state has one path for runner-bound work.

## Target architecture

### Control flow

```text
author: runner_pool :duckdb
          |
          v
manifest -> plan node -> admitted runner task in PostgreSQL
                                  |
                                  +--> O(1) demand counter
                                             |
                                             v
                                   infrastructure scaler
                                             |
                                             v
                                    start runner job
                                             |
                           outbound TLS distributed BEAM
                                             |
                                             v
                    register -> claim -> execute -> report -> ack
                                             |
                                      no task available
                                             |
                              wait for BEAM wake or deadline
                                             |
                                 final claim -> clean exit
```

Infrastructure never chooses a task or runner identity. Favn never creates or
deletes infrastructure. Infrastructure decides only how many processes of each
configured pool/release should exist.

### BEAM topology

Use a hub-and-spoke topology:

- the control plane has the stable, routable long node name;
- each runner starts with an OTP dynamic long node name using
  `undefined@<stable-runner-host-alias>`;
- OTP consequently configures the runner as a non-listening hidden node with
  automatic connections disabled;
- the runner explicitly connects outbound to the configured control-plane node;
- runners do not discover or connect to one another;
- do not use `:global`, `:pg`, or a mesh membership library for runner
  placement;
- use explicit registered control-plane process names, remote PIDs, messages,
  links/monitors, and bounded `GenServer.call` operations;
- the control plane monitors the remote runner-agent PID and treats `:DOWN` or
  `:nodedown` as presence loss.

This uses normal BEAM process communication without creating a transitive
all-to-all runner cluster. A runner can reconnect after a control-plane restart,
receive a new dynamic node name, and register the same process-instance
identity again.

The host alias is deployment-scoped, bounded, shared by ephemeral runners, and
must not contain a unique pod or job ID. OTP keeps remote node-name atoms and
OTP 29's dynamic-name pool reuses freed names per host. Allowing every
ephemeral container hostname into the node name would therefore create
unbounded atom growth in a long-lived control plane. The alias does not make a
runner addressable because dynamic runners do not listen, but its startup and
name-resolution behavior must be acceptance-tested on each platform.

Production distribution must use `-proto_dist inet_tls` with peer verification
and client certificates on both release types. The control-plane certificate
must match its long node name for TLS server-name verification; server options
must include `verify_peer` and `fail_if_no_peer_cert`. Pin the control-plane
distribution port range and expose EPMD only on the private runner network.
The cookie remains required, but is not a sufficient security boundary. Plain
TCP distribution is allowed only for explicit loopback source development.

### Infrastructure interaction

The one HTTP capacity endpoint exists because no runner process exists at scale
zero. It is for infrastructure controllers, not communication between live BEAM
nodes.

For each `{runner_pool, required_runner_release_id}`, infrastructure configures
one scalable workload:

- Azure: one event-driven Container Apps Job definition;
- Kubernetes: one KEDA `ScaledJob`;
- another platform: any job/process controller that can read a numeric metric
  and treat a clean process exit as completion.

Each user-defined pool/release is a separate deployment definition. A pool can
represent resource size, an executable image and plugin set, network access, or
another operator-defined compatibility boundary. Different pools may use the
same image digest when their runner release is the same; an executable-image
change requires a different runner release ID.

## Detailed codebase plan

### 1. Freeze terminology and invariants

Add a current architecture decision document before implementation and use
these names consistently:

| Term | Meaning |
| --- | --- |
| runner pool | Logical author-selected interchangeable runner environment, stored as a bounded string |
| runner release | Exact immutable customer runner build |
| runner instance | One OS process/BEAM VM started by infrastructure |
| runner session | One successful registration generation for an instance |
| runner task | One durable typed data-plane operation |
| task assignment | One fenced claim of a task by a runner session |
| execution pool | Existing shared-resource concurrency boundary |
| resident runner | Waits indefinitely until infrastructure stops it |
| elastic runner | Exits after bounded idle grace |

Core invariants:

- a task is compatible only when pool, release ID, runner protocol, and task
  capability all match;
- pool names are opaque to Favn; matching is exact and Favn assigns no built-in
  meaning to names such as `duckdb`, `gpu`, or `private_network`;
- task IDs are deterministic and bounded where the caller has a durable domain
  identity;
- task assignment is committed before a runner may execute;
- runner messages include workspace ID, task ID, assignment generation, runner
  instance ID, and runner session generation;
- stale messages can never advance a newer assignment;
- notifications are latency hints; PostgreSQL state decides correctness;
- an unacknowledged terminal result is retained and resent by the live runner;
- unknown external outcomes remain unknown and are never blindly retried;
- a clean elastic exit is permitted only when there is no assigned task and a
  final claim returns no work.

Within one exact pool/release, version one claims FIFO by enqueue time and task
ID. Do not add priorities or weighted workspace fairness until queue-age
measurements demonstrate a real need.

### 2. Complete #525's durable run-submission lifecycle

Implement issue #525 before runner-task dispatch. This is the durable
control-plane queue that turns a request to start a run into an admitted,
planned run. It is not the runner-task queue.

Add a `RunSubmissionStore` capability and a durable `run_submissions`
lifecycle shared by every producer:

- operator and API submissions;
- scheduled occurrences;
- backfills and rebuilds;
- recovery and child-run callers.

Persist normalized and redacted submission intent, authority and workspace,
idempotency key, deployment/manifest identity, requested run or target
identity, status, attempt, claim owner/generation/lease, timestamps, outcome,
and safe error details. The authority snapshot is derived only from the
validated `WorkspaceContext`, contains an allowlisted principal/workspace/role/
request identity, and is audit evidence rather than a reusable credential. Use
the issue's lifecycle:

```text
queued -> preparing -> admitting -> submitted
  |          |            |
  |          +------------+-> failed
  +-> cancelled/superseded
```

Use this command/transition matrix rather than accepting arbitrary status
updates:

| Current | Command/evidence | Next | Rule |
| --- | --- | --- | --- |
| `queued` | fenced claim | `preparing` | set claim generation and lease |
| `queued` | cancel | `cancelled` | only before a worker claim |
| `queued` | newer mutually exclusive durable intent | `superseded` | persist the replacement identity |
| `preparing` | durable plan prepared | `admitting` | same live claim generation |
| `preparing` | proven-safe transient failure or expired lease | `queued` | increment attempt/generation |
| `preparing` | permanent failure | `failed` | persist safe error |
| `preparing` | cancellation acknowledged before admission | `cancelled` | clear claim fields |
| `admitting` | matching run durably exists | `submitted` | persist run ID idempotently |
| `admitting` | proven-safe pre-admission failure | `queued` | increment attempt/generation |
| `admitting` | permanent or ambiguous admission outcome | `failed` | mark explicit unknown outcome; never blind retry |

`submitted`, `failed`, `cancelled`, and `superseded` are terminal and immutable.
Once a submission is `admitting`, reject submission cancellation: reconcile
the exact run identity and use the run-cancellation lifecycle if admission
committed. The matching-run proof includes workspace, run, deployment,
manifest, and requested target identity. For an asset request, the exact
`run_targets` row must have `is_primary = true`; for a pipeline request, its
exact pipeline target row is the proof because pipeline target rows are not
marked primary. A secondary asset can therefore never satisfy reconciliation.
A run identity has one logical owner within a workspace across submission
intent and durable runs. Enqueue, retry, and every `RunStore.create_run` path
acquire the same transaction-scoped PostgreSQL advisory lock keyed by
workspace and run ID before checking or writing either table. Enqueue and retry
reject an already-durable run or another submission. Run creation may coexist
only with the exact matching `admitting` submission that is creating that run;
otherwise it rejects the collision. All legacy direct run producers must use
this shared lock during Step 1, and Step 3 removes their ability to bypass the
submission lifecycle.
Cancelling a submitted run uses the run-cancellation lifecycle and does not
rewrite submission history. A safe operator retry atomically creates a new
`queued` submission linked by `retry_of_submission_id` and `retry_root_id`; it
never changes `failed` back to `queued`. The retry command carries its own
bounded idempotency key, and repeating that command returns the same linked
submission. Uniquely constrain retry identity by workspace, failed-submission
ID, and retry-command idempotency key so concurrent retries cannot create
duplicates. A distinct deliberate retry command and idempotency key may create
another linked child from the same proven-safe failure; it must also carry a
new workspace-unique run identity and a fresh authority snapshot for the
retrying operator. This is an explicit operator action rather than automatic
retry. An ambiguous admission must first reconcile by the original
idempotency key/run identity and is not retryable until proven safe. Claims
apply only to `queued`; lease fields are valid only in `preparing` or
`admitting`. Every transition checks workspace, submission ID, claim
generation, and expected status atomically.

Command receipts retain the original status/owner/generation result fence for
every nonterminal fenced command, so replay can never return a newer fence.
Version one accepts command replay for seven days from `occurred_at`, permits
at most five minutes of future clock skew, rejects commands outside that
window, and incrementally prunes older receipts through an indexed bounded
delete. This keeps empty polls and lease renewals from growing receipt history
without bound while ensuring a pruned old command cannot mutate later work.
Unfiltered and status-filtered operator pages each have an index matching their
workspace, order, and cursor predicates.

Add a supervised `RunSubmissionSupervisor` with bounded global and
per-workspace concurrency. Workers claim with leases and fencing, perform
preparation and planning outside producer processes, and call `RunManager` for
the final local admission decision. A scheduler tick transactionally persists
the occurrence and submission intent, then returns without waiting for
planning or admission.

Make retries deterministic. A submission becomes `submitted` only after the
corresponding run is durably created/admitted; recovery must recognize an
already-created run rather than create a duplicate. Provide queue depth and
oldest-age visibility, bounded retry, cancellation, and operator read models.
Existing synchronous callers may wait for a finite terminal result or use an
explicit asynchronous API; neither path may bypass the durable lifecycle.

For #525, version-one fairness means bounded global concurrency plus an
explicit per-workspace concurrency cap and non-starving workspace selection.
It does not mean weighted priorities or the runner-task queue's later FIFO
policy.

Keep the two durable queues explicit:

| Queue | Producer | Consumer | Creates runner demand |
| --- | --- | --- | --- |
| `run_submissions` | API, operator, scheduler, backfill, recovery | control-plane submission workers | no |
| `runner_tasks` | admitted DAG/run orchestration | compatible runners | yes |

The implementations should share established PostgreSQL lease, fencing,
idempotency, and observability patterns, but not a generic status model.
Runner demand starts only after a submission has produced an admitted run and
currently runnable DAG nodes have become durable runner tasks.

### 3. Add the public runner-pool contract

Add `runner_pool/1` alongside `execution_pool/1` in:

- `apps/favn_authoring/lib/favn/asset.ex`;
- `apps/favn_authoring/lib/favn/dsl/asset_declarations.ex`;
- `apps/favn_authoring/lib/favn/sql_asset.ex`;
- `apps/favn_authoring/lib/favn/multi_asset.ex`;
- `apps/favn_authoring/lib/favn/pipeline.ex`.

Semantics:

- `pipeline ... runner_pool :pure_elixir` is the pipeline default;
- an asset-level `runner_pool` overrides the pipeline default;
- an omitted effective value resolves to `:default`;
- a multi-asset shared declaration applies to all child assets unless a child
  overrides it;
- the value is a non-nil atom in source and a bounded manifest-approved string
  at runtime;
- selection is exact; there is no implicit fallback to another pool.

Example:

```elixir
defmodule MyApp.Pipelines.Daily do
  use Favn.Pipeline

  pipeline :daily do
    runner_pool :pure_elixir
    assets [MyApp.Raw.Events, MyApp.Mart.LargeJoin]
  end
end

defmodule MyApp.Mart.LargeJoin do
  use Favn.SQLAsset

  runner_pool :duckdb
  # ...
end
```

Propagate and validate the resolved value through:

- `Favn.Asset`;
- `Favn.Pipeline.Definition`;
- `Favn.Pipeline.Resolver`;
- `Favn.Manifest.Asset`;
- `Favn.Manifest.Pipeline`;
- `Favn.Manifest.PipelineResolver`;
- `Favn.Manifest.Generator`;
- `Favn.Manifest.Compatibility`;
- `Favn.Manifest.Rehydrate`;
- `Favn.Manifest.Index`;
- `Favn.Assets.Planner`;
- `Favn.Plan` node maps;
- `Favn.Plan.NodeIdentity`;
- `Favn.Run.PipelineContext`;
- `Favn.Contracts.RunnerWork`;
- run snapshot and node-result codecs;
- manifest atom inventories.

Increment the current manifest schema from 13 to 14 here. Freeze and increment
the runner protocol from 12 to 13 only when the complete typed protocol is
added in section 4. Do not accept old versions in the final code. During the
reviewable migration, the old protocol remains an explicitly named internal
transition seam and protocol 13 is not activatable until both its contracts and
the coordinator/agent path are implemented. Phase 3 must reject deployment of
protocol-13 manifests before invoking the singleton runner path. Update the
manifest scalability fixture, deterministic hashes, golden manifests, and
runner release metadata.

Replace the manifest-wide singular `required_runner_release_id` with a bounded,
canonically sorted map:

```elixir
runner_releases: %{
  "pure_elixir" => "rr_elixir_...",
  "duckdb" => "rr_duckdb_..."
}
```

The customer-owned build/publish command accepts an explicit release ID for
every effective pool in the manifest. It rejects missing or extra pool keys,
invalid IDs, and a release artifact whose reported identity does not match the
map. Multiple pools may intentionally name the same release ID when they use
the same executable image. Different executable images must use different
release IDs.

Once the protocol-13 coordinator is enabled in Phase 4, manifest activation
validates the complete map against boot-frozen configured pools and published
runner-release metadata. The active manifest version and each immutable run
snapshot persist the map. Task creation resolves:

```text
effective asset runner_pool
        |
        v
run snapshot runner_releases[runner_pool]
        |
        v
frozen {runner_pool, required_runner_release_id} on runner task
```

Never resolve a task against whichever release is currently active after its
run or operation was created. A rollout publishes a new manifest with a new
map; old snapshots retain the old mapping until their work drains.

Add boot-frozen control-plane `runner_pools` configuration. It contains only
provider-neutral policy:

```elixir
config :favn_orchestrator,
  runner_pools: [
    default: [mode: :elastic, idle_grace_ms: 15_000],
    pure_elixir: [mode: :elastic, idle_grace_ms: 15_000],
    duckdb: [mode: :elastic, idle_grace_ms: 15_000],
    private_network: [mode: :resident]
  ]
```

Do not put Azure CPU, memory, image names, or prices in this configuration.
There is no fixed pool vocabulary or size enum. Pool names are author-selected
atoms converted through the manifest's bounded atom inventory; never create
atoms from runtime input. A pool has exactly one configured mode and grace
policy in version one, inherited by all of its releases, so resident and
elastic instances cannot register interchangeably within that pool.
Manifest activation fails if any effective runner pool is absent from the
boot-frozen configuration.

### 4. Add typed runner-task contracts

Replace `Favn.Contracts.RunnerClient` with explicit protocol structs under
`Favn.Contracts.RunnerTask`:

- `Registration`;
- `RegistrationAck`;
- `ClaimRequest`;
- `Assignment`;
- `NoWork`;
- `Wake`;
- `Started`;
- `LeaseRenewal`;
- `RuntimeInputsResolved`;
- `RuntimeInputsAck`;
- `LogBatch`;
- `LogAck`;
- `Result`;
- `ResultAck`;
- `Cancellation`;
- `Shutdown`.

Every struct gets:

- a version;
- bounded identity fields;
- `validate/1`;
- safe redaction;
- a payload-size limit;
- an explicit codec for PostgreSQL or API boundaries;
- exact accepted enums with no dynamic atom creation.

Every live-session message after registration requires a positive session
generation. Every task-scoped message additionally requires a positive
assignment generation. `ClaimRequest` has an idempotent command ID and carries
the runner's non-empty supported task kinds plus bounded string capabilities;
`Assignment` and `NoWork` echo that command ID. `RuntimeInputsAck` carries the
exact persisted resolution fingerprint. Protocol codecs reject compressed
external terms before decoding so an encoded-size bound cannot hide
decompression expansion.

Supported task kinds in the first version:

- `:asset_attempt`;
- `:relation_inspection`;
- `:generation_capabilities`;
- `:generation_marker_read`;
- `:generation_marker_initialize`;
- `:generation_activate`;
- `:generation_reconcile`;
- `:generation_discard`.

Keep type-specific payload and result structs. Do not replace them with an
unvalidated generic map. `%RunnerWork{}` remains the payload for
`:asset_attempt`, but gains the resolved `runner_pool` and no longer carries a
run-wide remote `manifest_lease_id`.

Add a task retry classification:

- `:safe_to_retry`: read-only work or a proven pre-side-effect failure;
- `:reconcile_before_retry`: operation has an idempotency/reconciliation
  protocol;
- `:unknown_do_not_retry`: a side effect may have occurred;
- `:terminal`: an explicit final outcome.

Reuse `RunnerError` safe-failure semantics. Never translate connection loss
alone into `:safe_to_retry`.

Task states are:

```text
queued -> assigned -> preparing -> running
   |          |           |          |
   |          +-----------+----------+--> cancelling
   |                                  \-> succeeded | failed | cancelled | unknown
   +------------------------------------> cancelled

assigned | preparing -- proven safe release --> queued with a new assignment generation
cancelling -- completion race -----------> succeeded | failed | cancelled | unknown
```

Only `succeeded`, `failed`, `cancelled`, and `unknown` are terminal. A domain
retry after a terminal asset failure stays in the existing durable run retry
lifecycle and creates a new asset-attempt task only when due. Assignment
recovery may immediately requeue the same task; the runner queue has no second
delayed-retry scheduler in version one.

### 5. Replace the execution ledger with queue persistence

The schema below is the required final state. To keep every reviewed checkpoint
buildable, Step 6 first adds the new tables beside the still-used legacy table
without dual writes. Callers migrate one path at a time. Step 23 deletes the
old path and rewrites the reset-only Storage V2 baseline to this final
inventory.

Add `FavnOrchestrator.Persistence.RunnerTaskStore` to
`Persistence.Stores`. Split the existing `RunOwnershipStore` so it keeps only
run ownership callbacks.

Add command/query/result modules for:

- enqueue task idempotently;
- claim the next compatible task;
- mark started;
- renew assignment;
- persist runtime-input resolution and acknowledge it;
- append a deduplicated log batch;
- persist a terminal result idempotently;
- request cancellation;
- acknowledge cancellation;
- release/requeue a safe assignment;
- mark an outcome unknown;
- page active tasks for one run;
- read one task by ID;
- read capacity demand by exact pool and release;
- claim a bounded expired-assignment recovery batch;
- reconcile demand counters.

Replace `runner_executions` with these tables:

#### `runner_tasks`

Important columns:

- `workspace_id`;
- `task_id`;
- `task_kind`;
- `run_id` and nullable operation identity;
- `asset_step_id`;
- `runner_pool`;
- `required_runner_release_id`;
- `required_capability`;
- `status`;
- `enqueued_at`;
- `deadline_at`;
- `payload_version`;
- `payload`;
- `payload_hash`;
- `assigned_runner_instance_id`;
- `assigned_runner_session_generation`;
- `assignment_generation`;
- `assignment_expires_at`;
- `cancellation_requested_at`;
- `last_command_id`;
- `result_version`;
- `result`;
- `error`;
- timestamps and terminal timestamp.

Use a primary key scoped by workspace and a bounded task ID format.
Treat `{workspace_id, task_id}` as the task key at every persistence and wire
boundary; never look up a runner-supplied task ID without its workspace.
Use checks for every enum, hash, count, time ordering, payload size, and
assignment-field combination.

Required indexes include:

- unique deterministic domain identity;
- claim index on
  `(runner_pool, required_runner_release_id, status, enqueued_at, task_id)`;
- active tasks by run;
- expired assignments;
- terminal retention.

Use `SELECT ... FOR UPDATE SKIP LOCKED` only inside the store implementation.
No orchestrator module should know PostgreSQL claim syntax.

#### `runner_capacity_demands`

Keep transactionally updated counts per `{runner_pool, release_id}`:

- `outstanding_count`;
- `queued_count`;
- `active_count`;
- `oldest_queued_at`;
- `version`;
- `updated_at`.

`outstanding_count` includes admitted queued, assigned, preparing, running, and
cancelling tasks. It excludes run retry timers, admission waiters, cancelled
tasks, and terminal tasks. Keeping assigned work in the value is intentional:
KEDA job scaling subtracts jobs that are already running.

Update the counter in the same transaction as every task transition. Add a
bounded reconciler with explicit `:audit` and `:repair` commands. Audit compares
counters with authoritative task rows and transactionally marks a mismatch
unhealthy without silently repairing it. While unhealthy, normal queue
mutations and demand reads fail closed. An explicit repair rebuilds the row and
returns it to healthy. The capacity endpoint must return `503`, never a false
zero, when its persistence projection is unavailable.

#### Migration cleanup

In Step 23, rewrite the reset-only Storage V2 baseline instead of leaving a
final migration that creates and then drops the old model:

1. replace `runner_executions` with the new queue tables in `CreateStorageV2`;
2. replace `runner_execution_id` with `runner_task_id` in the log baseline;
3. update identifier and payload hardening migrations for the new tables;
4. delete `OptimizeRunnerExecutionPagingV2` and remove it from the migration
   registry;
5. remove every obsolete runner-execution index, constraint, foreign key, and
   inventory entry;
6. update the exact Storage V2 table, column, index, constraint, migration, and
   definition-fingerprint inventories.

No data migration, drop-only compatibility migration, or compatibility view is
required because there are no production users. The migrator must reject the
obsolete definition fingerprint with an explicit reset-required error. Existing
development and test databases using that baseline must be reset.

### 6. Refactor run execution around durable tasks

Keep `RunServer` as the DAG owner for the one-control-plane version. Replace
direct submit/await behavior:

- `StageAdmission` performs existing capacity, circuit, materialization,
  package, and freshness checks;
- an admitted asset attempt is persisted as a runner task instead of dispatched;
- `RunServer` records task IDs in its execution state and returns to its receive
  loop;
- `RunnerTaskResultRouter` notifies the owning `RunServer` when PostgreSQL
  persists a terminal result;
- if the `RunServer` is absent, `RunRecovery` finds the durable terminal task
  and resumes the run;
- `StageResult` consumes the persisted typed result and performs existing
  freshness, materialization, circuit, generation, retry, and DAG transitions;
- completion of a stage enqueues only newly runnable downstream nodes.

Refactor these modules:

- `RunServer.Execution.StageAdmission`;
- `RunServer.Execution.Sequential`;
- `RunServer.Execution.StageResult`;
- `RunServer.Execution.RunExecutionState`;
- `RunServer.Execution.RunWorkSet`, renamed to `ActiveTaskSet`;
- `RunServer.Execution.StageEntry`;
- `RunServer.Cancellation`;
- `RunServer.Recovery`;
- `RunRecovery`;
- `RunExecutionCleanup`;
- `RunCancellation`;
- `RunManager`.

Replace `execution_id`/`runner_task_id` in orchestration state with
`task_id` and `assignment_generation`. A task ID identifies logical work; an
assignment generation identifies one attempt to run it on one process.

Admission ordering is important:

1. create/persist the planned step intent;
2. acquire existing execution and resource admission;
3. acquire the materialization claim;
4. attach the execution package;
5. enqueue the admitted task and increment demand;
6. start infrastructure through the observed metric;
7. assign to a runner;
8. resolve/persist runtime inputs if required;
9. acknowledge start and execute.

Tasks waiting for pipeline `max_concurrency`, an execution pool, a resource
circuit, or another materialization claim do not count as runner demand. Existing
admission waiter wakeups promote them when capacity is actually available.

The current execution lease TTL already expands around an attempt timeout. Keep
that property and ensure the queue coordinator renews orchestration leases while
an admitted task waits for a cold runner.

### 7. Make runtime-input resolution an assignment phase

`RuntimeInputPins.prepare/4` cannot call an arbitrary static runner before task
assignment.

For an asset task with runtime inputs:

1. assign the task to a compatible runner;
2. install/lease its exact manifest locally;
3. run `FavnRunner.RuntimeInputResolver`;
4. send `RuntimeInputsResolved` with the task and assignment generation;
5. persist the encrypted pin under the existing control-plane authority;
6. acknowledge with the exact persisted fingerprint;
7. only then allow `FavnRunner.Worker` to start.

The live runner retains and resends the same resolved payload until
acknowledged. A control-plane restart reads an existing pin and returns the same
ack. A stale assignment cannot overwrite a pin.

Refactor rebuild runtime-input freezing through the typed task gateway. Its
deterministic task ID must include the rebuild operation/action/item identity so
dispatcher restart observes the existing result rather than executing a second
resolution.

### 8. Install manifests per assigned runner

Remove activation-time registration in the singleton runner cache.

An assignment contains immutable manifest identity. The runner:

1. checks `ManifestStore.ensure/3`;
2. asks the control plane for the exact manifest only when missing;
3. verifies content hash, schema, runner contract, and release ID;
4. calls local `ManifestStore.acquire/4` with a task-scoped lease;
5. executes;
6. releases the lease after the terminal result is acknowledged.

The control plane loads the manifest through its existing bounded
`ManifestStore`; the runner never reads PostgreSQL.

Keep the current compiled manifest cache and eviction bounds initially. Add
transfer byte/count telemetry because elastic pools may reveal that full
manifest transfer is too expensive. Content-addressed manifest chunking is a
separate measured optimization.

Remove run-wide remote lease creation, renewal timers, and release calls from
`RunServer` and `RunServer.Execution`.

### 9. Add dynamic runner registration and wakeup

Add this control-plane supervision subtree before `RunRecovery` and the API:

- `RunnerRegistry`;
- `RunnerQueueSupervisor`;
- one `RunnerQueueCoordinator` per active pool/release;
- `RunnerClaimSupervisor`;
- `RunnerTaskResultRouter`;
- `RunnerTaskRecovery`;
- `RunnerCapacityReconciler`.

`RunnerRegistry` is process-local because this version has one control plane.
It stores only live session data:

- bounded runner instance ID;
- random boot ID;
- remote runner-agent PID;
- dynamic node name for diagnostics only;
- pool;
- runner release ID;
- runner contract version;
- supported task kinds/capability fingerprint;
- lifecycle mode;
- session generation;
- status `:idle`, `:claiming`, `:reserved`, `:busy`, or `:draining`;
- current workspace/task key and assignment generation.

Runner instance IDs come from `FAVN_RUNNER_INSTANCE_ID` when infrastructure
provides one, otherwise from a cryptographically random boot-local value. Do not
derive durable identity from a node atom. Validate and bound the string before
registration.

Registration validates exact pool, release ID shape, contract version,
capabilities, and one-slot support. It returns an opaque session generation and
the control-plane wait policy. Monitor the remote agent PID. On disconnect,
remove only the matching generation and schedule durable assignment recovery.

`RunnerTasks.claim/1` is a stateless facade:

- validates the live session and atomically moves that one-slot runner from
  idle to claiming for an idempotent claim-request ID;
- reads the compatible queue coordinator's current generation;
- performs one atomic compatible PostgreSQL claim in a supervised short-lived
  worker using the bounded repository pool;
- moves the exact claim request to reserved while assignment preparation
  completes;
- returns a typed assignment or registers the agent as an idle waiter against
  the generation it observed;
- retries immediately when the generation changed between an empty claim and
  waiter registration;
- still requires each woken runner to perform the atomic claim.

Duplicate claim requests return the same in-flight or completed claim outcome;
they cannot reserve a second task for the runner. A dead claim worker resets
only its matching claim request and leaves any committed task for fenced
recovery.

The per-pool/release `RunnerQueueCoordinator` owns only an in-memory queue
generation and eligible idle waiters. After a committed enqueue it advances the
generation and sends BEAM `work_available` hints. It never performs PostgreSQL
I/O, decodes task payloads, handles logs, or waits for execution. Therefore a
slow claim cannot block registration, another pool, result delivery, or API
traffic.

Do not broadcast to every runner when one task arrives. Wake at most the number
of new admitted tasks, prefer the longest-idle compatible sessions, and rely on
claim fencing for races.

Use PostgreSQL notifications only to wake the local coordinator after commits
that may originate outside its process. Add a slow bounded reconciliation tick
for missed notifications; do not make runners poll PostgreSQL or the control
plane continuously.

### 10. Replace the runner server with an agent

Refactor `FavnRunner.Application` supervision around:

- `ControlPlaneConnection`;
- `RunnerAgent`;
- `TaskExecutor`;
- `TaskResultBuffer`;
- the existing `WorkerSupervisor`, connection registry, plugin supervisor, and
  manifest store.

Remove the monolithic `FavnRunner.Server` public submit/await queue.

That removal is the final-state requirement. During the checkpoint sequence,
the old server remains only for the explicitly inventoried non-asset callers
until Steps 14-16 migrate them; Step 23 deletes it. The old and new paths never
execute or persist the same operation.

Runner boot:

1. validate immutable release and runner-pool configuration;
2. explicitly connect to the one configured control-plane node with bounded
   exponential backoff and jitter;
3. register the agent PID;
4. recover/reannounce an in-memory active task or terminal unacknowledged result;
5. claim one task;
6. execute at most one task;
7. report and wait for durable result acknowledgement;
8. claim again.

Result delivery is push-with-ack over BEAM. The runner keeps the complete
bounded result until the control plane confirms it was persisted. Receiving the
same result twice is idempotent. The runner must not exit during result delivery.

Logs become bounded, sequenced `LogBatch` messages tied to task ID and
assignment generation. The control plane deduplicates batches through the
existing log store. Use batch size/time thresholds so a small control plane does
not receive one message and one PostgreSQL transaction per log line. Permit
only a bounded number of unacknowledged batches per runner, acknowledge only
after persistence, and resend on reconnect. Preserve the existing explicit
truncation/drop marker when the bounded runner buffer fills.

Cancellation becomes a typed message to the assigned agent. The runner stops
the worker through its existing lifecycle, reports the proven cancellation
outcome, and preserves `native_cancel_unknown` when applicable.

### 11. Define assignment leases and failure semantics

An assignment lease protects against a dead runner holding a task forever; it
does not prove an external side effect did not occur.

- renew only the exact task, runner session, and assignment generation;
- renewal happens at a fraction of the lease TTL with jitter;
- control-plane or connection loss does not immediately reassign running write
  work;
- a reconnecting runner reports its exact active assignment and resumes only if
  PostgreSQL still recognizes it;
- an expired safe read/pre-start assignment may be requeued;
- an expired running write becomes `:unknown` unless its operation has an
  explicit reconciliation protocol;
- generation operations reuse their existing marker/token reconciliation;
- asset retries still require a `RunnerError` with `outcome: :safe_failure`;
- task and infrastructure retries can start the same runner program more than
  once, so all claims and reports must be idempotent.

On control-plane restart, `RunnerTaskRecovery`:

1. claims a bounded batch of expired or uncertain tasks;
2. checks the task retry classification and durable domain evidence;
3. requeues only proven-safe work;
4. invokes reconciliation for supported generation operations;
5. marks all other outcomes unknown and surfaces operator action.

On graceful runner shutdown:

- stop claiming;
- finish or cancel the current worker within the configured drain deadline;
- deliver and obtain acknowledgement for a terminal result when possible;
- report an explicit unknown interruption when the deadline wins;
- exit non-zero for configuration/boot failures and zero for clean idle
  completion.

### 12. Add elastic and resident wait policies

Runner configuration:

- `FAVN_RUNNER_POOL`;
- `FAVN_RUNNER_INSTANCE_ID` optional;
- `FAVN_RUNNER_NODE_HOST_ALIAS`, rendered into
  `RELEASE_NODE=undefined@<alias>` before BEAM starts;
- `FAVN_RUNNER_MAX_UPTIME_MS` for elastic jobs;
- one stable `FAVN_CONTROL_PLANE_NODE`;
- release, TLS distribution, cookie, and shutdown settings.

The configured logical-pool policy, not a runner assertion, owns
`elastic|resident` mode and idle grace. Every old/new release of that pool
inherits the same policy. Registration includes the runner's expected mode and
rejects a mismatch. This keeps one scale-down contract per pool during release
coexistence and avoids mixing never-exit and self-exiting instances.

The release launcher requires the host alias in production, validates it as a
bounded DNS-style string, and never derives it from the platform hostname.
Application runtime code must not attempt to repair the node name after
distribution has started.

An elastic runner stops claiming after its maximum uptime, drains its current
task/result, and exits even under continuous demand. The infrastructure hard
timeout must exceed runner maximum uptime plus the maximum supported task
timeout and shutdown grace. This keeps a platform timeout from being the normal
way a long-lived elastic job is recycled.

The control plane owns the response:

- `{:work, assignment}` when a task was claimed;
- `{:wait, milliseconds, queue_generation}` for an idle elastic runner;
- `{:wait, :infinity, queue_generation}` for a resident runner;
- `{:stop, reason}` when draining or incompatible.

For version one, `milliseconds` is the configured pool `idle_grace_ms`; it is
not a forecast. An elastic runner waits for a BEAM wake until that deadline,
performs one final atomic claim, and exits only if it still receives no work.
This final claim closes the wake/deadline race.

Keep the response contract general enough for a later measured wait policy to
return a shorter or longer grace. Do not implement `keep_warm_until`, a prewarm
horizon, historical duration estimates, or DAG forecasts now.

### 13. Add the capacity endpoint

Add an authenticated private route:

```text
GET /internal/runner-demand/:runner_pool/:runner_release_id
```

Response:

```json
{"outstanding": 7}
```

Requirements:

- `outstanding` is an unquoted non-negative JSON number at a stable path for
  infrastructure scalers;
- lookup is O(1) against `runner_capacity_demands`;
- pool and release are validated as bounded strings without atom creation;
- only exact configured pool/release pairs are accepted;
- use `Cache-Control: no-store`;
- return `503` when storage, schema, or counter reconciliation is unhealthy;
- never calculate or replan a DAG in the request;
- do not expose task payloads or workspace data;
- rate-limit and instrument it independently;
- authenticate through a least-privilege `capacity_reader` service credential,
  reusing `API.Authentication` and scoped service-token machinery;
- support secret rotation without granting the scaler general operator access.

Expose the same projection as an OpenMetrics gauge for infrastructure that
scales from metrics rather than HTTP:

```text
favn_runner_outstanding{runner_pool="duckdb",runner_release_id="rr_..."} 7
```

Keep labels bounded to configured active pool/release pairs. The endpoint and
gauge report total non-terminal runner-task demand, including tasks already
assigned or executing. A scaler such as KEDA subtracts its running jobs when
deriving how many new jobs to start.

Add an operator endpoint/read model for all pool/release demand, queue age,
registered idle/busy runners, and uncertain tasks. It is diagnostic and is not
the scalar KEDA contract.

### 14. Refactor manifest activation and every runner-bound operation

Change manifest deployment so that `Manifests.deploy/2` validates and persists
the manifest without requiring a live runner. It must validate:

- the manifest schema and runner protocol versions;
- every declared `runner_pool`;
- every referenced `runner_release_id`;
- every durable task kind the manifest can produce;
- target capabilities that can be checked without opening a physical target.

Delete activation-time registration into one runner cache. A runner installs a
manifest package into its local bounded cache only after claiming a task for
that manifest and release. The package identity remains content-addressed and
is verified before use.

Every non-asset task also receives one deterministic pool/release pair:

- relation inspection uses the effective pool of the asset that owns the
  relation/target;
- initial target generation uses the effective pool of the target-owning asset;
- every rebuild create, copy, validate, promote, and reconciliation task uses
  the effective pool frozen into the rebuild plan from the target-owning asset;
- an operation that is not associated with an asset or target must require an
  explicit author/operator `runner_pool`; it has no implicit cross-pool
  fallback.

At operation creation, persist both the selected pool and the release resolved
from the operation's manifest/run snapshot `runner_releases` map. All child and
retry tasks copy that pair; they never re-resolve against a later activation.
Reject a multi-target operation whose targets require different pools unless it
is already decomposed into independently pinned per-target tasks.

Move all current runner-bound operations through `RunnerTasks`:

- asset attempts from `StageAdmission` and `RunServer`;
- physical target inspection from `TargetCompatibilityPlanner`;
- initial target generation from `InitialTargetGenerationReconciler`;
- rebuild creation, copy, validation, and promotion from `Rebuilds` and
  `RebuildDispatcher`;
- any future operation that imports customer code or opens a target adapter.

Inspection becomes a deterministic task whose caller awaits a bounded durable
result. Preserve the explicit `physical_inspection_unavailable` outcome.
Generation and rebuild tasks persist their continuation before enqueueing.
Existing idempotency keys, target-generation tokens, promotion fences, and
reconciliation rules remain the authority for external side effects.

No orchestration path may call `:erpc` directly after this slice. Add a static
check for direct runner calls outside the new runner connection and task
protocol modules.

Replace destructive runner replacement with release-aware coexistence:

1. publish the new runner release and manifest;
2. enqueue new tasks against the new release;
3. keep old release capacity available for its already-pinned tasks;
4. remove the old infrastructure definition only after its non-terminal task
   count and registered-runner count are both zero.

Rollback reactivates the previous manifest and its frozen pool-to-release map.
New runs then pin the previous releases again, while tasks already created
under the newer manifest remain pinned and drain on the newer runners. Keep
both infrastructure definitions until each reports drained; never retarget an
in-flight task merely because activation moved backward. This coexistence
supports application/manifest rollback between runner releases that all speak
the current supported runner protocol and manifest schema. It does not promise
an in-place rollback to the deleted legacy protocol, control-plane binary, or
database baseline.

Mixed runner protocol versions in one queue partition are unsupported. A
protocol change creates a new release partition.

### 15. Redefine readiness, health, and diagnostics

The control plane is ready with zero runners when:

- PostgreSQL, schema, queue, and capacity counters are healthy;
- scheduler, run lifecycle, and API supervision trees are running;
- configured pool/release contracts are internally valid;
- active manifests reference known pool/release contracts.

Remove singleton runner connection, remote release, remote manifest-cache, and
remote execution-store checks from control-plane readiness. Zero registered
runners is normal. Alert on queue age or an unsatisfied demand duration rather
than making the control plane unready.

Expose metrics and diagnostics for:

- outstanding demand, oldest queued age, and enqueue/claim throughput by
  pool/release;
- registered, idle, busy, draining, and disconnected runners;
- assignment lease expiry and stale-generation rejections;
- safe requeues, unknown outcomes, result redeliveries, and log retries;
- manifest transfer/cache hit/cache eviction;
- enqueue-to-start, boot, registration, preparation, execution, and
  result-acknowledgement latency.

Keep readiness bounded and fail closed on a queue/counter inconsistency.
Diagnostics may inspect more state, but must not be called by a scaler.

### 16. Remove the singleton-runner architecture and terminology

Delete, rather than deprecate, the legacy modules and contracts once their
callers have moved:

- `Favn.Contracts.RunnerClient`;
- `FavnOrchestrator.RunnerClient.BeamNode`;
- `FavnOrchestrator.RunnerClientValidator`;
- `FavnOrchestrator.RunnerDispatch`;
- `FavnOrchestrator.RunnerHealth`;
- singleton `RunnerDiagnostics`;
- `RunnerManifestRegistration`;
- `ActiveManifestReconciler`;
- `RunnerLogBridge`;
- `RunnerReplacement`;
- `RunExecutionOwnership`;
- `RunnerExecutionIdentity`;
- `FavnOrchestrator.Storage.ExecutionOwnershipCodec`;
- `FavnRunner.Server` submit/await/result-retention APIs;
- `FavnRunner.ResultRetention`;
- `OptimizeRunnerExecutionPagingV2` and obsolete baseline branches;
- run-wide remote manifest lease timers.

Delete the `runner_executions` schema, store capabilities, commands, results,
indexes, codecs, and tests after `runner_tasks` is authoritative. Remove the
runner-replacement public API and facade. Remove runtime runner-client options,
`FAVN_RUNNER_NODE` as a control-plane destination, and generated configuration
that assumes one runner.

Rename the identifier everywhere from `runner_execution_id` to
`runner_task_id`, including:

- log events and node results;
- materialization and run projections;
- API filters, DTOs, views, and operator output;
- cancellation and retry commands;
- `inflight_execution_ids`, which becomes `active_runner_task_ids`.

Remove `runner_ref`; pool, release, task, attempt, and assignment generation
are the explicit identities. Replace singleton `available|busy` wording with
queue and registry states.

Because there are no users, do not add aliases, dual writes, compatibility
tables, environment fallbacks, or tests for the deleted path. Add a CI check
that rejects the deleted module names, environment variable, table name, and
identifier outside archived historical documents.

### 17. Preserve a production-shaped local development loop

Local development still starts one control plane and one resident default
runner, but it uses the same registration, claim, preparation, result, and log
protocol as production. The only local exceptions are loopback networking and
permission to use plain distribution when explicitly in development.

Refactor:

- `FavnLocal.Config`;
- `RunnerMain`, `RunnerChild`, `RunnerLifecycle`, and `RunnerLocator`;
- local preflight, doctor, startup output, and reload handling.

Local runner readiness means registered and compatible, not callable through a
singleton client. Preserve the customer-code fingerprint boundary. On reload,
build a new runner release, publish its manifest, start its resident runner,
and drain the previous release using the same release-overlap rules.

`.favn/local` may record how a runner reaches the control plane. It must not
become a callable runner address consumed by orchestration.

### 18. Supply provider-neutral and reference deployment contracts

Favn core requires only:

- a stable control-plane DNS name;
- outbound runner connectivity to the control plane;
- authenticated TLS distribution in production;
- access to the private capacity endpoint for an external scaler;
- one independently scalable job definition per pool/release;
- one task slot per runner in version one;
- process exit as the scale-down signal;
- infrastructure retry and maximum-run-time settings compatible with Favn's
  durable claim and recovery rules.

Favn must not call Azure, Kubernetes, KEDA, VM, or autoscaling APIs.

#### Azure Container Apps reference

Recommend:

- one control-plane Container App with minimum and maximum replicas set to one;
- private HTTP ingress for the API and fixed private raw TCP ports for EPMD and
  distributed Erlang;
- one event-driven Container Apps Job per pool/release, with the pool's CPU and
  memory allocation;
- minimum executions zero, job parallelism one, completion count one, and
  platform retry zero;
- a job timeout greater than runner maximum uptime plus the maximum supported
  task duration and drain grace;
- a KEDA Metrics API rule with target value one that reads the exact
  pool/release capacity endpoint;
- an initial five-second polling interval, made deployment-configurable and
  retained only if the control-plane and managed-scaler load test passes;
- private networking and independently rotatable capacity-reader and
  distribution credentials.

Container Apps Jobs have no ingress, which fits the outbound runner-to-control
plane design. Treat raw TCP routing, EPMD/distribution port pinning, managed
KEDA `metrics-api` support and authentication, DNS, and TLS certificate rotation
as acceptance-test items. Record the managed KEDA version observed in the
test. Do not claim this topology is supported until the real Azure deployment
test passes.

Create separate old and new job definitions during a runner release rollout.
Their capacity endpoints remain release-specific. Delete an old definition
only after Favn reports it drained.

If a pool is intentionally resident on Azure, run it as a separately configured
Container App with a fixed minimum/maximum replica count and no public ingress,
not as a never-ending Job.

#### Kubernetes/KEDA reference

Recommend one KEDA `ScaledJob` per pool/release with:

- `minReplicaCount: 0`;
- pool-specific `maxReplicaCount`;
- Metrics API target value one;
- `pollingInterval: 5`, subject to the same measured deployment override;
- Job parallelism and completions set to one;
- pod restart policy `Never` and Job backoff limit zero;
- an active deadline greater than runner maximum uptime plus maximum task
  duration and drain grace;
- gradual rollout while an old release drains;
- network policy allowing only runner-to-control-plane distribution traffic.

Kubernetes documents that even a Job with one completion and no retries can
start the same program twice in some failures. The durable claim and assignment
fence therefore remain mandatory; infrastructure settings are not a
correctness mechanism.

On Kubernetes, resident runners use a `Deployment`; elastic runners use Jobs.
AWS can map elastic pools to one-task ECS tasks and resident pools to ECS
services. Nomad can map elastic pools to batch jobs and resident pools to
service/system jobs. In either case an external event scaler or controller
reads the same demand projection, and clean runner exit completes the
allocation. A resident process supervisor maps to resident mode, an elastic job
controller maps process exit to scale-down, and a VM scaler may terminate an
instance after the runner exits. These are deployment adapters around the same
Favn contract; they are not Favn core integrations.

### 19. Build the verification matrix

#### Focused unit and contract tests

Cover:

- pool DSL validation and safe bounded string handling;
- exact pool-to-release map completeness, canonicalization, hashing, snapshot
  freezing, rollout coexistence, and rejection of mismatched artifacts;
- manifest and plan serialization/fingerprints;
- task, registration, assignment, wake, log, and result protocol round trips;
- wait deadline plus final-claim race handling;
- retry classification and explicit unknown outcomes;
- capacity-counter state transitions;
- stable FIFO claim ordering.

#### PostgreSQL integration tests

Prove:

- concurrent runners cannot successfully claim the same task generation;
- a stale generation cannot append logs, heartbeat, complete, or cancel;
- enqueue, log batches, and result acknowledgement are idempotent;
- task transition and capacity-counter update commit or roll back together;
- expiry requeues only safe tasks and marks uncertain external work unknown;
- `FOR UPDATE SKIP LOCKED` claims do not convoy;
- FIFO ordering is stable under concurrent claims;
- cancel, complete, disconnect, timeout, and recovery races are fenced;
- counter reconciliation fails closed and repairs only from authoritative rows;
- indexes and query plans remain bounded at target queue volume;
- fresh bootstrap, rejection of the obsolete fingerprint, restore, and
  runtime-role grants all include the new tables and functions.

#### Distributed BEAM integration tests

Use OTP `:peer` to start many hidden dynamic-name runners. Verify:

- runners initiate connections to the stable control plane and do not form a
  peer mesh;
- all ephemeral runners use the bounded node-host alias and a high-churn test
  leaves control-plane `atom_count` and known-node state within the measured
  bound after delayed node-table collection;
- registration rejects duplicate instance IDs and mismatched pool, release,
  protocol, schema, or manifest support;
- monitors remove dead registry entries without changing durable task truth;
- runners reconnect after control-plane restart;
- duplicate delivery and result resend are harmless;
- disconnects during claim, preparation, execution, and acknowledgement take
  the documented recovery path;
- missed wake hints do not lose work;
- a wake reaches only eligible idle runners;
- resident and elastic exit policies behave independently.

Run a production-mode TLS-distribution case with mutual certificate
verification. A cookie-only case must fail production configuration.

#### Orchestrator and lifecycle tests

Verify:

- accepted API, scheduler, backfill, and recovery submissions persist through
  process and control-plane restart;
- scheduler ticks persist occurrence and submission intent without waiting for
  planning;
- submission retries and recovery cannot create a second run;
- one admitted submission creates runner tasks only as DAG nodes become
  runnable;
- queued or preparing submissions do not contribute to runner demand;
- one durable task is created for each admitted asset-node attempt;
- pipeline `max_concurrency: 2` never creates more than two outstanding admitted
  asset tasks for that run;
- blocked downstream DAG nodes do not create demand;
- arbitrary user-defined pools such as `pure_elixir`, `duckdb`, and
  `private_network` remain isolated;
- readiness succeeds with zero registered runners;
- queued tasks survive control-plane restart and `RunServer` recovery;
- runtime inputs are pinned before the worker receives customer code;
- inspection, generation, and rebuild tasks use the same queue;
- cancellation, retry, and release drain reach one durable terminal outcome;
- old and new releases coexist without cross-claiming.

#### Scale and performance tests

Exercise three pools at 0, 1, 10, and 100 real distributed runner processes
against the production gateway and PostgreSQL queue. A local peer node may host
the scale processes; separately boot a fresh peer with the production
`RunnerAgent` for cold-start qualification. Measure:

- capacity endpoint latency and database reads;
- claim throughput and queue-age distribution by workspace;
- runner registration/reconnect storms;
- log and result backpressure;
- control-plane scheduler and mailbox pressure;
- cold-start phase breakdown.

The capacity endpoint must stay O(1). Record an evidence-based startup service
level after deployment testing. Under normal available platform capacity, P95
time from a durably runnable task to persisted `Started` acknowledgement must
remain below five minutes. Treat 30 seconds as a measured optimization target,
not a product promise.

#### Deployment acceptance tests

Run the same black-box contract against local fake infrastructure and local
Kubernetes when available. Run it against Azure Container Apps only with
explicitly provided credentials, resource scope, spend authorization, and
cleanup ownership. A live managed-Kubernetes run is optional production
qualification, not a PR requirement. Never infer permission to create paid
resources.

1. scale from zero to N across at least three pools;
2. keep an idle runner through its grace when work arrives;
3. scale N to zero by clean process exit;
4. reject an incompatible release;
5. duplicate and kill runners at each assignment phase;
6. restart the control plane with queued and running tasks;
7. rotate TLS, cookie, and capacity API credentials;
8. verify DNS, EPMD, fixed distribution ports, and private network boundaries.

Record local/artifact conformance separately from live-platform qualification.
Do not describe Azure, Kubernetes, or another provider as supported until that
provider's live suite has passed on the exact release.

### 20. Update the canonical documentation

Update these point-in-time documents in the same program:

- `FEATURES.md` and `ROADMAP.md`;
- `docs/structure/favn_core.md`, `favn_authoring.md`,
  `favn_orchestrator.md`, and `favn_runner.md`;
- `docs/production/README.md`;
- `deployment_topology.md`;
- `control_plane_environment.md` and `control_plane_image.md`;
- `network_and_proxy.md`;
- `runner_releases.md`;
- `secret_rotation.md`;
- `upgrade_and_rollback.md`;
- `apps/favn/guides/configuration.md` and affected public module documentation;
- `apps/favn/lib/favn/ai.ex` when public authoring or operating guidance changes.

Document:

- the difference between the scheduler queue and the runner task queue;
- `execution_pool` admission versus `runner_pool` compute selection;
- scale-to-zero and capacity-value semantics;
- elastic and resident runner modes;
- why Jobs, not long-lived Services, are the elastic unit;
- release overlap and drain;
- unknown outcomes and operator recovery;
- the provider extension contract;
- the intentionally deferred optimizations.

Each architecture document must state the implementation date and supported
baseline so that future readers do not mistake this plan for current behavior.
The roadmap and issue #529 must distinguish the work: this program delivers
multi-runner scale behind one control plane; multi-control-plane availability
remains a separately designed later item. Production TLS distribution protects
the network connection but still grants connected BEAM nodes broad trust.
Issue #530 therefore remains the later least-privilege/untrusted-runner and
credential-rotation design, not a reason to postpone encrypted distribution.

### 21. Deliver in gated implementation phases

Deliver the complete program as one pull request from one long-lived feature
branch. Push approved phase commits to that branch as recoverable GitHub
checkpoints, but do not open the pull request until the final gate passes. Do
not use stacked branches or separate implementation pull requests.

#### Branch and review workflow

Before implementation:

1. preserve unrelated work and start a clean worktree from updated
   `origin/main`;
2. create one dedicated feature branch;
3. record the clean baseline SHA and run only the focused baseline checks needed
   to prove the environment works;
4. commit the accepted implementation plan as the first reviewed checkpoint if
   it is not already on the branch.

Create `docs/architecture/elastic-runners-implementation-log.md` on that branch.
For each phase checkpoint it records scope, focused verification
commands/results,
reviewer verdict and findings resolved, tree SHA, commit SHA, pushed branch CI,
and any explicit external-test limitation. It must never contain credentials,
secret values, or private infrastructure details.

The 26 numbered steps below are detailed implementation tasks, not 26
independent review gates. Execute them in order inside these seven phases:

| Phase | Detailed steps | Review boundary |
| --- | --- | --- |
| 1. Foundation | 0-1 | Isolated baseline and durable submission persistence |
| 2. Durable run submission | 2-3 | All producers use bounded asynchronous submissions |
| 3. Pool, release, protocol, and demand contracts | 4-7 | Frozen routing/protocol contracts and durable task demand |
| 4. Durable DAG and distributed runner core | 8-13 | Asset work executes safely through dynamic runners |
| 5. Remaining execution-path migration | 14-16 | Inspection, target generation, and rebuilds use runner tasks |
| 6. Operations and infrastructure deployment | 17-22 | Lifecycle, coexistence, security, and deployment references |
| 7. Legacy removal and final qualification | 23-25 | Final architecture, qualification, umbrella suite, and PR |

Phase 1 was completed through separately reviewed Step 0 and Step 1
checkpoints before this phase workflow was adopted. Those existing immutable
commits and their evidence satisfy the Phase 1 gate; do not rewrite or squash
them. Six phase review gates remain. In Phase 7, the final review and
commit/push sequence required by Step 25 is the phase review gate, not an
additional second review.

For every detailed implementation step within a phase:

1. implement only that step's stated scope;
2. update affected tests and current documentation in the same diff;
3. run focused tests plus clean compilation for every affected application;
4. keep the diff within the current phase and record material design decisions
   or temporary seams for the phase review;
5. do not require a separate independent review, content commit, push, CI wait,
   or evidence commit merely because one detailed step is complete.

At the end of every phase:

1. run the combined focused verification for every affected application and
   all exit gates in that phase;
2. ask a read-only sub-agent to review before committing; the reviewer must not
   edit files or implement its own findings;
3. require the sub-agent to read this complete implementation plan, inspect the
   exact diff from the previous approved phase checkpoint including every
   untracked file, read the affected code and tests, and compare the result with
   every detailed step and exit gate in the phase;
4. have the reviewer report correctness, concurrency, durability, security,
   BEAM/OTP design, test coverage, legacy-path, and documentation findings by
   severity;
5. resolve every actionable finding, rerun focused verification, and repeat
   review until the sub-agent explicitly approves the phase;
6. record the reviewed tree hash, commit the approved phase with a coherent
   message, verify the commit tree exactly equals the reviewed tree and the
   worktree is clean, then push the feature branch to GitHub as a checkpoint;
7. wait for any branch CI triggered by the push; resolve failures through the
   same phase review loop;
8. after CI passes, update the implementation log with the approved content
   commit SHA, verification, and reviewer verdict; have a sub-agent review that
   evidence-only diff, commit/push it as the phase evidence commit, and then
   begin the next phase.

The evidence commit does not attempt to record its own SHA, which would be
self-referential. The next phase compares its implementation diff from the
previous evidence commit. The content commit is the immutable reviewed tree;
the following evidence commit contains only the review/CI record for it.

Do not commit a known review failure. Do not rewrite or force-push approved
checkpoint commits. If a later phase exposes an earlier defect, fix it in a new
reviewed commit so checkpoint history remains useful. The implementing agent
retains responsibility for the result; sub-agent approval is an additional
quality gate, not a substitute for tests or judgment.

After the first approved checkpoint, incorporate upstream changes by merging
`origin/main`, never by rebasing or rewriting checkpoint commits. The merge diff
must remain part of the current uncommitted phase diff, receive focused
verification, and be included in that phase's single read-only review. It does
not create a separate review gate.

If implementation reveals a missing contract, changes an accepted architecture
decision, expands a phase materially, or invalidates a later exit gate,
stop code work. Update this plan, have a sub-agent re-read and approve the full
amendment, commit/push that plan-only checkpoint, and only then resume
implementation.

Temporary internal adapters are allowed only when the detailed step records
exactly which later step removes them. No public compatibility layer,
dual-write path, or legacy fallback may be introduced. Because there are no
users, each replacement should converge directly on the final contract.

Phase 3 has two compile-only seams for the still-unmigrated execution path:

- `Favn.Manifest.Version.required_runner_release_id` mirrors only the
  `"default"` entry of canonical `runner_releases`, and
  `transitional_default_release/1` lets the old singleton transport remain
  buildable. It does not decode an old manifest schema or accept a missing
  canonical release map. Steps 14-16 remove the remaining operation callers;
  Step 23 deletes the field and helper.
- `FavnOrchestrator.RunState.required_runner_release_id` mirrors the default
  release while the old immediate asset-dispatch path still exists. Step 8
  moves DAG execution to per-node pool/release runner tasks and removes this
  singular run field and its construction fallback.

Neither seam is serialized as the canonical manifest contract, exposed as a
new compatibility API, or dual-written to the new runner-task queue. The Phase
7 legacy search gate must prove both are gone.

#### Stepwise implementation

#### Phase 1: Foundation

##### Step 0: Isolate the branch and accept the baseline

- Create the clean worktree and single feature branch from updated
  `origin/main`.
- Bring in this accepted plan without unrelated working-tree changes.
- Inventory current singleton-runner modules, storage, configuration, call
  sites, tests, and documentation that must disappear.
- Record focused baseline commands, toolchain versions, PostgreSQL readiness,
  and any pre-existing failures.

Exit gate: the branch contains only the reviewed plan and baseline inventory,
the required local services are usable, and the reviewer agrees that every
legacy path has a named removal step.

##### Step 1: Add the durable run-submission persistence contract

- Add #525's `RunSubmissionStore`, schema, statuses, commands, claims, leases,
  fencing, idempotency, cancellation, retry metadata, and read models.
- Rewrite the reset-only Storage V2 baseline and exact schema inventories
  directly; do not add compatibility migrations.
- Add focused codec, store, PostgreSQL concurrency, stale-generation,
  idempotency, query-plan, and recovery tests.

Exit gate: durable submissions can be enqueued, claimed, transitioned,
recovered, cancelled, and inspected without invoking planning or a runner.

#### Phase 2: Durable run submission

##### Step 2: Add bounded run-submission workers

- Add supervised workers with bounded global and per-workspace concurrency,
  non-starving workspace selection, claim renewal, recovery, and shutdown.
- Perform preparation/planning outside producer and scheduler processes.
- Keep `RunManager` as final local admission owner and make
  submission-to-run handoff idempotent.
- Add focused restart, lease-expiry, duplicate-run, saturation, and fairness
  tests.

Exit gate: a persisted submission can reach one durable run through bounded
workers, restart safely at every state, and never create a duplicate run.

##### Step 3: Move every run producer onto asynchronous submissions

- Move API/operator, scheduler, backfill, rebuild, recovery, and child-run
  producers onto the one durable lifecycle.
- Make scheduler ticks atomically persist occurrence plus intent and return
  immediately.
- Add queue depth, oldest-age, retry, cancellation, and failure visibility.
- Delete synchronous producer bypasses only after all their callers move.

Exit gate: accepted requests survive process/control-plane restart and
repository search proves no producer bypasses `run_submissions`.

#### Phase 3: Pool, release, protocol, and demand contracts

##### Step 4: Add arbitrary pool and pool-to-release manifest contracts

- Add `runner_pool/1` defaults/overrides across public DSLs and propagate opaque
  bounded identifiers through definitions, manifests, plans, snapshots,
  fingerprints, rehydration, and atom inventories.
- Replace the singular manifest release with the exact, canonical
  `runner_releases` pool-to-release map and update build/publish/activation.
- Add boot-frozen provider-neutral pool policy with one mode/grace inherited by
  every release of that pool
  and 15-second default elastic grace.
- Bump manifest schema only; reject old manifests in the final code.

Exit gate: arbitrary configured names map deterministically to exact immutable
releases, different images can coexist in one manifest, and runs freeze the map.

##### Step 5: Freeze the complete runner protocol 13 contracts

- Define all registration, compatibility, claim, assignment, lease, wake,
  preparation, log, result, cancellation, wait/stop, and acknowledgement
  messages before implementing transport.
- Include workspace/task/session/assignment fencing identities and bounded
  validation in every applicable contract.
- Increment runner protocol 12 to 13 here and keep it non-activatable until the
  new coordinator and agent paths satisfy the contract.

Exit gate: protocol 13 is complete, versioned, round-trip tested, and will not
change semantically in later checkpoints without a reviewed plan amendment.

##### Step 6: Add runner-task persistence and atomic claims

- Add `runner_tasks`, deterministic task identity, FIFO `SKIP LOCKED` claims,
  assignment generations/leases, idempotent logs/results, cancellation, and
  recovery queries.
- Keep the legacy `runner_executions` table/path temporarily for unmigrated
  callers; do not dual-write or expose a public compatibility API.
- Add store, codec, concurrency, stale-generation, query-plan, and recovery
  tests.

Exit gate: new-store clients cannot double-claim or advance stale assignments,
while the still-unmigrated old path remains buildable and tested.

##### Step 7: Add transactional demand projection

- Add `runner_capacity_demands` and update it in the same transaction as every
  new runner-task transition.
- Implement bounded reconciliation, projection health, and fail-closed stale
  behavior.
- Prove total outstanding/queued/active counts and oldest age through race,
  rollback, reconciliation, and query-plan tests.

Exit gate: demand is O(1), transactionally consistent, and never reports a
known-false zero.

#### Phase 4: Durable DAG and distributed runner core

##### Step 8: Refactor DAG admission to durable asset tasks

- Keep `RunServer` as the one-control-plane DAG owner.
- Enqueue admitted currently runnable asset attempts through the new store,
  freezing their pool/release from the run snapshot.
- Preserve execution/pipeline admission, retries, materialization rules, and
  downstream DAG transitions; persist continuations before enqueue.
- Leave unmigrated non-asset operations on the named legacy seam.

Exit gate: one task exists per admitted asset attempt, blocked nodes create no
demand, and only durable results make downstream work runnable.

##### Step 9: Add dynamic runner registration and coordination

- Add process-local registry, session generations, monitors, compatibility
  checks, targeted wakeups, and bounded reconciliation.
- Use hub-and-spoke hidden dynamic BEAM nodes and explicit process messaging;
  do not add a mesh, `:global`, or `:pg`.
- Test reconnect, duplicate registration, presence loss, missed notifications,
  wake races, and bounded node/atom churn with OTP `:peer`.

Exit gate: zero or many compatible runners can register/disappear without
changing durable truth or leaking unbounded identities.

##### Step 10: Add the one-slot runner agent

- Add `ControlPlaneConnection`, `RunnerAgent`, `TaskExecutor`, and
  `TaskResultBuffer` with bounded reconnect backoff/jitter.
- Implement protocol-13 register, pull, execute, persist-before-ack result, and
  claim-again flow for the migrated asset path.
- Keep the old server only for explicitly inventoried non-asset callers; do not
  remove its API yet.

Exit gate: a protocol-13 runner executes at most one new task and duplicate or
reconnected messages remain idempotent while old callers still compile.

##### Step 11: Move manifest installation and runtime inputs to assignment

- Install/verify content-addressed manifests after claim rather than activation.
- Resolve and pin runtime inputs immediately before assignment without
  persisting secrets in task payloads.
- Preserve release identity, bounded caches, and customer-code fingerprints.

Exit gate: activation needs no runner and customer code starts only after its
exact manifest and secret-safe runtime inputs are prepared.

##### Step 12: Complete logs, results, and backpressure

- Route asset execution fully through the agent.
- Add bounded sequenced log batches and terminal results with
  persist-before-ack, deduplication, retry, reconnect, and truncation markers.
- Enforce bounded buffers and mailbox/backpressure telemetry.

Exit gate: lost links and duplicate delivery cannot lose/duplicate durable
logs or results, and neither side grows without a configured bound.

##### Step 13: Add cancellation, leases, recovery, and uncertainty

- Implement renewal, expiry, graceful drain, cancellation, safe requeue,
  reconciliation, and explicit unknown outcomes.
- Preserve safe-failure classification and native-cancel uncertainty.
- Test every claim/preparation/execution/ack/cancel/timeout/reconnect race.

Exit gate: only proven-safe work retries and stale runners cannot affect newer
assignments.

#### Phase 5: Remaining execution-path migration

##### Step 14: Move relation inspection onto runner tasks

- Route relation/target inspection through tasks pinned to the target-owning
  asset's frozen pool/release.
- Persist the awaiting continuation and preserve
  `physical_inspection_unavailable`.

Exit gate: inspection uses no direct runner RPC and restart resumes its durable
result wait.

##### Step 15: Move initial target generation onto runner tasks

- Route create/marker/reconciliation operations through the target-owning
  asset's frozen pool/release.
- Preserve generation tokens, external-outcome classification, and recovery.

Exit gate: generation uses no legacy RPC and failure injection proves safe
retry versus explicit uncertainty.

##### Step 16: Move rebuild operations onto runner tasks

- Freeze pool/release in each rebuild plan and copy it to create/copy/validate/
  promote/reconcile tasks.
- Preserve promotion fences, idempotency, cancellation, and recovery.
- Reject cross-pool aggregate work unless decomposed into pinned per-target
  tasks.

Exit gate: rebuilds use no legacy RPC and each phase resumes safely after
process/control-plane loss.

#### Phase 6: Operations and infrastructure deployment

##### Step 17: Add lifecycle modes, demand API, readiness, and observability

- Add elastic fixed grace plus final claim and resident infinite wait from pool
  policy.
- Add the authenticated O(1) demand endpoint and bounded OpenMetrics gauge.
- Redefine readiness for zero runners and add queue, runner, lease, unknown,
  result/log, and latency diagnostics/alerts.

Exit gate: scalers can read exact pool/release demand, runners self-exit without
losing work, and zero runners remains ready.

##### Step 18: Add release coexistence and drain

- Route new work through the newly activated pool-to-release map while old
  snapshots/tasks retain their previous map.
- Run old/new definitions concurrently and expose explicit drain evidence.
- Prove rollback by reactivating the previous manifest: new work returns to its
  previous releases while already-pinned newer work drains unchanged.
- Remove destructive singleton replacement.

Exit gate: releases cannot cross-claim and operators know exactly when old
infrastructure is safe to remove.

##### Step 19: Rebuild local development on the production protocol

- Refactor `mix favn.dev`, doctor, preflight, runner lifecycle/locator, reload,
  and startup output around registration and pull.
- Keep one resident default runner locally using the production task/result/log
  and release-drain paths.
- Allow plain distribution only for explicit loopback development.

Exit gate: the normal local workflow works without a singleton-protocol
shortcut.

##### Step 20: Add production transport security and neutral deployment contract

- Configure mutual TLS distribution, fixed ports, bounded aliases, private
  networking, scoped demand credentials, and rotation.
- Add provider-neutral job/resident contracts without provider SDKs.
- Recheck current OTP and infrastructure documentation.

Exit gate: production rejects cookie-only/plain distribution and conformance
tests prove the neutral demand/self-exit contract.

##### Step 21: Add the Azure Container Apps reference

- Add Container App/Jobs, networking, KEDA metric, release-overlap, credential,
  timeout, and pool-specific reference artifacts.
- Test artifact generation and scaler/runner behavior locally with fake
  infrastructure boundaries.
- A real Azure deployment requires available credentials, an agreed
  subscription/resource scope, and explicit user authorization for spend. If
  unavailable, record it as an external production-qualification gate rather
  than creating resources or blocking code correctness.

Exit gate: Azure artifacts pass local conformance; any authorized live result
is recorded, and Favn is not described as Azure-qualified without that result.

##### Step 22: Add Kubernetes/KEDA and other platform references

- Add Kubernetes Job/Deployment, KEDA, network policy, timeout, and
  release-overlap reference artifacts.
- Qualify them with local `kind`/equivalent when the toolchain is available;
  managed-cluster spend is not required for this PR.
- Document ECS, Nomad, resident-process, and VM-scaler mappings without adding
  core integrations.

Exit gate: artifacts/conformance tests share the same provider-neutral contract
and external limitations are explicit.

#### Phase 7: Legacy removal and final qualification

##### Step 23: Remove legacy architecture and finish canonical docs

- Delete the old server/submit API now that every caller has moved, then remove
  `runner_executions` and rewrite the reset-only baseline to the final schema.
- Delete every other singleton module, identifier, environment variable,
  compatibility version, fallback, and obsolete test listed in this plan.
- Update all canonical/public/operator/production docs and add static
  legacy-symbol checks.

Exit gate: exact repository and schema inventories find no live legacy path and
focused affected suites pass.

##### Step 24: Integrate main and run qualification on the final tree

- Fetch and merge current `origin/main` without rebasing; keep the merge
  uncommitted and include it in the final Phase 7 verification and review.
- Run section 19's focused PostgreSQL, distributed-BEAM, recovery, security,
  artifact-conformance, and performance matrix.
- Run 0/1/10/100 distributed runner-process scale against the real gateway and
  PostgreSQL queue. The infrastructure autoscaler remains locally simulated;
  live-cloud scale is bounded by explicit cost authorization and is never
  started implicitly.
- Verify query plans, mailboxes, buffers, atom/node state, and cold-work P95.

Exit gate: the exact merged tree has recorded qualification evidence and no
unresolved code, security, durability, performance, or documentation finding.

##### Step 25: Final umbrella gate and single pull request

Only after Phases 1-6 are approved and pushed and Steps 23-24 have satisfied
their exit gates:

1. confirm `origin/main` has not moved since Step 24; if it has, merge and repeat
   affected qualification before continuing;
2. run formatting, static checks, clean compilation, and the full umbrella test
   suite against that same qualified tree;
3. fix local failures without creating intermediate checkpoint commits and
   rerun the complete final gate until it passes;
4. inspect the full branch diff/history for unrelated changes, secrets,
   temporary seams, generated artifacts, skipped tests, and legacy symbols;
5. obtain the one final read-only sub-agent plan-to-implementation approval;
   this is the single Phase 7 review gate;
6. commit and push the exact approved content tree, then wait for branch CI;
7. if branch CI fails, fix the failure, repeat the affected local qualification
   and the Phase 7 review, commit and push a new approved content tree, and wait
   again;
8. after branch CI passes, append the Phase 7 implementation-log evidence,
   obtain read-only review of that evidence-only diff, and commit/push the
   evidence checkpoint;
9. create the single pull request with checkpoint, verification, CI, and
   external-qualification evidence.

Do not create the pull request while a required local suite or review finding
is unresolved or before the Phase 7 evidence checkpoint is pushed. CI after PR
creation is an additional gate and any CI-only fix uses a new reviewed commit.

Issue #525 is the first slice of the same production program, not an optional
parallel project. Its `run_submissions` lifecycle and the later `runner_tasks`
lifecycle remain separate because they protect different loss windows and have
different consumers.

### 22. Completion criteria

The program is complete only when:

- all run producers use #525's durable submission lifecycle and scheduler ticks
  no longer perform synchronous preparation;
- one control plane can coordinate zero to N runners in multiple pools;
- runners can be elastic or resident without changing orchestration semantics;
- infrastructure learns demand from an O(1), authenticated, release-specific
  endpoint and learns scale-down from runner process exit;
- every runner-bound operation uses a durable, fenced task;
- a lost process, BEAM link, wake hint, or duplicate platform start cannot
  silently lose or double-commit work;
- unsafe ambiguity becomes an explicit unknown outcome;
- control-plane readiness does not require a runner;
- current local development uses the production-shaped protocol;
- Azure Container Apps and Kubernetes reference-artifact conformance suites
  pass; a platform is called production-qualified only after its separately
  authorized live acceptance suite also passes;
- old and new runner releases drain concurrently;
- PostgreSQL recovery, query-plan, and load tests pass;
- distributed local qualification meets its recorded service level, and any
  claimed live reference deployment meets the documented P95 under-five-minute
  cold-work service level under normal available capacity;
- all singleton-runner code, storage, names, config, and documentation are
  removed;
- `FEATURES.md` and `ROADMAP.md` describe the shipped implementation rather
  than the proposed one.

### 23. Complexity, simplifications, and principal risks

This is a large cross-cutting refactor, not primarily a difficult scheduling
algorithm. The hardest work is:

- holding admission safely while a cold runner starts;
- fencing duplicates across BEAM and infrastructure failure;
- preserving secret-safe runtime-input resolution;
- converting target inspection/generation/rebuild RPCs to durable continuations;
- replacing local singleton lifecycle and result/cache assumptions;
- proving TLS distributed Erlang through managed networking.

The proposed version is the simplest credible multinode design:

- one control plane;
- PostgreSQL as the only durable authority;
- one task slot per runner;
- exact pools configured by users;
- runner pull plus BEAM wake hints;
- external scale-up plus runner self-exit;
- fixed idle grace rather than prediction.

A separate scale-zero-to-one architecture would introduce components that are
immediately replaced by multinode work. Configure a pool maximum of one to get
the single-runner behavior while exercising the final architecture.

### 24. Current primary references

Recheck these references when the infrastructure slice begins because managed
platform behavior and KEDA versions can change:

- [Distributed Erlang, OTP 29](https://www.erlang.org/doc/system/distributed.html)
  for dynamic node names, hidden nodes, connection transitivity, monitors, and
  the warning that default distribution is clear text;
- [OTP 29 system limits](https://www.erlang.org/doc/system/system_limits.html)
  for persistent remote node-name atoms and node limits;
- [OTP 29 `net_kernel` source](https://github.com/erlang/otp/blob/OTP-29.0.3/lib/kernel/src/net_kernel.erl)
  for the version-specific per-host dynamic-name reuse implementation;
- [Using TLS for Erlang distribution, OTP 29](https://www.erlang.org/doc/apps/ssl/ssl_distribution.html)
  for `inet_tls`, peer verification, and certificate configuration;
- [KEDA Metrics API scaler 2.20](https://keda.sh/docs/2.20/scalers/metrics-api/)
  for the stable numeric HTTP metric contract;
- [KEDA ScaledJob specification 2.20](https://keda.sh/docs/2.20/reference/scaledjob-spec/)
  for polling, minimum/maximum scale, running-job deduction, and rollout;
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
  for run-to-completion behavior and duplicate-start caveats;
- [Azure Container Apps Jobs](https://learn.microsoft.com/en-us/azure/container-apps/jobs)
  for event-driven KEDA scaling, retries, timeouts, networking, and no ingress;
- [Azure Container Apps scaling](https://learn.microsoft.com/en-us/azure/container-apps/scale-app)
  for custom ScaledJob-based KEDA rules and scaler authentication;
- [Azure Container Apps communication](https://learn.microsoft.com/en-us/azure/container-apps/connect-apps)
  for internal discovery and raw TCP port exposure.

These references justify the topology and acceptance tests. They do not replace
testing Favn's exact release, network, identity, and scaling configuration on
each supported platform.
