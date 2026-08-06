# Elastic runners implementation log

This append-only checkpoint log accompanies
[`elastic-runners.md`](elastic-runners.md). It records review and verification
evidence for the single `codex/elastic-runners` implementation branch. It must
not contain credentials, secret values, or private infrastructure details.

The implementation plan originally required an independent review and GitHub
checkpoint for every detailed step. That was intentionally simplified after
Step 1: Steps 0-25 remain ordered engineering tasks with focused verification,
but they are grouped into seven phase-level review and checkpoint gates.
Existing Step 0 and Step 1 records together satisfy Phase 1 and remain unchanged
as immutable historical evidence. Phase 2 onward is logged and reviewed once
per phase.

## Phase 1: Foundation

## Step 0: Isolate the branch and accept the baseline

Status: approved and checkpointed

Date: 2026-07-26

### Git baseline

- Source: fetched `origin/main`
- Baseline commit: `b8cb26771729edd154820099a70bd9a49689daf6`
- Local `main` before update: zero commits ahead and 22 commits behind
  `origin/main`
- Local `main` after update: exactly
  `b8cb26771729edd154820099a70bd9a49689daf6`
- Feature branch: `codex/elastic-runners`
- Isolated worktree: `C:\Users\eirik\code\favn-elastic-runners`
- The unrelated `C:\Users\eirik\code\favn` working tree was not modified,
  cleaned, reset, or used for implementation.

### Toolchain and service baseline

- Linux Erlang/OTP 28, ERTS 16.3
- Elixir and Mix 1.20.2, compiled with OTP 28
- Docker Engine 29.6.1 and Docker Compose 5.3.0
- PostgreSQL 18.4 from the checked-in `compose.postgres.yml`
- Container health: healthy
- TCP `127.0.0.1:5432`: reachable
- SQL probe: `select 1` succeeded
- Storage V2: 27 migrations current
- Runtime grants: successfully reconciled
- Runtime-role read of workspace and migration state: succeeded
- Existing `local-dev` workspace: present

The repository service helper could not translate the new WSL worktree path
when it selected native `docker.exe`; it produced `C:\mnt\c\...`. Native Docker
was therefore used for the same checked-in Compose service, followed by WSL Mix
for migration and grants. The final workspace-provision command reported a
conflict because the workspace already existed; direct runtime-role SQL
confirmed the expected `local-dev` workspace and current schema.

The migration command performed a clean isolated compilation in
`/tmp/favn-elastic-runners-build`. It completed all umbrella applications.
Dependency compiler/type warnings were present, but no Favn compilation failure
occurred. No full umbrella suite was run; the approved plan reserves it for
Step 25.

### Legacy removal inventory

Step 23 owns final deletion unless a narrower migration step is named:

- Static control-plane runner transport:
  `RunnerClient.BeamNode`, `RunnerClientValidator`, `RunnerDispatch`,
  `RunnerHealth`, and `RunnerDiagnostics`
- Singleton manifest/release lifecycle:
  `RunnerManifestRegistration`, `ActiveManifestReconciler`, and
  `RunnerReplacement`
- Singleton execution/log ownership:
  `RunExecutionOwnership`, `RunnerExecutionIdentity`, and `RunnerLogBridge`
- Runner singleton:
  `FavnRunner.Server`, `FavnRunner.ResultRetention`,
  `FavnRunner.ExecutionLifecycle`, `FavnRunner.RuntimeStarter`, and
  `FavnRunner.Shutdown`
- Singleton-dependent startup and compatibility:
  `FavnOrchestrator.RunnerReleaseCompatibility`,
  `FavnOrchestrator.RuntimeStarter`, `FavnLocal.RunnerMain`,
  `FavnLocal.RunnerChild`, and `FavnLocal.Lifecycle`
- Shared legacy contract:
  `Favn.Contracts.RunnerClient`
- Storage and identifiers:
  `runner_executions`, `runner_execution_id`,
  `ExecutionOwnershipCodec`, obsolete paging migration/indexes, and exact schema
  inventories
- Configuration:
  `FAVN_RUNNER_NODE`, static runner-client destinations, and singleton readiness
  requirements

The reproducible command, exact 124-file manifest, exclusions, and migration
ownership are recorded in
[`elastic-runners-legacy-inventory.md`](elastic-runners-legacy-inventory.md).
Static checks added in Step 23 must reduce the same live search to zero.

### Named migration ownership

- Step 3 removes all synchronous run-submission producer bypasses.
- Step 4 replaces the singular manifest-wide runner release with the canonical
  pool-to-release map.
- Steps 6-8 introduce runner-task storage and migrate asset dispatch without
  dual writes.
- Steps 9-13 introduce the dynamic coordinator/agent protocol and migrate
  runner compatibility, runtime startup, execution lifecycle, result retention,
  shutdown, and asset execution.
- Steps 14-16 migrate relation inspection, initial target generation, and
  rebuild operations from the legacy transport.
- Step 19 migrates `RunnerMain`, `RunnerChild`, `FavnLocal.Lifecycle`, local
  supervision/configuration, deployment templates, and their tests. Current
  direct `:erpc` references are limited to the legacy Beam client/local
  lifecycle and their tests.
- Step 23 removes the now-unused singleton server/transport, old schema,
  identifiers, configuration, compatibility versions, tests, and documentation.

Current exact-release identity is manifest-wide and appears broadly across
authoring, core contracts, orchestration, and PostgreSQL persistence. Step 4
owns the breaking map migration; later steps may consume only the frozen
pool/release pair.

### Step 0 verification

```text
git fetch origin main --prune
git rev-list --left-right --count main...origin/main
git branch -f main origin/main
git worktree add -b codex/elastic-runners ... main
docker compose -f compose.postgres.yml up -d postgres
docker inspect ...                         # healthy
pg_isready / select 1 / runtime SQL probes # passed
mix favn.postgres.migrate                  # passed
mix favn.postgres.grant_runtime            # passed
mix favn.postgres.provision_workspace      # existing-workspace conflict;
                                            # direct SQL confirmed baseline
git diff --check                           # tracked files passed
rg trailing-whitespace on all changed docs # passed, including untracked files
newline-at-EOF check on all changed docs   # passed
recorded-vs-generated legacy path compare  # 124 == 124, exact set
```

Reviewer verdict: approved with no remaining findings after two review rounds.
The first round required a reproducible complete legacy inventory and
untracked-file-aware checks; both were added and independently reproduced.

Reviewed tree SHA: `dddd5a48026e5e2076173575538cd8b99dd26a2b`

Reviewed content commit SHA:
`6c140a62854ec5c4e04f2caa9ae5a65574f757cf`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs).

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Step 1: Add the durable run-submission queue

Status: approved and checkpointed

Date: 2026-07-26

### Implemented contracts

- Added workspace-scoped durable run submissions and idempotent command
  receipts, including bounded retention and status-filtered keyset paging.
- Added atomic FIFO claims, leases, fencing generations, stale recovery,
  cancellation, supersession, safe retry, and exact replay semantics.
- Persisted only an allowlisted authority snapshot derived from a validated
  `WorkspaceContext`.
- Added exact deployment, manifest, target, and primary-asset reconciliation
  before a submission can become submitted.
- Added one shared PostgreSQL transaction advisory lock for logical run
  identity. Submission enqueue/retry and every durable run creation now use the
  same lock, so a legacy producer and the new submission path cannot create the
  same `(workspace_id, run_id)` concurrently.
- Added the persistence facade and PostgreSQL schema, constraints, indexes,
  diagnostics, codecs, validation, and focused integration coverage.

The implementation exposed contracts that were not precise enough in the
original plan. They were documented, independently reviewed, and checkpointed
before implementation resumed:

- Plan amendment commit:
  `efa4b3d9f97f2dd03cf8b47cc428bfd29c7ecd4d`
- Submission authority is derived from current workspace authority.
- Admitting cancellation is rejected and delegated to run reconciliation.
- Each safe retry is a deliberate new command with a new run identity and
  current operator authority.
- Command receipts accept a seven-day window plus five minutes of future clock
  skew and are pruned in bounded batches.
- Exact run/target reconciliation distinguishes a primary asset target from an
  exact pipeline target.

### Review history

The reviewer rejected the first implementation round for stale receipt fences,
supersession races, incomplete run matching, arbitrary authority input,
admitting cancellation, unbounded filtered paging and receipt retention,
incomplete lease/FIFO coverage, and an underspecified retry model. Those
findings were fixed.

The plan amendment was then rejected until it specified one cross-table
serialization mechanism and exact pipeline-target semantics. The plan was
corrected and approved before code review resumed.

The final implementation review found one remaining boundary error: a legal
100-item claim using maximum-length identifiers could exceed the 65,536-byte
durable receipt bound. The maximum claim and stale-claim batch was reduced to
50, and an integration test now uses 50 distinct 255-byte submission IDs and a
255-byte owner, verifies exact replay, and asserts both stored and encoded
receipt sizes remain within the durable bound.

Reviewer verdict: approved with no remaining findings.

### Step 1 verification

Disposable PostgreSQL database:
`favn_elastic_step1h_20260726`

Exact Storage V2 definition fingerprint:
`ea221d2eee01ec012ac9fd26fd5ff26ee28c7af45206eaab336d5310b0394bad`

```text
mix do --app favn_storage_postgres cmd mix test \
  test/storage_v2/run_submissions_test.exs
  # 22 passed

mix do --app favn_storage_postgres --app favn_orchestrator \
  compile --warnings-as-errors
  # passed

cd apps/favn_orchestrator
mix test test/api/manifests_router_test.exs \
  test/api/runs_router_service_auth_test.exs \
  test/coverage_test.exs \
  test/freshness/state_loader_test.exs \
  test/initial_target_generation_reconciler_test.exs \
  test/rebuilds_test.exs \
  test/run_server/execution/plan_preflight_test.exs \
  test/run_server/execution/sequential_test.exs \
  test/target_admission_test.exs
  # 51 passed

cd apps/favn_storage_postgres
mix test test/storage_v2/core_authority_test.exs \
  test/storage_v2/privileges_test.exs
  # 82 passed

mix credo --strict --ignore \
  Readability.TrailingWhiteSpace,Readability.EndOfLine,Consistency.LineEndings,\
  Design.AliasUsage,Refactor.RedundantWithClauseResult <11 focused files>
  # 297 modules/functions, no issues

git diff --cached --check
  # passed
```

The ignored focused Credo checks cover pre-existing `Runs.Store` alias,
redundant-`with`, whitespace, and Windows line-ending findings; no new issue was
hidden. No full umbrella suite was run because the approved plan reserves it
for Step 25.

Reviewed tree SHA: `5eccd5a5dc66d3d67395a8cb4e067d2c21c56140`

Reviewed content commit SHA:
`a6a76610c353e2134bb2ac9493dd367ddbf5fce5`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs).

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 2: Durable run submission

Status: approved and checkpointed

Date: 2026-07-26

### Implemented contracts

- Added a supervised asynchronous submission subtree with bounded global and
  per-workspace concurrency, round-robin workspace discovery, claim renewal,
  stale recovery, retry backoff, cancellation handoff, and owner-loss
  containment.
- Moved target preparation and DAG planning out of API, scheduler, backfill,
  rebuild, recovery, and child-run producer processes.
- Kept `RunManager` as the final local admission owner and reconciled every
  ambiguous acknowledgement against the exact durable run identity before
  retry or failure.
- Made schedule occurrence completion and submission enqueue one PostgreSQL
  transaction. Scheduler overlap cursors remain active while the reserved run
  is queued, preparing, or admitting.
- Added authenticated, workspace-scoped submission detail, keyset page, and
  aggregate queue-statistics API reads. Queued run detail returns accepted
  submission state instead of a transient 404.
- Reworked global workspace discovery into indexed queued and stale branches,
  excluding terminal history from the polling path.
- Removed the synchronous producer entry points and their migrated builder
  bypasses.

Exact Storage V2 definition fingerprint:
`14e39f94a62b8e0a86232e52e444b585a235a6d30015cdfbe1d8838f3d199e6e`

### Review history

The first phase review rejected the implementation for four issues:

- scheduler `:forbid` and `:queue_one` overlap protection could be cleared
  during the intentional submission queue delay;
- queue depth, age, retry, cancellation, and failure diagnostics had no
  authenticated operator HTTP boundary;
- global workspace discovery had neither an indexed design nor query-plan
  proof;
- durable recovery from every queued, preparing, and admitting state was not
  demonstrated.

All four were fixed. The recovery test also exposed temporary `trap_exit` state
and stale normal-exit messages when a worker caller survived more than one
claim. Worker link cleanup now restores the caller flag and ignores unrelated
normal exits while retaining abnormal-exit containment.

Final reviewer verdict: approved with no remaining high, medium, or low
findings. The reviewer inspected the exact diff from the previous approved
checkpoint, including all 13 untracked files.

### Phase 2 verification

```text
mix format --check-formatted <all modified Elixir files>
  # passed

mix do --app favn_orchestrator --app favn_storage_postgres \
  compile --warnings-as-errors
  # passed

cd apps/favn_orchestrator
mix test
  # 586 passed

cd apps/favn_storage_postgres
mix test test/storage_v2/run_submissions_test.exs \
  test/storage_v2/core_authority_test.exs
  # 107 passed against a fresh disposable database

mix test
  # 155/157 passed
  # the two remaining tests require the external PostgreSQL `createdb`
  # executable, which was not installed in the native Windows shell

git diff --check
  # passed apart from informational Windows line-ending warnings

rg synchronous producer bypass signatures
  # no production matches
```

No full umbrella suite was run because the accepted plan reserves that gate for
Phase 7.

Reviewed tree SHA: `b218009f65c52d0182cd210302b61a3b082e2239`

Reviewed content commit SHA:
`5699d83acddbb61420057eb6d4919448512dc690`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners --commit 5699d83a` returned
no runs).

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 3: Pool, release, protocol, and demand contracts

Status: approved and checkpointed

Date: 2026-07-27

### Implemented contracts

- Added arbitrary user-named runner pools without creating runtime atoms, plus
  provider-neutral resident/elastic pool configuration.
- Replaced the singleton runner release with an exact pool-to-release manifest
  map and propagated the selected pool and release through plans, run
  snapshots, runner work, inspection, and generation contracts.
- Added the frozen protocol-13 registration, claim, assignment, lifecycle,
  runtime-input, log, result, cancellation, wake, and shutdown messages with
  positive session/assignment fencing, bounded safe decoding, capability
  matching, and exact command identities.
- Added PostgreSQL runner tasks, atomic `SKIP LOCKED` compatible claims,
  immutable enqueue identities, leases, fenced transitions, bounded logs,
  runtime-input acknowledgements, terminal results, recovery claims, and exact
  O(1) demand projections with explicit audit/repair health.
- Kept protocol 13 non-activatable until the Phase 4 coordinator replaces the
  old singleton execution path.
- Removed the legacy generator release fallback, silent legacy option deletion,
  and stale public schema/protocol examples.
- Separated pre-start retry evidence from running outcome uncertainty. Assigned
  or preparing tasks can requeue before execution; running writes cannot.
  Terminal asset retries require a validated `RunnerError` with
  `outcome: :safe_failure`.

### Review history

The first valid phase review rejected nine issues: premature protocol
activation, incomplete command/fence/runtime-input acknowledgements, zero live
session generations, capability-blind claims, compressed external-term
expansion, incomplete immutable enqueue comparison, demand health without an
operational audit, a remaining singleton release fallback, and stale public
documentation. All nine were fixed.

The second review found two remaining boundary issues. Retry safety did not
distinguish durable pre-start state from ambiguous running outcomes, and
`RuntimeInputsResolved` allowed untyped or contradictory shapes. Retry
classification is now validated against task kind, durable state, terminal
outcome, and validated error evidence. Runtime input success and failure now
have mutually exclusive, typed, round-trippable contracts.

Final reviewer verdict: approved; Phase 3 is ready to checkpoint.

### Phase 3 verification

```text
mix format --check-formatted <all 130 modified/new Elixir files>
  # passed

mix compile --warnings-as-errors
  # passed

cd apps/favn
mix test
  # 155 passed

cd apps/favn_authoring
mix test
  # 137 passed

cd apps/favn_runner
mix test
  # 218 passed in the first phase run
  # after the final contract fix, 217/218 passed; the unchanged one-millisecond
  # manifest-lease timing assertion failed under the Windows clock granularity

cd apps/favn_orchestrator
mix test
  # 595 passed in the first phase run
  # after the final contract fix, 594/595 passed because an unchanged 100 ms
  # monitor assertion observed :noproc instead of :killed; targeted rerun passed

cd apps/favn_core
mix test
  # 374/375 passed
  # the unchanged Windows micro-timing assertion reports duration_us == 0

cd apps/favn_test_support
mix test
  # 7 passed

cd apps/favn_core
mix test test/contracts/runner_task_test.exs \
  test/sql_asset_runtime_inputs_test.exs
  # 11 passed

cd apps/favn_runner
mix test test/runtime_input_resolver_test.exs
  # 9 passed

cd apps/favn_storage_postgres
mix test test/storage_v2/runner_tasks_test.exs --seed 15
  # 17 passed against the disposable database

mix test <related Storage V2 runner release and authority tests>
  # 26 passed

cd apps/favn
mix docs
  # passed
```

The runner-task migration was also applied successfully to the fresh
`favn_elastic_phase3` disposable PostgreSQL database. Two unrelated full
Storage V2 tests remain unavailable because the native Windows host does not
provide `createdb` and `pg_dump`. No full umbrella suite was run because the
accepted plan reserves that gate for Phase 7.

Reviewed tree SHA: `72bf9435a8c1a3d6a65d0e68c245dcfc978cb1b9`

Reviewed content commit SHA:
`febd5df63f612a1bed0a8ef2c84df59e94c696c9`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs).

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 4: Distributed runner execution and recovery

Status: approved and checkpointed

Date: 2026-07-27

### Implemented runtime

- Replaced the singleton runner execution path with durable, per-asset runner
  tasks claimed by dynamically registered distributed-BEAM runner processes.
- Added exact pool/release/capability matching, positive runner-session and
  assignment fencing, lease renewal, bounded log delivery, durable terminal
  results, cancellation acknowledgements, and safe task recovery.
- Kept the live runner registry process-local while retaining all work,
  assignments, leases, runtime-input pins, logs, and outcomes durably in
  PostgreSQL. Runners reconnect and re-register after control-plane loss.
- Added atomic runtime-input pinning before execution. An unresolved payload is
  retained byte-for-byte across reconnects and cannot be resolved twice with
  different values.
- Reworked run execution state around active durable task IDs, including
  restart-safe result waits and cancellation when the original run process is
  no longer alive.
- Added real dynamic-name BEAM peer churn coverage and bounded runner-result
  routing across store read failures and router restarts.
- Removed the old singleton manifest runner route and `RunWorkSet`.

### Review history

The first review rejected six correctness issues: lease renewal replay could
collapse distinct renewals, cancellation depended on a live run process,
cancellation could acknowledge work that had already finished, unresolved
runtime inputs were not retained exactly across reconnect, node churn coverage
did not boot real dynamic BEAM peers, and result subscriptions were not durable
across store/router failure. All six were fixed.

The second review confirmed those functional fixes and found one process
violation: the already frozen `LeaseRenewal` wire schema had acquired an
`occurred_at` field. The field was removed. Stable renewal identity and time are
now derived from the existing absolute lease expiry, preserving the frozen wire
contract while keeping exact replay idempotent and later renewals distinct.

A later whole-PR review found that runner command age was still derived from
refreshable `occurred_at`, `Started` could derive a new issuance after lease
renewal, and command receipts copied complete task payloads and results. The
correction makes `issued_at` the stable command-age field while allowing
`occurred_at` to refresh, serializes deterministic first enqueues before
canonicalizing their first issuance, and carries stable `assigned_at` on each
assignment. Receipts now store bounded fencing snapshots and reference
immutable terminal-result and runtime-input-error history instead of
duplicating large values. Exact replay is covered after later task transitions,
lease renewal, acknowledgement loss, concurrent first enqueue, maximum
recovery batches, and near-limit payload/result values.

The narrowed frozen-diff follow-up added the missing child indexes for task,
outcome, and runtime-input-error foreign-key/prune lookups and verifies both
history anti-joins with forced-generic PostgreSQL plans. Enqueue now acquires
its identity advisory lock before bounded receipt/history pruning, and exact
enqueue equality includes `task_id` as well as domain identity and work.
Terminal completion replay is verified after a safe retry, reassignment, and a
newer terminal outcome. Version one deliberately retains terminal runner-task
rows; terminal-task deletion and its coordinated identity/history retirement
contract are not implemented in this change.

The final scoped reviewer approved the current-v1 correction with no remaining
finding. The review explicitly rejected speculative tombstone and idle
maintenance machinery because version one has no terminal runner-task deletion
path and creates no new rows while idle.

### Phase 4 verification

```text
mix compile --warnings-as-errors
  # passed

cd apps/favn_core
mix test
  # 377 passed

cd apps/favn_runner
mix test
  # 232 passed

cd apps/favn_orchestrator
mix test
  # 605 passed, including the dynamic distributed-BEAM peer churn coverage

cd apps/favn_storage_postgres
mix test test/storage_v2/runner_tasks_test.exs
  # 21 passed against a fresh disposable database

cd apps/favn_storage_postgres
mix test --exclude slow
  # 166 passed, 13 excluded against a fresh disposable database

```

No full umbrella suite was run because the accepted plan reserves that gate for
Phase 7.

Reviewed tree SHA: `b3d47624e0541ea3cf14fcf972de7b7dbb40dd17`

Reviewed content commit SHA:
`cec5e07a483b4bbdda6ce5d7325712801039ccca`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs).

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 5: Remaining execution-path migration

Status: approved and checkpointed

Date: 2026-07-27

### Implemented runtime

- Moved relation inspection, initial target-generation operations, and rebuild
  create/copy/validate/activate/reconcile continuations onto durable runner
  tasks pinned to each operation's frozen pool and exact runner release.
- Persisted restart-safe operation continuations and safe retry decisions under
  deterministic task identities. Adapter-proven pre-commit failures can be
  retried, while uncertain external writes require reconciliation.
- Added unique supervised rebuild planning and execution workers. Long runner
  cold-start waits no longer block the dispatcher; the worker renews both the
  rebuild operation lease and every target-operation lock without changing the
  rebuild aggregate version.
- Made rebuild cancellation and runner-task cancellation one PostgreSQL
  transaction, so queued work cannot survive a committed operation
  cancellation after control-plane loss.
- Made each task executor own its customer-code worker through a process link,
  so executor loss cannot leave an untracked asset or generation operation
  running.
- Removed the legacy active-manifest reconciler and the remaining direct
  inspection/generation/rebuild runner RPC paths.

### Review history

The first phase review rejected four correctness issues: rebuild continuations
could outlive their 30-second operation and target-lock leases; killing an asset
task executor could leave its customer-code worker alive; adapter-reported safe
generation failures were persisted as successful task results; and planning
cancellation reached runner work only through a live waiter. All four were
fixed with supervised continuation ownership, lease renewal, explicit domain
outcome mapping, and atomic durable task cancellation.

The final review inspected the complete plan and exact staged Phase 5 tree,
rechecked all four findings, and found no remaining actionable issue.

Final reviewer verdict: approved; Phase 5 is ready to checkpoint.

### Phase 5 verification

```text
mix compile --warnings-as-errors
  # passed

cd apps/favn_orchestrator
mix test
  # 609 passed

cd apps/favn_runner
mix test --seed 78344
  # 242 passed

cd apps/favn_storage_postgres
mix test test/storage_v2/runner_tasks_test.exs \
  test/storage_v2/core_authority_test.exs
  # 103 passed against a fresh disposable database
```

The focused PostgreSQL gate used a newly created disposable database and
verified the rewritten Storage V2 definition fingerprint. No full umbrella
suite was run because the accepted plan reserves that gate for Phase 7.

Reviewed tree SHA: `f836ada9483449149e67358cb06ab2404ac45df7`

Reviewed content commit SHA:
`1a902fc48607a1b50e45f035268a5d3c895e814c`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs); the
repository workflows target pull requests and pushes to `main`.

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 6: Operations and infrastructure deployment

Status: approved and checkpointed

Date: 2026-07-27

### Implemented runtime and references

- Added resident and elastic lifecycle modes. Resident runners park until an
  explicit wake; elastic runners honor the control-plane idle grace exactly,
  make one final claim, and exit. Pool names remain arbitrary user-defined
  environment identities.
- Added authenticated exact pool/release demand in JSON and OpenMetrics forms,
  bounded telemetry labels, rate limiting, zero-runner readiness, and
  all-partition capacity-projection health.
- Added exact durable release-drain authority covering current runner tasks,
  active pinned runs, and pending rebuild continuations. The bounded collection
  diagnostic reports `partition_limit` and `truncated`; the exact authenticated
  partition endpoint is the infrastructure-removal authority.
- Preserved manifest rollback semantics: existing work keeps its frozen
  pool/release map while newly submitted work follows the newly active map.
- Rebuilt the local source-development lifecycle on production registration,
  claim, result, and drain paths. A crashed retiring runner is restarted while
  durable old-release blockers remain.
- Implemented bounded OTP dynamic runner node names and production mutual-TLS
  distribution enforcement. Real TLS peers connect, while a same-cookie
  plaintext peer is rejected.
- Added provider-neutral resident/job contracts plus reviewed Azure Container
  Apps and Kubernetes/KEDA references. Favn core contains no provider SDK or
  infrastructure credential.
- Verified the current stable KEDA 2.20 metrics API and default ScaledJob
  accounting contract. The reference keeps assigned work in `outstanding` and
  explicitly uses the default strategy that subtracts running Jobs.

### Review history

The first phase review rejected eight issues: unbounded runner node naming,
unsafe retirement after runner loss, incomplete Azure authentication and boot
configuration, TLS files without enforced TLS transport, readiness that ignored
stale capacity, runner-local wait overrides, insufficient conformance/rollback
coverage, and unbounded unknown-release telemetry. All eight were corrected.

The second review found one remaining high-severity boundary: readiness and
operator drain diagnostics silently treated the first 256 historical
partitions as the complete set. Readiness now uses one all-row aggregate health
query, the collection diagnostic declares truncation, and safe removal uses an
exact pool/release endpoint. A 300-partition regression proves the aggregate
counts every row while the diagnostic remains bounded at 256; a separate
readiness test proves an unhealthy aggregate fails readiness.

Final reviewer verdict: approved with no remaining findings.

### Phase 6 verification

```text
mix compile --warnings-as-errors
  # passed

targeted mix format --check-formatted for every Phase 6 Elixir file
  # passed

cd apps/favn_core
mix test test/contracts/runner_task_test.exs \
  test/deployment/runner_pool_spec_test.exs \
  test/distribution_tls_test.exs
  # 17 passed, including real mutual-TLS/plaintext rejection

cd apps/favn_runner
mix test test/production_runtime_config_test.exs test/runner_agent_test.exs
  # 21 passed

cd apps/favn_orchestrator
mix test test/api/runner_capacity_router_test.exs \
  test/readiness_runner_test.exs \
  test/production_runtime_config_test.exs \
  test/operation_runner_tasks_test.exs \
  test/runner_registry_test.exs \
  test/run_manager/cancellation_test.exs
  # 37 passed

cd apps/favn_storage_postgres
mix test test/storage_v2/runner_tasks_test.exs \
  test/storage_v2/run_submissions_test.exs \
  test/storage_v2/runner_release_migration_test.exs
  # 52 passed against a fresh disposable database

mix test test/storage_v2/core_authority_test.exs:1730
  # 1 passed, 81 excluded; rollback routing and frozen in-flight pins

cd apps/favn
mix test test/deployment_reference_conformance_test.exs
  # 4 passed

cd apps/favn_local
mix test test/acceptance/docker_free_local_lifecycle_test.exs --include acceptance
  # 1 passed against a fresh disposable database

bicep 0.45.15 build deployment/azure-container-apps/control-plane.bicep
bicep 0.45.15 build deployment/azure-container-apps/elastic-runner-job.bicep
  # both passed without diagnostics
```

The Azure Bicep reference artifacts recorded above were later removed. Favn
keeps the deployment contract provider-neutral and leaves provider-specific
infrastructure in operator-owned repositories.

No full umbrella suite was run because the accepted plan reserves that gate for
Phase 7. No live Azure or managed Kubernetes resources were created; those
provider qualification gates remain explicitly external.

Reviewed tree SHA: `1031b98217485b2644617e5884234b18a0915169`

Reviewed content commit SHA:
`88a3cb480fa81a1edb02b495ff45118933a9870b`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs); the
repository workflows target pull requests and pushes to `main`.

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

## Phase 7: Legacy removal and final qualification

Status: approved and checkpointed

Date: 2026-07-27

### Implemented cleanup and qualification

- Removed the retired synchronous runner client, dispatch, execution-ownership,
  runner-server, and replacement paths. Durable runner tasks are now the single
  asset-attempt execution authority, with `task_id` as the canonical runtime
  identity.
- Removed stale migration modules, compatibility shims, obsolete runtime
  starters, and completed refactor-plan documents from the active tree. The
  historical material that remains useful is explicitly archived.
- Added an executable legacy-architecture guard to CI. Its exclusions are
  limited to archived history and the elastic-runner architecture plan that
  intentionally names the removed paths.
- Completed the local runner lifecycle cleanup, including safe shutdown of a
  runner process that exits before BEAM registration.
- Documented concurrent pool/release upgrades and rollback. Old and new
  generations may coexist; exact durable drain authority determines when old
  infrastructure can be removed.
- Added distributed-BEAM qualification. A fresh peer boots the production
  `FavnRunner.Application`, registers through the production gateway, claims
  PostgreSQL work, and reaches durable `Started`. The scale scenario exercises
  arbitrary pool names, 3,000 durable tasks, and 0/1/10/100 remote agents
  through the real gateway, registry, and PostgreSQL claim protocol.

### Review history

The first final-phase review rejected seven issues: singleton upgrade guidance,
successful exit status after rejected registration, duplicate execution
identity in run-server state, simulated-only scale and cold-start tests,
corrupted audit-document identifiers, a local pre-registration process leak,
and overly broad legacy-guard documentation exclusions. All seven were
corrected.

The consolidated re-review covered the full implementation plan and exact
uncommitted merge tree. It also checked historical-manifest activation,
descriptor handling, RunEnum/IEx integration, provider neutrality, merge
integrity, and direct legacy searches.

Final reviewer verdict: approved with no remaining findings.

### Phase 7 verification

```text
targeted mix format --check-formatted for all 193 changed Elixir files
  # passed

mix compile --warnings-as-errors
  # passed

mix credo --only warning --strict
  # 1,154 source files, no issues

mix sobelow --root apps/favn_orchestrator ... --strict --exit
mix sobelow --root apps/favn_view ... --strict --exit
  # both passed

mix dialyzer --list-unused-filters --format short
mix dialyzer --format dialyzer --quiet-with-result
  # 285 known findings skipped, 0 unnecessary skips; passed

mix hex.audit
mix deps.audit
  # no retired packages, advisories, or vulnerabilities

elixir scripts/check_no_legacy_runner_architecture.exs
  # passed

cd apps/favn_storage_postgres
mix test test/storage_v2/runner_tasks_test.exs
  # 26 passed against a fresh disposable database
  # production RunnerAgent cold start reached durable Started in 312 ms
  # 3,000-task distributed scale scenario had 706 ms claim-to-Started P95

mix test
  # complete umbrella fast-test suite passed against a fresh disposable database
  # 1,926 passed; no failures
```

The distributed measurements use a local peer host, so they qualify Favn's
BEAM, registry, gateway, and PostgreSQL path rather than a cloud provider's VM
or container cold-start time. No live Azure or managed Kubernetes resources
were created; those provider qualification gates remain explicitly external.

Reviewed tree SHA: `ce758faf1c986636e31c2e064ad8176387726686`

Reviewed content commit SHA:
`ae2eef0b59b494292c82532d0ba51b6e070ebfe1`

Pushed branch CI: no GitHub Actions workflow run was triggered by the branch
push (`gh run list --branch codex/elastic-runners` returned no runs); the
repository workflows target pull requests and pushes to `main`.

Evidence commit: this field is intentionally not self-recorded; see the
checkpoint workflow in the implementation plan.

### Post-checkpoint main synchronization

Before final PR qualification, `origin/main` advanced to
`73eb98299b0efb403e86789d159a9d7f0b46d20a` with scalar asset-attempt error
loading. The merge conflict in the operator-run overview test was resolved by
preserving the upstream blocked-step/scalar-error assertions while keeping
`runner_releases` as the canonical elastic-runner release projection.

The focused storage gate passed 80 tests on a fresh database. Compilation with
warnings as errors and the legacy guard passed. The complete umbrella suite
then passed 1,928 tests on another fresh database. A focused reviewer approved
the resolution with no remaining finding.

Reviewed synchronization tree SHA:
`4ab46ce7752b298ea3d0686aa4b8570a0a7fa72f`

Reviewed synchronization commit SHA:
`c66556eb1452a3fa852d5e389919e1ea7e3f7d6e`

### Final linear rebase onto main

The completed branch was linearly rebased onto
`2e7925531f3b84bed1b03bb136801423ba70b0f3`. A standard rebase cannot preserve
the manual tree resolution stored only in the original feature merge, so that
resolution was replayed explicitly as `034fad4e`. The legacy guard then caught
and prevented reintroduction of the deleted singleton runner architecture.

The final integration also preserves main's durable asset-evidence binding
migration and orchestrator contract. The reviewed reconciliation commit is
`e1647e91`.

Post-rebase integration checks:

```text
mix compile --warnings-as-errors
  # passed

elixir scripts/check_no_legacy_runner_architecture.exs
  # legacy runner architecture is absent

mix favn.postgres.migrate
mix favn.postgres.verify_schema
  # passed against a fresh disposable database
  # definition fingerprint:
  # 1ed24239d3ad81bec4b96e8afbd6bfdf82d150b1e1dc25d1fbf342fb927f36fd

git diff --check
  # passed
```

Per operator direction, the already-passing behavior suite was not repeated
after this code-neutral rebase. The conflict integration received compile,
fresh-schema, static legacy, and diff checks. A focused subagent reviewed the
complete integration, requested one missing current-main documentation
contract, and approved the corrected checkpoint with no remaining findings.

### Final integration qualification

The final Windows qualification exposed a real distributed-startup race:
`RunnerAgent` built its registration while the VM was still
`nonode@nohost`. It now connects to the orchestrator gateway before constructing
the registration, so the advertised node name is the assigned distributed BEAM
node. The registry still validates the exact node identity and monitors the
registered process, but no longer performs a redundant remote liveness RPC
during registration.

The same correction checkpoint fixed Windows-safe source discovery and
cold-BEAM test invocation, brought test stores in line with the production
preparation contract, and removed invalid fixture reuse of a single durable
task identity. These are captured by `264b69f6`.

Final verification:

```text
mix format --check-formatted
mix compile --warnings-as-errors
elixir scripts/check_test_tag_tiers.exs
elixir scripts/check_no_legacy_runner_architecture.exs
git diff --check
  # passed

mix test --no-compile --timeout 1200000
  # every umbrella fast-test slice passed against a fresh disposable database

mix test.acceptance
  # all acceptance and browser slices passed

mix test.slow
  # all runnable slow tests passed
  # distributed scale: 333 runners, 1,419 ms claim-to-Started P95
  # cold start: 344 ms
```

Two PostgreSQL backup/migration drill tests could not run in the native Windows
shell because `createdb`, `pg_dump`, and `pg_restore` were not installed. This
is an environment limitation rather than an application failure; the same
provider-neutral tests remain part of the Linux CI gate.

The first Linux CI run then found one stress-fixture mismatch under concurrent
database checkout pressure. The production `RunnerAgent` reconnects after a
retryable control-plane failure, but the distributed scale helper treated the
same response as terminal. The helper now retries only explicitly retryable
storage-unavailable and gateway-overload responses, with a fixed bound. Four
consecutive 333-runner focused runs passed after that correction.
