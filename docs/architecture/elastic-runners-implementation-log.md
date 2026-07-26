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
