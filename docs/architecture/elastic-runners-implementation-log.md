# Elastic runners implementation log

This append-only checkpoint log accompanies
[`elastic-runners.md`](elastic-runners.md). It records review and verification
evidence for the single `codex/elastic-runners` implementation branch. It must
not contain credentials, secret values, or private infrastructure details.

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
