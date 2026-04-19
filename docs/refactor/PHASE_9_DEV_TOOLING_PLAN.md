# Phase 9 Core Dev Tooling Plan

## Status

Planning document for the next PR only.

This plan treats the merged Phase 8 web/orchestrator boundary work as locked baseline context and narrows the next PR to the core local developer lifecycle only.

## 1. Locked assumptions

### Architecture baseline

- `favn_web` is the public web tier and lives in `web/favn_web`.
- `favn_orchestrator` is the private control plane and system of record.
- `favn_runner` is execution-only.
- `favn_view` is archived/transitional only and must not regain product-boundary importance.
- the web/orchestrator remote HTTP + SSE boundary already exists in baseline form.
- orchestrator-owned auth/session/audit baseline already exists.

### Ownership boundary

- `apps/favn_local` owns local lifecycle/tooling implementation (`dev`, `stop`, `reload`, `status`, `.favn/` state)
- `apps/favn` stays the public authoring/build surface
- future distribution UX may wrap multiple owner apps; packaging shape and ownership shape do not need to be the same

### Strict next-PR scope

Foundational, next PR:

- `mix favn.dev`
- `mix favn.stop`
- `mix favn.reload`
- `mix favn.status`
- minimal config and `.favn/` state to support those commands
- memory storage by default with SQLite as the immediate persistent local option

Explicitly out of scope for this PR:

- `mix favn.install`
- `mix favn.reset`
- `mix favn.logs`
- build/packaging tasks
- release/install/distribution polish
- durable auth hardening
- v1-safe production hardening
- richer web UX
- custom storage adapters

### Boundary guardrails

Product-critical:

- do not replace the separate web process with a same-BEAM shortcut
- do not add compile-time dependencies from `apps/favn` to internal runtime apps
- do not add a new umbrella app

Refactor-enabling:

- for local dev only, use a small localhost-only runner/orchestrator control path that preserves process separation and keeps future transport options open

Recommended default:

- use `.favn/runtime.json` plus a localhost-only local control path for lifecycle actions that cannot go through the product API
- use the private orchestrator HTTP API for manifest publish/register + activate so local reload and future production publish follow the same boundary

Why:

- it keeps `favn_web`, `favn_orchestrator`, and `favn_runner` as separate processes
- it avoids same-BEAM collapse
- it avoids compile-time deps from `apps/favn` to runtime apps
- it keeps the next PR focused on lifecycle rather than broad transport packaging

## 2. Immediate developer workflow

### First start

Developer flow after this PR:

1. run `mix favn.dev` from the user project root in one terminal
2. the task creates `.favn/`, resolves config, generates local secrets if missing, builds the current manifest, starts runner/orchestrator/web, registers and activates the manifest, then prints URLs, PIDs, storage mode, and log paths
3. keep that terminal open while the local stack is running
4. open the printed `favn_web` URL in the browser

### Daily change loop

Use the following decision table:

| Change type | Expected action |
| --- | --- |
| changes that only affect manifest contents shown in the UI | `mix favn.reload`, then browser refresh |
| `web/favn_web` code changes | full `mix favn.stop` then `mix favn.dev` |
| user asset/pipeline/schedule/source/connection/SQL changes | `mix favn.reload` |
| changes under `apps/favn`, `apps/favn_core`, or `apps/favn_runner` that affect manifest generation or execution behavior | `mix favn.reload` |
| changes under `apps/favn_orchestrator` or local dev config that affect orchestrator behavior, ports, storage mode, auth wiring, or control-path wiring | full stop and start |
| broken or partially running stack | `mix favn.stop` then `mix favn.dev` |

### Status checks

- `mix favn.status` is the boring operator view for local state
- it should work whether the stack is healthy, partially dead, or fully stopped

### Stopping services

- `Ctrl-C` or normal terminal exit from the foreground `mix favn.dev` session should stop the full owned stack
- `mix favn.stop` remains the explicit cleanup/fallback command from another terminal
- logs and SQLite data are preserved by default

## 3. Detailed plan for `mix favn.dev`

Category: foundational

### Responsibilities

`mix favn.dev` should:

1. acquire a local lifecycle lock so only one dev command mutates `.favn/` at a time
2. resolve local dev config
3. create required `.favn/` directories
4. load or generate persistent local secrets
5. build and pin the current manifest version from the user project
6. start runner
7. start orchestrator
8. register the current manifest in runner
9. publish/register the current manifest in orchestrator through the private API
10. activate the manifest in orchestrator
11. start `favn_web` as a thin built local web process
12. verify readiness and print a compact startup summary
13. remain attached in the foreground until interrupted

### Startup order

Recommended order:

1. `.favn/` preparation and lock
2. config + secret resolution
3. manifest build/pin
4. runner start and readiness
5. orchestrator start and readiness
6. manifest registration in runner
7. manifest publish/register + activation in orchestrator
8. web process start and readiness
9. final status write + console summary
10. wait in the foreground and stop owned services when the session exits

Why this order:

- runner should be ready before orchestrator tries to dispatch work through the configured runner client
- orchestrator should be ready before web starts relaying to it
- manifest publish/registration + activation should happen before the browser opens the stack so the UI reflects real local state immediately

### Readiness checks

Runner readiness:

- process is alive
- local runner node responds to a lightweight RPC ping

Orchestrator readiness:

- process is alive
- local orchestrator node responds to RPC ping
- configured HTTP port is accepting connections

Web readiness:

- process is alive
- configured local web URL returns any successful HTTP response or redirect

Recommended default timeouts:

- runner ready timeout: 10 seconds
- orchestrator ready timeout: 15 seconds
- web ready timeout: 20 seconds

### Default local wiring

Recommended defaults:

- orchestrator API URL: `http://127.0.0.1:4101`
- web URL: `http://127.0.0.1:4173`
- storage mode: `:memory`
- SQLite path when enabled: `.favn/data/orchestrator.sqlite3`

Wiring behavior:

- `mix favn.dev` injects `FAVN_ORCHESTRATOR_BASE_URL` for `favn_web`
- `mix favn.dev` injects a generated local service token into both orchestrator and web
- `mix favn.dev` injects a generated local web session secret for `favn_web`
- secrets persist in `.favn/secrets.json` so stop/start keeps local login wiring stable
- `favn_web` should run in a production-like local mode for this PR, not a Vite/HMR-oriented dev mode

### Foreground lifecycle

Foreground should remain the default behavior.

Default `mix favn.dev`:

- starts runner, orchestrator, and web, then stays attached in the foreground
- stops the full owned stack when interrupted or when the terminal session exits normally
- writes current runtime ownership to `.favn/runtime.json` so another terminal can run `mix favn.reload`, `mix favn.status`, or `mix favn.stop`
- acquires `.favn/lock` only for short runtime state mutation windows, never for the full foreground wait lifetime

Optional interactive mode:

- not required as a separate first-cut mode if normal `mix favn.dev` is already foreground and explicit
- if added later, it should still attach only to the orchestrator/control-plane runtime

### Failure reporting

If startup fails:

1. write failure details to `.favn/history/last_failure.json`
2. print which service failed and where its logs are
3. stop any services started by the failed invocation
4. leave logs and SQLite state intact
5. exit non-zero

### Avoiding orphaned processes

Foundational recommendation:

- launch services under one foreground owner session that records child pids/process groups in `.favn/runtime.json`

Why:

- the foreground session can clean up owned services automatically
- `mix favn.stop` can still terminate the owned subtree when cleanup did not happen cleanly
- stale pid detection is simpler
- local crash reporting is easier to reason about

### Behavior when one process is already running

Recommended default:

- if all owned services are already running and healthy with the same config hash, print the stack summary and exit `0`
- if the stack is partially running, partially dead, or the config hash differs, fail with an actionable message and tell the developer to run `mix favn.stop` first

Reason:

- no surprise duplicate processes
- no hidden partial repair logic in the first cut
- lower review and maintenance risk

## 4. Detailed plan for `mix favn.stop`

Category: foundational

### Responsibilities

`mix favn.stop` should:

1. acquire the lifecycle lock
2. read `.favn/runtime.json`
3. stop web/orchestrator/runner cleanly even if one of them is already dead
4. delete active runtime state files
5. preserve logs, manifest cache, secrets, and SQLite data by default

### Stop order

Recommended order:

1. stop `favn_web`
2. ask orchestrator to stop admitting new work and cancel any in-flight local runs
3. stop `favn_runner`
4. stop `favn_orchestrator`

Why:

- web goes down first so no new browser traffic enters the stack
- orchestrator gets a chance to record shutdown-driven cancellation intent before runner disappears
- runner exits before orchestrator fully disappears, reducing local orphaned execution work

### Timeout behavior

Recommended defaults:

- graceful stop timeout per service: 10 seconds
- orchestrator drain/cancel wait before hard stop: 15 seconds

### Forced kill fallback

If graceful stop times out:

1. send a stronger termination signal to the stored process group
2. wait a short second timeout
3. record forced-kill fallback in `.favn/history/last_failure.json`

### Behavior when one process is already dead

Expected behavior:

- continue stopping the rest
- report the dead service as stale/dead in the console summary
- clear `.favn/runtime.json` when no active owned services remain

### Idempotency

Product-critical expectation:

- `mix favn.stop` is idempotent
- running it against an already stopped stack exits `0` and prints `stack not running`

## 5. Detailed plan for `mix favn.reload`

Category: foundational

This is the most important next-PR command.

### Responsibilities

`mix favn.reload` should:

1. acquire the lifecycle lock
2. confirm the local stack is running
3. recompile the user project
4. rebuild and pin the current manifest version
5. restart the runner so changed project code is loaded cleanly
6. register the new manifest in runner
7. publish/register the new manifest in orchestrator through the private API
8. activate the new manifest in orchestrator for future runs
9. update `.favn/manifests/latest.json` and `.favn/runtime.json`
10. leave `favn_web` alone

### When browser refresh is enough

For the next PR, the normal web behavior should be:

- `favn_web` stays running unchanged
- browser refresh calls web
- web calls orchestrator
- orchestrator returns state from the current active manifest and current runs

So for ordinary `favn` development, `mix favn.reload` plus browser refresh should be enough.

### When `mix favn.reload` is required

Use `mix favn.reload` for:

- asset changes
- pipeline changes
- schedule changes
- manifest-affecting DSL changes
- SQL asset or source changes
- connection/runtime config changes consumed by the runner
- changes in `apps/favn`, `apps/favn_core`, or `apps/favn_runner` that affect manifest generation or execution behavior

### What happens to in-flight runs

Recommended default for the first cut:

- `mix favn.reload` should fail if orchestrator reports any in-flight runs
- it should print the run ids and ask the developer to wait, cancel them explicitly, or stop the stack

Why:

- restarting the runner under active work is surprising and hard to explain cleanly in a first-cut local workflow
- refusing reload is safer and easier to review than best-effort implicit cancellation in the first PR

Future nice-to-have:

- a later `--force` mode could explicitly cancel and continue, but that should stay out of the next PR

### Orchestrator behavior during reload

Recommended default:

- do not restart orchestrator during `mix favn.reload`
- `mix favn.reload` should send the newly built manifest to orchestrator through the private API and then activate it

### Manifest publish endpoint

Foundational recommendation:

- add a private orchestrator endpoint to register one manifest version without restarting orchestrator
- recommended shape: `POST /api/orchestrator/v1/manifests`
- keep `POST /api/orchestrator/v1/manifests/:manifest_version_id/activate` as the separate activation step

Why this belongs in or alongside the next PR:

- `mix favn.reload` needs a real no-restart publish path
- the same behavior is needed in production so orchestrator can accept new manifests without stopping
- it keeps local dev aligned with the steady-state boundary instead of inventing a dev-only import mechanism

Use full stop/start instead when:

- orchestrator code changed
- orchestrator config changed
- local lifecycle/control-path wiring changed

### Local runner control boundary

- manifest re-registration during reload must target the live runner process
- local control cannot rely on one-off helper VMs that do not affect the running runner instance
- local orchestrator runner-client wiring should explicitly use configured local runner control (`:runner_client` + `:runner_client_opts`) at startup

### Should reload recover a stopped stack?

Recommended default:

- no
- `mix favn.reload` should fail if the stack is not running
- it should print `stack not running; use mix favn.dev`

Reason:

- reload should mean reload, not bootstrap
- hidden auto-start behavior makes failures harder to interpret

## 6. Detailed plan for `mix favn.status`

Category: foundational

### Responsibilities

`mix favn.status` should show:

- whether web/orchestrator/runner are expected to be running
- whether their recorded pids are actually alive
- URLs / ports where applicable
- storage mode
- current active manifest id if available
- last known manifest id if orchestrator is down but cache exists
- recent failure info if a service died unexpectedly

### Output style

Boring operator-friendly text, for example:

```text
Favn local dev stack

status: running
storage: memory
manifest: mv_01H...

web: running pid=12345 url=http://127.0.0.1:4173 log=.favn/logs/web.log
orchestrator: running pid=12346 url=http://127.0.0.1:4101 log=.favn/logs/orchestrator.log
runner: running pid=12347 node=favn_runner_dev@127.0.0.1 log=.favn/logs/runner.log

last failure: none
```

### Data sources

Recommended order:

1. `.favn/runtime.json`
2. live pid/process-group checks
3. local RPC calls for runner/orchestrator details
4. `.favn/manifests/latest.json` fallback when orchestrator is unavailable
5. `.favn/history/last_failure.json` for recent failure/restart notes

## 7. Minimal config model

Category: foundational

Keep the next-PR config small and local-dev-specific.

### Recommended config namespace

Use one small config entry under `apps/favn`:

```elixir
config :favn, :dev,
  storage: :memory,
  sqlite_path: ".favn/data/orchestrator.sqlite3",
  orchestrator_api_enabled: true,
  orchestrator_port: 4101,
  web_port: 4173,
  orchestrator_base_url: "http://127.0.0.1:4101",
  web_base_url: "http://127.0.0.1:4173",
  service_token: nil,
  web_session_secret: nil
```

### Meaning

- `storage`: `:memory | :sqlite`
- `sqlite_path`: only used when storage is `:sqlite`
- `orchestrator_api_enabled`: kept explicit so dev tooling can fail clearly if someone disables the API
- `orchestrator_port`: private local orchestrator HTTP port
- `web_port`: local built-web port
- `orchestrator_base_url`: injected into `favn_web`
- `web_base_url`: printed and used for status output
- `service_token`: optional override; if `nil`, generate and persist in `.favn/secrets.json`
- `web_session_secret`: optional override; if `nil`, generate and persist in `.favn/secrets.json`

### Immediate non-goals for config

Future roadmap only:

- Postgres-first config design
- custom storage adapter config
- release/distribution config model
- a large matrix of web/orchestrator/runner deployment modes

### Internal-only runtime values

Do not put these in the public config model for the next PR:

- local node names
- local Erlang cookie
- log file paths
- process-group ids

These belong in `.favn/runtime.json` or `.favn/secrets.json`, not in user-facing config.

## 8. `.favn/` state model

Category: foundational

### Recommended layout

```text
.favn/
├── runtime.json
├── secrets.json
├── lock
├── logs/
│   ├── orchestrator.log
│   ├── runner.log
│   └── web.log
├── data/
│   └── orchestrator.sqlite3
├── manifests/
│   └── latest.json
└── history/
    └── last_failure.json
```

### File roles

- `runtime.json`: active stack metadata, config hash, owner-session pid, child pids, process groups, ports, node names, start time, current storage mode
- `secrets.json`: generated local service token, web session secret, and local RPC cookie
- `lock`: lifecycle command lock file
- `logs/*`: combined stdout/stderr logs for each service
- `data/orchestrator.sqlite3`: SQLite local persistence when enabled
- `manifests/latest.json`: last built manifest metadata and cache pointer for status/reload reporting
- `history/last_failure.json`: most recent startup/shutdown/failure detection summary

### Ephemeral vs preserved

Ephemeral:

- `runtime.json`
- `lock`

Preserved across stop/start by default:

- `secrets.json`
- `logs/*`
- `data/orchestrator.sqlite3`
- `manifests/latest.json`
- `history/last_failure.json`

### What later `mix favn.reset` should delete

Before-v1 but not next PR:

- everything under `.favn/`, including SQLite data, logs, manifest cache, secrets, and failure history

## 9. Module/file plan

Category: foundational

### `apps/favn` user-facing task entrypoints

- `apps/favn/lib/mix/tasks/favn.dev.ex`
- `apps/favn/lib/mix/tasks/favn.stop.ex`
- `apps/favn/lib/mix/tasks/favn.reload.ex`
- `apps/favn/lib/mix/tasks/favn.status.ex`

### `apps/favn` boring tooling library

- `apps/favn/lib/favn/dev.ex`
  - thin public helper surface for optional IEx mode: `reload/0`, `status/0`, `stop/0`
- `apps/favn/lib/favn/dev/config.ex`
  - resolve config defaults, CLI flags, and generated-secret fallbacks
- `apps/favn/lib/favn/dev/state.ex`
  - read/write `.favn/runtime.json`, `.favn/secrets.json`, `.favn/manifests/latest.json`, and failure history
- `apps/favn/lib/favn/dev/lock.ex`
  - lifecycle lock handling
- `apps/favn/lib/favn/dev/process.ex`
  - foreground owner-session process handling, child process management, signal/timeout handling
- `apps/favn/lib/favn/dev/rpc.ex`
  - local-node RPC helpers for runner/orchestrator control without compile-time deps
- `apps/favn/lib/favn/dev/manifest.ex`
  - build/pin/cache manifest metadata for startup and reload
- `apps/favn/lib/favn/dev/status.ex`
  - live stack inspection and console rendering
- `apps/favn/lib/favn/dev/reload.ex`
  - compile/build manifest, runner restart, manifest publish/register, activation flow
- `apps/favn/lib/favn/dev/orchestrator_client.ex`
  - private HTTP client for manifest publish/activate and status reads needed by local dev tooling
- `apps/favn/lib/favn/dev/service/orchestrator.ex`
  - orchestrator command/env builder
- `apps/favn/lib/favn/dev/service/runner.ex`
  - runner command/env builder
- `apps/favn/lib/favn/dev/service/web.ex`
  - `favn_web` build/start command/env builder
- `apps/favn/lib/favn/dev/stack.ex`
  - high-level coordination for `dev`, `stop`, and shared lifecycle behavior

### Minimal runtime-side helpers outside `apps/favn`

Refactor-enabling, still in scope because they are required to preserve boundaries:

- `apps/favn_orchestrator/lib/favn_orchestrator/runner_client/local_node.ex`
  - local dev runner-client implementation using a localhost-only control path to a separate runner node

Minimal orchestrator-side API addition required for reload:

- `POST /api/orchestrator/v1/manifests`
  - accepts one canonical manifest payload or manifest-version envelope and persists/registers it
  - protected by the existing private service-auth boundary

Optional but not required if raw RPC is sufficient:

- thin orchestrator/runner readiness helper functions in their existing facades

### Explicit boundary note

Product-critical:

- `apps/favn` must not call `FavnOrchestrator` or `FavnRunner` directly as compile-time deps
- any runtime-module references from `apps/favn` should go through local RPC helpers and dynamic module names

## 10. Testing plan

Category: foundational

### Unit tests in `apps/favn/test`

- config resolution tests
- `.favn` path/state read-write tests
- lock behavior tests
- status rendering tests
- stale runtime-state detection tests
- SQLite path normalization/default tests

### Integration tests in `apps/favn/test`

- foreground owner-session lifecycle tests using small fixture processes
- `mix favn.dev` startup sequencing tests with fixture/stub services where direct unit isolation is enough
- `mix favn.stop` graceful-stop and forced-kill fallback tests
- `mix favn.reload` refusal when stack is stopped
- `mix favn.reload` refusal when in-flight runs exist
- `mix favn.reload` runner-only restart behavior while keeping web and orchestrator pids unchanged
- `mix favn.reload` manifest publish behavior against the real private orchestrator API

### Runtime integration coverage

Add focused tests around the local dev runner bridge:

- `apps/favn_orchestrator/test/...` for `runner_client/local_node.ex`
- verify manifest register/submit/cancel calls cross a separate local runner node without compile-time runner deps in orchestrator
- add API tests for `POST /api/orchestrator/v1/manifests` register behavior and follow-up activation flow

### End-to-end smoke path

At least one serial smoke path should cover:

1. `mix favn.dev`
2. `mix favn.status`
3. verify web and orchestrator URLs respond
4. `mix favn.reload`
5. verify active manifest changes or is re-registered cleanly
6. `mix favn.stop`
7. verify runtime state is cleared while logs/SQLite remain

Lifecycle-specific additions:

- test that `mix favn.dev` does not hold `.favn/lock` across the full foreground lifetime
- test second-terminal `reload/status/stop` behavior while foreground `dev` is running
- test startup/bootstrap failure cleanup for stale process/state recovery

Recommended location:

- `apps/favn/test/integration/dev_stack_smoke_test.exs`

This can be slower and serial, but it should exist so the next PR proves the real local lifecycle at least once.

## 11. Internal implementation slicing/order

Category: refactor-enabling

Keep the PR as one coherent feature batch, but implement it in small reviewable slices.

### Slice 1: foundation and boundary-safe control path

- `.favn` state model
- config resolution
- lifecycle lock
- foreground owner-session helpers
- local runner/orchestrator RPC control path

### Slice 2: usable stack lifecycle first

- `mix favn.dev`
- `mix favn.stop`
- `mix favn.status`
- memory mode default
- SQLite path wiring
- foreground owner-session cleanup semantics

### Slice 3: manifest-driven reload workflow

- manifest build/pin/cache
- `mix favn.reload`
- active-manifest reporting in status
- browser-refresh-oriented web behavior
- manifest publish through the private orchestrator API

### Slice 4: optional follow-up polish and smoke coverage

- `Favn.Dev.reload/0`, `status/0`, `stop/0`
- end-to-end smoke path
- doc polish

Priority rule:

- if scope gets tight, do not cut `dev/stop/reload/status`
- cut optional interactive-shell follow-up first, not the foreground core loop

## 12. Required doc updates

Category: foundational

### `README.md`

Update to:

- restate that Phase 8 baseline boundary work now exists
- state that the immediate next PR is core local dev tooling only
- add pointers to the new Phase 9 plan/TODO docs

### `docs/REFACTOR.md`

Update to:

- add Phase 9 plan/TODO doc links
- narrow the immediate Phase 9 slice to core local dev tooling
- explicitly defer install/reset/logs/build tasks out of the next PR while keeping them in Phase 9 or pre-v1 roadmap

### `docs/FEATURES.md`

Update to:

- add a clear next-PR checklist for the core local dev loop
- separate later Phase 9 tooling from the next PR
- keep broader build/install/reset/logs work visible on the roadmap

### Add new docs

Recommended and should land now:

- `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md`
- `docs/refactor/PHASE_9_TODO.md`

Reason:

- this slice is large enough to need its own planning/source-of-truth docs
- `docs/FEATURES.md` should stay high-level while `PHASE_9_TODO.md` stays execution-oriented

## 13. Tooling roadmap before v1

| Item | Timing | Category | Notes |
| --- | --- | --- | --- |
| `mix favn.dev` | next PR | foundational | foreground local stack startup |
| `mix favn.stop` | next PR | foundational | clean shutdown and idempotency |
| `mix favn.reload` | next PR | foundational | daily-driver manifest reload plus runner restart |
| `POST /api/orchestrator/v1/manifests` | next PR | product-critical | no-restart manifest publish path used by local dev and later production flows |
| `mix favn.status` | next PR | foundational | boring operator view |
| optional dedicated interactive shell mode | later Phase 9 if still needed | nice-to-have | likely unnecessary if normal `mix favn.dev` is already foreground |
| `.favn/` local state model | next PR | foundational | runtime metadata, logs, secrets, SQLite path |
| SQLite local persistence mode | next PR | foundational | immediate persistent local option |
| `mix favn.install` | later Phase 9 | refactor-enabling | bootstrap runtime dependencies/setup |
| `mix favn.reset` | later Phase 9 | refactor-enabling | destructive local cleanup, separate from stop |
| `mix favn.logs` | later Phase 9 | nice-to-have | log tailing/selection on top of preserved logs |
| `mix favn.build.web` | before v1 but not next | product-critical | honest split deployment packaging |
| `mix favn.build.orchestrator` | before v1 but not next | product-critical | honest split deployment packaging |
| `mix favn.build.runner` | before v1 but not next | product-critical | honest split deployment packaging |
| `mix favn.build.single` | before v1 but not next | product-critical | first supported single-node assembly |
| bootstrap/admin setup task | before v1 but not next | product-critical | replace ad hoc env-only bootstrap for real product setup |
| broader live Postgres verification path | later Phase 9 | refactor-enabling | production-like confidence, not core local loop |
| durable auth/session hardening | before v1 but not next | product-critical | not part of next PR |
| richer log/observability UX | future/nice-to-have | nice-to-have | after core lifecycle is stable |
| Docker/dev-container local mode | future/nice-to-have | future roadmap only | explicitly not required for next PR |

## 14. Open questions with recommended defaults

### 1. How should local orchestrator-to-runner, manifest publish, and second-terminal control work before broader transport packaging?

Recommended default:

- use `.favn/runtime.json` plus a localhost-only local control path for lifecycle actions
- use the private orchestrator API for manifest publish/register + activate

Why:

- separate processes stay intact
- no new public API surface is needed
- no compile-time dependency from `apps/favn` to runtime apps

### 2. Should `mix favn.reload` cancel in-flight runs automatically?

Recommended default:

- no
- fail fast and ask the user to wait/cancel/stop first

### 3. Should `mix favn.reload` auto-start the stack if it is down?

Recommended default:

- no
- keep bootstrap and reload as separate commands

### 4. Should `mix favn.dev` try to auto-repair partial stacks?

Recommended default:

- no for the first cut
- print status and require `mix favn.stop` first

### 5. Should a dedicated `mix favn.dev --interactive` land in the next PR?

Recommended default:

- no separate dedicated mode is required in the next PR if normal `mix favn.dev` is already foreground
- add one later only if we discover a real debugging need beyond the normal foreground loop

### 6. Which helper functions should be available in IEx?

Recommended default:

- `Favn.Dev.reload()`
- `Favn.Dev.status()`
- `Favn.Dev.stop()`

Boundary warning:

- do not add helper shortcuts that call across steady-state product boundaries in-process as the normal workflow
- helpers should remain lifecycle/debug utilities only
