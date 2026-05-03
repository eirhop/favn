# Phase 9 Remaining Dev Tooling And Packaging Plan

## Status

Source-of-truth completion record for the Phase 9 developer-tooling and
packageability slice after the command surface landed.

This document starts from the now-implemented core lifecycle baseline:

- `mix favn.dev`
- `mix favn.dev --sqlite`
- `mix favn.stop`
- `mix favn.reload`
- `mix favn.status`

It does not reopen that work. The command surface below now exists and should be
treated as implemented first-cut behavior:

- `mix favn.install`
- `mix favn.reset`
- `mix favn.logs`
- `mix favn.build.web`
- `mix favn.build.orchestrator`
- `mix favn.build.runner`
- `mix favn.build.single`
- `mix favn.read_doc`

The Phase 9 batch is now closed for v0.5 scope.

Current packaging reality:

- `mix favn.build.runner` is the most complete packaging target today
- `mix favn.build.web` and `mix favn.build.orchestrator` now emit explicit
  metadata-oriented artifacts with honest `artifact` metadata and
  `OPERATOR_NOTES.md`
- `mix favn.build.single` now emits an explicit topology-preserving assembly
  artifact with honest non-operational start/stop scripts and
  `OPERATOR_NOTES.md`

## 1. Feature Summary

Phase 9 should close by making the already-corrected `web + orchestrator +
runner` topology reliable locally and honest to package for deployment, without
exposing internal apps as user-facing dependencies.

Recommended default:

- keep `{:favn, ...}` as the only user dependency
- keep all Favn-managed project side effects under `.favn/`
- make `mix favn.install` the explicit prerequisite for tooling that needs
  resolved runtime inputs
- keep build targets explicit and topology-preserving
- make `single` a convenience assembly of three explicit runtimes, not one
  collapsed runtime
- finish hardening lifecycle recovery and packaging honesty so the
  already-implemented command surface is reliable, test-covered, and
  operationally truthful

## 2. Current Phase 9 Reality

### Implemented command surface

- `mix favn.install`
- `mix favn.reset`
- `mix favn.logs`
- `mix favn.build.web`
- `mix favn.build.orchestrator`
- `mix favn.build.runner`
- `mix favn.build.single`
- `mix favn.read_doc`

### Partially realized packaging work

- install layout, fingerprinting, toolchain capture, runtime-input recording,
  and failure-history plumbing are in place
- `build.runner` already packages the strongest end-to-end artifact shape in this
  phase
- `build.web` and `build.orchestrator` now ship explicit metadata-oriented
  packaging contracts (`artifact.kind=assembly_metadata`) with operator notes
- `build.single` now ships a project-local backend-only SQLite launcher contract
  (`artifact.kind=project_local_backend_launcher`, `operational=false`) with
  managed start/stop scripts that still depend on the installed runtime source
  root

### Remaining hardening work

- no remaining Phase 9 hardening work

## 3. Scope And Non-Goals

### In scope

- explicit install/setup flow
- destructive local reset flow
- boring log access helper
- split build targets for `web`, `orchestrator`, and `runner`
- single-node assembly target
- project-local artifact/state layout for install, build, dist, logs, runtime,
  SQLite data, manifest cache, and failure history
- recovery/diagnostics validation needed to close Phase 9 honestly

### Out of scope

- production hardening and safe-release security work
- Docker as the default local or packaging path
- a general-purpose environment doctor beyond targeted diagnostics needed for
  these commands
- reintroducing same-BEAM product shortcuts
- reopening `dev`, `stop`, `reload`, or `status` as new feature work
- magical auto-deploy or remote rollout automation

## 4. Assumptions

- public package topology migration is complete
- `apps/favn` is the thin public wrapper and public `mix favn.*` entrypoint owner
- `apps/favn_authoring` owns authoring and manifest-facing implementation
- `apps/favn_local` owns local lifecycle/tooling and packaging implementation
- `web/favn_web` remains the web workspace/package source of truth
- local lifecycle already persists project-local runtime state in `.favn/`
- manifest publish/activate over the private orchestrator API already exists

## 5. Locked Constraints

1. one public dependency: users only need `{:favn, ...}`
2. project-local side effects only: Favn-managed install/build/runtime artifacts
   live under `.favn/`
3. preserve runtime boundaries: local and packaged flows must keep explicit
   web/orchestrator/runner separation
4. no Docker requirement by default
5. do not replace already-landed lifecycle commands
6. do not treat production hardening as Phase 9 scope

## 6. Command Set And Semantics

### Storage configuration rule

Recommended default:

- storage selection is explicit
- storage is always an orchestrator concern
- the same logical storage modes should be supported across local dev and
  packaging, while the default differs by workflow

Supported storage modes for this Phase 9 slice:

- `memory`
- `sqlite`
- `postgres`

Workflow defaults:

- local dev default: `memory`
- local dev persistent convenience mode: `sqlite`
- single-node packaging default: `sqlite`
- split deployment recommended default: `postgres`

Important boundary rule:

- web and runner do not choose storage directly
- they only receive the orchestrator address and related credentials/config
- storage adapter selection and storage connection config belong to orchestrator

### Proposed public config shape

Recommended default:

- keep public user configuration under `config :favn, :local` for local dev
- keep packaging/deployment configuration explicit through generated env files
  and documented environment variables
- do not require users to configure internal apps directly in the normal path

Example local config:

```elixir
config :favn, :local,
  storage: :memory,
  sqlite_path: ".favn/data/orchestrator.sqlite3",
  postgres: [
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "my_app_favn_dev",
    ssl: false,
    pool_size: 10
  ]
```

Meaning:

- `storage`: `:memory | :sqlite | :postgres`
- `sqlite_path`: used when `storage: :sqlite`
- `postgres`: used when `storage: :postgres`

Recommended command-line overrides:

- `mix favn.dev` uses configured local storage mode
- `mix favn.dev --sqlite` forces `storage: :sqlite`
- `mix favn.dev --postgres` forces `storage: :postgres`
- `mix favn.build.single --storage sqlite|postgres` overrides the single bundle
  default explicitly

Recommendation:

- keep `mix favn.dev --sqlite` and `mix favn.dev --postgres` explicit
- keep `mix favn.build.single --storage sqlite|postgres` explicit
- do not add a general `--storage` flag if named modes keep the UX clearer

Alternative:

- use one generic `--storage <mode>` flag everywhere

Pros:

- more uniform CLI

Cons:

- slightly more abstract for the most common cases

Recommendation:

- named convenience flags are fine for local dev if the underlying config model
  still uses explicit storage modes

### `mix favn.install`

Current status:

- implemented first cut

Recommended default:

- explicit prerequisite/setup command
- resolves and verifies the project-local runtime inputs needed by local dev and
  build/package commands
- writes an install fingerprint under `.favn/install/`
- does not silently run from other commands

What it installs or resolves:

- version-matched internal runtime build inputs for `web`, `orchestrator`, and
  `runner`
- project-local JS dependency state required by `favn_web`
- project-local toolchain/install metadata used to detect stale state later

What it should not do by default:

- it should not produce final deployment bundles
- it should not start services
- it should not mutate runtime state outside `.favn/install/` and failure history

Should it resolve/build local web and orchestrator runtime artifacts ahead of time?

Recommended default:

- resolve install inputs ahead of time: yes
- build final target artifacts ahead of time: no

Why:

- it keeps install explicit and reusable
- it avoids stale hidden deployment builds
- it keeps `build.*` commands as the place where final artifacts are created

Should it verify JS/node/npm tooling for `favn_web`?

- yes
- missing Node/npm is one of the main reasons local dev or packaging would fail
- install is the boring place to fail early with a targeted message

Auto-trigger behavior:

- recommended default: no auto-trigger
- `mix favn.dev` and `mix favn.build.*` should validate install state and fail
  with `install required` or `install stale; run mix favn.install`

Offline behavior:

- if `.favn/install/install.json` matches the required fingerprint, later
  commands may proceed offline
- if install inputs are missing or stale, offline operation should fail fast and
  explicitly

Missing-tool behavior:

- fail early during install with actionable diagnostics
- record the failure under `.favn/history/`
- do not leave partially-written runtime state

Stale-artifact behavior:

- compare current inputs against an install fingerprint covering at least:
  - `favn` package version
  - runtime source fingerprint for `favn_web` and internal release inputs
  - relevant lockfiles/tool versions used during install
- if stale, fail explicit commands and require a fresh install
- allow `mix favn.install --force` as the boring rebuild path

Alternative:

- auto-run install from `dev` and `build`

Pros:

- fewer manual steps for first-time users

Cons:

- hidden network/toolchain side effects
- harder-to-explain failures
- less predictable CI/operator behavior

Recommendation:

- keep install explicit

### `mix favn.reset`

Current status:

- implemented first cut

Recommended default:

- destructive local cleanup command that removes all Favn-managed project-local
  state under `.favn/`

Semantics:

- require the stack to be stopped first
- if recorded runtime state exists and live Favn-owned processes are still
  running, fail and tell the user to run `mix favn.stop`
- if runtime state is stale and no live processes remain, clear the stale state
  and continue
- delete the full `.favn/` tree

Should it delete all of `.favn/`?

- yes
- first cut should stay simple and honest

Should there be `--keep-*` modes?

- recommended default: no

Why:

- `reset` should mean full reset
- partial cleanup modes create more semantics and testing cost than value in the
  first cut

Alternative:

- `--keep-logs`, `--keep-install`, `--keep-data`

Pros:

- more operator convenience

Cons:

- more surprising combinations
- more edge-case behavior to document and test

Recommendation:

- keep the first cut destructive and simple

### `mix favn.logs`

Current status:

- implemented first cut

Recommended default:

- thin file-tail helper over preserved log files under `.favn/logs/`

Supported behavior:

- default: show recent logs for all services
- service selection: `web`, `orchestrator`, or `runner`
- `--tail N`
- `--follow`
- historical access while the stack is down

Operational shape:

- read known log paths from runtime/configured layout, not from process stdout
- when showing all services, print service-prefixed output in stable order
- when following all services, multiplex the three files with service prefixes

What it should not become:

- not a structured observability system
- not a log indexing/query engine
- not a replacement for reading the raw files directly

Alternative:

- add structured JSON querying and filtering now

Pros:

- richer operator experience

Cons:

- not needed to close Phase 9
- pushes the design toward observability work instead of boring tooling

Recommendation:

- keep it as a thin helper

### `mix favn.read_doc`

Current status:

- implemented first cut

Operational meaning:

- read module/function docs from locally available compiled modules through
  `Code.fetch_docs/1`
- support `mix favn.read_doc ModuleName` and
  `mix favn.read_doc ModuleName function_name`
- function-name form returns all public arities for that name

Scope guardrails:

- no fuzzy search
- no full documentation browser
- no source-jump behavior
- no external HexDocs/web fetching
- private functions are not shown in first-cut public output

### `mix favn.build.web`

Current status:

- implemented with explicit metadata-oriented output and packaging honesty
  metadata/operator notes

Operational meaning:

- build the deployable public web/BFF artifact for the current Favn version
- does not include user business code
- expects to talk to a separately deployed orchestrator over the private API

Recommended default:

- consume installed `favn_web` build inputs from `.favn/install/`
- write a final artifact bundle under `.favn/dist/web/<build_id>/`
- write build working files under `.favn/build/web/<build_id>/`

### `mix favn.build.orchestrator`

Current status:

- implemented with explicit metadata-oriented output and packaging honesty
  metadata/operator notes

Operational meaning:

- build the deployable private orchestrator artifact for the current Favn version
- does not include user business code
- remains storage-neutral at build time; runtime storage config is provided by
  the operator

Recommended default:

- consume installed orchestrator release inputs from `.favn/install/`
- write a final artifact bundle under `.favn/dist/orchestrator/<build_id>/`
- write build working files under `.favn/build/orchestrator/<build_id>/`

### `mix favn.build.runner`

Current status:

- implemented and currently the most complete packaging target in Phase 9

Operational meaning:

- build the project-specific execution artifact that combines the internal runner
  runtime with the user's business code and one pinned manifest version

Recommended default:

- consume the current user project, installed runner build inputs, selected
  plugins, and a freshly pinned manifest version
- write a final artifact bundle under `.favn/dist/runner/<build_id>/`
- write build working files under `.favn/build/runner/<build_id>/`

### `mix favn.build.single`

Current status:

- implemented with the correct topology and explicit non-operational assembly
  semantics via metadata, scripts, and operator notes

Operational meaning:

- assemble one deployable bundle that contains separate web, orchestrator, and
  runner runtimes plus wiring/config needed to run them together on one node
- convenience topology only; it must not collapse the architecture conceptually

Recommended default:

- internally build or reuse matching `web`, `orchestrator`, and `runner`
  artifacts from the same build batch
- write a final assembled bundle under `.favn/dist/single/<build_id>/`
- write assembly working files under `.favn/build/single/<build_id>/`

## 7. `.favn/` Artifact And State Layout

Recommended layout:

```text
.favn/
├── runtime.json
├── secrets.json
├── lock
├── install/
│   ├── install.json
│   ├── toolchain.json
│   ├── cache/
│   │   └── npm/
│   └── runtimes/
│       ├── web/
│       │   ├── source/
│       │   └── node_modules/
│       ├── orchestrator/
│       │   └── source/
│       └── runner/
│           └── template/
├── build/
│   ├── web/<build_id>/
│   ├── orchestrator/<build_id>/
│   ├── runner/<build_id>/
│   └── single/<build_id>/
├── dist/
│   ├── web/<build_id>/
│   ├── orchestrator/<build_id>/
│   ├── runner/<build_id>/
│   └── single/<build_id>/
├── logs/
│   ├── web.log
│   ├── orchestrator.log
│   └── runner.log
├── data/
│   └── orchestrator.sqlite3
├── manifests/
│   ├── latest.json
│   └── cache/
│       └── <manifest_version_id>.json
└── history/
    ├── last_failure.json
    └── failures/
        └── <timestamp>-<command>.json
```

### Directory roles

- `runtime.json`: current live stack state only
- `secrets.json`: generated local secrets and local control credentials
- `lock`: short-lived lifecycle mutation lock
- `install/`: resolved project-local runtime inputs reused by `dev` and build
  commands
- `build/`: reproducible intermediate working directories and per-build metadata
- `dist/`: operator-facing output bundles
- `logs/`: preserved service logs
- `data/`: preserved local SQLite state
- `manifests/latest.json`: last built manifest metadata for status/reload flows
- `manifests/cache/`: serialized pinned manifests reused by build/package flows
- `history/`: recent failure summaries and preserved per-command failure records

### Ephemeral vs preserved

Ephemeral:

- `runtime.json`
- `lock`
- `build/` contents are reproducible scratch data, though preserved until reset

Preserved by default:

- `install/`
- `dist/`
- `logs/`
- `data/`
- `manifests/`
- `history/`
- `secrets.json`

### What `mix favn.reset` deletes

- all of `.favn/`
- including install artifacts, dist outputs, logs, SQLite data, manifest cache,
  secrets, and failure history

Alternative:

- keep `dist/` outside `.favn/`

Pros:

- easier manual discovery for operators

Cons:

- breaks the project-local side-effect rule
- scatters Favn-owned artifacts

Recommendation:

- keep all Favn-managed outputs under `.favn/`

## 8. Build And Package Contracts

### Shared build contract

Each `mix favn.build.*` command should:

- require a current successful install fingerprint
- create a new `build_id`
- write build metadata under `.favn/build/<target>/<build_id>/build.json`
- write final artifact metadata under `.favn/dist/<target>/<build_id>/metadata.json`
- leave the final operator-consumable bundle in `.favn/dist/<target>/<build_id>/`

Recommended shared metadata fields:

- `schema_version`
- `target`
- `build_id`
- `built_at`
- `favn_package_version`
- `install_fingerprint`
- `elixir_version`
- `otp_release`
- `source_fingerprint`
- `required_env`
- `compatibility`

### Versioning and compatibility representation

Recommended default:

- write explicit compatibility metadata rather than hiding assumptions in file
  names

At minimum:

- web artifacts declare orchestrator API compatibility, for example `api_v1`
- orchestrator artifacts declare supported API/storage/runtime compatibility
- runner artifacts declare manifest schema version, manifest version id, runner
  contract version, and included plugin list
- single artifacts declare the exact `web`, `orchestrator`, and `runner` build
  ids assembled together

Alternative:

- rely only on package version matching

Pros:

- fewer fields

Cons:

- too implicit for debugging mismatches
- weaker operator story during upgrades

Recommendation:

- include explicit compatibility metadata files

### `mix favn.build.web`

Consumes:

- `.favn/install/runtimes/web/`

Outputs:

- `.favn/build/web/<build_id>/`
- `.favn/dist/web/<build_id>/`

Dist contents should include at least:

- deployable web runtime bundle
- `metadata.json`
- `README.txt` or equivalent operator notes for required env/config

Required metadata should include at least:

- target `web`
- build id
- Favn version
- supported orchestrator API version
- required env such as orchestrator base URL, service token, and public web origin

### `mix favn.build.orchestrator`

Consumes:

- `.favn/install/runtimes/orchestrator/`

Outputs:

- `.favn/build/orchestrator/<build_id>/`
- `.favn/dist/orchestrator/<build_id>/`

Dist contents should include at least:

- deployable orchestrator runtime bundle
- `metadata.json`
- operator notes for storage and service-auth configuration

Required metadata should include at least:

- target `orchestrator`
- build id
- Favn version
- supported API version
- supported storage modes and adapter expectations
- runner contract compatibility version

Recommended default storage stance:

- storage-neutral build artifact
- runtime config selects memory, SQLite, or Postgres
- operator recommendation for split deployment: Postgres

Supported runtime configuration contract:

- `FAVN_STORAGE=memory|sqlite|postgres`
- if `sqlite`:
  - `FAVN_SQLITE_PATH=/path/to/orchestrator.sqlite3`
- if `postgres`:
  - `FAVN_POSTGRES_HOST`
  - `FAVN_POSTGRES_PORT`
  - `FAVN_POSTGRES_USERNAME`
  - `FAVN_POSTGRES_PASSWORD`
  - `FAVN_POSTGRES_DATABASE`
  - `FAVN_POSTGRES_SSL`
  - optional pool and timeout env if needed

Why this should be explicit:

- it makes the storage support matrix visible
- it keeps local, single-node, and split deployment stories aligned
- it avoids implying that SQLite is the only durable option outside dev

### `mix favn.build.runner`

Recommended default:

- one runner build packages one pinned manifest version

Consumes:

- current user project code
- selected plugins from the current dependency graph/config
- freshly generated and pinned manifest from `favn_authoring`
- `.favn/install/runtimes/runner/`

Outputs:

- `.favn/build/runner/<build_id>/`
- `.favn/dist/runner/<build_id>/`

Dist contents should include at least:

- deployable runner runtime bundle
- compiled user business code
- pinned serialized manifest
- manifest metadata envelope
- included plugin inventory
- `metadata.json`

Required metadata should include at least:

- target `runner`
- build id
- project/app identity
- Favn version
- manifest schema version
- manifest version id
- manifest hash
- runner contract version
- included plugin identifiers and versions

Runner packaging rule:

- `favn_runner` remains internal and is not part of the user's public authoring
  dependency surface
- the runner artifact still includes the internal runner runtime because that is
  a packaging concern, not a public dependency concern
- the build task, not the user's dependency list, is responsible for assembling
  the internal runner runtime around the authored project code

Alternative:

- package runner without a manifest and fetch manifests remotely at runtime

Pros:

- fewer runner rebuilds when manifests change

Cons:

- weaker deployment determinism
- more complex remote coordination before Phase 9 is even complete
- easier to blur the line between authored code version and runnable payload

Recommendation:

- keep the first cut pinned to one manifest version per runner build

### `mix favn.build.single`

Recommended default:

- assemble a single deployable bundle containing three explicit runtimes and
  their wiring, not one merged BEAM system

Consumes:

- matching `web`, `orchestrator`, and `runner` outputs from the same build batch
  or internally generated equivalents

Outputs:

- `.favn/build/single/<build_id>/`
- `.favn/dist/single/<build_id>/`

Dist contents should include at least:

- `web/`
- `orchestrator/`
- `runner/`
- `config/assembly.json`
- `env/web.env`
- `env/orchestrator.env`
- `env/runner.env`
- `bin/start`
- `bin/stop`
- `metadata.json`

Config passing model:

- generate one assembly manifest that records service addresses, shared secrets,
  storage choice, and matching build ids
- start scripts translate that assembly manifest into per-service env/config at
  runtime

Recommended default storage mode:

- SQLite

Why:

- memory is not operator-friendly for a packaged single-node bundle
- Postgres adds an external dependency that fights the point of the single-node
  convenience mode

Alternative:

- one merged runtime

Pros:

- superficially simpler packaging

Cons:

- collapses the architecture and teaches the wrong mental model
- makes later split deployment less honest

Recommendation:

- keep three explicit runtimes inside one bundle

Supported single-node storage modes:

- `sqlite` by default
- `postgres` explicitly when the operator wants an external durable database even
  for a single-node runtime bundle

Recommended first-cut single-node config contract:

- bundle writes `config/assembly.json` with `storage.mode`
- generated env files include either:
  - `FAVN_STORAGE=sqlite` plus `FAVN_SQLITE_PATH=...`
  - or `FAVN_STORAGE=postgres` plus explicit Postgres env
- `bin/start` reads the assembly config and exports orchestrator env before
  starting the separate runtimes

Important recommendation:

- support Postgres in single-node mode, but do not make it the default
- single-node Postgres should mean "single compute node, external Postgres",
  not an embedded Postgres dependency inside the bundle

### Split deployment contract

Recommended honest operator story:

- `build.web` produces the public web/BFF artifact only
- `build.orchestrator` produces the private control-plane artifact only
- `build.runner` produces the project-specific execution artifact only
- the operator deploys them separately and wires them with explicit URLs,
  credentials, and runner registration/publish steps

Important honesty rule:

- split deployment is not one command that secretly assumes one node
- it is three separate artifacts with explicit compatibility metadata

## 9. Ownership By App

### `apps/favn`

Owns:

- public package identity
- public `Mix.Tasks.Favn.*` entrypoints only
- public docs/examples for user-facing flows
- thin delegation to `favn_authoring` and `favn_local`

Should not own:

- `.favn/` filesystem logic
- install/build/reset/logs implementation details
- direct compile-time dependencies on runtime product apps

### `apps/favn_local`

Owns:

- `.favn/` artifact/state layout
- install/reset/logs implementation
- build/package orchestration and artifact writers
- local validation and diagnostics behavior
- recovery/cleanup semantics around project-local state

Recommended implementation ownership:

- `Favn.Dev.Install`
- `Favn.Dev.Reset`
- `Favn.Dev.Logs`
- `Favn.Dev.Build.*`
- shared `.favn` metadata/diagnostics helpers

### `apps/favn_authoring`

Owns:

- manifest generation, serialization, hashing, and pinning used by build flows
- plugin resolution from the authored project surface
- runner-facing manifest/build inputs consumed by `favn_local`

Should not own:

- local runtime state
- project-local install/build/dist directory policy
- process lifecycle or log handling

### `apps/favn_core`

Owns only if needed:

- shared structs/types/contracts for manifest or compatibility metadata that are
  genuinely cross-boundary

Should not absorb:

- packaging workflows
- project-local tooling orchestration

### Runtime apps

`favn_orchestrator`, `favn_runner`, and `favn_web` should only gain minimal
export/build hooks or compatibility metadata seams needed to support packaging.
They should not become owners of public build-task workflow logic.

## 10. User Workflows

### First-time setup

1. add `{:favn, ...}`
2. configure authored modules normally
3. run `mix favn.install`
4. run `mix favn.dev`
5. use `mix favn.status`, `mix favn.logs`, `mix favn.reload`, and `mix favn.stop`
   during normal iteration

### Normal local dev

1. `mix favn.install` once per relevant runtime/tooling change
2. `mix favn.dev`
3. `mix favn.reload` for manifest/project changes
4. `mix favn.logs` when needed
5. `mix favn.stop`
6. `mix favn.reset` only when a full local wipe is desired

Local storage examples:

- memory default via config:

```elixir
config :favn, :local, storage: :memory
```

- SQLite local persistence:

```elixir
config :favn, :local,
  storage: :sqlite,
  sqlite_path: ".favn/data/orchestrator.sqlite3"
```

- Postgres local dev:

```elixir
config :favn, :local,
  storage: :postgres,
  postgres: [
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "my_app_favn_dev",
    ssl: false
  ]
```

### Build runner

1. run `mix favn.install`
2. run `mix favn.build.runner`
3. take the output from `.favn/dist/runner/<build_id>/`
4. deploy that artifact as the execution runtime for the current project build
5. publish/activate the included manifest version in the target orchestrator

### Build split deployment

1. run `mix favn.install`
2. run `mix favn.build.web`
3. run `mix favn.build.orchestrator`
4. run `mix favn.build.runner`
5. deploy web, orchestrator, and runner separately
6. wire explicit config between them using the produced metadata/env contract
7. publish and activate the runner artifact's included manifest in orchestrator

Split deployment storage recommendation:

- use Postgres unless there is a deliberate reason to keep the control plane on
  SQLite
- configure storage only on orchestrator deployment inputs
- web and runner stay storage-agnostic

### Build single-node deployment

1. run `mix favn.install`
2. run `mix favn.build.single`
3. deploy the assembled bundle from `.favn/dist/single/<build_id>/`
4. start the bundle with the generated single-node scripts
5. default to SQLite-backed orchestrator storage inside the bundle

Single-node Postgres option:

- `mix favn.build.single --storage postgres`
- generated bundle expects explicit Postgres env in the orchestrator config
- web and runner configuration stay unchanged apart from the orchestrator base
  address and credentials

## 11. Testing Strategy

### Install and artifact layout

- install layout creation tests
- install fingerprint and stale-detection tests
- missing Node/npm diagnostics tests
- offline reuse tests when install state is current

### Reset and logs

- reset refusal while live stack is still running
- reset success after stop
- reset cleanup of full `.favn/`
- logs access for all services and single-service selection
- logs historical access when the stack is down
- follow/tail behavior coverage

### Build contracts

- metadata contract tests for `web`, `orchestrator`, `runner`, and `single`
- output layout tests for `.favn/build/` and `.favn/dist/`
- runner build tests that assert inclusion of user code, pinned manifest, and
  selected plugins
- single build tests that assert three explicit service bundles are present

### Validation and polish

- stale runtime recovery when all services are dead
- partial/dead service recovery through `status` plus `stop`
- startup failure cleanup and failure-history recording
- explicit SQLite smoke path across install, dev, reload, stop, logs, reset, and
  single packaging
- broader opt-in Postgres verification for local and orchestrator packaging paths
- port conflict and missing prerequisite diagnostics tests

Recommended default:

- keep Postgres verification opt-in through explicit environment-driven test
  paths

## 12. Docs That Must Be Updated

- `README.md`
  - keep the explicit `mix favn.install` first-time setup step aligned with the
    implemented command surface
  - keep the honest distinction between local dev, split deployment, and
    single-node packaging
- `docs/REFACTOR.md`
  - keep the Phase 9 completion status aligned with this plan
- `docs/FEATURES.md`
  - keep the high-level roadmap wording aligned with Phase 9 completion
- `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md`
  - this document is the architecture source of truth for the completion record
- `docs/refactor/PHASE_9_TODO.md`
  - execution checklist derived from this design

## 13. Open Questions With Recommended Defaults

### Should `mix favn.install` be explicit-only or auto-triggered?

Recommended default:

- explicit-only

### Should install produce final web/orchestrator builds ahead of time?

Recommended default:

- no
- resolve install inputs and JS dependencies only

### Should `mix favn.reset` offer keep modes in the first cut?

Recommended default:

- no

### Should `mix favn.logs` stay a thin helper or become structured log tooling?

Recommended default:

- thin helper only

### Should `mix favn.build.runner` package one or many manifest versions?

Recommended default:

- one pinned manifest version per build

### What should single-node default storage be?

Recommended default:

- SQLite

### Should Postgres be supported for local dev, single-node, and split deployment?

Recommended default:

- yes
- local dev: supported but not default
- single-node: supported but not default
- split deployment: supported and recommended default

Reason:

- it keeps the storage support story honest and explicit
- it avoids locking packaged workflows to SQLite-only assumptions
- it matches the fact that storage is already an orchestrator adapter concern

### Should split deployment artifacts hide the deployment topology?

Recommended default:

- no
- keep three explicit artifacts and explicit config wiring

### How much diagnostics work belongs in Phase 9?

Recommended default:

- targeted diagnostics only: missing install, missing Node/npm, stale state,
  startup cleanup, and port conflicts
- do not expand into a broad doctor framework yet

## 14. Suggested Remaining Execution Order

### Slice 1: close install-state validation gaps

- make `mix favn.dev` and `mix favn.build.*` fail clearly when install state is
  missing or stale
- finish targeted prerequisite and stale-state diagnostics
- verify offline reuse and failure recording semantics

### Slice 2: finish lifecycle hardening

- harden stale-runtime recovery and partial/dead service cleanup behavior
- verify startup failure cleanup and idempotent stop semantics
- finish targeted port-conflict diagnostics

### Slice 3: finish packaging honesty

- keep `build.runner` as the reference for what an operationally honest build
  should look like
- keep `build.web` and `build.orchestrator` explicitly metadata-oriented until
  true deployable packaging is introduced in a later slice, and keep those
  semantics honest in metadata and docs
- keep `build.single` topology-preserving and backend-only while the launcher is
  still project-local; self-contained runtime closure, executed start/stop
  verification, web launchers, and split-topology launchers remain later slices

### Slice 4: finish verification and docs

- close remaining stale-state diagnostics polish
- close remaining install fingerprint/offline-reuse verification polish
- finish docs so Phase 9 is described as hardening, validation, and packaging
  honesty rather than command creation

Slice status: completed.

Priority rule:

- if scope must be narrowed temporarily, keep `install` and `build.runner`
  ahead of optional niceties
- do not cut the topology-preserving `single` semantics even if implementation is
  minimal

## 15. Acceptance Criteria For Phase 9 Completion

Phase 9 is complete when all of the following are true:

1. users can start from `{:favn, ...}` and run `mix favn.install` followed by
   `mix favn.dev` without needing to understand internal apps
2. all Favn-managed install/build/runtime side effects live under `.favn/`
3. `mix favn.reset` fully removes project-local Favn state after the stack is
   stopped
4. `mix favn.logs` provides boring access to current and historical service logs
5. `mix favn.build.web`, `mix favn.build.orchestrator`, and
   `mix favn.build.runner` each produce explicit operator-facing artifacts with
   compatibility metadata
6. `mix favn.build.runner` packages user code, internal runner runtime, pinned
   manifest, and selected plugins without making `favn_runner` part of the
   public dependency surface
7. `mix favn.build.single` produces one bundle containing explicit web,
   orchestrator, and runner runtimes, not one collapsed runtime
8. lifecycle recovery, partial/dead service recovery, startup cleanup, explicit
   SQLite verification, and broader opt-in Postgres verification all have
   coverage
9. missing prerequisite, stale state, and port-conflict failures produce
   actionable diagnostics
10. docs describe the local and packaging story honestly without same-BEAM or
     machine-global assumptions

## 16. Post-v0.5 Follow-Up

These do not belong in the active Phase 9 execution checklist:

- watch mode and auto-reload
- broader doctor or environment validation frameworks
- richer log tooling or log-query features
- restart-single-service convenience flows
- convenience niceties beyond the targeted diagnostics needed for the current
  command surface
