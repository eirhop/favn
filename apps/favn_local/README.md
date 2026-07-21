# favn_local

`favn_local` owns local developer tooling and project-local packaging workflows
for Favn.

`apps/favn` exposes the public `mix favn.*` tasks. Those tasks delegate to
`Favn.Dev` in this app.

## Scope and ownership

`favn_local` owns:

- local stack lifecycle implementation (`dev`, `stop`, `status`, `reload`)
- local project bootstrap and validation (`init`, `doctor`)
- install/reset/log tooling (`install`, `reset`, `logs`)
- local run investigation and cancellation (`runs`, `logs RUN_ID`)
- local SQL data inspection and read-only querying (`inspect`, `query`)
- local operational-backfill submission, planning, inspection, rerun, and repair
- runner/manifest release construction and deployment operations
  (`build.runner`, `build.manifest`, `publish`, `activate`)
- project-local filesystem state under `.favn/`

`favn_local` does not define public authoring APIs. Authoring and manifest
compile logic remains in `favn_authoring`.

## Public task surface

These tasks are exposed by `apps/favn` and implemented by `favn_local`:

- `mix favn.install`
- `mix favn.init`
- `mix favn.doctor`
- `mix favn.dev`
- `mix favn.status`
- `mix favn.run`
- `mix favn.backfill`
- `mix favn.runs`
- `mix favn.logs`
- `mix favn.inspect`
- `mix favn.query`
- `mix favn.diagnostics`
- `mix favn.reload`
- `mix favn.stop`
- `mix favn.reset`
- `mix favn.build.runner`
- `mix favn.build.manifest`
- `mix favn.publish`
- `mix favn.activate`
- `mix favn.build.web`
- `mix favn.build.orchestrator`
- `mix favn.build.single`
- `mix favn.bootstrap.single`

`mix favn.backfill` is a local operator convenience over the private
orchestrator backfill HTTP endpoints. It is not a stable external API contract.

## Typical usage

### First-time local setup

```bash
mix favn.init --duckdb --sample
mix deps.get
mix favn.doctor
mix favn.install
mix favn.dev
mix favn.dev --scheduler
```

`mix favn.init --duckdb --sample` generates a minimal DuckDB-backed consumer
sample: connection module, namespace modules, raw Elixir load asset, downstream
SQL asset, a `deps(:all)` pipeline, local Favn config, and `.env.example` values
for local web login. Generated files are idempotent: matching files are reported
as already present, and changed files are left untouched.

### Inspect and iterate

```bash
mix favn.status
mix favn.run MyApp.Pipelines.Daily
mix favn.run MyApp.Pipelines.Monthly --window month:2026-03 --timezone Europe/Oslo
mix favn.run MyApp.Source.Events:movement --window month:2026-07 \
  --dependencies none --refresh force_selected
mix favn.backfill submit MyApp.Pipelines.Daily --from 2026-04-01 --to 2026-04-07 --kind day
mix favn.backfill submit MyApp.Pipelines.Monthly --window month:2025-05..2026-05 --dry-run
mix favn.backfill windows RUN_ID
mix favn.runs list --status error --limit 20
mix favn.runs show RUN_ID
mix favn.runs cancel RUN_ID
mix favn.runs cancel RUN_ID --wait --wait-timeout-ms 30000
mix favn.logs RUN_ID
mix favn.inspect relation raw.sales.orders
mix favn.inspect partitions raw.sales.orders
mix favn.query "select count(*) from raw.sales.orders"
mix favn.logs --service runner --tail 200
mix favn.reload
mix favn.stop
```

`mix favn.inspect ...` and `mix favn.query "select ..."` are safe to run
directly from a consumer project. The tasks load `.env` before consumer runtime
config and start only `:favn_sql_runtime` before connecting, so the SQL session
pool is supervised without starting the consumer app or its plugins.

`mix favn.runs cancel RUN_ID` requests cancellation through the local
orchestrator HTTP API using the trusted local-dev context. Add `--wait` when the
command should poll that run until it is terminal or the local wait timeout
expires. Backfill parent runs still need explicit backfill cancellation support;
cancel active child/window runs individually for now.

### Clean local state

```bash
mix favn.reset
```

### Build artifacts

```bash
mix favn.build.runner
mix favn.build.manifest \
  --runner-release .favn/dist/runner/rr_.../runner-release.json
FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.publish \
  --manifest .favn/dist/manifest/mv_.../manifest-index.json \
  --orchestrator-url http://control-plane.internal:4101
FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.activate \
  --manifest-version mv_... --workspace-id production \
  --orchestrator-url http://control-plane.internal:4101
```

Runner and manifest builds automatically re-run in `MIX_ENV=prod`. The Favn
path checkout must be clean and checked out at a detached tag or commit; a
floating branch is rejected because it cannot identify the packaged Favn
source. The generated context vendors the customer OTP application, its `priv`
files, and its exact runtime dependency closure.

Declare modules or applications reached only through dynamic dispatch:

```elixir
config :favn,
  runner_build: [
    extra_modules: [MyApp.DynamicHelper],
    extra_applications: [:my_runtime_app]
  ]
```

Changing either list requires a new runner release. SQL, pipeline, and schedule
modules are not executable roots unless they are also Elixir assets or are
reached from another runtime root.

The production compile preserves the exact `Application.compile_env/3` values
recorded by selected runtime applications and imports the customer
`config/runtime.exs` in the final release. It does not import arbitrary
development or manifest-only config into the runner. Unselected customer BEAMs
are removed from the release, so SQL-only and authoring-only modules cannot
escape the runner fingerprint.

## Configuration contract

`favn_local` reads local tooling config from `config :favn, :local`.

`config :favn, :dev` is still read and merged for backward compatibility, but
`:local` is the source-of-truth namespace for new config.

Example:

```elixir
config :favn, :local,
  postgres: [
    url: "ecto://favn_runtime:favn_runtime_local@127.0.0.1:5432/favn_dev",
    ssl: false,
    pool_size: 10
  ],
  workspace_id: "local-dev",
  scheduler: false,
  orchestrator_port: 4101,
  web_port: 4173
```

PostgreSQL is the only supported control-plane backend. Run
`scripts/postgres/setup` from the Favn repository first; it migrates the schema,
grants the runtime role, and provisions the configured local workspace.

### Consumer dependency boundary

Consumer projects configure the local PostgreSQL connection through `config
:favn, :local` or `FAVN_DATABASE_URL`.

Do not add `:favn_storage_postgres`, `:favn_orchestrator`, `:favn_runner`, or
`:favn_local` directly to a normal consumer `mix.exs` for local development. Those
apps are Favn-owned runtime/package components materialized under `.favn/` by
`mix favn.install` and used by `mix favn.dev`.

Scheduler selection:

- default: disabled
- `scheduler: true` in `config :favn, :local` enables local schedules
- `mix favn.dev --scheduler` enables local schedules and overrides config
- `mix favn.dev --no-scheduler` disables local schedules and overrides config

Numeric local config values, including local ports and Postgres port/pool size,
must be positive integers or strings containing only a positive integer after
trimming whitespace. Malformed strings fall back to the documented defaults.

Local env files:

- `mix favn.dev`, `mix favn.reload`, `mix favn.inspect`, and `mix favn.query`
  load `<project-root>/.env`, then use a
  fresh Mix process to evaluate `config/runtime.exs` before compiling the
  project, building manifests, or launching/restarting services
- each command reads its env file once; a later `mix favn.reload` starts a new
  bootstrap and therefore sees current env-file and runtime-config values
- `FAVN_ENV_FILE` can point at an alternate env file; relative paths are resolved
  from the project root
- existing shell environment variables win over values in the env file
- Favn-owned service values such as local ports, runner node names, and service
  tokens override env-file values when launching managed services
- raw env values are not written into `.favn/` runtime metadata
- env values are inherited by the configured process and are not placed in
  command-line arguments

## `.favn/` layout

`favn_local` keeps all managed project-local side effects under `.favn/`:

- `install/` install metadata, toolchain capture, runtime input snapshots
- `build/` per-target build working directories
- `dist/` per-target final artifact outputs
- `logs/` local service logs
- `data/` local DuckDB files and other project data-plane state
- `manifests/` latest and cached manifest metadata
- `history/` failure metadata
- `secrets.json` generated project-local service, session, and Distributed
  Erlang credentials; owner-readable/writable only on POSIX filesystems
- `runtime.json` live stack state

`.favn/` is local runtime state and must stay uncommitted. Favn persists only
generated local credentials in `secrets.json`; configured password/token/secret
overrides are used at runtime and are not copied into Favn state files.

## How core flows work

### Install

`mix favn.install`:

- validates Elixir runtime metadata
- computes and stores an install fingerprint, using filtered Git tree metadata
  for clean Git sources, hashing only changed files for dirty Git worktrees,
  and falling back to full deterministic content hashing outside Git, while
  excluding generated dependency/build/doc/temp directories and rejecting
  source symlinks so materialization cannot escape the runtime root
- captures toolchain metadata
- resolves a runnable Favn runtime workspace under `.favn/install/runtime_root`
- records runtime source/materialization metadata in `.favn/install/runtime.json`
- installs runtime-root Mix deps for orchestrator/runner/storage apps
- installs the Phoenix esbuild and Tailwind binaries used by local endpoint
  watchers unless `--skip-web-install` is passed

`mix favn.dev` and build tasks validate install freshness before running. When
the source-tree hash changes, rerunning `mix favn.install` refreshes the
materialized runtime; `mix favn.install --force` remains available for an
unconditional rebuild. Failed reinstalls invalidate readiness so a partially
prepared workspace cannot be mistaken for the previous working install.

### Local dev startup

`mix favn.dev` starts a runner process plus one operator process that runs both
orchestrator and the Phoenix/LiveView `favn_view` app. It writes runtime state to
`.favn/runtime.json`. The UI is available at `http://127.0.0.1:4173` by default.

The local scheduler is disabled by default so active pipeline schedules do not
surprise one-time local ETL work. Manual `mix favn.run PipelineModule` is the
recommended safe default. Scheduled tutorial or smoke flows should opt in with
`mix favn.dev --scheduler`.

Runtime and consumer compilation is incremental. Unchanged starts reuse Mix
compiler manifests; they do not force a full rebuild.

The orchestrator process starts with local-dev mode enabled. Local CLI calls use
an explicit trusted local-dev context that is accepted only while the API is
bound to `127.*`; they do not perform password login and do not create local
operator users. Production/server auth is a separate runtime configuration path.

`mix favn.run TARGET` submits an asset or pipeline to the running local stack over
the private orchestrator HTTP boundary using the loopback-only local-dev context.
It resolves the active manifest target ID, submits the run, and waits
for terminal status by default.
Windowed pipelines accept explicit local run windows with `--window
hour:YYYY-MM-DDTHH`, `--window day:YYYY-MM-DD`, `--window month:YYYY-MM`, or
`--window year:YYYY`, plus optional `--timezone`.
Asset targets accept `--dependencies all|none` and `--refresh auto|missing|force_selected|force_selected_upstream|force_all`.
Pipeline targets reject `--dependencies` and accept only `auto`, `missing`, and
`force_all` refresh. Omitted options preserve orchestrator defaults of
dependency scope `all` and refresh `auto`. `force_selected_upstream` requires
dependency scope `all`.

Dependency scope determines which assets are planned; refresh determines how
freshness is applied within that plan. `--dependencies none --refresh
force_selected` is intended for targeted repair after the operator has confirmed
that upstream inputs are suitable. It must not be treated as the normal
scheduling or pipeline path.
The local tooling HTTP client is intentionally limited to plain HTTP loopback
URLs for Favn-managed local services.

Runner-side consumer config transport is local-dev-only. It carries only
`:discovery`, `:connection_modules`, `:connections`, `:execution_pools`,
`:runner_plugins`, `:duckdb_in_process_client`, and `:duckdb_adbc` from the
consumer project's `config :favn`. Relative
connection database paths are expanded against the consumer project root before
the runner starts. Tagged `Favn.RuntimeConfig.Ref` and `Favn.RuntimeValue.Ref`
values are preserved as inert refs, so Azure token refs resolve only inside the
runner. Secrets can be present for local use, but diagnostics redact connection
values, tokens, passwords, database URLs, and plugin config.

Before startup, `favn_local` incrementally compiles the installed runtime
workspace under `.favn/install/runtime_root`. Runner and operator services then
use those beams with `--no-compile` and without taking Mix's OS build lock; the
shared build tree is read-only while they run. Consumer-config validation also
uses the compiled transport module instead of compiling a generated decoder in
each service process.

`--root-dir` remains supported as a runtime-source override for install
resolution. Local dev and reload then launch from the installed runtime
workspace.

Readiness is checked through the configured loopback HTTP health endpoints.

Phoenix asset watchers are managed by the `favn_view` endpoint during local dev.

### Reload

`mix favn.reload`:

- requires a healthy running stack
- recompiles and rebuilds/pins manifest
- restarts runner and re-registers manifest
- publishes/activates manifest in orchestrator
- updates runtime active manifest metadata

Runner restart uses a short node name for `--sname`, derived from stored
runtime full node name.

Manifest registration probes the live runner node for a compatible
`register_manifest` entrypoint before RPC dispatch so local-dev startup fails
with an explicit contract error instead of raw `:undef` when the runtime root
is out of sync.

### Operator inspection, logs, and reset

- `mix favn.logs` reads files under `.favn/logs/` (supports service filter,
  tail, and follow)
- `mix favn.logs RUN_ID` reads persisted run events from the local orchestrator
- `mix favn.runs list` and `mix favn.runs show RUN_ID` inspect persisted run
  summaries/details through the local orchestrator API
- `mix favn.inspect relation RELATION` and `mix favn.inspect partitions RELATION`
  inspect configured SQL relations; pass `--connection NAME` when multiple Favn
  SQL connections are configured
- `mix favn.query "select ..."` uses a best-effort read-only guardrail by
  default; this is not a SQL sandbox or security boundary. Pass `--allow-write`
  only for deliberate local mutation. The SQL inspection commands load `.env`
  before `config/runtime.exs`, then start only the SQL runtime before connecting,
  including `Favn.SQL.SessionPool`
- `mix favn.reset` removes `.favn/` after verifying no managed services are
  still running

### Packaging targets

- `build.runner`: immutable
  `.favn/dist/runner/<runner_release_id>/` OCI context containing the verified
  descriptor, aligned manifest release, vendored release inputs, digest-pinned
  Dockerfile, bundle hashes, and operator notes. The task neither invokes
  Docker nor pushes an image.
- `build.manifest --runner-release PATH`: recomputes the executable fingerprint
  and writes an immutable manifest release only when it exactly matches the
  supplied descriptor. Code/dependency/plugin/toolchain drift fails with
  `runner_rebuild_required` and bounded changed categories.
- `publish`: uploads missing execution packages and stages the exact manifest.
- `activate`: activates an exact staged manifest for an explicit workspace after
  the control plane verifies the connected runner release.

`publish` and `activate` accept the private URL through
`--orchestrator-url` or `FAVN_ORCHESTRATOR_URL`. Their service credential is
read only from `FAVN_ORCHESTRATOR_SERVICE_TOKEN`; it is not a CLI option or an
artifact field.

The older packaging commands below remain temporarily available while the
Docker-first local lifecycle is replaced:

- `build.web`: Phoenix/LiveView UI artifact metadata and bundle contract
- `build.orchestrator`: orchestrator artifact metadata and storage contract
- `build.single`: assembles a project-local backend-only PostgreSQL launcher with
  generated `config/assembly.json`, `env/backend.env.example`, and executable
  `bin/start|stop` scripts. It runs runner, PostgreSQL persistence, and
  orchestrator in one backend BEAM runtime. The launcher is operational and
  start/stop tested, but remains project-local and non-relocatable because it
  depends on the installed runtime source root. It does not provide a supported web
  startup path; the current UI requires an in-BEAM orchestrator facade.

## Platform assumptions

`favn_local` supports Unix and Windows process checks/termination paths.

No separate Node/SvelteKit frontend process is required for local dev.
