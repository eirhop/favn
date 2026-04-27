# favn_local

`favn_local` owns local developer tooling and project-local packaging workflows
for Favn.

`apps/favn` exposes the public `mix favn.*` tasks. Those tasks delegate to
`Favn.Dev` in this app.

## Scope and ownership

`favn_local` owns:

- local stack lifecycle implementation (`dev`, `stop`, `status`, `reload`)
- install/reset/log tooling (`install`, `reset`, `logs`)
- project-local packaging flows (`build.runner`, `build.web`,
  `build.orchestrator`, `build.single`)
- project-local filesystem state under `.favn/`

`favn_local` does not define public authoring APIs. Authoring and manifest
compile logic remains in `favn_authoring`.

## Public task surface

These tasks are exposed by `apps/favn` and implemented by `favn_local`:

- `mix favn.install`
- `mix favn.dev`
- `mix favn.status`
- `mix favn.run`
- `mix favn.logs`
- `mix favn.reload`
- `mix favn.stop`
- `mix favn.reset`
- `mix favn.build.runner`
- `mix favn.build.web`
- `mix favn.build.orchestrator`
- `mix favn.build.single`

## Typical usage

### First-time local setup

```bash
mix favn.install
mix favn.dev
mix favn.dev --scheduler
```

### Inspect and iterate

```bash
mix favn.status
mix favn.run MyApp.Pipelines.Daily
mix favn.logs --service runner --tail 200
mix favn.reload
mix favn.stop
```

### Clean local state

```bash
mix favn.reset
```

### Build artifacts

```bash
mix favn.build.runner
mix favn.build.web
mix favn.build.orchestrator
mix favn.build.single --storage sqlite
mix favn.build.single --storage postgres
```

## Configuration contract

`favn_local` reads local tooling config from `config :favn, :local`.

`config :favn, :dev` is still read and merged for backward compatibility, but
`:local` is the source-of-truth namespace for new config.

Example:

```elixir
config :favn, :local,
  storage: :memory,
  sqlite_path: ".favn/data/orchestrator.sqlite3",
  postgres: [
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "favn",
    ssl: false,
    pool_size: 10
  ],
  scheduler: false,
  orchestrator_port: 4101,
  web_port: 4173
```

Storage selection:

- default: `:memory`
- `mix favn.dev --sqlite` forces `:sqlite`
- `mix favn.dev --postgres` forces `:postgres`

Scheduler selection:

- default: disabled
- `scheduler: true` in `config :favn, :local` enables local schedules
- `mix favn.dev --scheduler` enables local schedules and overrides config
- `mix favn.dev --no-scheduler` disables local schedules and overrides config

## `.favn/` layout

`favn_local` keeps all managed project-local side effects under `.favn/`:

- `install/` install metadata, toolchain capture, runtime input snapshots
- `build/` per-target build working directories
- `dist/` per-target final artifact outputs
- `logs/` local service logs
- `data/` local sqlite data
- `manifests/` latest and cached manifest metadata
- `history/` failure metadata
- `runtime.json` live stack state
- `secrets.json` local generated secrets

## How core flows work

### Install

`mix favn.install`:

- validates tool prerequisites (node/npm by default)
- computes and stores an install fingerprint, including a deterministic hash of
  the copied runtime source tree while excluding generated dependency/build
  directories
- captures toolchain metadata
- resolves a runnable Favn runtime workspace under `.favn/install/runtime_root`
- records runtime source/materialization metadata in `.favn/install/runtime.json`
- installs runtime-root Mix deps for orchestrator/runner/storage apps
- stores npm cache under `.favn/install/cache/npm`

`mix favn.dev` and build tasks validate install freshness before running. When
the source-tree hash changes, rerunning `mix favn.install` refreshes the
materialized runtime; `mix favn.install --force` remains available for an
unconditional rebuild.

### Local dev startup

`mix favn.dev` starts runner, orchestrator, and web as separate local processes
and writes runtime state to `.favn/runtime.json`.

The local scheduler is disabled by default so active pipeline schedules do not
surprise one-time local ETL work. Manual `mix favn.run PipelineModule` is the
recommended safe default. Scheduled tutorial or smoke flows should opt in with
`mix favn.dev --scheduler`.

The orchestrator process receives bootstrap actor credentials from the consumer
project `.env` file when these keys are present:

```sh
FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME=admin
FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=admin-password
FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME=Favn Admin
FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES=admin,operator
```

Shell environment values for those keys override `.env` values. If they are not
provided, local tooling keeps using the generated local operator credentials
from `.favn/secrets.json`. These credentials are never forwarded to the web
process; the web login page always authenticates through orchestrator-owned
password auth.

`mix favn.run PipelineModule` submits a pipeline to the running local stack over
the private orchestrator HTTP boundary. It logs in with the generated local
operator credentials from `.favn/secrets.json`, resolves the active manifest's
pipeline target ID, submits the run, and waits for terminal status by default.
The local tooling HTTP client is intentionally limited to plain HTTP loopback
URLs for Favn-managed local services.

Runner-side consumer config transport is local-dev-only. It carries only
`:connection_modules`, `:connections`, `:runner_plugins`, and
`:duckdb_in_process_client` from the consumer project's `config :favn`. Relative
connection database paths are expanded against the consumer project root before
the runner starts. Secrets can be present for local use, but diagnostics redact
connection values, tokens, passwords, database URLs, and plugin config.

Before startup, `favn_local` force-compiles the installed runtime workspace
under `.favn/install/runtime_root` so orchestrator/runner startup does not boot
stale internal runtime beams.

`--root-dir` remains supported as a runtime-source override for install
resolution. Local dev and reload then launch from the installed runtime
workspace.

Readiness is checked by TCP connection to configured service URLs.

Web asset build is performed only when `web/favn_web/dist/index.html` is
missing.

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

### Logs and reset

- `mix favn.logs` reads files under `.favn/logs/` (supports service filter,
  tail, and follow)
- `mix favn.reset` removes `.favn/` after verifying no managed services are
  still running

### Packaging targets

- `build.runner`: project-specific runner artifact with user code + pinned
  manifest + plugin inventory metadata
- `build.web`: web/BFF artifact metadata and bundle contract
- `build.orchestrator`: orchestrator artifact metadata and storage contract
- `build.single`: assembles `web/`, `orchestrator/`, and `runner/` into one
  single-node bundle with generated `config/assembly.json`, `env/*.env`, and
  `bin/start|stop`

## Platform assumptions

`favn_local` supports Unix and Windows process checks/termination paths.

Node/npm are required for web install/build flows.
