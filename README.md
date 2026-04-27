<p align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="220" />
</p>

<h1 align="center">Favn</h1>

<p align="center">
  Define business-oriented data assets in Elixir, model how they depend on each other, and turn them into predictable runs.
</p>

<p align="center">
  <a href="#status"><strong>Status</strong></a>
  ·
  <a href="#what-favn-gives-you"><strong>Features</strong></a>
  ·
  <a href="#core-concepts"><strong>Concepts</strong></a>
  ·
  <a href="#quickstart"><strong>Quickstart</strong></a>
  ·
  <a href="#local-development"><strong>Local Dev</strong></a>
  ·
  <a href="#common-public-api-entry-points"><strong>API</strong></a>
  ·
  <a href="#documentation"><strong>Docs</strong></a>
</p>

Favn is an Elixir library for defining business-oriented data assets, understanding how they depend on each other, and turning those definitions into predictable runs.

It is aimed at teams who want data and workflow logic to live in normal Elixir modules instead of being spread across ad hoc scripts, scheduler config, and undocumented conventions.

## Status

Favn is in private development.

The `v0.5.0` architecture refactor is now closed enough to resume normal feature
development on top of the new boundaries.

- breaking changes are still allowed before `v1.0`
- `{:favn, ...}` remains the one public package users should depend on
- local development tooling is available today through `mix favn.install`, `mix favn.dev`, `mix favn.run`, `mix favn.reload`, `mix favn.status`, and `mix favn.stop`
- local development startup uses HTTP-level orchestrator readiness checks and structured local API failure diagnostics
- the local web UI now lands on a run inspector at `/runs` for understanding recent pipeline/asset executions, failures, outputs, and manifest context after `mix favn.run`
- local development registers one pinned manifest version across runner and orchestrator so scheduled runs execute against the same manifest identity
- local documentation lookup is available through `mix favn.read_doc ModuleName` and `mix favn.read_doc ModuleName function_name`
- initial packaging tooling now includes `mix favn.build.runner` for project-local runner artifact output under `.favn/dist/runner/<build_id>/`
- split-target packaging now also includes `mix favn.build.web` and `mix favn.build.orchestrator` with honest metadata-oriented outputs under `.favn/dist/web/<build_id>/` and `.favn/dist/orchestrator/<build_id>/`
- single-node assembly packaging now includes `mix favn.build.single` with topology-preserving assembly output under `.favn/dist/single/<build_id>/` (see `OPERATOR_NOTES.md` in each artifact)

## What Favn Gives You

- Elixir DSLs for single assets, multi-assets, SQL assets, namespaces, schedules, and pipelines
- explicit dependency modeling between assets
- compile-time manifest generation for authored business code
- deterministic planning from selected targets and dependency rules
- pipeline definitions for scheduled or operator-triggered runs
- local runtime workflow support for authoring, inspection, and orchestration
- SQL-aware asset authoring with reusable SQL definitions and relation references
- public SQL client access for named Favn connections via `Favn.SQLClient`

## Core Concepts

### Assets

Assets are the units of business work in Favn. An asset can be written as normal Elixir (`use Favn.Asset`) or as a SQL-backed asset (`use Favn.SQLAsset`).

### Pipelines

Pipelines select one or more target assets and describe how they should run, for example whether dependencies should be included and whether the pipeline should have a schedule.
Cron schedules support both standard 5-field expressions and 6-field expressions with a leading seconds field, so production schedules can be defined down to seconds when needed.
When `missed: :all` is used, scheduler catch-up is capped per schedule entry per tick to avoid unbounded backlog submission for high-frequency schedules. The orchestrator default cap is 1,000 occurrences per schedule entry per tick and can be adjusted with `config :favn_orchestrator, :scheduler, max_missed_all_occurrences: ...`.

### Manifests

Favn compiles your authored modules into a manifest so planning and runtime behavior can operate from a stable, explicit description of the graph.

## Quickstart

### 1. Add the dependency

Favn is not published on Hex yet. For private local development with the Favn
monorepo checked out next to your project, use path dependencies:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"}
  ]
end
```

If your project executes DuckDB-backed SQL assets directly, add the DuckDB
plugin from the same local checkout:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb, path: "../favn/apps/favn_duckdb"}
  ]
end
```

Storage adapter apps such as `favn_storage_sqlite` are runtime/control-plane
adapters, not required for ordinary consumer authoring or DuckDB execution.

Multiple `git` dependencies with different `subdir` values from this monorepo
are not the supported plugin-consumption model before Hex packaging because Mix
checks them out as separate dependency projects. For real external consumption,
Favn packages will be published as normal package dependencies.

### 2. Define an asset

```elixir
defmodule MyApp.Warehouse do
  use Favn.Namespace, relation: [connection: :warehouse]
end

defmodule MyApp.Warehouse.Raw do
  use Favn.Namespace, relation: [catalog: "raw"]
end

defmodule MyApp.Warehouse.Raw.Sales do
  use Favn.Namespace, relation: [schema: "sales"]
end

defmodule MyApp.Warehouse.Raw.Sales.Orders do
  use Favn.Asset

  @doc "Load raw orders"
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  @relation true
  def asset(_ctx) do
    :ok
  end
end
```

Namespace defaults are inherited from parent modules. Child asset modules only
need `use Favn.Namespace` when they are adding or overriding shared relation
defaults. `@relation true` is the normal leaf-module path, while
`@relation [name: "..."]` is the normal way to override only the relation
name. SQL asset namespace inheritance is finalized during explicit
asset/manifest compilation, so parent namespace modules do not need to compile
before child SQL asset modules in the same parallel compiler batch.

### 3. Define a downstream SQL asset

```elixir
defmodule MyApp.Warehouse.Gold do
  use Favn.Namespace, relation: [catalog: "gold"]
end

defmodule MyApp.Warehouse.Gold.Sales do
  use Favn.Namespace, relation: [schema: "sales"]
end

defmodule MyApp.Warehouse.Gold.Sales.OrderSummary do
  use Favn.SQLAsset

  @meta owner: "analytics", category: :sales, tags: [:gold]
  @materialized :view

  query do
    ~SQL"""
    select *
    from raw.sales.orders
    """
  end
end
```

Relation-style SQL references are the primary authoring path. When a reference
resolves to an owned asset relation on the same connection, Favn infers the
dependency automatically, so `@depends` is only needed for dependencies that
are not visible in SQL or cannot be resolved from owned relations.

### 4. Define a pipeline

```elixir
defmodule MyApp.Pipelines.DailySales do
  use Favn.Pipeline

  pipeline :daily_sales do
    asset MyApp.Warehouse.Gold.Sales.OrderSummary
    deps :all
    schedule cron: "0 2 * * *", timezone: "Etc/UTC"
  end
end
```

### 5. Configure authored modules

```elixir
import Config

config :favn,
  asset_modules: [
    MyApp.Warehouse.Raw.Sales.Orders,
    MyApp.Warehouse.Gold.Sales.OrderSummary
  ],
  pipeline_modules: [
    MyApp.Pipelines.DailySales
  ]
```

### 6. Inspect and compile from IEx

```elixir
{:ok, assets} = Favn.list_assets()
{:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
{:ok, manifest} = Favn.generate_manifest()
```

### 7. Query a configured SQL connection from Elixir

```elixir
{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select 1")
:ok = Favn.SQLClient.disconnect(session)
```

For this flow, configure `:connection_modules` and runtime `:connections` under
`config :favn` using the `Favn.Connection` contract.

DuckDB local-file connections are serialized by default inside the SQL runtime so
local sessions and materializations do not run unsafe concurrent catalog writes
against the same database file. Backends that support parallel writes can opt out
with `write_concurrency: :unlimited` in their runtime connection config, while
local DuckDB files can keep the default or set `write_concurrency: 1` explicitly.

## Local Development

Favn includes a local developer loop for running the current project with the Favn runtime stack.

```bash
mix favn.install
mix favn.dev
mix favn.run MyApp.Pipelines.DailySales
mix favn.logs
mix favn.status
mix favn.reload
mix favn.stop
mix favn.reset
mix favn.read_doc Favn
mix favn.read_doc Favn generate_manifest
```

This is now the stable public entrypoint for local iteration on the refactored architecture.

`mix favn.dev` starts with the local scheduler disabled by default. This keeps
one-time local ETL and DuckDB-backed dogfooding on the manual path unless you
explicitly opt into active schedules. Use `mix favn.run PipelineModule` for the
normal local execution loop, and use `mix favn.dev --scheduler` only when you
want scheduled pipelines to run locally. `mix favn.dev --no-scheduler` is
available when you want to make the disabled setting explicit or override local
config.

For a consumer-style authoring and DuckDB execution tutorial, see
`examples/basic-workflow-tutorial`. It lives outside the umbrella apps, uses
local path dependencies back to `apps/favn` and `apps/favn_duckdb`, and has its
own compile/test workflow. The embedded tutorial also exercises the
`mix favn.install` and `mix favn.dev` local-tooling loop from a consumer-style
project.

`mix favn.install` resolves and materializes a runtime workspace under
`.favn/install/runtime_root`. The install fingerprint includes a deterministic
hash of the copied runtime source tree, excluding generated dependency/build
directories, so source-only Favn updates refresh the installed runtime. `mix
favn.dev` validates that fingerprint and compiles the installed runtime
workspace before startup so live runner/orchestrator processes do not boot stale
internal runtime beams. `--root-dir` remains an install/runtime-source override
for split-root workflows.

Local tooling HTTP calls are plain HTTP loopback calls to Favn-managed local
services. They intentionally do not support remote or HTTPS URLs in the local
developer loop.

The local orchestrator bootstrap actor can be configured from the consumer
project `.env` used to run `mix favn.dev`. Configure
`FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME` and
`FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD` before startup, and set
`FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES=admin,operator` when you want that local
actor to have admin privileges. Then use those orchestrator-owned credentials
on the web login page. If bootstrap credentials are not configured, local
tooling keeps using the generated local operator credentials stored under
`.favn/secrets.json`.

`mix favn.run PipelineModule` submits a manifest-scoped pipeline run to the
currently running local stack. It uses the project-local service token and local
operator credentials generated under `.favn/secrets.json`, so tutorial and local
smoke runs do not require hand-written private orchestrator API requests. This
manual run path is the recommended default for one-time local ETL.

The local runner receives only the explicitly supported consumer `:favn` config
needed for local execution: `:connection_modules`, `:connections`,
`:runner_plugins`, and `:duckdb_in_process_client`. This transport is local-dev
plumbing only; it uses a tagged payload rather than arbitrary app-env forwarding,
normalizes relative connection database paths from the consumer project root,
and redacts connection/plugin secrets from diagnostics.

If you are upgrading from earlier pre-closeout local SQL storage state, run
`mix favn.reset` once so local persisted payloads are recreated in the current
canonical format.

Pre-closeout SQL payload compatibility is intentionally not supported. Existing
SQLite/Postgres rows that were persisted as BEAM term blobs must be reset or
recreated before running with the closeout adapters.

### favn_local configuration

`favn_local` reads local tooling config from `config :favn, :local` (with
`config :favn, :dev` still supported for backward compatibility).

```elixir
config :favn, :local,
  storage: :memory,
  scheduler: false,
  sqlite_path: ".favn/data/orchestrator.sqlite3",
  postgres: [
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "favn",
    ssl: false,
    pool_size: 10
  ]
```

Storage modes:

- `mix favn.dev` uses configured storage (default `:memory`)
- `mix favn.dev --sqlite` forces SQLite
- `mix favn.dev --postgres` forces Postgres
- `mix favn.dev --scheduler` enables local scheduled runs
- `mix favn.dev --no-scheduler` disables local scheduled runs and overrides config

`mix favn.build.single` defaults to SQLite and accepts
`--storage sqlite|postgres`.

### favn_local implementation notes

- public `mix favn.*` tasks are owned by `apps/favn`
- implementation is owned by `apps/favn_local`
- `mix favn.build.runner` is rooted in the current Mix project; `--root-dir`
  must match the current project root
- runner artifacts include the pinned manifest and compiled beams from the
  current project app so helper modules and connection modules are available at
  execution time
- install now materializes a runnable runtime workspace under
  `.favn/install/runtime_root`, records runtime metadata in
  `.favn/install/runtime.json`, and caches npm data under
  `.favn/install/cache/npm`

## Common Public Authoring API Entry Points

- `Favn.list_assets/0,1`
- `Favn.get_asset/1`
- `Favn.get_pipeline/1`
- `Favn.resolve_pipeline/2`
- `Favn.generate_manifest/0,1`
- `Favn.build_manifest/0,1`
- `Favn.plan_asset_run/2`

## Documentation

- `docs/FEATURES.md` tracks the implemented feature set today
- `docs/ROADMAP.md` tracks planned next work and later ideas
- `docs/REFACTOR.md` tracks the `v0.5` architecture and migration plan
- `docs/lib_structure.md` describes the current library layout
- `docs/test_structure.md` describes the current test layout
- `examples/basic-workflow-tutorial` is the first end-to-end tutorial project

## AI Doc Entry Point

Favn includes a compiled-doc reader intended for AI-assisted workflows.

Use Favn when you are defining assets and pipelines in Elixir, compiling them
into a manifest, planning runs, or using the local dev/runtime tooling.

Read `Favn.AI` for the task map and the next module to inspect.

```bash
mix favn.read_doc Favn.AI
```

Common follow-up reads:

```bash
mix favn.read_doc Favn generate_manifest
mix favn.read_doc Favn.Asset
mix favn.read_doc Favn.SQLAsset
mix favn.read_doc Favn.Pipeline
mix favn.read_doc Favn.Dev
```

Suggested section for a consumer project's `AGENTS.md`:

```md
## Favn

Favn is used to define business-oriented assets and pipelines in Elixir,
compile them into a manifest, and run or inspect them locally.

Before guessing about Favn APIs, read `mix favn.read_doc Favn.AI` and follow
the module pointers there.
```

## Current Direction

The current release work is focused on product hardening and operator/developer
experience on top of the now-closed manifest-first architecture.

That architecture work did not change the core user-facing goal: define assets
and pipelines in Elixir, compile them into an explicit graph, and run them with
predictable behavior.
