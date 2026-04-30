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
- local development tooling is available today through `mix favn.init`, `mix favn.doctor`, `mix favn.install`, `mix favn.dev`, `mix favn.run`, `mix favn.backfill`, `mix favn.reload`, `mix favn.status`, and `mix favn.stop`
- local development startup uses HTTP-level orchestrator readiness checks and structured local API failure diagnostics
- the local web UI now includes a run inspector at `/runs`, an asset catalog at `/assets`, and an operational backfill area at `/backfills` for submitting explicit active-manifest pipeline backfills, inspecting parent windows, rerunning failed windows, and browsing coverage-baseline and asset/window state projections
- local development registers one pinned manifest version across runner and orchestrator so scheduled runs execute against the same manifest identity
- local documentation lookup is available through `mix favn.read_doc ModuleName` and `mix favn.read_doc ModuleName function_name`
- initial packaging tooling now includes `mix favn.build.runner` for project-local runner artifact output under `.favn/dist/runner/<build_id>/`
- split-target packaging now also includes `mix favn.build.web` and `mix favn.build.orchestrator` with honest metadata-oriented outputs under `.favn/dist/web/<build_id>/` and `.favn/dist/orchestrator/<build_id>/`
- single-node assembly packaging now includes `mix favn.build.single` with topology-preserving assembly output under `.favn/dist/single/<build_id>/` (see `OPERATOR_NOTES.md` in each artifact)
- operational backfill foundations are implemented in the control plane for resolving ranges, submitting parent/child pipeline backfills, tracking per-window state, exposing private orchestrator HTTP reads/commands, and driving those endpoints from `mix favn.backfill` in local dev; operational backfill does not accept lookback-policy input until concrete runtime semantics exist

## What Favn Gives You

- Elixir DSLs for single assets, multi-assets, SQL assets, namespaces, schedules, pipeline window policies, and pipelines
- explicit dependency modeling between assets
- compile-time manifest generation for authored business code
- deterministic planning from selected targets and dependency rules
- pipeline definitions for scheduled or operator-triggered runs
- local runtime workflow support for authoring, safe landed-data inspection, and orchestration
- SQL-aware asset authoring with reusable SQL definitions and relation references
- public SQL client access for named Favn connections via `Favn.SQLClient`
- DuckDB connection bootstrap for DuckLake sessions, including extension install/load, Azure credential-chain secrets, DuckLake attach, and catalog selection

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

defmodule MyApp.Warehouse.Raw.Orders do
  @moduledoc """
  Raw commerce orders as received from the source platform.

  One row represents one source order. Cancelled orders are retained so
  downstream models can decide how to treat them.
  """

  use Favn.Asset

  @doc "Fetch, normalize, and write raw orders."
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  source_config :source_system,
    segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
    token: secret_env!("SOURCE_SYSTEM_TOKEN")
  @relation true
  def asset(ctx) do
    _segment_id = ctx.config.source_system.segment_id
    :ok
  end
end
```

Namespace defaults are inherited from parent modules. The recommended default is
`warehouse.ex` for the connection namespace, `warehouse/raw.ex` or
`warehouse/mart.ex` for layer catalogs, and leaf files for assets. `@relation
true` is the normal leaf-module path, while `@relation [name: "..."]` overrides
only the relation name. SQL asset namespace inheritance is finalized during
explicit asset/manifest compilation, so parent namespace modules do not need to
compile before child SQL asset modules in the same parallel compiler batch.
Use your own layer names, such as bronze/silver/gold or raw/intermediate/mart,
and use schemas instead of catalogs if that is your platform convention.
Keep asset-specific logic near the asset; move code to `integrations/` or `sql/`
only when it is transport-specific or genuinely reusable.

Assets can declare required runtime configuration with `source_config/2`,
`env!/1`, and `secret_env!/1`. Manifests record the required environment keys
and secret flags, but never embed resolved runtime values. The runner resolves
the values before asset execution and exposes them through `ctx.config`, failing
the run early with diagnostics such as `missing_env SOURCE_SYSTEM_TOKEN` when a
required value is absent.

For source-system raw landing assets, keep the source client outside the asset,
read source IDs/tokens through `ctx.config`, write raw rows through
`Favn.SQLClient`, and return structured metadata such as row counts, mode,
relation, load timestamp, and hashed source identity. The standalone tutorial in
`examples/basic-workflow-tutorial` shows this pattern with a full-refresh raw
orders asset followed by SQL transformations.

### 3. Define a downstream SQL asset

```elixir
defmodule MyApp.Warehouse.Mart do
  use Favn.Namespace, relation: [catalog: "mart"]
end

defmodule MyApp.Warehouse.Mart.OrderSummary do
  @moduledoc """
  Sales order mart used by business reporting.

  One row represents one reportable order after raw source fields have been
  normalized for analytics.
  """

  use Favn.SQLAsset

  @meta owner: "analytics", category: :sales, tags: [:mart]
  @depends MyApp.Warehouse.Raw.Orders
  @materialized :view

  query do
    ~SQL"""
    select *
    from raw.orders
    """
  end
end
```

Relation-style SQL references are the primary authoring path. Two-part SQL names
are read as `schema.table`; three-part names are read as
`catalog.schema.table`. When a reference resolves to an owned asset relation on
the same connection, Favn infers the dependency automatically. Use `@depends`
when the dependency is not visible in SQL, cannot be resolved from owned
relations, or the project intentionally uses catalog-only layer names.
DuckDB-backed SQL materialization creates the owned target schema when needed
before creating the table or view.
For longer queries, place the SQL file next to the SQL asset module, for example
`warehouse/mart/order_summary.ex` plus `warehouse/mart/order_summary.sql`, and
use `query file: "order_summary.sql"`.

### 4. Define a pipeline

```elixir
defmodule MyApp.Pipelines.DailySales do
  use Favn.Pipeline

  pipeline :daily_sales do
    asset MyApp.Warehouse.Mart.OrderSummary
    deps :all
    schedule cron: "0 2 * * *", timezone: "Etc/UTC"
    window :daily
  end
end
```

Windowed pipelines support `:hourly`, `:daily`, `:monthly`, and `:yearly`
policies. Manual local runs pass the concrete requested window, while scheduled
windowed runs resolve the previous complete period in the schedule timezone.
Pipelines without a `window` clause run without an anchor window.

Operational backfill foundations can resolve explicit or relative ranges into
the same concrete window anchors and submit one child pipeline run per resolved
window through the orchestrator. Local development can submit explicit
operational backfills and inspect their control-plane state with
`mix favn.backfill` or the web operator flow at `/backfills`.

### 5. Configure authored modules

```elixir
import Config

config :favn,
  asset_modules: [
    MyApp.Warehouse.Raw.Orders,
    MyApp.Warehouse.Mart.OrderSummary
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

Connection runtime values can also use `Favn.RuntimeConfig.Ref.env!/1` and
`Favn.RuntimeConfig.Ref.secret_env!/1` when values should be resolved from the
runner environment just before adapter connection.

DuckDB local-file connections are serialized by default inside the SQL runtime so
local sessions and materializations do not run unsafe concurrent catalog writes
against the same database file. Backends that support parallel writes can opt out
with `write_concurrency: :unlimited` in their runtime connection config, while
local DuckDB files can keep the default or set `write_concurrency: 1` explicitly.

DuckDB connections can also declare `duckdb_bootstrap` runtime config when a
session needs setup before SQLClient or SQL asset execution. This is the
recommended path for local DuckLake dogfooding with Azure Data Lake Storage and a
PostgreSQL metadata catalog. Add `Favn.SQL.Adapter.DuckDB.bootstrap_schema_field/0`
to the connection module schema, then configure extension install/load, Azure
credential-chain secret creation, DuckLake attach, and `USE` under the named
connection. Secret runtime refs are resolved on the runner side and redacted from
diagnostics.

## Local Development

Favn includes a local developer loop for running the current project with the Favn runtime stack.

```bash
mix favn.init --duckdb --sample
mix deps.get
mix favn.doctor
mix favn.install
mix favn.dev
mix favn.run MyApp.Pipelines.DailySales --window day:2026-04-27 --timezone Europe/Oslo
mix favn.backfill submit MyApp.Pipelines.DailySales --from 2026-04-01 --to 2026-04-07 --kind day
mix favn.backfill windows RUN_ID --limit 100 --offset 0
mix favn.logs
mix favn.status
mix favn.reload
mix favn.stop
mix favn.reset
mix favn.read_doc Favn
mix favn.read_doc Favn generate_manifest
```

This is now the stable public entrypoint for local iteration on the refactored architecture.

For a fresh standalone Mix consumer project with local path dependencies back to
this checkout, `mix favn.init --duckdb --sample` generates the first dogfooding
path: a DuckDB connection module, raw and gold namespace modules, one Elixir raw
load asset, one SQL business output asset, a `deps(:all)` pipeline, local Favn
config, and `.env.example` web-login credentials. The task also adds a local
`favn_duckdb` path dependency when it can recognize the project's `defp deps/0`
shape. Rerun `mix deps.get`, then use `mix favn.doctor` before starting the
stack.

The generated pipeline is `MyApp.Pipelines.LocalSmoke` for a project whose OTP
app is `:my_app`:

```bash
mix favn.install
mix favn.dev
mix favn.run MyApp.Pipelines.LocalSmoke --wait
```

After the run, inspect `/runs` and `/assets` in the local web UI. Asset detail
pages show latest materialization metadata when available and can load a safe
read-only preview for manifest-owned SQL relations: columns, row count, and up to
20 sample rows. The generated DuckDB output can also be queried from the
consumer project:

```elixir
{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select * from gold.order_summary")
:ok = Favn.SQLClient.disconnect(session)
```

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
project. Its raw orders asset is the canonical source-system landing example:
it resolves a source segment and token through `ctx.config`, calls a small source
client, lands JSON rows into DuckDB through `Favn.SQLClient`, and returns
structured run metadata for inspection.

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
`.favn/secrets.json`. Missing local secrets are generated on demand, but corrupt
or unreadable secrets files fail startup instead of being silently replaced.

Local numeric config values, including dev ports and Postgres port/pool size,
are accepted only as positive integers or strings that contain exactly a
positive integer after trimming whitespace. Malformed strings use the documented
local defaults.

`mix favn.run PipelineModule` submits a manifest-scoped pipeline run to the
currently running local stack. It uses the project-local service token and local
operator credentials generated under `.favn/secrets.json`, so tutorial and local
smoke runs do not require hand-written private orchestrator API requests. This
manual run path is the recommended default for one-time local ETL.

`mix favn.backfill` exposes the local operational-backfill workflow for running
local stacks. Use `submit` for explicit `--from`/`--to`/`--kind` pipeline ranges,
`windows RUN_ID` to inspect child windows, `coverage-baselines` and
`asset-window-states` to inspect projected backfill state, and `rerun-window
RUN_ID --window-key KEY` for failed window reruns. Operational backfill submit
does not accept lookback-policy input; asset window lookback remains part of
normal windowed execution only.

The local web UI exposes the same operator workflow through `/backfills`,
including active-manifest pipeline selection, explicit range submission,
optional coverage-baseline selection, parent backfill detail pages with child
window rows, one-window failed reruns, `/backfills/coverage-baselines`, and
`/assets/window-states`.

Backfill read commands are bounded. They default to `--limit 100 --offset 0`, reject
`--limit` values above `500`, and print a next-page hint when more rows are available.

For `submit`, `--wait-timeout-ms` controls local CLI polling only, while
`--run-timeout-ms` controls the child run execution timeout sent to the
orchestrator.

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
- `docs/structure/` maps current ownership, code layout, and test layout by app
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
mix favn.read_doc Favn.Backfill.RangeRequest
mix favn.read_doc Favn.Dev
```

Suggested section for a consumer project's `AGENTS.md`:

```md
## Favn

Favn is used to define business-oriented assets and pipelines in Elixir,
compile them into a manifest, and run or inspect them locally.

Before guessing about Favn APIs, read `mix favn.read_doc Favn.AI` and follow
the module pointers there. Prefer the recommended consumer shape unless this
project documents a stronger local convention.
```

## Current Direction

The current release work is focused on product hardening and operator/developer
experience on top of the now-closed manifest-first architecture.

That architecture work did not change the core user-facing goal: define assets
and pipelines in Elixir, compile them into an explicit graph, and run them with
predictable behavior.
