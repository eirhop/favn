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

Current release work is focused on hardening the authoring, manifest, local
tooling, and single-node runtime support boundaries for a stable `v1`.

- breaking changes are still allowed before `v1.0`
- `{:favn, ...}` is the primary public dependency for consumer projects
- `{:favn_duckdb, ...}` is the supported DuckDB plugin backed by `duckdbex` for
  bundled local/in-memory DuckDB execution
- `{:favn_duckdb_adbc, ...}` is the supported ADBC-backed DuckDB plugin for
  deployments that need explicit DuckDB shared-library/driver control
- storage, orchestrator, runner, local tooling, and web apps are internal runtime
  or product components, not ordinary user dependencies
- local development tooling is available today through `mix favn.init`, `mix favn.doctor`, `mix favn.install`, `mix favn.dev`, `mix favn.run`, `mix favn.backfill`, `mix favn.runs`, `mix favn.logs`, `mix favn.inspect`, `mix favn.query`, `mix favn.diagnostics`, `mix favn.reload`, `mix favn.status`, and `mix favn.stop`
- local development startup uses HTTP-level orchestrator readiness checks, validated Distributed Erlang node/cookie inputs, explicit service wrapper pid/log-write failures, structured local API failure diagnostics, and normalized runner RPC dispatch failures at the orchestrator boundary
- the old SvelteKit frontend has been removed; local dev now starts a single operator process that runs orchestrator plus the thin Phoenix/LiveView `apps/favn_view` shell on the standard local web port, `http://127.0.0.1:4173`
- local development registers one pinned manifest version across runner and orchestrator so scheduled runs execute against the same manifest identity
- run snapshot, run event, and operational-backfill read-model persistence store explicit JSON-safe storage records, not BEAM terms or reconstructed exception structs; runner/backfill failures are normalized, bounded, and redacted before durable storage
- run-event storage treats exact duplicate writes as idempotent success and rejects duplicate sequences with different event content
- stage-parallel pipeline runs drain each submitted stage before failing later work, so independent sibling assets are reported even when one sibling fails; manifest-persisted freshness policies can skip already-fresh nodes, force selected nodes, block failed dependencies, and dirty downstream nodes only when an upstream actually refreshes
- run live updates expose documented global and run-scoped SSE streams with persisted cursors, Last-Event-ID replay, retry hints, and heartbeats from the orchestrator API
- production mutating orchestrator command APIs require `Idempotency-Key` for run submit/rerun/cancel, manifest activation, backfill submit, and backfill-window rerun; duplicate retries replay the original logical result, conflicting input returns `409`, and SQLite persists only key/request fingerprints plus operation-versioned JSON-safe replay response DTOs
- the first `v1` production target is documented as a single backend node with SQLite control-plane persistence on durable attached storage and runner-owned DuckDB data-plane execution; Phase 1 runtime config validation covers SQLite storage, orchestrator API/service auth, scheduler, local runner mode, and same-BEAM web readiness/public-origin/secret-key-base config, and the orchestrator exposes service-authenticated operator diagnostics for storage/schema readiness, active manifest, scheduler state evidence, runner availability, in-flight runs, and recent failed runs; the single-node operator runbook documents stopped-backend SQLite control-plane and separate DuckDB data-plane backup/restore procedures, while backup automation and fuller release packaging remain follow-up; see `docs/production/single_node_contract.md` and `docs/production/single_node_operator_runbook.md`
- local documentation lookup is available through `mix favn.read_doc ModuleName` and `mix favn.read_doc ModuleName function_name`
- initial packaging tooling now includes `mix favn.build.runner` for project-local runner artifact output under `.favn/dist/runner/<build_id>/`
- split-target packaging now also includes `mix favn.build.web` and `mix favn.build.orchestrator` with honest metadata-oriented outputs under `.favn/dist/web/<build_id>/` and `.favn/dist/orchestrator/<build_id>/`
- single-node packaging now includes `mix favn.build.single` with a verified project-local backend-only SQLite launcher under `.favn/dist/single/<build_id>/`; it still depends on the installed runtime source root and is not a self-contained or relocatable release artifact (see `OPERATOR_NOTES.md` in each artifact)
- backend bootstrap tooling now includes `mix favn.bootstrap.single`, which verifies an orchestrator service token through `/api/orchestrator/v1/bootstrap/service-token`, validates a manifest JSON file, registers and activates the manifest by default, asks the orchestrator to register that persisted manifest with the local runner, and reports service-auth active-manifest verification status; it does not make browser login/session/audit durable
- operational backfill foundations are implemented in the control plane for resolving ranges, dry-running plans, submitting parent/child pipeline backfills, tracking per-window state, exposing private orchestrator HTTP reads/commands, and driving those endpoints from `mix favn.backfill` in local dev; child pipeline backfills default to `refresh: :missing`, and operational backfill does not accept lookback-policy input until concrete runtime semantics exist
- operational backfill read-model storage is derived projection state; the closeout migration to JSON-safe DTO storage rebuilds the durable backfill read-model tables, and operators can repopulate them with `mix favn.backfill repair --apply` from authoritative run snapshots/events when needed

## What Favn Gives You

- Elixir DSLs for single assets, multi-assets, SQL assets, namespaces, schedules, asset freshness policies, pipeline window policies, execution concurrency policy, and pipelines
- explicit dependency modeling between assets
- compile-time manifest generation for authored business code
- deterministic planning from selected targets and dependency rules
- pipeline definitions for scheduled or operator-triggered runs
- local runtime workflow support for authoring, safe landed-data inspection, and orchestration
- SQL-aware asset authoring with reusable SQL definitions and relation references
- public SQL client access for named Favn connections via `Favn.SQLClient`
- DuckDB connection bootstrap for DuckDB catalog files and DuckLake sessions, including extension load, Azure credential-chain secrets, keyed DuckDB/DuckLake attaches, catalog selection, and catalog-level write admission
- an ADBC-backed DuckDB SQL adapter with bounded query results and explicit external-output expectations for large data

## Core Concepts

### Assets

Assets are the units of business work in Favn. An asset can be written as normal Elixir (`use Favn.Asset`) or as a SQL-backed asset (`use Favn.SQLAsset`).

### Pipelines

Pipelines select one or more target assets and describe how they should run, for example whether dependencies should be included and whether the pipeline should have a schedule.
Cron schedules support both standard 5-field expressions and 6-field expressions with a leading seconds field, so production schedules can be defined down to seconds when needed.
When `missed: :all` is used, scheduler catch-up is capped per schedule entry per tick to avoid unbounded backlog submission for high-frequency schedules. The orchestrator default cap is 1,000 occurrences per schedule entry per tick and can be adjusted with `config :favn_orchestrator, :scheduler, max_missed_all_occurrences: ...`.

Pipeline and asset execution concurrency is an orchestrator concern. Pipeline
`max_concurrency` limits how many asset steps from one run may execute at once,
while named `execution_pool` declarations protect shared resources across runs.
SQL/database `write_concurrency` remains separate and only protects writer or
backend admission after asset execution has started.

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

If your project executes DuckDB-backed SQL assets directly, add a DuckDB plugin
from the same local checkout. For bundled local/in-memory DuckDB execution, use
`favn_duckdb`:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb, path: "../favn/apps/favn_duckdb"}
  ]
end
```

For ADBC-backed execution with explicit DuckDB shared-library control, use
`favn_duckdb_adbc`:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb_adbc, path: "../favn/apps/favn_duckdb_adbc"}
  ]
end
```

`favn_duckdb_adbc` requires a DuckDB ADBC driver on the runtime machine. For
pinned production installs, configure the `libduckdb` path and entrypoint:

```elixir
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.2/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
```

See DuckDB's ADBC client documentation for driver installation details:
https://duckdb.org/docs/stable/clients/adbc.html

Do not add internal runtime apps as ordinary consumer dependencies. Storage
adapters such as `favn_storage_sqlite` and `favn_storage_postgres`, runtime apps
such as `favn_orchestrator`, `favn_runner`, and `favn_local`, and the web app are
owned by Favn's runtime/package tooling rather than by authored business code.
Local SQLite control-plane storage is selected with `config :favn, :local` or
`mix favn.dev --sqlite`, not by adding `:favn_storage_sqlite` to the consumer
project.

Multiple `git` dependencies with different `subdir` values from this monorepo
are not the supported plugin-consumption model before Hex packaging because Mix
checks them out as separate dependency projects. For real external consumption,
Favn packages will be published as normal package dependencies.
If a private git/subdir setup currently needs direct `override: true` entries for
internal apps to satisfy Mix, treat that as a temporary private-development
workaround only. It is not the intended consumer dependency model.

### 2. Define an asset

```elixir
defmodule MyDataPlatform.Lakehouse do
  use Favn.Namespace, relation: [connection: :important_lakehouse]
end

defmodule MyDataPlatform.Lakehouse.Raw do
  use Favn.Namespace, relation: [catalog: "raw"]
end

defmodule MyDataPlatform.Lakehouse.Raw.Sales do
  use Favn.Namespace, relation: [schema: "sales"]
end

defmodule MyDataPlatform.Lakehouse.Raw.Sales.Orders do
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
`connections/important_lakehouse.ex` for server/session/auth configuration,
`lakehouse.ex` for the connection namespace, `lakehouse/raw.ex` or
`lakehouse/mart.ex` for lakehouse phase catalogs, `lakehouse/raw/sales.ex` for a
segment/domain schema, and leaf files for assets. `@relation true` is the normal
leaf-module path, while `@relation [name: "..."]` overrides only the relation
name. SQL asset namespace inheritance is finalized during explicit asset/manifest
compilation, so parent namespace modules do not need to compile before child SQL
asset modules in the same parallel compiler batch. Use your own phase names,
such as bronze/silver/gold or raw/intermediate/mart, but keep the distinction:
connection is server/session/auth, catalog is the lakehouse phase, schema is the
segment/domain, and table/view is the asset name.
Keep asset-specific logic near the asset; move code to `integrations/` or `sql/`
only when it is transport-specific or genuinely reusable.

Assets and generated multi-assets can declare required runtime configuration with
`source_config/2`, `env!/1`, and `secret_env!/1`. Manifests record the required
environment keys and secret flags, but never embed resolved runtime values. The
runner resolves the values before asset execution and exposes them through
`ctx.config`, failing the run early with stable `:missing_runtime_config`
diagnostics such as `missing_env SOURCE_SYSTEM_TOKEN` when a required value is
absent.

Assets can also declare freshness with `@freshness`, for example `@freshness :daily`,
`@freshness {:daily, timezone: "Europe/Oslo"}`, `@freshness [max_age: {:hours, 6}]`,
or `@freshness :always`. Windowed assets default to exact window-success freshness.
During pipeline execution, the orchestrator records latest freshness state, skips
fresh nodes under the selected refresh policy, and keeps stale explanations as an
internal control-plane query surface.

Freshness policy input variants are documented in `Favn.Freshness.Policy`:

- `:daily` / `:day` for daily calendar freshness in `"Etc/UTC"`
- `{:daily, timezone: "Europe/Oslo"}` for daily calendar freshness in a timezone
- `[max_age: {:hours, 6}]` for rolling freshness, using `{unit, amount}`
- `[window_success: true]` for exact runtime-window success
- `:always` to always run when planned

Read `Favn.Freshness` as the breadcrumb module for freshness concepts and
`Favn.Freshness.Key` when you need the stable keys used by orchestrator state.

For source-system raw landing assets, keep the source client outside the asset,
read source IDs/tokens through `ctx.config`, write raw rows through
`Favn.SQLClient`, and return structured metadata such as row counts, mode,
relation, load timestamp, and hashed source identity. The standalone tutorial in
`examples/basic-workflow-tutorial` shows this pattern with a full-refresh raw
orders asset followed by SQL transformations.

### 3. Define a downstream SQL asset

```elixir
defmodule MyDataPlatform.Lakehouse.Mart do
  use Favn.Namespace, relation: [catalog: "mart"]
end

defmodule MyDataPlatform.Lakehouse.Mart.Sales do
  use Favn.Namespace, relation: [schema: "sales"]
end

defmodule MyDataPlatform.Lakehouse.Mart.Sales.OrderSummary do
  @moduledoc """
  Sales order mart used by business reporting.

  One row represents one reportable order after raw source fields have been
  normalized for analytics.
  """

  use Favn.SQLAsset

  @meta owner: "analytics", category: :sales, tags: [:mart]
  @depends MyDataPlatform.Lakehouse.Raw.Sales.Orders
  @materialized :view

  query do
    ~SQL"""
    select *
    from raw.sales.orders
    """
  end
end
```

Relation-style SQL references are the primary authoring path. Two-part SQL names
are read as `schema.table`; three-part names are read as
`catalog.schema.table`. Catalog-qualified SQL asset references and targets must
also include schema, so `raw.orders` is treated as `schema.table`, not
`catalog.table`; use `raw.sales.orders` for catalog/schema/table. When a
reference resolves to an owned asset relation on the same connection, Favn
infers the dependency automatically. Use `@depends` when the dependency is not
visible in SQL or cannot be resolved from owned relations.
DuckDB-backed SQL materialization creates the owned target schema when needed
before creating the table or view. DuckDB appender materialization treats a
successful appender close as consuming the handle; if close fails, the handle
remains retryable or explicitly releasable, and adapter-owned materialization
releases it as part of failure cleanup. Runner-side SQL asset materialization
planning emits the shared `%Favn.SQL.WritePlan{}` adapter contract consumed by
SQL runtime adapters.
For longer queries, place the SQL file next to the SQL asset module, for example
`lakehouse/mart/sales/order_summary.ex` plus `lakehouse/mart/sales/order_summary.sql`, and
use `query file: "order_summary.sql"`.

### 4. Define a pipeline

```elixir
defmodule MyDataPlatform.Pipelines.DailySales do
  use Favn.Pipeline

  pipeline :daily_sales do
    asset MyDataPlatform.Lakehouse.Mart.Sales.OrderSummary
    deps :all
    schedule cron: "0 2 * * *", timezone: "Etc/UTC"
    window :daily
    max_concurrency 2
  end
end
```

Windowed pipelines support `:hourly`, `:daily`, `:monthly`, and `:yearly`
policies. Manual local runs pass the concrete requested window, while scheduled
windowed runs resolve the previous complete period in the schedule timezone.
Invalid scheduled window policy data is isolated to that scheduler entry instead
of crashing the scheduler runtime. Pipelines without a `window` clause run
without an anchor window.

Operational backfill foundations can resolve explicit or relative ranges into
the same concrete window anchors and submit one child pipeline run per resolved
window through the orchestrator. Local development can submit explicit
operational backfills and inspect their control-plane state with
`mix favn.backfill` or the web operator pipeline backfill flow. Child pipeline
backfills default to `refresh: :missing` so already-successful windows can be
skipped while missing windows are filled. Operators can pass `--refresh force`
or choose force refresh in the web flow to intentionally recompute windows whose
external side effects need repair. The web `/runs` surface groups a parent
backfill run with its child window runs so operators see one coherent backfill,
not many unrelated runs.

Use execution concurrency controls when source systems or shared infrastructure
must be protected before asset code starts:

```elixir
config :favn,
  execution_pools: [
    global: [max_concurrency: 8],
    github_api: [max_concurrency: 2],
    shopify_api: [max_concurrency: 1]
  ]
```

```elixir
defmodule MyDataPlatform.Lakehouse.Raw.GitHub.PullRequests do
  use Favn.Asset

  @execution_pool :github_api
  def asset(ctx) do
    MyDataPlatform.GitHub.fetch_pull_requests(ctx.config.github)
  end
end

defmodule MyDataPlatform.Pipelines.RawGitHub do
  use Favn.Pipeline

  pipeline :raw_github do
    assets MyDataPlatform.Lakehouse.Raw.GitHub
    execution_pool :github_api
    max_concurrency 2
  end
end
```

`max_concurrency` is per pipeline run. `execution_pool` is global to the
orchestrator; every admitted asset step using the same pool consumes a shared
slot until the step finishes, fails, is cancelled, or times out. Asset-level
`@execution_pool` overrides the pipeline default for that asset. The reserved
`:global` pool, when configured, applies to every admitted asset step. Declared
execution pools must be configured; unknown pools fail closed instead of running
unprotected.

### 5. Configure authored modules

```elixir
import Config

config :favn,
  discovery: [
    apps: [:my_app],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ]
```

Use explicit `asset_modules`, `pipeline_modules`, `schedule_modules`, or `connection_modules` lists
when a project needs tighter control than app-scoped discovery.

### 6. Inspect and compile from IEx

```elixir
{:ok, assets} = Favn.list_assets()
{:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
{:ok, manifest} = Favn.generate_manifest()
```

### 7. Query a configured SQL connection from Elixir

```elixir
{:ok, session} = Favn.SQLClient.connect(:important_lakehouse)
{:ok, result} = Favn.SQLClient.query(session, "select 1")
:ok = Favn.SQLClient.disconnect(session)
```

For this flow, configure app-scoped `discovery` and runtime `:connections` under
`config :favn` using the `Favn.Connection` contract. Use explicit
`:connection_modules` only when the project needs tighter control than
`connections: :all` discovery.

Connection runtime values can also use `Favn.RuntimeConfig.Ref.env!/1` and
`Favn.RuntimeConfig.Ref.secret_env!/1` when values should be resolved from the
runner environment just before adapter connection.

DuckDB runtime config separates the opened DuckDB session database from attached
persistent catalogs. Use `open: [database: ":memory:"]` for production-style
DuckLake sessions where persistence lives in attached DuckLake catalogs, or use a
file-backed `open.database` for local debugging. Configure DuckDB session setup
under `duckdb: [...]`: extension `load`, typed settings, secrets, keyed catalog
`attach`, and optional `use`.

DuckDB and DuckDB ADBC connections use runner-local warm session reuse by default
when the adapter is poolable. Disable it per connection with:

```elixir
pool: [enabled: false]
```

Optional local idle-retention tuning is connection-level `pool` config:

```elixir
config :favn,
  connections: [
    important_lakehouse: [
      open: [database: ":memory:"],
      pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000],
      duckdb: [
        load: [:ducklake, :postgres, :azure],
        attach: [
          raw: [
            type: :ducklake,
            metadata: "ducklake:postgres:",
            meta_secret: :raw_meta,
            data_path: "abfss://lakehouse.dfs.core.windows.net/raw/",
            write_concurrency: 1
          ]
        ],
        use: :raw
      ]
    ]
  ]
```

Pooling is local to one runner BEAM and is not distributed across runner nodes.
It reuses warm DuckDB/ADBC sessions only when the connection/config hash,
required catalog set, and adapter fingerprint match. A checked-out pooled session
is exclusive to its checkout owner process; the shared SQL client rejects
non-owner operations and disconnect attempts. Existing catalog/write concurrency
still bounds active work and new session/bootstrap, so enabling pooling does not
increase write concurrency.
If `required_catalogs` is omitted for a connection with configured catalog
policies, Favn treats bootstrap as all-catalog bootstrap and acquires every
configured catalog permit before opening the adapter session.
Elixir assets executed by the runner inherit their owned relation catalog as the
default SQLClient scope when they open that same relation connection without
passing `required_catalogs` explicitly. That default is process-local; spawned
asset tasks should wrap their child body with `Favn.SQLClient.with_required_catalogs/2`
or pass `required_catalogs` explicitly.

Raw SQL execute/materialize/transaction paths do not return sessions to the idle
pool after mutation unless Favn has explicitly proven the operation pool-safe
internally. Read-only inspection/query paths may reuse warm sessions.

Attached catalogs can be DuckLake catalogs or DuckDB files. Favn relation names
map directly to DuckDB three-part names: a relation with `catalog: "raw"`,
`schema: "sales"`, and `name: "orders"` renders as `raw.sales.orders`, so SQL
assets can read from `raw` or `int` and write modeled outputs into `mart` within
one DuckDB connection. DuckDB file catalogs default to `write_concurrency: 1`,
while DuckLake catalogs default to `:unlimited` unless configured otherwise.
Write admission is keyed by `{connection, catalog}` so a single-writer `mart`
file catalog does not block independent DuckLake writes.

Catalog-level write admission is guaranteed for Favn SQL asset materialization,
where the target relation carries an explicit catalog. Raw
`Favn.SQLClient.execute/3` and `Favn.SQLClient.query/3` calls cannot reliably
infer the write catalog from arbitrary SQL today; use asset materialization for
protected writes or serialize manual raw SQL at the caller until a future
explicit `catalog:` option exists.

Safe retries are bounded around DuckDB session creation/bootstrap and read-only
inspection/query paths. Favn does not blindly retry SQL writes, and operations
reported with unknown commit state are not retried.

Low-tier Azure PostgreSQL metadata catalogs can still become the bottleneck for
DuckLake. Configure finite, conservative DuckLake catalog `write_concurrency`
values and consider PgBouncer or scaling the metadata database when metadata
connection or lock pressure appears. Runner-local pooling and single-flight
creation reduce repeated attach/bootstrap pressure in a single BEAM, but they do
not replace catalog write admission and do not solve multi-runner distributed
metadata pressure by themselves.

When DuckLake uses PostgreSQL for metadata, one concurrent DuckLake writer can
use multiple PostgreSQL backend connections. In observed deployments, one writer
used about three PostgreSQL backends. Size `write_concurrency` with that
multiplier, plus headroom for admin tools, migrations, monitoring, and other
application traffic.

Add `Favn.SQL.Adapter.DuckDB.config_schema_fields/0` or
`Favn.SQL.Adapter.DuckDB.ADBC.config_schema_fields/0` to DuckDB connection module
schemas. Add `production_storage_schema_fields/0` when production local-file
session databases should reject `:memory:`, relative paths, missing parents, and
unwritable parents before opening DuckDB. SQLite control-plane backups do not
include DuckDB data-plane files or DuckLake object storage. Secret runtime refs
inside nested DuckDB config are resolved on the runner side and redacted from
diagnostics. DuckDB worker unavailability,
worker-call timeouts, bootstrap failures, materialization failures, and appender
failures are normalized into structured SQL errors suitable for logs, API/UI
payloads, and run diagnostics without exposing configured secrets. Worker-call
timeouts are reported as unknown-outcome failures rather than blindly retryable
errors, because the DuckDB operation may still be running in the worker.

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
mix favn.backfill submit MyApp.Pipelines.DailySales --window month:2025-05..2026-05 --refresh force
mix favn.backfill windows RUN_ID --limit 100 --offset 0
mix favn.backfill rerun-window RUN_ID --window-key day:Etc/UTC:2026-04-01T00:00:00.000000Z --allow-success --refresh force
mix favn.backfill repair --pipeline-module MyApp.Pipelines.DailySales --apply
mix favn.runs list --status error --limit 20
mix favn.runs show RUN_ID
mix favn.logs RUN_ID
mix favn.inspect relation raw.sales.orders
mix favn.query "select count(*) from raw.sales.orders"
mix favn.logs
mix favn.status
mix favn.reload
mix favn.stop
mix favn.reset
mix favn.read_doc Favn
mix favn.read_doc Favn generate_manifest
```

These supported `mix favn.*` commands are the stable public entrypoints for
local iteration.

Security-sensitive Elixir changes should also run the repo-local audit tooling:
`mix deps.audit` for known dependency vulnerabilities, `mix hex.audit` when Hex
package freshness or metadata is relevant, and Sobelow for HTTP/Plug-facing code.
Treat findings as review input: fix high/critical issues and document any
intentional false positives.

For a fresh standalone Mix consumer project with local path dependencies back to
this checkout, `mix favn.init --duckdb --sample` generates the first dogfooding
path: a DuckDB connection module under `connections/`, a root lakehouse
namespace with connection only, raw and mart phase namespaces as catalogs, sales
segment namespaces as schemas, one Elixir raw load asset, one SQL business
output asset, a `deps(:all)` pipeline, local Favn config, and `.env.example`.
The task also adds a local `favn_duckdb` path dependency when it can recognize
the project's `defp deps/0` shape. Rerun `mix deps.get`, then use
`mix favn.doctor` before starting the stack.

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
{:ok, session} = Favn.SQLClient.connect(:important_lakehouse)
{:ok, result} = Favn.SQLClient.query(session, "select * from mart.sales.order_summary")
:ok = Favn.SQLClient.disconnect(session)
```

The generated sample attaches local DuckDB catalog files for `raw` and `mart`,
uses `sales` as the schema default under each phase, and materializes relations
as `raw.sales.orders` and `mart.sales.order_summary`.

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
internal runtime beams. During foreground startup it prints concise terminal
progress lines before slow phases such as runtime compile, project compile,
service startup, manifest publication, and HTTP readiness checks. `--root-dir`
remains an install/runtime-source override for split-root workflows.

Local tooling HTTP calls are plain HTTP loopback calls to Favn-managed local
services. They intentionally do not support remote or HTTPS URLs in the local
developer loop. `mix favn.dev` starts the runner, orchestrator, and the Phoenix
`favn_view` app. The UI is available at `http://127.0.0.1:4173` by default and
does not require a separate Node/SvelteKit process.

AI-assisted development guidance is split between the root `AGENTS.md` and
project-local OpenCode skills. `AGENTS.md` contains universal repo-wide rules.
OpenCode skills under `.opencode/skills/` contain conditional framework-specific
guidance: `favn-architecture` for boundaries, manifests, public facades, and
orchestration contracts; `phoenix-web-api` for Phoenix Endpoint, Router,
Controller, Plug, and API work, especially in `apps/favn_orchestrator`;
`phoenix-liveview` for LiveView, HEEx, components, Storybook, Tidewave, and UI
work in `apps/favn_view`; and `ecto-storage` for repos, migrations, Ecto
queries, transactions, storage adapters, and persistence tests. Tidewave is
dev-only and project-local OpenCode MCP servers are configured in `opencode.json`
for `favn_view` on `http://127.0.0.1:4173/tidewave/mcp` and the orchestrator API
on `http://127.0.0.1:4101/tidewave/mcp`; start the relevant runtime before
running `opencode mcp list` or `opencode`. PhoenixStorybook is for UI component
states under `apps/favn_view/storybook/`. The obsolete SvelteKit-focused
OpenCode agent has been removed.

Production/server startup is separate from local dev. When production runtime
configuration is enabled, the orchestrator requires explicit
`FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME` and
`FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD` values in the runtime environment and
refuses startup if they are missing.
Password login now uses Argon2id password hashes, returns an opaque session token
once, and applies an explicit absolute session TTL. Passwords must be nonblank,
15 to 1,024 characters, and are not subject to arbitrary composition rules.
Clients must forward the session token to the private orchestrator API as
`x-favn-session-token`. SQLite storage keeps only encoded Argon2 password hashes,
deterministic session-token hashes, revocation timestamps, actor metadata, and
redacted audit entries, with auth roles, credential records, and audit entries
stored as explicit JSON-safe DTO records rather than generic BEAM-term payloads;
password changes revoke active sessions for that actor.

Local numeric config values, including dev ports and Postgres port/pool size,
are accepted only as positive integers or strings that contain exactly a
positive integer after trimming whitespace. Malformed strings use the documented
local defaults.

`mix favn.run PipelineModule` submits a manifest-scoped pipeline run to the
currently running local stack through the loopback-only local-dev context, so
tutorial and local smoke runs do not require hand-written private orchestrator
API requests or local passwords. This manual run path is the recommended default
for one-time local ETL. Each invocation uses a fresh idempotency key by default;
pass `--idempotency-key KEY` only when you intentionally want deterministic
orchestrator replay behavior for a local submission. When waiting, the command
reports terminal run errors from the orchestrator separately from a local CLI wait
timeout. Use `--wait-timeout-ms` to change only the local polling budget and
`--run-timeout-ms` to change the per-asset execution timeout sent to the
orchestrator. The older `--timeout-ms` flag remains an alias for both when the
more specific flags are not provided.

`mix favn.backfill` exposes the local operational-backfill workflow for running
local stacks. Use `submit` for explicit `--from`/`--to`/`--kind` pipeline ranges
or compact `--window kind:FROM..TO` ranges such as
`--window month:2025-05..2026-05`. Add `--dry-run` to ask the orchestrator to
resolve and print the concrete windows without creating parent or child runs.
Use `--refresh force` to recompute every planned node in the selected windows;
the default remains `--refresh missing` for submit paths that do not specify a
policy.
`windows RUN_ID` inspects child windows; `coverage-baselines` and
`asset-window-states` to inspect projected backfill state, and `rerun-window
RUN_ID --window-key KEY` for failed window reruns. Add `--allow-success
--refresh force` to rerun one successful window for external side-effect repair.
Use `repair` to dry-run or,
with `--apply`, rebuild derived coverage-baseline, backfill-window, and latest
asset/window projections from authoritative run snapshots after projection drift
or read-model deletion. `repair --backfill-run-id RUN_ID` rebuilds that parent
window ledger only; use `--pipeline-module` or no scope when latest
asset/window state must also be recomputed. Child pipeline backfills default to
`refresh: :missing` unless `auto`, `missing`, or `force` is explicitly supplied.
Operational backfill submit does not accept lookback-policy input; asset window
lookback remains part of normal windowed execution only.

The Phoenix/LiveView UI is a thin operator shell behind the public orchestrator
facade. The `/runs` page shows orchestrator-owned run groups with status,
window, progress, and health filters. Run detail pages include an asset/window
matrix, failures, window runs, persisted events, and a wall-clock execution
timeline. Timeline rows show data windows as metadata; the x-axis is execution
time, so bar length remains a duration signal.

Backfill read commands are bounded. They default to `--limit 100 --offset 0`, reject
`--limit` values above `500`, only accept known status values and manifest-owned
pipeline or asset filters, and print a next-page hint when more rows are available.

For `submit`, `--wait-timeout-ms` controls local CLI polling only, while
`--run-timeout-ms` controls the child run execution timeout sent to the
orchestrator.

For repetitive endpoint-style `Favn.MultiAsset` pipelines, prefer a monthly
dry-run before submitting a longer operational backfill:

```bash
mix favn.backfill submit MyApp.Pipelines.InventoryByDayBackfill \
  --window month:2025-05..2026-05 \
  --dry-run

mix favn.backfill submit MyApp.Pipelines.InventoryByDayBackfill \
  --window month:2025-05..2026-05 \
  --wait-timeout-ms 900000 \
  --run-timeout-ms 300000
```

`mix favn.runs list`, `mix favn.runs show RUN_ID`, and `mix favn.logs RUN_ID`
provide lightweight run investigation through the orchestrator HTTP boundary.
`mix favn.status` includes active-run counts and recent failed run ids when the
local stack is running. `mix favn.inspect relation RELATION`,
`mix favn.inspect partitions RELATION`, and `mix favn.query "select ..."`
provide local SQL inspection without ad-hoc `mix run -e` snippets. `mix
favn.query` uses a best-effort read-only guardrail by default; it is not a SQL
sandbox or security boundary. Pass `--connection NAME` when multiple SQL
connections are configured. `mix favn.query` starts the current Mix app and the
SQL runtime itself, including `Favn.SQL.SessionPool`; users should run it
directly rather than wrapping it in `mix do app.start + ...`.

DuckDB and DuckDB ADBC bootstrap now accept run-scoped catalog requirements.
SQL asset execution and relation inspection pass the rendered relation catalogs
to the SQL runtime so raw-only work does not eagerly attach unrelated DuckLake
catalogs such as `mart`.

The local runner receives only the explicitly supported consumer `:favn` config
needed for local execution: `:discovery`, `:connection_modules`, `:connections`,
`:runner_plugins`, and `:duckdb_in_process_client`. This transport is local-dev
plumbing only; it uses a tagged payload rather than arbitrary app-env forwarding,
maps top-level keys explicitly, validates transported module/local atoms before
recreating them, normalizes relative connection database paths from the consumer
project root, and redacts connection/plugin secrets from diagnostics.

If you are upgrading from earlier pre-closeout local SQL storage state, run
`mix favn.reset` once so local persisted payloads are recreated in the current
canonical format.

Pre-closeout SQL payload compatibility is intentionally not supported. Existing
SQLite/Postgres rows that were persisted as BEAM term blobs must be reset or
recreated before running with the closeout adapters.

Pre-DTO SQLite auth rows are also intentionally unsupported. If a private-dev
database contains auth roles, credentials, or audit entries persisted before the
explicit auth DTO boundary, reset or recreate that SQLite control-plane state.

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

Do not add `:favn_storage_sqlite` to a consumer `mix.exs` for local SQLite
control-plane storage. The SQLite storage adapter is owned by Favn's local
runtime/package setup under `.favn/`; consumer projects select it through the
local config above or `mix favn.dev --sqlite`.

`mix favn.build.single` emits a verified project-local backend-only SQLite launcher under
`.favn/dist/single/<build_id>/`. Configure it by copying
`env/backend.env.example` to `env/backend.env` or setting `FAVN_ENV_FILE`, then
use the generated `bin/start` and `bin/stop` scripts. The launcher starts the
runner, SQLite adapter, and orchestrator in one backend BEAM runtime. It remains
project-local and non-relocatable because it depends on the installed runtime
source root. Postgres production mode is not included, and the web service is
still deployed as a separate explicit process.

Production orchestrator service tokens are configured as comma-separated
`service_identity:token` entries in `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS`.
Each token must be at least 32 characters and must not look like a placeholder,
example, password, secret, test value, todo, or token-label string; identities
must be nonblank and unique. Rotation is supported by configuring multiple active
identities. The orchestrator stores only token hashes from production runtime
config and records the matched `service_identity` in audit logs, diagnostics, and
bootstrap auth responses without exposing raw token values.

The Phoenix/LiveView UI lives in `apps/favn_view`. For the first production
target it runs as a separate OTP app in the same BEAM as the backend apps and
uses the public orchestrator facade for readiness. Web health endpoints are
available at `/api/web/v1/health/live` and `/api/web/v1/health/ready`.

`mix favn.bootstrap.single` bootstraps the backend control-plane side of the
single-node shape through orchestrator APIs. Required inputs can be passed as
`--manifest`, `--orchestrator-url`, and `--service-token`, or by the supported
environment defaults documented by the task. Bootstrap uses
`FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN` or
`FAVN_VIEW_ORCHESTRATOR_SERVICE_TOKEN` for service auth, with
`FAVN_ORCHESTRATOR_SERVICE_TOKEN` accepted only as a legacy fallback.
The command verifies service-token auth, reads and verifies the manifest JSON,
registers the manifest, activates it by default, and calls
`/api/orchestrator/v1/manifests/:manifest_version_id/runner/register` so the
orchestrator registers the persisted manifest with the local runner. Repeating
the command with the same manifest and runner registration is safe and
repeatable, and the command prints manifest registration, runner registration,
and active-manifest verification status. Active-manifest verification uses the
service-auth-only `/api/orchestrator/v1/bootstrap/active-manifest` endpoint. The
single-node acceptance path verifies first-admin login, manifest-pinned run
submission, run history, auth/session state, diagnostics, and restart survival
against the generated backend artifact with fresh SQLite storage. Built artifact
contents under `dist_dir` are read-only after build; mutable runtime state is
kept outside the artifact tree in runtime-home, database, log, and pid paths.

Storage adapter startup reports recoverable configuration failures as
`{:error, reason}`. Scheduler state keys are exact across built-in adapters:
`{pipeline_module, nil}` addresses the nil schedule id and does not fall back to
the latest concrete schedule id.

SQLite adapter startup validates the configured database path and can classify
the schema as empty, ready, missing, upgrade-required, newer than the running
release, or inconsistent. Local/default SQLite startup still auto-runs
migrations, while manual startup can reject non-ready existing databases and can
initialize empty databases when explicitly enabled.
SQLite is also the first durable backend for orchestrator auth/session/audit
state, storing auth roles, credential records, and audit entries as explicit
JSON-safe DTO records; Postgres auth persistence remains deferred.

Detailed operator diagnostics are available through the private orchestrator
HTTP endpoint `GET /api/orchestrator/v1/diagnostics` with service auth and the
local `mix favn.diagnostics` wrapper. Each check returns `check`, `status`,
`summary`, `details`, and optional `reason`, and all diagnostics are redacted
before being returned, logged, or passed to the optional
`:favn_orchestrator, :metrics_hook` callback. Scheduler diagnostics include
deterministic state summary evidence without exposing storage rows. Runner
diagnostics include redacted data-plane connection summaries when the runner can
expose them through the adapter boundary.

### favn_local implementation notes

- public `mix favn.*` tasks are owned by `apps/favn`
- public `mix favn.*` tasks reject unsupported options and unexpected positional
  arguments before performing work
- implementation is owned by `apps/favn_local`
- `mix favn.build.runner` is rooted in the current Mix project; `--root-dir`
  must match the current project root
- runner artifacts include the pinned manifest and compiled beams from the
  current project app so helper modules and connection modules are available at
  execution time
- install now materializes a runnable runtime workspace under
  `.favn/install/runtime_root` and records runtime metadata in
  `.favn/install/runtime.json`

## Common Public API Entry Points

The stable `v1` API is intended to cover authoring, manifest compilation and
registration inputs, `Favn.SQLClient`, and supported local commands. Runtime
helper functions exposed on `Favn` are runtime-dependent conveniences and are not
part of the stable `v1` contract unless they are documented here or in
`docs/production/public_api_boundary.md`.

- `Favn.list_assets/0,1`
- `Favn.asset_module?/1`
- `Favn.get_asset/1`
- `Favn.get_pipeline/1`
- `Favn.resolve_pipeline/2`
- `Favn.generate_manifest/0,1`
- `Favn.build_manifest/0,1`
- `Favn.plan_asset_run/2`
- `Favn.SQLClient.connect/1,2`, `query/2,3`, `execute/2,3`, `transaction/2,3`,
  `capabilities/1`, `relation/2`, `columns/2`, `with_connection/2,3`, and
  `disconnect/1`

`Favn.Assets` remains available as a compatibility-only authoring DSL for compact
multi-asset modules. Prefer `Favn.Asset` for new single assets and
`Favn.MultiAsset` for generated or repetitive multi-asset modules. Multi-assets
support module-level `source_config/2`; each generated asset carries the same
runtime config declarations and shared runtime code reads resolved values from
`ctx.config`. At runtime, Favn rehydrates `ctx.asset.config.rest` structural keys
and known Favn enum fields for idiomatic access. Arbitrary adapter-specific
`rest.extra` payload keys and static `rest.params` entries remain
manifest/JSON-shaped unless Favn explicitly supports that key.

## Documentation

- `docs/FEATURES.md` tracks the implemented feature set today
- `docs/ROADMAP.md` tracks planned next work and later ideas
- `docs/production/public_api_boundary.md` defines the intended package and
  stable public API boundary for `v1`
- `docs/production/single_node_contract.md` defines the first `v1` production deployment contract
- `docs/production/single_node_operator_runbook.md` documents the SQLite-first
  single-node operator path
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

Current release work is focused on product hardening, explicit support
boundaries, and operator/developer experience. The user-facing goal remains:
define assets and pipelines in Elixir, compile them into an explicit graph, and
run them with predictable behavior.
