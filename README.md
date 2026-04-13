<div align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="300" />
  <p><strong>Asset-first orchestration for Elixir</strong></p>
  <p>Define business logic as assets. Let Favn discover dependencies, plan runs, and execute deterministic workflows.</p>
</div>

<p align="center">
  <strong>Status:</strong> Not recommended for production. API and DSL may change.
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#current-limitations">Current limitations</a> •
  <a href="#roadmap-and-release-focus">Roadmap</a> •
  <a href="#installation">Installation</a> • 
  <a href="/docs/FEATURES.md">Features</a> • 
  <a href="/lib/favn.ex">Docs</a>

</p>

## Favn at a glance

- Plain Elixir functions become assets with metadata and dependencies.
- Preferred single-asset authoring uses one module with `use Favn.Asset` and `def asset(ctx)`.
- SQL assets can be authored as one module with `use Favn.SQLAsset`, `query do ... end` or `query file: "..."`, and a real `~SQL""" ... """` sigil.
- Assets can optionally own warehouse relations through `@relation`.
- Dependency graphs are discovered automatically from asset definitions.
- Runs are planned deterministically and executed in dependency order.
- Runtime state and run events are exposed through a small public API.

## Preferred single-asset DSL

Phase 2 introduces `Favn.Asset` as the preferred one-module-per-asset DSL:

```elixir
defmodule MyWarehouse.Raw.Sales.Orders do
  use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
  use Favn.Asset

  @doc "Extract raw orders from upstream API"
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  @relation true
  def asset(ctx) do
    _target = ctx.asset.relation
    :ok
  end
end
```

`Favn.Asset` supports:

- `@doc`
- `@meta`
- `@depends` (module shorthand `@depends MyApp.UpstreamAsset` only for single-asset modules; use tuple refs for multi-asset modules)
- `@window`
- `@relation`

Single-asset modules compile to one canonical `%Favn.Asset{}` with ref `{Module, :asset}`.

## Preferred single-asset SQL DSL

Phase 3 introduces `Favn.SQLAsset` as the preferred one-module-per-asset SQL DSL:

```elixir
defmodule MyWarehouse.Gold.Sales.FctOrders do
  use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "sales"]
  use Favn.SQLAsset

  @doc "Gold fact table for orders"
  @meta owner: "analytics", category: :sales, tags: [:gold]
  @depends MyWarehouse.Silver.Sales.StgOrders
  @window Favn.Window.daily(lookback: 2)
  @materialized {:incremental, strategy: :delete_insert, window_column: :order_date}

  query do
    ~SQL"""
    select
      order_id,
      customer_id,
      order_date,
      total_amount
    from silver.sales.stg_orders
    """
  end
end
```

`Favn.SQLAsset` currently supports:

- `@doc`
- `@meta`
- `@depends`
- `@window`
- `@materialized`
- `@relation`

SQL assets compile into the same canonical `%Favn.Asset{}` shape as Elixir assets, including ref `{Module, :asset}` and inferred relation ownership.

Phase 4a runtime integration now includes:

- `Favn.render/2`
- `Favn.preview/2`
- `Favn.explain/2`
- `Favn.materialize/2`

`render/2` is backend/session free and emits canonical rendered SQL plus backend-neutral positional bindings.
`preview/2` returns both canonical rendered SQL (`preview.render.sql`) and the actual executed preview statement (`preview.statement`).

## SQL DSL direction

Favn SQL uses a real `~SQL` sigil as the single SQL body language.

SQL assets declare their main query with `query do ... end` or `query file: "..."`.
Reusable SQL uses `defsql ... do ... end` or `defsql ..., file: "..."` through `Favn.SQL`.

Both asset queries and reusable SQL macros use the same `~SQL` body syntax and the same `@name` placeholder syntax for injected values.

File-backed SQL is loaded at compile time only, tracked as an external compile resource, and compiled into the same SQL template IR as inline SQL.

Implemented now:

- real `~SQL` sigil
- `query do ... end`
- reusable `defsql ... do ... end` via `Favn.SQL`
- one `@name` placeholder model for runtime inputs and reusable SQL arguments
- ordered scanner-based SQL IR with parsed placeholders, reusable SQL calls, and asset refs
- expression SQL macros and relation SQL macros
- direct asset references in relation position, such as `from MyApp.Raw.Sales.Orders`
- compile-time validation for duplicate visible SQL definitions and reusable SQL cycles

Not supported yet for direct SQL asset refs:

- dialect-specific table functions
- `update ... from`
- `merge into`
- arbitrary nested dialect syntax
- plain relation names inferred from raw text

Implemented in Phase 5:

- relation-based dependency inference from typed SQL relation references
- additive merge of explicit + inferred dependencies with provenance metadata
- relation ownership diagnostics (unmanaged warnings, ambiguous ownership errors)
- lexical CTE alias exclusion for relation inference across top-level and nested subqueries

Planned later:

- registered external source/dependency identities to replace unmanaged warnings with explicit registrations

Favn explicitly avoids fake sigils, Jinja-style templating, arbitrary Elixir interpolation inside SQL, and string stitching as the normal authoring workflow.

Placeholder rules:

- in `query` bodies, non-reserved `@name` placeholders resolve from runtime params
- in `defsql` bodies, non-reserved `@name` placeholders must match declared arguments

## Introduction

Favn is an asset-first orchestration library for Elixir.

It helps you define business logic as simple, well-documented functions, automatically discover their relationships, and reliably execute them as deterministic workflows.

Favn means to hold or embrace—reflecting its role in keeping your workflows connected and reliable.

Instead of building pipelines manually, you describe your system through assets and their dependencies. Favn takes care of planning, execution, and coordination—ensuring that everything runs in the correct order, at the right time, and on the right machine.

Designed for the BEAM, Favn scales from a single node to distributed systems where work is executed in parallel across available resources.

Favn is built to be:
- predictable — deterministic runs based on explicit dependencies  
- reliable — execution you can trust in production  
- observable — clear insight into runs, state, and flow  
- ergonomic — simple APIs with strong documentation at the center  
- agent-friendly — easy for both humans and AI to understand, use, and extend  

Whether you're building ETL pipelines, system integrations, workflows, or AI-driven processes, Favn acts as the layer that holds everything together and ensures it runs as expected.

Favn doesn’t just run your workflows. It takes care of them.

## Quickstart

Define an asset module:

```elixir
defmodule MyApp.SalesAssets do
  use Favn.Assets

  @asset true
  def extract_orders(_ctx), do: :ok

  @asset true
  @depends :extract_orders
  def build_daily_report(_ctx), do: :ok
end
```

Relation ownership can be declared separately from logical dependencies:

```elixir
defmodule MyApp.Warehouse.Raw.Sales do
  use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
end

defmodule MyApp.Warehouse.Raw.Sales.Assets do
  use Favn.Namespace
  use Favn.Assets

  @asset true
  @relation true
  def orders(ctx) do
    ctx.asset.relation
    :ok
  end
end
```

Register the module and run a target asset:

```elixir
# config/config.exs
import Config

config :favn,
  asset_modules: [MyApp.SalesAssets]
```

```elixir
# from your app runtime / iex
{:ok, run_id} = Favn.run_asset({MyApp.SalesAssets, :build_daily_report}, dependencies: :all)
{:ok, run} = Favn.await_run(run_id)
```

Backfill a windowed asset or pipeline over a range (single submitted run):

```elixir
range = %{
  kind: :day,
  start_at: DateTime.from_naive!(~N[2025-01-01 00:00:00], "Etc/UTC"),
  end_at: DateTime.from_naive!(~N[2025-01-04 00:00:00], "Etc/UTC"),
  timezone: "Etc/UTC"
}

{:ok, run_id} = Favn.backfill_asset({MyApp.SalesAssets, :build_daily_report}, range: range)
{:ok, run} = Favn.await_run(run_id)

# provenance persisted on run snapshots
run.backfill.range
run.backfill.anchor_ranges
run.pipeline.backfill_range
```

Freshness policy helpers over persisted window/node results:

```elixir
{:ok, freshness} =
  Favn.check_asset_freshness({MyApp.SalesAssets, :build_daily_report},
    window_key: hd(run.plan.target_node_keys) |> elem(1),
    max_age_seconds: 3_600
  )
```


## New in v0.2 DSL shape

v0.2 has been refactored to the following authoring contract and attribute order:

```elixir
@asset "Asset Name"
@meta owner: "data-platform", category: :sales, tags: [:daily]
@window Favn.Window.daily(lookback: 1)
@doc "What this asset does"
@depends {:MyApp.UpstreamAssets, :upstream_asset}
@spec asset_name(map()) :: :ok | {:ok, map()} | {:error, term()}
def asset_name(ctx) do
  :ok
end
```

Notes:
- Use one `@depends` entry per declaration; repeat `@depends` for multiple dependencies.
- Use `@window` when an asset should execute against a runtime-resolved window.
- `@uses` is deferred and intentionally out of scope for this refactor.
- Missing `@doc`/`@spec` is allowed (UI will simply have less metadata).

## Configuration

Favn is configured through the `:favn` application environment:

```elixir
import Config

config :favn,
  asset_modules: [MyApp.SalesAssets],
  pipeline_modules: [MyApp.Pipelines.DailySales],
  pubsub_name: MyApp.PubSub,
  scheduler: [enabled: true, default_timezone: "Etc/UTC"],
  storage_adapter: Favn.Storage.Adapter.Memory,
  storage_adapter_opts: []
```

Key settings:

- `asset_modules`: modules that define assets with `use Favn.Asset`, `use Favn.SQLAsset`, or `use Favn.Assets`.
- `pipeline_modules`: pipeline modules discovered by the scheduler runtime.
- `:pubsub_name`: PubSub server name used for run event broadcasting.
- `:scheduler`: trigger scheduler runtime options (`enabled`, `default_timezone`, `tick_ms`). `enabled` defaults to `true`; `tick_ms` defaults to `15_000`.
- `:storage_adapter`: storage adapter module (must persist both runs and scheduler state).
- `:storage_adapter_opts`: options passed to the configured storage adapter.

Custom adapters are supported. To integrate cleanly, implement the full `Favn.Storage.Adapter`
contract, including run persistence callbacks and scheduler-state callbacks.

SQLite durable storage (single-node) can be configured with:

```elixir
import Config

config :favn,
  storage_adapter: Favn.Storage.Adapter.SQLite,
  storage_adapter_opts: [
    database: "/var/lib/my_app/favn.db",
    busy_timeout: 5_000,
    pool_size: 1
  ]
```

SQLite ordering notes:

- `runs.updated_seq` is allocated from a dedicated `favn_counters` row (`run_write_order`)
  inside the same transaction that persists the run snapshot.
- `list_runs` remains ordered by latest persisted write:
  `updated_seq DESC, updated_at_us DESC, id DESC`.

## Current limitations

- The default run store is node-local in-memory storage.
- Public run execution is asynchronous by default (`Favn.run_asset/2` returns a run id).
- Independent ready steps execute in parallel within a run, bounded by `max_concurrency`.
- Run events are best-effort pubsub notifications.

## SQL connection foundation (v0.4)

The first SQL foundation step is documented in:

- [`docs/CONNECTION_FOUNDATION_ARCHITECTURE.md`](docs/CONNECTION_FOUNDATION_ARCHITECTURE.md)
- [`docs/SQL_ADAPTER_ARCHITECTURE.md`](docs/SQL_ADAPTER_ARCHITECTURE.md)
- [`docs/sql_adapter_scope.md`](docs/sql_adapter_scope.md) (includes the DuckDB/duckdbex v0.4 implementation request and risk gates)

The architecture and current implementation provide:

- `Favn.Connection` definition providers
- schema-driven runtime config merge/validation
- boot-time connection loading with fail-fast validation
- redacted public lookup APIs on `Favn`

This foundation is the base contract that `Favn.SQL.Adapter` work builds on.

The internal SQL runtime contract now includes:

- `Favn.SQL.Adapter` as the backend behaviour boundary
- `Favn.SQL` as the runtime facade starting from `%Favn.Connection.Resolved{}`
- `Favn.SQL.Adapter.DuckDB` as the first internal DuckDB runtime adapter backed by `duckdbex`
- DuckDB appender-backed table writes now preserve normal table create semantics, run inside explicit transactions, and close/release appender handles on all paths
- DuckDB runtime errors are normalized to `%Favn.SQL.Error{}` with retryability metadata and rollback-failure surfacing

DuckDB runtime constraints in v0.4:

- single-node operation is the supported model
- concurrent reads and writes are supported within a single process, with optimistic conflict handling
- external concurrent writers can still conflict; those conflicts surface as normalized retryable execution errors
- `duckdbex` is an internal implementation detail and is kept replaceable for later extraction to a first-party package (planned direction: `favn_duckdb`)

## Runtime behavior in this release

- **Planning and orchestration**: Favn plans dependency-aware runs with deterministic topological stages and orchestrates execution through a run-scoped coordinator with bounded parallel step dispatch per run.
- **Run lifecycle**: runtime orchestration now uses explicit internal run and step state machines; public run status includes `:running | :ok | :error | :cancelled | :timed_out`.
- **Step retries**: failed steps can be retried with deterministic step-level policy (`retry` option on `Favn.run_asset/2`) without restarting the run.
- **Execution boundary**: one-step asset invocation is isolated behind an asynchronous runtime executor boundary.
- **Storage facade contract**: run retrieval/listing APIs normalize storage failures to one of:
  - `:not_found`
  - `:invalid_opts`
  - `{:store_error, reason}`
- **Checkpoint persistence policy**: runtime checkpoints are required; if snapshot persistence fails, `Favn.run_asset/2` returns `{:error, {:storage_persist_failed, reason}}`.
- **Event delivery**: run events are published as best-effort observability signals and do not affect run correctness.
- **Internal telemetry**: runtime boundaries emit machine-oriented `:telemetry` events under `[:favn, :runtime, ...]` for operators and future external exporters.
- **Logger correlation metadata**: runtime coordinator/executor processes attach lightweight metadata (`run_id`, `ref`, `stage`, `attempt`) for human diagnostics without treating logs as telemetry.

## Runtime windowing foundation (in progress)

v0.3 introduces shared runtime windowing primitives intended for both Elixir
assets now and SQL assets later.

Current foundation modules:

- `Favn.Window`
- `Favn.Window.Spec`
- `Favn.Window.Anchor`
- `Favn.Window.Runtime`
- `Favn.Window.Key`

These modules establish canonical window data types and deterministic
window-key encoding/decoding. Planner/runtime/storage integration lands in
subsequent v0.3 slices.

Current runtime internals now key step state by `{asset_ref, window_key}`
The plan model also includes `target_node_keys` so runtime target completion
checks are node-key-based rather than ref scans.
It also includes `node_stages` so runtime recovery/promotion logic can iterate
stages by node key.
Planner v1 now expands windowed assets into concrete `{asset_ref, window_key}`
nodes from an optional `anchor_window` (hour/day/month with lookback).
`resume_from_failure` reruns now support duplicate refs when node-keyed
persisted run results are available.

Asset modules can now attach window specs directly on assets:

```elixir
@asset true
@window Favn.Window.daily(lookback: 1)
def daily_sales(ctx), do: :ok
```

Runtime context now includes:

- `ctx.window` (concrete execution window for the current node)
- `ctx.asset.ref` and `ctx.asset.relation`
- `ctx.pipeline.anchor_window` (run-level requested window intent)
- `ctx.pipeline.run_kind` (`:pipeline` or `:pipeline_backfill`)
- `ctx.pipeline.resolved_refs` (deterministic resolved target refs snapshot)
- `ctx.pipeline.deps` (resolved dependency mode used for planning)

## Guarantees in this release

- **Run lifecycle semantics**
  - `Favn.run_asset/2` returns `{:ok, run_id}` when a run is submitted.
  - Step admission order is deterministic for equally-ready refs; completion order is naturally non-deterministic under parallel execution.
  - `Favn.cancel_run/1` requests cancellation and returns a cancellation acknowledgement tuple.
  - `Favn.await_run/2` returns `{:ok, %Favn.Run{status: :ok}}` on success or `{:error, %Favn.Run{status: :error | :cancelled | :timed_out}}` on non-success terminal outcomes.
  - Failed runs preserve structured failure context in both `run.error` and `run.asset_results[ref].error`.
  - `Favn.get_run/1` returns the latest stored run state for an ID.
- **Storage error contract**
  - `Favn.get_run/1` and `Favn.list_runs/1` return storage errors only as `:not_found`, `:invalid_opts`, or `{:store_error, reason}`.
  - Adapter-specific raw errors are wrapped as `{:store_error, reason}`.
- **Event delivery scope**
  - `Favn.subscribe_run/1` and `Favn.unsubscribe_run/1` manage PubSub subscriptions for run topics.
  - Event delivery is best-effort; missing subscribers or publish failures do not change run success/failure outcomes.
  - Events use a stable envelope schema (`schema_version`, `event_type`, `entity`, `run_id`, `sequence`, `emitted_at`, `status`, `data`, optional `ref`/`stage`).
  - `status` is the **internal runtime** run/step status at emission time (not `%Favn.Run{}` public status).

## Not guaranteed yet / non-goals

- Durable distributed execution guarantees across nodes.
- Exactly-once event delivery, replay, or durable event logs.
- Persistent storage guarantees beyond the configured adapter behavior (default adapter is in-memory and node-local).
- Global concurrency fairness across runs and retries.

## v0.3 pipeline DSL direction (foundation PR)

The first v0.3 pipeline foundation slice keeps pipelines as a composition layer on top of the existing asset graph planner.

- Pipelines are **not** a second graph/planning DSL.
- Pipeline selection resolves to asset refs and then uses dependency-based planning.
- Initial DSL is intentionally small and user-friendly:
  - `asset`
  - `assets`
  - `select`
  - `deps`
  - `schedule`
  - `window`
  - `source`
  - `outputs`

Pipeline `partition` has been removed; use `window` for runtime window intent.

Selection authoring supports both:

- shorthand (`asset` / `assets`) for common cases
- `select do ... end` for flexible selection (module/tag/category/ref style criteria)

Inside `select do ... end`, selectors are additive (union-based), not intersection-based.
That means each selector contributes refs to one combined target set before dedupe/sort.

A pipeline definition should use either shorthand selection or `select`, but not both in the same definition.

Schedule authoring now supports both:

- reusable named schedules in modules that `use Favn.Triggers.Schedules`
- inline schedule definitions directly inside `pipeline ... do`

Pipeline schedule references use explicit refs:

- `schedule {MyApp.Schedules, :daily_oslo}`
- `schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :skip, overlap: :forbid`

Schedule DSL validation is strict at authoring time:

- cron must be a valid 5-field cron expression
- when both cron day-of-month and day-of-week are restricted, evaluation uses standard OR semantics
- timezone must be a known zoneinfo identifier

Deferred from the first v0.3 pipeline foundation PR:

- schedule/polling runtime engines
- API-triggered execution surface
- DB-managed pipeline installations/runtime overrides
- source/output runtime binding behaviors
- multi-output runtime fan-out behavior

Planned later: a `where` clause for richer asset filtering/querying. `where` is intended as a filtering layer only and must not replace dependency-based graph planning.

### Pipeline foundation API (v0.3)

The first v0.3 implementation adds manual pipeline planning/runs:

- `Favn.plan_pipeline(MyApp.Pipelines.DailySales)`
- `Favn.run_pipeline(MyApp.Pipelines.DailySales, params: %{requested_by: "operator"})`

Execution model guidance:

- `run_pipeline/2` is the primary operator-facing execution entrypoint.
- `run_asset/2` is a lower-level primitive for simple assets, tests, debugging, and development flows.
- In operator workflows, prefer selecting/running a pipeline (and narrow to a single asset with `deps: :none` when needed).
- `run_asset/2` can also take explicit manual pipeline provenance/context when needed:
  `Favn.run_asset(ref, pipeline_context: %{...})`.

Assets can read pipeline-aware fields from `ctx.pipeline` during pipeline-triggered runs (for example pipeline identity, config, trigger metadata, and runtime params).
`ctx.params` remains the generic run params map, while `ctx.pipeline.params` exposes the pipeline-trigger params/provenance payload.

### Stable operator actions (v0.3)

v0.3 includes stable operator actions for:

- `run` (`run_asset/2`, `run_pipeline/2`)
- `cancel` (`cancel_run/1`)
- `rerun` (`rerun_run/2`)

Rerun behavior:

- rerun is run-id based and supports two modes:
  - `:resume_from_failure` (default)
  - `:exact_replay` (explicit)
- both modes use persisted execution intent from the source run
- rerun is allowed for terminal runs (`:ok | :error | :cancelled | :timed_out`)
- reruns persist lineage metadata (`replay_mode`, `rerun_of_run_id`, `parent_run_id`, `root_run_id`, `lineage_depth`)

## Roadmap and release focus

- Add durable production-ready storage adapters with stronger operational guarantees.
- Expand run query capabilities for richer operator UIs.
- Improve event observability integrations (telemetry/export pipelines).
- Land orchestration-layer fundamentals for v1 (pipeline composition + manual execution first, then API/schedule/polling/freshness and installed runtime config via `ctx`).
- Add release packaging/versioning via Hex.

## Installation

Favn is not published on Hex yet.

Install it from the GitHub repository by adding `favn` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:favn, git: "https://github.com/eirhop/favn.git", tag: "v0.3.0"}
  ]
end
```

Once Favn is published on Hex, the dependency can move to a normal versioned
package declaration:

```elixir
def deps do
  [
    {:favn, "~> 0.3.0"}
  ]
end
```
