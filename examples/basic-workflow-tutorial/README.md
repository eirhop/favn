# Basic Workflow Tutorial

This project is the beginner-friendly, end-to-end Favn tutorial.

It is intentionally built as a standalone project under
`examples/basic-workflow-tutorial` so you can learn the real user path without
umbrella shortcuts.

If you are new to ETL/ELT, data platforms, and orchestration, this guide starts
from first principles.

## What this teaches (in simple terms)

- how to describe data work as small, connected building blocks (`assets`)
- how to connect those blocks into one repeatable flow (`pipeline`)
- how Favn compiles your definitions into a frozen runtime contract
  (`manifest`)
- how Favn plans work in dependency order
- how to inspect the business data produced by the workflow

## Quick glossary

- `asset`: one unit of work (for example, make a table or compute a summary)
- `pipeline`: a named run target (which asset(s) to run and dependency behavior)
- `manifest`: a compiled snapshot of your full graph; runs are pinned to it
- `runner`: executes the actual asset logic
- `orchestrator`: plans runs, tracks status, and stores run events in the local
  tooling runtime
- `DuckDB`: the local data engine where this example writes tables

## What you are building

You are modeling a fake commerce company called `Northbeam Outfitters`.

The graph has 15 assets:
- `sources`: static lookup inputs
- `raw`: synthetic generated base tables
- `stg`: cleaned and joined analytics tables
- `gold`: business-facing summary tables
- `ops`: final completion marker asset

Top output asset: `FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete`.

## Code walkthrough (aligned with module docs)

This section explains what each module does in plain language, using the same
intent described in each module's `@moduledoc`.

### Project and runtime boundary modules

- `FavnReferenceWorkload` in `examples/basic-workflow-tutorial/lib/favn_reference_workload.ex`
  - "Reference workload scaffold for the Favn v0.5 architecture"
  - Think of this as the root namespace for the example.
- `FavnReferenceWorkload.Connections.Warehouse` in `examples/basic-workflow-tutorial/lib/favn_reference_workload/connections/warehouse.ex`
  - "DuckDB warehouse connection used by the reference workload"
  - This defines how Favn connects to the local DuckDB file.
- `FavnReferenceWorkload.Pipelines.ReferenceWorkloadDaily` in `examples/basic-workflow-tutorial/lib/favn_reference_workload/pipelines/reference_workload_daily.ex`
  - "Canonical manual reference workload pipeline"
  - This is the one named pipeline that targets the final asset and includes
    all dependencies (`deps(:all)`).

### Source assets (`sources` schema)

- `FavnReferenceWorkload.Warehouse.Sources.CountryRegions`
  - "External region lookup source used by synthetic customer generation"
  - Treat this as a lookup table the rest of the graph can read.
- `FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog`
  - "External channel lookup source used by synthetic order generation"
  - Another lookup table used to keep generated orders realistic.

These are `Favn.Source` assets, so they model available data relations but are
not transformation logic.

### Raw assets (`raw` schema)

- `FavnReferenceWorkload.Warehouse.Raw.Customers`
  - "Deterministic customer ingest from a simulated API JSON payload"
  - Simulates an API response and loads it to `raw.customers` through DuckDB
    `read_json(...)`, then filters by `sources.country_regions`.
- `FavnReferenceWorkload.Warehouse.Raw.Products`
  - "Deterministic product ingest from a simulated API JSON payload"
  - Loads product JSON rows into `raw.products` through `read_json(...)`.
- `FavnReferenceWorkload.Warehouse.Raw.Orders`
  - "Deterministic order ingest from a simulated API JSON payload"
  - Loads order JSON rows and checks customer/channel lookups exist.
- `FavnReferenceWorkload.Warehouse.Raw.OrderItems`
  - "Deterministic order-item ingest from a simulated API JSON payload"
  - Loads item JSON rows and ensures order/product references are valid.
- `FavnReferenceWorkload.Warehouse.Raw.Payments`
  - "Deterministic payment ingest from a simulated API JSON payload"
  - Loads payment JSON rows and keeps only orders that exist.

All raw modules are `Favn.Asset` Elixir assets. They simulate an external API
client returning JSON and then load those JSON payloads into concrete DuckDB
`raw.*` tables with `read_json(...)`.

Best-practice shape shown here:
- reusable clients live under `examples/basic-workflow-tutorial/lib/favn_reference_workload/client/`
- assets keep `use Favn.Namespace` minimal and inherit namespace defaults from
  parent modules
- assets pass only `ctx.asset.relation` into the DuckDB client instead of the
  whole runtime context

### Staging assets (`stg` schema)

- `FavnReferenceWorkload.Warehouse.Stg.Customers`
  - "Normalized customer attributes for analytics modeling"
  - Standardizes fields (like country casing) and adds `market_tier`.
- `FavnReferenceWorkload.Warehouse.Stg.OrderFacts`
  - "Joined order-level fact model across orders, items, products, and payments"
  - Builds the central analytics fact table by joining raw layers.
- `FavnReferenceWorkload.Warehouse.Stg.ProductDaily`
  - "Daily product performance aggregate"
  - Aggregates product units/revenue by day.

### Gold assets (`gold` schema)

- `FavnReferenceWorkload.Warehouse.Gold.Customer360`
  - "Customer lifecycle and value summary"
  - Builds per-customer business metrics (orders, LTV, activity dates).
- `FavnReferenceWorkload.Warehouse.Gold.RevenueDaily`
  - "Daily revenue KPI output"
  - Builds daily revenue and active customer KPI rows.
- `FavnReferenceWorkload.Warehouse.Gold.ChannelEfficiency`
  - "Channel performance summary for acquisition analysis"
  - Compares channels by orders, customers, and revenue.
- `FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview`
  - "One-row executive KPI snapshot for the reference workload"
  - Produces the top-level summary row used as the final business output.

### Final ops asset (`ops` schema)

- `FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete`
  - "Terminal marker asset for the canonical reference workload pipeline"
  - This is a small Elixir `Favn.Asset` (`asset/1` returns `:ok`) that depends
    on `Gold.ExecutiveOverview`, so a successful run proves the full graph ran.

## How dependency inference works here

In SQL assets, dependencies are inferred from relation references in SQL.

Simple example from this project:
- `from stg.order_facts` means "this asset depends on `stg.order_facts`"
- `inner join gold.customer_360` means "this asset also depends on
  `gold.customer_360`"

That is why most SQL assets do not need explicit `@depends`; the SQL itself is
the dependency declaration.

## Step 0: Open this directory

Run all commands from:

```bash
examples/basic-workflow-tutorial
```

## Step 1: Install dependencies and compile

```bash
mix deps.get
mix compile
```

What this does:
- installs Elixir dependencies
- compiles the example assets and pipeline modules

## Step 2: Run the end-to-end test suite (fastest confidence path)

```bash
mix test
```

What this verifies:
- manifest generation for the full graph
- DuckDB execution of SQL assets

If this passes, the main reference workload path is healthy.

## Step 3: Explore the manifest in IEx

```bash
iex -S mix
```

Then run:

```elixir
{:ok, manifest} = Favn.generate_manifest()
length(manifest.assets)
length(manifest.pipelines)
```

What this means:
- Favn compiled your authored modules into a manifest
- the graph can be planned and run from this stable snapshot

## Step 4: Query the generated DuckDB outputs

Still in IEx:

```elixir
alias Favn.SQLClient

{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select count(*) as n from gold.customer_360", [])
Favn.SQLClient.disconnect(session)

result
```

Try similar checks for:
- `gold.revenue_daily`
- `gold.channel_efficiency`
- `gold.executive_overview`

Why this matters:
- you are looking directly at the data products produced by the workload
- this is the "business output" side of the architecture

## Step 5: Local tooling loop (`mix favn.*`)

This project is intended to support this local loop once issue #129 is fixed:

```bash
mix favn.install
mix favn.dev
mix favn.status
mix favn.logs --service orchestrator --tail 200
mix favn.reload
mix favn.stop
mix favn.reset
```

Current repo limitation:
- `mix favn.install` works from this embedded tutorial project
- `mix favn.dev` currently fails orchestrator readiness from this nested example
  path and is tracked in https://github.com/eirhop/favn/issues/129
- SQLite-backed local tooling is intentionally not part of the default tutorial
  path while `mix favn.dev --sqlite` startup is tracked separately in
  https://github.com/eirhop/favn/issues/128

## Alternative configurations you can try

The default tutorial path keeps things simple (DuckDB + in-memory local control
plane), but you can change behavior through
`examples/basic-workflow-tutorial/config/config.exs`.

### 1) Change DuckDB file location

Current config:

```elixir
config :favn,
  connections: [
    warehouse: [database: ".favn/data/reference_workload.duckdb"]
  ]
```

You can point this to another path, for example:

```elixir
config :favn,
  connections: [
    warehouse: [database: "/tmp/reference_workload.duckdb"]
  ]
```

### 2) Change local orchestrator storage mode

Current config is in-memory:

```elixir
config :favn, :local,
  storage: :memory
```

Alternative modes:
- `:memory` for ephemeral local state (fast, but not persisted)
- `:sqlite` for SQLite-backed local control-plane state once configured with
  `favn_storage_sqlite`
- `:postgres` for Postgres-backed local control-plane storage

You can also switch at runtime with flags:
- `mix favn.dev --postgres`

### 3) Change DuckDB execution placement

Current plugin config:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

Alternative:
- set `execution_mode: :separate_process` to run DuckDB execution in a separate
  process boundary.

### 4) Change pipeline behavior

Current pipeline uses `deps(:all)` so the full graph runs from the top target.

Possible alternatives in pipeline definitions:
- use a narrower target asset for faster development loops
- change dependency mode if you want more selective execution
- add schedule settings if you want scheduled (instead of manual) runs

For beginners, keep `deps(:all)` until you are comfortable with graph behavior.

## Expected files and data

Data plane (DuckDB):
- `.favn/data/reference_workload.duckdb`

Local tooling runtime state (when `mix favn.*` starts normally):
- `.favn/runtime.json`
- `.favn/logs/*.log`

## Troubleshooting

- `install failed: web dependency install failed`
  - use hyphenated flags (`--skip-web-install`, not `--skip_web_install`)
- `runner_node_unreachable` with host errors
  - known issue: distributed node host validation can reject current host value
  - tracked in `https://github.com/eirhop/favn/issues/107`
- partial stack state message from `mix favn.dev`
  - run `mix favn.stop` and retry

## Where to look in code

- main config: `examples/basic-workflow-tutorial/config/config.exs`
- pipeline: `examples/basic-workflow-tutorial/lib/favn_reference_workload/pipelines/reference_workload_daily.ex`
- raw/stg/gold assets: `examples/basic-workflow-tutorial/lib/favn_reference_workload/warehouse/`
- e2e tests: `examples/basic-workflow-tutorial/test/`

## What features this tutorial demonstrates

- public authoring DSLs (`Favn.Asset`, `Favn.SQLAsset`, `Favn.Pipeline`)
- SQL-first dependency inference from relation references
- manifest generation and manifest pinning
- runner execution through the DuckDB plugin

This is the canonical first learning path for the current Favn architecture.
