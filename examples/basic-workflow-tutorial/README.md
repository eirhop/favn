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
- `sources`: static lookup seed tables
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
  - It includes a short local smoke-test schedule that is due every 15 seconds.

### Seed assets (`sources` schema)

- `FavnReferenceWorkload.Warehouse.Sources.CountryRegions`
  - "Deterministic region lookup seed used by synthetic customer generation"
  - Creates the `sources.country_regions` lookup table the rest of the graph can
    read.
- `FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog`
  - "Deterministic channel lookup seed used by synthetic order generation"
  - Creates the `sources.channel_catalog` lookup table used to keep generated
    orders realistic.

These are normal Elixir `Favn.Asset` modules because the tutorial owns the seed
data. Use `Favn.Source` only when a relation is managed outside Favn and should
be observed, not created, by the workload.

### Raw assets (`raw` schema)

- `FavnReferenceWorkload.Warehouse.Raw.Customers`
  - "Deterministic customer ingest from a simulated API JSON payload"
  - Simulates an API response and loads it to `raw.customers` through DuckDB
    `read_json(...)`, then filters by `sources.country_regions`.
- `FavnReferenceWorkload.Warehouse.Raw.Products`
  - "Deterministic product ingest from a simulated API JSON payload"
  - Loads product JSON rows into `raw.products` through `read_json(...)`.
- `FavnReferenceWorkload.Warehouse.Raw.Orders`
  - "Deterministic source-system order ingest from a simulated API JSON payload"
  - Resolves source runtime config from `ctx.runtime_config`, calls the fake source
    client with a narrow config map, loads order JSON rows through
    `Favn.SQLClient`, checks customer/channel lookups exist, and returns
    structured run metadata.
- `FavnReferenceWorkload.Warehouse.Raw.OrderItems`
  - "Deterministic order-item ingest from a simulated API JSON payload"
  - Loads item JSON rows and ensures order/product references are valid.
- `FavnReferenceWorkload.Warehouse.Raw.Payments`
  - "Deterministic payment ingest from a simulated API JSON payload"
  - Loads payment JSON rows and keeps only orders that exist.

All raw modules are `Favn.Asset` Elixir assets. They simulate an external API
client returning JSON and then load those JSON payloads into concrete DuckDB
`raw.*` tables with `read_json(...)`.

`Raw.Orders` is the canonical source-system raw landing example. It declares
the reusable `RuntimeConfigs.source_system()` bundle, reads the resolved source segment and token from
`ctx.runtime_config.source_system`, keeps the fake source client outside the asset, lands
rows into the owned raw relation, and returns metadata with `rows_written`,
`mode`, `relation`, `loaded_at`, and a SHA-256 `segment_id_hash`. The raw segment
ID and token are intentionally not returned in metadata.

Best-practice shape shown here:
- reusable clients live under `examples/basic-workflow-tutorial/lib/favn_reference_workload/client/`
- structural parent namespaces define shared defaults while asset modules use
  only their asset DSL and inherit through module ancestry
- assets pass only `ctx.asset.relation` into the DuckDB client instead of the
  whole runtime context
- source-system assets pass only the resolved source config to source clients,
  not the whole runtime context

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

That is why most SQL assets do not need explicit `depends`; the SQL itself is
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

If this passes, the main reference workload path is healthy. The tests keep
some direct runtime calls behind private test helpers so they can assert narrow
behavior quickly; copy the `mix favn.*` commands below for normal local usage.

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

## Step 4: Run through the local orchestrator boundary

Use the local tooling loop from this consumer-style project:

```bash
mix favn.install
mix favn.dev
mix favn.run FavnReferenceWorkload.Pipelines.ReferenceWorkloadDaily
mix favn.status
mix favn.logs --service control-plane --tail 200
mix favn.reload
mix favn.stop
mix favn.reset
```

`mix favn.run` submits the tutorial pipeline to the running local orchestrator
using the active manifest. This is the recommended user-facing way to execute
the workload locally; it keeps run planning, manifest pinning, execution, and
status tracking behind the orchestrator/runner boundary.

## Step 5: Query the generated DuckDB outputs

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

## Local stack configuration

The local CLI path uses Favn's loopback-only local-dev context and does not need
orchestrator usernames or passwords.

The source-system raw landing example also needs deterministic local source
credentials when it runs through the runner:

```sh
FAVN_REFERENCE_SOURCE_SEGMENT_ID=northbeam-demo-segment
FAVN_REFERENCE_SOURCE_TOKEN=local-demo-token
```

These values are resolved by the runner into `ctx.runtime_config`; they are not embedded
in the manifest. The returned run metadata includes only a hash of the segment
identity, never the raw segment ID or token.

Then open the View URL printed by `mix favn.dev`. The generated local operator
username is `admin`; its owner-only generated password is the
`bootstrap_password` value in `.favn/secrets.json`. The login page always uses
orchestrator-owned password authentication.

Install freshness notes:
- `mix favn.install` pulls and verifies the version-matched prebuilt control
  plane, then records its immutable digest under `.favn/install`
- repeating install reuses the exact valid local digest
- `mix favn.install --force` repulls, revalidates, and regenerates Compose state

The generated Compose application owns PostgreSQL and its credentials; no host
database setup or `FAVN_DATABASE_URL` is required. The scheduler is disabled by
default so one-time local ETL does not run active schedules unexpectedly. To
exercise the tutorial's 15-second scheduled smoke flow, start the stack with
`mix favn.dev --scheduler` instead. After changing `.env`, use `mix favn.reload`
for runner/runtime changes or restart the stack when changing service settings.

## Alternative configurations you can try

The default tutorial path keeps things simple (DuckDB data plane + PostgreSQL
control plane), but you can change behavior through
`examples/basic-workflow-tutorial/config/config.exs`.

### 1) Change DuckDB file location

Current config:

```elixir
config :favn,
  connections: [
    warehouse: [database: ".data/reference_workload.duckdb", write_concurrency: 1]
  ]
```

The explicit `write_concurrency: 1` keeps this local DuckDB tutorial on a
single-writer path during parallel pipeline execution.

You can point this to another path, for example:

```elixir
config :favn,
  connections: [
    warehouse: [database: "/tmp/reference_workload.duckdb", write_concurrency: 1]
  ]
```

### 2) Understand the local PostgreSQL boundary

PostgreSQL is the only supported control-plane backend. The tutorial selects the
workspace in application config:

```elixir
config :favn, :local,
  workspace_id: "local-dev"
```

The generated Compose application owns its PostgreSQL image, volume, database,
roles, and credentials. A consumer project cannot select another local
control-plane database through `config :favn`, `.env`, or
`FAVN_DATABASE_URL`. Do not add `:favn_storage_postgres` to the consumer
project's `mix.exs`.

Production database configuration is a separate deployment concern and is
supplied to the prebuilt control plane through its documented runtime
environment contract.

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

Current pipeline uses `deps(:all)` so the full graph runs from the top target,
and a `*/15 * * * * *` schedule for local scheduler smoke testing. The schedule
uses `overlap: :forbid` because the tutorial smoke should prove the local loop,
not stress-test concurrent DuckDB writes.

Scheduled local runs require `mix favn.dev --scheduler` or `scheduler: true` in
`config :favn, :local`; otherwise run the pipeline manually with `mix favn.run`.

Possible alternatives in pipeline definitions:
- use a narrower target asset for faster development loops
- change dependency mode if you want more selective execution
- add schedule settings if you want scheduled (instead of manual) runs

For beginners, keep `deps(:all)` until you are comfortable with graph behavior.

## Expected files and data

Data plane (DuckDB):
- `.data/reference_workload.duckdb`

Local Compose runtime state (when `mix favn.*` starts normally):
- `.favn/runtime.json`
- `.favn/compose/compose.yml`

## Troubleshooting

- Docker Engine or Compose is unavailable
  - start the Linux-container Docker daemon and verify `docker compose version`
- a Compose service is partial or unhealthy
  - inspect `mix favn.status` and `mix favn.logs`, then run `mix favn.stop` before retrying

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
