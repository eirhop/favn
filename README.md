<div align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="300" />
  <p><strong>Asset-first ETL/ELT orchestration for Elixir</strong></p>
  <p>Define assets, declare lineage, and run deterministic graphs with orchestration config passed via <code>ctx</code>.</p>
</div>

<p align="center">
  <strong>Status:</strong> Pre-v1 and under active refactor. API and DSL may change.
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> •
  <a href="#in-scope-for-v1">In scope for v1</a> •
  <a href="#out-of-scope-for-v1">Out of scope for v1</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#roadmap">Roadmap</a> •
  <a href="#installation">Installation</a> • 
  <a href="/FEATURES.md">Features</a> • 
  <a href="/lib/favn.ex">Docs</a>
</p>

## Favn at a glance

- Asset-first execution model for ETL/ELT workloads.
- Canonical authoring DSL: `@asset`, `@depends`, `@uses`, `@freshness`, `@meta`.
- Canonical runtime contract: `def asset(ctx)`.
- Dependencies represent ordering/lineage; assets materialize externally.
- Orchestration config stays outside function attributes and is accessed through `ctx`.

## In scope for v1

- Stable asset DSL and single-argument asset contract.
- Deterministic dependency-based planning and execution.
- Retry/timeout/cancellation/rerun lifecycle support.
- Freshness-aware rerun/skip decisions.
- Graph/run/lineage visibility for operators.
- v1 orchestration triggers for `schedule`, `polling`, and `polling cursor/state`.

## Out of scope for v1

- Broad non-asset automation platforms beyond ETL/ELT asset orchestration.
- Multi-domain orchestration messaging outside the asset-first product scope.
- Function-attribute trigger DSL (`schedule`/`polling` in asset attributes).
- In-memory data passing contract between assets.

## Quickstart

Define an asset module with the canonical DSL and `def asset(ctx)` contract:

```elixir
defmodule MyApp.SalesAssets do
  use Favn.Assets

  @asset :extract_orders
  @uses [:warehouse_api]
  @freshness [max_age: :timer.hours(1)]
  @meta [owner: "data-platform", domain: :sales]
  def extract_orders(ctx) do
    source = ctx.config[:source] || :warehouse_api
    _ = source

    # Materialize externally (for example to object storage or warehouse table)
    :ok
  end

  @asset :daily_sales_report
  @depends [:extract_orders]
  @uses [:warehouse]
  @freshness [max_age: :timer.hours(2)]
  @meta [owner: "finance-analytics", tier: :gold]
  def daily_sales_report(ctx) do
    report_date = ctx.runtime[:date]
    _ = report_date

    # Read upstream materializations, compute report, write externally
    :ok
  end
end
```

Register modules and run a target asset:

```elixir
# config/config.exs
import Config

config :favn,
  asset_modules: [MyApp.SalesAssets]
```

```elixir
# from your app runtime / iex
{:ok, run_id} = Favn.run({MyApp.SalesAssets, :daily_sales_report})
{:ok, run} = Favn.await_run(run_id)
```

## Configuration

Favn is configured through the `:favn` application environment:

```elixir
import Config

config :favn,
  asset_modules: [MyApp.SalesAssets],
  pubsub_name: MyApp.PubSub,
  storage_adapter: Favn.Storage.Adapter.Memory,
  storage_adapter_opts: []
```

Key settings:

- `asset_modules`: modules that define assets with `use Favn.Assets`.
- `:pubsub_name`: PubSub server name used for run event broadcasting.
- `:storage_adapter`: run storage adapter module.
- `:storage_adapter_opts`: options passed to the configured storage adapter.

## Roadmap

`FEATURES.md` is the canonical roadmap and milestone source of truth: [FEATURES.md](/FEATURES.md).

## Installation

Favn is not published on Hex yet.

Install from GitHub in `mix.exs`:

```elixir
def deps do
  [
    {:favn, git: "https://github.com/eirhop/favn.git", tag: "v0.1.1"}
  ]
end
```
