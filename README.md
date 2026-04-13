<div align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="300" />
  <p><strong>Asset-first orchestration for Elixir</strong></p>
  <p>Define assets close to the business logic. Let Favn discover dependencies, plan runs, and execute deterministic workflows.</p>
</div>

## What Favn Is

Favn is an Elixir library for defining, inspecting, and orchestrating data assets.
It favors normal Elixir modules, explicit metadata, and dependency-driven execution over large framework-specific abstractions.

## Status

Favn `v0.4.0` is complete.

- Private development project
- Breaking changes are still allowed before `v1.0`
- SQL assets, connections, schedules, pipelines, and windowing are available

## Choose Your DSL

- `Favn.Asset`: preferred single-asset Elixir DSL
- `Favn.SQLAsset`: preferred single-asset SQL DSL
- `Favn.MultiAsset`: repetitive extraction assets with shared runtime logic
- `Favn.Assets`: compact multi-asset function DSL; still supported, but use `Favn.Asset` for new single-asset modules

## Quickstart

```elixir
defmodule MyApp.Raw.Sales.Orders do
  use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
  use Favn.Asset

  @doc "Extract raw orders"
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  @relation true
  def asset(ctx) do
    _target = ctx.asset.relation
    :ok
  end
end
```

```elixir
# config/config.exs
import Config

config :favn,
  asset_modules: [MyApp.Raw.Sales.Orders]
```

```elixir
Favn.list_assets()
{:ok, run_id} = Favn.run_asset({MyApp.Raw.Sales.Orders, :asset})
{:ok, run} = Favn.await_run(run_id)

# Operator-facing connection inspection is redacted:
[%Favn.Connection.Info{} = conn] = Favn.list_connections()
{:ok, %Favn.Connection.Info{} = warehouse} = Favn.get_connection(:warehouse)
```

## Install And Configure

Favn is currently installed from Git:

```elixir
defp deps do
  [
    {:favn, git: "https://github.com/eirhop/favn.git", tag: "v0.4.0"}
  ]
end
```

Minimal config:

```elixir
import Config

config :favn,
  asset_modules: [MyApp.Raw.Sales.Orders]
```

Add `pipeline_modules`, `connections`, `scheduler`, `storage_adapter`, and `storage_adapter_opts` as your app grows.

## AI Agent Start Point

Put this in `AGENTS.md`:

```text
When working in this repository, always start by reading the moduledoc for Favn.AgentGuide.
```

## Read Next

- `Favn`: public runtime and operator API facade
- `Favn.AgentGuide`: routing guide for AI agents and new contributors
- `Favn.Asset`: single-asset Elixir authoring
- `Favn.SQLAsset`: single-asset SQL authoring
- `Favn.MultiAsset`: repetitive extraction authoring
- `Favn.Pipeline`: pipeline composition
- `docs/FEATURES.md`: roadmap and feature status
