# Flux

Flux is an Elixir library for defining business-oriented data assets, discovering their dependencies, and orchestrating deterministic runs from those dependency relationships. It is designed as a core runtime that host applications can use to power workflow execution, inspection, and operator-facing tooling.

## Features

- **Asset DSL** via `use Flux.Assets` and `@asset` annotations on normal Elixir functions.
- **Registry** for loading configured asset modules and exposing a stable discovery surface.
- **Graph inspection** for upstream/downstream traversal and dependency-aware introspection.
- **Planner** for deriving an executable order from a target asset and its transitive dependencies.
- **Runner** for executing planned asset steps for a run target.
- **Run store** for recording run metadata and statuses.
- **Run events** for publishing run lifecycle updates to subscribers.

## Quickstart

Define an asset module:

```elixir
defmodule MyApp.SalesAssets do
  use Flux.Assets

  @asset true
  def extract_orders(_ctx, _deps) do
    {:ok, %Flux.Asset.Output{output: [%{id: 1, total: 100}]}}
  end

  @asset depends_on: [:extract_orders]
  def build_daily_report(_ctx, deps) do
    orders = Map.fetch!(deps, {__MODULE__, :extract_orders})
    {:ok, %Flux.Asset.Output{output: %{count: length(orders)}}}
  end
end
```

Register the module and run a target asset:

```elixir
# config/config.exs
import Config

config :flux,
  asset_modules: [MyApp.SalesAssets]
```

```elixir
# from your app runtime / iex
{:ok, run} = Flux.run({MyApp.SalesAssets, :build_daily_report}, dependencies: :all)
```

## Configuration

Flux is configured through the `:flux` application environment:

```elixir
import Config

config :flux,
  asset_modules: [MyApp.SalesAssets],
  pubsub_name: MyApp.PubSub,
  run_store: Flux.RunStore.Memory,
  run_store_opts: []
```

Key settings:

- `asset_modules`: modules that define assets with `use Flux.Assets`.
- `:pubsub_name`: PubSub server name used for run event broadcasting.
- `:run_store`: run store implementation module.
- `:run_store_opts`: options passed to the configured run store.

## Current limitations

- The default run store is node-local in-memory storage.
- Execution is currently synchronous.
- Run events are best-effort pubsub notifications.

## Roadmap and release focus

Current development is focused on stabilizing the core API and runtime behavior:

- Harden the public API for asset inspection and run orchestration.
- Improve planner/runner ergonomics and error reporting.
- Add stronger durability and query capabilities for run storage.
- Expand event delivery semantics and observability integrations.
- Prepare Hex packaging and versioned release practices for broader adoption.

## Installation

Flux is not published on Hex yet.

Install it from the GitHub repository by adding `flux` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:flux, git: "https://github.com/eirhop/flux.git", branch: "main"}
  ]
end
```

Once Flux is published on Hex, the dependency can move to a normal versioned
package declaration:

```elixir
def deps do
  [
    {:flux, "~> 0.1.0"}
  ]
end
```
