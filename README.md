# Flux
**State:** Not production ready, API and DSL may change. 

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
  storage_adapter: Flux.Storage.Adapter.Memory,
  storage_adapter_opts: []
```

Key settings:

- `asset_modules`: modules that define assets with `use Flux.Assets`.
- `:pubsub_name`: PubSub server name used for run event broadcasting.
- `:storage_adapter`: run storage adapter module.
- `:storage_adapter_opts`: options passed to the configured storage adapter.

## Current limitations

- The default run store is node-local in-memory storage.
- Run execution is synchronous within a single BEAM node.
- Run events are best-effort pubsub notifications.

## Runtime behavior in this release

- **Planning and execution**: Flux plans dependency-aware runs with deterministic topological stages and executes each stage in deterministic ref order.
- **Run lifecycle**: each run transitions through `:running` to terminal `:ok` or `:error`, recording timestamps, per-asset results, outputs, and terminal error details.
- **Storage facade contract**: run retrieval/listing APIs normalize storage failures to one of:
  - `:not_found`
  - `:invalid_opts`
  - `{:store_error, reason}`
- **Event delivery**: run events are published as best-effort observability signals and do not affect run correctness.

## Guarantees in this release

- **Run lifecycle semantics**
  - `Flux.run/2` returns `{:ok, %Flux.Run{status: :ok}}` on success or `{:error, %Flux.Run{status: :error}}` on execution failure.
  - Failed runs preserve structured failure context in both `run.error` and `run.asset_results[ref].error`.
  - `Flux.get_run/1` returns the latest stored run state for an ID.
- **Storage error contract**
  - `Flux.get_run/1` and `Flux.list_runs/1` return storage errors only as `:not_found`, `:invalid_opts`, or `{:store_error, reason}`.
  - Adapter-specific raw errors are wrapped as `{:store_error, reason}`.
- **Event delivery scope**
  - `Flux.subscribe_run/1` and `Flux.unsubscribe_run/1` manage PubSub subscriptions for run topics.
  - Event delivery is best-effort; missing subscribers or publish failures do not change run success/failure outcomes.

## Not guaranteed yet / non-goals

- Durable distributed execution guarantees across nodes.
- Exactly-once event delivery, replay, or durable event logs.
- Persistent storage guarantees beyond the configured adapter behavior (default adapter is in-memory and node-local).
- Asynchronous or parallel asset execution beyond the current deterministic stage-by-stage runtime model.

## Roadmap and release focus

- Add durable production-ready storage adapters with stronger operational guarantees.
- Expand run query capabilities for richer operator UIs.
- Improve event observability integrations (telemetry/export pipelines).
- Add release packaging/versioning via Hex.

## Installation

Flux is not published on Hex yet.

Install it from the GitHub repository by adding `flux` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:flux, git: "https://github.com/eirhop/flux.git", tag: "v0.1.0"}
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
