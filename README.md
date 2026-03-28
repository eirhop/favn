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
  <a href="/FEATURES.md">Features</a> • 
  <a href="/lib/favn.ex">Docs</a>

</p>

## Favn at a glance

- Plain Elixir functions become assets with metadata and dependencies.
- Dependency graphs are discovered automatically from asset definitions.
- Runs are planned deterministically and executed in dependency order.
- Runtime state, outputs, and run events are exposed through a small public API.

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
  def extract_orders(_ctx, _deps) do
    {:ok, %Favn.Asset.Output{output: [%{id: 1, total: 100}]}}
  end

  @asset depends_on: [:extract_orders]
  def build_daily_report(_ctx, deps) do
    orders = Map.fetch!(deps, {__MODULE__, :extract_orders})
    {:ok, %Favn.Asset.Output{output: %{count: length(orders)}}}
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
{:ok, run} = Favn.run({MyApp.SalesAssets, :build_daily_report}, dependencies: :all)
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

## Current limitations

- The default run store is node-local in-memory storage.
- Public run execution is currently synchronous (`Favn.run/2`) even though the
  runtime now uses an internal coordinator process.
- Run events are best-effort pubsub notifications.

## Runtime behavior in this release

- **Planning and orchestration**: Favn plans dependency-aware runs with deterministic topological stages and orchestrates execution through a run-scoped coordinator.
- **Run lifecycle**: runtime orchestration now uses explicit internal run and step state machines; the public `%Favn.Run{}` projection remains compatible with `:running | :ok | :error`.
- **Execution boundary**: one-step asset invocation is isolated behind a runtime executor boundary.
- **Storage facade contract**: run retrieval/listing APIs normalize storage failures to one of:
  - `:not_found`
  - `:invalid_opts`
  - `{:store_error, reason}`
- **Checkpoint persistence policy**: runtime checkpoints are required; if snapshot persistence fails, `Favn.run/2` returns `{:error, {:storage_persist_failed, reason}}`.
- **Event delivery**: run events are published as best-effort observability signals and do not affect run correctness.

## Guarantees in this release

- **Run lifecycle semantics**
  - `Favn.run/2` returns `{:ok, %Favn.Run{status: :ok}}` on success or `{:error, %Favn.Run{status: :error}}` on execution failure.
  - Failed runs preserve structured failure context in both `run.error` and `run.asset_results[ref].error`.
  - `Favn.get_run/1` returns the latest stored run state for an ID.
- **Storage error contract**
  - `Favn.get_run/1` and `Favn.list_runs/1` return storage errors only as `:not_found`, `:invalid_opts`, or `{:store_error, reason}`.
  - Adapter-specific raw errors are wrapped as `{:store_error, reason}`.
- **Event delivery scope**
  - `Favn.subscribe_run/1` and `Favn.unsubscribe_run/1` manage PubSub subscriptions for run topics.
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

Favn is not published on Hex yet.

Install it from the GitHub repository by adding `favn` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:favn, git: "https://github.com/eirhop/favn.git", tag: "v0.1.1"}
  ]
end
```

Once Favn is published on Hex, the dependency can move to a normal versioned
package declaration:

```elixir
def deps do
  [
    {:favn, "~> 0.1.1"}
  ]
end
```
