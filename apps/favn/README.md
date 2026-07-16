# Favn

Favn is an Elixir package for defining data assets, running them locally, and
inspecting the result in a local UI.

An asset is one unit of data work, such as loading raw orders or building a
daily summary table. A pipeline is a named selection of assets to run together.
You write normal Elixir modules with Favn DSLs. Favn local tooling loads those
modules, starts the local runtime, and lets you run a pipeline from the terminal
or UI.

## Status

Favn is private pre-v1 software. These docs describe the intended public `:favn`
package, but names and options may still change before v1.

## Install

In a local Favn workspace or private package setup:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"}
  ]
end
```

When Favn is published to your package source, use the approved package version
instead of a path dependency.

## Start Locally

In a new or existing Mix project, generate a DuckDB sample:

```bash
mix favn.init --duckdb --sample
mix favn.doctor
mix favn.install
mix favn.dev
```

Open the printed local UI URL, usually `http://127.0.0.1:4173`.

In another terminal, run the generated sample pipeline:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.run MyApp.Source.Events:movement --window month:2026-07 \
  --dependencies none --refresh force_selected
mix favn.runs list
mix favn.stop
```

## Small DSL Example

Define one asset:

```elixir
defmodule MyApp.Assets.CustomerSnapshot do
  use Favn.Asset

  @doc "Build the customer snapshot."
  @meta owner: "analytics", tags: [:daily]
  def asset(_ctx) do
    {:ok, %{rows_written: 0}}
  end
end
```

Group assets in a pipeline:

```elixir
defmodule MyApp.Pipelines.DailyCustomers do
  use Favn.Pipeline

  pipeline :daily_customers do
    asset MyApp.Assets.CustomerSnapshot
    deps :all
  end
end
```

Most users should let `mix favn.dev` and `mix favn.run` handle the runtime flow.
Manifest functions are available for tooling and debugging, but they are not the
first thing you need to learn.

Direct asset runs default to planning all upstream dependencies with automatic
freshness. Use `--dependencies none --refresh force_selected` only for targeted
repair after independently confirming the target's upstream inputs. See the
[local development guide](guides/local-development.md) for all asset and
pipeline refresh modes.

## Use `:favn` For

- defining Elixir assets with `Favn.Asset`
- defining SQL assets with `Favn.SQLAsset`
- grouping assets with `Favn.Pipeline`
- running SQL from Elixir code with `Favn.SQLClient`
- using public local commands such as `mix favn.*`
- inspecting manifests and plans with `Favn` when building tools or debugging

Application code should depend on `:favn` and optional plugin packages only. Do
not depend on Favn runtime, storage, or UI implementation apps directly.

## Guides

- [Getting Started](guides/getting-started.md): start Favn locally and run a pipeline.
- [Authoring Assets](guides/authoring-assets.md): define assets, SQL assets, multi-assets, namespaces, pipelines, schedules, windows, and freshness.
- [Transactional SQL Asset Checks](guides/sql-asset-checks.md): validate staged candidates and published targets atomically with fail, warn, and successful no-op policies.
- [Local Development](guides/local-development.md): use `mix favn.*` commands locally.
- [Configuration](guides/configuration.md): configure discovery, local runtime, SQL connections, DuckDB, ADBC, pooling, and env files.
- [SQL Client](guides/sql-client.md): use `Favn.SQLClient` from Elixir code.
- [Adapters](guides/adapters.md): understand SQL plugins and runtime storage adapters.
- [AI Agent Development](guides/ai-agents.md): use `Favn.AI` and `mix favn.read_doc` for AI-assisted development.
- [Cheatsheet](guides/cheatsheet.cheatmd): quick lookup for DSL modules, manifest calls, and commands.
- [Advanced Manifest Notes](guides/manifest-first.md): use manifest functions for tooling and debugging.
- [Runtime Model](guides/runtime-model.md): understand runtime ownership when you need deeper context.

## Public API Pointers

- `Favn`: functions for manifests, planning, and inspection.
- `Favn.AI`: doc routing entry point for AI-assisted development.
- `Favn.Asset`: Elixir asset DSL.
- `Favn.SQLAsset`: SQL asset DSL.
- `Favn.Pipeline`: pipeline DSL.
- `Favn.Namespace`: shared relation defaults for asset modules.
- `Favn.SQLClient`: SQL client for configured Favn connections.
