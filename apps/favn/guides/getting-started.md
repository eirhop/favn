# Getting Started With Favn

This tutorial starts Favn locally, opens the UI, and runs a sample pipeline.

You do not need to call manifest functions by hand for normal local use. Favn's
local commands build and load what they need from your DSL modules.

## Prerequisites

- An Elixir Mix project.
- The Favn monorepo or private package source available to your project.

## 1. Add Favn

For local workspace use:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"}
  ]
end
```

Use your approved package version when Favn is published to your package source.

If you also want the generated DuckDB sample, add the DuckDB plugin dependency
when prompted by `mix favn.init` or add it manually from the same checkout.

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb, path: "../favn/apps/favn_duckdb"}
  ]
end
```

## 2. Generate The Local Sample

Run this from your project root:

```bash
mix favn.init --duckdb --sample
```

This creates a small local setup with:

- a connection module
- namespace modules for a local lakehouse layout
- one raw Elixir asset
- one downstream SQL asset
- a sample pipeline, usually `MyApp.Pipelines.LocalSmoke`
- local Favn config

## 3. Check The Setup

```bash
mix favn.doctor
```

Fix any reported config or dependency issues before continuing.

## 4. Install Local Runtime Files

```bash
mix favn.install
```

This prepares Favn's local runtime workspace under `.favn/install`.

## 5. Start Favn Locally

```bash
mix favn.dev
```

The command prints local URLs. Open the UI URL, usually:

```text
http://127.0.0.1:4173
```

Keep `mix favn.dev` running.

## 6. Run The Sample Pipeline

In another terminal:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
```

If your generated pipeline has a different module name, use the name printed by
`mix favn.init`.

## 7. Inspect The Result

List recent runs:

```bash
mix favn.runs list
```

Read logs for a run:

```bash
mix favn.logs RUN_ID
```

Inspect a relation:

```bash
mix favn.inspect relation raw.sales.orders --connection important_lakehouse
```

Run a local read-only SQL query:

```bash
mix favn.query "select * from mart.sales.order_summary" --connection important_lakehouse
```

## 8. Stop Favn

```bash
mix favn.stop
```

## What Happened

You used Favn's public local commands to create a project layout, check config,
start a local runtime, run a pipeline, inspect run state, and stop the stack.

The generated files show the main authoring pieces you will use in real projects:

- `Favn.Connection` for named SQL connections
- `Favn.Namespace` for relation defaults
- `Favn.Asset` for Elixir assets
- `Favn.SQLAsset` for SQL assets
- `Favn.Pipeline` for named runs

## Common Problems

| Problem | Fix |
| --- | --- |
| `mix favn.doctor` reports missing config | Check `config/config.exs` and the generated connection modules. |
| `mix favn.install` fails | Fix the reported dependency, tool, or filesystem issue and run it again. |
| `mix favn.dev` starts but UI does not load | Check the printed web URL, then run `mix favn.status` and `mix favn.diagnostics`. |
| `mix favn.run` cannot find the pipeline | Use the generated pipeline module name printed by `mix favn.init`. |
| Query or inspect cannot choose a connection | Pass `--connection important_lakehouse` or your configured connection name. |

## Next Step

Read [Authoring Assets](authoring-assets.md) to understand and edit the generated
DSL modules. Read [Configuration](configuration.md) when you need to change local
ports, storage, connections, DuckDB, or discovery.
