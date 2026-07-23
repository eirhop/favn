# Getting Started With Favn

This tutorial scaffolds and starts Favn locally.

You do not need to call manifest functions by hand for normal local use. Favn's
local commands build and load what they need from your DSL modules.

The complete first-start path is:

```bash
mix favn.init
mix favn.install
mix favn.dev
```

## Prerequisites

- An Elixir Mix project.
- Linux amd64, or amd64 WSL2 with Linux containers.
- Docker Engine and Docker Compose v2.
- Elixir 1.20.2 on Erlang/OTP 29.
- Pull access to Favn's private GHCR control-plane package.
- The Favn monorepo or private package source available to your project.

## 1. Add Favn

Before Hex publication, check out Favn at the approved Git tag or commit, detach
the checkout, keep it clean, and use path dependencies from that checkout:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"}
  ]
end
```

Your customer-owned Docker build resolves this dependency like any other
application dependency. Use your approved package version when Favn is
published to your package source.

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

Favn maintainers who need to switch a real consumer project between this
approved checkout and an active local checkout should use the
[`FAVN_CHECKOUT` dependency switch and `mix favn.maintainer.dev`](local-development.md#testing-a-local-favn-checkout).
The normal checkout remains the default.

## 2. Scaffold Local Favn

Run this from your project root:

```bash
mix favn.init
```

This writes the documented customer-owned local Compose file under
`deploy/local/` and the customer runner Dockerfile under `deploy/runner/`.
Commit and customize both like ordinary project configuration. Favn never
overwrites a modified scaffold.

When the project declares the optional `favn_duckdb_adbc` runner plugin, request
the tested native DuckDB driver explicitly:

```bash
mix favn.init --include duckdb-adbc
```

Use `--include duckdb-adbc@VERSION` to select another version supported by the
installed Favn release.

## 3. Install The Control Plane Image

```bash
mix favn.install
```

This verifies Docker Engine, resolves the version-matched prebuilt control-plane
image to an immutable digest, and writes image-only install state under
`.favn/`. It does not compile the control plane or own the Compose file.

## 4. Optionally Check The Installed Setup

```bash
mix favn.doctor
```

Fix any reported config, dependency, image, or Compose issue before continuing.

## 5. Start Favn Locally

```bash
mix favn.dev
```

Favn generates a local runner release ID, invokes the customer-owned
`deploy/runner/Dockerfile` with Docker build cache and refreshed base images,
validates the result, builds the aligned manifest, and starts the three-service
local topology. Pass `--runner-image IMAGE` only to use an image built or pulled
outside this workflow.

The command prints local URLs. Open the UI URL, usually:

```text
http://127.0.0.1:4173
```

Keep `mix favn.dev` running.

## 6. Run A Pipeline

In another terminal:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
```

Replace the example with a pipeline declared by your project. To generate the
separate legacy DuckDB authoring sample, run
`mix favn.init --duckdb --sample`.

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
| `mix favn.dev` reports a missing scaffold file | Run `mix favn.init`, review the generated files, and retry. |
| The runner needs DuckDB ADBC | Declare `favn_duckdb_adbc`, then initialize with `mix favn.init --include duckdb-adbc`. |
| `mix favn.dev` reports a Compose contract error | Keep one labeled service for each required Favn role; extra unlabeled services are allowed. |
| `mix favn.dev` starts but UI does not load | Check the printed web URL, then run `mix favn.status` and `mix favn.diagnostics`. |
| `mix favn.run` cannot find the pipeline | Use the generated pipeline module name printed by `mix favn.init`. |
| Query or inspect cannot choose a connection | Pass `--connection important_lakehouse` or your configured connection name. |

## Next Step

Read [Authoring Assets](authoring-assets.md) to understand and edit the generated
DSL modules. Read [Configuration](configuration.md) when you need to change local
ports, storage, connections, DuckDB, or discovery.
