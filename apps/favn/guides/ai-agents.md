# AI Agent Development

Favn exposes compiled documentation so agents can discover the supported public
API without reading runtime internals. Start every unfamiliar Favn task with:

```bash
mix favn.read_doc Favn.AI
```

Follow its module pointers with `mix favn.read_doc ModuleName` or
`mix favn.read_doc ModuleName function_name`.

## Task routing

| Task | Read next |
| --- | --- |
| Assets, pipelines, namespaces, windows, or freshness | `Favn.Asset`, `Favn.Pipeline`, `Favn.Namespace`, then [Authoring Assets](authoring-assets.md) |
| Local commands and runtime | [Local Development](local-development.md), then `mix help favn.dev` or the relevant `mix favn.*` task |
| SQL assets | `Favn.SQLAsset`, then the relevant SQL guide |
| Output schema and evolution | `Favn.SQLAsset contract`, `Favn.SQL.Contract`, then [SQL Output Contracts](sql-output-contracts.md) |
| Transactional checks | `Favn.SQLAsset check`, `Favn.SQL.CheckResult`, then [SQL Asset Checks](sql-asset-checks.md) |
| Runtime inputs | `Favn.SQLAsset.RuntimeInputs` and its result/error modules, then [SQL Runtime Inputs](sql-runtime-inputs.md) |
| Connections, pooling, or DuckDB setup | `Favn.Connection`, then [Configuration](configuration.md) and [DuckDB Session Scripts](duckdb-session-scripts.md) |
| SQL from Elixir | `Favn.SQLClient`, then [SQL Client](sql-client.md) |
| Runner-local supervised state | `Favn.Runner.Plugin`, then [Runner Plugins](runner-plugins.md) |
| Manifest tooling or debugging | `Favn`, then [Manifest-First](manifest-first.md) |

## Public contract guardrails

- Consumer code depends on `:favn` and supported plugins, not orchestrator,
  runner, storage, SQL-runtime, or UI implementation apps.
- Use documented DSL modules and `mix favn.*` commands before considering internals.
- `settings` are static authoring values, submitted inputs are `ctx.params`, and
  environment-dependent values or secrets use `runtime_config`.
- SQL identifiers and lineage are explicit. Do not infer a schema DSL or invent
  structured DuckDB extension, secret, setting, or attach options.
- SQL runtime-input resolvers implement `Favn.SQLAsset.RuntimeInputs`; configuration
  does not become SQL parameters automatically.
- Runner plugins are runner-local and disposable. Never use them for durable
  business state or correctness-sensitive cross-run messaging.

Use the linked guide when exact options, lifecycle, limits, redaction, retries,
or failure outcomes matter. Keep project-specific conventions in the consumer
project's own agent instructions.

## Consumer agent prompt

```md
## Favn

Favn defines business-oriented assets and pipelines in Elixir and compiles them
into a manifest. Before guessing about its APIs, run
`mix favn.read_doc Favn.AI` and follow the documented module and guide pointers.
Use public `:favn` APIs and supported plugins, not runtime implementation apps.
```

Repository contributors should follow the root `AGENTS.md`. It contains internal
boundaries and verification rules that do not belong in consumer documentation.
