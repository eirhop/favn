# `apps/favn_duckdb`

Runner-side DuckDB execution plugin for Phase 7.

## Purpose

- own DuckDB adapter/runtime code outside `favn_runner`
- provide one plugin boundary through `FavnRunner.Plugin`
- support exactly two runtime placements:
  - `:in_process`
  - `:separate_process`

## Dependency boundary

Allowed umbrella dependency direction:

- `favn_duckdb -> favn_runner`

Must not depend on:

- `favn`
- `favn_orchestrator`
- storage adapters

## Runner plugin configuration

Configure through generic runner plugin config:

```elixir
config :favn, :runner_plugins, [
  {FavnDuckdb, execution_mode: :in_process}
]
```

`execution_mode` accepts exactly:

- `:in_process` (default)
- `:separate_process`

Separate-process mode options:

- `worker_name` (default: `FavnDuckdb.Worker`)
- `worker_call_timeout` (default: `:infinity`)

Example:

```elixir
config :favn, :runner_plugins, [
  {
    FavnDuckdb,
    execution_mode: :separate_process,
    worker_name: FavnDuckdb.Worker,
    worker_call_timeout: 30_000
  }
]
```

## Notes

- placement is runtime/plugin config only (not manifest or DSL)
- separate-process mode uses one long-lived worker (no pooling/autoscaling in Phase 7)
- bulk table writes stay appender-oriented in DuckDB paths
