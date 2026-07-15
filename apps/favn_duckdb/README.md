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
- `favn_duckdb -> favn_sql_runtime`

External dependency:

- `duckdbex` for DuckDB client integration

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

- physical-session setup uses trusted native SQL files configured as
  `duckdb.startup` and named `duckdb.resources`; SQL assets select stable names
  with `@resources`, while `duckdb.catalogs` contains only Favn resource and
  write-admission metadata
- the structured `load`, `settings`, `secrets`, `attach`, and `use` config forms
  are removed; see `apps/favn/guides/duckdb-session-scripts.md`
- environment-backed credentials resolve at runner startup; use a native
  refresh-capable provider or restart the runner after rotation because idle
  timeout does not impose a maximum physical-session age
- placement is runtime/plugin config only (not manifest or DSL)
- separate-process mode uses one long-lived worker (no pooling/autoscaling in Phase 7)
- separate-process worker unavailability and worker-call timeouts are normalized
  into structured SQL diagnostics by the DuckDB adapter; worker-call timeouts are
  non-retryable because the operation outcome is unknown
- bulk table writes stay appender-oriented in DuckDB paths
- appender close semantics are explicit: successful close consumes the worker
  handle, while failed close keeps it available for retry or explicit release;
  adapter-owned materialization releases after failed closes
