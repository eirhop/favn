# Local Development Commands

Favn exposes local development through `mix favn.*` tasks.

Use these commands from your application project. Do not depend on Favn runtime,
storage, or UI implementation apps directly.

## First Local Flow

```bash
mix favn.init --duckdb --sample
mix favn.doctor
mix favn.install
mix favn.dev
```

Open the printed UI URL, usually `http://127.0.0.1:4173`.

In another terminal:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.runs list
mix favn.logs RUN_ID
mix favn.stop
```

## Setup Commands

| Command | Use it for | Common options |
| --- | --- | --- |
| `mix favn.init --duckdb --sample` | Generate local Favn files and a sample DuckDB pipeline. | `--duckdb`, `--sample` |
| `mix favn.doctor` | Check local configuration before running. | `--skip-compile` |
| `mix favn.install` | Prepare `.favn/install`. | `--force`, `--skip-runtime-deps-install`, `--root-dir PATH` |
| `mix favn.dev` | Start PostgreSQL-backed local runtime processes and the UI. | `--scheduler`, `--root-dir PATH` |

`mix favn.dev` stays in the foreground. It prints service logs and URLs. Stop it
with `mix favn.stop` from another terminal or by ending the foreground process.
Startup compiles only stale runtime and project modules. A clean Git runtime
source uses Git tree metadata for install validation; dirty or non-Git sources
fall back to content hashing. Use `mix favn.install --force` when you explicitly
need a clean runtime workspace rebuild.
Runner and operator processes reuse that compiled runtime with `--no-compile`;
they do not serialize startup on Mix's build lock.

Before startup, `mix favn.dev` loads the project `.env` and starts a fresh Mix
process that evaluates `config/runtime.exs`. This means runtime config can branch
on env-file values. Existing shell values take precedence. `mix favn.reload`
repeats the same bootstrap, so changes to `.env` or `config/runtime.exs` are
picked up together.

## Runtime Commands

### Run Work

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.run MyApp.Pipelines.LocalSmoke --no-wait
mix favn.run MyApp.Assets.MonthlyOrders --window month:2026-01
mix favn.run MyApp.Source.Events:movement --window month:2026-07 \
  --dependencies none --refresh force_selected
```

Options:

| Option | Meaning |
| --- | --- |
| `--window VALUE` | Window request such as `month:2026-01`. |
| `--timezone TZ` | Timezone for `--window`. |
| `--dependencies all\|none` | Asset planning scope. Defaults to `all`. Asset-only. |
| `--refresh MODE` | Refresh behavior. Defaults to `auto`. |
| `--idempotency-key KEY` | Reuse key for safe command retry. |
| `--wait` / `--no-wait` | Wait for completion or return after submission. Wait is default. |
| `--timeout-ms N` | Alias for wait/run timeout when specific values are absent. |
| `--wait-timeout-ms N` | Local polling timeout. Default is 60 seconds. |
| `--run-timeout-ms N` | Runtime execution timeout. |
| `--poll-interval-ms N` | Poll interval. Default is 1 second. |

Asset refresh modes are `auto`, `missing`, `force_selected`,
`force_selected_upstream`, and `force_all`. Pipeline targets do not accept
`--dependencies` and support only `auto`, `missing`, and `force_all` refresh.
`force_selected_upstream` requires dependency scope `all`.

Use `--dependencies none --refresh force_selected` for a targeted repair only
after confirming the selected asset's upstream inputs are suitable. Dependency
scope chooses which nodes are planned; refresh chooses how freshness is applied
inside that plan. Omitting both options keeps the safe `all` plus `auto`
defaults.

### List, Show, And Cancel Runs

```bash
mix favn.runs list
mix favn.runs list --status error --limit 20
mix favn.runs show RUN_ID
mix favn.runs cancel RUN_ID --wait
```

List options: `--status`, `--limit`, `--root-dir`.

Cancel options: `--wait`, `--timeout-ms`, `--wait-timeout-ms`,
`--poll-interval-ms`, `--root-dir`.

### Read Logs

Service logs:

```bash
mix favn.logs
mix favn.logs --service runner --tail 200
mix favn.logs --service all --follow
```

Run events:

```bash
mix favn.logs RUN_ID
mix favn.logs RUN_ID --tail 200
```

Options:

| Option | Meaning |
| --- | --- |
| `--service operator|web|orchestrator|runner|all` | Select service logs. |
| `--tail N` | Number of lines or events. Default is 100. |
| `--follow` | Follow service logs. Cannot be used with `RUN_ID`. |

### Inspect Relations And Query SQL

```bash
mix favn.inspect relation raw.sales.orders --connection important_lakehouse
mix favn.inspect partitions raw.sales.orders --connection important_lakehouse
mix favn.query "select * from mart.sales.order_summary" --connection important_lakehouse
```

`mix favn.inspect` relation forms:

- `name`
- `schema.name`
- `catalog.schema.name`

`mix favn.query` options:

| Option | Meaning |
| --- | --- |
| `--connection NAME` | Required when more than one SQL connection exists. |
| `--limit N` | Display row limit. Default is 50. |
| `--allow-write` | Allow local mutation. Without it, Favn applies a best-effort read-only guardrail. |

The read-only guardrail is not a security sandbox. It is a local safety check.

## Status, Diagnostics, Reload, Stop, Reset

```bash
mix favn.status
mix favn.diagnostics
mix favn.diagnostics --json
mix favn.reload
mix favn.stop
mix favn.reset
```

| Command | Use it for |
| --- | --- |
| `mix favn.status` | Show whether the local stack is running, stopped, partial, stale, or unknown. |
| `mix favn.diagnostics` | Show storage, scheduler, runner, and recent failure checks. |
| `mix favn.reload` | Rebuild and reload authored modules into a running local stack. Blocks when runs are in flight. |
| `mix favn.stop` | Stop the local stack. |
| `mix favn.reset` | Delete local `.favn/` install/build/runtime artifacts. Use with care. |

## Backfills

Backfills are advanced local/operator workflows.

```bash
mix favn.backfill submit MyApp.Pipelines.Daily --from 2026-04-01 --to 2026-04-07 --kind day
mix favn.backfill submit MyApp.Pipelines.Daily --window day:2026-04-01..2026-04-07 --dry-run
mix favn.backfill windows RUN_ID
mix favn.backfill rerun-window RUN_ID --window-key day:2026-04-01
mix favn.backfill repair --all --apply
```

Common submit options:

| Option | Meaning |
| --- | --- |
| `--from`, `--to`, `--kind` | Explicit range input. |
| `--window KIND:FROM..TO` | Compact range input. Cannot be combined with `--from`, `--to`, or `--kind`. |
| `--timezone TZ` | Defaults to `Etc/UTC`. |
| `--dry-run` | Plan without creating runs. |
| `--refresh force` | Recompute selected windows. |
| `--wait` / `--no-wait` | Wait is default. |

## Packaging Commands

These are deployment/operator commands, not needed for the first local run:

| Command | Use it for |
| --- | --- |
| `mix favn.build.runner` | Build runner artifact metadata/output. |
| `mix favn.build.web` | Build web artifact metadata/output. |
| `mix favn.build.orchestrator` | Build orchestrator artifact metadata/output. |
| `mix favn.build.single` | Build the single-node local deployment artifact. |
| `mix favn.bootstrap.single` | Bootstrap a single-node backend from a manifest JSON file. |

## What You Should See In The UI

After `mix favn.dev`, the UI should open at the printed web URL. It should show
the local Favn operator view. After `mix favn.run`, you should see recent run
state move from pending/running to a final state such as ok or error.

Do not rely on UI loading state as final truth. Use `mix favn.runs`,
`mix favn.logs`, and `mix favn.diagnostics` when debugging.
