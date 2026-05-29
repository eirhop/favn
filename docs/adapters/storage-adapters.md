# Storage Adapters

Reader: adapter authors and Favn contributors working on orchestrator
persistence.

Documentation type: reference.

Storage adapters save the orchestrator's control-plane state. They are internal
Favn infrastructure, not user-facing APIs.

Do not use storage adapters for DuckDB asset tables, warehouse data, source
credentials, SQL sessions, or UI state. Public application code should use the
public `:favn` package, `Favn.SQLClient`, SQL assets, and supported runtime
configuration instead.

## Storage Adapter Or SQL Execution Adapter

Favn has two adapter categories that are easy to confuse:

| Adapter kind | What it does | Examples | Who uses it |
| --- | --- | --- | --- |
| Storage adapter | Saves orchestrator state such as manifests, runs, schedules, logs, auth state, and idempotency records. | SQLite storage, Postgres storage | The orchestrator runtime. |
| SQL execution adapter | Opens SQL backend sessions and runs SQL for assets or `Favn.SQLClient`. | DuckDB, DuckDB ADBC | The SQL runtime and runner. |

Storage adapters do not execute user SQL. SQL execution adapters do not decide
which manifest is active, when schedules fire, or how run state is stored.

## What Storage Adapters Own

Storage adapters own the database implementation for orchestrator state:

- Schema setup and migration readiness checks.
- Saving and reading manifest versions.
- Saving active manifest selection.
- Saving run snapshots and run events.
- Saving scheduler cursors and runtime settings.
- Saving operator read models such as logs, execution groups, freshness, target
  status, and backfill views.
- Saving admission, materialization-claim, auth, session, audit, and idempotency
  records.
- Bounded reads for operator pages and runtime decisions.

Storage adapters do not own:

- Scheduling rules.
- Run lifecycle decisions.
- Runner execution.
- SQL backend sessions.
- DuckDB or DuckLake catalogs.
- User asset data.
- Operator UI state.

The orchestrator decides lifecycle behavior and calls storage when it needs to
save or read control-plane state.

## SQLite Storage

SQLite storage is for local development and single-node durable control-plane
storage.

| Option | Required | Description |
| --- | --- | --- |
| `database` | yes | SQLite database path. Production paths should be absolute and outside build artifacts. |
| `pool_size` | no | Ecto repo pool size. |
| `busy_timeout` | no | How long SQLite waits for a busy database before failing. |
| `migration_mode` | no | `:auto` or `:manual`; default is `:auto`. |
| `initialize_empty?` | no | Allows initialization when manual migration mode sees an empty database. |
| `require_absolute_path` | no | Requires absolute database paths when enabled. |

Readiness checks report states such as empty database, ready, missing schema,
upgrade required, schema newer than this release, or inconsistent schema.

Do not run multiple backend nodes against one SQLite database. Do not place the
database on NFS, SMB, object-storage mounts, or distributed filesystems.

## Postgres Storage

Postgres storage is for deployments that need a PostgreSQL-backed
control-plane database.

| Option | Required | Description |
| --- | --- | --- |
| `repo_mode` | no | `:managed` or `:external`; default is `:managed`. |
| `repo_config` | managed mode | Ecto repo config. Requires `:hostname`, `:database`, `:username`, and `:password`. |
| `repo` | external mode | Existing Ecto Postgres repo module. |
| `migration_mode` | no | `:manual` or `:auto` in managed mode. External mode requires `:manual`. |

Managed mode starts a Favn-owned repo from `repo_config`.

External mode uses a repo already started by the host application. In external
mode, the host application must run migrations before it accepts orchestrator
traffic.

## Read Models And Repair

Manifest rows, run snapshots, and run events are the core saved state.

Some tables are read models. They exist so operator pages and runtime reads can
stay bounded. Examples include execution-group summaries, target statuses,
freshness views, and backfill views.

When a projector supports it, the orchestrator may repair or rebuild a read
model. The storage adapter provides the reads and writes. The orchestrator owns
the decision to repair and the lifecycle rules around that repair.

## Failure Modes

| Area | Symptom | What to do |
| --- | --- | --- |
| SQLite config | Missing database path, invalid path rule, or invalid migration mode. | Fix adapter config before starting the runtime. |
| SQLite readiness | Schema is missing, too old, too new, inconsistent, or the database is empty when that is not allowed. | Run the configured migration path or restore a compatible backup. |
| SQLite placement | Locking errors, slow writes, or corrupt behavior on shared filesystems. | Use a local disk and one backend node. Use Postgres for multi-node deployments. |
| Postgres config | Invalid repo mode, missing repo config fields, or invalid migration mode. | Fix repo configuration and migration ownership. |
| Postgres readiness | Schema is not ready. | Apply migrations before accepting orchestrator traffic. |
| Storage conflict | A manifest, run event, materialization claim, or cursor update loses a guarded write. | Let the orchestrator command or lifecycle code decide whether to retry, repair, or reject. |

## Contributor Checklist

Before changing storage adapter docs or behavior, check:

- Is this about saving orchestrator state? If not, it may belong to a SQL
  execution adapter, the runner, or the public `:favn` guide instead.
- Does the text make clear that storage adapters are internal infrastructure?
- Does the text avoid telling user code to call storage adapters directly?
- Does the text leave scheduling, run lifecycle, admission decisions, and repair
  decisions with the orchestrator?
- Does the text keep DuckDB, DuckLake, and SQL session behavior with SQL
  execution adapters and `Favn.SQLClient`?
- Are failure modes described in terms an operator or contributor can act on?

## Related Structure Docs

- `docs/structure/favn_storage_sqlite.md`
- `docs/structure/favn_storage_sqlite_database.md`
- `docs/structure/favn_storage_postgres.md`
- `docs/structure/favn_sql_runtime.md`
- `docs/structure/favn_duckdb.md`
- `docs/structure/favn_duckdb_adbc.md`
