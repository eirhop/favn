# Configuration

This guide documents the main public `config :favn` options for authoring, local
development, SQL connections, DuckDB, ADBC, pooling, and env files.

Start with the generated config from `mix favn.init --duckdb --sample`, then edit
only what your project needs.

## Authoring Discovery

Use discovery when Favn should find assets, pipelines, schedules, and connections
from your OTP app:

```elixir
config :favn,
  discovery: [
    apps: [:my_app],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ]
```

Rules:

- `apps` must be a non-empty list of atoms.
- `assets`, `pipelines`, `schedules`, and `connections` may be `:all` when you
  want discovery for that kind.

Use explicit modules when you want a fixed list:

```elixir
config :favn,
  asset_modules: [MyApp.Lakehouse.Raw.Sales.Orders],
  pipeline_modules: [MyApp.Pipelines.DailySales],
  connection_modules: [MyApp.Connections.Warehouse]
```

Explicit module keys:

| Key | Value |
| --- | --- |
| `:asset_modules` | list of asset modules, or `:all` with discovery |
| `:pipeline_modules` | list of pipeline modules, or `:all` with discovery |
| `:schedule_modules` | list of schedule modules, or `:all` with discovery |
| `:connection_modules` | list of connection modules, or `:all` with discovery |

## Local Runtime Config

Local dev reads `config :favn, :local`. Task flags override config values.

Minimal default:

```elixir
config :favn,
  local: [
    storage: :memory
  ]
```

Common local options:

| Key | Default | Meaning |
| --- | --- | --- |
| `:storage` | `:memory` | `:memory`, `:sqlite`, or `:postgres`. `mix favn.dev --sqlite` and `--postgres` override this. |
| `:sqlite_path` | `.favn/data/orchestrator.sqlite3` | SQLite file when `storage: :sqlite`. |
| `:postgres` | local defaults | Postgres options when `storage: :postgres`. |
| `:orchestrator_api_enabled` | `true` | Keep enabled for local commands. |
| `:orchestrator_port` | `4101` | Local API port. |
| `:web_port` | `4173` | Local UI port. |
| `:orchestrator_base_url` | derived from port | Usually leave unset. |
| `:web_base_url` | derived from port | Usually leave unset. |
| `:scheduler` | `false` | Enable with config or `mix favn.dev --scheduler`. |
| `:service_token` | generated if absent | Advanced local auth override. |
| `:web_session_secret` | generated if absent | Advanced local web-session override. |

Postgres local options:

| Key | Default |
| --- | --- |
| `:hostname` | `"127.0.0.1"` |
| `:port` | `5432` |
| `:username` | `"postgres"` |
| `:password` | `"postgres"` |
| `:database` | `"favn"` |
| `:ssl` | `false` |
| `:pool_size` | `10` |

## SQL Connection Modules

A connection module names a SQL connection and says which adapter handles it.

```elixir
defmodule MyApp.Connections.Warehouse do
  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      config_schema: Favn.SQL.Adapter.DuckDB.config_schema_fields()
    }
  end
end
```

Connection definition fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `:name` | yes | Atom name used in config and `Favn.SQLClient`. |
| `:adapter` | yes | SQL adapter module. |
| `:config_schema` | yes | Accepted runtime config keys. |
| `:doc` | no | Description for tools/docs. |
| `:metadata` | no | Extra metadata map. |

Config schema field entries may use these `:type` values:

- `:string`
- `:atom`
- `:boolean`
- `:integer`
- `:float`
- `:path`
- `:module`
- `{:in, values}`
- `{:custom, fun}`

## Runtime Connection Values

Configure values under `connections`:

```elixir
config :favn,
  discovery: [apps: [:my_app], connections: :all],
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [load: [:parquet]]
    ]
  ]
```

Rules:

- Connection names must be atoms.
- Values may be keyword lists or maps with atom keys.
- Runtime connection names must match loaded connection modules.
- Unknown keys fail unless the connection schema or Favn runtime reserves them.

Reserved runtime keys:

| Key | Meaning |
| --- | --- |
| `:write_concurrency` | Advanced connection-level write admission. |
| `:admission_timeout_ms` | How long writes wait for admission. Default `30_000`; may be `:infinity`. |
| `:pool` | Session pooling options. |

## Runner Plugins

Local SQL execution needs a runner plugin for the SQL backend you use. The
generated DuckDB sample uses:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

Use `FavnDuckdbADBC` instead when using the ADBC DuckDB plugin.

## Runtime Config Refs

Use refs when a config value comes from the environment:

```elixir
config :favn,
  connections: [
    warehouse: [
      open: [database: Favn.RuntimeConfig.Ref.env!("WAREHOUSE_DB_PATH")]
    ]
  ]
```

`env!/2` options:

| Option | Default | Meaning |
| --- | --- | --- |
| `:secret?` | `false` | Marks value as secret. |
| `:required?` | `true` | Missing value should fail. |

Use `secret_env!/1` for secrets.

## DuckDB Config

Basic DuckDB config:

```elixir
config :favn,
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [load: [:parquet]]
    ]
  ]
```

`open`:

| Key | Required | Meaning |
| --- | --- | --- |
| `:database` | yes | `":memory:"` or a non-empty DuckDB file path. |

Old root keys such as `:database`, `:duckdb_bootstrap`, and root
`:write_concurrency` are rejected. Use `open: [...]` and `duckdb: [...]`.

`duckdb` groups:

| Key | Meaning |
| --- | --- |
| `:load` | DuckDB extensions to load. |
| `:settings` | DuckDB settings. |
| `:secrets` | Temporary DuckDB secrets. |
| `:attach` | Attached DuckDB or DuckLake catalogs. |
| `:use` | Attached catalog to select after attach. |

Standard DuckDB settings currently support:

| Setting | Values |
| --- | --- |
| `:azure_transport_option_type` | `:default` or `:curl` |

## DuckDB Attach Config

Attach a DuckDB file catalog:

```elixir
duckdb: [
  attach: [
    raw: [type: :duckdb, path: ".favn/data/raw.duckdb", write_concurrency: 1]
  ]
]
```

Attach a DuckLake catalog:

```elixir
duckdb: [
  secrets: [
    lakehouse_meta: [
      type: :postgres,
      host: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_HOST"),
      port: 5432,
      database: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_DATABASE"),
      user: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_USER"),
      password: Favn.RuntimeConfig.Ref.secret_env!("DUCKLAKE_POSTGRES_PASSWORD"),
      sslmode: :require
    ]
  ],
  attach: [
    lake: [
      type: :ducklake,
      metadata: "ducklake:postgres:",
      meta_secret: :lakehouse_meta,
      data_path: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_DATA_PATH"),
      write_concurrency: 1
    ]
  ],
  use: :lake
]
```

Attach `write_concurrency` values:

- `:unlimited`
- `:single`
- positive integer

Defaults:

| Attach type | Default |
| --- | --- |
| `:duckdb` | `1` |
| `:ducklake` | `:unlimited` |

For DuckLake on PostgreSQL metadata, keep `write_concurrency` conservative. One
admitted writer can use multiple PostgreSQL backend connections.

## DuckDB Secrets

Azure ADLS credential-chain secret:

```elixir
secrets: [
  azure_adls: [
    type: :azure,
    provider: :credential_chain,
    account_name: "myaccount",
    chain: [:cli, :env],
    scope: "abfss://container@myaccount.dfs.core.windows.net/path/"
  ]
]
```

Postgres secret fields:

| Field | Required | Notes |
| --- | --- | --- |
| `:type` | yes | `:postgres` |
| `:host` | yes | non-empty string |
| `:port` | yes | `1..65_535` |
| `:database` | yes | non-empty string |
| `:user` | yes | non-empty string |
| `:password` | password or auth | string or secret ref |
| `:auth` | password or auth | Azure Postgres Entra auth config |
| `:sslmode` | no | `:disable`, `:allow`, `:prefer`, `:require`, `:verify-ca`, or `:verify-full` |

`password` and `auth` are mutually exclusive.

Azure Postgres Entra auth examples:

```elixir
auth: [type: :azure_postgres_entra, provider: :managed_identity, endpoint: :auto]
auth: [type: :azure_postgres_entra, provider: :azure_cli]
```

## DuckDB ADBC Config

Use `:favn_duckdb_adbc` when a deployment needs explicit DuckDB driver control.

Global driver config:

```elixir
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.2/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
```

ADBC uses the same connection shape: `open: [...]`, `duckdb: [...]`, and
`pool: [...]`.

Runner plugin result bounds:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdbADBC,
     default_row_limit: 10_000,
     default_result_byte_limit: 20_000_000}
  ]
```

ADBC DuckDB settings include standard settings plus:

| Setting | Values |
| --- | --- |
| `:pg_pool_acquire_mode` | `:force`, `:wait`, `:try` |
| `:threads` | positive integer |
| `:pg_pool_max_connections` | non-negative integer |
| `:pg_pool_wait_timeout_millis` | non-negative integer |
| `:pg_pool_max_lifetime_millis` | non-negative integer |
| `:pg_pool_idle_timeout_millis` | non-negative integer |
| `:pg_pool_enable_thread_local_cache` | boolean |
| `:pg_pool_enable_reaper_thread` | boolean |
| `:pg_pool_health_check_query` | string |

`pg_connection_limit` is rejected. Use `pg_pool_max_connections`.

For DuckLake on Postgres, prefer:

```elixir
settings: [
  pg_pool_acquire_mode: :wait,
  pg_pool_max_connections: 5,
  pg_pool_enable_thread_local_cache: false
]
```

## Session Pooling

DuckDB sessions may be pooled. Pooling is enabled by default for poolable
connections.

Disable pooling:

```elixir
pool: [enabled: false]
```

Tune pooling:

```elixir
pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]
```

Pool options:

| Option | Default | Meaning |
| --- | --- | --- |
| `:enabled` | `true` | Enable pooling for poolable connections. |
| `:max_idle_per_key` | `1` | Idle sessions kept for one pool key. |
| `:idle_timeout_ms` | `300_000` | How long idle sessions are kept. |

Pooling is local to one runtime process. It does not increase write capacity.
Sessions are still owned by one process at a time.

## SQL Admission And Catalog Scope

Open a session with required catalogs:

```elixir
Favn.SQLClient.connect(:warehouse, required_catalogs: ["raw"])
```

Pass an explicit write target:

```elixir
Favn.SQLClient.execute(session, sql, admission: [catalog: "raw"])
Favn.SQLClient.execute(session, sql, admission: [required_catalogs: ["raw", "mart"]])
```

Favn does not parse arbitrary SQL to infer write targets. Use explicit admission
options for raw writes.

## `.env` Files

Local dev loads `.env` from the project root before compile/runtime startup.

Rules:

- Missing default `.env` is fine.
- Set `FAVN_ENV_FILE` to use another env file.
- If `FAVN_ENV_FILE` is set and missing, startup fails.
- Existing process env wins over values in the file.
- Supports `KEY=value`, optional `export`, quotes, comments, and blank lines.

## Common Config Failures

| Problem | What to do |
| --- | --- |
| Discovery finds no modules | Check `discovery[:apps]`, explicit module lists, and app compilation. |
| Connection has no provider | Add a `Favn.Connection` module or enable connection discovery. |
| Runtime connection has unknown keys | Add keys to the connection schema or fix the config spelling. |
| Missing required env var | Provide the env value or change the config ref. |
| DuckDB has old root keys | Move to `open: [...]` and `duckdb: [...]`. |
| DuckLake attach fails | Check extensions, secrets, metadata, data path, and `meta_secret`. |
| SQL timeout after a write | Treat the write result as unknown; inspect before retrying. |
