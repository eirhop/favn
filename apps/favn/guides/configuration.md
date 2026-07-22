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
`mix favn.dev`, `mix favn.reload`, `mix favn.inspect`, and `mix favn.query` first
load the project's `.env`, then evaluate the consumer project's
`config/runtime.exs` in a fresh Mix process. Dev collects connection,
execution-pool, and plugin configuration for the local runner.
Environment-specific values from `.env` may therefore be read in runtime config.

Minimal local configuration:

```elixir
config :favn,
  local: [
    workspace_id: "local-dev",
    scheduler: false,
    compose_file: "deploy/compose.local.yml"
  ]
```

Common local options:

| Key | Default | Meaning |
| --- | --- | --- |
| `:workspace_id` | `"local-dev"` | Workspace selected by local CLI/UI requests. |
| `:orchestrator_port` | `4101` | Local API port. |
| `:web_port` | `4173` | Local UI port. |
| `:scheduler` | `false` | Enable with config or `mix favn.dev --scheduler`. |
| `:compose_file` | `"deploy/compose.local.yml"` | Project-relative consumer-owned Compose file. `mix favn.dev --compose-file PATH` overrides it. |

Compose selection is explicit and deterministic: the command-line
`--compose-file` value wins over `config :favn, :local`, which wins over the
default above. The path must be a regular non-symlink file inside the project.
Run `mix favn.init --target compose` to create the default starting template.

The local tooling generates PostgreSQL credentials, service authentication,
the View session secret, and the distribution cookie into owner-only files
under `.favn/`. PostgreSQL and all control-plane storage configuration belong
to the selected consumer Compose application; a customer does not supply a host
database URL or select a storage adapter through Favn config.

The generated DuckDB sample uses runtime path references instead of embedding a
host path in the manifest:

| Variable | Host default | Local runner value |
| --- | --- | --- |
| `FAVN_LOCAL_SAMPLE_DATABASE_PATH` | `.favn/data/local_smoke.duckdb` | `/var/lib/favn/data/local_smoke.duckdb` |
| `FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH` | `.favn/data/raw.duckdb` | `/var/lib/favn/data/raw.duckdb` |
| `FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH` | `.favn/data/mart.duckdb` | `/var/lib/favn/data/mart.duckdb` |

An existing shell value wins over `.env`; an `.env` value wins over these host
defaults. The local Compose service supplies the container paths. If you change
the runner data mount, update those service environment values together.

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
      circuit_breaker: [failure_threshold: 5, probe_after_ms: :timer.minutes(1)],
      duckdb: [startup: [file: {:priv, :my_app, "duckdb/startup.sql"}]]
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
| `:circuit_breaker` | Durable connection admission after consecutive resource failures. Requires positive `failure_threshold` and `probe_after_ms`. |

Execution pools accept the same policy alongside their concurrency limit:

```elixir
config :favn,
  execution_pools: [
    partner_api: [
      max_concurrency: 2,
      circuit_breaker: [failure_threshold: 3, probe_after_ms: :timer.minutes(5)]
    ]
  ]
```

Circuit identity is workspace plus resource kind and name. An open circuit
blocks only nodes that use that pool or connection. After `probe_after_ms`, one
normal eligible node receives the exclusive half-open probe; success closes the
circuit and resets its consecutive failure count. Circuit breakers do not change
node retry policy or authorize repeating an unknown side effect.

## Runner Plugins

Runner plugins start supervised services inside the isolated execution runtime.
The generated DuckDB sample uses its execution plugin:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

Use `FavnDuckdbADBC` instead when using the ADBC DuckDB plugin.

For ordinary consumer-owned OTP children, use the built-in simple path:

```elixir
config :favn,
  runner_plugins: [
    {Favn.Runner.SupervisedChildren,
     children: [MyApp.RuntimeCache, MyApp.ApiSession]}
  ]
```

Use a module implementing `Favn.Runner.Plugin` when it needs to validate options
or compute child specifications. `child_specs/1` returns `{:ok, children}` or
`{:error, reason}`. The optional `applications/1` callback declares packaged OTP
applications that Favn must start first. Consumers depend on the public `:favn`
package, not `:favn_runner`.

Plugin state is local to one runner and disappears on restart or replacement.
It is suitable for rebuildable caches, pools, sessions, and rate limiters, not
business data, checkpoints, idempotency, or correctness-sensitive communication
between runs. Read
[Runner Plugins And Runner-Local Services](runner-plugins.md).

The optional Azure package contributes a cached credential service:

```elixir
config :favn,
  runner_plugins: [
    Favn.Azure.RunnerPlugin,
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

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
      duckdb: [startup: [file: {:priv, :my_app, "duckdb/startup.sql"}]]
    ]
  ]
```

The SQL file contains the native statement, for example `LOAD parquet;`.

`open`:

| Key | Required | Meaning |
| --- | --- | --- |
| `:database` | yes | `":memory:"` or a non-empty DuckDB file path. |

Old root keys such as `:database`, `:duckdb_bootstrap`, and root
`:write_concurrency` are rejected. Use `open: [...]` and `duckdb: [...]`.

`duckdb` accepts only native session-script configuration:

| Key | Meaning |
| --- | --- |
| `:startup` | Optional SQL file run for every new physical session. |
| `:resources` | Named SQL files selected by SQL assets and catalog metadata. |
| `:catalogs` | Favn-owned resource and write-admission metadata; it does not generate SQL. |

```elixir
duckdb: [
  startup: [
    file: {:priv, :my_app, "duckdb/startup.sql"},
    params: [timezone: "Europe/Oslo"]
  ],
  resources: [
    landing_storage: [
      file: {:priv, :my_app, "duckdb/landing_storage.sql"},
      params: [
        token: Favn.RuntimeConfig.Ref.secret_env!("LANDING_TOKEN")
      ]
    ]
  ],
  catalogs: [
    landing: [
      resource: :landing_storage,
      write_concurrency: 1,
      write_scope: "production-ducklake-metadata"
    ]
  ]
]
```

Script `file` is either `{:priv, otp_app, "relative/path.sql"}` or an absolute
path. A runtime ref may resolve to an absolute path. `params` values may be
literals, environment-backed runtime-config refs, or supported deferred
`Favn.RuntimeValue` refs and are referenced as `@name` in the SQL file. They are
value-only typed literals, not identifiers or SQL fragments.

For example, `Favn.Azure.Credentials.token_ref/2` from the optional
`:favn_azure` package supplies a secret cached token at physical-session
planning time:

```elixir
params: [
  azure_token:
    Favn.Azure.Credentials.token_ref(
      "https://storage.azure.com/",
      provider: "managed_identity"
    )
]
```

Catalog `write_concurrency` is a positive integer or `:unlimited` and defaults
to `1`. `write_scope` is an optional stable string shared by catalog aliases or
connections that compete for the same backend capacity. Favn does not infer
that identity from arbitrary SQL.

Write native `INSTALL`, `LOAD`, `SET`, `CREATE SECRET`, `ATTACH`, and `USE`
statements in the files. The removed structured `load`, `settings`, `secrets`,
`attach`, and `use` keys are rejected. Read
[DuckDB Session Scripts And Resources](duckdb-session-scripts.md) for complete
examples, physical-session lifecycle, pooling identity, safety rules, and a
failure example.

Environment-backed `Favn.RuntimeConfig.Ref` values resolve when the runner
starts, not on every checkout. Rotating one therefore requires a runner restart
unless the native DuckDB provider refreshes it itself. Deferred Azure token refs
resolve when a physical-session plan is built; a refresh changes the pool
fingerprint so a session initialized with the old token is not selected for the
new plan. Superseded idle sessions for the same connection requirements are
closed so their admission leases cannot block refreshed session bootstrap.
Idle-pool timeout is not a maximum session age. In local development,
`mix favn.reload` restarts the runner and reevaluates runtime config.

## DuckDB ADBC Config

Use `:favn_duckdb_adbc` when a deployment needs explicit DuckDB driver control.

Global driver config:

```elixir
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.2/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
```

ADBC uses the same `open`, native `duckdb.startup/resources/catalogs`, and
`pool` shape.

Runner plugin result bounds:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdbADBC,
     default_row_limit: 10_000,
     default_result_byte_limit: 20_000_000}
  ]
```

DuckDB and extension settings are native SQL in `startup` or a named resource,
so the ADBC adapter does not maintain a separate setting allowlist. For
DuckLake on PostgreSQL, configure its pool settings in SQL and keep Favn catalog
`write_concurrency` below the usable metadata connection budget.

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

`mix favn.dev`, `mix favn.reload`, `mix favn.inspect`, and `mix favn.query` load
`.env` from the project root before `config/runtime.exs`, compile, or runtime
startup. Each command reads the file once and evaluates runtime config in a
fresh Mix process.

Rules:

- Missing default `.env` is fine.
- Set `FAVN_ENV_FILE` to use another env file.
- If `FAVN_ENV_FILE` is set and missing, startup fails.
- Existing process env wins over values in the file.
- Supports `KEY=value`, optional `export`, quotes, comments, and blank lines.

For example:

```dotenv
FAVN_RUNTIME_MODE=cloud
```

```elixir
# config/runtime.exs
import Config

database =
  case System.fetch_env!("FAVN_RUNTIME_MODE") do
    "cloud" -> "cloud.duckdb"
    "local" -> "local.duckdb"
  end

config :favn, :connections,
  warehouse: [open: [database: database]]
```

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
