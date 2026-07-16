# DuckDB Session Scripts And Resources

Favn keeps DuckDB setup close to native DuckDB. Instead of modelling every
extension, setting, secret, or `ATTACH` option as Elixir data, a connection
points at trusted SQL files. SQL assets declare only the stable resource names
they need.

Use this feature for physical-session preparation such as `INSTALL`, `LOAD`,
`SET`, `CREATE SECRET`, `ATTACH`, and `USE`. Keep business writes in assets.

> #### Trusted-code warning {: .warning}
>
> Session scripts execute arbitrary SQL with the connection's authority. Favn
> cannot sandbox or prove them idempotent. A partially successful file may run
> again on a fresh-session retry, and trusted SQL can deliberately leak a secret
> despite diagnostic redaction. Review these files like application code.

## Complete Example

Put trusted SQL in the owning OTP application's `priv` directory:

```text
priv/duckdb/startup.sql
priv/duckdb/azure_extension.sql
priv/duckdb/landing_storage.sql
```

```sql
-- priv/duckdb/startup.sql
SET TimeZone = @timezone;
```

```sql
-- priv/duckdb/azure_extension.sql
INSTALL azure;
LOAD azure;
```

```sql
-- priv/duckdb/landing_storage.sql
INSTALL azure;
LOAD azure;

CREATE OR REPLACE SECRET landing_storage (
  TYPE azure,
  PROVIDER credential_chain,
  ACCOUNT_NAME @account_name,
  SCOPE @scope
);

ATTACH @metadata_path AS landing (
  DATA_PATH @data_path
);
```

Configure the connection:

```elixir
config :favn,
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      pool: [enabled: true, max_idle_per_key: 1],
      duckdb: [
        startup: [
          file: {:priv, :my_app, "duckdb/startup.sql"},
          params: [timezone: "Europe/Oslo"]
        ],
        resources: [
          azure_extension: [
            file: {:priv, :my_app, "duckdb/azure_extension.sql"}
          ],
          landing_storage: [
            file: {:priv, :my_app, "duckdb/landing_storage.sql"},
            params: [
              account_name:
                Favn.RuntimeConfig.Ref.env!("AZURE_STORAGE_ACCOUNT"),
              scope:
                Favn.RuntimeConfig.Ref.env!("LANDING_STORAGE_SCOPE"),
              metadata_path:
                Favn.RuntimeConfig.Ref.secret_env!("DUCKLAKE_METADATA_PATH"),
              data_path:
                Favn.RuntimeConfig.Ref.secret_env!("DUCKLAKE_DATA_PATH")
            ]
          ]
        ],
        catalogs: [
          landing: [
            resource: :landing_storage,
            write_concurrency: 1,
            write_scope: "ducklake-production-metadata"
          ]
        ]
      ]
    ]
  ]
```

`{:priv, :my_app, "duckdb/startup.sql"}` has three parts:

- `:priv` is the literal locator tag.
- `:my_app` is the OTP application that owns the file.
- the final value is a relative path below that application's `priv/` directory.

The other supported locator is an absolute path. A
`Favn.RuntimeConfig.Ref.env!/1` may resolve `file` at runtime, but its resolved
value must also be absolute. Relative filesystem paths are rejected because a
runner's working directory is not a stable deployment contract.

### Cached Azure token parameters

When native `PROVIDER credential_chain` is not suitable, the optional
`:favn_azure` package can provide a runner-cached access token as a secret
parameter:

```elixir
config :favn,
  runner_plugins: [
    Favn.Azure.RunnerPlugin,
    {FavnDuckdb, execution_mode: :in_process}
  ],
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [
        resources: [
          landing_storage: [
            file: {:priv, :my_app, "duckdb/landing_storage.sql"},
            params: [
              azure_token:
                Favn.Azure.Credentials.token_ref(
                  "https://storage.azure.com/",
                  provider: "managed_identity"
                )
            ]
          ]
        ]
      ]
    ]
  ]
```

The trusted SQL file uses `@azure_token` in the deployment's native DuckDB
`CREATE SECRET` statement. Favn does not own or freeze that extension-specific
syntax.

For DuckLake metadata in Azure Database for PostgreSQL, request the audience
`https://ossrdbms-aad.database.windows.net` and use the resulting secret
parameter as `PASSWORD @access_token` in a native `TYPE postgres` DuckDB secret.
The complete managed-identity configuration and SQL are in
[Runner Plugins And Runner-Local Services](runner-plugins.md#ducklake-postgresql-with-managed-identity).

`token_ref/2` contains no token. It resolves from the shared cache when Favn
builds a physical-session plan. The rendered value is treated as secret, and a
refresh changes the parameter fingerprint so the new plan cannot select a
pooled session initialized with the old token. Read
[Runner Plugins And Runner-Local Services](runner-plugins.md) for provider
options, direct use from Elixir code, and the disposable runner-state boundary.

Declare resources on a SQL asset:

```elixir
defmodule MyApp.Lakehouse.Raw.Orders do
  use Favn.SQLAsset

  @resources [:landing_storage]
  @materialized :table

  query do
    ~SQL"""
    select *
    from read_parquet('abfss://landing/orders/*.parquet')
    """
  end
end
```

If every SQL asset below a namespace needs a resource, inherit it additively:

```elixir
defmodule MyApp.Lakehouse.Raw do
  use Favn.Namespace,
    relation: [catalog: :raw],
    resources: [:azure_extension]
end
```

The leaf asset's resources and all ancestor namespace resources are normalized,
deduplicated, and sorted. Resources do not have their own dependency graph and
must not depend on lexical execution order. Make a resource self-contained,
combine coupled setup in one file, or put a truly universal prerequisite in
`startup`. The `landing_storage` file above therefore loads `azure` itself.

## Cross-Catalog And External-File Example

Favn already derives catalogs from the owned target relation and rendered Favn
relation references. An asset that reads DuckLake `core`, reads a separately
credentialed object-store path, and writes native DuckDB `mart` can therefore
look like this:

```sql
-- priv/duckdb/core_catalog.sql
ATTACH @metadata_path AS core (DATA_PATH @data_path);
```

```sql
-- priv/duckdb/mart_catalog.sql
ATTACH @database_path AS mart;
```

```elixir
duckdb: [
  resources: [
    core_catalog: [file: {:priv, :my_app, "duckdb/core_catalog.sql"}, params: core_params],
    mart_catalog: [file: {:priv, :my_app, "duckdb/mart_catalog.sql"}, params: mart_params],
    landing_storage: [
      file: {:priv, :my_app, "duckdb/landing_storage.sql"},
      params: landing_params
    ]
  ],
  catalogs: [
    core: [resource: :core_catalog, write_concurrency: 1],
    mart: [resource: :mart_catalog, write_concurrency: 1]
  ]
]
```

Here `core_params`, `mart_params`, and `landing_params` stand for ordinary
keyword lists whose runtime-only fields use `Favn.RuntimeConfig.Ref` values as
shown in the complete example above.

```elixir
defmodule MyApp.Lakehouse.Mart.ImportedOrders do
  use Favn.SQLAsset

  @depends MyApp.Lakehouse.Core.Orders
  @resources [:landing_storage]
  @relation [connection: :warehouse, catalog: :mart, schema: :sales]
  @materialized :table

  query do
    ~SQL"""
    select core_orders.*
    from core.sales.orders as core_orders
    join read_json('abfss://staging/import/orders.json') as staged
      using (order_id)
    """
  end
end
```

The session plan contains `core` and `mart` from relation metadata plus the two
explicit resources. Catalog config can select each catalog's own preparation
resource as well. Favn does not parse the `read_json` path, so the narrow
`landing_storage` credential must remain explicit. The publication transaction
protects the `mart` target according to the adapter's transaction contract; it
does not create a distributed transaction or pin the DuckLake/object-store
inputs to one shared snapshot.

## Quack Example

Quack needs no Favn-specific secret, setting, or attachment schema. Put its
current native syntax in a resource file:

```sql
-- priv/duckdb/mart_quack.sql
INSTALL quack;
LOAD quack;
SET httpfs_connection_caching = true;

CREATE OR REPLACE SECRET mart_quack (
  TYPE quack,
  TOKEN @token,
  SCOPE @uri
);

ATTACH @uri AS mart (TYPE quack);
```

```elixir
duckdb: [
  resources: [
    mart_quack: [
      file: {:priv, :my_app, "duckdb/mart_quack.sql"},
      params: [
        uri: Favn.RuntimeConfig.Ref.env!("MART_QUACK_URI"),
        token: Favn.RuntimeConfig.Ref.secret_env!("MART_QUACK_TOKEN")
      ]
    ]
  ],
  catalogs: [
    mart: [
      resource: :mart_quack,
      write_concurrency: 1,
      write_scope: "production-mart-quack-server"
    ]
  ]
]
```

An asset whose target relation is in catalog `mart` selects `mart_quack`
through catalog metadata; it does not also need `@resources [:mart_quack]`.
Use `@resources` for capabilities that relation inference cannot see, such as
the separate `landing_storage` secret above.

The asset statement is submitted to the local DuckDB client session. The Quack
attachment makes `mart` a remote catalog, and DuckDB forwards operations on that
catalog to the server. Favn never opens the server's underlying DuckDB file.
Single-writer admission still uses the `mart` target, but Favn does not promise
atomicity across local, DuckLake, and Quack catalogs or across multiple Quack
servers.

Quack is beta and first available in DuckDB 1.5.3. Pin and test a compatible
DuckDB build; for ADBC, configure a 1.5.3-or-newer driver explicitly. Favn does
not maintain an extension-version allowlist. An unavailable or incompatible
Quack extension fails the `resource:mart_quack` bootstrap step with redacted
diagnostics. This avoids freezing beta extension policy into Favn's DSL while
keeping the failure attributable.

See DuckDB's current
[Quack overview](https://duckdb.org/docs/current/quack/overview) and
[extension status](https://duckdb.org/docs/current/core_extensions/quack) before
deploying; its protocol, functions, settings, and defaults may still change.

## Parameters Use `@name`

Session scripts and asset SQL both spell a value parameter as `@name`. They are
different scopes:

- session-script values come from `duckdb.startup.params` or the selected
  `duckdb.resources.<name>.params`;
- asset-query values come from the asset runtime, windows, submitted params, or
  `@runtime_inputs`.

Favn recognizes script parameters only in SQL code, not in quoted strings,
quoted identifiers, line comments, nested block comments, or dollar-quoted
strings. Every configured parameter must be used and every referenced parameter
must be configured. Values are rendered as typed SQL literals with escaping;
they cannot supply identifiers, keywords, clauses, or additional statements.

Do not use `{{var}}`. If a script needs a dynamic identifier or SQL fragment,
make separate trusted SQL files and select them through deployment config.

## Session Lifecycle In Simple Terms

A session is one physical DuckDB adapter connection owned exclusively by one
runner process while checked out:

1. Favn determines the required catalogs and resource names for the operation.
2. It looks for an idle physical session with the exact same connection,
   catalogs, resources, script-content hashes, parameter fingerprints, and
   adapter fingerprint.
3. If none exists, Favn opens a physical session, runs `startup` once, then runs
   the selected resource files in sorted resource-name order.
4. Only after setup succeeds does Favn run the asset query or SQL client work.
5. A proven-safe session may return to the runner-local pool. Otherwise it is
   closed. Idle eviction, errors, runner shutdown, and disabled pooling also end
   it.

A pipeline does not own one shared session for all its assets. Each SQL asset or
SQL-client checkout gets exclusive use of a session; a compatible physical
session may happen to be reused later. Startup and resource scripts run on
physical-session creation, not at the beginning of every pipeline and not on
every pool checkout.

Environment-backed references, including `secret_env!/1`, are resolved when the
runner starts. Changing an environment variable does not update an already
resolved connection, and an idle timeout is not a maximum physical-session age.
Prefer native providers that refresh credentials themselves. When a deployment
rotates an environment-resolved value, restart the runner so it resolves the new
value and closes its old session pool; do not assume the next asset or pool
checkout will do that. In local development, `mix favn.reload` performs that
runner restart and reevaluates runtime config.

Supported deferred `Favn.RuntimeValue` parameters have a different lifecycle:
they resolve while Favn builds the session plan. The Azure token ref uses this
path, refreshes through a runner-local cache before expiry, and changes the pool
fingerprint when the token changes. If refresh fails, planning can reuse the old
token only while it is still valid; an expired token is never returned. When a
fingerprint changes for otherwise identical session requirements, Favn evicts
the superseded idle physical session and closes an active superseded session on
checkin. This releases its finite admission lease so the replacement can
bootstrap with the current credential.

Catalog entries do not generate SQL. They give Favn the small amount of metadata
it still owns: which resource prepares a catalog, its write-admission limit, and
an optional stable `write_scope` shared by aliases or connections that compete
for the same backend. The SQL file owns the actual `ATTACH` statement and all
DuckDB-specific options.

## Safety Rules

Session scripts are trusted deployment code with the same authority as the
connection. Favn bounds file and rendered sizes, validates exact parameters,
redacts declared secret values from its errors, and keys pooled sessions by
script identity. It cannot prove arbitrary SQL safe.

Follow these rules:

- Make scripts idempotent and safe to retry after partial failure.
- Limit them to session preparation. Do not insert business rows, publish
  tables, call webhooks, or perform other durable external side effects.
- Prefer temporary/session-scoped secrets and state where DuckDB supports them.
  DuckDB secrets are temporary by default; use `PERSISTENT` only after reviewing
  its on-disk storage and lifecycle deliberately.
- Use `Favn.RuntimeConfig.Ref.secret_env!/1` for secret parameters. Never place
  secret literals in source-controlled SQL or config.
- Treat environment-resolved credential rotation as a runner lifecycle event.
  Prefer refresh-capable native providers; otherwise restart the runner after
  rotation so both resolved config and physical sessions are replaced. Deferred
  Azure token refs instead use the bounded runner cache and session fingerprint.
- Do not print, select, copy, or otherwise expose a secret from the script.
  Redaction cannot protect a secret that trusted SQL deliberately writes out.
- Review extension installation and remote access like application code. Pin
  deployment artifacts where reproducibility matters.
- Keep files small and purpose-specific. A resource should prepare one coherent
  capability.
- Use conservative catalog `write_concurrency`; pooling does not increase
  DuckLake metadata capacity.

## A Bad Lifecycle

This startup file is unsafe:

```sql
INSERT INTO audit.session_starts VALUES (current_timestamp);
LOAD azure;
```

Suppose the `INSERT` commits but `LOAD azure` fails. Favn never exposes that
half-prepared session. A later retry creates a fresh physical session and runs
the file again, inserting a second audit row even though no asset query has run.
The same duplication can happen after a process crash or runner restart.

Move the durable insert into a normal asset with explicit materialization and
retry semantics. Keep the session script limited to idempotent session setup.

## Removed Structured Configuration

The old `duckdb.load`, `duckdb.settings`, `duckdb.secrets`, `duckdb.attach`, and
`duckdb.use` forms are not supported. Write their native DuckDB SQL in startup
or resource files. This keeps Favn's DSL small while allowing new DuckDB
extensions and syntax without a Favn release.
