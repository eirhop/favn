# `favn_azure`

Optional Azure authentication package for Favn runners.

It provides:

- `Favn.Azure.RunnerPlugin`, which supervises one bounded runner-local token
  cache;
- `Favn.Azure.Credentials`, the public API for Azure CLI and managed-identity
  tokens;
- secret deferred token refs for DuckDB session-script parameters.

## Runner setup

```elixir
config :favn,
  runner_plugins: [
    Favn.Azure.RunnerPlugin,
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

## Elixir code

```elixir
{:ok, token} =
  Favn.Azure.Credentials.fetch_access_token(
    "https://vault.azure.net",
    provider: "managed_identity"
  )
```

Use `provider: "cli"` for a local `az login` session. Use
`provider: "managed_identity"` for managed identity; `endpoint: :auto` selects
Azure App Service when its identity environment is present and otherwise uses
IMDS. Pass `client_id` for a user-assigned identity. These canonical string
names can come directly from environment configuration and can also be passed
unchanged to DuckDB's native Azure `CHAIN` parameter. Built-in atom names and
the legacy `azure_cli` name are not accepted.

The cache shares concurrent fetches for the same request and provider options,
refreshes on demand before expiry, and returns a cached token after refresh
failure only while that token remains valid. Global in-flight work, entries,
per-key waiters, request sizes, and timeouts are bounded. Calls outside a runner
fetch in an owned timeout-bounded task when the plugin is not configured. A
configured but temporarily unavailable default cache fails closed instead of
bypassing its concurrency bounds. The plugin also starts the packaged `:favn_azure` OTP
application, so managed identity has its required `:inets` and `:ssl` services
inside an isolated runner.

## DuckDB

```elixir
params: [
  azure_token:
    Favn.Azure.Credentials.token_ref(
      "https://storage.azure.com/",
      provider: "managed_identity"
    )
]
```

The trusted SQL resource uses `@azure_token` in native DuckDB syntax. Favn
resolves it once during pooled-session preparation, reuses the prepared plan for
bootstrap, redacts it, and hashes it into pool identity.

For DuckLake metadata in Azure Database for PostgreSQL, use the audience
`https://ossrdbms-aad.database.windows.net` and inject that token as the
`PASSWORD` parameter of a native DuckDB `TYPE postgres` secret. Both DuckDB
adapters resolve through the same cache. Pool reuse keeps the existing physical
session while the token is current; token refresh replaces a superseded idle
session before bootstrapping PostgreSQL again. See the complete example in
[`Runner Plugins And Runner-Local Services`](../favn/guides/runner-plugins.md#ducklake-postgresql-with-managed-identity).

The cache is disposable runner state, not durable storage. Read
[`Runner Plugins And Runner-Local Services`](../favn/guides/runner-plugins.md)
for the lifecycle, GenServer session example, safety boundary, and future
provider model.
