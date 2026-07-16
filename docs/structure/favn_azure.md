# `favn_azure` Structure

Reader: contributors and documentation agents changing Azure runner
authentication.

Documentation type: reference.

`favn_azure` is an optional public integration package. It owns Azure token
acquisition and the first concrete implementation of the public runner-plugin
lifecycle. It does not own DuckDB syntax, SQL connection behavior, orchestrator
state, or durable credential storage.

## App Metadata

- Mix app: `:favn_azure`
- Description: Azure integration helpers for Favn adapters
- Runtime applications: `:logger`, `:inets`, `:ssl`
- Direct runtime dependencies: `:favn_core`, `:jason`
- Test-only dependency: `:favn_test_support`
- Public entrypoints: `Favn.Azure.RunnerPlugin`, `Favn.Azure.Credentials`

## Ownership Map

| Path | Owns |
| --- | --- |
| `apps/favn_azure/lib/favn/azure/runner_plugin.ex` | Public runner lifecycle contribution for one Azure credential supervision subtree. |
| `apps/favn_azure/lib/favn/azure/credentials.ex` | Public token/access-token API and secret DuckDB runtime-value refs. Calls use the configured cache, fail closed while that cache is unavailable, and otherwise run direct providers in an owned timeout-bounded task. |
| `apps/favn_azure/lib/favn/azure/credentials/cache.ex` | Bounded runner-local token cache, asynchronous single-flight fetch coordination, on-demand refresh, still-valid stale fallback, global in-flight/entry/per-key-waiter limits, and timeouts. Provider I/O never runs in the GenServer. |
| `apps/favn_azure/lib/favn/azure/credentials/supervisor.ex` | Task supervisor and cache lifecycle. |
| `apps/favn_azure/lib/favn/azure/credentials/request.ex` | Normalized non-secret token cache identity and structured configuration validation: resource, canonical built-in provider string or custom module, client id, and endpoint mode. |
| `apps/favn_azure/lib/favn/azure/credential_provider.ex` | Provider behaviour for one raw token acquisition. |
| `apps/favn_azure/lib/favn/azure/credentials/azure_cli.ex` | Azure CLI provider for an arbitrary requested Azure resource. |
| `apps/favn_azure/lib/favn/azure/credentials/managed_identity.ex` | IMDS and Azure App Service managed-identity provider with bounded transient retry. |
| `apps/favn_azure/lib/favn/azure/postgres_entra_token.ex` | PostgreSQL resource facade delegating to the shared credential service. |
| `apps/favn_azure/lib/favn/azure/token.ex` | Token struct with normalized UTC expiry and always-redacted Inspect output. |
| `apps/favn_azure/lib/favn/azure/token_error.ex` | Redacted acquisition error with retryability and bounded details. |

## Boundaries

- `Favn.Azure.RunnerPlugin` implements `Favn.Runner.Plugin`; `favn_azure` does
  not depend on internal `:favn_runner`.
- Token cache state is local to one runner and disposable. It is not a key
  vault, durable credential store, distributed cache, or asset communication
  channel.
- Callers use `Favn.Azure.Credentials`, never `GenServer.call` against cache
  internals.
- Public built-in provider identifiers are exactly `"cli"` and
  `"managed_identity"`, matching native DuckDB Azure chain values. Built-in atom
  forms and `azure_cli` are rejected. Custom provider modules implement
  `Favn.Azure.CredentialProvider` and remain a separate extension path.
- Cached tokens are keyed by normalized request identity plus a cryptographic
  fingerprint of bounded provider options, never by the access token or App
  Service identity header.
- A refresh failure may return the cached token only while it is still valid.
  Expired tokens are never returned.
- Direct provider acquisition has the same finite caller timeout boundary. A
  configured default cache is never silently bypassed during restart.
- DuckDB integration is provider-neutral: `token_ref/2` returns a secret
  `Favn.RuntimeValue.Ref`; `favn_sql_runtime` resolves that generic contract,
  while trusted SQL owns native DuckDB `CREATE SECRET` syntax.
- Azure Database for PostgreSQL uses the
  `https://ossrdbms-aad.database.windows.net` audience. DuckLake resources pass
  that ref as the password of a native DuckDB PostgreSQL secret; both DuckDB
  adapters resolve it during pool preparation.
- User-assigned identity selection belongs in the request `client_id`. App
  Service identity headers are read at provider-call time and never enter the
  request, cache key, logs, or Inspect output.
- The orchestrator owns control-plane state and scheduling. This package does
  not authenticate Favn control-plane storage.

## Verification

- `apps/favn_azure/test/favn/azure/credentials_test.exs` covers provider parsing,
  managed-identity routing/retry, single-flight reuse, stale-valid fallback,
  token redaction, runtime refs, and PostgreSQL facade delegation.
- `apps/favn_azure/test/favn/azure/runner_plugin_test.exs` covers lifecycle child
  contribution and option validation.
- `apps/favn_duckdb_adbc/test/sql/adapter/duckdb_adbc_azure_pool_test.exs`
  covers PostgreSQL token reuse, expiry refresh, superseded-session eviction,
  and new ADBC physical-session bootstrap.
