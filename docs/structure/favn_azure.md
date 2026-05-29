# `favn_azure` Structure

Reader: contributors and documentation agents who need ownership guidance for
Azure integration helpers.

Documentation type: reference.

`favn_azure` owns Azure-specific token acquisition helpers used by Favn adapters.
It is an internal plugin-support app, not a public HexDocs target. Public user
docs should mention Azure behavior through adapter configuration guides rather
than exposing these modules as the primary API.

## App Metadata

- Mix app: `:favn_azure`
- Description: Azure integration helpers for Favn adapters
- Runtime applications: `:logger`, `:inets`, `:ssl`
- Direct dependencies: `:jason`
- Public HexDocs status: internal only; plugin docs deferred

## Ownership Map

| Path | Owns |
| --- | --- |
| `apps/favn_azure/lib/favn/azure/postgres_entra_token.ex` | Facade for fetching Microsoft Entra tokens for Azure Database for PostgreSQL. Selects the configured provider and normalizes invalid auth config errors. |
| `apps/favn_azure/lib/favn/azure/postgres_entra_token_provider.ex` | Behaviour for PostgreSQL Entra token providers. |
| `apps/favn_azure/lib/favn/azure/postgres_entra_token/azure_cli.ex` | Azure CLI token provider using `az account get-access-token` for the PostgreSQL resource. |
| `apps/favn_azure/lib/favn/azure/postgres_entra_token/managed_identity.ex` | Managed identity token provider for IMDS and Azure App Service identity endpoints, including bounded retry for retryable connection failures. |
| `apps/favn_azure/lib/favn/azure/token.ex` | Runtime token struct. Tokens are for immediate adapter bootstrap use and must not be persisted, logged, or exposed. |
| `apps/favn_azure/lib/favn/azure/token_error.ex` | Redacted token acquisition error struct with retryability and details. |

## Boundaries

- `favn_azure` owns Azure token acquisition details only.
- Adapters own how tokens are used for connection bootstrap.
- The orchestrator owns control-plane state and scheduling; Azure helpers do not
  own runtime lifecycle semantics.
- Public Favn docs should not require users to call these modules directly unless
  a future plugin API intentionally promotes them.

## Verification

For changes in this app, prefer focused tests under `apps/favn_azure` if present.
Do not run umbrella-wide tests for documentation-only updates.
