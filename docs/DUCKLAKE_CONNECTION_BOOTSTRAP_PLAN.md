# DuckDB/DuckLake Connection Bootstrap Plan

Planning issue: <https://github.com/eirhop/favn/issues/170>

## Goal

Add first-class DuckDB connection bootstrap support so a consumer project can configure one named SQL connection, start the local Favn stack, and run SQLClient or SQL assets against an attached DuckLake catalog backed by Azure Data Lake Storage and a PostgreSQL metadata catalog.

The feature must keep data-plane setup in the SQL runtime and DuckDB plugin boundary. The orchestrator should only persist and report failure metadata. The web tier should only display already-redacted diagnostics.

## Current State

- `Favn.SQL.Client.connect/2` resolves named connections, acquires admission, calls `adapter.connect/2`, builds capabilities, and returns a `%Favn.SQL.Session{}`.
- `Favn.SQL.Adapter.DuckDB.connect/2` opens DuckDB and creates one connection handle, but does not run any setup SQL.
- `Favn.Connection.Validator` already resolves runtime config refs and tracks `secret_fields` on `%Favn.Connection.Resolved{}`.
- Connection runtime config refs are currently resolved at the top-level value boundary. The nested bootstrap shape below requires either recursive connection config resolution or a flatter first-cut config that keeps env refs in top-level schema fields.
- `Favn.RuntimeConfig.Redactor` already provides a shared redaction mechanism for runtime-config-backed values.
- SQL assets execute through `Favn.SQLAsset.Runtime`, which calls `Favn.SQL.Client.connect/2` through the runner-owned connection registry. This is already the right seam for bootstrap.
- Local dev transports only explicit consumer `:favn` connection config to the runner, so bootstrap config must remain part of named connection runtime config and must be redacted in local diagnostics.

## Ownership

- `apps/favn_sql_runtime` owns the generic bootstrap lifecycle: when bootstrap is invoked, error shape, connection cleanup on failure, and public SQL client behavior.
- `apps/favn_duckdb` owns DuckDB-specific bootstrap config interpretation, SQL generation, execution ordering, extension install/load, DuckDB secrets, DuckLake attach, `USE`, quoting, and adapter diagnostics.
- `apps/favn_runner` needs no new orchestration logic; SQL assets should receive bootstrap behavior automatically through `Favn.SQL.Client.connect/2`.
- `apps/favn_orchestrator` should only store runner failure payloads already produced by the runner/runtime path.
- `web/favn_web` should only display redacted bootstrap diagnostics if existing run error surfaces are sufficient.
- `apps/favn_core` should only change if recursive runtime config resolution or shared redaction helpers must be expanded for nested connection config. Avoid new public core structs unless tests show connection config cannot express the feature safely.

## Public Configuration Shape

Prefer a typed DuckDB-owned bootstrap shape over ad hoc interpolated `init_sql`. Keep a small raw SQL escape hatch only if needed for dogfooding.

Recommended first-cut shape:

```elixir
config :favn,
  connection_modules: [MyApp.Connections.Warehouse],
  connections: [
    warehouse: [
      database: ":memory:",
      write_concurrency: :unlimited,
      duckdb_bootstrap: [
        extensions: [
          install: [:ducklake, :postgres, :azure],
          load: [:ducklake, :postgres, :azure]
        ],
        secrets: [
          azure_adls: [
            type: :azure,
            provider: :credential_chain,
            account_name: Favn.RuntimeConfig.Ref.env!("AZURE_STORAGE_ACCOUNT")
          ]
        ],
        attach: [
          name: :lake,
          type: :ducklake,
          metadata: Favn.RuntimeConfig.Ref.secret_env!("DUCKLAKE_POSTGRES_DSN"),
          data_path: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_DATA_PATH")
        ],
        use: :lake
      ]
    ]
  ]
```

The matching connection definition must allow the bootstrap key:

```elixir
%Favn.Connection.Definition{
  name: :warehouse,
  adapter: Favn.SQL.Adapter.DuckDB,
  config_schema: [
    %{key: :database, required: true, type: :path},
    %{key: :duckdb_bootstrap, type: {:custom, &MyApp.ConnectionSchemas.duckdb_bootstrap/1}}
  ]
}
```

If requiring every consumer to write the custom validator is too much friction, add a DuckDB-owned helper such as `FavnDuckdb.Connection.bootstrap_schema_field/0` or `Favn.SQL.Adapter.DuckDB.bootstrap_schema_field/0`. Do not make `Favn.Connection.Validator` understand DuckDB-specific keys.

Because nested `Favn.RuntimeConfig.Ref` values are not currently resolved inside arbitrary maps/lists, implementation should choose one of two explicit paths:

- Preferred: extend runtime config resolution for connection values to recursively resolve nested refs and record enough secret metadata for redaction.
- Minimal fallback: keep secret/runtime values as top-level connection config keys, then let `duckdb_bootstrap` reference those keys by atom.

## Runtime Design

1. Extend `Favn.SQL.Adapter` with an optional `bootstrap/3` callback:

```elixir
@callback bootstrap(conn(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
```

2. In `Favn.SQL.Client.connect_and_build_session/4`, after `adapter.connect/2` succeeds and before capabilities are built, call bootstrap if the adapter exports it.

3. On bootstrap failure, disconnect the adapter connection, release the admission lease, and return the bootstrap error. This prevents partially initialized sessions from escaping.

4. Keep bootstrap per session for the first cut. DuckDB in-memory and DuckLake attach state are connection-scoped, and per-session bootstrap is predictable. Later optimization can add adapter-owned process-level extension install caching if dogfooding shows startup cost is material.

5. Keep bootstrap under the same session admission lease as the connection. The first dogfooding config can use `write_concurrency: :unlimited` for DuckLake where that is safe.

## DuckDB Bootstrap Design

Add a small DuckDB-owned bootstrap module, for example `Favn.SQL.Adapter.DuckDB.Bootstrap`, with these responsibilities:

- Normalize `resolved.config[:duckdb_bootstrap]` into explicit ordered steps.
- Validate supported extension names and identifiers before building SQL.
- Generate and execute `INSTALL <extension>` steps.
- Generate and execute `LOAD <extension>` steps.
- Generate and execute `CREATE SECRET <name> (...)` for Azure credential-chain secrets.
- Generate and execute `ATTACH <metadata> AS <name> (TYPE ducklake, DATA_PATH <data_path>)`.
- Generate and execute `USE <name>` after attach when configured.
- Return `:ok` when no bootstrap config exists.

Step execution should use the existing DuckDB client query path so test doubles can observe the exact statement order. Each generated step should carry a stable step id such as `install_ducklake`, `load_azure`, `create_secret_azure_adls`, `attach_lake`, or `use_lake`.

## Secret Handling

- Runtime refs are already resolved before the adapter sees config. The adapter must never log raw `resolved.config`.
- DuckDB bootstrap errors should include `connection`, `operation: :bootstrap`, `step`, and sanitized details.
- SQL text in error details should either be omitted or redacted using `resolved.secret_fields` plus nested bootstrap secret metadata.
- Values under secret fields and secret refs must be replaced with `:redacted` before being included in `%Favn.SQL.Error{}` details. If nested refs are supported, `Favn.Connection.Sanitizer` and any local config transport diagnostics need nested redaction coverage, not only top-level `secret_fields` replacement.
- PostgreSQL metadata DSNs should be treated as secret by default because they commonly include passwords.
- Azure `ACCOUNT_NAME` is not inherently secret, but redaction should still respect a user-provided `secret_env!` ref.

## Diagnostics

Use the existing `%Favn.SQL.Error{}` contract with a bootstrap-specific shape:

```elixir
%Favn.SQL.Error{
  type: :connection_error,
  adapter: Favn.SQL.Adapter.DuckDB,
  operation: :bootstrap,
  connection: :warehouse,
  message: "DuckDB connection bootstrap failed at attach_lake",
  retryable?: false,
  details: %{
    step: :attach_lake,
    bootstrap_kind: :ducklake_attach,
    reason: "... redacted adapter reason ..."
  }
}
```

Runner SQL asset errors should continue wrapping this as `Favn.SQLAsset.Error` with `type: :backend_execution_failed`, preserving the SQL error as `cause`.

## Implementation Tasks

1. Decide nested refs versus top-level refs for the first cut, then update connection runtime config resolution and sanitizer behavior accordingly.
2. Add optional bootstrap lifecycle to `Favn.SQL.Adapter` and `Favn.SQL.Client`.
3. Add SQL runtime tests proving bootstrap runs after connect, runs before returned sessions can query, and disconnects/releases admission on failure.
4. Add DuckDB bootstrap normalization and execution module.
5. Add DuckDB bootstrap config validation helpers for consumer connection definitions.
6. Add DuckDB tests for ordered extension install/load, Azure credential-chain secret SQL, DuckLake attach SQL, `USE`, failed-step diagnostics, and secret redaction.
7. Add runner SQL asset coverage proving manifest-pinned SQL assets automatically use bootstrapped connections through the existing registry path.
8. Add public docs in `README.md`, `docs/FEATURES.md`, and `Favn.Connection` or DuckDB module docs once implemented.
9. Update `docs/ROADMAP.md` to remove or downgrade the issue once the feature lands.

## Test Plan

- `apps/favn_sql_runtime/test/sql/client_bootstrap_test.exs`: generic optional callback lifecycle, success, failure cleanup, invalid adapter return handling.
- `apps/favn_duckdb/test/sql/adapter/duckdb_bootstrap_test.exs`: DuckDB-specific statement generation and diagnostics using a fake client.
- `apps/favn_runner/test/execution/sql_asset_test.exs`: SQL asset execution through a bootstrapped named connection.
- `apps/favn/test/sql_client_test.exs`: public `Favn.SQLClient` can query after bootstrap, if a lightweight fake or DuckDB-local assertion is practical.
- Existing local config transport tests should be extended if redacted diagnostics include nested `duckdb_bootstrap` config.
- `apps/favn_sql_runtime/test/connection/validator_test.exs`: recursive nested runtime ref resolution and secret redaction metadata if the preferred nested-ref path is chosen.

## Manual Integration Verification

The automated tests lock the bootstrap lifecycle, generated DuckDB statements,
malformed-config diagnostics, cleanup behavior, and redaction. They intentionally
do not install remote DuckDB extensions or connect to Azure/PostgreSQL by default.

Before relying on this for the first dogfooding pipeline, verify manually from a
consumer project with real credentials:

```bash
export AZURE_STORAGE_ACCOUNT=...
export DUCKLAKE_POSTGRES_DSN=...
export DUCKLAKE_DATA_PATH=abfss://...

mix favn.dev
mix favn.run MyWork.Pipelines.SourceToDuckLake --wait
```

Then confirm that `Favn.SQLClient.connect(:warehouse)` can query the attached
DuckLake catalog after bootstrap, and that an intentionally bad attach value
reports `operation: :bootstrap` with a failing step and redacted metadata.

After Elixir changes, run the repository-required checks:

```bash
mix format
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix xref graph --format stats --label compile-connected
```

## Open Decisions

- Whether `duckdb_bootstrap` should be the final key name or `bootstrap` with adapter-specific validation. The safer first cut is `duckdb_bootstrap` to avoid implying a cross-adapter contract before a second adapter exists.
- Whether to provide raw named SQL steps in the first cut. Avoid unless the dogfooding DuckLake path cannot be represented by typed steps.
- Whether `INSTALL` should be optional in production configs where extensions are preinstalled. The proposed shape supports separate `install` and `load` lists.
- Whether DuckDB extension installation needs adapter-owned once-per-runtime caching. Defer until measured.

## Non-Goals

- Azure PostgreSQL managed identity token provider.
- Generic database migration framework.
- Arbitrary SQL console or UI-driven bootstrap editing.
- Orchestrator-owned data-plane setup.
- Production cloud deployment automation.
