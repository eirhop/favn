# Generic CRM example

This standalone project is a small, runnable Favn workload for exercising the
CLI and inspecting the resulting DuckDB data. It uses four medallion layers:

1. **Landing** - compact deterministic JSON in `.data/generic_crm/landing/`.
2. **Source** - DuckDB tables loaded from the landing files.
3. **Core** - CRM-vendor-neutral customer, opportunity, and activity models.
4. **Mart** - account health, daily pipeline, and executive reporting tables.

The workload demonstrates full refreshes, exact daily windows, lookback,
freshness, checks, output contracts, retry policy, cancellation, schedules,
backfills, additive source changes, and managed-target rebuilds. The temporary
manual test record is in
`docs/refactor/generic-crm-cli-lifecycle-qa.md`.

## Compile and test

From this directory:

```powershell
mix deps.get
mix compile
mix test
```

Every DuckDB-writing asset uses the one-slot `local_duckdb` execution pool.
This serializes direct, pipeline, and rebuild writes to the file-backed catalog.
The connection pool may retain one idle session for reuse; that idle limit is
not the concurrency control.

## Run locally

Provide the normal local PostgreSQL control plane, provision a workspace, then
start the Favn stack:

```powershell
$env:FAVN_RUNTIME_INPUT_PIN_KEY = '01234567890123456789012345678901'
$env:FAVN_DATABASE_URL = 'ecto://favn_migrator:favn_migrator_local@127.0.0.1:5432/favn_dev'
mix favn.postgres.migrate
mix favn.postgres.grant_runtime --role favn_runtime
mix favn.postgres.provision_workspace --id local-dev --slug local-dev --name 'Local Development'

$env:FAVN_DATABASE_URL = 'ecto://favn_runtime:favn_runtime_local@127.0.0.1:5432/favn_dev'
mix favn.dev
mix favn.reload
mix favn.run FavnReferenceWorkload.Pipelines.BootstrapCrmDemo --refresh force_all
mix favn.run FavnReferenceWorkload.Pipelines.DailyCrmAnalytics --window day:2026-07-23 --refresh force_all
```

With no `--window`, the daily pipeline selects the latest complete daily
window. The fixture contains facts only through 2026-07-23, so the quick start
uses that exact day to produce meaningful mart rows. A default run on a later
date is valid but produces an empty daily mart. Use `mix favn.backfill` for a
range.

The data plane defaults to `generic_crm.duckdb`. Set `DUCKDB_ADBC_DRIVER` to a
compatible DuckDB library before starting Favn; on Windows this is normally the
absolute path to `duckdb.dll`.

Stop the local stack before opening the same database with another DuckDB
client, because the running example owns its single file-backed session.

## Lifecycle probes

The example contains three intentionally small probes:

- `Lifecycle.RetryProbe` fails safely twice and succeeds on attempt three.
- `Lifecycle.CancellationProbe` stays active long enough to cancel with
  `mix favn.runs cancel`.
- `Pipelines.LifecycleScheduleProbe` fires every ten seconds, but new schedules
  are inactive until explicitly activated with `mix favn.schedules activate`.

Start with `mix favn.dev --scheduler` only when testing schedule submission.
Starting without that flag leaves scheduler execution disabled.

## Schema change and rebuild

`CRM_EXAMPLE_SCHEMA_VERSION` selects the compile-time account contract:

- `v1` has `account_id`, `name`, and `segment`.
- `v2` adds the non-null `industry` field.

After changing the value, always force recompilation before reloading:

```powershell
$env:CRM_EXAMPLE_SCHEMA_VERSION = 'v2'
mix compile --force
mix favn.reload
```

Run the V2 landing account asset before planning the rebuild so the new
`industry` input exists:

```powershell
mix favn.run FavnReferenceWorkload.Warehouse.Landing.WriteExtracts:accounts_snapshot --dependencies all --refresh force_all
mix favn.rebuild plan FavnReferenceWorkload.Warehouse.Source.Accounts --reason "exercise additive CRM account contract"
```

Approve the returned immutable plan and hash with `mix favn.rebuild start`,
then follow it with `mix favn.rebuild status`.

For isolated manual testing, give each run a newly provisioned
`CRM_EXAMPLE_WORKSPACE_ID` and unused `CRM_EXAMPLE_DUCKDB_PATH`. Tests use
`generic_crm_test.duckdb` by default; override it with
`CRM_EXAMPLE_TEST_DUCKDB_PATH` when needed.
