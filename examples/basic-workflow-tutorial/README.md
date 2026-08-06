# Basic workflow tutorial

A complete, runnable Favn project you can read in one sitting and copy as a
starting point. It lands data from a stand-in CRM API and publishes it through
three warehouse layers, one DuckDB catalog per layer.

Everything runs locally with no external services beyond the PostgreSQL control
plane, and every setting has a working default.

## What it demonstrates

| Concept | Where to look |
| --- | --- |
| Source-system transport, isolated from everything else | `lib/crm_demo/integrations/crm/client.ex` |
| Imperative extraction with immutable parts and a completion manifest | `lib/crm_demo/landing/crm/` |
| One `asset/1` runtime shared by several datasets | `lib/crm_demo/landing/crm/snapshots.ex` |
| Runtime inputs: binding a file list chosen at run time | `lib/crm_demo/warehouse/source/crm/inputs.ex` |
| SQL output contracts, grain, and row-count reconciliation | `lib/crm_demo/warehouse/source/crm/events/deal.ex` |
| Reusable contract fragments and matching SQL projection helpers | `lib/crm_demo/contracts/`, `lib/crm_demo/sql/` |
| Namespace inheritance for connection, catalog, schema, window, and coverage | `lib/crm_demo/warehouse/` |
| Full refresh, daily windows, incremental writes, and views | the Source, Core, and Mart assets |
| Custom transactional checks | `lib/crm_demo/warehouse/mart/sales/account_health.ex` |
| Trusted DuckDB session scripts, one catalog per phase | `priv/duckdb/`, `config/config.exs` |
| Retry, cancellation, and schedule behavior | `lib/crm_demo/lifecycle/` |
| Elastic runner scale-up and self-exit | `lib/crm_demo/lifecycle/elastic_scale_probe/` |

## Project layout

Folder paths mirror module namespaces, and each folder has one job.

```text
priv/duckdb/                        trusted ATTACH script per phase
lib/crm_demo/
├── connections/warehouse.ex        the DuckDB connection
├── integrations/crm/client.ex      HTTP-shaped transport, nothing else
├── landing/crm/                    Elixir assets that extract and preserve
├── support/landing/                landing manifest and file storage
├── warehouse/source/crm/           typed relations mirroring the source
├── warehouse/core/sales/           reusable, source-independent models
├── warehouse/mart/sales/           analytics-facing models
├── contracts/                      shared output-contract columns
├── sql/                            shared SQL projection helpers
├── lifecycle/                      probes for run lifecycle behavior
└── pipelines/                      runnable entrypoints
```

Each SQL asset is one `.ex` module declaring identity, dependencies, contract,
and materialization, plus one adjacent `.sql` file holding the query. The
contract's column order and the query's projection order are the same.

## The data flow

```text
CRM API  ──►  landing/crm       ──►  warehouse/source/crm  ──►  warehouse/core/sales  ──►  warehouse/mart/sales
             .jsonl parts +          source.crm.account         core.sales.customer        mart.sales.account_health
             _manifest.json          source.crm.contact         core.sales.opportunity     mart.sales.pipeline_daily
                                     source.crm.deal            core.sales.engagement      mart.sales.executive_overview
                                     source.crm.activity
```

A relation address is `catalog.schema.name`: the catalog is the warehouse phase
and the schema is the business domain, which is the split
`Favn.RelationRef` describes. Each phase is its own attached DuckDB catalog, so
the addresses above are the real ones an operator or a report can query.

Landing is the only layer that talks to the source system. Source reads only the
files one completed manifest listed. Core reads only persisted relations. A
failed extraction writes no manifest, so nothing downstream can pick it up, and
a retry lands a new run rather than modifying the failed one.

## Compile and test

```bash
mix deps.get && mix compile && mix test
```

The tests cover the client, the landing contract, the runtime-input resolver,
the compiled manifest, and a full materialization against real DuckDB files.

## Run it

Provide a local PostgreSQL control plane, provision a workspace, then start the
stack:

```bash
mix favn.postgres.upgrade
```

```bash
mix favn.postgres.provision_workspace --id local-dev --slug local-dev --name 'Local Development'
```

```bash
mix favn.dev
```

Load the reference data, then one day of events:

```bash
mix favn.run CrmDemo.Pipelines.CrmReference --refresh force_all
```

```bash
mix favn.run CrmDemo.Pipelines.CrmDaily --window day:2026-07-23 --refresh force_all
```

The fixture contains events on 2026-07-22 and 2026-07-23 only. Without
`--window`, the daily pipeline selects the latest complete day, which is valid
but produces empty marts. Use `mix favn.backfill` for a range.

Inspect the result with any DuckDB client, after stopping the stack - the
running example owns its file-backed session. Each phase is its own database
file, so attach the one you want exactly as `priv/duckdb/mart_catalog.sql` does:

```sql
ATTACH 'crm_demo_mart.duckdb' AS mart;
select * from mart.sales.executive_overview;
```

The alias has to be `mart`. A stored view holds the address it was created with,
so opening `crm_demo_mart.duckdb` directly and querying `sales.executive_overview`
fails to bind - the view is looking for `mart.sales.*`.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DUCKDB_ADBC_DRIVER` | unset | Absolute path to a DuckDB library. Required before starting Favn. |
| `CRM_DEMO_WORKSPACE_ID` | `local-dev` | Control-plane workspace this project runs in. |
| `CRM_API_BASE_URL` | unset | Declared as optional runtime config; the stand-in client records it. |
| `CRM_API_TOKEN` | unset | Declared as an optional secret so redaction is visible in the UI. |

The connection opens an in-memory database and attaches one catalog per warehouse
phase - `crm_demo_source.duckdb`, `crm_demo_core.duckdb`, and
`crm_demo_mart.duckdb` - through the trusted `ATTACH` scripts in `priv/duckdb/`.
Every asset lives in one of those three, so the connection needs no file of its
own. Tests use the same layout under a `crm_demo_test` prefix, and landed files
go under `.data/landing/`. All of it is configured in `config/config.exs`.

Every DuckDB-writing asset uses the one-slot `duckdb` execution pool, which
serializes writes across those file-backed catalogs.

## Lifecycle probes

```bash
mix favn.run CrmDemo.Lifecycle.RetryProbe
```

Fails safely twice and succeeds on attempt three.

```bash
mix favn.run CrmDemo.Lifecycle.CancellationProbe --no-wait
```

Stays active long enough to cancel with `mix favn.runs cancel`.

`CrmDemo.Pipelines.ScheduleProbe` fires every ten seconds. New schedules are
inactive until activated with `mix favn.schedules activate`, and the scheduler
only runs when the stack was started with `mix favn.dev --scheduler`.

`CrmDemo.Lifecycle.ElasticScaleProbe.{Fast, Medium, Slow}` are independent,
side-effect-free assets that run for 20, 40, and 60 seconds. The
[production-shaped Compose qualification](../../deployment/docker-compose/README.md)
submits them as three runs before starting its scaler. Those staggered durations
make `0 -> 3 -> 2 -> 1 -> 0` elastic runner capacity observable. They are not a
benchmark and should not be copied into a customer asset graph.
