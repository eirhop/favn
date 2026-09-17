# Publish SQL catalogs from CI

Use `mix favn.catalog.publish` to make a project's public manifest and semantic
model queryable in your data platform. Run it in a short-lived, precompiled CI
project with access to the target database. It needs no orchestrator API call,
runner registration, application deployment, or running Favn services.

The two artifacts are independent. Updating metric descriptions or formulas can
publish just `semantic.json`; publishing the manifest catalog leaves the selected
semantic version unchanged. Supplying both installs them in one transaction.

## Configure the destination

Provide a compiled connection module using the ordinary `Favn.Connection`
contract. For DuckDB, its definition uses
`Favn.SQL.Adapter.DuckDB.ADBC.config_schema_fields()`.

Create a **dedicated** `config/catalog_publish.exs`:

```elixir
import Config

config :favn,
  catalog_targets: [
    analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]
  ],
  connection_modules: [warehouse: MyProject.Connections.Warehouse],
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [
        startup: [file: "priv/sql/publisher.sql"],
        catalogs: [mart: [write_concurrency: 1]]
      ]
    ]
  ]
```

The publisher's `connection_modules` configuration is an explicit **name-to-module
mapping**, unlike runtime discovery's module list. Only the chosen provider and
its runtime values are resolved. Unrelated missing secrets or provider callbacks
cannot prevent publication. The invocation owns an unregistered connection
registry; it never alters the runner's registry.

The trusted SQL startup file attaches the existing database as `mart`. For an
already-provisioned local DuckDB file, for example:

```sql
ATTACH '/warehouse/analytics.duckdb' AS mart;
```

For DuckLake, use your normal attachment, extensions, secrets and catalog
resources described in [DuckDB session scripts](duckdb-session-scripts.md).
The publisher requests only its selected catalog's resources. Do not open a
server-owned DuckDB file as another writer. Remote transports are unsupported
until separately qualified; a generic transaction capability is insufficient.

`analytics` is a logical target. Test and Production can use the same target
configuration with different connection values and promote **identical artifact
bytes**. The destination is not embedded in the artifacts. Duplicate aliases
claiming the same connection/catalog/schema boundary are rejected.

The native driver must already be installed. A dedicated config may also contain
`config :favn, :duckdb_adbc, driver: "/installed/libduckdb.so", entrypoint:
"duckdb_adbc_init"`. This is passed to this invocation without changing global
application configuration. The qualified CI combination is DuckDB 1.5.5 with the
checksum-pinned DuckLake extension in the repository CI workflow.

## Build once, publish either artifact

Execution builds additionally emit
`.favn/dist/catalog/mc_<digest>/catalog.json` and print its path. It contains public
assets, contracts, lineage, pipelines, schedules and declared policies. It is
outside the execution archive: creating this export neither changes the archive
inventory nor deploys the execution manifest. A failed export leaves the valid
execution bundle reusable on the next build.

Build semantics separately using the [semantic build workflow](sql-semantic-models.md).
For publishing, use an isolated **precompiled** project. The publisher does not
compile, run `app.config`/`app.start`, discover modules, import `runtime.exs`, or
start the customer application, runner or orchestrator. Mix itself evaluates
project build configuration before running a task; that configuration must be
suitable for CI. Never import general runtime configuration into the dedicated
publisher config. Both configuration files are trusted Elixir code.

The command owns its SQL dependencies and closes them on completion or timeout.
Cleanup survives termination of the publication worker; the caller waits up to
one additional second while cleanup continues independently. Concurrent invocations
inside the same BEAM are rejected as `catalog_runtime_busy`; separate CI containers
compete through the database transaction and expected selection instead.

An initial publication explicitly expects empty selections:

```sh
mix favn.catalog.publish \
  --manifest .favn/dist/catalog/mc_EXAMPLE/catalog.json \
  --target analytics --expect-manifest none:0

mix favn.catalog.publish \
  --semantics dist/semantics/sm_EXAMPLE/semantic.json \
  --target analytics --expect-semantics none:0
```

Subsequent publications supply the **previous version and revision**, captured
for the intended deployment before competing jobs run:

```sh
mix favn.catalog.publish \
  --semantics dist/semantics/sm_NEW/semantic.json \
  --target analytics --expect-semantics sm_PREVIOUS:7
```

Copy actual complete version IDs from the receipt or `mart.meta.selection`.
Examples abbreviate hashes for readability. Revisions are monotonic and prevent
an old job from overwriting a newer selection, including after a deliberate
rollback. On conflict, review the new selection; do not automatically reread it
and retry with a new expectation. To roll back definitions, publish a retained
artifact with the current explicit expectation. This does not roll back data.

Both `--manifest` and `--semantics` are accepted together, with an expectation for
each. `--config` overrides the dedicated config path. `--timeout-ms` defaults to
300000 and must be between 1 and 900000. Artifact limits are 64 MiB/10,000 assets
for the public manifest and 16 MiB for semantics. Relational writes are bounded
to 500 rows and 1 MiB per batch; a single oversized projected row is rejected.
Full immutable documents use their respective artifact limits.

## Query the catalog and metrics

All projected tables include `context` (`manifest` or `semantic`) and `version`.
Include **both** in joins. A semantic version carries its own asset/contract
snapshot, independent of the current manifest selection.

```sql
SELECT a.ref, a.description, c.name, c.type, c.nullable
FROM mart.meta.asset a
JOIN mart.meta.selection s
  ON s.context = a.context AND s.version = a.version
JOIN mart.meta."column" c
  ON c.context = a.context AND c.version = a.version AND c.asset_ref = a.ref
WHERE a.context = 'manifest';

SELECT m.ref, m.macro_catalog, m.macro_schema, m.macro_name,
       i.ordinal, i."column", m.canonical_sql
FROM mart.meta.metric m
JOIN mart.meta.selection s
  ON s.context = m.context AND s.version = m.version
JOIN mart.meta.metric_input i
  ON i.context = m.context AND i.version = m.version AND i.metric_ref = m.ref
ORDER BY m.ref, i.ordinal;
```

The generated tables are:

| Table | Content |
| --- | --- |
| `release` | Context/version, immutable content identity and canonical JSON document |
| `asset` | Ref, kind, description, authored relation JSON and public detail JSON |
| `column` | Asset ref, ordered columns, types, nullability, descriptions and lineage detail |
| `contract` | Asset fingerprint and complete contract JSON: grain, keys, relationships, row-count rules |
| `edge` | Declared asset dependency edges |
| `pipeline`, `schedule` | Stable ref and complete public definition JSON |
| `model` | Semantic source, dimensions, hierarchies and time rules in detail JSON |
| `metric` | Stable ref, installed macro coordinates, canonical expression and usage metadata |
| `metric_input` | Exact parameter order and source column bindings |
| `selection` | Independent selected version and revision for each context |
| `receipt` | Durable operation identity and completed JSON result |
| `catalog_schema` | Publisher schema version; not an artifact version |

The `detail` and `document` columns are canonical JSON text, queryable with
DuckDB JSON functions. The full public document is the source for rebuilding
projections. Namespace hierarchy is derived from dot-separated asset references.
Pipeline selectors are `[kind, value]` records; schedules are `{"ref": ...}` or
`{"inline": {...}}`. Policy envelopes contain declared window, coverage,
freshness, retry, placement and settings data; nested records use string keys,
enums use strings, tuples use arrays, dates/times use ISO 8601 strings, and absent
declarations use JSON null. Asset `runtime_requirements` lists non-secret scope,
field, environment-variable name and required flag. Secret declarations and all
resolved values are omitted.
Authored settings are public declarations: keep credentials in runtime connection
configuration. Executable SQL packages, source locations and resolved runtime
credentials are excluded from the manifest catalog. Semantic canonical macro
expressions are intentionally public.

For example, a published `average_price(net, units)` macro can be called as:

```sql
SELECT mart.metrics_FULL_SEMANTIC_DIGEST.sales_average_price(net, units)
FROM (VALUES (90, 3), (120, 2)) sales(net, units);
-- SUM(net) / NULLIF(SUM(units), 0) = 210 / 5 = 42
```

Use the exact macro name and full schema digest from metadata; quote identifiers
when constructing SQL. Bind arguments in `metric_input.ordinal` order. A dashboard
can pin a version or capture the selected version once per request. Readers need
only a suitable read-only database connection. Consumers own authorization,
source-relation mapping, grouping, joins, and first/last time selection.

## Success, conflicts and interrupted CI

The command emits a JSON result and exits successfully only after a committed
receipt or a proven completed replay. The result includes the operation ID,
selected versions/revisions, artifact identities, retained size/count, and
`compatibility: "unknown"`. Replaying the same completed operation returns its
original receipt **without regressing later selections**.

Metadata, macros, selection changes and the receipt are one transaction. Failed
writes with rollback preserve previous selections. Concurrent initial bootstrap
or selection conflicts fail explicitly. Existing reserved objects or unknown
catalog schema versions are rejected. Versions and macros are retained; there is
no automatic cleanup in this release. Identical semantic artifacts in different
metadata schemas share verified immutable macros in the selected catalog.

A lost commit acknowledgement is reconciled by reading the receipt on a fresh
session. If evidence is unavailable, the result is `publication_outcome_unknown`.
Do not blindly rerun a write while an earlier operation may still be running.
Use the same artifacts, target and expectations with `--reconcile` to read the
receipt without resubmitting publication. A missing receipt does not prove that a
still-running native operation cannot commit. SQL startup scripts remain trusted
connection setup; use read-only attachment configuration for recovery readers
when appropriate. Keep the original CI inputs for recovery even if the process
was killed before it could print its operation ID.

Artifact hashes validate integrity, not authorship or safe execution of arbitrary
SQL. Use artifacts from trusted builds. This command is not an untrusted upload
endpoint and does not sandbox SQL formulas.

**Selected means published definitions**, not an active execution deployment,
fresh data, or a verified compatible serving schema. Runtime readiness and
served-contract evidence belong to #721; MCP discovery belongs to #719.

Runtime freshness, checks and coverage are published automatically with managed
table writes, independently of this CI command. See
[query runtime metadata](sql-runtime-catalog.md).
