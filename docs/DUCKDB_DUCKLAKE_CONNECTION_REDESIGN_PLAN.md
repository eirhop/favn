# DuckDB/DuckLake Connection Redesign Plan

## Goal

Redesign Favn's DuckDB connection runtime config so it mirrors DuckDB's own
session model:

- open one DuckDB session database
- load extensions
- apply settings
- create secrets
- attach named persistent catalogs
- select an optional default catalog with `USE`

The important split is that `open.database` identifies the DuckDB session
database, while `duckdb.attach` identifies persistent catalogs where Favn assets
read and write data. For production DuckLake usage, the recommended session
database is `":memory:"` because persistence lives in attached DuckLake catalogs
or attached DuckDB files.

## Current State

- `Favn.RelationRef` already models `connection`, `catalog`, `schema`, and
  `name`, and `Favn.SQLAsset.Renderer` already renders catalog-qualified
  relations as `catalog.schema.name`.
- SQL assets execute through `Favn.SQLAsset.Runtime`, which opens a SQL session
  per asset execution through `Favn.SQL.Client.connect/2`, materializes, and
  disconnects unless default-on runner-local pooling for poolable DuckDB/ADBC
  adapters keeps a compatible warm session idle for reuse. Checked-out sessions
  remain exclusive to one asset execution at a time.
- DuckDB bootstrap is currently adapter-owned under `:duckdb_bootstrap` and runs
  per session after `adapter.connect/2` succeeds.
- DuckDB bootstrap currently supports one attached catalog shape, but the attach
  config is not keyed by catalog name.
- SQL admission currently resolves one `Favn.SQL.ConcurrencyPolicy` per
  connection and keys permits by `{:connection, connection_name}`. This is too
  coarse for a connection that attaches both concurrent DuckLake catalogs and a
  single-writer DuckDB file catalog.
- `mix favn.doctor` currently checks config shape, connection module availability,
  runtime config presence, plugin availability, and manifest generation. It does
  not validate asset relation catalogs against adapter runtime config.

## Public Config Schema

Recommended final runtime config shape:

```elixir
config :favn,
  connections: [
    lakehouse: [
      open: [
        database: ":memory:"
      ],

      pool: [
        enabled: true,
        max_idle_per_key: 1,
        idle_timeout_ms: 300_000
      ],

      duckdb: [
        load: [:ducklake, :postgres, :azure, :json],

        settings: [
          azure_transport_option_type: :curl
        ],

        secrets: [
          azure_lakehouse: [
            type: :azure,
            provider: :credential_chain,
            account_name: "examplelakehouse",
            chain: :cli
          ],

          lakehouse_raw_meta: [
            type: :postgres,
            host: "example-meta.postgres.database.azure.com",
            port: 5432,
            database: "lakehouse_raw_meta",
            user: "favn_app",
            auth: [type: :azure_postgres_entra, provider: :azure_cli],
            sslmode: :require
          ],

          lakehouse_int_meta: [
            type: :postgres,
            host: "example-meta.postgres.database.azure.com",
            port: 5432,
            database: "lakehouse_int_meta",
            user: "favn_app",
            auth: [type: :azure_postgres_entra, provider: :azure_cli],
            sslmode: :require
          ]
        ],

        attach: [
          raw: [
            type: :ducklake,
            metadata: "ducklake:postgres:",
            meta_secret: :lakehouse_raw_meta,
            data_path: "abfss://examplelakehouse.dfs.core.windows.net/ducklake/raw/",
            write_concurrency: :unlimited
          ],

          int: [
            type: :ducklake,
            metadata: "ducklake:postgres:",
            meta_secret: :lakehouse_int_meta,
            data_path: "abfss://examplelakehouse.dfs.core.windows.net/ducklake/int/",
            write_concurrency: :unlimited
          ],

          mart: [
            type: :duckdb,
            path: ".data/mart/mart.duckdb",
            write_concurrency: 1
          ]
        ],

        use: :raw
      ]
    ]
  ]
```

Recommended connection definition shape:

```elixir
defmodule MyApp.Connections.Lakehouse do
  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :lakehouse,
      adapter: Favn.SQL.Adapter.DuckDB.ADBC,
      doc: "DuckDB session with DuckLake raw/int and local mart catalogs.",
      metadata: %{engine: :duckdb},
      config_schema: Favn.SQL.Adapter.DuckDB.ADBC.config_schema_fields()
    }
  end
end
```

Adapter-owned schema fields should replace the current consumer-visible
combination of `%{key: :database, ...}` and `bootstrap_schema_field/0`. A single
helper is better because `open` and `duckdb` must be validated together.

`pool` is connection-level runtime config, not nested DuckDB bootstrap SQL. It
controls local reuse of warm sessions after a successful connect and bootstrap.
Pooling is enabled by default for poolable DuckDB/ADBC adapters. Disable with:

```elixir
pool: [enabled: false]
```

The supported tuning shape is intentionally small:

```elixir
pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]
```

Pooling is runner-local/per-BEAM. It is not a distributed pool and does not
coordinate across runner nodes, increase catalog/write concurrency, or make raw
write/materialization failures safe to retry.

`duckdb.load` should be the first public shape. Do not keep public `install` in
the redesign unless dogfooding proves it is necessary. Extension installation is
an operational concern and mutates the DuckDB extension directory; the runtime
bootstrap should primarily mirror the executable session SQL in the acceptance
criteria. If a later local-dev convenience needs install, add explicit
`duckdb.install` ordered before `duckdb.load` as a separate change.

## Boundary Split

Connection definition modules should contain static contract metadata:

- connection name
- adapter module
- docs and metadata
- adapter-owned runtime config schema and validation hook

Runtime config should contain deployment-specific DuckDB session setup:

- session database under `open.database`
- default-on runner-local session reuse under connection-level `pool`
- extension loads under `duckdb.load`
- settings under `duckdb.settings`
- secrets under `duckdb.secrets`
- attached catalogs under `duckdb.attach`
- optional default catalog under `duckdb.use`
- per-catalog write concurrency under each attach entry

The `favn` public DSL should not learn DuckDB internals. The generic connection
loader should keep accepting adapter-provided custom schema fields. The DuckDB
adapter packages should own parsing, normalization, SQL generation, redaction,
and diagnostics for `open` and `duckdb`.

## Internal Structs And Modules

Add shared DuckDB config modules in the DuckDB adapter boundary, then have both
DuckDB adapters use them:

- `Favn.SQL.Adapter.DuckDB.Config`
- `Favn.SQL.Adapter.DuckDB.Config.Open`
- `Favn.SQL.Adapter.DuckDB.Config.Bootstrap`
- `Favn.SQL.Adapter.DuckDB.Config.Setting`
- `Favn.SQL.Adapter.DuckDB.Config.Secret`
- `Favn.SQL.Adapter.DuckDB.Config.Attach`
- `Favn.SQL.Adapter.DuckDB.Config.WriteConcurrency`
- `Favn.SQL.Adapter.DuckDB.Config.Pool`
- `Favn.SQL.Adapter.DuckDB.Bootstrap.SQL`
- `Favn.SQL.Adapter.DuckDB.Bootstrap.Step`

Suggested structs:

```elixir
%Favn.SQL.Adapter.DuckDB.Config{
  open: %Open{database: ":memory:"},
  pool: %Pool{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000},
  bootstrap: %Bootstrap{
    load: ["ducklake", "postgres", "azure", "json"],
    settings: [%Setting{name: "azure_transport_option_type", value: "curl"}],
    secrets: [%Secret{name: "lakehouse_raw_meta", type: :postgres, ...}],
    attach: %{
      "raw" => %Attach{
        name: "raw",
        type: :ducklake,
        metadata: "ducklake:postgres:",
        meta_secret: "lakehouse_raw_meta",
        data_path: "abfss://.../raw/",
        write_concurrency: :unlimited
      },
      "mart" => %Attach{
        name: "mart",
        type: :duckdb,
        path: ".data/mart/mart.duckdb",
        write_concurrency: 1
      }
    },
    use: "raw"
  }
}
```

Keep these modules adapter-owned rather than placing DuckDB-specific structs in
`favn_core`. The generic SQL runtime only needs a normalized way to ask the
adapter for write admission scopes.

Add a generic adapter callback for admission policy lookup:

```elixir
@callback concurrency_policies(Favn.Connection.Resolved.t()) ::
            {:ok, [Favn.SQL.ConcurrencyPolicy.t()]} | {:error, Favn.SQL.Error.t()}
```

This should replace or wrap the current singular `default_concurrency_policy/1`
concept for adapters that need multiple scopes.

## Bootstrap SQL Generation

The new bootstrap should generate ordered steps from normalized config:

```sql
LOAD ducklake;
LOAD postgres;
LOAD azure;
LOAD json;
SET azure_transport_option_type = 'curl';
CREATE SECRET azure_lakehouse (...);
CREATE SECRET lakehouse_raw_meta (...);
CREATE SECRET lakehouse_int_meta (...);
ATTACH 'ducklake:postgres:' AS raw
  (DATA_PATH 'abfss://.../raw/', META_SECRET lakehouse_raw_meta);
ATTACH 'ducklake:postgres:' AS int
  (DATA_PATH 'abfss://.../int/', META_SECRET lakehouse_int_meta);
ATTACH '.data/mart/mart.duckdb' AS mart;
USE raw;
```

Generation rules:

- `open.database` is consumed by `adapter.connect/2`, not emitted as `.open` SQL.
- `duckdb.load` emits `LOAD <identifier>` in declared order.
- `duckdb.settings` emits `SET <identifier> = <literal>` in declared order.
- `duckdb.secrets` emits `CREATE SECRET <identifier> (...)` in declared order.
- Azure secret `scope` is optional. It should only be emitted as DuckDB `SCOPE`
  when a deployment needs DuckDB to choose between multiple Azure secrets by URI
  prefix. It is not a substitute for DuckLake `DATA_PATH` and should not be
  required just because an attached catalog has a data path.
- `duckdb.attach` emits one `ATTACH` per catalog in declared order.
- `duckdb.use` emits one `USE <identifier>` after all attaches.
- All identifiers must be validated before SQL generation and quoted safely when
  emitted.
- Each step must carry `statement`, `safe_statement`, `kind`, and `id` so tests,
  diagnostics, and docs can show the reproducible SQL without leaking secrets.

DuckLake attach generation should prefer the requested SQL shape:

```sql
ATTACH 'ducklake:postgres:' AS raw
(DATA_PATH 'abfss://.../ducklake/raw/', META_SECRET lakehouse_raw_meta);
```

The existing Azure PostgreSQL Entra token flow should remain on the PostgreSQL
secret materialization path. Tokens are fetched immediately before `CREATE
SECRET`, inserted as temporary `PASSWORD`, redacted in diagnostics, and never
stored in normalized config.

## Validation Strategy

Validation should run in adapter-owned config normalization and be reachable via
connection schema validation, `Favn.Connection.Loader`, runtime connection, and
doctor checks.

Required validations:

- `open.database` is present and is either `":memory:"` or a non-empty local path.
- Reject old top-level `:database` and `:duckdb_bootstrap` keys with an error that
  names the new `open` and `duckdb` keys.
- `duckdb.load` entries are valid DuckDB identifiers.
- `duckdb.settings` names are valid DuckDB identifiers and supported setting
  values are normalized.
- `duckdb.secrets` names are valid DuckDB identifiers.
- `duckdb.attach` catalog names are valid DuckDB identifiers.
- `duckdb.use`, when present, references an attached catalog.
- Each `type: :ducklake` attach has `metadata`, `meta_secret`, and `data_path`.
- Each `type: :duckdb` attach has `path`.
- `meta_secret` references a defined secret with `type: :postgres`.
- Catalog write concurrency values are `:unlimited`, `:single`, `1`, or a positive
  integer.
- DuckDB file attaches default to `write_concurrency: 1`.
- DuckLake attaches default to `write_concurrency: :unlimited`, documented as the
  scalable default but still configurable if a deployment discovers metadata or
  storage contention.
- `pool.enabled` is a boolean. `pool.max_idle_per_key` and
  `pool.idle_timeout_ms` are positive integers when pooling is enabled.
- Pool config belongs at the connection level; reject `duckdb.pool` so bootstrap
  SQL remains separate from session lifecycle policy.
- Secrets and sensitive paths are redacted in diagnostics and bootstrap errors.

Azure `SCOPE` guidance:

- Omit `scope` when one Azure secret covers the session's Azure storage access.
- Use `scope` only when multiple Azure secrets exist for different storage
  accounts, containers, filesystems, or prefixes.
- When configured, `scope` must match DuckDB's Azure secret rules and include a
  trailing slash.
- Prefer the broadest correct prefix, for example the shared lake root, rather
  than repeating each attach `data_path`.
- Keep `data_path` on each DuckLake attach because it defines where that catalog
  stores data.

Asset catalog validation should be manifest-level because it needs both runtime
connection config and compiled assets:

- Build the manifest.
- Resolve required connections through `Favn.Connection.Loader.resolve_required/1`.
- For each asset relation with `connection` and `catalog`, ask the adapter for
  configured catalog names.
- Report an error when an asset uses a catalog not present in `duckdb.attach`.
- Treat assets with no catalog as valid only if the connection has a `duckdb.use`
  default or the adapter explicitly supports unqualified writes.

Add an optional adapter callback for this rather than hard-coding DuckDB in
doctor:

```elixir
@callback configured_catalogs(Favn.Connection.Resolved.t()) ::
            {:ok, MapSet.t(String.t())} | {:error, Favn.SQL.Error.t()}
```

`mix favn.doctor` can then add a check named `relation catalogs` that validates
asset relation catalogs against configured runtime catalogs for all adapters that
export the callback.

## Migration Recommendation

Make this a clean breaking change.

Favn is private pre-v1 software, the old shape is actively misleading, and the
new model changes admission semantics. Supporting both shapes would preserve the
confusion and complicate bootstrap, diagnostics, docs, and tests. Do not carry a
compatibility translator.

Recommended migration behavior:

- Remove public docs for top-level `database`, connection-level DuckDB
  `write_concurrency`, and `duckdb_bootstrap`.
- Change DuckDB adapter schema helpers to require `open` and accept `duckdb`.
- Return a compile/config validation error if old keys are present.
- Error message: `DuckDB connection config now uses open: [database: ...] and duckdb: [...]; move duckdb_bootstrap entries under duckdb and move write_concurrency under duckdb.attach.<catalog>.write_concurrency`.
- Keep no runtime fallback, but include one docs example showing old-to-new
  mapping for the single-catalog case.

Single-catalog DuckLake migration:

```elixir
connections: [
  lakehouse: [
    open: [database: ":memory:"],
    duckdb: [
      load: [:ducklake, :postgres, :azure, :json],
      secrets: [...],
      attach: [
        raw: [
          type: :ducklake,
          metadata: "ducklake:postgres:",
          meta_secret: :lakehouse_meta,
          data_path: "abfss://...",
          write_concurrency: :unlimited
        ]
      ],
      use: :raw
    ]
  ]
]
```

## Scheduler And Admission Control

Admission should be keyed by write target, not by connection:

```elixir
{connection_name, catalog_name}
```

Current SQL admission is acquired at session connect for the whole connection,
then also wraps write operations. That model serializes too much for DuckDB when
one session has multiple attached catalogs. The redesigned flow should acquire
write permits at write operation time using the write target.

Recommended SQL runtime changes:

- Let `Favn.SQL.ConcurrencyPolicy` represent one scope, but allow a connection to
  resolve multiple policies.
- Store policies on `%Favn.SQL.Session{}` as a map keyed by target, for example
  `%{{:catalog, "raw"} => policy, {:catalog, "mart"} => policy}`.
- Remove write-only policy acquisition from `SQLClient.connect/2`; keep connect
  acquisition only for future policies with `applies_to: :connect` or `:all`.
- For `SQLClient.materialize/3`, derive the target catalog from
  `%Favn.SQL.WritePlan{connection, target: %Favn.SQL.Relation{catalog: catalog}}`.
- Acquire `{:duckdb_catalog, connection_name, catalog}` for writes to configured
  catalogs.
- For raw `SQLClient.execute/3`, use explicit operation targets such as
  `catalog: "raw"` or the retained session `required_catalogs` scope for
  catalog-aware admission. Favn does not parse arbitrary SQL text to infer target
  catalogs.
- Preserve the existing process-local reentrant permit behavior so materialization
  plans using transactions do not deadlock when nested execute calls happen under
  the same target scope.

Recommended DuckDB policy defaults:

- `type: :duckdb` attach defaults to `1` to protect attached DuckDB files.
- `type: :ducklake` attach defaults to `:unlimited`, with docs that operators can
  reduce this if their DuckLake metadata/storage path requires throttling.
- Connection-level `write_concurrency` should be rejected for DuckDB. It remains
  available for other SQL adapters until they adopt catalog policies.

The orchestrator can keep topological stage parallelism. It already submits
independent stage siblings concurrently. The runner already starts a worker per
submitted asset, and SQL asset runtime already opens a fresh SQL session for each
asset materialization. That means scalable DuckLake writes mostly require fixing
the SQL admission scope rather than changing the run scheduler first.

Multiple DuckDB sessions are required for scalable parallel DuckLake execution.
Each worker should open its own `":memory:"` DuckDB session, run the same
bootstrap, attach the same catalogs, and write to its target DuckLake catalog.
Attached DuckDB file catalogs should still serialize writes through
catalog-level admission because independent sessions can contend on the same
file lock.

Default-on pooling for poolable DuckDB/ADBC adapters changes session lifecycle
cost, not concurrency semantics. A pool may keep successfully bootstrapped
sessions warm after use, but a checked-out session is still exclusive to one asset
execution. Disable with `pool: [enabled: false]`. Existing catalog/write
concurrency must bound both active work and new session/bootstrap. Pooling must
not allow more concurrent writes than the configured catalog policy. The SQL
client must enforce checkout ownership so copied session structs cannot operate
on or disconnect a pooled session from another process.

Pool reuse keys must include all inputs that can affect session safety:

- connection identity
- normalized connection/config hash after runtime refs are resolved
- required catalog set used for scoped bootstrap
- adapter fingerprint, including adapter module and implementation version or
  equivalent compatibility marker

The pool is local to one runner BEAM. It does not solve multi-runner distributed
DuckLake metadata pressure by itself.

## Safe Retry Boundaries

Retries must be bounded and operation-aware:

- Safe to retry with small bounded attempts: session creation, DuckDB bootstrap
  before handing the session to an execution, and read-only inspection/query
  operations whose adapter path is known to be read-only.
- Not safe to retry blindly: SQL writes, materialization writes, appender writes,
  `COPY`, DDL, or any operation whose commit state is unknown.
- Unknown commit state must be surfaced as an unknown-outcome failure rather than
  retried.

This rule matters most for separate-process DuckDB workers and ADBC calls where a
timeout or worker-call failure may mean the DuckDB operation is still running or
has already committed.

## Azure PostgreSQL DuckLake Metadata Guidance

Pooling can reduce repeated DuckDB session/bootstrap cost inside one runner, but
DuckLake metadata writes still hit the configured PostgreSQL metadata service.
Low-tier Azure PostgreSQL deployments should use conservative DuckLake catalog
`write_concurrency` values, monitor connection and lock pressure, and consider
PgBouncer or scaling the metadata database. Pooling and single-flight creation
reduce repeated attach/bootstrap pressure but are not a replacement for
metadata-tier capacity planning, especially with multiple runner BEAMs.

## Documentation Plan

Update these docs when implementing:

- `README.md`: replace the single-catalog bootstrap example with the new
  `open`/`duckdb` shape and call out `":memory:"` for DuckLake.
- `docs/FEATURES.md`: describe DuckDB session database versus attached persistent
  catalogs, catalog-level write admission, runner-local pooling, and retry
  boundaries.
- `docs/ROADMAP.md`: update the DuckLake bootstrap item to point at multi-catalog
  DuckDB runtime config.
- `docs/structure/favn_duckdb.md`: document ownership of config parsing,
  bootstrap SQL generation, and diagnostics for the duckdbex adapter.
- `docs/structure/favn_duckdb_adbc.md`: same for ADBC, including Entra token
  support.
- `docs/structure/favn_sql_runtime.md`: document catalog-level SQL admission
  scope, policy lookup, default-on runner-local pooling, and safe retry
  boundaries.
- `Favn.Connection` moduledoc: remove old `database` and `duckdb_bootstrap`
  guidance and show the connection definition helper.
- DuckDB adapter moduledocs: include the raw/int/mart example and a manual SQL
  reproduction section.

Docs should include this mental model:

```text
DuckDB session database: open.database = ":memory:"
Persistent raw catalog: ATTACH 'ducklake:postgres:' AS raw (...)
Persistent int catalog: ATTACH 'ducklake:postgres:' AS int (...)
Persistent mart catalog: ATTACH '.data/mart/mart.duckdb' AS mart
Favn relation: connection=:lakehouse catalog="raw" schema="analytics" name="events"
DuckDB relation: raw.analytics.events
```

Docs should also show how to debug by copying sanitized bootstrap SQL from
diagnostics, replacing redacted values locally, and running it in a DuckDB shell
in the same order as Favn.

## Testing Strategy

Add focused tests at the owning layer:

- DuckDB config normalization tests for `open`, `duckdb.load`, settings, secrets,
  multi-attach, defaults, `use`, and old-key rejection.
- DuckDB bootstrap SQL generation tests proving exact order: `LOAD`, `SET`,
  `CREATE SECRET`, `ATTACH`, `USE`.
- DuckDB bootstrap diagnostic tests proving `safe_statement` redacts metadata DSNs,
  ADLS paths, account names when secret, passwords, and Entra tokens.
- ADBC token tests proving Azure PostgreSQL Entra token support still materializes
  PostgreSQL `CREATE SECRET` with temporary `PASSWORD` and redacted errors.
- SQL runtime admission tests proving two writes to `{lakehouse, raw}` can run
  concurrently when unlimited, two writes to `{lakehouse, mart}` serialize when
  the limit is `1`, and writes to raw do not block writes to mart.
- Runner SQL asset tests proving `relation.catalog = "raw"` produces a write plan
  targeting the raw catalog and queries can render three-part cross-catalog
  references.
- Doctor tests proving manifest asset catalogs are validated against configured
  `duckdb.attach` catalogs.
- Regression tests proving a single-catalog DuckLake config is representable in
  the new shape.

Avoid browser or end-to-end tests for this feature unless a UI diagnostic surface
is changed. Most acceptance criteria are adapter, SQL runtime, runner, and doctor
layer concerns.

## Implementation Plan And PR Slices

The current implementation is being carried as one cohesive PR because config
shape, bootstrap, catalog admission, session pooling, and retry semantics are
tightly coupled. The PR should still keep changes layered internally:

1. Add adapter-owned DuckDB config structs and normalization, including `open`,
   connection-level `pool`, `duckdb`, old-key rejection, and recursive runtime-ref
   redaction.
2. Add `config_schema_fields/0` to both DuckDB adapters and update connect paths
   to read `open.database`.
3. Rewrite bootstrap step generation to consume normalized `duckdb` config and
   support multiple keyed attaches.
4. Preserve Azure credential-chain and PostgreSQL Entra token secret
   materialization in the new secret structs.
5. Add adapter callbacks for configured catalogs and catalog concurrency policies.
6. Refactor `Favn.SQL.ConcurrencyPolicy`, `Favn.SQL.Session`, and
   `Favn.SQL.Admission` so write operations acquire permits by
   `{connection_name, catalog_name}` and bootstrap honors required catalogs.
7. Add runner-local session pooling keyed by connection/config hash, required
   catalog set, and adapter fingerprint, with exclusive checkout semantics and
   idle eviction.
8. Add bounded, operation-aware retries only for session creation/bootstrap and
   read-only inspection/query; surface unknown commit state without retry.
9. Update `Favn.SQLAsset.Runtime` and materialization planning only as needed to
   expose the target catalog to admission; the renderer already emits
   three-part names.
10. Add `mix favn.doctor` relation catalog validation through an adapter callback,
    not DuckDB-specific branching in doctor.
11. Update docs and generated local init examples from `database` and
    `duckdb_bootstrap` to `open` and `duckdb`, and document default-on `pool`.
12. Run focused tests during implementation, then run `mix format`,
    `mix compile --warnings-as-errors`, and `mix test` before merge.

## Risks And Open Questions

- DuckLake concurrent writes should be verified against the deployed DuckDB and
  DuckLake versions. The architecture permits parallel sessions, but metadata
  catalog behavior and object-store consistency still need dogfooding.
- Attached DuckDB file catalogs are likely unsafe for parallel writes from
  multiple sessions. Defaulting to `write_concurrency: 1` is required, and tests
  should treat this as a contract.
- Raw `SQLClient.execute/3` cannot reliably infer a write catalog from arbitrary
  SQL. Manual execute paths should use session `required_catalogs` or explicit
  operation catalog targets when catalog-scoped admission is required.
- Extension `INSTALL` is intentionally omitted from the new public shape. If local
  dogfooding requires it, add it explicitly rather than reviving the old
  `extensions: [install: ..., load: ...]` nesting.
- Bootstrap cost may increase when pooling is disabled with `pool: [enabled:
  false]` or no compatible idle session exists. Pooling can reduce this cost
  inside one runner BEAM, but correctness still comes from exclusive checkout,
  catalog admission, and default discard of raw execute/materialize/transaction
  mutation paths, not from shared mutable sessions.
- Pooling reduces local bootstrap churn but can hide backend metadata pressure in
  one BEAM while other runners still create their own sessions. Do not document it
  as a distributed DuckLake scaling solution.
- Low-tier Azure PostgreSQL metadata catalogs may need conservative DuckLake
  `write_concurrency`, PgBouncer, or database scaling even when pooling is
  enabled.
- Runtime config refs and redaction must continue to work recursively inside the
  new nested `duckdb` shape. Any diagnostic output should use normalized safe
  statements and `Favn.RuntimeConfig.Redactor` rather than inspecting raw config.
- `duckdb.attach` order matters for predictable diagnostics and manual SQL
  reproduction. Keyword config preserves order; map config should either be
  normalized deterministically or discouraged for ordered bootstrap sections.
