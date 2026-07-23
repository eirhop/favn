# Authoring Assets, Pipelines, And Schedules

This guide documents the public Favn DSLs you use in application code.

Start with `Favn.Asset`, `Favn.SQLAsset`, `Favn.Pipeline`, and
`Favn.Namespace`. Use `Favn.MultiAsset`, reusable SQL, and source relations when
your project needs those patterns.

## Recommended Project Layout

Use boring, predictable files. Keep connection, namespace, asset, SQL, and
pipeline modules easy to find.

```text
lib/my_app/
  connections/important_lakehouse.ex
  lakehouse.ex
  lakehouse/raw.ex
  lakehouse/raw/sales.ex
  lakehouse/raw/sales/orders.ex
  lakehouse/mart.ex
  lakehouse/mart/sales.ex
  lakehouse/mart/sales/order_summary.ex
  lakehouse/mart/sales/order_summary.sql
  integrations/shopify.ex
  pipelines/daily_sales.ex
  sql/calendar.ex
```

Recommended relation levels:

| Level | Meaning | Example |
| --- | --- | --- |
| connection | server, session, and auth config | `:important_lakehouse` |
| catalog | database or lakehouse phase | `"raw"`, `"mart"` |
| schema | domain or segment | `"sales"` |
| name | table or view | `"orders"` |

Do not use catalog and schema interchangeably in new projects.

## Namespaces

Use structural `Favn.Namespace` modules to share declarations across descendant
assets. Namespace declarations use the same macro syntax as asset declarations.

```elixir
defmodule MyApp.Lakehouse do
  use Favn.Namespace
  relation connection: :important_lakehouse
  meta owner: "data-platform"
  settings environment: "production"
end

defmodule MyApp.Lakehouse.Raw do
  use Favn.Namespace
  relation catalog: "raw"
  resources [:azure_extension]
  runtime_config MyApp.RuntimeConfigs.storage()
end

defmodule MyApp.Lakehouse.Raw.Sales do
  use Favn.Namespace
  relation schema: "sales"
  materialized :table
  runtime_inputs MyApp.Lakehouse.Raw.Inputs
end
```

Namespace modules are structural: descendants use only `Favn.Asset`,
`Favn.MultiAsset`, `Favn.SQLAsset`, or `Favn.Source`. Module ancestry selects
the namespace defaults automatically.

Namespace relation keys:

| Key | Value |
| --- | --- |
| `:connection` | atom connection name |
| `:catalog` | string or atom |
| `:schema` | string or atom |

Leaf asset modules can then use `relation true` to infer the relation name from
the module name.

Inheritance resolves root-to-leaf and then applies leaf declarations:

- `relation` merges `connection`, `catalog`, and `schema` by key.
- `resources` and `runtime_config` compose additively.
- `settings` and `meta` shallow-merge; the closest value wins per top-level key.
- `runtime_inputs`, `freshness`, `window`, `coverage`, and `materialized` use the
  closest declaration; `nil` clears an optional scalar default.
- Multi-asset child declarations apply after namespace and module defaults.

Each descendant consumes declarations from its own DSL vocabulary. SQL session
resources and materialization apply to SQL assets, while inherited metadata also
applies to source relations. Keep a default on the narrowest namespace whose
compatible descendants share it.

## Elixir Assets

Use `Favn.Asset` when the work is Elixir code.

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.Asset

  @doc "Load raw sales orders."
  settings endpoint: "/orders"
  meta owner: "data-platform"
  meta category: :sales, tags: [:raw, :daily]
  relation true
  def asset(ctx) do
    relation = ctx.asset.relation

    {:ok,
     %{
       rows_written: 0,
       relation: Enum.join([relation.catalog, relation.schema, relation.name], ".")
     }}
  end
end
```

Rules:

- Define exactly one public `asset(ctx)` function.
- Put DSL declarations before `def asset(ctx)`.
- Return `:ok`, `{:ok, metadata}`, or `{:error, reason}`.
- Do not return secrets in metadata.

Common declarations:

| Declaration | Use it for |
| --- | --- |
| `@doc` | Human description of the asset. |
| `settings` | Non-secret static values compiled into the manifest. |
| `meta` | Search/filter metadata such as `owner`, `category`, and `tags`. |
| `depends` | Upstream asset dependencies. May be repeated. |
| `window` | Time window shape for windowed work. |
| `coverage` | Historical windows expected from a windowed asset. |
| `freshness` | When Favn may skip work because output is fresh. |
| `execution_pool` | Shared execution pool name for runtime admission. |
| `relation` | Relation owned by the asset. Use `true` to infer it from namespace. |

`depends` accepts a single-asset module or a multi-asset ref:

```elixir
depends MyApp.Lakehouse.Raw.Sales.Customers
depends {MyApp.Lakehouse.Raw.Sales.Shopify, :orders}
```

`relation` accepts:

```elixir
relation true
relation [name: "orders"]
relation [connection: :warehouse, catalog: "raw", schema: "sales", name: "orders"]
```

## Settings And Runtime Context

Use `settings` for non-secret static values that belong to the asset. Repeated
declarations shallow-merge from left to right, so a later top-level key replaces
the earlier value:

```elixir
settings source: "orders"
settings request: %{path: "/v1/orders", timeout_ms: 5_000}

def asset(ctx) do
  source = ctx.asset.settings.source
  path = ctx.asset.settings.request["path"]
  MyApp.SourceClient.fetch(source, path)
end
```

Top-level keys are atoms. Nested maps use string keys because settings retain a
stable JSON-safe manifest shape. Settings are bounded to 128 top-level entries
and 64 KiB of canonical JSON, and top-level keys must be identifier-shaped
atoms no longer than 128 bytes. Keep the runtime namespaces distinct:

| Value | Runtime path |
| --- | --- |
| Asset settings | `ctx.asset.settings` |
| Pipeline settings | `ctx.pipeline.settings` |
| Per-run parameters | `ctx.params` |
| Resolved environment values and secrets | `ctx.runtime_config` |
| Descriptive metadata | not a runtime configuration bag |

Use settings to create reusable runtime patterns with your own JSON-like shape.
Favn does not impose framework-owned substructures such as `config`, `custom`,
`extra`, or `rest`; those legacy bags do not exist.

## Runtime Config In Assets

Use `runtime_config/1,2`, `env!/1,2`, and `secret_env!/1,2` when asset code needs
values from the runtime environment.

Define shared mappings once in ordinary authoring code:

```elixir
defmodule MyApp.RuntimeConfigs do
  use Favn.RuntimeConfig

  bundle :source_system,
    segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
    token: secret_env!("SOURCE_SYSTEM_TOKEN"),
    endpoint: env!("SOURCE_SYSTEM_ENDPOINT", required?: false)
end
```

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.Asset

  runtime_config MyApp.RuntimeConfigs.source_system()

  def asset(ctx) do
    segment_id = ctx.runtime_config.source_system.segment_id
    token = ctx.runtime_config.source_system.token

    MyApp.SourceClient.fetch_orders(segment_id, token)
    :ok
  end
end
```

Select bundles for a namespace with
`runtime_config MyApp.RuntimeConfigs.source_system()` after `use Favn.Namespace`.
Descendant Elixir assets, generated multi-assets, and SQL assets inherit them;
unrelated assets select the bundle explicitly. A namespace may also declare
`runtime_inputs ResolverModule` when its descendant SQL assets share the same
consumer. A SQL asset with non-empty effective runtime configuration must have
an effective resolver. Runtime configuration is never bound into SQL
automatically. Favn records unresolved references, never values. Missing
required values fail before asset code runs. Pass only the needed scope to
helper modules and never log resolved secrets.

## SQL Assets

Use `Favn.SQLAsset` when the asset is mainly SQL.

```elixir
defmodule MyApp.Lakehouse.Mart.Sales.OrderSummary do
  use Favn.SQLAsset

  @doc "Build a daily order summary."
  settings minimum_order_count: 1
  meta owner: "analytics"
  meta category: :sales
  meta tags: [:mart, :daily]
  depends MyApp.Lakehouse.Raw.Sales.Orders
  resources [:landing_storage]
  materialized :view
  relation true
  query do
    ~SQL"""
    select
      order_date,
      count(*) as order_count
    from raw.sales.orders
    group by order_date
    having count(*) >= @minimum_order_count
    """
  end
end
```

Rules:

- Use exactly one `query` declaration.
- Provide exactly one effective `materialized` declaration on the SQL asset or
  an ancestor namespace.
- Put optional `resources [...]` before `query`; names stored in the compact
  manifest index select trusted native physical-session SQL files. File paths
  and file contents are not stored in the index.
- Do not define `asset/1`; `Favn.SQLAsset` generates runtime work.
- Inline SQL must use `~SQL`.
- `~SQL` does not allow interpolation.

Query forms:

```elixir
query do
  ~SQL"select * from raw.sales.orders"
end

query file: "order_summary.sql"
```

Referenced scalar SQL settings automatically become bound parameters. A name
cannot exist in both `settings` and `ctx.params`; Favn reports the collision
instead of choosing hidden precedence. Settings cannot supply relation or
identifier placeholders such as `from @source`. Secrets are never automatic
SQL variables; expose a narrowly derived scalar through `runtime_inputs` when a
query genuinely needs one.

Materialization values:

| Value | Meaning |
| --- | --- |
| `:table` | Write a table. |
| `:view` | Create a view. |
| `{:incremental, strategy: :append}` | Append new rows. Requires `window`. |
| `{:incremental, strategy: :delete_insert, window_column: :order_date}` | Replace one window by deleting and inserting. Requires `window`. |

### DuckLake Physical Partitioning

For table or incremental assets written to DuckLake, declare the physical file
layout separately:

```elixir
materialized {:incremental,
              strategy: :delete_insert,
              window_column: :partition_month}

partitioned_by [
  :tenant_id,
  {:year, :occurred_at},
  {:month, :occurred_at},
  {:bucket, 32, :account_id}
]
```

Plain atoms or strings are identity keys. Supported structured transforms are
`{:year, column}`, `{:month, column}`, `{:day, column}`, `{:hour, column}`, and
`{:bucket, positive_count, column}`.

`window_column` controls the transactional delete scope for one incremental
window. `partitioned_by` controls DuckLake's physical layout for new files.
Neither implies the other.

Favn validates the structured declaration and referenced query columns. Other
adapters reject it explicitly. For a new DuckLake target, Favn creates the
empty table, applies the partition specification, and then inserts candidate
rows in the same transaction. For later writes, it reapplies the declared
specification; DuckLake treats an unchanged specification as a no-op.

Changing `partitioned_by` affects only subsequently written DuckLake data.
Favn does not track historical partition layouts or claim that existing files
were rewritten. Older data may retain its previous layout. Use a full Favn
rebuild when all table data should be rewritten with the current specification.

### Declare The Output Contract

Table and incremental assets may declare one typed output contract:

```elixir
contract do
  grain by: [:record_id], description: "one normalized record"

  column :record_id, :integer,
    null: false,
    from: [{MyApp.Assets.SourceRecords, :source_id}],
    via: :transformation

  column :payload, :json, from: [{"external.records", "payload"}]
  unique [:record_id]
  row_count equals: param(:expected_rows), on_violation: :fail
  row_count min: 1, when: :target_exists, on_violation: :skip_materialization
end
```

Favn validates the staged candidate's ordered column names and types before
target mutation. It generates ordinary transactional checks for non-null
columns, structured grain, unique keys, and ordered exact or bounded row-count
claims. Use a
grain description for operator-facing row identity and add `by:` when Favn
should enforce uniqueness over output columns.

Write the query's select list explicitly and use the contract to describe and
validate its output and lineage. Reuse repeated column metadata with
`Favn.SQL.ContractFragment` and `include Module`. Read
[SQL Output Contracts](sql-output-contracts.md) for logical types, all options,
policy behavior, enforcement order, durable assurance, and semantic diffing.

### Validate A SQL Materialization

Table and incremental SQL assets can run ordered checks inside the same
transaction as their materialization:

```elixir
check :candidate_has_rows,
  at: :before_materialize,
  on_violation: :fail,
  message: "The candidate must contain rows" do
  ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
end

check :known_statuses,
  at: :after_materialize,
  on_violation: :warn,
  message: "The published target contains unknown statuses" do
  ~SQL"""
  select
    count(*) filter (where status not in ('open', 'closed')) = 0 as passed,
    count(*) filter (where status not in ('open', 'closed')) as invalid_rows
  from target()
  """
end
```

`query()` is the exact staged candidate Favn will write. `target()` is the
existing target before the write and the transaction-visible modified target
afterward. Every check must return exactly one row containing a non-null native
Boolean `passed` column; other bounded scalar columns become durable metrics.

Use `on_violation: :fail` to roll back, `:warn` to commit with a quality warning,
or `:skip_materialization` before the write to keep an existing target and
commit a successful warning/no-op. Skip checks and before checks that read
`target()` require `when: :target_exists`, so first-target bootstrap can proceed
normally. Use checks and contracts with table or incremental materializations.

Read [Transactional SQL Asset Checks](sql-asset-checks.md) for the complete
option table, transaction order, result contract, persisted outcomes, limits,
and reusable `defsql` examples.

### Resolve Runtime-Only SQL Bind Parameters

Use one behaviour-based resolver when the final run window determines an
external snapshot, manifest, watermark, or other execution-specific SQL bind
value:

```elixir
defmodule MyApp.Source.Orders.Inputs do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(ctx) do
    snapshot = MyApp.SourceManifests.completed_for!(ctx.window)

    {:ok,
     %Result{
       params: %{snapshot_id: snapshot.id},
       identity: snapshot.id,
       metadata: %{file_count: length(snapshot.files)}
     }}
  end
end
```

Attach it exactly once before `query`:

```elixir
runtime_inputs MyApp.Source.Orders.Inputs
```

This declaration macro is the only supported DSL form. Do not use an anonymous
function, capture, MFA tuple, or inline resolver block. Returned values remain
ordinary bound parameters; they cannot add SQL source or identifiers. Keep
credentials in runtime configuration, mark sensitive bind names with
`sensitive_params`, and remember that retries resolve the values again rather
than reading a persisted pin.

Read [Runtime Inputs For SQL Assets](sql-runtime-inputs.md) for the complete
callback, result/error shapes, limits, timing, redaction, retry boundary, and AI
documentation breadcrumb.

## Reusable SQL

Use `Favn.SQL` for SQL fragments reused by several SQL assets.

```elixir
defmodule MyApp.SQL.Calendar do
  use Favn.SQL

  defsql orders_in_window(start_at, end_at) do
    ~SQL"""
    select *
    from raw.sales.orders
    where order_date >= @start_at
      and order_date < @end_at
    """
  end
end
```

Keep runnable asset settings such as `materialized`, `window`, and `relation`
on `Favn.SQLAsset` modules.

Read [DuckDB Session Scripts And Resources](duckdb-session-scripts.md) for
connection configuration, `{:priv, otp_app, path}` locators, `@name` script
parameters, physical-session lifecycle, and safety guidance. Do not put durable
business writes in a session script.

## Multi-Assets

Use `Favn.MultiAsset` when many similar assets share one runtime function.

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Shopify do
  use Favn.MultiAsset

  settings method: "GET"
  settings request: %{primary_key: "id"}
  meta owner: "data-platform"
  meta category: :shopify, tags: [:raw]
  relation true
  freshness :daily

  asset :orders do
    description "Extract Shopify orders."
    settings path: "/orders.json", data_path: "orders"
  end

  asset :customers do
    description "Extract Shopify customers."
    settings path: "/customers.json", data_path: "customers"
    freshness :always
  end

  @doc "Execute one Shopify extraction."
  def asset(ctx) do
    MyApp.Shopify.Client.extract(ctx.asset.settings, ctx)
  end
end
```

Rules:

- Define exactly one public `asset(ctx)` function.
- Define at least one `asset :name do ... end` block.
- Each generated asset is referenced as `{Module, :name}`.
- Put all shared declarations before the first child.
- Shared `settings` and `meta` are shallow-merged with child declarations.
- Shared dependencies are combined with child dependencies.
- A child scalar such as `freshness`, `window`, `coverage`, `retry`,
  `execution_pool`, or `relation` overrides the shared value; explicit `nil`
  clears it.
- Nested settings maps replace as one top-level value rather than deep-merging.
- Module-level runtime config applies to every generated asset.
- Use `description` inside children and reserve `@doc` for the shared real
  `asset/1` function.

Inside `Favn.MultiAsset`, `depends :other_asset` references another generated
asset in the same module. Use `{OtherModule, :asset_name}` for another module.

## Pipelines

Use `Favn.Pipeline` to name a run target.

```elixir
defmodule MyApp.Pipelines.DailySales do
  use Favn.Pipeline

  pipeline :daily_sales do
    asset MyApp.Lakehouse.Mart.Sales.OrderSummary
    deps :all
    schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :one
  end
end
```

Rules:

- Define one `pipeline :name do ... end` per module.
- Use direct `asset`/`assets` selection or a `select do ... end` block. Do not mix
  the two styles in one pipeline.
- Selectors are additive and deduplicated.
- Execution order still comes from dependencies between assets.

Pipeline clauses:

| Clause | Meaning |
| --- | --- |
| `asset Module` | Select one single asset module. |
| `asset {Module, :name}` | Select one multi-asset ref. |
| `assets [...]` | Select many assets. |
| `select do ... end` | Select by asset, module prefix, tag, or category. |
| `deps :all` | Include upstream dependencies. |
| `deps :none` | Run only selected targets. |
| `settings %{...}` | Non-secret static values at `ctx.pipeline.settings`. |
| `meta %{...}` | Descriptive metadata for operators and tooling; keys normalize to strings. |
| `schedule ...` | Inline or named schedule. |
| `window ...` | Runtime window policy. |
| `max_concurrency N` | Limit parallel asset steps in one run. |
| `execution_pool :name` | Default execution pool for selected assets. |
| `source :name` | Source label for the pipeline. |
| `outputs [:name]` | Output labels for tooling. |

Selector block example:

```elixir
select do
  module MyApp.Lakehouse.Mart
  tag :daily
  category :sales
end
```

Pipeline metadata is JSON-like descriptive data, not runtime configuration.
Repeated `meta` declarations shallow-merge, and their keys normalize to strings
so the values remain identical after manifest and run-snapshot persistence.

## Schedules

Add an inline schedule to a pipeline:

```elixir
schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :one, overlap: :forbid
```

Schedule options:

| Option | Values | Default |
| --- | --- | --- |
| `:cron` | 5-field cron or 6-field cron with leading seconds | required |
| `:timezone` | IANA timezone string | application default (`"Etc/UTC"` fallback) |
| `:missed` | `:skip`, `:one`, `:all` | `:skip` |
| `:overlap` | `:forbid`, `:allow`, `:queue_one` | `:forbid` |
| `:active` | boolean | `true` |

You can also reference a named schedule:

```elixir
schedule {MyApp.Schedules, :daily}
```

## Windows

Use asset windows when work should run for a time range.

```elixir
window Favn.Window.daily(timezone: "Europe/Oslo")
```

Asset window constructors:

| Constructor | Options |
| --- | --- |
| `Favn.Window.hourly/1` | `refresh_from`, `required`, `timezone` |
| `Favn.Window.daily/1` | `refresh_from`, `required`, `timezone` |
| `Favn.Window.monthly/1` | `refresh_from`, `required`, `timezone` |
| `Favn.Window.yearly/1` | `refresh_from`, `required`, `timezone` |

Option notes:

- `required: true` means a runtime window must be supplied.
- An omitted `timezone` uses `config :favn, :default_timezone`, then
  `"Etc/UTC"`.

Pipeline window policy:

```elixir
schedule cron: "0 2 * * *", timezone: "Europe/Oslo"
window :monthly, anchor: :current_period, lookback: 1
```

Supported kinds: `:hourly`, `:daily`, `:monthly`, `:yearly`, or `:hour`, `:day`,
`:month`, `:year`.

Pipeline window options:

| Option | Meaning |
| --- | --- |
| `:anchor` | `:previous_complete_period` (default) or `:current_period`. |
| `:timezone` | IANA timezone. |
| `:lookback` | Non-negative number of prior anchors added to scheduled runs. |
| `:allow_full_load` | Whether a full load without a window is allowed. |

Schedule cadence and processing-window granularity are independent. The example
runs daily while selecting the current monthly anchor. Manual runs and backfills
must still request monthly windows.

Operational lookback belongs to the pipeline. Scheduled July with `lookback: 1`
selects June and July. Explicit manual windows and backfill ranges stay exact.
The asset describes only how each supplied anchor maps to its canonical data
window:

```elixir
window Favn.Window.monthly(
  refresh_from: :day,
  required: true,
  timezone: "Europe/Oslo"
)
```

`refresh_from: :day` tracks today's window-success separately for every exact
month selected by the pipeline. Do not replace this with explicit `freshness
:daily`, which is one asset-wide daily success.

Run input and operator responses preserve the requested anchors, the permitted
expansion, and the effective anchors separately. This makes scheduled lookback,
manual runs, backfills, reruns, and retries deterministic and visible without
asking the runner to resolve window policy again.

## Coverage

Coverage declares which canonical windows a windowed asset is expected to have:

```elixir
coverage from: ~D[2020-01-01],
         through: :latest_closed,
         availability_delay: {:hours, 6}
```

`from` is required and accepts a `Date` or timezone-aware `DateTime`. `through`
defaults to `:latest_closed` and also accepts `:current`, `Date`, or
timezone-aware `DateTime`. Fixed boundaries are inclusive. Hourly authored
boundaries must be `DateTime` values.

`availability_delay` accepts a non-negative `{unit, amount}` using seconds,
minutes, hours, or days, and is valid only with `:latest_closed`. It delays when
a closed window becomes expected; it does not delay execution, create a timer,
or change retries.

Manifest builds warn when a recurring cron occurrence is provably earlier than
the availability delay for a selected asset. They also warn when a windowed
pipeline and selected windowed asset use different effective timezones. These
warnings explain potentially surprising behavior but do not block compilation
or scheduled execution.

Coverage has no kind or timezone. It uses the effective asset window. Declaring
coverage without an effective window is a compile error. A windowed asset
without coverage remains valid and reports unknown coverage.

Coverage is available on `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, and
`Favn.Namespace`. It is one scalar policy: the closest declaration replaces the
whole inherited policy, omission inherits it, and `coverage nil` explicitly
clears it.

## Freshness

Freshness tells Favn when work may be skipped because output is already current.

```elixir
freshness :daily
freshness {:daily, timezone: "Europe/Oslo"}
freshness [max_age: {:hours, 6}]
freshness [window_success: true]
freshness :always
```

Notes:

- Windowed assets default to window-success freshness unless you set another
  policy.
- Non-windowed assets have no implicit freshness.
- Use `:always` when the asset should run whenever selected.

## Source Relations

Use `Favn.Source` for an external table or view that Favn should know about but
should not execute.

```elixir
defmodule MyApp.Lakehouse.Raw.Payments.StripeCharges do
  @moduledoc "External raw Stripe charges table."

  use Favn.Source

  meta owner: "data-platform"
  meta category: :payments
  meta tags: [:raw]
  relation connection: :important_lakehouse,
           catalog: "raw",
           schema: "payments",
           name: "stripe_charges"
end
```

Rules:

- Define no functions.
- Declare exactly one `relation`.
- Use `@moduledoc` for the source description and `meta` for operator context.

## Next Steps

- Read [Configuration](configuration.md) for discovery, local runtime, DuckDB,
  pooling, and env file options.
- Read [SQL Client](sql-client.md) when Elixir assets need to run SQL.
- Use [Cheatsheet](cheatsheet.cheatmd) for quick reminders.
