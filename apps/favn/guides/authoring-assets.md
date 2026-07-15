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

Use `Favn.Namespace` to share relation defaults across many assets.

```elixir
defmodule MyApp.Lakehouse do
  use Favn.Namespace, relation: [connection: :important_lakehouse]
end

defmodule MyApp.Lakehouse.Raw do
  use Favn.Namespace, relation: [catalog: "raw"]
end

defmodule MyApp.Lakehouse.Raw.Sales do
  use Favn.Namespace, relation: [schema: "sales"]
end
```

Supported namespace relation keys:

| Key | Value |
| --- | --- |
| `:connection` | atom connection name |
| `:catalog` | string or atom |
| `:schema` | string or atom |

Leaf asset modules can then use `@relation true` to infer the relation name from
the module name.

## Elixir Assets

Use `Favn.Asset` when the work is Elixir code.

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.Asset

  @doc "Load raw sales orders."
  @meta owner: "data-platform", category: :sales, tags: [:raw, :daily]
  @relation true
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
- Put DSL attributes directly above `def asset(ctx)`.
- Return `:ok`, `{:ok, metadata}`, or `{:error, reason}`.
- Do not return secrets in metadata.

Common attributes:

| Attribute | Use it for |
| --- | --- |
| `@doc` | Human description of the asset. |
| `@meta` | Search/filter metadata such as `owner`, `category`, and `tags`. |
| `@depends` | Upstream asset dependencies. May be repeated. |
| `@window` | Time window shape for windowed work. |
| `@freshness` | When Favn may skip work because output is fresh. |
| `@execution_pool` | Shared execution pool name for runtime admission. |
| `@relation` | Relation owned by the asset. Use `true` to infer it from namespace. |

`@depends` accepts a single-asset module or a multi-asset ref:

```elixir
@depends MyApp.Lakehouse.Raw.Sales.Customers
@depends {MyApp.Lakehouse.Raw.Sales.Shopify, :orders}
```

`@relation` accepts:

```elixir
@relation true
@relation [name: "orders"]
@relation [connection: :warehouse, catalog: "raw", schema: "sales", name: "orders"]
```

## Runtime Config In Assets

Use `source_config/2`, `env!/1`, and `secret_env!/1` when asset code needs values
from the runtime environment.

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.Asset

  source_config :source_system,
    segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
    token: secret_env!("SOURCE_SYSTEM_TOKEN")

  def asset(ctx) do
    segment_id = ctx.config.source_system.segment_id
    token = ctx.config.source_system.token

    MyApp.SourceClient.fetch_orders(segment_id, token)
    :ok
  end
end
```

Favn records the names of required values. It does not store secret values in the
authored data. Missing values fail before asset code runs.

## SQL Assets

Use `Favn.SQLAsset` when the asset is mainly SQL.

```elixir
defmodule MyApp.Lakehouse.Mart.Sales.OrderSummary do
  use Favn.SQLAsset

  @doc "Build a daily order summary."
  @meta owner: "analytics", category: :sales, tags: [:mart, :daily]
  @depends MyApp.Lakehouse.Raw.Sales.Orders
  @materialized :view
  @relation true
  query do
    ~SQL"""
    select
      order_date,
      count(*) as order_count
    from raw.sales.orders
    group by order_date
    """
  end
end
```

Rules:

- Use exactly one `query` declaration.
- Add exactly one `@materialized` attribute.
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

Materialization values:

| Value | Meaning |
| --- | --- |
| `:table` | Write a table. |
| `:view` | Create a view. |
| `{:incremental, strategy: :append}` | Append new rows. Requires `@window`. |
| `{:incremental, strategy: :delete_insert, window_column: :order_date}` | Replace one window by deleting and inserting. Requires `@window`. |

Unsupported incremental options include `:merge`, `:replace`, and `unique_key`.

### Validate A SQL Materialization

Table and incremental SQL assets can run ordered checks inside the same
transaction as their materialization:

```elixir
check :candidate_has_rows,
  at: :before_materialize,
  on_false: :fail,
  message: "The candidate must contain rows" do
  ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
end

check :known_statuses,
  at: :after_materialize,
  on_false: :warn,
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

Use `on_false: :fail` to roll back, `:warn` to commit with a quality warning, or
`:skip_materialization` before the write to keep an existing target and commit a
successful no-op. Skip checks and before checks that read `target()` require
`when: :target_exists`, so first-target bootstrap can proceed normally. Views
cannot use checks.

Read [Transactional SQL Asset Checks](sql-asset-checks.md) for the complete
option table, transaction order, result contract, persisted outcomes, limits,
and reusable `defsql` examples.

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

Keep runnable asset settings such as `@materialized`, `@window`, and `@relation`
on `Favn.SQLAsset` modules.

## Multi-Assets

Use `Favn.MultiAsset` when many similar assets share one runtime function.

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Shopify do
  use Favn.MultiAsset

  defaults do
    meta owner: "data-platform", category: :shopify, tags: [:raw]

    rest do
      primary_key "id"
      paginator :cursor, cursor_path: "links.next"
    end
  end

  @doc "Extract Shopify orders."
  @relation true
  @freshness :daily
  asset :orders do
    rest do
      path "/orders.json"
      data_path "orders"
    end
  end

  def asset(ctx) do
    MyApp.Shopify.Client.extract(ctx.asset.config, ctx)
  end
end
```

Rules:

- Define exactly one public `asset(ctx)` function.
- Define at least one `asset :name do ... end` block.
- Each generated asset is referenced as `{Module, :name}`.
- Module-level runtime config applies to every generated asset.

Supported `defaults` clauses:

- `meta owner: ..., category: ..., tags: ...`
- `window Favn.Window.daily(...)`
- `rest do ... end`

Supported `rest` entries:

| Entry | Meaning |
| --- | --- |
| `path "/path"` | Source path or endpoint. |
| `data_path "items"` | Path to data inside a response. |
| `params %{...}` | Static request/query params. |
| `primary_key "id"` | Source primary key. |
| `paginator kind, opts` | Pagination config. |
| `incremental opts` | Incremental extraction config. Defaults to cursor style. |
| `method :get` | Request method. |
| `extra %{...}` | Adapter/project-specific extra data. |

Inside `Favn.MultiAsset`, `@depends :other_asset` references another generated
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
| `config %{...}` | Static pipeline config. |
| `meta %{...}` | Metadata for operators and tooling. |
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

## Schedules

Add an inline schedule to a pipeline:

```elixir
schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :one, overlap: :forbid
```

Schedule options:

| Option | Values | Default |
| --- | --- | --- |
| `:cron` | 5-field cron or 6-field cron with leading seconds | required |
| `:timezone` | IANA timezone string | runtime default |
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
@window Favn.Window.daily(lookback: 1, timezone: "Europe/Oslo")
```

Asset window constructors:

| Constructor | Options |
| --- | --- |
| `Favn.Window.hourly/1` | `lookback`, `refresh_from`, `required`, `timezone` |
| `Favn.Window.daily/1` | `lookback`, `refresh_from`, `required`, `timezone` |
| `Favn.Window.monthly/1` | `lookback`, `refresh_from`, `required`, `timezone` |
| `Favn.Window.yearly/1` | `lookback`, `refresh_from`, `required`, `timezone` |

Option notes:

- `lookback` is a non-negative integer. Default is `0`.
- `required: true` means a runtime window must be supplied.
- `timezone` defaults to `"Etc/UTC"`.

Pipeline window policy:

```elixir
window :daily, timezone: "Europe/Oslo", anchor: :previous_complete_period
```

Supported kinds: `:hourly`, `:daily`, `:monthly`, `:yearly`, or `:hour`, `:day`,
`:month`, `:year`.

Pipeline window options:

| Option | Meaning |
| --- | --- |
| `:anchor` | Usually `:previous_complete_period`. |
| `:timezone` | IANA timezone. |
| `:allow_full_load` | Whether a full load without a window is allowed. |

## Freshness

Freshness tells Favn when work may be skipped because output is already current.

```elixir
@freshness :daily
@freshness {:daily, timezone: "Europe/Oslo"}
@freshness [max_age: {:hours, 6}]
@freshness [window_success: true]
@freshness :always
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
  use Favn.Namespace,
    relation: [connection: :important_lakehouse, catalog: "raw", schema: "payments"]

  use Favn.Source

  @doc "External raw Stripe charges table."
  @meta owner: "data-platform", category: :payments, tags: [:raw]
  @relation [name: "stripe_charges"]
end
```

Rules:

- Define no functions.
- Declare exactly one `@relation`.
- Use optional `@doc` and `@meta` for operator context.

## Compact Asset Modules

`Favn.Assets` is still supported, but new projects should usually prefer one
module per asset with `Favn.Asset` or `Favn.SQLAsset`.

```elixir
defmodule MyApp.SalesETL do
  use Favn.Assets

  @asset true
  @doc "Extract raw orders."
  def extract_orders(_ctx), do: :ok

  @asset true
  @doc "Build daily sales."
  @depends :extract_orders
  def daily_sales(_ctx), do: :ok
end
```

Each `@asset` function compiles to `{Module, :function_name}`.

## Next Steps

- Read [Configuration](configuration.md) for discovery, local runtime, DuckDB,
  pooling, and env file options.
- Read [SQL Client](sql-client.md) when Elixir assets need to run SQL.
- Use [Cheatsheet](cheatsheet.cheatmd) for quick reminders.
