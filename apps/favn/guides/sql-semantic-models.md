# SQL Semantic Models

Define business calculations beside the output contract in an ordinary
`Favn.SQLAsset` module. Build a separate semantic artifact that tells dashboards
and AI which columns a calculation needs and how to call it using normal SQL.
Consumers connect directly to DuckDB; there is no Favn query API.

## Author a model

The connection `:analytics` and the asset's query SQL must already exist. Place
one named `semantic` block after `contract` and before `query`:

```elixir
defmodule MyApp.Mart.Sales do
  use Favn.SQLAsset

  relation connection: :analytics, schema: "mart", name: "fct_sales"
  materialized :table

  contract do
    grain by: [:sale_line_id], description: "One row per sale line"
    column :sale_line_id, :integer, null: false
    column :sale_date, :date, null: false
    column :store_id, :integer, null: false
    column :gross_value, :decimal, null: false
    column :discount_value, :decimal, null: false
    column :units_sold, :integer, null: false
  end

  semantic :sales do
    time :sale_date, grain: :day, timezone: "Europe/Oslo"

    metric net_revenue(gross_value, discount_value),
      unit: {:currency, "NOK"}, time_aggregate: :aggregate,
      description: "Sales revenue after discounts" do
      ~SQL"SUM(@gross_value - @discount_value)"
    end

    metric units_sold(units_sold),
      unit: :count, time_aggregate: :aggregate,
      description: "Units sold during the selected period" do
      ~SQL"SUM(@units_sold)"
    end

    metric average_unit_price(gross_value, discount_value, units_sold),
      unit: {:custom, "NOK/unit"},
      description: "Net revenue divided by total units sold" do
      ~SQL"net_revenue(@gross_value, @discount_value) / NULLIF(units_sold(@units_sold), 0)"
    end
  end

  query file: "fct_sales.sql"
end
```

The containing asset supplies the model's source. Each metric signature lists
source columns **in public argument order**. `@gross_value` refers to that input;
it is not a setting or a runtime parameter. There is one calculation concept,
`metric`; both sums and ratios use it.

For larger expressions, replace the body with
`metric net_revenue(gross_value, discount_value), file: "metrics/net_revenue.sql",
unit: {:currency, "NOK"}, time_aggregate: :aggregate, description: "..."`.
The file is relative to the asset source and contains one expression using the
same `@` arguments. File changes participate in recompilation; missing files fail.

## Declaration reference

| Declaration or option | Meaning |
| --- | --- |
| `semantic :name do` | At most one model per SQL asset, with an output contract. Names are lowercase ASCII identifiers up to 64 bytes, unique in the artifact. |
| `metric name(columns...), opts` | One aggregate expression or a scalar composition of other metrics in this model. Inputs are distinct, existing source columns; every declared input must be used. |
| `unit:` | Required `:count`, `:ratio`, `:percent`, `{:currency, "NOK"}`, or `{:custom, "NOK/unit"}`. No automatic currency conversion. |
| `description:` | Required nonempty business definition. |
| `format:` | Optional literal `decimals: 0..12` and `style: :number`, `:percent`, or `:currency`. Display only; percent values remain fractions. |
| `time column, grain:, timezone:` | Non-null business `:date`, grain `:day` or `:month`, and an IANA timezone describing the calendar. Dates are not timezone-converted. |
| `time_aggregate:` | Required for leaf metrics; composed metrics inherit compatible child rules. See below. |
| `minimum_grain: [:store]` | Output grouping must preserve the full key of this contract relationship, or a filter must fix that key to one member. A display label does not establish identity. |
| `dimension :store, label: :store_name` | At most one dimension. Its key comes from structured contract grain; the label is an existing source column. |
| `hierarchy :geography, [:region, :store_id]` | Ordered local drill path ending with the dimension's complete key. Retain ancestors when grouping repeated labels. |

Declare relationships inside `contract`, with an explicit dependency, mapping,
cardinality, and violation policy. See [SQL Output Contracts](sql-output-contracts.md)
for validation and transaction behavior. Dimension declarations do not replace
relationship checks or establish database foreign keys.

A metric may call another metric using its exact ordered inputs. Swapping child
inputs fails the build. Cycles, cross-model calls, undeclared/unused inputs, and
ambiguous names fail. Composition cannot mix raw aggregates with metric calls or
combine incompatible time selectors. A ratio of sums stays a ratio of sums;
do not average already calculated prices.

Formulas support a bounded SQL expression grammar: aggregate `SUM`, `MIN`, `MAX`,
`AVG`, and `COUNT`, including `DISTINCT` and `FILTER`; arithmetic, comparisons,
Boolean logic, `CASE`, casts, `COALESCE`, `NULLIF`, `ABS`, and `ROUND`.
Subqueries, table reads/functions, windows, CTEs, volatile/context functions,
user-defined functions, stars, and multiple statements fail closed. Native parsing
identifies every aggregate call. Composed expressions must contain exactly the
aggregate calls inherited from their child metrics; hidden or added aggregates
fail validation. Expanded formulas are limited to 1,024 aggregate calls.

## Select the right rows

| Rule | What the consumer must do before evaluating the formula |
| --- | --- |
| `:aggregate` | Use all selected raw rows within each group. |
| `:first` | Within each requested time bucket, take the earliest observed row per entity. |
| `:last` | Within each requested time bucket, take the latest observed row per entity. |
| `:none` | Keep at most one business-date value per output group. |

First/last require the time column in structured source grain. The remaining
columns are the entity key. Selection uses observations **inside the requested
interval**. There is no carry-forward from earlier dates, invented coverage, or
implicit zero. Select each entity's last row, not one global maximum date.
The macro does not apply these selectors or enforce minimum grain. Consumers
must read and honor the metadata. Timestamp/hourly/DST interpretation is outside
this version; publish a business-date column when needed.

## Build and inspect

Use Linux with Python 3 and an installed supported DuckDB shared library through
the `:favn_duckdb_adbc` plugin. Native validation uses an isolated one-shot process,
not a production data connection. It does not download extensions or drivers.
Configure the installed driver using the plugin's normal driver configuration or
`DUCKDB_ADBC_DRIVER`. Unsupported environments and validation/cleanup failures
produce no artifact.

```sh
export MIX_BUILD_PATH=_build_semantic
mix favn.build.semantics --output dist/semantics
mix favn.semantic.inspect --artifact dist/semantics/sm_<digest>/semantic.json \
  --metric sales.net_revenue --format json
mix favn.semantic.diff --from previous/semantic.json --to current/semantic.json
```

Set a **dedicated build path before Mix starts**, including in CI. Never launch a
runner from that directory or reuse an active runner's build output. Mix may
compile dependencies while locating the task, before the task's own checks run.
A separate checkout/build directory avoids changing lazily loaded runner code.
Normal development compilation/reload remains separate and may restart a runner.

The build needs source but no runner release map or running control plane. It
produces immutable, content-addressed semantic data, including a contract snapshot,
metric dependencies, exact ordered invocation metadata, and native macro SQL.
Descriptions and formulas change semantic identity. A separately rebuilt execution
release may also change identity after source edits; semantic publication does not
require rebuilding or deploying it.

Output is written only after successful validation and confirmed native cleanup.
An interrupted build cannot select a partial artifact. Rebuilding identical
content is idempotent; corrupt or unknown-version artifacts fail on read.

## Query from a dashboard

The artifact provides the immutable macro namespace/name and ordered input
bindings. Install its generated macro definitions in the consumer's DuckDB
session. Durable catalog publication/activation is separate work; do not assume
DuckLake itself persists arbitrary macro definitions.

This example uses `metrics_v1` as a readable stand-in for the artifact's exact
immutable namespace:

```sql
SELECT sales.store_id,
       metrics_v1.sales_net_revenue(
           sales.gross_value, sales.discount_value
       ) AS net_revenue,
       metrics_v1.sales_average_unit_price(
           sales.gross_value, sales.discount_value, sales.units_sold
       ) AS average_unit_price
FROM mart.fct_sales AS sales
WHERE sales.sale_date >= DATE '2026-01-01'
  AND sales.sale_date < DATE '2026-02-01'
GROUP BY sales.store_id;
```

A dashboard maps the recorded source relation to its own query alias, takes
inputs in recorded order, quotes identifiers, and binds filter values. It still
owns joins and grouping. Generated formulas use explicit arguments and add no
Favn request hop. They should be compared with equivalent inline SQL for the
actual workload; no universal optimizer or performance guarantee is implied.

DuckDB accepts wrong columns when their types fit. Documentation and metadata
make the correct call available; neither makes arbitrary manual SQL or AI output
infallible. The artifact's logical types come from contracts. Synthetic native
binding types are validation evidence, not guaranteed precision, scale, or
nullability of every consumer query.

## Give AI the same context

Read the artifact using `favn.semantic.inspect --format json`. Supply the metric's
business description, units, source relation, ordered bindings, canonical SQL,
dependency graph, and time/grain requirements together. Build calls from those
records rather than guessing argument names. Use one immutable semantic version
throughout a request.

This feature builds and inspects artifacts; it does not create a Favn query
service, SQL metadata catalog, dashboard, or MCP server. Catalog publication and
served-contract compatibility are tracked in #720; runtime freshness/quality
context in #721; AI discovery/MCP in #719. A compatible catalog release must be
checked against the contract of the served data generation, not merely the newest
execution manifest. Unknown served compatibility cannot imply safe activation.
