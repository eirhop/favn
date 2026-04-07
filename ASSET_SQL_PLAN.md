# Favn SQL Assets — Concept

## Status

Concept draft.

This document describes a proposed SQL asset authoring model for Favn. It is not final API, but a direction for future feature slicing and implementation.

---

## Why Favn SQL Assets

Favn is already moving toward an asset-first orchestration model where:

- assets are the main public executable node type
- orchestration stays outside the function-level asset DSL
- assets materialize externally
- runtime context flows through `ctx`

A SQL asset layer should build on that foundation instead of introducing a second product model.

The goal is to make SQL assets:

- simple to author
- native to Elixir/Favn
- compiler-driven
- adapter-portable
- easy to plan, backfill, and reason about

This is **not** intended to be “dbt recreated in Elixir”.

---

## Design goals

### 1. Plain SQL first
Users should mostly write normal SQL.

### 2. Minimal syntax
The user-facing DSL should stay very small.

### 3. Typed references
Dependencies and sources should resolve through typed identities, not string-heavy templating.

### 4. Multi-asset authoring
Users should be able to define many SQL assets inside one module.

### 5. Compiler does the hard work
The compiler should infer dependencies, resolve names, validate refs, and prepare materialization plans.

### 6. Reuse Elixir docs and metadata conventions
Use `@doc` and `@meta` for consistency with the existing Favn asset DSL and Elixir documentation style.

---

## Core idea

A module can act as a **SQL asset pack** and define multiple SQL assets inside it.

Example:

```elixir
defmodule MyApp.SalesAssets do
  use Favn.SQLAssets

  asset :stg_orders do
    @doc "Daily staging orders view"
    materialized :view
    window column: :order_date, every: :day, lookback: 2

    sql ~sql"""
    select
      order_id,
      customer_id,
      order_date,
      total_amount
    from #{Raw.Orders}
    """
  end

  asset :fact_orders do
    @doc "Gold sales fact table"
    @meta owner: "analytics", category: :sales, tags: [:gold]
    materialized :incremental
    window column: :order_date, every: :day, lookback: 2

    sql ~sql"""
    select
      o.order_id,
      o.customer_id,
      o.order_date,
      o.total_amount
    from #{:stg_orders} o
    join #{Raw.Customers} c
      on o.customer_id = c.customer_id
    """
  end
end

This gives users:
	•	one module per domain or bounded context
	•	many assets in one place
	•	small authoring surface
	•	less file/module sprawl

⸻

Why not one module per SQL asset

One module per table is clean internally, but it becomes noisy for users as projects grow.

A large warehouse may contain hundreds of tables. Requiring one module per table would likely create too much authoring overhead.

So the preferred design is:
	•	users define many SQL assets in one module
	•	compiler expands them into independent internal asset identities

This keeps user ergonomics high while preserving a clean internal planning model.

⸻

Public authoring model

use Favn.SQLAssets

Marks a module as a container for SQL assets.

The module becomes a compile-time authoring surface that can emit many canonical Favn assets.

Example:

defmodule MyApp.SalesAssets do
  use Favn.SQLAssets
end


⸻

asset :name do ... end

Defines one SQL asset inside the module.

Example:

asset :fact_orders do
  ...
end

The asset name should be unique within the module.

⸻

@doc

Used inside an asset block for human-facing documentation.

Example:

asset :fact_orders do
  @doc "Gold fact table for sales analytics"
  ...
end

This should be reused for catalog/docs generation just like other Favn assets.

⸻

@meta

Used inside an asset block for non-execution metadata.

Example:

asset :fact_orders do
  @meta owner: "analytics", category: :sales, tags: [:gold]
  ...
end

This stays aligned with the current Favn asset DSL.

⸻

materialized

Defines how the asset is persisted.

Initial materializations:
	•	:view
	•	:table
	•	:incremental

Example:

materialized :table


⸻

window

Defines the runtime window contract expected by the asset.

Example:

window column: :order_date, every: :day, lookback: 2

Meaning:
	•	column: :order_date
	•	the time column that defines the asset’s runtime slices
	•	every: :day
	•	the logical execution grain of the asset
	•	lookback: 2
	•	each run should also reprocess the previous 2 windows

This window contract is relevant for planning, incremental processing, and backfills.

Notes
	•	window is an asset concern
	•	pipeline defines the anchor window
	•	runtime resolves the actual ctx.window for each asset execution

⸻

sql

Defines inline SQL.

Example:

sql ~sql"""
select *
from #{Raw.Orders}
"""

Inline SQL is best for smaller models and examples.

⸻

sql_file

Defines SQL loaded from an external file.

Example:

sql_file "sales/fact_orders.sql"

External SQL files are best for larger queries and teams that prefer SQL in standalone files.

⸻

References inside SQL

The goal is to avoid explicit ref() / source() calls in the normal path.

Users should be able to reference assets and sources directly inside SQL interpolation.

Local asset reference

#{:stg_orders}

This means:
	•	resolve the local SQL asset named :stg_orders within the current SQL asset module

External source or asset reference

#{Raw.Orders}
#{FinanceAssets.MonthlyRevenue}

This means:
	•	resolve another typed source/asset identity outside the current module

Why this is preferred

This keeps the user mental model simple:

“When I need a relation in SQL, I interpolate the asset/source identity.”

The compiler should determine whether the identity is:
	•	a local SQL asset
	•	an external SQL asset
	•	a source
	•	invalid

⸻

Sources

A source is an external relation that Favn reads from but does not build itself.

Example direction:

defmodule Raw.Orders do
  use Favn.Source, relation: "raw.orders"
end

Then SQL can reference:

#{Raw.Orders}

This keeps external relations typed and reusable.

⸻

Compiler responsibilities

The compiler should be responsible for the smart behavior.

1. Build canonical assets

Each asset ... do block should compile into a canonical internal Favn asset identity.

2. Infer dependencies

Dependencies should be inferred from SQL references where possible.

Examples:
	•	#{:stg_orders} → dependency on local asset
	•	#{FinanceAssets.MonthlyRevenue} → dependency on external asset

3. Validate references

Fail fast if a SQL reference is:
	•	unknown
	•	invalid
	•	ambiguous
	•	not a supported source/asset identity

4. Resolve adapter-specific relation names

Users write logical identities.
The compiler/adapter resolves real relation names for:
	•	Snowflake
	•	Databricks
	•	BigQuery
	•	DuckDB / MotherDuck
	•	Postgres-like databases

5. Validate materialization/window combinations

Example:
	•	:incremental should likely require window
	•	invalid combinations should fail early

6. Prepare execution/render plans

Do not treat SQL as only a final string.

The compiler should build a richer intermediate representation such as:
	•	SQL chunks
	•	relation tokens
	•	asset/source refs
	•	materialization settings
	•	window settings

Then the adapter renders the final SQL.

7. Support docs and lineage generation

Because refs are typed, lineage and docs can be generated more safely than string-based templating systems.

⸻

Runtime model

Favn SQL Assets should build on Favn’s runtime window model.

Asset-level window

Each asset defines the runtime shape it expects:

window column: :order_date, every: :day, lookback: 2

Pipeline-level anchor window

Pipeline should define the orchestration anchor window.

Example direction:

pipeline :daily_sales do
  asset {SalesAssets, :fact_orders}
  schedule {Schedules, :daily_default}
  window :day
end

Runtime context

At execution time, runtime should provide:
	•	ctx.pipeline.anchor_window
	•	ctx.window

Meaning:
	•	ctx.pipeline.anchor_window
	•	the pipeline/scheduler/operator-requested window
	•	ctx.window
	•	the actual resolved window for this specific asset execution

This allows mixed-granularity graphs.

⸻

Mixed-granularity example

Suppose a gold asset depends on:
	•	one hourly asset
	•	one daily asset
	•	one monthly asset with daily refresh and 2-month lookback

A daily pipeline anchor run might expand into:
	•	24 hourly upstream windows
	•	1 daily upstream window
	•	current month + previous 2 monthly windows

This is one reason the window model should be implemented before full SQL assets.

⸻

Example asset pack

defmodule MyApp.SalesAssets do
  use Favn.SQLAssets

  asset :stg_orders do
    @doc "Daily staging orders"
    materialized :view
    window column: :order_date, every: :day, lookback: 2

    sql ~sql"""
    select
      order_id,
      customer_id,
      order_date,
      total_amount
    from #{Raw.Orders}
    """
  end

  asset :fact_orders do
    @doc "Gold orders fact"
    @meta owner: "analytics", category: :sales, tags: [:gold]
    materialized :incremental
    window column: :order_date, every: :day, lookback: 2

    sql ~sql"""
    select
      o.order_id,
      o.customer_id,
      o.order_date,
      o.total_amount
    from #{:stg_orders} o
    join #{Raw.Customers} c
      on o.customer_id = c.customer_id
    """
  end
end


⸻

Example with SQL files

defmodule MyApp.SalesAssets do
  use Favn.SQLAssets

  asset :stg_orders do
    @doc "Daily staging orders"
    materialized :view
    window column: :order_date, every: :day, lookback: 2
    sql_file "sales/stg_orders.sql"
  end

  asset :fact_orders do
    @doc "Gold orders fact"
    @meta owner: "analytics", category: :sales, tags: [:gold]
    materialized :incremental
    window column: :order_date, every: :day, lookback: 2
    sql_file "sales/fact_orders.sql"
  end
end

Possible SQL file content:

select
  o.order_id,
  o.customer_id,
  o.order_date
from #{:stg_orders} o
join #{Raw.Customers} c
  on o.customer_id = c.customer_id


⸻

Relationship to current Favn concepts

Favn SQL Assets should reuse and align with existing Favn concepts where possible.

Reuse
	•	@doc
	•	@meta
	•	canonical asset identities
	•	dependency-driven planning
	•	pipeline orchestration outside function DSL
	•	runtime context through ctx

New concepts
	•	use Favn.SQLAssets
	•	asset ... do
	•	materialized
	•	window
	•	sql
	•	sql_file
	•	source identities
	•	compiler-based SQL relation inference

⸻

What Favn SQL Assets should not do

Do not recreate dbt syntax directly

Avoid building a Jinja-like templating clone.

Do not overload users with modules/files

Do not require one module per asset as the only path.

Do not push orchestration into SQL authoring

Schedules, triggers, backfills, and catchup policy belong to pipelines/runtime.

Do not force explicit ref() / source() for normal usage

Keep references as simple as possible.

Do not overbuild v1

Start with a small core.

⸻

Suggested feature slicing

Slice 1

Runtime window model for existing Elixir assets:
	•	replace pipeline partition with runtime window
	•	introduce asset-level window contract
	•	runtime ctx.window

Slice 2

Introduce source identities.

Slice 3

Introduce Favn.SQLAssets multi-asset module DSL.

Slice 4

Support inline SQL with typed interpolation.

Slice 5

Support external SQL files.

Slice 6

Compiler-based dependency inference.

Slice 7

Basic materializations:
	•	:view
	•	:table

Slice 8

Incremental materialization with window support.

Slice 9

Backfill-aware planning and materialization history.

⸻

Summary

Favn SQL Assets should be:
	•	asset-first
	•	multi-asset per module
	•	plain SQL first
	•	compiler-driven
	•	typed by asset/source identity
	•	window-aware
	•	consistent with Favn docs/metadata conventions
	•	simpler than dbt for users
	•	more Elixir-native in structure

The key user experience goal is:

author many SQL assets naturally in one module, write mostly normal SQL, and let the compiler infer and validate the rest.

