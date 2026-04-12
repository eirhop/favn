# Favn Assets and SQL Assets — Plan

## Status

Phase 1 and Phase 2 implemented.

This document defines the target long-term asset authoring direction for Favn and the phased path to implement it safely.

It replaces the earlier SQL multi-asset pack direction with a **module-per-asset** model as the primary authoring style for both:

* Elixir assets via `Favn.Asset`
* SQL assets via `Favn.SQLAsset`

It also keeps space for a later advanced model where one module can **generate many canonical assets** for repetitive ingestion patterns.

---

## Summary

Favn should converge on one primary mental model:

> one materializing asset = one module

That should be true for both Elixir and SQL assets.

This gives Favn:

* a clearer and more consistent authoring model
* simpler relation ownership
* cleaner namespace inference
* easier IEx workflows
* easier docs, lineage, and operator tooling
* better fit between Elixir assets, SQL assets, and future UI concepts

The preferred long-term authoring surface should be:

* `Favn.Asset` for one Elixir asset in one module
* `Favn.SQLAsset` for one SQL asset in one module
* `Favn.Namespace` for inherited config for Asset or SQLAsset in nested modules
* later: a generated-assets mode for repetitive patterns

The existing `Favn.Assets` multi-asset DSL should remain supported for compatibility and compact authoring, but it should no longer be the center of the design.

---

## Why this direction

Favn already has a strong canonical runtime model:

* assets compile into canonical `%Favn.Asset{}` values
* planning and execution operate on canonical asset refs
* runtime windowing is shared
* orchestration is outside asset authoring

The new SQL direction revealed a cleaner overall design:

* SQL assets feel much more natural as one module per asset
* produced relation inference becomes straightforward
* module namespace can carry connection/catalog/schema defaults
* plain SQL can stay plain SQL

That same shape also fits many Elixir assets, especially materializing warehouse assets such as raw ingestion loaders.

For example, a natural project structure becomes:

* `MyWarehouse.Raw.Sales.Orders` — Elixir asset writing `raw.sales.orders`
* `MyWarehouse.Silver.Sales.StgOrders` — SQL asset reading that raw relation
* `MyWarehouse.Gold.Sales.FctOrders` — downstream SQL asset

This gives a coherent repo structure across layers.

---

## Core principles

### 1. Primary model: one module per asset

The preferred authoring model should be one module per asset.

### 2. Generated assets later, not now

Favn should later support one module generating many canonical assets, but that should be a separate advanced model, not the default one.

### 3. Plain SQL first

SQL bodies should stay as standard SQL as much as possible.

### 4. Shared canonical asset model

Elixir and SQL assets must compile into the same canonical `%Favn.Asset{}` shape.

### 5. Shared produced relation model

Both Elixir and SQL assets must be able to declare or infer the relation they materialize.

### 6. Convention first, override when needed

Namespace and module path should provide strong defaults, but explicit overrides must be supported.

### 7. Shared temporal model

Elixir assets, SQL assets, and pipelines should use the same Favn window semantics.

### 8. Local developer ergonomics matter

Render, preview, explain, inspect, and materialize should be easy from IEx.

---

## Target authoring models

---

## `Favn.Asset` — single Elixir asset module

This should become the preferred Elixir asset authoring model.

Example:

```elixir
defmodule MyWarehouse do
  use Favn.Namespace, connection: :warehouse
end

defmodule MyWarehouse.Raw do
  use Favn.Namespace, catalog: "raw"
end

defmodule MyWarehouse.Raw.Sales do
  use Favn.Namespace, schema: "sales"
end

defmodule MyWarehouse.Raw.Sales.Orders do
  use Favn.Asset

  @doc "Extract raw orders from upstream API"
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  @produces true
  @window Favn.Window.daily(on: :order_date, lookback: 1)

  def asset(ctx) do
    target = ctx.asset.produces
    # => %Favn.RelationRef{
    #      connection: :warehouse,
    #      catalog: "raw",
    #      schema: "sales",
    #      name: "orders"
    #    }

    :ok
  end
end

defmodule MyWarehouse.Raw.Sales.Customers do
  use Favn.Asset

  @doc "Extract raw customers from upstream API with asset name override"
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  @produces name: "crm_customers"

  def asset(ctx) do
    ctx.asset.produces
    :ok
  end
end
```

### Why this shape

It keeps Elixir asset authoring aligned with the SQL asset direction:

* one module
* one obvious asset
* one obvious produced relation
* one obvious runtime entrypoint
* easy IEx usage

### Public contract

A `Favn.Asset` module should compile to one canonical asset.

Likely rules:

* module defines exactly one public asset function, likely `def asset(ctx)`
* `@doc`, `@meta`, `@depends`, `@window`, and `@produces` attach to that asset
* module-level defaults such as `connection`, `catalog`, and `schema` may be used for relation ownership

### Relation defaults inside `Favn.Asset`

Module-level defaults should be supported to reduce repetition and be aligned between Asset and SQLAsset:

### `@depends`

`@depends` should continue to express **logical asset dependency**.

Example:

```elixir
@depends MyWarehouse.Raw.Sales.Orders
```

or equivalent canonical ref shape.

### `@produces`

`@produces` should express **physical relation ownership**.

Example:

```elixir
@produces true
```

or:

```elixir
@produces name: :orders
```

or:

```elixir
@produces connection: :warehouse, catalog: :raw, schema: :sales, name: :orders
```

This must remain separate from `@depends`.

---

## `Favn.Assets` — multi-asset compact mode

The existing `Favn.Assets` DSL should remain supported, but it should become a secondary compact mode rather than the main authoring direction.

It remains useful when:

* a team prefers several related assets in one module
* older code should continue to work unchanged
* the author wants a concise compact definition style

### Required extension to `Favn.Assets`

`Favn.Assets` must be extended with produced relation support so Elixir assets can participate in relation ownership and SQL dependency linking.

Example:

```elixir
defmodule MyApp.RawAssets do
  use Favn.Assets
  use Favn.Namespace

  connection :warehouse
  catalog :raw
  schema :sales

  @asset true
  @produces true
  def orders(ctx) do
    target = ctx.asset.produces
    :ok
  end

  @asset true
  @depends :orders
  @produces name: :stg_orders
  def stage_orders(ctx) do
    :ok
  end
end
```

### Meaning

* `@depends` stays logical dependency
* `@produces` becomes relation ownership
* module defaults reduce repetition

This change is required so SQL assets can later infer dependencies from Elixir-produced warehouse relations.

---

## `Favn.Namespace` — inherited SQL config and maybe other generic config in the future like metadata.

Namespace modules provide inherited SQL configuration.

Example:

```elixir
defmodule MyWarehouse do
  use Favn.SQL.Namespace, connection: :warehouse
end

defmodule MyWarehouse.Gold do
  use Favn.SQL.Namespace, catalog: "gold"
end

defmodule MyWarehouse.Gold.Sales do
  use Favn.SQL.Namespace, schema: "sales"
end
```

### Responsibilities

Namespace modules may define inherited defaults such as:

* `connection`
* `catalog`
* `schema`
* naming policy
* future SQL asset defaults

### Merge order

Config should resolve from broadest scope to narrowest scope:

1. root namespace
2. ancestor namespaces
3. asset module

Narrower scope wins.

---

## `Favn.SQLAsset` — single SQL asset module

This should be the preferred SQL authoring model.

Example:

```elixir
defmodule MyWarehouse.Gold.Sales.FctOrders do
  use Favn.SQLAsset

  @doc "Gold fact table for orders"
  @meta owner: "analytics", category: :sales, tags: [:gold]
  @depends MyWarehouse.Silver.Sales.StgOrders
  @window Favn.Window.daily(on: :order_date, lookback: 2)
  @materialized {:incremental, strategy: :delete_insert, unique_key: [:order_id]}

  query do
    ~SQL"""
    select
      order_id,
      customer_id,
      order_date,
      total_amount
    from gold.sales.stg_orders
    """
  end
end
```

### Core rules

* one SQL asset module = one asset
* SQL body remains normal SQL authored through a real `~SQL` sigil
* metadata uses familiar Favn conventions
* produced relation is normally inferred from namespace/module path
* explicit override via `@produces` is allowed

### Explicit produced relation override

Example:

```elixir
@produces schema: "mart", name: "fact_orders"
```

### SQL authoring principle

Favn should avoid custom SQL syntax in the SQL body for v1.

Favn SQL should use one real SQL body language:

* asset SQL uses `query do ... end`
* reusable SQL should later use `defsql ... do ... end`
* both should contain `~SQL` bodies

Do not require:

* `ref(...)`
* `source(...)`
* interpolation-heavy SQL DSL
* Jinja-like templating
* fake sigils

The SQL text should stay as close to standard SQL as possible so ordinary SQL editor tooling remains usable.

---

## Shared produced relation model

A shared produced relation model is the bridge between Elixir assets and SQL assets.

This is required so a SQL asset can read a warehouse relation produced by an Elixir asset, and Favn can understand that relationship.

---

## Canonical relation identity

Introduce a shared canonical relation identity:

```elixir
defmodule Favn.RelationRef do
  @enforce_keys [:name]
  defstruct [:connection, :catalog, :schema, :name]

  @type t :: %__MODULE__{
          connection: atom() | nil,
          catalog: binary() | nil,
          schema: binary() | nil,
          name: binary()
        }
end
```

### Naming terms

Internal canonical naming should use:

* `connection`
* `catalog`
* `schema`
* `name`

These are the most portable names across SQL platforms.

### DSL aliases

User-facing DSL may accept:

* `database` as alias for `catalog`
* `table` as alias for `name`

These must normalize into `%Favn.RelationRef{}`.

### Value normalization

Recommended normalization:

* `connection` stays atom
* `catalog`, `schema`, and `name` may be authored as atom or string
* internally, `catalog`, `schema`, and `name` normalize to strings

This keeps relation identifiers SQL-safe and avoids atom leakage.

---

## Canonical asset extension

Extend `%Favn.Asset{}` with:

* `produces :: Favn.RelationRef.t() | nil`

This should be a first-class field on the canonical asset struct, not loose metadata.

### Why

Produced relation ownership is:

* structural
* indexable
* needed for lineage and dependency inference
* shared across asset types

---

## `@produces` semantics

`@produces` means:

> this asset owns or materializes this relation

This is distinct from `@depends`, which means:

> this asset logically depends on another asset

These two concepts must remain separate.

### Recommended accepted forms

```elixir
@produces true
```

```elixir
@produces name: :orders
```

```elixir
@produces connection: :warehouse, catalog: :raw, schema: :sales, name: :orders
```

### Inference rules

Recommended behavior:

* no `@produces` means the asset does not declare a produced relation
* `@produces true` means:

  * use module defaults or namespace defaults
  * infer `name` if possible from function name or module leaf
* partial `@produces` values merge on top of defaults

### Validation rules

* `name` is required after normalization/inference
* `connection` must be atom when present
* `catalog`, `schema`, `name` become strings internally
* `database` and `catalog` cannot both be supplied
* `table` and `name` cannot both be supplied
* unknown keys are rejected

---

## Runtime context exposure

The resolved produced relation should be passed into runtime context as:

* `ctx.asset.produces`

This should be a `%Favn.RelationRef{}`.

This avoids repeated relation construction inside asset code and gives both Elixir and SQL assets a shared way to inspect their output target.

---

## Relation ownership index

Favn should build a relation ownership index from all canonical assets with `produces != nil`.

Conceptually:

```elixir
%{
  %Favn.RelationRef{connection: :warehouse, catalog: "raw", schema: "sales", name: "orders"} =>
    {MyApp.RawAssets, :orders}
}
```

or, for single-asset modules, their canonical ref.

### Purpose

This index is needed for:

* dependency inference
* lineage generation
* ownership inspection
* conflict validation

### Uniqueness rule

No two assets may claim the same produced relation.

If two assets compile to the same canonical `%Favn.RelationRef{}`, compilation or startup must fail.

---

## Linking Elixir raw assets to SQL assets

A common Favn workflow is:

1. Elixir asset calls an API or external system
2. Elixir asset writes a raw warehouse relation
3. SQL assets transform that relation

This plan explicitly supports that workflow.

Example:

```elixir
defmodule MyWarehouse.Raw.Sales.Orders do
  use Favn.Asset

  connection :warehouse
  catalog :raw
  schema :sales

  @doc "Extract raw orders from upstream API"
  @produces connection: :warehouse catalog: "raw" schema: "sales" name: "orders"

  def asset(ctx) do
    target = ctx.asset.produces
    :ok
  end
end
```

A downstream SQL asset may read `raw.sales.orders`, and Favn can connect that relation back to the Elixir producer asset via the relation ownership index.

This is why `@produces` is required as a first-class concept.

---

## Dependency model

### Explicit dependencies first

SQL assets and Elixir assets should continue to support explicit dependencies.

Examples:

```elixir
@depends MyWarehouse.Raw.Sales.Orders
```

or, in compact mode, current local/cross-module forms.

This remains the safest and clearest first implementation path.

### Inferred dependencies later

Later phases may infer dependencies by matching referenced SQL relations to the relation ownership index.

Example flow:

1. SQL asset references `raw.sales.orders`
2. compiler normalizes that relation
3. relation ownership index maps it to an Elixir asset
4. dependency is inferred

If no owner exists, the relation is treated as an external unmanaged relation.

### Important rule

Inference must be additive, not mandatory.

Explicit dependency declaration must remain supported.

---

## Plain SQL first

SQL authoring should prefer:

```elixir
query do
  ~SQL"""
  select *
  from raw.sales.orders
  """
end
```

not custom SQL DSL constructs inside the query body.

### Why

This gives the best chance of compatibility with:

* SQL highlighting
* SQL formatting
* SQL language servers
* easy debugging and copy/paste

The Favn-specific semantics should live around the SQL body, not inside it, whenever possible.

---

## Helper APIs and developer ergonomics

A SQL asset module should be easy to work with from IEx.

Target helper capabilities include:

* render final SQL
* preview result without materialization
* explain query
* materialize asset output
* inspect resolved connection
* inspect resolved produced relation

Recommended public APIs:

* `Favn.render(asset, opts)`
* `Favn.preview(asset, opts)`
* `Favn.explain(asset, opts)`
* `Favn.materialize(asset, opts)`

Recommended accepted public inputs:

* single-asset module such as `MyApp.Gold.Sales.FctOrders`
* canonical asset ref such as `{MyApp.Gold.Sales.FctOrders, :asset}`
* `%Favn.Asset{type: :sql}` for already-resolved assets

These helpers should stay SQL-asset focused in Phase 4. Internal runtime/render code may work from
compiled `%Favn.SQLAsset.Definition{}` values, but that struct should not be part of the public API.

Additional Phase 4 API rules:

* `render/2` is a pure render helper from the caller's point of view: it may resolve compiled modules,
  asset metadata, and reusable SQL definitions, but it must not open SQL sessions or hit the backend
* `preview/2` should expose the actual executed preview statement separately from the canonical rendered
  query SQL; the canonical query remains in `render.sql`
* renderer output uses one backend-neutral positional binding model; adapters may rewrite placeholders if
  needed, but renderer output stays adapter-neutral
* direct SQL asset refs across different connections are a render-time error in Phase 4

Similar conveniences may later be added for Elixir materializing assets.

---

## Future generated assets / multi-assets

Favn should later support a second authoring mode where one module generates many canonical assets.

This is important for repetitive ingestion patterns, such as:

* many similar API endpoints
* shared extraction logic
* one output relation per endpoint
* many canonical runtime assets generated from one template module

### Important distinction

This should be modeled as:

* one generator module
* many compiled canonical assets

not as the primary authoring model for all assets.

### Why

The common case should not pay the complexity cost of the exceptional repetitive case.

### Design principle

* primary model: one authored module = one asset
* advanced later model: one generator module = many compiled assets
* runtime model: always many distinct canonical assets

This fits the existing compiler seam well and avoids muddying the core DSL.

---

## Runtime model

Elixir assets and SQL assets must run on the same shared Favn runtime model.

That means:

* same planning model
* same run model
* same retry/cancel/timeout semantics
* same scheduler integration
* same backfill semantics
* same window semantics

The SQL execution path may differ internally, but the logical orchestration model must stay shared.

### Runtime context

Relevant shared runtime concepts should include:

* `ctx.window`
* `ctx.pipeline.anchor_window`
* `ctx.asset.produces`

---

## What this plan intentionally does not do

### Do not replace all multi-asset Elixir authoring immediately

`Favn.Assets` should stay supported. Considering removed in future

### Do not make generated assets the default model now

That is a later advanced feature.

### Do not require custom SQL templating syntax

Plain SQL remains the preferred path.

### Do not overload `@depends`

Logical dependency and produced relation ownership remain separate concepts.

### Do not put scheduling into asset DSLs

Schedules and orchestration policy remain outside the asset body.

### Do not require external SQL files in v1

Inline SQL is enough for the first implementation.

---

## Example target project shape

```text
MyWarehouse/
  Raw/
    Sales/
      Orders.ex        # Elixir asset producing raw.sales.orders
      Customers.ex     # Elixir asset producing raw.sales.customers
  Silver/
    Sales/
      StgOrders.ex     # SQL asset producing silver.sales.stg_orders
  Gold/
    Sales/
      FctOrders.ex     # SQL asset producing gold.sales.fact_orders
```

This is the intended intuitive warehouse-oriented structure.

---

## Phased implementation plan

### Phase 1 — shared produced relation foundation

Deliver:

* [x] `Favn.RelationRef`
* [x] `%Favn.Asset{produces: ...}`
* [x] namespace config inheritance
* [x] `Favn.Assets` support for:
  * `@produces`
  * `ctx.asset.produces`
* [x] validation and normalization
* [x] relation ownership uniqueness checks
* [x] relation ownership index

This phase links Elixir materializing assets to warehouse relation ownership.

### Phase 2 — `Favn.Asset` single-asset Elixir DSL

Deliver:

* [x] `Favn.Asset` as preferred one-module-per-asset Elixir DSL
* [x] single asset entrypoint convention (`def asset(ctx)`)
* [x] support for:

  * [x] `@doc`
  * [x] `@meta`
  * [x] `@depends`
  * [x] `@window`
  * [x] `@produces`
  * [x] relation defaults through `Favn.Namespace`
* [x] compilation into one canonical `%Favn.Asset{}`

This phase establishes the preferred Elixir asset model.

#### Phase 2 implementation notes

Implemented semantics:

* one `use Favn.Asset` module compiles to exactly one canonical asset with ref `{Module, :asset}`
* `@depends` in `Favn.Asset` accepts:

  * `Some.Module` (only when `Some.Module` uses `Favn.Asset`, normalized to `{Some.Module, :asset}`)
  * `{Some.Module, :asset_name}`
* `@produces true` relation-name inference for single-asset modules uses module leaf `Macro.underscore/1`

  * `MyWarehouse.Raw.Sales.Orders` -> `"orders"`
  * `MyWarehouse.Gold.Sales.FctOrders` -> `"fct_orders"`
* `Favn.get_asset/1` now also accepts a single-asset module ref directly (`Favn.get_asset(MyAssetModule)`)

### Phase 3 — `Favn.SQLAsset` authoring and catalog integration

Deliver:

* [x] one-module-per-asset SQL DSL
* [x] real `~SQL""" ... """` sigil
* [x] `query do ... end` for asset queries
* [x] support for:
  * [x] `@doc`
  * [x] `@meta`
  * [x] `@depends`
  * [x] `@window`
  * [x] `@materialized`
  * [x] optional `@produces` override
* [x] produced relation inference from namespace/module path
* [x] compilation into one canonical `%Favn.Asset{}`

This phase establishes the SQL asset authoring model.

#### Phase 3 implementation notes

Implemented semantics:

* one `use Favn.SQLAsset` module compiles to exactly one canonical asset with ref `{Module, :asset}`
* `Favn.SQLAsset` reuses `Favn.Namespace` for inherited `connection` / `catalog` / `schema` defaults
* SQL asset queries are authored as `query do ... end`
* `~SQL` is now a real Elixir sigil and currently returns a plain SQL string
* `@depends` accepts:

  * `Some.Module` only when `Some.Module` is another single-asset module, normalized to `{Some.Module, :asset}`
  * `{Some.Module, :asset_name}`
* `@materialized` is required and currently accepts:

  * `:view`
  * `:table`
  * `{:incremental, strategy: :append | :replace | :delete_insert | :merge, unique_key: [...]}`
* SQL assets infer their produced relation from `Favn.Namespace` defaults plus module leaf `Macro.underscore/1`
* SQL assets require a resolved produced relation with a connection name
* `~SQL` stays plain SQL and currently rejects interpolation/modifiers in phase 3
* SQL modules expose the existing asset compiler seam via `__favn_asset_compiler__/0`
* generated `asset/1` currently returns `{:error, :sql_asset_runtime_not_implemented}` until phase 4 runtime execution lands

### SQL DSL direction

Favn SQL uses a real `~SQL` sigil as the single SQL body language.

SQL assets declare their main query with `query do ... end`.
Reusable SQL is declared with `defsql ... do ... end`.

Both asset queries and reusable SQL macros use the same `~SQL` body syntax and the same `@name` placeholder syntax for injected values.

What is implemented now:

* real `~SQL` sigil
* `query do ~SQL""" ... """ end`
* `defsql ... do ... end` reusable SQL definitions through `Favn.SQL`
* expression SQL macros and relation SQL macros
* direct asset references in relation position
* one `@name` placeholder/input model with reserved runtime inputs
* compile-time normalization and validation into SQL IR

#### Phase 3 follow-up — finalized SQL authoring DSL

This follow-up completes the SQL authoring model before Phase 4 runtime execution.

The finalized DSL contract includes:

* asset queries via `query do ... end`
* reusable SQL via `defsql ... do ... end`
* one `~SQL` body language across both
* one `@name` placeholder model across both
* expression SQL macros and relation SQL macros from day one
* direct Favn asset references in relation position
* compile-time SQL normalization and validation
* an internal SQL representation suitable for later render/execute work

##### Proposed public DSL

Asset query example:

```elixir
defmodule MyApp.Gold.Sales.FctCustomerRevenue do
  use Favn.SQLAsset
  use MyApp.SQL

  @materialized :table

  query do
    ~SQL"""
    select
      customer_id,
      sum(cents_to_dollars(total_amount_cents)) as gross_revenue
    from orders_in_window(@window_start, @window_end)
    where (@country is null or country = @country)
    group by 1
    """
  end
end
```

Reusable expression SQL example:

```elixir
defmodule MyApp.SQL do
  use Favn.SQL

  defsql cents_to_dollars(amount_cents) do
    ~SQL"""
    cast((@amount_cents / 100.0) as numeric(16, 2))
    """
  end
end
```

Reusable relation SQL example:

```elixir
defmodule MyApp.SQL do
  use Favn.SQL

  defsql orders_in_window(start_at, end_at) do
    ~SQL"""
    select
      order_id,
      customer_id,
      total_amount,
      country
    from MyApp.Raw.Sales.Orders
    where inserted_at >= @start_at
      and inserted_at < @end_at
    """
  end
end
```

##### `@name` placeholder model

Inside any `~SQL` body, `@name` is the only value placeholder syntax.

Meaning:

* SQL built-ins such as `@window_start` and `@window_end` resolve from reserved runtime inputs
* in asset `query` bodies, any other `@name` resolves from SQL params supplied by the caller
* inside `defsql`, function arguments become local SQL params with the same names
* in `defsql`, non-reserved placeholders must match declared arguments; reusable SQL does not implicitly capture query params
* when a `defsql` body calls another `defsql`, passed arguments bind that callee's local SQL params
* SQL bodies never reference `ctx` directly

The model should stay intentionally small:

* reserved runtime inputs are explicit and documented
* all non-reserved names come from params
* missing inputs fail with clear diagnostics
* reserved names cannot be shadowed by `defsql` arguments

##### Expression and relation SQL macros

`defsql` should compile to reusable SQL fragments with one of two validated shapes:

* expression macro: body expands to a SQL expression and can be used in expression position
* relation macro: body expands to a relation-producing query and can be used in `from` / `join` / relation position

Phase 3 should support both from the beginning, with the distinction captured in the
compiled representation and validated at compile time where possible.

The author should not need separate syntaxes such as `defsql_expr` or `defsql_relation`.
Instead, Favn should infer and store the shape during normalization.

##### Direct asset references inside SQL

Phase 3 should allow direct module references in relation position, for example:

```sql
from MyApp.Raw.Sales.Orders
join MyApp.Silver.Sales.StgCustomers
```

These should normalize into Favn-aware relation reference nodes in the internal SQL
representation.

Not supported yet for direct asset refs:

* dialect-specific table functions
* `update ... from`
* `merge into`
* arbitrary nested dialect syntax
* plain relation names inferred from raw text

In Phase 3 this is a SQL authoring and validation feature, not yet full dependency
inference. Dependency inference remains Phase 5 work.

##### Internal representation recommendation

Phase 3 should stop treating `~SQL` as only a stored plain string.

Recommended direction:

* preserve the original SQL source string for docs/debugging
* normalize the SQL body into a dedicated Favn SQL IR at compile time
* the IR should represent at least:
  * literal SQL segments
  * `@name` placeholders
  * `defsql` calls with resolved module/name/arity metadata
  * direct asset references in relation position
  * normalized query bodies for assets and reusable SQL definitions
  * definition kind metadata for expression vs relation bodies

This does not require a full SQL parser in Phase 3.
It does require enough structure that Phase 4 rendering and adapter execution can work
from the IR instead of re-parsing authored strings or forcing a rewrite.

##### Compile-time validation in this follow-up

Phase 3 validates:

* `query` contains exactly one `~SQL` body
* `defsql` contains exactly one `~SQL` body
* no interpolation or arbitrary Elixir execution inside `~SQL`
* duplicate reusable SQL names / arities in a module
* reserved input names are not used as `defsql` arguments
* `defsql` call arity matches the declared reusable SQL definition
* duplicate visible imported/local reusable SQL definitions are compile-time errors
* cyclic reusable SQL definitions are compile-time errors
* compiled asset references must resolve to single-asset modules with produced relations
* unresolved asset references stay as deferred symbolic refs in the SQL IR
* obvious relation-only positions such as `from` / `join` do not receive known expression macros
* obvious expression positions do not receive known relation macros

Phase 3 defers:

* runtime value presence checks against actual invocation params
* adapter-specific SQL type validation
* full dependency inference from relation usage
* execution-time parameter encoding and prepared-statement behavior
* full SQL semantic analysis across every dialect edge case

##### Phase boundary discipline

What belongs in Phase 3:

* final authoring syntax
* final placeholder/input model
* reusable SQL definition and call model
* expression vs relation macro distinction
* direct asset-reference syntax in SQL
* compile-time normalization/validation
* stable SQL IR for later runtime work

What belongs in Phase 4a:

* render final executable query SQL plus canonical bound params
* preview / explain / materialize helper APIs
* runtime context to SQL-input resolution
* pure render helper semantics with no backend/session work in `render/2`
* one backend-neutral positional binding model from renderer to adapters
* render-time deferred asset-ref resolution
* render-time validation that direct SQL asset refs stay within one connection
* fully inlined `defsql` expansion
* materialization planning for `:view` and `:table`
* adapter integration and execution
* execution-time errors for missing params or backend failures

What belongs in Phase 4b:

* dedicated incremental materialization planning
* explicit strategy semantics and validation
* window-aware incremental execution and lookback handling
* first backend support for safe incremental strategies
* clear unsupported-strategy errors for deferred strategies

What belongs in Phase 5:

* dependency inference from normalized relation references
* additive inferred `@depends` diagnostics
* relation-ownership-driven lineage and compile-time warnings around unresolved refs

The DSL should explicitly avoid:

* fake sigils
* Jinja-style templating
* arbitrary Elixir interpolation inside SQL
* string stitching as the normal authoring workflow
* multiple placeholder syntaxes across SQL contexts

### Phase 4a — SQL runtime integration and helper APIs

Status: implemented in current codebase.

Deliver:

* SQL execution through shared runtime
* render/preview/explain/materialize helper APIs
* inspectable resolved connection and produced relation
* strict render result struct with final SQL, canonical bound params, and diagnostics metadata
* pure `render/2` semantics with no backend calls or session creation
* backend-neutral positional bindings from renderer, with adapter-side placeholder rewriting when needed
* render-time deferred asset-ref resolution and fully inlined `defsql` expansion
* explicit render-time failure for cross-connection direct asset refs
* materialization planning for `:view` and `:table`

This phase makes SQL assets practical for everyday work.

Preview helper rule:

* `preview/2` should return the canonical rendered query in `preview.render.sql`
* `preview.statement` should be the actual preview SQL executed against the adapter, for example with an
  added preview `limit`

Important limit for this phase:

* incremental SQL materialization is not part of Phase 4a
* `@materialized {:incremental, ...}` should keep compiling but fail at runtime with a clear unsupported-phase error until Phase 4b lands
* rendered direct relation identifiers currently assume standard unquoted identifier compatibility; dialect-specific identifier quoting should be introduced behind adapter-aware rendering in a later phase

### Phase 4b — incremental SQL materialization

Deliver:

* dedicated incremental planning layer on top of rendered query SQL
* explicit first-pass strategy support for `:append` and `:delete_insert`
* required strategy metadata and validation for window-aware writes
* explicit deferral of `:merge` until backend capability and planner semantics are ready
* explicit deferral of current `:replace` incremental semantics until they are renamed or specified more precisely

This phase keeps incremental behavior deliberate instead of letting it drift into runtime integration.

Write-plan naming rule for Phase 4a:

* avoid using incremental-style `replace` language for normal `:view` and `:table` rebuild behavior
* prefer names such as `replace_existing?`, `rebuild?`, or explicit `create_or_replace` semantics
* reserve incremental strategy naming for the dedicated incremental materialization phase

### Phase 5 — relation-based inference

Deliver:

* controlled dependency inference from SQL relation usage
* inference through relation ownership index
* diagnostics and validation

This phase improves ergonomics without changing the core model.

### Phase 6 — generated assets / multi-assets

Deliver later:

* generator module that compiles into many canonical assets
* repetitive endpoint ingestion support
* separate advanced authoring mode

This phase supports repetitive ingestion without weakening the clarity of the core DSL.

---

## Open questions

### 1. Canonical ref shape for `Favn.Asset`

Resolved in Phase 2.

* canonical internal ref remains `{module, :asset}`
* public convenience module lookup is supported by `Favn.get_asset(module)` for single-asset modules

### 2. Produced relation name inference

Resolved in Phase 2.

* default inference uses module leaf `Macro.underscore/1`
* custom naming should be explicit via `@produces name: ...` when needed

### 3. `Favn.Asset` function naming

Resolved in Phase 2.

* `Favn.Asset` requires exactly one public `def asset(ctx)` entrypoint

### 4. Namespace discovery rules

How should ancestor namespace config be discovered and validated to keep behavior deterministic and easy to explain?

### 5. SQL relation extraction scope

How much relation extraction is reasonable in early phases without introducing fragile parsing behavior?

---

## Final recommendation

Adopt the following target direction for Favn:

* `Favn.Asset` as the preferred single-module Elixir asset DSL
* `Favn.SQLAsset` as the preferred single-module SQL asset DSL
* `Favn.Namespace` for inherited configuration
* shared produced relation model via `Favn.RelationRef`
* extension of `Favn.Assets` with `@produces` and relation defaults
* relation ownership index as the bridge between Elixir and SQL assets
* explicit dependencies first, inferred dependencies later
* plain SQL bodies, not custom SQL templating
* generated/multi-assets later as a separate advanced compiler-based mode

This gives Favn one coherent long-term asset model while still leaving room for compact multi-asset authoring and future generated assets where they truly add value.
