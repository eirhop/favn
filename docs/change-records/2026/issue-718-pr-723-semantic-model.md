# Change Record: SQL-native semantic models with independent releases

| Field | Value |
| --- | --- |
| Status | Planning — complexity decisions open |
| Type | Feature |
| Primary issue | [#718](https://github.com/eirhop/favn/issues/718) |
| Pull request | [#723](https://github.com/eirhop/favn/pull/723) (draft; planning only) |
| Related work | [#720 catalog publication](https://github.com/eirhop/favn/issues/720), [#721 runtime state](https://github.com/eirhop/favn/issues/721), [#719 AI/MCP](https://github.com/eirhop/favn/issues/719) |
| Affected areas | Public authoring, Core contracts/compiler, local build tooling, DuckDB integration, generated relationship checks |
| Source baseline | `8d2b8e1f1e574dabb4670ef0e56f46e073f51f8d` on `origin/main` |
| Last updated | 2026-09-17 |

## One-minute summary

Define a metric once as ordinary SQL and publish its formula, exact column
bindings, usage rules, and dependency graph as an independently versioned
artifact. Dashboards construct ordinary DuckDB macro calls from that metadata;
they query their existing SQL connection directly. Contracts continue to own
columns, grain, and enforced relationships. Semantic models own analytical
meaning and can change without rebuilding or redeploying a runner. This record
specifies the proposed implementation; none of these new APIs exists yet.

The current delivery is a planning record and draft PR only. Implementation
starts in a later task. Examples in this record are design examples, not files
installed in a consumer project.

## Complexity review: decisions to discuss

The full-scope estimate is 2,420–4,010 production lines and 2,550–4,100 lines
of tests, fixtures, examples, and documentation: 4,970–8,110 total additions.
These are rough planning estimates, not implemented code or two additive plans.
The largest avoidable complexity is preserving byte-identical execution rebuilds
when a semantic block changes. Independent semantic deployment needs less than
that. The recommendations below are open decisions; the detailed scope further
below remains the proposal being evaluated until those choices are agreed.

### Recommended: separate deployment without rewriting runtime modules

Keep the same-file DSL and separate semantic manifest. Build and later publish
that manifest against the served data contract without rebuilding or deploying
the running release. Existing execution packages, active execution manifest,
runner process, and served data remain untouched by that semantic-only workflow.

Allow a later full execution build to produce a new identity. Normal project
compilation changes BEAM bytes and can shift SQL source positions; local dev may
therefore restart its runner using its existing conservative reload behavior.
Semantic publication must not implicitly invoke that execution build/reload path.
When a local runner is active, build semantics in a separate CI checkout or
isolated build directory that the runner does not load from. Its current launcher
inherits mutable authoring code paths, so merely skipping reload while replacing
BEAM files there would not protect lazily loaded modules. Ordinary development
compilation/reload remains outside this deployment-only guarantee.
Do not filter hashes, reuse an immutable release ID for changed bytes, or claim
that a semantic build cannot affect ordinary local authoring output.

This removes runtime BEAM projection, resource/reflection filtering, special
runtime inventories and load paths, production assembly changes, and source-span
normalization. Keep semantic capture out of execution declaration records, but
do not promise that independently rebuilt execution artifacts are byte-identical.
The implementation would prove that semantic publication needs no new runner
release and leaves deployed execution untouched, rather than proving identical
fresh execution builds. #720 still checks the served generation's consumed
contract before activation; it cannot accept an incompatible concurrent schema
change just because the command is called a semantic build.

The budget's isolation slice alone accounts for 450–750 production and 450–750
supporting lines. Removing only that slice gives 1,970–3,260 production plus
2,100–3,350 supporting lines (4,070–6,610 total). This is subtraction from an
estimate, not a validated replacement budget: capture/build/tests would need
re-estimation after the guarantee is agreed.

### Other reductions worth making

| Recommendation | What becomes simpler | Explicit tradeoff |
| --- | --- | --- |
| Build the final semantic artifact directly from the project | Omit the optional intermediate authoring bundle and `--input` mode. CI hands the finished artifact to #720. | Validation needs project source; publication still does not. |
| Keep local inspection/diff as bounded lists and JSON | Remove continuation cursors for a size-limited local artifact; preserve deterministic ordering and meaningful change classification. | No paginated browsing API in #718. |
| Keep native validation a one-shot build process | Use a supported, bounded subprocess facility, not a reusable worker service/framework. | Parser isolation, failure handling, and confirmed native process cleanup still need proof; killing an Elixir task is not enough. |

Do not save lines by removing ordered input metadata, formula dependency checks,
closed SQL validation, conservative type claims, or explicit time/grain rules.
Those protect correctness and make the catalog useful to dashboards and AI.

Relationship enforcement is another separable workstream, but it is requested
in #718 and belongs to the existing contract/check engine. Deferring it would
reduce the first delivery by its estimated 300–500 production and 300–500
supporting lines, plus some integration work. It would also remove the promised
foreign-key/cardinality validation from that delivery. That is a scope tradeoff,
not an equivalent implementation. Keep it unless the user wants a smaller
initial feature; never keep `on_violation: :fail` while providing metadata only.

Recommendation: agree on deployment independence instead of rebuilt-byte
identity, omit the intermediate bundle, and simplify local inspection. Keep the
SQL-native DSL and correctness rules. Do not reduce the budget by an arbitrary
percentage or start implementation before these choices are settled.

## Impact and problem analysis

Today dashboards repeat business formulas and relationship knowledge. The same
quantity can become `SUM(net_value) / SUM(units)` in one dashboard and an average
of row-level prices in another. Contracts describe the data but do not give a
consumer the approved calculation or how to invoke it.

The intended consumer experience is: discover `sales.net_revenue`, read its
ordered column bindings, construct a macro call using the query's table alias,
and choose filters and grouping in normal SQL. An AI consumer receives the same
metadata and generated examples. Metadata reduces mistakes; it does not prove
that arbitrary SQL, joins, or AI output are correct.

### Assumptions and decisions

- DuckDB is the first formula dialect. DuckLake remains a storage/catalog
  integration, not a Favn query service.
- Consumers own SQL generation and execution. No Favn API wrapper, metric
  request language, or interception of dashboard queries is introduced.
- Explicit column arguments are accepted. A macro cannot verify that a numeric
  argument came from the intended source column.
- One source relation per semantic model; metrics compose within that model.
- A semantic-only change must leave execution packages, execution manifest
  identity, runner identity, and freshness/rebuild decisions unchanged.
- Relationship checks affect publication and therefore belong in the execution
  contract. Editing them is intentionally an execution change.
- Time selection and minimum grain are machine-readable usage requirements.
  A formula macro does not enforce them.
- The DSL and limits below settle implementation choices that were illustrative
  in #718. Departures from the issue are listed explicitly later in this record.

### Evidence

| Evidence | What it establishes | Limit |
| --- | --- | --- |
| [`Favn.SQL`](../../../apps/favn_authoring/lib/favn/sql.ex) and [`Template`](../../../apps/favn_core/lib/favn/sql/template.ex) | Existing function-style `defsql`, literal `~SQL`, expression templates, and local arguments can be reused. | Templates alone do not prove that SQL reads only declared inputs. |
| [`Contract`](../../../apps/favn_core/lib/favn/sql/contract.ex), [`Check`](../../../apps/favn_core/lib/favn/sql/check.ex), [`SQLAsset`](../../../apps/favn_authoring/lib/favn/sql_asset.ex) | Contracts generate bounded checks; checked publication already supports checks before and after mutation within a transaction. | Relationship resolution and incremental cardinality checks need new behavior. |
| [`ManifestBuilder`](../../../apps/favn_authoring/lib/favn_authoring/deployment/manifest_builder.ex) | Building the existing deployable manifest requires runner release bindings. | It cannot simply become the semantic-only build entrypoint. |
| [`SourceRelease`](../../../apps/favn_local/lib/favn_local/source_release.ex) | Local runner identity hashes compiled BEAM files. | Moving formulas into another ordinary `.ex` module would still couple releases. |
| [`Manifest.Asset`](../../../apps/favn_core/lib/favn/manifest/asset.ex), [`TargetCompatibility`](../../../apps/favn_core/lib/favn/target_compatibility.ex) | Runtime descriptors and package hashes drive execution compatibility. | The existing field `semantic_generation_id` is a data/execution concept, not the analytical artifact proposed here. |
| DuckDB 1.5.4 in-memory probes during design | Aggregate expressions work inside scalar macros; compatible but wrong numeric arguments bind; a synthetic relation does not prevent a subquery from reading another relation. | This is feasibility evidence, not adapter, production, or load qualification. |
| [DuckDB macro documentation](https://duckdb.org/docs/current/sql/statements/create_macro), [SQL-to-JSON documentation](https://duckdb.org/docs/current/data/json/sql_to_and_from_json) | Native macro and parser facilities exist. | Parser representation and macro persistence must be qualified against supported runtime pins. |

Static source inspection is sufficient for this plan. No development-server
runtime inspection or live data access is claimed.

## Current behavior

The execution manifest binds authored assets to runner releases. Dashboards
maintain formulas and joins independently. There is no analytical artifact with
its own build or release identity.

```mermaid
flowchart LR
    A[Asset contracts and SQL] --> B[Execution manifest build]
    R[Runner release bindings] --> B
    B --> C[Runner publishes data]
    C --> D[SQL tables]
    E[Dashboard formulas and join knowledge] --> F[Consumer SQL]
    D --> F
    B -->|Invalid contract| X[Build fails]
```

## Authoring DSL

The new entrypoint is `semantic :model_name do ... end` inside an SQL asset,
after its contract and before `query`. Source is inferred; discovery uses the
existing asset registry. The containing asset supplies the source and contract. One asset
can declare at most one semantic block; its literal model name is unique in the
compiled catalog and supplies stable IDs such as `sales.net_revenue`.

The following asset declarations show the combined authoring surface. They assume the existing `:analytics` connection and adjacent query
files; those existing APIs are unchanged.

```elixir
# lib/my_app/mart/store.ex
defmodule MyApp.Mart.Store do
  use Favn.SQLAsset

  relation connection: :analytics, schema: "mart", name: "dim_store"
  materialized :table

  contract do
    grain by: [:store_id], description: "One row per store"
    column :store_id, :integer, null: false
    column :store_name, :string, null: false
    column :region, :string, null: false
  end

  semantic :store do
    dimension :store, label: :store_name
    hierarchy :geography, [:region, :store_id]
  end

  query file: "dim_store.sql"
end

# lib/my_app/mart/sales.ex
defmodule MyApp.Mart.Sales do
  use Favn.SQLAsset
  alias MyApp.Mart.Store

  depends Store
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

    relationship :store, Store,
      on: [store_id: :store_id],
      cardinality: :many_to_one,
      on_violation: :fail
  end

  semantic :sales do
    time :sale_date, grain: :day, timezone: "Europe/Oslo"

    metric net_revenue(gross_value, discount_value),
      unit: {:currency, "NOK"},
      time_aggregate: :aggregate,
      description: "Sales revenue after discounts" do
      ~SQL"SUM(@gross_value - @discount_value)"
    end

    metric units_sold(units_sold),
      unit: :count,
      time_aggregate: :aggregate,
      description: "Units sold during the selected period" do
      ~SQL"SUM(@units_sold)"
    end

    metric average_unit_price(gross_value, discount_value, units_sold),
      unit: {:custom, "NOK/unit"},
      description: "Net revenue divided by total units sold" do
      ~SQL"""
      net_revenue(@gross_value, @discount_value)
        / NULLIF(units_sold(@units_sold), 0)
      """
    end
  end

  query file: "fct_sales.sql"
end
```

The function-style `metric` signature, options, expression grammar, composition
rules, and ordered consumer bindings are unchanged. The source column is resolved
from the containing contract: `gross_value` means that asset's column, and
`@gross_value` inside SQL is the corresponding macro argument. A metric file is
relative to this same asset file:

```elixir
# Alternative to the inline declaration, inside semantic :sales do.
metric net_revenue(gross_value, discount_value),
  file: "metrics/net_revenue.sql",
  unit: {:currency, "NOK"},
  time_aggregate: :aggregate,
  description: "Sales revenue after discounts"
```

Opening/closing rules also stay next to the columns they describe. This example
assumes `MyApp.Mart.Product` has a non-null integer `product_id` and structured
grain `[:product_id]`:

```elixir
# lib/my_app/mart/inventory.ex
defmodule MyApp.Mart.Inventory do
  use Favn.SQLAsset
  alias MyApp.Mart.{Product, Store}

  depends Store
  depends Product
  relation connection: :analytics, schema: "mart", name: "fct_inventory"
  materialized :table

  contract do
    grain by: [:inventory_date, :store_id, :product_id],
      description: "One product in one store at the daily boundary"

    column :inventory_date, :date, null: false
    column :store_id, :integer, null: false
    column :product_id, :integer, null: false
    column :opening_units, :integer, null: false
    column :closing_units, :integer, null: false
    column :units_delta, :integer, null: false

    relationship :store, Store,
      on: [store_id: :store_id], cardinality: :many_to_one, on_violation: :fail

    relationship :product, Product,
      on: [product_id: :product_id], cardinality: :many_to_one, on_violation: :fail
  end

  semantic :inventory do
    time :inventory_date, grain: :day, timezone: "Europe/Oslo"

    metric opening_units(opening_units),
      unit: :count, time_aggregate: :first,
      description: "Opening stock at each entity's first observation in the period" do
      ~SQL"SUM(@opening_units)"
    end

    metric closing_units(closing_units),
      unit: :count, time_aggregate: :last,
      description: "Closing stock at each entity's last observation in the period" do
      ~SQL"SUM(@closing_units)"
    end

    metric units_delta(units_delta),
      unit: :count, time_aggregate: :aggregate,
      description: "Total unit movement during the period" do
      ~SQL"SUM(@units_delta)"
    end
  end

  query file: "fct_inventory.sql"
end
```

| Placement rule | Behavior |
| --- | --- |
| `contract do` | Owns schema, grain, and enforced relationships. |
| `semantic :name do` | Owns dimensions, hierarchies, business time, and metrics for this SQL asset. Name is a literal lowercase ASCII identifier with the existing 64-byte limit. |
| Source | Implicitly the containing asset; an explicit `source` or a second model for the same asset is rejected. |
| Nested declarations | `dimension`, `hierarchy`, `time`, and `metric` are valid only inside the semantic block. Relationships remain contract declarations. |
| Ordering | The semantic block follows the contract and precedes `query`, matching the existing declaration-before-query convention. |
| Optional block | Assets without semantic declarations retain their authoring behavior. A semantic block requires an output contract. |
| Formula files | Loaded relative to the asset source and tracked by the compiler as semantic inputs, so file-only edits rebuild the semantic output. |
| Duplicate capture | Duplicate blocks/names and stale or unresolved asset references are build errors. Deterministic sorted discovery uses the existing asset build, not a manually maintained file list. |

### Relationship contract

| Relationship field | Contract |
| --- | --- |
| First argument | Unique role name on the source, such as `:store`, `:billing_store`, or `:shipping_store`. |
| Target | SQL asset explicitly listed in `depends`; its compiled contract has structured grain. No semantic dimension declaration is required for execution. |
| `on:` | Required ordered, nonempty mapping of local columns to the complete target grain in target-grain order. Composite keys are supported. No SQL predicates. |
| `cardinality:` | Required `:many_to_one` or `:one_to_one`, from source to target. |
| `on_violation:` | Required `:fail` or `:warn`; invalid schema or execution errors always fail. |
| Nullability | Derived from the source contract, without a second `required:` option. A composite foreign key is either entirely required or entirely nullable; mixed declarations fail the build. |

For example, `on: [country: :country_code, store: :store_id]` maps a composite
target grain `[:country_code, :store_id]`. Local and target key types must match
under existing normalized contract type equality. Duplicate roles, columns in
one mapping, missing `depends`, missing structured grain, or unresolved targets
are build errors. Several roles may target the same asset. Snowflake relations
use the same declaration on a dimension asset; no implicit dependency expansion
or hidden join planning is added.

Required keys use existing non-null checks. An optional composite key with all
columns null has no reference; a partially null tuple violates the relationship.
The relationship check verifies every non-null candidate key exists and verifies
the referenced target key is unique. `:one_to_one` additionally checks uniqueness
on the complete transaction-visible source target after mutation, excluding
all-null optional keys. Checking only the incremental candidate would miss
collisions with retained rows.

Checks use the existing transaction engine and `origin: :contract`, with stable
relationship claim IDs. A failing before-check prevents mutation; a failing
after-check rolls the mutation back. A warning commits with a quality warning.
Checks require table/incremental publication and a target readable in the same
SQL session with the required transaction guarantees; unsupported materialization
or adapter capabilities fail before writing. Reads resolve the actual pinned
dependency generation, not a reconstructed unversioned table name.

This verifies publication at its transaction snapshot. It is not a permanent
database foreign-key constraint and cannot prevent a later independent dimension
publication from invalidating old facts. Cardinality metadata is a declared rule;
consumers must also consider check state, especially with `:warn`.

### Semantic declarations

There is one formula declaration, `metric`. A simple sum and a derived ratio
are both metrics. No second `measure` grammar, `divide/2` arithmetic DSL, or
`inputs:` list duplicates the function signature. In SQL, `@gross_value` means
the named formula argument bound to the source contract's `gross_value` column.
It is not a variable supplied by a Favn server, setting, secret, or parameter.

| Declaration | Exact v1 contract |
| --- | --- |
| `semantic :sales do` | Stable model name, unique in one artifact. Lowercase ASCII identifier. The containing SQL asset supplies its source. |
| `dimension name, label: column` | At most one dimension per model; structured source grain supplies the ordered key. Label is a source column; no duplicate key declaration. |
| `hierarchy name, [columns]` | Named ordered drill path on a dimension, coarse to fine. Columns must exist and be distinct; the final levels contain the complete ordered dimension key. No related-dimension paths in v1. |
| `time column, grain:, timezone:` | At most one. Required non-null source `:date` column; grain is `:day` or `:month`; timezone is a validated IANA name describing the business calendar. A DATE is already a business date and is not timezone-converted. Timestamp columns and hourly grain are rejected in v1. |
| `metric name(args...), opts do ~SQL"..." end` | One aggregate SQL expression. At least one distinct argument; each argument names an exact source column. Signature order is the public macro argument order. |
| `metric name(args...), file: "formula.sql", ...` | Equivalent file form, relative to the asset source file. The file contains the same SQL expression with `@` arguments. Literal inline and file forms are mutually exclusive. |
| `unit:` | Required `:count`, `:ratio`, `:percent`, `{:currency, "NOK"}`, or `{:custom, "NOK/unit"}`. Currency uses an explicit uppercase three-letter code; no conversion or dimensional algebra is inferred. |
| `description:` | Required nonempty business definition. |
| `format:` | Optional declarative map: `decimals: 0..12`, `style: :number | :percent | :currency`. Compatible with the unit; no executable formatter. |
| `time_aggregate:` | Required on a leaf metric; derived for composed metrics as specified below. No implicit assumption that all inputs are additive. |
| `minimum_grain:` | Optional list of source relationship roles, for example `[:store]`. Grouping must preserve each role's full key, or a filter must fix that key to one member. Labels alone do not establish grain. |

Names cannot be overloaded by arity. Unknown options and duplicate declarations
fail explicitly. `format` is display metadata: `:percent` stores fractional
values, so `0.2` displays as 20%; it does not alter SQL. No namespace defaults,
automatic currency assumptions, or localized labels are introduced.

Hierarchy ordering is a declared drill path, not proof of a functional
dependency between labels. Consumers retain ancestor columns in grouping to
avoid combining identically named members under different parents.

Timestamp support needs an explicit representation contract first. Existing
`:datetime` can describe both naive and timezone-aware native timestamps, and a
business timezone alone does not say whether a naive value represents UTC or
local wall time. V1 avoids guessing: authors publish a business-date column in
the source asset when time semantics are needed. Timestamp interpretation, DST,
and hourly buckets are deferred together.

### Formula composition and validation

A metric can call another metric in its model using normal function syntax.
Calls resolve to named metric dependencies before SQL validation; the compiler
inlines their expressions into the canonical expanded formula. Calls must pass
the referenced metric's exact source columns in its declared order. Thus
`net_revenue(@discount_value, @gross_value)` fails during authoring even though
DuckDB alone would accept it. Cross-model calls, cycles, ambiguous built-in
function names, unused signature inputs, and undeclared inputs fail the build.

The public dependency graph is `metric -> metric -> source column`. It retains
direct edges and a flattened ordered input binding list, so consumers need not
parse either Elixir or SQL. Expansion preserves parentheses, null behavior, and
aggregate placement; nesting one aggregate around an aggregate metric fails.

V1 permits ordinary arithmetic, comparisons, Boolean expressions, `CASE`,
literal constants, casts, `COALESCE`, `NULLIF`, `ABS`, `ROUND`, and the aggregates
`SUM`, `MIN`, `MAX`, `AVG`, and `COUNT`, including `COUNT(DISTINCT @column)` and
aggregate `FILTER`. At least one aggregate must occur after expansion. Only
declared `@` arguments may supply column references; the compiler's synthetic
column names are not another authoring escape hatch. Other functions require a
deliberate extension of the supported set and tests.

Reject subqueries, table reads/functions, CTEs, windows, ordering inside
aggregates, stars, SQL statements, settings/parameters, external helpers,
user-defined functions, dynamic identifiers, and volatile/context functions.
`COUNT(@sale_line_id)` expresses row count for the non-null source grain. String
columns are valid inputs to `COUNT(DISTINCT ...)`; numeric-only validation on all
inputs would be wrong.

Reuse the existing SQL template handling for literals, source spans, and `@`
arguments. The DuckDB integration parses a generated one-expression SELECT
through its native parser, validates a closed expression tree, then binds it
against a zero-row synthetic relation using an explicit native type profile for
the logical contract types. Validate the
tree before binding to exclude independent reads, even when they would bind
successfully. The integration owns the version-sensitive parser representation;
Core receives a closed, dialect-neutral validation result and diagnostics.

Use a local isolated DuckDB session inside a separately owned OS process, with no
attached user catalogs, credentials, extension autoload, or network access. The
build worker protocol and termination rules are specified under failures below.
No production database is queried. Missing
compiler capability or an unsupported parser/runtime pin fails the build; do not
emit an unvalidated artifact. The build records its validator/dialect version.
Binding verifies aggregate legality for the recorded native type profile. Record
that profile and its native `validation_result_type` as validation evidence, not
as the guaranteed result type of an arbitrary consumer query. Publish a logical
result family only when sound across supported physical variants; otherwise
report `unknown`. Native precision, scale, width, and output nullability remain
query-bound or conservatively unknown. Do not insert narrowing casts to manufacture
an exact result-type guarantee. Units and business meaning remain author claims.

For example, both `DECIMAL(18,2)` and `DECIMAL(38,10)` satisfy a `:decimal`
contract, but the same sum expression can return `DECIMAL(38,2)` or
`DECIMAL(38,10)`. The consumer obtains the actual native type from its query
result. V1 does not expand the physical column-contract DSL or prove absence of
data-dependent overflow.

SQL empty-set and null semantics remain DuckDB's. For example, `SUM` can return
null and division uses explicit `NULLIF` in the formula. Authors write
`COALESCE` when zero is the intended business rule; Favn does not insert it.

### Time and grain: formulas do not select rows

`time_aggregate` describes the rows on which a formula is meaningful. It does
not direct Favn to generate a dashboard query or reaggregate already calculated
metric results.

| Value | Consumer obligation |
| --- | --- |
| `:aggregate` | Evaluate the formula over all selected source rows in each output group. Ratios remain ratios of aggregates; never sum or average precomputed ratios by default. |
| `:first` | Within each requested time bucket, select the earliest observed row per entity before evaluating the formula across entities. |
| `:last` | Within each requested time bucket, select the latest observed row per entity before evaluating the formula across entities. |
| `:none` | Each output group must contain at most one business-time value. Combining several dates is not defined by this metric. |

`:first`, `:last`, and `:none` require `time`. First/last also require the time
column in structured source grain; the remaining grain columns identify the
entity. An empty remaining key describes a single global entity. The compiled
requirements contain the time column, timezone, source grain, entity key, and
selection direction. Duplicate rows at the contract grain are a data-quality
failure, not silently resolved with an arbitrary tie breaker.

First/last select observed rows inside the consumer's requested interval and
bucket. They do not carry values forward from before that interval, invent
missing observations, or imply that every entity has complete coverage. Missing
entities contribute no row; null values follow the formula. A point-in-time
query filters to the chosen date explicitly.

A composed metric is scalar SQL over metric calls, constants, and the permitted
scalar operators/functions; it cannot mix raw aggregate inputs with metric
calls. All child metrics must have the same effective row-selection rule and
time/entity coordinates. The compiler inherits that rule and unions minimum
grain requirements. An explicit option may repeat the inferred rule or add
minimum-grain requirements, but cannot weaken them. Mixed flow/snapshot or
opening/closing dependencies are rejected in v1: they need separately selected
and aggregated inputs, which a single scalar macro cannot express correctly.

This is the deliberate simplification behind one formula DSL. A future
multi-stage metric is a separate design, not an implicit feature of this macro.

The Inventory example above declares opening, closing, and flow metrics together.

The original issue's `:sum` becomes `:aggregate`: a formula may be a distinct
count, minimum, or weighted ratio, so the usage rule must not prescribe summing
its outputs. `:first` incorporates the issue's subsequent opening-balance
request. `minimum_grain: [:store]` can restrict a metric to store-or-finer output;
region grouping alone fails that usage rule unless the selection fixes one store.
Favn validates these declarations, while the consumer applies them to its query.

### Canonical SQL and native macros

Canonical expressions refer to positional internal arguments derived from the
public signature. Store authored SQL, expanded canonical SQL, typed ordered
bindings, direct dependencies, and effective usage rules together. Expanded
macro bodies are self-contained; they do not rely on a different metric's
mutable database definition.

For readability the examples below call the immutable release schema
`metrics_v1`. Production names are `metrics_<full semantic content digest>`;
`v1` is not an instruction to overwrite one schema for every release. The build
rejects collisions after DuckDB identifier normalization, including collisions
from joining model and metric names with underscores.

```sql
CREATE SCHEMA metrics_v1;

CREATE MACRO metrics_v1.sales_net_revenue(gross_value, discount_value) AS
    SUM(gross_value - discount_value);

CREATE MACRO metrics_v1.sales_average_unit_price(
    gross_value, discount_value, units_sold
) AS
    SUM(gross_value - discount_value) / NULLIF(SUM(units_sold), 0);

CREATE MACRO metrics_v1.inventory_closing_units(closing_units) AS
    SUM(closing_units);
```

These are DuckDB **scalar macros containing aggregate expressions**, not a new
native aggregate-macro kind. Metadata uses `kind: scalar` and
`evaluation: aggregate_expression`. The artifact includes the unbound macro
definition and a structured namespace binding; its content digest does not
hash SQL that already embeds that same digest. Rendering the qualified DDL is
a deterministic projection after hashing.

#718 builds and verifies the definitions. #720 installs macros when supported,
or publishes their definitions for consumers to instantiate in a writable local
session/catalog. Do not assume DuckLake itself persists arbitrary DuckDB macros.
Macro availability and serving data are separate capabilities.

### Structured invocation metadata

The following is a readable projection of one compiled record. Version tokens
are illustrative. Wire values use strings, closed field sets, and explicit
schema versions; consumers do not deserialize Elixir modules or evaluate code.

```yaml
schema_version: 1
semantic_version: sm_example
contract_snapshot: dc_example
metric: sales.net_revenue
source_asset: MyApp.Mart.Sales.asset
source_relation:
  connection_ref: analytics
  catalog: null
  schema: mart
  name: fct_sales
macro:
  schema: metrics_v1
  name: sales_net_revenue
  kind: scalar
  evaluation: aggregate_expression
inputs:
  - position: 1
    parameter: gross_value
    source_asset: MyApp.Mart.Sales.asset
    column: gross_value
    contract_type: decimal
    nullable: false
  - position: 2
    parameter: discount_value
    source_asset: MyApp.Mart.Sales.asset
    column: discount_value
    contract_type: decimal
    nullable: false
unit: {kind: currency, code: NOK}
time_aggregate: aggregate
minimum_grain: []
source_grain: [sale_line_id]
time: {column: sale_date, grain: day, timezone: Europe/Oslo}
```

The real record also includes the native validation profile/result, the logical
result family or `unknown`, conservative nullability, formula digest, canonical
expression, description, optional display format,
compatibility requirements, and dependency edges. Input bindings are ordered,
typed references to source columns, not loosely formatted SQL strings.

A dashboard reads this once per pinned semantic version, selects its own alias
`sales` for the source, and emits the arguments by position. It quotes identifiers
through its SQL library and binds filter values normally. Required relationship
key mappings provide join metadata. The client must choose a relationship role
when several roles reach the same table and must not invent a join on a label.

Documentation, CLI inspection, and copyable invocation examples are generated
from these same records. Alias substitution is structured identifier binding,
not text replacement in arbitrary SQL. No separate handwritten parameter
registry is maintained in Favn or the dashboard.

### End-user dashboard SQL

After #720 publishes the artifact and makes its macros available, a dashboard
can generate this exact query through its existing SQL connection:

```sql
SELECT
    store.region,
    metrics_v1.sales_net_revenue(
        sales.gross_value, sales.discount_value
    ) AS net_revenue,
    metrics_v1.sales_average_unit_price(
        sales.gross_value, sales.discount_value, sales.units_sold
    ) AS average_unit_price
FROM mart.fct_sales AS sales
JOIN mart.dim_store AS store ON store.store_id = sales.store_id
WHERE sales.sale_date >= DATE '2026-01-01'
  AND sales.sale_date < DATE '2026-02-01'
GROUP BY store.region;
```

With sale rows `(gross=100, discount=10, units=3)` and
`(gross=140, discount=20, units=2)` in one region, net revenue is `210` and
average unit price is `42`. Averaging the row prices would incorrectly give `45`.
Grouping changes normally; the consumer does not sum previously computed ratios.
Optional relationships generally need a left join to retain unknown members;
the example's required store relationship permits the shown inner join when
the served data satisfies the contract.

A closing-balance query first selects the last observation per store/product:

```sql
WITH closing AS (
    SELECT inventory.*
    FROM mart.fct_inventory AS inventory
    WHERE inventory.inventory_date >= DATE '2026-01-01'
      AND inventory.inventory_date < DATE '2026-02-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY inventory.store_id, inventory.product_id
        ORDER BY inventory.inventory_date DESC
    ) = 1
)
SELECT
    store.region,
    metrics_v1.inventory_closing_units(closing.closing_units) AS closing_units
FROM closing
JOIN mart.dim_store AS store ON store.store_id = closing.store_id
GROUP BY store.region;
```

For monthly output spanning several months, the consumer also includes the
business-month bucket in that partition and output grouping. Opening stock uses
ascending order and the opening metric. Flow totals and closing totals require
separate row selections; aggregate each at the desired output keys, then join
those grouped results. Joining raw fact tables can multiply rows and is never
implied by metric metadata.

Native macro expansion leaves ordinary SQL for DuckDB to optimize. Acceptance
compares expanded formula results and projection plans against handwritten SQL.
This does not promise identical plans, constant-time macros, or a latency target
for all data layouts and joins. There is no Favn service call per dashboard query.

### SQL discovery and AI context

#718 supplies the typed records and their relational projection specification;
#720 owns physical tables, migrations, and publication. The intended normalized
tables include `semantic_release`, `metric`, `metric_input`, `metric_dependency`,
`dimension`, `relationship`, `relationship_key`, `hierarchy_level`, and
`time_dimension`, alongside the broader asset/contract/column/lineage catalog.
All semantic rows carry `semantic_version`. There is no separate `measure` table.

These SQL examples describe the future #720 consumer contract, not tables
created by this PR. A client obtains an immutable release token once and binds
the same token in every catalog read:

```sql
SELECT metric_ref, source_asset_ref, description, unit, time_aggregate,
       minimum_grain, macro_schema, macro_name, canonical_sql
FROM meta.metric
WHERE semantic_version = $semantic_version
  AND metric_ref = 'sales.net_revenue';

SELECT position, parameter_name, source_asset_ref, column_name,
       contract_type, nullable
FROM meta.metric_input
WHERE semantic_version = $semantic_version
  AND metric_ref = 'sales.net_revenue'
ORDER BY position;

SELECT relationship_ref, source_asset_ref, target_asset_ref,
       cardinality, on_violation
FROM meta.relationship
WHERE semantic_version = $semantic_version
  AND source_asset_ref = 'MyApp.Mart.Sales.asset';
```

The relationship's ordered key rows give `sales.store_id = store.store_id`.
Asset/column rows give physical identifiers and types. Dependencies give, for
example, `sales.average_unit_price -> sales.net_revenue -> sales.gross_value`.
An AI can read these tables directly; #719 later provides compact discovery and
context tools over the same records, with generated examples and usage rules.

#721 adds actual publication, quality, coverage, and freshness context. Formula
provenance does not prove arbitrary query lineage or data freshness. Consumers
report the semantic version separately from the data publication/generation or
storage snapshot actually read. Pinning a formula does not freeze changing data.

## Compiler and runtime separation

Separating two JSON manifests after compiling one mixed module is insufficient.
The execution and semantic projections must split before their identities are
formed, and the runner must load the runtime bytes whose identity was computed.

| Current evidence | Required change |
| --- | --- |
| `SQLAsset.__before_compile__/1` emits execution raw-definition getters; SQL assets also permit ordinary Elixir functions. | Capture semantic data into a separate compiler-owned root. Do not put it into execution raw definitions or treat the entire asset module as disposable metadata. |
| `SourceRelease.current/1` hashes all compiled BEAM files. | Derive local runner identity from a complete verified runtime inventory whose asset modules have semantic metadata projected out. |
| [`RunnerProcessLauncher`](../../../apps/favn_local/lib/favn_local/runner_process_launcher.ex) inherits the authoring node's code paths. | Launch from the verified inventory, without a fallback path to the customer authoring BEAMs. Hashing a filtered list while loading the old files would be incorrect. |
| [`ExecutionPackage`](../../../apps/favn_core/lib/favn/manifest/execution_package.ex) serializes SQL source locations and spans. | Give execution definitions stable diagnostic coordinates so adding lines to a semantic block does not change an unrelated execution package. |
| [`RunnerRelease`](../../../apps/favn_core/lib/favn/runner_release.ex) and [production release rules](../../production/runner_releases.md) use immutable customer-assigned production IDs. | Preserve that meaning. This plan does not make production IDs semantic hashes or permit reusing an ID for a different image. |

The proposed pipeline is one compilation capture, followed by projections. It
must not evaluate authored Elixir twice to manufacture a different runner module:
compile-time side effects or nondeterministic macro expansion could otherwise
produce different behavior in the two passes.

```mermaid
flowchart TD
    A[One SQL asset file with contract and semantics] --> B[Authoring compilation and capture]
    B --> C[Execution definitions and contract snapshot]
    B --> D[Semantic definitions]
    B --> E[Expanded runtime code]
    C --> F[Execution manifest and packages]
    C --> G[Semantic validation and artifact]
    D --> G
    E --> H[Verified runtime inventory]
    H --> I[Local runner or customer release assembly]
    F --> I
    G --> J[Independent catalog publication in issue 720]
    B -->|Invalid| X[Diagnostics and no release output]
    H -->|Projection cannot be verified| X
```

#### Capture and identities

1. The ordinary asset compile captures contract/query/check declarations and
   semantic declarations separately, under the same source asset reference.
   Literal metric SQL, options, argument order, and formula-file contents become
   inert typed semantic records. They never enter `SQLExecution`, execution raw
   definitions, SQL runtime requirements, or execution hash payloads.
2. Preserve the compiled execution-only getters and generated `asset/1` route.
   They currently support direct local SQL execution as well as authoring; this
   revision does not redesign that route merely to isolate semantic metadata.
3. Give all SQL assets one compiler-tagged semantic capture root, even when no
   semantic block exists. The authoring variant exposes the captured record; the
   runtime variant replaces that root with a fixed `{:error, :authoring_only}`
   result. Prefer this stable stub over deleting functions: exports and
   `__info__` stay consistent when metrics or the whole block are added/removed.
   Metric declarations do not generate one runtime function per metric.
4. Build the runtime variant from the same already-expanded compiler output.
   Preserve ordinary functions, guards, literals, module attributes, callbacks,
   `on_load`, runtime-input resolvers, helper dependencies, and module names.
   Projection removes only tagged semantic metadata and diagnostic annotations;
   it must not pattern-match arbitrary user functions by a convenient name.
   Calls from runtime code to the authoring-only capture API are unsupported and
   diagnosed where statically visible; the runtime stub also fails explicitly.
5. Normalize non-executable file/line/debug annotations for all modules emitted
   from the affected compilation unit, including sibling modules in the same
   `.ex` file. Otherwise adding metric lines can still change a sibling BEAM.
   Preserve source-derived **executable literals**, including values expanded
   from `__ENV__.line`, `__ENV__.file`, `__MODULE__`, module attributes, and macros.
   If those values change, the edit has changed execution behavior and must
   change runtime identity. Do not call it a semantic-only change based on the
   textual location of the edit.
6. Build and validate a complete module inventory. Do not introduce whole-program
   dependency pruning: other customer/dependency modules remain included under
   existing execution requirements. Preserve external-resource and compiler
   dependency tracking in the authoring build; helper or resource edits invalidate
   affected capture/projection results. Unsupported compiler output or missing
   required expansion/debug information fails with a precise diagnostic. There
   is no fallback to excluding whole modules, source globs, or directories.

Elixir persists `@external_resource` paths in BEAM attributes. The semantic file
loader must therefore record compiler-owned semantic file dependencies separately
from ordinary user/query/runtime resources. Keep their paths and content digests
in authoring dependency tracking and the semantic capture, but remove exclusively
semantic entries from the runtime BEAM's attributes, debug/reflection data, and
packaged resource inventory. Replacing the semantic getter alone is insufficient.

Resource classification follows recorded compiler ownership, not filename or
directory heuristics. A path also declared by user code, used by an execution
query, or present in the required runtime resource inventory is shared/runtime;
it stays included and relevant changes invalidate execution identity. Do not
claim a semantic-only edit when a resource is runtime-observable or ownership is
unresolved. Semantic SQL files, source maps, and capture receipts must not leak
into release resource payloads through a broad copy of the authoring directory.
Preserve all other user attributes and reflection behavior.

The capture, runtime inventory, and contract snapshot are generated build data,
not additional deployable services or user-maintained files. Projection receipts
may identify the full authoring build for cache validation, but those receipts
must stay outside runtime identity and packaged runtime payloads: their source
digest changes on a formula edit. Runtime identity hashes the actual stable
runtime payload. New and removed runtime modules, dependencies, or executable
content must change it. Incremental builds must remove stale generated outputs.

#### Source locations and package compatibility

Execution-only getters and execution packages currently carry authored file/line
values. Normalize compiler-owned diagnostic fields to stable source roles and
positions relative to each SQL expression/query. Preserve the SQL text, node
order, real arguments, types, policies, bindings, and every executable value.
Keep physical Elixir locations in a separate authoring source map for build
diagnostics and inspection. Runtime errors report the stable source role and SQL
position; authoring tools can map that back using the selected authoring build.

Do not merely exclude fields from a hash while continuing to publish different
payload bytes under the same digest. Normalize the actual serialized execution
payload first and continue hashing/verifying all of that canonical payload.
This changes the execution-package/source-location contract and needs appropriate
schema/version/codec updates and fresh-process tests. The initial platform
upgrade can change identities; the formula-only stability claim applies after
both compared builds use this compiler and contract version.

#### The loaded code must match the inventory

`favn_local` owns constructing and launching an immutable source-runtime directory
from the verified inventory. It must not pass customer authoring `ebin` paths to
the runner as fallback paths. Include the correct `.app` module lists, dependency
paths, and required runtime resources. Validate package/inventory integrity before
start and prove after boot that loaded customer modules come from this inventory
with the expected bytes. A stale, incomplete, or mismatched inventory fails before
advertising runner readiness; no silent reuse of the previous identity.

The authoring/operator process can still load the full authoring variant. Project
compilation, editor tooling, and source diagnostics may therefore run again on a
metric edit. The requirement is that this does not replace the **runner** or
alter its execution packages, manifest, generation, or freshness identities.

Production assembly must stage the same runtime projection in the customer-owned
release/image workflow. Update the generated deployment recipe and canonical
instructions; ensure a later `mix release`/copy step cannot restore authoring BEAMs
over the staged runtime variants. Favn does not build or deploy customer images.
A semantic-only pipeline compiles authoring metadata and publishes its semantic
artifact using an existing compatible execution/runner release. It need not run
the image build at all. If the customer chooses to build an image with different
contents or labels, that image still requires its own immutable production ID.
Equal execution behavior is not permission to assign one ID to two image digests.

### Artifact and deployment boundaries

Use explicit Core contracts for a generated `DataContractSnapshot` and a
`SemanticManifest`. Do not add analytical definitions to `Manifest.Asset`, SQL
execution packages, runner registration, or execution-manifest hashing.

| Artifact | Contents and identity | Lifecycle |
| --- | --- | --- |
| Existing execution manifest/packages | Executable assets, enforced contracts/checks, runtime requirements, runner release map. Existing `mv_` identity remains. | Existing execution deployment and data publication. |
| Data-contract snapshot | Exported asset refs, resolved relation descriptors, contracts, relationships, asset dependencies, column lineage, and source contract fingerprints. No settings values, secrets, executable code, or runner release requirement. `dc_` plus full SHA-256 of canonical payload. | Generated build input and provenance, not a third deployed service. Can be exported with an execution build or independently from its same canonical compilation. |
| Semantic manifest | Snapshot reference, required contract projection, dimensions/time/hierarchies, metric records, dependencies, canonical expressions, macro projections, compiler/dialect versions. `sm_` plus full SHA-256 of canonical payload. | Independent build now; publication, activation, retention, and rollback in #720. |

The semantic artifact includes the public snapshot content required for catalog
projection, so a publisher does not need customer authoring modules. Referenced
upstream asset/column rows needed for lineage are included as data. It records
the exact snapshot used and separately the compatibility requirements for its
consumed fields. Build timestamps, machine paths, and presentation ordering do
not affect content identity. Description/display edits do change semantic
identity, but never execution identity.

The DuckDB plugin owns the build-time compiler capability behind a Core
behaviour. Core does not own a database session; Authoring does not depend on
runner execution. A missing compiler plugin is an explicit build error.

Closed serialization validates versions, required fields, references, limits,
and content digests on read. Unknown fields or enum values fail; external strings
must not create atoms. Write a complete validated artifact to a temporary path,
then atomically rename it into its immutable content-addressed directory.
Identical content is idempotent. An existing path with different bytes is an
integrity error; never overwrite it. Interrupted builds leave no selectable
partial artifact. Semantic builds do not modify the input snapshot.

#### Compatibility and the later publisher

Compatibility compares consumed columns, exact normalized types and nullability,
grain, time/key fields, relationship mappings, and referenced physical relation
descriptors. Additional unrelated columns are compatible. Removing an input,
changing its logical type, or changing a required grain/key is incompatible.
This comparison does not establish native precision/scale equivalence: those
details are absent from today's logical contracts. Native query binding/result
types remain the consumer engine's authority. A same-type business
reinterpretation cannot be detected mechanically and needs an explicit new
definition/version and owner review.

#720 must compare against the contract of the **served data generation**, not
merely the newest active execution manifest. A rebuilt target may still be
unpublished. The exact snapshot is build provenance; equality with its entire
hash or its runner release is not required for a compatible semantic deployment.
If served-contract evidence is unavailable, report compatibility as unknown and
do not automatically activate that semantic release.

Future publishing may use an already deployed compatible runner to execute a
bounded metadata task. It must not require a new runner release for a formula
edit. Initial installation of this Favn capability is naturally a platform
upgrade; subsequent semantic authoring changes are independent.

For #720, retain complete versioned rows/macros for the previous release until a
new release is complete and its pointer is activated. Replacing individual tables
and writing a pointer last is insufficient if it destroys rows the old pointer
still needs. Consumers capture one release ID and use it throughout a request;
rollback selects a retained compatible release. Macro namespaces are immutable
and retained with their releases. #718 specifies this handoff but implements no
activation store, scheduler, publisher, remote mutation, or retention job.

### Build workflow

The normal semantic build uses the project's existing asset discovery:

```sh
# Reads semantic blocks from the normal SQL asset files.
mix favn.build.semantics --output dist/semantics

mix favn.semantic.inspect --artifact dist/semantics/sm_<digest>/semantic.json \
  --metric sales.net_revenue --format json

mix favn.semantic.diff --from previous/semantic.json --to current/semantic.json
```

This command performs normal authoring compilation/capture and produces the
data-contract snapshot and semantic projection needed for validation, without
requiring `runner_releases`, booting a runner/orchestrator, or activating an
execution manifest. It must not interpret successful compilation as authorization
to rebuild or deploy a runner. Normal local reload consumes the same projection
boundary before making its runner-replacement decision.

For split CI stages, expose the generated, closed authoring bundle as an optional
build input. It contains the resolved semantic declarations and exact contract
snapshot, never closures or arbitrary executable AST. A subsequent build can use
`mix favn.build.semantics --input PATH --output dist/semantics` without source
modules. The input mode and project capture mode are mutually exclusive. The
bundle is schema-versioned, bounded by the existing snapshot/semantic limits,
content-verified, and tied to its captured source build; stale or mismatched
capture results fail. Users do not hand-edit it or maintain a second semantic
file list.

Semantic validation, macro rendering, atomic local artifact writing, and the
separately owned DuckDB validation process have the responsibilities described below.
Independent deployed publication/activation remains #720. Relationships still
belong to the execution contract: changing enforcement is not a semantic-only
change just because it appears beside a semantic block.

### Inspection, diffs, and limits

Inspection exposes dimensions, relationships, hierarchy levels, time rules,
metric SQL, ordered inputs, output types, dependencies, and generated invocation
examples from the decoded artifact. V1 offers CLI text/JSON and pure bounded
Core inspection functions. UI integration is deferred until #720 provides a
durable catalog owner/read facade; View must not read authoring internals.

Diffs use stable model/metric IDs. Formula, input order/bindings, output type,
unit, time rules, minimum grain, removal, and rename are breaking semantic
changes. Description/format-only changes are informational; additions are
additive unless they introduce a rejected collision. Dependency changes propagate
to affected composed metrics. Relationship/key changes are reported with their
execution-contract impact. Diff classification informs review; it does not stop
independent immutable versions from coexisting.

| Boundary | Initial limit and failure behavior |
| --- | --- |
| Semantic artifact | 16 MiB encoded, 256 models, 256 metrics per model; reject before writing on overflow. Existing data snapshot/manifest size limits still apply; cap the standalone snapshot at 64 MiB. |
| Metric | 32 arguments, 16 KiB authored SQL, 64 KiB expanded SQL, dependency depth 32; check expansion size during traversal, not after allocating an unbounded expansion. |
| Names/text | 64-byte author names, 1,024-byte descriptions, 128-byte custom units; reject rather than truncate definitions. |
| Model metadata | One source, one dimension, one time declaration, 32 hierarchies, 16 levels per hierarchy. |
| Relationships | 32 per source; at most two grouped checks per relationship. Preserve the existing 50 authored-check budget and 18 non-relationship contract checks; explicitly extend the generated-check cap to 82. |
| Build validation | One local session in a separately owned OS worker; 10-second startup, 5-second per-expression and 5-minute total build deadlines. Shutdown escalates from terminate after 2 seconds to kill, then waits at most 3 seconds for confirmed exit. No automatic extension download. |
| Inspection/diff | Deterministic ordering, limit default 100/max 1,000, explicit continuation cursor; CLI may stream pages rather than accumulate unbounded output. |
| Diagnostics | At most 100 diagnostics per build plus a count of omitted diagnostics; each message at most 1,024 bytes. |

These are initial closed contract limits, not a new configuration framework.
Keep source spans separate from serialized release identity. Authoring files are
trusted project code, like the existing DSL; isolated formula validation is not
a sandbox for malicious Elixir source.

### Scope, issue reconciliation, and non-goals

| Issue proposal | Final plan and reason |
| --- | --- |
| Same-file semantic authoring | `semantic :name do` in the SQL asset; separate semantic and execution artifacts. |
| `measure` plus structured/SQL `metric` | One function-style `metric` with ordinary SQL and named composition; avoids duplicate concepts and expression languages. |
| Metric/measure/column graph | Metric/metric/column graph with the same explicit transitive provenance and ordered consumer inputs. |
| `relationship` targets a semantic dimension, with `required:` | Execution contract references the target's structured grain; source nullability is authoritative. |
| Candidate-only one-to-one check | Check the resulting source relation after mutation so incremental collisions are detected and rolled back. |
| Synthetic binding proves dependency isolation | Parse and reject non-expression access first, then bind declared typed inputs. A subquery can otherwise bypass that assumption. |
| `time_aggregate: sum` and later `first` request | `aggregate`, `first`, `last`, `none`, with explicit per-entity observed-row selection and inherited composition rules. |
| Dimension attributes inside formulas and related-dimension hierarchy levels | Deferred; v1 hierarchies use local attributes and metrics use one fact's columns. Cross-grain denominator/deduplication needs a separate design. |
| General time column | V1 requires a business DATE with day/month grain. Timestamp representation and DST/hourly behavior need a later explicit contract; logical `:datetime` alone is insufficient. |
| UI inspection in #718 | CLI/typed artifact inspection now; UI integration after the durable catalog owner exists in #720. |
| One example with all measures in a daily-inventory fact | Complete example application with Store/Product dimensions, a Sales flow fact, and an Inventory snapshot fact, making the different time rules unambiguous. |

#718 implements authoring, contract export, validation, artifact build/decode,
inspection/diffs, generated macro definitions/docs, and enforced relationships.
It does not implement SQL target sync (#720), publication/freshness state (#721),
AI discovery/MCP (#719), query planning, runtime metric evaluation, dashboards,
result caches, row-level authorization, multiple dialects, or cross-fact metrics.
The later issues need their own records reconciled with these release boundaries;
their existing execution-manifest-trigger wording is not an immutable constraint.

## Implementation slices and complexity budget

This is the single estimate for the full scope described in this record. It is
not a commitment to implement all of it: the simplification decisions above
remain open. Supporting lines include tests, fixtures, examples, and canonical
documentation; they are not all production code.

| Slice | Outcome and owner | Depends on | Production added/deleted | Supporting added/deleted |
| --- | --- | --- | --- | --- |
| A | Verified expanded-code projection, stable execution source roles, runtime inventory, local launcher and production assembly integration | None; feasibility gate first | +450–750 / -40–90 | +450–750 / -20–50 |
| 1 | Core snapshot/semantic types, closed codec and dependency/compatibility contracts | A | +300–500 / -0–30 | +250–400 / -0–20 |
| 2 | Contract relationships and transactional checks | 1 | +300–500 / -20–60 | +300–500 / -10–40 |
| 3 | Nested semantic DSL and separate typed capture in SQLAsset; formula composition | 1, A | +450–700 / -10–40 | +400–600 / -10–30 |
| 4 | Native DuckDB validation and macro rendering | 3 | +300–500 / -0–20 | +350–550 / -0–20 |
| 5 | Project/bundle semantic build, owned validation worker, inspection/diff and Mix tasks | 1, 3, 4 | +600–1,000 / -20–60 | +450–750 / -20–40 |
| 6 | Same-file examples, guides, Favn.AI routing and end-to-end isolation acceptance | 2–5 | +20–60 / -0–20 | +350–550 / -20–60 |

Totals: production additions 2,420–4,010; supporting additions
2,550–4,100. Count runtime projection/launch tests in A and overall workflow
tests in slice 6, without double counting. Exclude this record, generated files,
locks, vendored code, and formatting-only changes. Explain overruns above an upper estimate by more than 25% or 100 lines,
whichever is smaller, and materially fewer deletions than planned. A general
module-pruning framework, a second source evaluator, or a new deployment service
is outside this budget and scope.

Core owns the typed capture/projection/inventory contracts and canonical identities.
Authoring owns compiler integration and semantic capture. Local tooling owns
runtime staging, source identity, launch paths, and reload decisions. Public build
tasks and deployment recipes consume the same inventory. Runner execution and
relationship checks keep their existing owners; View and storage do not acquire
semantic deployment state in #718.

### Compiler isolation acceptance gate

Implement and qualify the compiler/runtime split first. It is the feasibility
gate for the full rebuilt-artifact identity guarantee, not an optimization to
add after shipping the DSL. If supported compiler output cannot meet these
invariants within a bounded implementation, return for plan re-review rather
than silently reverting to separate user files or weakened hashing.

| Scenario | Required evidence |
| --- | --- |
| Same-file DSL | Complete Store, Sales, and Inventory examples compile; duplicate blocks, missing contracts, bad nesting, declarations after `query`, explicit `source`, and collisions fail precisely. |
| Formula-only edits | Change inline SQL, descriptions, argument metadata, metric count, and entire block presence; semantic identity changes while runtime inventory bytes, execution package/manifest, runner release, target compatibility, and freshness/generation identities stay equal. |
| Source-line shifts | Add/remove lines before query/check declarations and an ordinary sibling module; canonical execution payloads and runtime inventory stay equal when executable values are unchanged. Verify diagnostic source maps still identify the original source. |
| Real execution changes | Change query SQL, contracts, relationship policies, runtime helper bodies, compile-time constants, runtime-input resolvers, same-file sibling code, or runtime resources; the owning execution/package/runtime identities must change as appropriate. A semantic-only path cannot silently carry these into an old runner. |
| Observable source constants | A helper returns `__ENV__.line` or another source-derived literal. If its expanded value changes, runtime identity changes; normalization cannot erase that behavior. |
| One evaluation | Instrument a compile-time macro/side effect and prove projection uses the captured expanded output, not a second source evaluation. |
| Formula files | Edit/delete an adjacent metric SQL file without touching the Elixir file; recapture semantic output and reject missing files. Add/remove/rename a file-backed metric or block; verify exclusively semantic resource paths/content are absent from runtime Attr/debug/reflection and packaged resources, with unchanged execution identity. A resource also used by a query, user declaration, or runtime inventory remains included and changes execution identity as appropriate. |
| Incremental correctness | Clean/incremental/parallel builds produce the same projections; deletions/renames remove old capture data and modules. No previous build's semantic root is reused after source changes. |
| Runtime projection fidelity | Preserve ordinary functions, attributes, callbacks, helper/resource dependencies, execution getters and direct `asset/1` behavior; semantic root returns its fixed unavailable result. Check exports, `__info__`, specs and reflection for consistency. |
| Actual local load path | Boot with authoring customer paths unavailable; inspect `:code.which`/object code and assert loaded module bytes match the verified inventory. Demonstrate semantic-only reload keeps the existing runner process. |
| Production packaging | Assemble the release from the inventory and inspect packaged module bytes/paths; show semantic-only publication skips image construction and does not reassign an existing production ID to different image contents. |
| Failure boundaries | Missing expansion information, unsupported compiler versions, corrupt/incomplete inventory, stale capture, projection mismatch and unavailable runtime modules prevent readiness/publication with bounded diagnostics. |

The formula, time-selection, dependency, relationship, serialization, native
cleanup, and consumer SQL acceptance scenarios below also apply.
No supported-runtime guarantee is claimed from merely parsing these examples.

## Operational design

### Failures, diagnostics, and recovery

Invalid declarations, references, SQL, types, collisions, usage-rule composition,
limits, or serialization block artifact creation. Diagnostics name the model,
metric/relationship, source location, and stable failure class. The build reports
safe expected/actual types or missing column names, never credentials, row values,
full native exception terms, or complete arbitrary SQL in logs. An explicit local
inspection command may show the authored formula because that is its purpose.

There is no service loop or retry worker in #718. A one-shot authoring owner
launches a separate OS process for DuckDB compilation; Core defines only the
typed request/result behaviour. Use bounded framed messages, a startup handshake,
and the limits above. The owner controls the worker/process group, terminates it
on timeout/cancellation, escalates to kill, and waits for confirmed exit. Worker
owner-loss monitoring must also stop native work after a parent crash, using an
independent watchdog or OS parent-death facility; an Elixir query task alone is
insufficient. Reuse a proven subprocess facility if available, rather than a new
general-purpose worker framework.

Only successful validation followed by confirmed worker exit permits artifact
publication. A cleanup failure reports `compiler_cleanup_unconfirmed`, produces
no artifact, and identifies the owned process for operator cleanup without
claiming it stopped. Unsupported process-control environments fail preflight.
Test startup failure, blocked native calls, caller cancellation, owner crash,
forced termination, and unconfirmed cleanup. Remove temporary outputs on known
failure; stale temporary paths are never selectable as artifacts. A new explicit
invocation can rebuild the same deterministic artifact. Parser/extension failures
do not trigger downloads or silently skip validation. Bound native errors before
presenting them.

Relationship checks inherit existing publication failure, cancellation, and
unknown-commit handling. Do not retry a possibly committed transaction on the
basis of a new check error category. Invalid check results are failures regardless
of `on_violation: :warn`. Logs use stable claim IDs and bounded counts, not failing
business keys. Emit one build outcome and a bounded set of diagnostics, not a
log per input column or scanned row.

### Migration and rollout

Favn is pre-v1: extend the closed execution contract and bump its appropriate
schema/package/runner compatibility versions for relationships, with explicit
rejection of incompatible envelopes. Register every new reachable contract type
in persisted execution codecs and prove decoding in a fresh process. Do not
add analytical models to that persistence codec or execution package.

There is no PostgreSQL schema migration or deployed semantic activation in #718.
Existing assets without semantics/relationships keep their behavior. Contracts
with newly enforced relationships need data qualification before adoption. The
initial platform release provides the new compiler/check capability; semantic-only
releases thereafter require neither a runner rebuild nor data republication.
Rollback of a relationship deployment follows normal execution/data-generation
compatibility; semantic rollback is separately owned by #720.

## Verification plan

| Acceptance criterion | Required proof | Owner |
| --- | --- | --- |
| DSL is one clear SQL formula interface | Inline/file parity; duplicate/unknown options; names, source refs and descriptions; examples compile. | Authoring |
| Exact bindings and provenance | Reject missing/unused arguments, swapped child-call bindings, cycles, cross-model refs and collisions; traverse metric/metric/column paths without parsing SQL. | Core/Authoring |
| SQL isolation and type correctness | Reject undeclared bare/placeholder columns, scalar subqueries, table reads/functions, windows, nested aggregates, UDFs and multiple statements. Accept distinct string count, CASE/FILTER/casts, null and zero-denominator examples. Compare DECIMAL(18,2)/DECIMAL(38,10) bindings under one logical contract: native result types may differ and must be labeled validation evidence. | DuckDB plugin |
| Canonical formula and macro agree | Fixed Sales fixture yields revenue 210 and weighted price 42; compare inline SQL, macro and composition; empty/null inputs retain documented behavior. | Plugin integration |
| Projection remains narrow | EXPLAIN against a table with unrelated columns; explicit-argument macro scans the same required input columns as equivalent inline SQL. No assertion about all optimizer plans. | Plugin integration |
| First/last semantics are precise | Ragged entity dates: A=10 then 12, B=20 only earlier; closing is 32, not 42 or 12. Opening is 30. Test multiple months, missing entities, point-date selection and duplicate grain failure. Reject :datetime and hourly time declarations in v1. | Consumer example/integration |
| Usage rules cannot be weakened | Composed metrics inherit compatible selectors/minimum grain; reject aggregate/last, first/last, and unsupported direct-aggregate/composition mixtures. Metadata distinguishes query requirements from macro enforcement. | Core |
| Relationship publication is correct | Required/optional/composite/snowflake roles, orphan and duplicate target keys, full-table and incremental one-to-one collisions; fail rolls back, warn persists bounded quality result, unsupported capability fails before writing. Use different keys in active and pinned dependency generations and prove the check reads the pinned generation. | Authoring/Runner/plugin |
| Execution uses no authoring code | Compiled check packages execute after unloading authoring modules; persisted populated relationships decode in a fresh BEAM. | Core/Runner |
| Artifact is deterministic and closed | Reordered file discovery has same digest; meaningful input order differs; corrupt/oversized/unknown records fail; interrupted write leaves no artifact; repeated build is idempotent. | Core/build tooling |
| Semantic deployment is independent | Formula-only edit changes only semantic artifact; clean/incremental execution builds retain release/package/manifest/generation/freshness identities; generated-bundle mode works without source asset modules. | Local acceptance |
| Compatibility uses consumed shape | Extra unreferenced column accepted; missing/type-changed input, key/time/grain/relation changes rejected; unknown served contract stays unknown. | Core |
| Consumers do not guess arguments | Example dashboard emitter reads artifact metadata, chooses aliases and emits the documented SQL; generated docs use same input records; negative same-type manual argument swap is demonstrably not blocked by DuckDB. | Example/acceptance |
| Diff is meaningful | Added/removed/renamed/formula/unit/input order/time/minimum-grain changes classified; description/format changes informational; transitive effects included. | Core |
| Limits and cleanup work | Budget boundary and one-over cases, expansion blowup/depth, OS-worker startup failure, blocked native call, timeout/cancellation, parent crash, terminate/kill/reap, and cleanup failure. Confirm process exit and no artifact on failure; unconfirmed cleanup must never report success. | Owning layers |

Start with owning app tests using `mise exec -- mix do --app APP cmd mix test`
and the repository's tier flags. During implementation run formatting,
warnings-as-errors compilation, relevant fast/acceptance/slow integration checks,
the tag-tier guard, public docs/examples, and independent final review. Any native
DuckDB qualification must use the repository-supported runtime pin, not only the
developer CLI. Keep tests that protect behavior; avoid mirroring every struct.

The current documentation-only delivery needs relative-link checks, Elixir/SQL
example syntax review, Mermaid rendering review, and `git diff --check`. It does
not warrant running the umbrella implementation suite. No live deployment,
production load result, metadata publication, or MCP operation is claimed.

## Risks and settled tradeoffs

| Risk | Decision and limit |
| --- | --- |
| Wrong same-type macro arguments | Publish exact ordered bindings and generated examples. Direct SQL remains flexible; catalog metadata cannot enforce provenance at arbitrary call sites. |
| Semi-additive data produces plausible wrong totals | Require explicit selectors and reject mixed-selector composition. First/last are observed-within-period semantics, not carry-forward accounting. |
| Native parser representation changes | Keep version-sensitive validation in the DuckDB plugin; pin and qualify supported versions, fail closed on unknown expression shapes. |
| Expanding the allowed SQL set grows a query compiler | Support one bounded aggregate expression; reject full queries and automatic dashboard planning. Extensions must justify new grammar with real examples. |
| Relationships create expensive scans or become stale later | Reuse transactional checks, expose policy/quality state, qualify representative incremental plans, and state the snapshot-only guarantee. No permanent FK promise. |
| Same-file metadata leaks into execution identity | Verified runtime projection and clean/incremental inventory proof under the full isolation scope. The simpler deployment-only alternative is an open decision above. |
| Target cannot store macros or served version is uncertain | #720 publishes definitions for local installation and gates automatic activation on served-contract evidence. |

## Planning status and verification

Implementation has not started. At the user's request, this document contains
one consolidated plan; superseded proposals and duplicated estimates have been
removed. Complexity reductions are proposals for discussion, not silently
accepted changes to the contracts above. Final implementation approval waits for
those decisions and an independent review of the resulting scope.

Static source inspection confirms that local runner identity hashes BEAM files,
local launch inherits code paths, and execution packages include source spans.
These facts explain the cost of the full identity guarantee. No Tidewave runtime
inspection, deployed isolation, performance qualification, or Favn integration
is claimed. Earlier native SQL probes established revenue 210, weighted price
42, and ragged closing stock 32; they do not prove the unimplemented compiler.

| Consolidation check | Result |
| --- | --- |
| Independent review | `review_semantic_plan` approved this consolidated discussion document on 2026-09-17 after rechecking the isolated-build clarification. No remaining blocking findings. This is not implementation approval. |
| Relative links | All repository targets exist. |
| Elixir examples | All three retained blocks parse with `Code.string_to_quoted/1`; proposed APIs are not implemented or compiled. |
| Whitespace | `git diff --check` passes. |
| Diagrams | Both diagrams render on GitHub at `25d7ad80`: current flow has 8 nodes and proposed flow has 11. The complexity discussion is visible in the rendered document. |

Implementation checks remain planned. No replacement complexity budget is
approved until the simplification decisions are settled.
