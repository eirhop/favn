# SQL Output Contracts

Reader: authors who want a SQL asset to publish an explicit, enforceable data
contract.

Documentation type: how-to and reference guide.

Start with `Favn.SQLAsset`. Use a contract for stable output shape, grain,
keys, exact or bounded volume, reusable column metadata, and column lineage.
Use custom `check` declarations for asset-specific quality rules expressed as
arbitrary SQL.

## Define A Contract

A table or incremental SQL asset may declare one contract:

```elixir
defmodule MyApp.Assets.NormalizedRecords do
  use Favn.SQLAsset

  materialized :table

  contract do
    grain by: [:record_id], description: "one normalized record"

    column :record_id, :integer,
      null: false,
      description: "stable record identity",
      tags: [:identifier],
      from: [{MyApp.Assets.SourceRecords, :source_id}],
      via: :transformation

    column :payload, :json,
      from: [{"external.records", "payload"}],
      via: :identity

    column :observed_at, :datetime, null: false

    unique [:record_id, :observed_at]

    row_count min: 1,
      when: :target_exists,
      on_violation: :skip_materialization
  end

  query do
    ~SQL"""
    select
      source_id as record_id,
      payload,
      observed_at
    from source_records
    """
  end
end
```

Write SQL expressions, casts, aliases, joins, and backend-specific syntax in
`query`. The contract validates and documents the staged candidate produced by
that query.

## Contract Vocabulary

### Grain

Grain says what one row represents. Use structured columns when possible:

```elixir
grain by: [:record_id, :observed_at], description: "one observation per record"
```

`by:` columns must exist in the contract and must be non-null. They generate a
uniqueness check over the candidate. The description remains operator-facing
context.

When row identity is real but cannot be expressed by output columns, use a
description alone:

```elixir
grain description: "one deterministic result row for each generated input unit"
```

Descriptive grain is useful metadata, but Favn cannot mechanically prove it.
A declared grain therefore requires `by:`, `description:`, or both.

### Columns

Columns are ordered. Their order is part of candidate schema validation.

```elixir
column :record_id, :uuid,
  null: false,
  description: "stable identity",
  tags: [:identifier],
  from: [{MyApp.Assets.SourceRecords, :source_id}],
  via: :transformation,
  renamed_from: :id
```

| Option | Default | Meaning |
| --- | --- | --- |
| `null` | `true` | Whether published rows may contain null. `false` generates a candidate check. |
| `description` | `nil` | Human context for operators and generated documentation. |
| `tags` | `[]` | Stable string or atom labels; manifests normalize them to strings. |
| `from` | `[]` | Explicit source columns. It is always a plain list. |
| `via` | `nil` | `:identity`, `:transformation`, or `:aggregation`. |
| `renamed_from` | `nil` | Previous column atom used by semantic contract diffing. It does not rename SQL. |

Logical types are `:boolean`, `:integer`, `:float`, `:decimal`,
`:string`, `:binary`, `:date`, `:time`, `:datetime`, `:json`, and `:uuid`.
Favn maps common backend-native type names into these logical types before
comparison.

Pair `via:` with at least one `from:` source, and pair `:identity` with exactly
one source. Omit both when lineage is not known.

Lineage uses three unambiguous tuple shapes:

```elixir
from: [
  {MyApp.Assets.SourceRecords, :source_id},
  {{MyApp.Assets.GeneratedRecords, :compact}, :record_id},
  {"external.records", "record_id"}
]
```

The tuples compile into typed manifest values. Use `from:` to record lineage
explicitly.

### Reusable Column Fragments

Use an explicit column-only fragment when the same metadata columns repeat
across a data layer:

```elixir
defmodule MyApp.Contracts.AuditMetadata do
  use Favn.SQL.ContractFragment

  column :processed_at, :datetime, null: false,
    description: "start time of the Favn run"

  column :favn_run_id, :string, null: false
end
```

Include it exactly where those columns appear in each asset's ordered output:

```elixir
contract do
  column :record_id, :integer, null: false
  include MyApp.Contracts.AuditMetadata
  column :payload, :json
end
```

A fragment contains ordered `column` declarations. Keep grain, keys, row counts,
and custom checks in the consuming asset contract, where their meaning remains
visible. Include a fragment once at its output position and keep every flattened
column name unique. Favn stores the flattened canonical columns used for
validation plus a bounded provenance record containing the fragment module,
start index, and column names. The compact manifest index therefore carries the
complete operator-facing contract while executable checks remain in the
content-addressed execution package.

### Unique Keys

Declare additional uniqueness claims separately from grain:

```elixir
unique [:record_id, :version]
```

Every referenced column must exist. Duplicate key declarations are rejected.
Each unique key generates a candidate check.

### Row Count

Declare one or more exact or bounded candidate row-count claims:

```elixir
row_count equals: 500, on_violation: :fail
row_count min: 1, on_violation: :fail
row_count max: 10_000, on_violation: :warn
row_count min: 1, max: 10_000, on_violation: :fail
```

Choose either `equals:` or a `min:`/`max:` bound. Keep `min:` less than or equal
to `max:` and use non-negative integer literals. Repeated declarations retain
their authored order and each declaration has its own `when:` and
`on_violation:` options.

An exact count may come from the asset's normal settings or runtime params:

```elixir
row_count equals: param(:expected_rows), on_violation: :fail
```

Use `param/1` as the value of `equals:` with a literal atom name.
The compiled contract stores a typed parameter requirement, not a resolved
value. The runner applies the normal settings/params collision rules and
requires a non-negative integer before opening a SQL session. Values remain
bound SQL parameters and are never interpolated into SQL source or copied into
error details.

For an empty refresh where an existing target should remain available:

```elixir
row_count min: 1,
  when: :target_exists,
  on_violation: :skip_materialization
```

`when:` controls applicability. `on_violation:` controls the outcome after an
applicable claim returns false. This is the same vocabulary used by custom SQL
checks.

| `on_violation` | Asset result | Target | Downstream work |
| --- | --- | --- | --- |
| `:fail` | failed | transaction rolls back | does not continue through normal success gating |
| `:warn` | successful with `quality_status: :warning` | candidate is written | continues |
| `:skip_materialization` | successful with `quality_status: :warning` and `write_outcome: :no_op` | existing target is unchanged | continues |

`:skip_materialization` requires `when: :target_exists`. On first-target
bootstrap, that claim is condition-skipped and materialization proceeds rather
than pretending a missing target is a successful no-op.

Combine exact reconciliation with empty-candidate protection by declaring the
failing reconciliation first:

```elixir
row_count equals: param(:expected_row_count),
  on_violation: :fail

row_count min: 1,
  when: :target_exists,
  on_violation: :skip_materialization
```

For a zero-row candidate, the no-op is reachable only when exact reconciliation
also expects zero. A non-zero expectation fails first and rolls back; the later
no-op claim cannot hide it. When the target is missing, the second claim is
condition-skipped and normal bootstrap behavior continues.

### Favn-Owned Runtime Inputs

SQL asset queries and reusable `defsql` bodies may bind two execution identity
values owned by Favn:

```sql
select
  @favn_run_id as favn_run_id,
  @favn_run_started_at as processed_at
```

`@favn_run_id` is the current non-empty run id and
`@favn_run_started_at` is the runner context's UTC `DateTime`. They use the same
bound-parameter path as `@window_start` and `@window_end`. All four names are
reserved and cannot be supplied by settings, submitted params, runtime-input
resolvers, or contract `param/1`.

## What Favn Checks Automatically

Contracts have two enforcement classes.

Hard structural validation happens after candidate staging and before target
mutation:

- exact column names and order;
- compatible logical types; and
- nullability metadata when the adapter explicitly marks it reliable.

A structural mismatch always fails and rolls back. It is not controlled by
`on_violation`, because publishing a structurally incompatible target would
break the contract itself.

Data claims compile into the ordinary transactional check engine:

- every `null: false` column must contain no null values;
- structured grain columns must be unique;
- every `unique` declaration must be unique; and
- every `row_count` declaration must satisfy its configured exact, minimum,
  maximum, or range constraint in authored order.

Generated checks use grouped stable claim identities: `columns.not_null`,
`keys.unique`, `row_count.equals.literal.N`, `row_count.equals.param.NAME`,
`row_count.min.N`, `row_count.max.N`, and `row_count.range.MIN.MAX`. Repeated
semantic identities receive an `.occurrence.N` suffix. Each
row-count check computes `count(*)` once and returns the actual count plus the
applicable expected/bound metrics. Grouping makes a wide contract a bounded
number of scans and durable results instead of one check per required column.
Durable results identify their origin as `:contract`; authored checks use origin
`:authored`. Both appear together in the asset assurance UI and share the same
fail, warn, and no-op behavior.

## Transaction And Result Model

For a contracted asset, Favn:

1. validates required contract parameters before opening a SQL session;
2. opens the adapter transaction;
3. stages the candidate once;
4. inspects and validates its structural schema;
5. runs contract-generated and authored before-materialization checks;
6. applies the write plan unless a claim selects a successful no-op;
7. runs authored after-materialization checks; and
8. commits only when required validation succeeds.

Candidate schema observations, structural differences, check results,
`quality_status`, and `write_outcome` are persisted with run metadata. The
orchestrator exposes the authored contract beside those observations so the UI
can show expected versus observed shape, lineage, Contract checks, and Custom
checks without querying the runner or adapter.

Use contracts with snapshot table or incremental materialization and a SQL
adapter that supports staged transactional write plans and candidate column
inspection.

## Bounds

One contract supports up to 1,000 ordered columns, 128 explicit fragment
compositions, 128 explicit unique keys, and 16 ordered row-count claims.
Automatic enforcement uses one grouped required-column check, one grouped key
check, and one check per row-count claim, for at most 18 generated checks. Wide
schemas do not consume the separate budget of 50 authored custom checks.
Candidate schema evidence retains up to 1,000 observed columns. If an adapter
reports more, validation fails with a structured column limit difference and
persists the total count plus an explicit truncation flag.

Each custom or generated check still uses the metric and message limits in
[Transactional SQL Asset Checks](sql-asset-checks.md).

## Evolve A Contract Deliberately

Favn can compare two compiled contracts without guessing intent. Added,
removed, reordered, renamed, type-changed, nullability-changed, grain, key, and
row-count changes are represented structurally.

Fragment provenance is deliberately separate from semantic compatibility.
`Favn.SQL.Contract.Diff.provenance_between/2` reports fragment additions,
removals, moves, and changed flattened membership. Changing only provenance
keeps identical canonical columns semantically identical.

Use `renamed_from:` only when a rename is intentional:

```elixir
column :record_id, :integer, null: false, renamed_from: :id
```

This records evolution intent for review and UI diffing. Emit `record_id`
explicitly in the query. The immutable manifest index and referenced execution
package capture the complete authored definition, while semantic diffing
explains what changed.

## Keep Custom Rules In `check`

The contract DSL stays focused on claims that are reusable, typed, and useful
to tooling. Use `check` for a rule that needs arbitrary SQL:

```elixir
check :values_are_supported,
  at: :before_materialize,
  on_violation: :warn,
  message: "Candidate contains values outside the preferred set" do
  ~SQL"""
  select
    count(*) filter (where state not in ('ready', 'pending')) = 0 as passed,
    count(*) filter (where state not in ('ready', 'pending')) as invalid_rows
  from query()
  """
end
```

Keep named parameterized and other arbitrary SQL rules in a custom `check`.

For custom check phases, runtime relations, metrics, and result limits, read
[Transactional SQL Asset Checks](sql-asset-checks.md). For compiled structures,
read `Favn.SQL.Contract`, `Favn.SQL.Contract.Column`,
`Favn.SQL.ContractFragment`,
`Favn.SQL.Contract.Fragment`, `Favn.SQL.Contract.Composition`,
`Favn.SQL.Contract.Param`, and `Favn.SQL.ContractValidation`.
