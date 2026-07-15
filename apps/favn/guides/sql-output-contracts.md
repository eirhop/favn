# SQL Output Contracts

Reader: authors who want a SQL asset to publish an explicit, enforceable data
contract.

Documentation type: how-to and reference guide.

Start with `Favn.SQLAsset`. Use a contract for stable output shape, grain,
keys, minimum volume, and column lineage. Use custom `check` declarations for
asset-specific quality rules that do not belong in the reusable contract DSL.

## Define A Contract

A table or incremental SQL asset may declare one contract:

```elixir
defmodule MyApp.Assets.NormalizedRecords do
  use Favn.SQLAsset

  @materialized :table

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

The contract describes the candidate produced by `query`; it does not generate
the query or its `select` list. Keeping those responsibilities separate leaves
SQL expressions, casts, aliases, joins, and backend-specific syntax explicit.
There is deliberately no `select contract()` helper.

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

Supported logical types are `:boolean`, `:integer`, `:float`, `:decimal`,
`:string`, `:binary`, `:date`, `:time`, `:datetime`, `:json`, and `:uuid`.
Favn maps common backend-native type names into these logical types before
comparison. Arrays and other collection types do not satisfy scalar contracts;
model them explicitly only after Favn adds a corresponding logical type.

When `via:` is present, `from:` must contain at least one source;
`:identity` requires exactly one. Omit both when lineage is not known.

Lineage uses three unambiguous tuple shapes:

```elixir
from: [
  {MyApp.Assets.SourceRecords, :source_id},
  {{MyApp.Assets.GeneratedRecords, :compact}, :record_id},
  {"external.records", "record_id"}
]
```

The tuples compile into typed manifest values. An `external()` or `input()`
wrapper would add vocabulary without adding information, so the canonical DSL
uses `from:` directly. Favn does not infer column lineage from SQL text.

### Unique Keys

Declare additional uniqueness claims separately from grain:

```elixir
unique [:record_id, :version]
```

Every referenced column must exist. Duplicate key declarations are rejected.
Each unique key generates a candidate check.

### Row Count

Declare a minimum candidate row count:

```elixir
row_count min: 1, on_violation: :fail
```

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
- `row_count` must satisfy its configured minimum.

Generated checks use grouped stable claim identities: `columns.not_null`,
`keys.unique`, and `row_count.min.N`. Grouping makes a wide contract a bounded
number of scans and durable results instead of one check per required column.
Durable results identify their origin as `:contract`; authored checks use origin
`:authored`. Both appear together in the asset assurance UI and share the same
fail, warn, and no-op behavior.

## Transaction And Result Model

For a contracted asset, Favn:

1. opens the adapter transaction;
2. stages the candidate once;
3. inspects and validates its structural schema;
4. runs contract-generated and authored before-materialization checks;
5. applies the write plan unless a claim selects a successful no-op;
6. runs authored after-materialization checks; and
7. commits only when required validation succeeds.

Candidate schema observations, structural differences, check results,
`quality_status`, and `write_outcome` are persisted with run metadata. The
orchestrator exposes the authored contract beside those observations so the UI
can show expected versus observed shape, lineage, Contract checks, and Custom
checks without querying the runner or adapter.

Contracts require snapshot table or incremental materialization and a SQL
adapter that supports staged transactional write plans and candidate column
inspection. Contracted views are rejected.

## Bounds

One contract supports up to 1,000 ordered columns and 128 explicit unique keys.
Automatic enforcement is grouped into at most three checks—required columns,
keys, and row count—so wide schemas do not consume the separate budget of 50
authored custom checks. Candidate schema evidence retains up to 1,000 observed
columns. If an adapter reports more, validation fails with a structured column
limit difference and persists the total count plus an explicit truncation flag.

Each custom or generated check still uses the metric and message limits in
[Transactional SQL Asset Checks](sql-asset-checks.md).

## Evolve A Contract Deliberately

Favn can compare two compiled contracts without guessing intent. Added,
removed, reordered, renamed, type-changed, nullability-changed, grain, key, and
row-count changes are represented structurally.

Use `renamed_from:` only when a rename is intentional:

```elixir
column :record_id, :integer, null: false, renamed_from: :id
```

This records evolution intent for review and UI diffing. The query must still
emit `record_id`; Favn does not rewrite SQL or upgrade stored data.

Contract revision numbers are intentionally absent. The manifest version is the
immutable revision of the complete authored definition, while semantic diffing
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

Do not add named parameterized check declarations to a contract merely to avoid
writing SQL. A custom `check` is clearer until a claim is common enough to earn
a typed contract primitive.

For custom check phases, runtime relations, metrics, and result limits, read
[Transactional SQL Asset Checks](sql-asset-checks.md). For compiled structures,
read `Favn.SQL.Contract`, `Favn.SQL.Contract.Column`, and
`Favn.SQL.ContractValidation`.
