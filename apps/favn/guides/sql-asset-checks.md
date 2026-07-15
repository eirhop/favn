# Transactional SQL Asset Checks

Reader: authors who need to validate a table or incremental SQL asset before
Favn commits its materialization.

Documentation type: how-to and reference guide.

Start with `Favn.SQLAsset` for the complete SQL asset DSL. This guide focuses on
the `check/3` declarations available to table and incremental assets.

## Define Checks

Declare checks in the SQL asset module. Give every check a unique atom name and
choose when it runs and what a false result means.

```elixir
defmodule MyApp.Lakehouse.Mart.Sales.Orders do
  @moduledoc "Validated order mart used by sales reporting."

  use Favn.SQLAsset

  @materialized :table

  check :candidate_has_valid_keys,
    at: :before_materialize,
    on_false: :fail,
    message: "Every candidate row must have an order id" do
    ~SQL"""
    select
      count(*) filter (where order_id is null) = 0 as passed,
      count(*) filter (where order_id is null) as invalid_rows
    from query()
    """
  end

  check :keep_existing_target_when_empty,
    at: :before_materialize,
    when: :target_exists,
    on_false: :skip_materialization,
    message: "The candidate was empty; the existing target was kept" do
    ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
  end

  check :known_statuses,
    at: :after_materialize,
    on_false: :warn,
    message: "The order mart contains unknown statuses" do
    ~SQL"""
    select
      count(*) filter (where status not in ('open', 'closed')) = 0 as passed,
      count(*) filter (where status not in ('open', 'closed')) as invalid_rows
    from target()
    """
  end

  query do
    ~SQL"select order_id, status from raw.sales.orders"
  end
end
```

## Execution Model

Favn compiles check SQL into the manifest. The runner does not load the
authoring module to execute it.

At runtime Favn:

1. opens one adapter transaction and checks whether the owned target exists;
2. stages the rendered asset candidate once when a check uses `query()`;
3. runs all `:before_materialize` checks in their declaration order;
4. materializes that same staged candidate unless a check requests a no-op;
5. runs all `:after_materialize` checks in their declaration order; and
6. commits only if the write and required checks succeed.

All before checks run before all after checks even if the declarations are
interleaved. Group checks by phase in the module so their runtime order is easy
to read.

`on_false: :fail` and any SQL or result-shape error roll back the transaction.
A warning commits the write. A materialization skip commits a successful no-op
without changing the target. Successful warnings and no-ops remain successful
asset executions, so normal freshness updates and downstream gating continue.

Checked materialization requires an adapter that can execute Favn's write plan
inside the active transaction. Views are unsupported because their future rows
are not a fixed snapshot covered by that transaction.

## `check/3` Reference

```elixir
check :unique_name,
  at: :before_materialize,
  on_false: :fail,
  when: :target_exists,
  message: "Human-readable context" do
  ~SQL"select true as passed"
end
```

| Input | Required | Values and meaning |
| --- | --- | --- |
| name | yes | A unique, non-`nil` atom. One asset supports at most 50 checks. |
| `at` | yes | `:before_materialize` or `:after_materialize`. |
| `on_false` | yes | `:fail`, `:warn`, or `:skip_materialization`. |
| `when` | no | `:target_exists` skips the check during first-target bootstrap. |
| `message` | no | Static human-readable context, limited to 1,024 bytes. |

`on_false` controls only a valid result whose `passed` value is `false`:

| Value | Behavior |
| --- | --- |
| `:fail` | Stop, roll back, and fail the asset attempt. |
| `:warn` | Record a durable warning and continue in the same transaction. |
| `:skip_materialization` | Before the write, commit a successful no-op and mark later checks `:not_run`. |

`:skip_materialization` is valid only at `:before_materialize` and requires
`when: :target_exists`. This prevents a missing target from being treated as a
successful no-op during bootstrap. A missing target condition-skips that check
and allows the initial materialization to proceed.

## Read The Candidate And Target

Two runtime relation helpers are available only inside check SQL:

- `query()` is the exact staged candidate that Favn will materialize. It is the
  same snapshot for every check and the write.
- `target()` is the transaction-visible owned target. Before materialization it
  is the existing target; afterward it is the modified target.

A before check that uses `target()` must declare `when: :target_exists` so
first-target bootstrap never queries a missing relation.

Both helpers can be passed through reusable `defsql` functions, including
file-backed definitions. Favn tracks the helper through nested calls:

```elixir
defmodule MyApp.SQL.Quality do
  use Favn.SQL

  defsql has_rows(relation) do
    ~SQL"select count(*) > 0 as passed, count(*) as row_count from @relation"
  end
end

defmodule MyApp.Lakehouse.Mart.Sales.Orders do
  use MyApp.SQL.Quality
  use Favn.SQLAsset

  @materialized :table

  check :candidate_has_rows, at: :before_materialize, on_false: :fail do
    ~SQL"select * from has_rows(query())"
  end

  query do
    ~SQL"select * from raw.sales.orders"
  end
end
```

`query()` and `target()` are reserved names and are rejected in the asset's main
`query` body.

## Return Contract And Metrics

Every executed check must return exactly one row and contain exactly one column
named `passed`. That value must be a non-null native SQL Boolean.

Other columns become durable metrics. A result may contain at most 32 metric
columns. Supported scalar values are null, Boolean, number, Decimal, string,
date, time, naive datetime, and datetime values. Strings are limited to 4,096
bytes and the JSON-encoded metric map is limited to 65,536 bytes. Column names
must be unique.

These are invalid regardless of `on_false` and cause rollback:

- zero or multiple result rows;
- a missing, null, duplicated, or non-Boolean `passed` column;
- arrays, nested objects, or other unsupported metric types;
- metric count or byte limits being exceeded; and
- SQL rendering, execution, or adapter errors.

## Understand Persisted Outcomes

Run detail metadata exposes `check_results`, `quality_status`, and
`write_outcome`. Each check result follows `Favn.SQL.CheckResult`.

| Check outcome | Meaning |
| --- | --- |
| `:passed` | The check returned `passed: true`. |
| `:warned` | It returned false with `on_false: :warn`. |
| `:failed` | It returned false with `on_false: :fail`. |
| `:materialization_skipped` | It returned false and selected the successful no-op path. |
| `:condition_skipped` | `when: :target_exists` was false during bootstrap. |
| `:not_run` | Earlier work halted the transaction or selected a no-op. |
| `:errored` | SQL execution or result validation failed. |

A committed write reports `write_outcome: :written`; a successful skip reports
`:no_op`. Failed attempts report outcomes such as `:rolled_back`,
`:not_started`, or `:unknown`. Treat `:unknown` as an operator investigation
case rather than assuming the backend committed or rolled back.

## Choose A Policy Deliberately

- Use `:fail` for invariants that make the target unsafe to publish.
- Use `:warn` for important quality signals that should not block publication.
- Use `:skip_materialization` only when keeping an existing target is a valid
  successful outcome, such as an empty source extract.
- Prefer small aggregate checks. Return diagnostic counts as scalar metrics
  instead of returning invalid rows.
- Use stable, business-oriented check names and messages because they become
  durable run metadata.

For the public authoring API, read `Favn.SQLAsset` and
`Favn.SQLAsset.check/3`. For reusable SQL, read `Favn.SQL`. For the typed runtime
outcome used in run metadata, read `Favn.SQL.CheckResult`.
