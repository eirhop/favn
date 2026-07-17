# Transactional SQL Asset Checks

Reader: authors who need to validate a table or incremental SQL asset before
Favn commits its materialization.

Documentation type: how-to and reference guide.

Start with `Favn.SQLAsset` for the complete SQL asset DSL. This guide focuses on
the `check/3` declarations available to table and incremental assets.

## Decide Whether A Check Is The Right Tool

Use a transactional SQL check when all of these are true:

- the rule can be expressed as a read-only SQL aggregate;
- it validates the exact candidate or target owned by this SQL asset;
- the result should participate in the same commit or rollback decision as the
  materialization; and
- a bounded set of scalar metrics is enough to explain the result.

Good examples include required keys, duplicate counts, accepted enum values,
row-count thresholds, and candidate-versus-existing-target drift.

Choose the owning tool for adjacent work:

- keep transformation logic in the asset's main `query`;
- use an upstream `Favn.Asset` or source client for external API calls, file
  checks, and other imperative work;
- declare `depends` and use
  freshness policy instead;
- keep mutating repair SQL and side effects at an explicit execution edge;
- return aggregate counts and inspect invalid rows or large samples separately;
  and
- use table or incremental materialization with an adapter that runs Favn's
  write plan inside an active transaction.

Treat checks as publication gates and quality annotations.

## Define Checks

Declare up to 50 authored checks in the SQL asset module. Give every check a
unique atom name and choose when it runs and what a false result means. Output
contracts have a separate budget of at most three grouped generated checks.

```elixir
defmodule MyApp.Assets.NormalizedRecords do
  @moduledoc "Normalized records with transactional quality checks."

  use Favn.SQLAsset

  relation connection: :main, catalog: "normalized", schema: "default"
  materialized :table

  check :candidate_has_valid_keys,
    at: :before_materialize,
    on_violation: :fail,
    message: "Every candidate row must have a record id" do
    ~SQL"""
    select
      count(*) filter (where record_id is null) = 0 as passed,
      count(*) filter (where record_id is null) as invalid_rows
    from query()
    """
  end

  check :keep_existing_target_when_empty,
    at: :before_materialize,
    when: :target_exists,
    on_violation: :skip_materialization,
    message: "The candidate was empty; the existing target was kept" do
    ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
  end

  check :values_are_supported,
    at: :after_materialize,
    on_violation: :warn,
    message: "The target contains unsupported states" do
    ~SQL"""
    select
      count(*) filter (where state not in ('ready', 'pending')) = 0 as passed,
      count(*) filter (where state not in ('ready', 'pending')) as invalid_rows
    from target()
    """
  end

  query do
    ~SQL"select source_id as record_id, state from source_records"
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

`on_violation: :fail` and any SQL or result-shape error roll back the transaction.
A warning commits the write. A materialization skip commits a successful no-op
without changing the target and records `quality_status: :warning`. Successful
warnings and no-ops remain successful asset executions, so normal freshness
updates and downstream gating continue.

Checked materialization requires an adapter that can execute Favn's write plan
inside the active transaction and a table or incremental candidate whose rows
are covered by that transaction.

### Choose The Phase

| Need | Phase | Relation | Advice |
| --- | --- | --- | --- |
| Validate rows about to be written | `:before_materialize` | `query()` | Prefer this when the candidate alone answers the question; failure avoids doing write work. |
| Compare the candidate with the current target | `:before_materialize` | `query()` and `target()` | Add `when: :target_exists`; decide how bootstrap should behave. |
| Validate the exact transaction-visible published target | `:after_materialize` | `target()` | Use when materialization semantics can change what should be checked. A failure rolls the write back. |

Do not use an after check when an equivalent candidate check is sufficient. An
after failure is still atomic, but the backend performs the write before rolling
it back.

## `check/3` Reference

```elixir
check :unique_name,
  at: :before_materialize,
  on_violation: :fail,
  when: :target_exists,
  message: "Human-readable context" do
  ~SQL"select true as passed"
end
```

| Input | Required | Values and meaning |
| --- | --- | --- |
| name | yes | A unique, non-`nil` atom. One asset supports at most 50 authored checks; an output contract adds at most three grouped generated checks. |
| `at` | yes | `:before_materialize` or `:after_materialize`. |
| `on_violation` | yes | `:fail`, `:warn`, or `:skip_materialization`. |
| `when` | no | `:target_exists` skips the check during first-target bootstrap. |
| `message` | no | Static human-readable context, limited to 1,024 bytes. |

`on_violation` controls only a valid result whose `passed` value is `false`:

| Value | Behavior |
| --- | --- |
| `:fail` | Stop, roll back, and fail the asset attempt. |
| `:warn` | Record a durable warning and continue in the same transaction. |
| `:skip_materialization` | Before the write, commit a successful warning/no-op and mark later checks `:not_run`. |

`:skip_materialization` is valid only at `:before_materialize` and requires
`when: :target_exists`. This prevents a missing target from being treated as a
successful no-op during bootstrap. A missing target condition-skips that check
and allows the initial materialization to proceed.

### Choose The Violation Policy

| Policy | Use it when | Do not use it when |
| --- | --- | --- |
| `:fail` | Publishing the candidate would violate a required invariant. | The condition is informational or an existing target is intentionally acceptable. |
| `:warn` | The target remains safe to publish, but operators should see durable quality degradation. | Downstream consumers would receive unsafe or misleading data. |
| `:skip_materialization` | Keeping an existing target is a valid successful outcome for an empty candidate. | Stale data is unsafe, the target must be refreshed, or the false result represents an actual failure. |

`on_violation` never handles SQL, adapter, or result-contract errors. Those always
fail and roll back. In particular, `:warn` and `:skip_materialization` must not
be used as attempts to hide connectivity or query failures.

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

defmodule MyApp.Assets.NormalizedRecords do
  use MyApp.SQL.Quality
  use Favn.SQLAsset

  relation connection: :main, catalog: "normalized", schema: "default"
  materialized :table

  check :candidate_has_rows, at: :before_materialize, on_violation: :fail do
    ~SQL"select * from has_rows(query())"
  end

  query do
    ~SQL"select source_id as record_id, state from source_records"
  end
end
```

`query()` and `target()` are reserved names and are rejected in the asset's main
`query` body.

## Practical Patterns

The following checks are independent patterns. Copy the ones that match the
asset's publication policy rather than adding every pattern to every asset.

### Block An Invalid Candidate

Use a before fail check for a required invariant that can be evaluated from the
candidate alone:

```elixir
check :unique_record_ids,
  at: :before_materialize,
  on_violation: :fail,
  message: "Record ids must be present and unique" do
  ~SQL"""
  select
    count(*) = count(distinct record_id) as passed,
    count(*) - count(distinct record_id) as invalid_rows
  from query()
  """
end
```

This catches both null and duplicate IDs. Because it runs before the write,
failure avoids materialization work and rolls back the transaction.

### Publish With A Durable Warning

Use an after warning when the exact published target should be visible to
downstream assets but operators still need a quality signal:

```elixir
check :values_are_supported,
  at: :after_materialize,
  on_violation: :warn,
  message: "The target contains unsupported states" do
  ~SQL"""
  select
    count(*) filter (where state not in ('ready', 'pending')) = 0 as passed,
    count(*) filter (where state not in ('ready', 'pending')) as invalid_rows
  from target()
  """
end
```

On false, the asset succeeds with `quality_status: :warning` and
`write_outcome: :written`. Do not choose `:warn` if unknown statuses make the
target unsafe for consumers.

### Keep An Existing Target When The Candidate Is Empty

Use a before skip only when retaining the existing target is a legitimate
success policy:

```elixir
check :keep_existing_target_when_empty,
  at: :before_materialize,
  when: :target_exists,
  on_violation: :skip_materialization,
  message: "The candidate was empty; the existing target was kept" do
  ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
end
```

With an existing target and an empty candidate, the asset succeeds with
`quality_status: :warning` and `write_outcome: :no_op`, the target is unchanged,
and later checks are `:not_run`. During first-target bootstrap, the condition produces
`:condition_skipped` and Favn writes the candidate. If an empty first target is
invalid, add a separate unguarded `:fail` check or choose a different bootstrap
policy.

Do not use this pattern when keeping stale data would be worse than failing the
run.

### Compare Candidate And Existing Target

Use both runtime relations when a large change relative to the current target
should block publication:

```elixir
check :row_count_did_not_collapse,
  at: :before_materialize,
  when: :target_exists,
  on_violation: :fail,
  message: "Candidate row count fell by more than 20 percent" do
  ~SQL"""
  select
    candidate_rows >= target_rows * 0.8 as passed,
    candidate_rows,
    target_rows
  from (select count(*) as candidate_rows from query()) candidate,
       (select count(*) as target_rows from target()) existing
  """
end
```

The guard lets first-target bootstrap proceed because there is no baseline yet.
Choose the threshold from the asset's publication contract; do not copy a generic
percentage without understanding expected volume changes.

## Return Contract And Metrics

Every executed check must return exactly one row and contain exactly one column
named `passed`. That value must be a non-null native SQL Boolean.

Other columns become durable metrics. A result may contain at most 32 metric
columns. Supported scalar values are null, Boolean, number, Decimal, string,
date, time, naive datetime, and datetime values. Strings are limited to 4,096
bytes and the JSON-encoded metric map is limited to 65,536 bytes. Column names
must be unique.

These are invalid regardless of `on_violation` and cause rollback:

- zero or multiple result rows;
- a missing, null, duplicated, or non-Boolean `passed` column;
- arrays, nested objects, or other unsupported metric types;
- metric count or byte limits being exceeded; and
- SQL rendering, execution, or adapter errors.

## Understand Persisted Outcomes

Run detail metadata exposes `check_results`, `quality_status`, and
`write_outcome`. Each check result follows `Favn.SQL.CheckResult`. Its `origin`
is `:contract` for a generated contract claim and `:authored` for a custom
check; contract results also expose a stable `claim_id`.

| Check outcome | Meaning |
| --- | --- |
| `:passed` | The check returned `passed: true`. |
| `:warned` | It returned false with `on_violation: :warn`. |
| `:failed` | It returned false with `on_violation: :fail`. |
| `:materialization_skipped` | It returned false and selected the successful no-op path. |
| `:condition_skipped` | `when: :target_exists` was false during bootstrap. |
| `:not_run` | Earlier work halted the transaction or selected a no-op. |
| `:errored` | SQL execution or result validation failed. |

A committed write reports `write_outcome: :written`; a successful skip reports
`:no_op`. Failed attempts report outcomes such as `:rolled_back`,
`:not_started`, or `:unknown`. Treat `:unknown` as an operator investigation
case rather than assuming the backend committed or rolled back.

## Authoring Advice

- Use `:fail` for invariants that make the target unsafe to publish.
- Use `:warn` for important quality signals that should not block publication.
- Use `:skip_materialization` only when keeping an existing target is a valid
  successful outcome for an empty candidate.
- Prefer small aggregate checks. Return diagnostic counts as scalar metrics
  instead of returning invalid rows.
- Use stable, descriptive check names and messages because they become
  durable run metadata.
- Keep check SQL deterministic for the duration of the transaction. Prefer the
  staged `query()` and transaction-visible `target()` over re-reading mutable
  upstream relations.
- Keep related fail checks before warnings when an early invariant can make
  later results irrelevant.
- Treat checks as part of the asset contract. Review policy changes as carefully
  as changes to the materialization query.

For the public authoring API, read `Favn.SQLAsset` and
`Favn.SQLAsset.check/3`. For reusable SQL, read `Favn.SQL`. For the typed runtime
outcome used in run metadata, read `Favn.SQL.CheckResult`.
