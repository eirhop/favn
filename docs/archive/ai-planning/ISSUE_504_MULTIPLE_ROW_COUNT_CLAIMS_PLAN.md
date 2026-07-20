# Issue 504: Multiple Ordered Row-Count Claims

Status: temporary implementation plan for issue #504.

## Objective

Allow one SQL output contract to declare multiple independent `row_count`
claims. The claims must retain authored order, compile into ordinary
transactional checks, use one canonical persisted shape, and remain visible
through the orchestrator-owned assurance read model.

The canonical authoring form is repeated singular declarations:

```elixir
contract do
  row_count equals: param(:expected_row_count),
    on_violation: :fail

  row_count min: 1,
    when: :target_exists,
    on_violation: :skip_materialization
end
```

## Architecture

### Ownership

- `favn_authoring` collects repeated declarations without interpreting runtime
  outcomes.
- `favn_core` owns the ordered `%Favn.SQL.Contract{row_counts: [...]}` model,
  validation, generated check identities, semantic diffs, serialization, and
  persisted-payload rehydration.
- `favn_runner` continues to execute the manifest check list in order. Its
  existing reduce-while behavior already prevents a later no-op from hiding an
  earlier failure.
- `favn_orchestrator` projects every claim and check result into one bounded
  operator DTO.
- `favn_view` renders only that DTO and does not inspect runner, storage, or
  compiler internals.

### Canonical model and limits

`Favn.SQL.Contract` stores an ordered `row_counts` list. Each element remains a
validated `%Favn.SQL.Contract.RowCount{}` with exactly one exact constraint or
one bounded constraint plus its own condition and violation policy.

One contract supports at most 16 row-count claims. Together with the grouped
required-column and unique-key checks, one contract therefore emits at most 18
generated checks. The existing separate limit of 50 authored checks remains
unchanged.

### Generated identities

The existing semantic ID remains unchanged for the first occurrence, for
example `row_count.min.1`. A repeated semantic ID receives a deterministic
declaration-order suffix such as `row_count.min.1.occurrence.2`. This preserves
the concise semantic ID for the first occurrence while guaranteeing unique
generated check names and durable result identities.

### Persistence contract

The in-memory and persisted model is always the canonical ordered list:

- `row_counts: [object, ...]`.

The manifest schema, runner contract, and execution-package schema advance to
9, 9, and 2 respectively. Readers accept only those current versions; stale
singular fields and older versions are rejected rather than maintained through
compatibility branches.

### Ordering semantics

Generated row-count checks appear in declaration order before the existing
grouped required-column and unique-key checks. Runtime parameter requirements
are collected from all exact parameter claims and deduplicated by name.

Runner behavior remains:

1. a failed `:fail` claim halts and rolls back;
2. a violated `:skip_materialization` claim halts with a successful no-op;
3. `when: :target_exists` condition-skips on bootstrap; and
4. later claims never replace an earlier terminal outcome.

## Implementation steps

1. Change the authoring accumulator and parser to append repeated row-count
   declarations.
2. Replace the core singular field with the bounded ordered list; update
   validation, parameter requirements, generated specs, identities, and diffs.
3. Raise the generated-contract-check budget to match the explicit claim cap.
4. Make `row_counts` the only serialized and rehydrated shape and advance the
   manifest, runner-contract, and execution-package versions.
5. Project complete ordered claim details through the orchestrator catalogue,
   including literal/parameter equality, bounds, condition, and policy.
6. Render all claims in the asset assurance component and update its Storybook
   fixture.
7. Update public moduledocs, AI breadcrumbs, HexDocs guides, README, feature,
   roadmap, and structure documentation.
8. Add focused authoring, core manifest/diff, runner ordering, orchestrator DTO,
   and LiveView tests.

## Focused verification

Do not run the full umbrella suite for this change. Run only:

```bash
mix format
MIX_ENV=test mix do --app favn_authoring cmd mix test test/sql_check_dsl_test.exs
MIX_ENV=test mix do --app favn_core cmd mix test test/sql/contract_test.exs test/manifest/serializer_test.exs test/manifest/compatibility_test.exs test/manifest/version_test.exs test/manifest/execution_package_test.exs
MIX_ENV=test mix do --app favn_runner cmd mix test test/execution/sql_asset_test.exs
MIX_ENV=test mix do --app favn_orchestrator cmd mix test test/operator/catalogue/assurance_test.exs
MIX_ENV=test mix do --app favn cmd mix test test/manifest_generator_test.exs
MIX_ENV=test mix do --app favn_view cmd mix test test/favn_view/components/asset_detail_page_test.exs
mix compile --warnings-as-errors
```
