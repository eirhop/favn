# Favn.SQL.Adapter Architecture (v0.4 Step 2)

## 1. Goal

Define and implement a runtime-only internal SQL backend contract for Favn v0.4.

This enables:

- adapter-backed SQL execution starting from `%Favn.Connection.Resolved{}`
- normalized capability, relation, column, result, error, and write-plan models
- a central runtime facade (`Favn.SQL`) used by runtime execution

## 2. Design principles

- Narrow public contract
- Struct-first internal APIs
- Clear separation: `Favn.Connection` resolves config, SQL adapter executes backend behavior
- Runtime-only SQL sessions (no compile/discovery coupling)
- Adapter-owned SQL rendering for fallback paths
- Additive evolution for future backends

## 3. Recommended architecture

```text
lib/
  favn/
    sql.ex
    sql/
      adapter.ex
      capabilities.ex
      relation_ref.ex
      relation.ex
      column.ex
      result.ex
      error.ex
      write_plan.ex
      session.ex
      adapter/
        duckdb.ex   # upcoming
```

## 4. Behaviour design

`Favn.SQL.Adapter` uses a hybrid model.

Required callbacks:

- `connect/2`
- `disconnect/2`
- `capabilities/2` (from `%Resolved{}`, not session connection)
- `execute/3`
- `query/3`
- `introspection_query/3` (adapter renders fallback introspection SQL)
- `materialization_statements/3` (adapter renders fallback write SQL)

Optional callbacks:

- `ping/2`
- `schema_exists?/3`
- `relation/3`
- `list_schemas/2`
- `list_relations/3`
- `columns/3`
- `transaction/3`
- `materialize/3`

## 5. Struct design

- `%Favn.SQL.Capabilities{}`: typed backend support matrix
- `%Favn.SQL.RelationRef{}`: requested relation identity (catalog/schema/name)
- `%Favn.SQL.Relation{}`: discovered relation metadata
- `%Favn.SQL.Column{}`: discovered column metadata
- `%Favn.SQL.Result{}`: normalized execution/query/materialization output
- `%Favn.SQL.Error{}`: normalized error payload
- `%Favn.SQL.WritePlan{}`: normalized materialization input
- `%Favn.SQL.Session{}`: internal runtime session record

## 6. Error model

`%Favn.SQL.Error{}` types:

- `:invalid_config`
- `:authentication_error`
- `:connection_error`
- `:execution_error`
- `:unsupported_capability`
- `:introspection_mismatch`
- `:missing_relation`

Retry semantics are carried by `retryable?`.

## 7. Capability model

`%Favn.SQL.Capabilities{}` includes:

- relation type support
- replace semantics for views/tables
- transaction support
- merge support
- materialized view support
- comments support
- metadata timestamps support
- query tracking support
- backend extensions

Support values use `:supported | :unsupported | :emulated`.

## 8. Introspection model

Minimum introspection runtime APIs in `Favn.SQL`:

- `schema_exists?/3`
- `get_relation/3`
- `list_schemas/2`
- `list_relations/3`
- `columns/3`

Strategy:

- use optional adapter callbacks when implemented
- otherwise use adapter-rendered SQL via required `introspection_query/3`
- normalize all returns in facade (`Favn.SQL`)

## 9. Execution model

Core execution APIs:

- `execute/3`
- `query/3`

Facade invariants:

- all adapter-facing paths normalize return contracts
- all `%Favn.SQL.Error{}` values are decorated with connection name when missing
- unexpected values are converted into normalized `%Favn.SQL.Error{}`

## 10. Materialization model

Materializations:

- `:view`
- `:table`
- `:incremental` (`:append | :replace | :delete_insert | :merge`)

Strategy:

- use optional optimized `materialize/3` when available
- otherwise call adapter-required `materialization_statements/3` and execute via facade
- fallback SQL generation remains adapter-owned

## 11. `Favn.SQL` facade

`Favn.SQL` owns:

- resolving connection entries from registry
- adapter lifecycle orchestration
- uniform contract normalization across all adapter paths
- fallback execution sequencing
- best-effort disconnect semantics

`Favn.SQL` does not own SQL dialect rendering.

## 12. Execution flow

1. compiler emits canonical SQL metadata/write intent (no sessions)
2. runtime resolves connection name
3. `Favn.SQL.resolve_connection/1` returns `%Resolved{}`
4. `Favn.SQL.connect/2` calls adapter capabilities + connect
5. runtime executes introspection/write operations through `Favn.SQL`
6. `Favn.SQL` chooses optimized callback or adapter-rendered fallback path
7. results/errors are normalized
8. `Favn.SQL.disconnect/2` performs best-effort teardown

## 13. Conformance test contract

Each SQL adapter must prove:

- lifecycle and capabilities callbacks
- execution and query callback behavior
- fallback rendering callbacks (`introspection_query/3`, `materialization_statements/3`)
- normalized error mapping
- optional callback interoperability

## 14. DuckDB fit check

DuckDB can implement required callbacks quickly and later add optional optimized callbacks.

No DuckDB-specific SQL is required in facade layer; rendering stays in adapter.

## 15. Phased implementation plan

1. Core contract + structs + facade normalization
2. DuckDB adapter required callbacks
3. Optional optimized callbacks and materialization improvements
4. SQL asset runtime integration on shared planner/runtime model

## 16. Tradeoffs and rejected alternatives

Rejected:

- oversized required callback surface
- single opaque callback (`run/2` style)
- macro-heavy adapter DSL in v0.4
- facade-owned dialect rendering

## 17. Final recommendation

Use the implemented hybrid contract:

- runtime-only `Favn.SQL` facade
- required adapter rendering callbacks for fallback SQL
- strict struct-driven normalization in facade
- additive optional callbacks for optimization

This keeps architecture clean, practical, and ready for DuckDB and later adapters.
