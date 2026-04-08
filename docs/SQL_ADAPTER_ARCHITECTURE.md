# Favn.SQL.Adapter Architecture (v0.4 Step 2)

## 1. Goal

`Favn.SQL.Adapter` enables the first end-to-end SQL asset runtime path in v0.4.0 by introducing a stable internal backend contract that starts from `%Favn.Connection.Resolved{}` and drives SQL lifecycle, introspection, execution, and materialization.

Why now:

- `Favn.Connection` is implemented and gives us validated runtime connection payloads.
- v0.4 requires a concrete SQL execution foundation (DuckDB first) without coupling runtime/planner/compiler directly to backend-specific modules.
- SQL assets, materialization planning, and window-aware incremental execution all require normalized SQL primitives that do not exist yet.

Architectural fit:

- `Favn.Connection` remains the owner of connection definition, runtime merge, validation, and registry lookup.
- `Favn.SQL.Adapter` starts at runtime with `%Favn.Connection.Resolved{}` and returns normalized SQL structs/errors.
- `Favn.SQL` acts as an internal service/facade used by planner/runtime/compiler seams so the rest of Favn stays backend-agnostic.

---

## 2. Design principles

1. **Narrow, stable contract**
   - Require only the smallest callback set needed for v0.4.
   - Keep advanced paths optional and capability-gated.

2. **Explicit capability model**
   - No adapter-name conditionals in runtime/planner.
   - Feature gates come from `%Favn.SQL.Capabilities{}`.

3. **Additive evolution**
   - v0.4 contract should accept optional callbacks/fields for later backends.
   - Prefer optional callbacks + fallback orchestration in `Favn.SQL`.

4. **Separation from `Favn.Connection`**
   - `Favn.Connection` resolves and validates config.
   - SQL adapter concerns begin only after `%Favn.Connection.Resolved{}` exists.

5. **Reuse shared runtime/window model**
   - SQL execution integrates with existing run/planner/window semantics.
   - Adapter layer should not invent parallel orchestration concepts.

6. **Avoid DuckDB-shaped architecture**
   - DuckDB is first implementation, not the architecture template.
   - Internal structs represent Favn concepts, not backend-specific nomenclature.

---

## 3. Recommended architecture

### Module boundary (minimal, implementation-ready)

```text
lib/
  favn/
    sql.ex
    sql/
      adapter.ex
      capabilities.ex
      relation.ex
      relation_ref.ex
      column.ex
      result.ex
      error.ex
      write_plan.ex
      options.ex
      materialization.ex
      fallback.ex
      adapter/
        duckdb.ex
```

### Roles

- `Favn.SQL.Adapter`
  - Behaviour defining backend contract.
  - Small required core + optional optimized callbacks.

- `Favn.SQL`
  - Internal facade/service boundary used by runtime/planner/compiler.
  - Resolves adapter from `%Connection.Resolved{}`.
  - Handles lifecycle, capability checks, fallback materialization, and normalized errors.

- `Favn.SQL.Capabilities`
  - Typed capability descriptor returned by adapter.

- `Favn.SQL.Relation`
  - Canonical relation identity (catalog/schema/name/type).

- `Favn.SQL.Column`
  - Normalized column metadata used by introspection/planning.

- `Favn.SQL.Result`
  - Canonical execution/query/materialization outcome shape.

- `Favn.SQL.Error`
  - Normalized adapter/runtime SQL error model with typed categories.

- `Favn.SQL.WritePlan`
  - Canonical materialization instruction struct.

- `Favn.SQL.Adapter.DuckDB`
  - Concrete v0.4 adapter implementation.

### Additional minimal modules

- `Favn.SQL.Options`
  - Typed option normalization helpers for `connect/query/execute` calls.
- `Favn.SQL.Materialization`
  - Build `%WritePlan{}` from compiled SQL asset metadata.
- `Favn.SQL.Fallback`
  - Canonical SQL fallback implementation when optional `materialize/3` is absent.

---

## 4. Behaviour design

`Favn.SQL.Adapter` uses a **hybrid behaviour**: minimal required callbacks + optional optimized callbacks.

### Suggested behaviour type signatures

```elixir
defmodule Favn.SQL.Adapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Relation, Column, Result, Error, WritePlan}

  @type conn :: term()
  @type adapter_opts :: keyword()
  @type query_opts :: keyword()

  # Lifecycle (required)
  @callback connect(Resolved.t(), adapter_opts()) :: {:ok, conn()} | {:error, Error.t()}
  @callback disconnect(conn(), adapter_opts()) :: :ok | {:error, Error.t()}

  # Capability discovery (required)
  @callback capabilities(conn(), adapter_opts()) :: {:ok, Capabilities.t()} | {:error, Error.t()}

  # Execution (required core)
  @callback execute(conn(), iodata(), query_opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  @callback query(conn(), iodata(), query_opts()) :: {:ok, Result.t()} | {:error, Error.t()}

  # Optional optimized callbacks
  @callback ping(conn(), adapter_opts()) :: :ok | {:error, Error.t()}
  @callback schema_exists?(conn(), binary(), adapter_opts()) :: {:ok, boolean()} | {:error, Error.t()}
  @callback relation(conn(), Favn.SQL.RelationRef.t(), adapter_opts()) :: {:ok, Relation.t() | nil} | {:error, Error.t()}
  @callback list_schemas(conn(), adapter_opts()) :: {:ok, [binary()]} | {:error, Error.t()}
  @callback list_relations(conn(), binary() | nil, adapter_opts()) :: {:ok, [Relation.t()]} | {:error, Error.t()}
  @callback columns(conn(), Favn.SQL.RelationRef.t(), adapter_opts()) :: {:ok, [Column.t()]} | {:error, Error.t()}

  @callback transaction(conn(), (conn() -> {:ok, term()} | {:error, Error.t()}), adapter_opts()) ::
              {:ok, term()} | {:error, Error.t()}

  @callback materialize(conn(), WritePlan.t(), adapter_opts()) :: {:ok, Result.t()} | {:error, Error.t()}

  @optional_callbacks [
    ping: 2,
    schema_exists?: 3,
    relation: 3,
    list_schemas: 2,
    list_relations: 3,
    columns: 3,
    transaction: 3,
    materialize: 3
  ]
end
```

### Responsibility split

- **Lifecycle**: `connect/2`, `disconnect/2`, optional `ping/2`.
- **Capabilities**: `capabilities/2` must always be available.
- **Introspection**: optional optimized callbacks; facade can fallback via information schema queries.
- **Execution**: `execute/3` and `query/3` required.
- **Materialization**: optional `materialize/3`; facade must provide fallback.

Why this belongs in behaviour:

- Required callbacks define non-negotiable backend boundary for v0.4 runtime viability.
- Optional callbacks allow backend optimization without forcing every adapter to implement an expansive surface before it can work.

---

## 5. Struct design

### `Favn.SQL.Capabilities`

Why: centralized, typed feature contract for planner/runtime/facade decisions.

```elixir
@type support :: :supported | :unsupported | :emulated

defstruct [
  relation_types: [:table, :view],
  replace_view: :unsupported,
  replace_table: :unsupported,
  transactions: :unsupported,
  merge: :unsupported,
  materialized_views: :unsupported,
  relation_comments: :unsupported,
  column_comments: :unsupported,
  metadata_timestamps: :unsupported,
  query_tracking: :unsupported,
  extensions: %{}
]
```

Normalization:

- Favn-level: fixed capability keys with enum values.
- Backend-specific: extra flags in `extensions`.

### `Favn.SQL.Relation`

Why: canonical relation identity and type used in introspection/materialization.

```elixir
defstruct [
  :catalog,
  :schema,
  :name,
  :type,             # :table | :view | :materialized_view | :temporary | :unknown
  :identifier,       # rendered/quoted backend identifier
  :exists?,
  metadata: %{}
]

```

### `Favn.SQL.RelationRef`

Why: keeps adapter/facade introspection APIs struct-driven for relation lookup inputs.

```elixir
defstruct [:catalog, :schema, :name]
```

### `Favn.SQL.Column`

Why: predictable column metadata shape for validation/planning.

```elixir
defstruct [
  :name,
  :position,
  :data_type,
  :nullable?,
  :default,
  :comment,
  metadata: %{}
]
```

### `Favn.SQL.Result`

Why: normalize outcomes across execute/query/materialize.

```elixir
defstruct [
  :kind,             # :execute | :query | :materialize
  :command,          # backend command tag, optional
  :rows_affected,
  rows: [],
  columns: [],
  notices: [],
  metadata: %{}
]
```

### `Favn.SQL.Error`

Why: typed error contract runtime/planner can reason about.

```elixir
defstruct [
  :type,
  :message,
  :retryable?,
  :adapter,
  :connection,
  :operation,
  :sqlstate,
  details: %{},
  cause: nil
]
```

### `Favn.SQL.WritePlan`

Why: normalized materialization input independent of backend SQL syntax.

```elixir
defstruct [
  :materialization,  # :view | :table | :incremental
  :strategy,         # nil | :append | :replace | :delete_insert | :merge
  :target,           # %Relation{}
  :select_sql,
  :replace?,
  :if_not_exists?,
  :transactional?,
  :window,
  :unique_key,
  :incremental_predicate_sql,
  :pre_statements,
  :post_statements,
  options: %{},
  metadata: %{}
]
```

Normalization ownership:

- Favn-level: materialization kind/strategy, target identity, base SQL payload, deterministic flags.
- Backend-specific: adapter tuning and hints in `options`/`metadata`.

---

## 6. Error model

### Error typing

`Favn.SQL.Error.type`:

- `:invalid_config`
- `:authentication_error`
- `:connection_error`
- `:execution_retryable`
- `:execution_non_retryable`
- `:unsupported_capability`
- `:introspection_mismatch`
- `:missing_relation`

### Mapping rules

Adapters map backend errors into `Favn.SQL.Error` as close to source as possible:

- config parsing/required runtime param missing -> `:invalid_config`
- auth failure codes/messages -> `:authentication_error`
- network/session initialization failures -> `:connection_error`
- lock timeout/transient busy/deadlock -> `:execution_retryable`
- syntax/semantic SQL errors -> `:execution_non_retryable`
- explicit unsupported feature path -> `:unsupported_capability`
- relation metadata mismatch (expected type mismatch, malformed introspection row) -> `:introspection_mismatch`
- relation not found where required -> `:missing_relation`

### Runtime/planner reliability contract

Runtime/planner should only rely on:

- `type`
- `retryable?`
- `operation`
- `connection`
- `message`

Backend detail remains in:

- `sqlstate`
- `details`
- `cause`

---

## 7. Capability model

Use **typed enum values**, not booleans only.

Why:

- Booleans cannot represent "emulated by facade fallback" vs "natively supported".
- Enum `:supported | :unsupported | :emulated` keeps v0.4 simple while enabling future precision.

Fields to include in v0.4 `Capabilities`:

- relation support (`relation_types` includes `:view`, `:table`)
- `replace_view`
- `replace_table`
- `transactions`
- `merge`
- `materialized_views`
- `relation_comments`
- `column_comments`
- `metadata_timestamps`
- `query_tracking`

This shape is additive: new capability keys can be appended without breaking existing adapters.

---

## 8. Introspection model

v0.4 minimum introspection needs:

- schema exists?
- relation exists?
- relation type
- list schemas
- list relations
- fetch columns

Recommendation:

- Keep optional fine-grained callbacks in behaviour.
- Consolidate orchestration in `Favn.SQL` so runtime/planner calls only facade methods:
  - `Favn.SQL.schema_exists?/3`
  - `Favn.SQL.get_relation/3`
  - `Favn.SQL.list_schemas/2`
  - `Favn.SQL.list_relations/3`
  - `Favn.SQL.columns/3`

Facade fallback strategy if optional callback missing:

- Use `query/3` against canonical catalog/information schema SQL for the backend.
- Normalize rows into `%Relation{}` / `%Column{}`.
- Return normalized `%Favn.SQL.Error{}` on mismatch.

---

## 9. Execution model

Lower-level contract:

- `execute/3` for statements where rows are not required.
- `query/3` for row-returning statements.

Result handling:

- Both return `%Favn.SQL.Result{}`.
- `kind` distinguishes operation; runtime need not parse backend driver payloads.

Statement sequencing:

- Sequencing policy lives in `Favn.SQL` (e.g., `pre_statements`, main op, `post_statements`).
- Adapter only executes single statement per call in required core; batching can be added later.

Transactions:

- Optional `transaction/3` callback for optimized native transaction boundary.
- If missing and capability is unsupported, facade executes non-transactionally.
- If `WritePlan.transactional?` is true and adapter cannot provide safe transaction semantics, facade returns `:unsupported_capability`.

Query options:

- options normalized by `Favn.SQL.Options` (`timeout`, `params`, `tag`, `tracking`).
- unknown options filtered/validated by facade before adapter call.

---

## 10. Materialization model

### Materialization kinds

- `:view`
- `:table`
- `:incremental`

### Incremental strategies

- `:append`
- `:replace`
- `:delete_insert`
- `:merge`

### Ownership boundary

**Favn core owns:**

- compile-time canonical materialization metadata
- `%WritePlan{}` construction
- capability gating and strategy validation
- fallback materialization orchestration

**Adapter owns:**

- SQL dialect-specific rendering/execution details
- optimized `materialize/3` when implemented
- backend-specific safety/performance semantics

### Fallback semantics when `materialize/3` omitted

`Favn.SQL` executes canonical flow using required `execute/3` + `query/3` and capability checks:

- `:view` -> create/replace view SQL path
- `:table` -> create table as select / replace strategy path
- `:incremental/:append` -> insert into target select
- `:incremental/:replace` -> replace full target path
- `:incremental/:delete_insert` -> delete predicate window then insert
- `:incremental/:merge` -> only if `capabilities.merge == :supported`; else `:unsupported_capability`

---

## 11. `Favn.SQL` facade

Why it must exist:

- prevents widespread runtime/planner coupling to adapter modules
- centralizes lifecycle + fallback + error normalization
- provides single seam for future observability/query tracking

### Internal facade APIs (proposed)

```elixir
resolve_connection(name :: atom()) :: {:ok, Favn.Connection.Resolved.t()} | {:error, Favn.SQL.Error.t()}
connect(resolved, opts \\ []) :: {:ok, session} | {:error, Favn.SQL.Error.t()}
disconnect(session) :: :ok | {:error, Favn.SQL.Error.t()}

capabilities(session) :: {:ok, Favn.SQL.Capabilities.t()} | {:error, Favn.SQL.Error.t()}

execute(session, sql, opts \\ []) :: {:ok, Favn.SQL.Result.t()} | {:error, Favn.SQL.Error.t()}
query(session, sql, opts \\ []) :: {:ok, Favn.SQL.Result.t()} | {:error, Favn.SQL.Error.t()}

schema_exists?(session, schema, opts \\ []) :: {:ok, boolean()} | {:error, Favn.SQL.Error.t()}
get_relation(session, ref, opts \\ []) :: {:ok, Favn.SQL.Relation.t() | nil} | {:error, Favn.SQL.Error.t()}
list_schemas(session, opts \\ []) :: {:ok, [binary()]} | {:error, Favn.SQL.Error.t()}
list_relations(session, schema \\ nil, opts \\ []) :: {:ok, [Favn.SQL.Relation.t()]} | {:error, Favn.SQL.Error.t()}
columns(session, ref, opts \\ []) :: {:ok, [Favn.SQL.Column.t()]} | {:error, Favn.SQL.Error.t()}

materialize(session, %Favn.SQL.WritePlan{}, opts \\ []) :: {:ok, Favn.SQL.Result.t()} | {:error, Favn.SQL.Error.t()}
```

`session` should be an internal struct (e.g. `%Favn.SQL.Session{resolved, adapter, conn, capabilities}`) owned by facade.

---

## 12. Execution flow (future SQL asset run)

1. SQL asset compiles to canonical asset metadata (including materialization + connection name).
2. Runtime resolves connection name from asset metadata.
3. `Favn.SQL.resolve_connection/1` fetches `%Favn.Connection.Resolved{}`.
4. `Favn.SQL.connect/2` resolves adapter module from `resolved.adapter` and connects.
5. `Favn.SQL.capabilities/1` loads capability snapshot.
6. `Favn.SQL.Materialization` builds `%WritePlan{}` from compiled metadata + runtime window context.
7. `Favn.SQL.materialize/3`:
   - uses adapter `materialize/3` if implemented, else fallback strategy executor.
8. `Favn.SQL` returns normalized `%Result{}` or `%Error{}`.
9. Runtime consumes normalized result/error for retry/failure semantics.
10. `Favn.SQL.disconnect/1` closes adapter session.

This keeps planner/runtime logic backend-agnostic and deterministic.

---

## 13. Conformance test contract

Each SQL adapter must pass a shared adapter conformance suite.

### Required groups

1. **Lifecycle**
   - connect success/failure normalization
   - disconnect idempotency behavior
   - optional ping behavior if implemented

2. **Capabilities**
   - returns `%Capabilities{}` with all required keys
   - no missing capability fields

3. **Introspection**
   - schema existence checks
   - relation lookup and type normalization
   - list schemas/relations/columns shape guarantees

4. **Execution**
   - execute returns `%Result{kind: :execute}`
   - query returns `%Result{kind: :query, rows: ...}`
   - error normalization for syntax/connection failures

5. **Materialization**
   - view/table/incremental strategy behavior
   - unsupported strategy returns `:unsupported_capability`

6. **Error normalization**
   - map known backend failures to required `Error.type` values
   - retryable classification correctness for transient failures

7. **Fallback compatibility**
   - if `materialize/3` omitted, facade fallback path still passes materialization scenarios
   - if introspection callbacks omitted, facade fallback introspection remains valid where supported

---

## 14. DuckDB fit check

Why this design fits DuckDB well:

- DuckDB can implement required core quickly (`connect/disconnect/capabilities/execute/query`).
- DuckDB can optionally add optimized introspection/materialization later without interface changes.
- Capability model cleanly expresses gaps (e.g., `merge` may be unsupported/emulated depending on version).
- `WritePlan` supports table/view/incremental strategies without embedding DuckDB-specific syntax in core.

Why this is not DuckDB-hardcoded:

- Adapter receives normalized relation/write structs, not DuckDB-specific tokens.
- Fallback orchestration lives in `Favn.SQL`, reusable for future backends.
- Backend-specific extensions are isolated in `Capabilities.extensions` and adapter-local SQL rendering.

---

## 15. Phased implementation plan

### Phase 1 — Core contract + facade skeleton

Modules:

- `Favn.SQL.Adapter`
- `Favn.SQL`
- `Favn.SQL.Capabilities`
- `Favn.SQL.Relation`
- `Favn.SQL.Column`
- `Favn.SQL.Result`
- `Favn.SQL.Error`
- `Favn.SQL.WritePlan`
- `Favn.SQL.Session` (internal)
- `Favn.SQL.Options`

Tests:

- struct and type contract tests
- facade adapter resolution tests from `%Connection.Resolved{}`
- error normalization unit tests

Risk reduced:

- stabilizes contract before backend implementation details expand.

### Phase 2 — DuckDB adapter core

Modules:

- `Favn.SQL.Adapter.DuckDB`

Tests:

- adapter conformance: lifecycle/capabilities/execute/query/error mapping

Risk reduced:

- proves required core contract is sufficient for a real backend.

### Phase 3 — Materialization + fallback

Modules:

- `Favn.SQL.Materialization`
- `Favn.SQL.Fallback`

Tests:

- `%WritePlan{}` -> SQL orchestration tests
- optional `materialize/3` fallback tests
- incremental strategy matrix tests (`append/replace/delete_insert/merge` capability-gated)

Risk reduced:

- ensures SQL asset runtime can proceed even with minimal adapters.

### Phase 4 — Runtime/compiler integration seam

Modules touched:

- SQL asset compiler seam (existing v0.3 readiness area)
- runtime executor boundary integration with `Favn.SQL`

Tests:

- end-to-end SQL asset execution through shared planner/window runtime path
- run error/retry behavior with normalized SQL errors

Risk reduced:

- verifies no divergence between Elixir and SQL asset execution semantics.

---

## 16. Tradeoffs and rejected alternatives

1. **Rejected: callback-heavy large behaviour**
   - Too much mandatory surface in v0.4 slows first adapter delivery.
   - Encourages brittle placeholders and low-quality implementations.

2. **Rejected: opaque single high-level callback (e.g. `run/2`)**
   - Hides lifecycle/introspection/capability boundaries.
   - Makes fallback and planner-aware feature checks hard.

3. **Rejected: adapter DSL/macros in v0.4**
   - Adds abstraction complexity before contract stability.
   - Locked decision explicitly prefers plain `@behaviour` modules.

4. **Rejected: direct runtime-to-adapter calls without facade**
   - Spreads backend branching and error normalization everywhere.
   - Prevents consistent fallback and capability policy enforcement.

---

## 17. Final recommendation

Adopt a **struct-driven, hybrid-behaviour SQL adapter architecture** with:

- a **small required core** (`connect`, `disconnect`, `capabilities`, `execute`, `query`),
- **optional optimized callbacks** (`materialize`, introspection, transaction, ping),
- a central **`Favn.SQL` facade** that owns adapter resolution, lifecycle orchestration, fallback execution, capability gating, and error normalization,
- canonical internal structs (`Capabilities`, `Relation`, `Column`, `Result`, `Error`, `WritePlan`) as the stable contract between runtime/planner/compiler and backend adapters.

This is the most implementation-ready path for v0.4: fast to ship with DuckDB, cleanly extensible for later backends, and aligned with existing `Favn.Connection` and runtime/window architecture.
