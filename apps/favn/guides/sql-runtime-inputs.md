# Runtime Inputs For SQL Assets

Reader: authors whose SQL asset needs execution-specific bind values that cannot
be selected while the manifest is compiled.

Start with `Favn.AI`, then read `Favn.SQLAsset` and
`Favn.SQLAsset.RuntimeInputs`. This guide covers the complete authoring workflow.

## Choose The Right Contract

Use runtime SQL inputs for values such as an immutable snapshot ID, a selected
external manifest, a watermark, or a signed file location chosen for the final
run window.

| Need | Use | Do not use runtime SQL inputs because |
| --- | --- | --- |
| Select an external snapshot or watermark for the final window and bind it into SQL | `Favn.SQLAsset.RuntimeInputs` | This is the intended contract. |
| Read credentials, endpoints, tenant IDs, or deployment configuration | `Favn.RuntimeConfig.Ref` and `ctx.config` | Configuration is resolved separately and credentials should not become SQL parameters. |
| Accept normal run parameters already supplied by an operator or caller | Normal submitted `params` | A resolver would duplicate an existing input path. |
| Generate SQL source, table names, relation names, or lifecycle callbacks dynamically | Predeclare SQL/relation structure or redesign the asset | Resolver output is data only and never becomes SQL structure. |
| Perform multi-step API work or external writes | `Favn.Asset` | Elixir assets are the escape hatch for imperative side effects. |
| Guarantee the exact same selected input after retry, restart, rerun, or replay | Persist the selection in an upstream system or asset first | Runtime input payloads are not pinned in the current contract. |

Runtime input resolvers may perform I/O, but they are not sandboxes. Configure
client timeouts below Favn's resolver deadline and keep credentials in runtime
configuration whenever possible.

## One Canonical DSL Form

Declare exactly one resolver module before `query`:

```elixir
@runtime_inputs MyApp.Source.Orders.Inputs
```

This module attribute is the only supported public form. Anonymous functions,
captures, MFA tuples, and inline resolver blocks are not accepted. Remove any
experimental declarations such as these:

```elixir
@runtime_inputs fn ctx -> resolve_inputs(ctx) end
@runtime_inputs &MyApp.Source.Orders.Inputs.resolve/1
@runtime_inputs {MyApp.Source.Orders.Inputs, :resolve, []}

runtime_inputs do
  # unsupported
end
```

The stable module reference is independently testable and serializable. The
manifest stores `%Favn.RuntimeInputResolver.Ref{}`; authoring code does not
construct that internal reference directly.

## Define The Resolver

The resolver must explicitly implement `Favn.SQLAsset.RuntimeInputs` and export
`resolve/1`:

```elixir
defmodule MyApp.Source.Orders.Inputs do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(ctx) do
    snapshot = MyApp.SourceManifests.completed_for!(ctx.window)

    {:ok,
     %Result{
       params: %{
         snapshot_id: snapshot.id,
         watermark: snapshot.watermark
       },
       identity: snapshot.id,
       metadata: %{file_count: length(snapshot.files)}
     }}
  end
end
```

`ctx` is the final `Favn.Run.Context`, including the effective window, attempt,
run identity, current asset ref, deadline, and resolved runtime configuration.
Do not log or return the whole context.

## Attach It To The SQL Asset

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.SQLAsset

  @runtime_inputs MyApp.Source.Orders.Inputs
  @materialized {:incremental,
    strategy: :delete_insert,
    window_column: :occurred_at
  }

  query do
    ~SQL"""
    select *
    from raw.sales.order_snapshots
    where snapshot_id = @snapshot_id
      and occurred_at < @watermark
    """
  end
end
```

Resolved values use the normal `@name` placeholder and adapter binding path,
including through nested `defsql`. They cannot add SQL source, identifiers,
relations, or lifecycle callbacks. The same merged parameters are used by the
main query and transactional SQL checks without invoking the resolver again.

## Practical Patterns

### Bind An Immutable File Selection

Collections are not scalar SQL bind values. Encode a selected immutable file
list into JSON and decode it with the target adapter's JSON functions:

```elixir
%Result{
  params: %{files_json: Jason.encode!(snapshot.files)},
  identity: snapshot.id,
  metadata: %{file_count: length(snapshot.files)}
}
```

The resolver selects the files; SQL remains responsible for interpreting the
bound JSON value. Do not interpolate file paths into authored SQL source.

### Reuse One Resolved Parameter Through `defsql` And Checks

Resolver parameters use the same compiler path as submitted parameters, so a
reusable SQL definition and a transactional check can reference the same name:

```elixir
defmodule MyApp.SQL.Snapshots do
  use Favn.SQL

  defsql selected_snapshot(snapshot_id) do
    ~SQL"select * from raw.sales.order_snapshots where snapshot_id = @snapshot_id"
  end
end

defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use MyApp.SQL.Snapshots
  use Favn.SQLAsset

  @runtime_inputs MyApp.Source.Orders.Inputs
  @materialized :table

  check :snapshot_matches, at: :before_materialize, on_false: :fail do
    ~SQL"select count(*) > 0 as passed from query() where snapshot_id = @snapshot_id"
  end

  query do
    ~SQL"select * from selected_snapshot(@snapshot_id)"
  end
end
```

Favn invokes the resolver once. The staged query, every check, and the write all
use the same merged parameter map.

## Result Contract

Return only `{:ok, %Favn.SQLAsset.RuntimeInputs.Result{}}` or
`{:error, %Favn.SQLAsset.RuntimeInputs.Error{}}`.

| Result field | Contract |
| --- | --- |
| `params` | Atom or string names mapped to scalar SQL bind values. |
| `identity` | Non-empty stable identity for the selected external input. |
| `metadata` | Explicitly safe JSON-compatible lineage metadata. |
| `sensitive_params` | Names in `params` whose values must be redacted. |

Supported parameter values are `nil`, booleans, numbers, strings, `Date`,
`Time`, `NaiveDateTime`, `DateTime`, and `Decimal`. Encode collections into an
adapter-supported scalar representation such as JSON before returning them.

Submitted parameters and resolved parameters may not use the same normalized
name. `window_start` and `window_end` are reserved. A resolver cannot override
the final window.

If a bind value is sensitive, name it explicitly:

```elixir
%Result{
  params: %{signed_url: signed_url},
  identity: snapshot_id,
  metadata: %{source: "orders"},
  sensitive_params: [:signed_url]
}
```

Sensitive values remain runner-local and are redacted from errors, lineage,
telemetry, inspection, and runtime result details. Keep `identity` and
`metadata` safe even when no sensitive parameters are present.

## Timing And Limits

Favn resolves runtime inputs once per asset attempt after the effective window
is final and before SQL rendering, session acquisition, admission, or a
transaction.

| Boundary | Limit |
| --- | --- |
| resolver duration | 30 seconds or the remaining node deadline, whichever is shorter |
| parameters | 128 |
| encoded parameter payload | 4 MiB |
| identity | 1 KiB |
| metadata | 64 KiB and 128 entries |

Timeout and cancellation terminate resolver work through runner-owned process
cleanup. Resolver failures happen before a SQL connection is opened.

Resolved payloads are not persisted or pinned. A retry attempt, runner restart,
rerun, or replay resolves again. Do not use a replay-sensitive resolver when
correctness requires exactly the same external selection across attempts.

## Typed Failures

Return an operator-safe typed error for expected source failures:

```elixir
alias Favn.SQLAsset.RuntimeInputs.Error

{:error,
 %Error{
   reason: :snapshot_unavailable,
   message: "No completed orders snapshot exists for the run window",
   retryable?: true,
   metadata: %{source: "orders"}
 }}
```

`retryable?` is preserved as classification only; this contract does not add an
automatic retry policy. Error messages and metadata must not contain resolved
parameters, credentials, the complete context, or inspected exception terms.

## Test Resolver Modules Directly

Keep the resolver a normal small module. Unit test its selection logic and typed
success/error results without starting Favn. Add an owning SQL asset test for
parameter binding, collision handling, and any sensitive-value redaction that
is specific to the asset.

## Documentation Breadcrumb

Read these in order:

1. `Favn.AI` for the public task map.
2. `Favn.SQLAsset` for DSL placement and SQL execution semantics.
3. `Favn.SQLAsset.RuntimeInputs` for the callback.
4. `Favn.SQLAsset.RuntimeInputs.Result` and
   `Favn.SQLAsset.RuntimeInputs.Error` for accepted outcomes.
5. `Favn.RuntimeInputResolver.Ref` only for manifest/compiler work.
