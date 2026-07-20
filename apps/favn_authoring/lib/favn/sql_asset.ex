defmodule Favn.SQLAsset do
  @moduledoc """
  Preferred single-asset SQL DSL.

  Use this module when one module should define one SQL asset. Like
  `Favn.Asset`, it compiles to one canonical `%Favn.Asset{}` with ref
  `{Module, :asset}`, but the authored body comes from `query/1` instead of a
  handwritten `asset/1` function.

  ## When to use it

  Use `Favn.SQLAsset` when:

  - the asset body is primarily SQL
  - you want Favn-aware relation references, placeholders, and reusable SQL
  - you want the SQL asset to participate in the same dependency and runtime model as Elixir assets

  Like Elixir assets, SQL assets should carry a business-oriented `@moduledoc`
  that explains the data grain, business rules, and downstream purpose.

  Keep asset-specific SQL close to the asset. Inline SQL is fine for short
  queries. For file-backed SQL, place the `.sql` file next to the asset module
  and use a relative path such as `query file: "fct_orders.sql"`. Put SQL under
  `MyApp.SQL.*` only when it is reusable across assets.

  ## Minimal example

      # lib/my_app/lakehouse/mart/sales/fct_orders.ex
      defmodule MyApp.Lakehouse.Mart.Sales.FctOrders do
        @moduledoc \"\"\"
        Order fact mart used for revenue and customer reporting.

        One row represents one completed order. Test orders are excluded and
        order timestamps are grouped by business day in the lakehouse timezone.
        \"\"\"

        use Favn.SQLAsset

        @doc "Build the order fact mart."
        settings minimum_order_value: 10
        meta owner: "analytics"
        meta category: :sales
        meta tags: [:mart]
        window Favn.Window.daily(lookback: 1)
        freshness [window_success: true]
        depends MyApp.Lakehouse.Raw.Sales.Orders
        materialized {:incremental, strategy: :delete_insert, window_column: :order_date}

        query do
          ~SQL\"""
          select *
          from raw.sales.orders
          where order_date >= @window_start
            and order_date < @window_end
            and order_value >= @minimum_order_value
          \"""
        end
      end

  ## Contract

  - define exactly one `query/1` declaration
  - provide one effective `materialized` declaration on the asset or an ancestor
    namespace
  - attach `@doc` to `query`; Favn transfers it to the generated `asset/1`
  - declare `settings`, `meta`, `depends`, `window`, `freshness`, `retry`,
    `materialized`, optional `relation`, optional `resources`, optional
    `runtime_config`, and optional `runtime_inputs` before `query`
  - use `~SQL` for inline SQL bodies
  - use `query file: "..."` for asset-local file-backed SQL loaded at compile
    time

  ## Declarations

  - `@doc`: asset documentation
  - `settings`: non-secret, JSON-like static values compiled into the manifest
  - `meta`: keyword or map metadata such as `owner`, `category`, and `tags`
  - `depends`: repeatable dependency declaration
  - `window`: one `Favn.Window.*` spec
  - `freshness`: optional asset freshness policy
  - `retry`: optional node-attempt retry policy overriding the pipeline default
  - `relation`: optional owned relation declaration
  - `materialized`: SQL materialization strategy, required either locally or by
    namespace inheritance
  - `resources`: optional list of named physical-session resources
  - `runtime_config`: optional runtime-configuration bundle or inline requirements
  - `runtime_inputs`: optional module implementing `Favn.SQLAsset.RuntimeInputs`

  Repeated `settings` and `meta` declarations shallow-merge from left to right.
  SQL settings are also reusable scalar query inputs: a referenced setting such
  as `@minimum_order_value` becomes a bound parameter automatically. Binding is
  limited to referenced scalar settings. A name present in both `settings` and
  runtime `params` is rejected instead of applying hidden precedence.

  Settings cannot provide SQL identifiers or relations. For example,
  `from @source` is rejected even when `settings source: "orders"` exists; use
  a Favn-aware relation reference for identifiers. Secrets and deployment
  configuration belong in `runtime_config` and are available only to an
  explicit `runtime_inputs` module, never as automatic SQL placeholders.

  ## Retry policy

  SQL assets use the same `retry` declaration as `Favn.Asset`:

      retry max_attempts: 4,
             backoff: {:exponential, initial: 5_000, max: 120_000, jitter: 0.2}
      materialized :table
      query do
        ~SQL"select * from staged_orders"
      end

  The effective precedence is explicit operator override, asset `retry`,
  pipeline `retry`, then one attempt. Policy controls count and timing only.
  Favn never blindly retries a write, materialization, transaction, check, or
  unknown outcome merely because `max_attempts` is greater than one. SQL
  session/bootstrap/read safety retries are separate internal operations and do
  not consume node attempts.

  Read [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html) for
  the complete safety and replay contract.

  ## Freshness

  SQL assets use the same `freshness` contract as `Favn.Asset`. Attach at most
  one `freshness` before `query`.

  Supported V1 values are `:daily`, `{:daily, timezone: "Europe/Oslo"}`,
  `[max_age: {:hours, 6}]`, `[window_success: true]`, and `:always`. Windowed SQL
  assets default to exact window-success freshness; non-windowed SQL assets have
  no implicit freshness. `window Favn.Window.monthly(refresh_from: :day)` keeps
  exact monthly identities while allowing each month to refresh once per local
  day; explicit `freshness :daily` is asset-wide instead.

  Read `Favn.Freshness.Policy` for policy input details and
  `Favn.Freshness.Key` for stored freshness keys.

  `depends` supports:

  - `Other.SingleAssetModule`
  - `{Other.MultiAssetModule, :asset_name}`

  `materialized` currently supports:

  - `:table`
  - `:view`
  - `{:incremental, strategy: :append}`
  - `{:incremental, strategy: :delete_insert, window_column: :column_name}`

  Incremental rules:

  - incremental materialization requires `window`
  - `:append` does not accept `:window_column`
  - `:delete_insert` requires `:window_column`
  - `:merge`, `:replace`, and `unique_key` are not currently supported

  ## Output Contracts

  Table and incremental assets may declare one typed output contract. The
  contract is compiled into the manifest and describes ordered columns,
  structured or descriptive grain, unique keys, ordered exact or bounded
  row-count claims, reusable column-fragment provenance, and explicit column
  lineage:

      contract do
        grain by: [:record_id], description: "one normalized record"

        column :record_id, :integer,
          null: false,
          from: [{MyApp.Assets.SourceRecords, :source_id}],
          via: :transformation

        column :payload, :json, from: [{"external.records", "payload"}]
        unique [:record_id]

        row_count equals: param(:expected_row_count),
          on_violation: :fail

        row_count min: 1,
          when: :target_exists,
          on_violation: :skip_materialization
      end

  Candidate names, order, logical types, and reliable adapter nullability
  metadata are hard requirements checked before target mutation. Non-null
  columns, `grain by:`, unique keys, and row-count constraints compile into the
  ordinary transactional check engine. Generated checks carry origin
  `:contract` and stable claim identities; authored checks carry origin
  `:authored`, and both appear in the same assurance result model.
  Required-column and key enforcement is grouped; every row-count declaration
  adds one ordered check. A contract adds at most 18 generated checks and does
  not consume the 50 authored-check budget.

  Grain may use `by:`, `description:`, or both. A description is useful when
  row identity cannot be expressed by output columns, but only structured `by:`
  can generate a mechanical uniqueness check. Column `from:` is always a plain
  list of `{Module, :column}`, `{{Module, :asset}, :column}`, or
  `{"external.dataset", "field"}` tuples. `via:` may be `:identity`,
  `:transformation`, or `:aggregation`; use these declarations to record
  lineage explicitly.

  Repeated column metadata may use an explicit `Favn.SQL.ContractFragment` and
  `include Module` at the required output position. Includes flatten into the
  canonical ordered columns and retain separate composition provenance. Declare
  columns in the fragment and keep grain, keys, row counts, and checks local to
  the asset.

  Row counts accept literal `equals:`, `min:`, `max:`, or a `min:`/`max:` range.
  Repeat `row_count` to declare independent claims with their own conditions and
  violation policies; claims execute in authored order. Exact counts may use
  `equals: param(:name)` to bind a normal setting or runtime param that the
  runner validates as a non-negative integer before opening a SQL session.

  Write `select` expressions, aliases, casts, and backend-specific SQL in
  `query`; the contract validates and documents the result. Run
  `mix favn.read_doc Favn.SQLAsset contract` or read the HexDocs guide
  `guides/sql-output-contracts.md` for all logical types, options, automatic
  checks, policy behavior, and semantic diffing.

  ## Transactional Checks

  Table and incremental assets can declare up to 50 uniquely named authored
  SQL-native checks. An output contract adds at most 18 generated checks. Checks
  use the same compiler, reusable `defsql` definitions,
  parameters, window values, and relation resolution as `query`.

  Use checks for read-only aggregate invariants over the exact candidate or
  owned target when the result must participate in the materialization commit.
  Keep transformation logic in the main `query`, imperative or external-system
  validation in an upstream `Favn.Asset`, and dependency/freshness policy in
  their dedicated DSL declarations. Checks are publication gates and quality
  annotations, not a general test runner.

      check :has_rows,
        at: :before_materialize,
        when: :target_exists,
        on_violation: :skip_materialization,
        message: "No rows were available; the existing target was kept" do
        ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
      end

      check :known_statuses,
        at: :after_materialize,
        on_violation: :warn do
        ~SQL"select count(*) filter (where status not in ('open', 'closed')) = 0 as passed from target()"
      end

  Every executed check returns exactly one row with one non-null native Boolean
  `passed` column. Up to 32 additional bounded scalar columns become durable
  metrics. A false result follows its required `on_violation` policy:

  - `:fail` rolls back and fails the asset attempt
  - `:warn` commits with `quality_status: :warning`
  - `:skip_materialization` commits a successful `quality_status: :warning`,
    `write_outcome: :no_op`

  `:skip_materialization` is valid only before materialization and requires
  `when: :target_exists`. SQL errors and invalid result shapes always fail and
  roll back; `on_violation` does not turn execution or contract errors into
  warnings. Checked views are rejected because their published rows are not a
  transactionally fixed snapshot.

  `query()` is the staged candidate that is also materialized; `target()` is the
  existing target before the write and the modified target afterward. A before
  check using `target()` must declare `when: :target_exists`. The same condition
  is required for `:skip_materialization`, allowing missing-target bootstrap to
  proceed normally.

  Prefer a before check when the candidate alone answers the question. Use an
  after check only when the exact transaction-visible published target matters.
  Use `:warn` only when the target remains safe for consumers, and use
  `:skip_materialization` only when retaining the existing target is a valid
  successful outcome rather than hidden staleness.

  Favn opens one transaction, runs all before checks in declaration order,
  materializes the exact staged candidate, runs all after checks in declaration
  order, and then commits. A failed check, invalid result, or SQL error rolls
  back. A warning and a successful no-op remain successful asset executions for
  freshness and downstream gating.

  Check results use `Favn.SQL.CheckResult` and are exposed in durable run detail
  metadata with `quality_status` and `write_outcome`. Read
  `Favn.SQLAsset.check/3` for the exact option and return contract. The package
  guide `guides/sql-asset-checks.md` provides the complete authoring workflow,
  examples, metric limits, and failure modes.

  ## Dependency Inference

  Relation-style references are the preferred way to reference upstream SQL
  inputs when the SQL name unambiguously matches the owned relation convention.

  When a relation reference resolves to an owned asset relation in the same
  connection, Favn infers the dependency automatically. Use `depends` when the
  dependency is not visible in the SQL body, when you need a non-SQL upstream,
  when the relation cannot be resolved from owned asset metadata. New lakehouse
  projects should use catalog for the database/phase and schema for the
  segment/domain; catalog-qualified SQL references require a schema so
  `raw.sales.orders` is unambiguous.

  ## Compiles To

  The DSL keeps both authored SQL and normalized SQL IR so Favn can validate
  reusable SQL definitions, placeholders, and typed relation usage at compile
  time. The final public output is still one canonical `%Favn.Asset{}` plus
  SQL-specific definition metadata used by rendering and execution.

  ## Runtime Context

  The generated `asset/1` calls into the SQL runtime automatically. Runtime
  inputs such as window bounds and explicit `params` are resolved during render
  and execution, not inside user-authored Elixir code.

  Ancestor `Favn.Namespace` modules may select runtime-config bundles for all
  descendant SQL assets. Bundles merge from root to leaf, followed by local
  `runtime_config` declarations, using the same deduplication and conflict
  semantics as Elixir assets. A SQL asset with non-empty effective runtime
  configuration must also have an effective `runtime_inputs` declaration. The
  closest namespace or leaf declaration selects that resolver, which receives
  the resolved values through the `runtime_config` field of
  `Favn.Run.Context`. Namespace configuration never becomes automatic SQL
  parameters.

  A SQL asset may declare one behaviour-based resolver for runtime-only bind
  values that cannot be selected when the manifest is compiled:

      defmodule MyApp.Orders.Inputs do
        @behaviour Favn.SQLAsset.RuntimeInputs

        alias Favn.SQLAsset.RuntimeInputs.Result

        @impl true
        def resolve(ctx) do
          manifest = MyApp.Manifests.completed_for!(ctx.window)

          {:ok,
           %Result{
             params: %{files_json: Jason.encode!(manifest.files)},
             identity: manifest.id,
             metadata: %{file_count: length(manifest.files)}
           }}
        end
      end

      defmodule MyApp.Orders do
        use Favn.SQLAsset

        runtime_inputs MyApp.Orders.Inputs
        materialized :table

        query do
          ~SQL"select * from read_ndjson(from_json(@files_json, '[\"VARCHAR\"]'))"
        end
      end

  `runtime_inputs MyApp.Orders.Inputs` before `query` is the only supported
  declaration. Anonymous functions, captures, MFA tuples, and inline resolver
  blocks are not accepted. Remove those forms instead of wrapping or migrating
  them at runtime; the manifest must contain one stable typed module reference.

  The resolver runs only when the asset will execute, after the effective window
  is final and before SQL rendering or session admission. The orchestrator
  atomically pins the normalized result under the run and planned node before
  SQL work can start. Every retry and safe restart recovery reuses that persisted
  winner without calling the resolver again. Its values use the normal SQL
  binding path and can be referenced by nested `defsql` calls. Resolver values
  never become SQL source. Submitted and resolved parameter names may not
  collide, and `window_start`, `window_end`, `favn_run_id`, and
  `favn_run_started_at` remain reserved. The two Favn-owned execution values are
  bound automatically from the runner context and may be referenced directly as
  `@favn_run_id` and `@favn_run_started_at`.

  Resolution has a 30-second upper bound, additionally limited by the remaining
  node deadline. The runner accepts at most 128 parameters, a 4 MiB parameter
  payload, a 1 KiB identity, and 64 KiB/128 entries of JSON-safe metadata.
  Sensitive names declared in the result are redacted outside the dedicated pin
  payload and require protected persistence. Missing protection fails before
  materialization. Normal new runs resolve fresh, exact replay requires source
  pins, and resume/retry-remaining inherit existing pins while resolving only
  nodes the source run never reached.

  Read `Favn.SQLAsset.RuntimeInputs`, then
  `Favn.SQLAsset.RuntimeInputs.Result` and
  `Favn.SQLAsset.RuntimeInputs.Error`. The package guide
  [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) provides the complete
  authoring workflow, supported values, limits, redaction rules, and pinning
  boundary.

  ## SQL Session Resources

  Declare trusted physical-session capabilities by stable name before `query`:

      resources [:azure_extension, :landing_storage]
      materialized :table
      query do
        ~SQL"select * from read_parquet('abfss://landing/orders/*.parquet')"
      end

  Ancestor `Favn.Namespace` modules may add resources for all descendant SQL
  assets. Namespace and leaf resources are additive, normalized to stable
  lowercase snake_case strings, deduplicated, sorted, and stored in the
  manifest. The runtime resolves those names to trusted native SQL files from
  connection config before the asset query runs.

  During runtime execution, Favn also scopes DuckDB/DuckLake catalogs from the
  rendered target relation and rendered Favn asset references. Catalog metadata
  may select an additional resource. Ad-hoc SQL references to undeclared
  catalogs are intentionally not used for session preparation; declare
  dependencies, relation ownership, or `resources` explicitly.

  Session scripts and asset queries both reference values as `@name`, but they
  have separate parameter sources. Session-script values come from connection
  `params`; asset values come from asset runtime inputs. Read
  [DuckDB Session Scripts And Resources](duckdb-session-scripts.html) for file
  locators, lifecycle, pooling, and safety rules.

  ## Common Mistakes

  - defining more than one query
  - forgetting `materialized`
  - using interpolation inside `~SQL`
  - using invalid `depends`, `window`, `freshness`, or `relation` values
  - using an inline function, capture, MFA tuple, or block instead of
    `runtime_inputs ResolverModule`
  - inheriting or declaring `runtime_config` without an effective
    `runtime_inputs ResolverModule`
  - expecting runtime-config values to become automatic SQL parameters
  - placing `resources` after `query`, using unstable names, or expecting a
    resource dependency graph
  - expecting `asset/1` to be user-defined in a `Favn.SQLAsset` module
  - returning multiple rows or a non-Boolean `passed` value from a check
  - adding checks to `:view` materialization

  ## See also

  - `Favn.SQL`
  - `Favn.SQLAsset.RuntimeInputs`
  - `Favn.SQLAsset.RuntimeInputs.Result`
  - `Favn.SQLAsset.RuntimeInputs.Error`
  - `Favn.SQL.CheckResult`
  - `Favn.Window`
  - `Favn.Freshness.Policy`
  - `Favn.Connection`
  """

  alias Favn.Asset
  alias Favn.Asset.RelationResolver
  alias Favn.DSL.AssetDeclarations
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Requirements
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQL
  alias Favn.SQL.Check
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.{Composition, Fragment, Param}
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.SessionRequirements
  alias Favn.SQL.Source
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Definition
  alias Favn.SQLAsset.Materialization
  alias Favn.SQLAsset.RelationUsage
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    env = __CALLER__

    quote bind_quoted: [file: env.file, line: env.line] do
      Favn.DSL.AssetDeclarations.claim_module!(__MODULE__, :sql_asset, file, line)
      Favn.DSL.AssetDeclarations.register!(__MODULE__)

      Favn.DSL.AssetDeclarations.register!(__MODULE__, [
        :materialized,
        :runtime_inputs,
        :resources
      ])

      Module.register_attribute(__MODULE__, :favn_sql_asset_raw, persist: false)

      Module.register_attribute(__MODULE__, :favn_sql_checks, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_sql_contracts, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_sql_imports, accumulate: true)

      @on_definition Favn.SQLAsset
      @before_compile Favn.SQLAsset

      import Favn.DSL.AssetDeclarations,
        only: [
          settings: 1,
          meta: 1,
          depends: 1,
          window: 1,
          freshness: 1,
          retry: 1,
          execution_pool: 1,
          relation: 1,
          runtime_config: 1,
          runtime_config: 2,
          env!: 1,
          env!: 2,
          secret_env!: 1,
          secret_env!: 2
        ]

      import Favn.SQLAsset,
        only: [
          check: 3,
          contract: 1,
          materialized: 1,
          param: 1,
          query: 1,
          resources: 1,
          runtime_inputs: 1
        ]

      import Favn.SQL, only: [sigil_SQL: 2]
    end
  end

  @doc "Declares the SQL materialization strategy."
  defmacro materialized(value) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :materialized, unquote(value))
    end
  end

  @doc "Declares the runtime-input resolver used by this SQL asset."
  defmacro runtime_inputs(module) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :runtime_inputs, unquote(module))
    end
  end

  @doc "Declares named SQL session resources required by this asset."
  defmacro resources(values) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :resources, unquote(values))
    end
  end

  @doc """
  Declares a transactional SQL check for the asset query or target.

  Checks run inside the same transaction as the checked materialization. All
  `:before_materialize` checks run in declaration order, followed by the write,
  then all `:after_materialize` checks in declaration order.

  ## Arguments and options

  `name` must be a unique non-`nil` atom. One SQL asset supports at most 50
  checks.

  The keyword options are:

  - `:at` - required; `:before_materialize` or `:after_materialize`
  - `:on_violation` - required; `:fail`, `:warn`, or `:skip_materialization`
  - `:when` - optional; `:target_exists` condition-skips the check when the
    target is missing
  - `:message` - optional human-readable context, limited to 1,024 bytes

  `:skip_materialization` is valid only before the write and requires
  `when: :target_exists`. A before check using `target()` also requires that
  condition.

  ## SQL result contract

  The body must produce exactly one row with one non-null native Boolean column
  named `passed`. Up to 32 other bounded scalar columns become metric entries in
  `Favn.SQL.CheckResult.metrics`. Supported metric values are null, Boolean,
  number, Decimal, string, date, time, naive datetime, and datetime scalars.
  Strings are limited to 4,096 bytes and the JSON-encoded metric map to 65,536
  bytes. A false `passed` value applies `:on_violation`:

  - `:fail` rolls back and fails the asset
  - `:warn` records a durable warning and continues
  - `:skip_materialization` commits a successful warning/no-op without changing
    the existing target

  SQL errors and invalid result shapes always fail and roll back, regardless of
  `:on_violation`.

  Choose `:fail` for required publication invariants, `:warn` for non-blocking
  quality degradation, and `:skip_materialization` only when keeping an existing
  target is explicitly a successful publication outcome. Checks are for read-only
  aggregate validation; transformations belong in the main `query` and
  external or imperative validation belongs in an upstream `Favn.Asset`.

  `query()` resolves to the exact staged candidate used by the write.
  `target()` resolves to the transaction-visible owned target. Both helpers may
  flow through nested reusable `defsql` calls.

  ## Example

      check :has_rows,
        at: :before_materialize,
        when: :target_exists,
        on_violation: :skip_materialization do
        ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
      end

  Read the package guide `guides/sql-asset-checks.md` for complete examples,
  metric limits, bootstrap semantics, and persisted outcome meanings.
  """
  defmacro check(name, check_opts, do: body) do
    name = expand_literal!(name, __CALLER__, "check name")

    unless is_atom(name) and not is_nil(name) do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "check name must be a non-nil atom, got: #{Macro.to_string(name)}"
      )
    end

    normalized_opts = normalize_check_opts!(check_opts, __CALLER__)
    sql = extract_sql!(body, __CALLER__)

    raw = %{
      name: name,
      opts: normalized_opts,
      sql: sql,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: DSLCompiler.normalize_file(__CALLER__.file),
      sql_line: __CALLER__.line
    }

    quote bind_quoted: [raw: Macro.escape(raw)] do
      @favn_sql_checks raw
      :ok
    end
  end

  @doc """
  Declares the asset's typed output contract.

  A SQL asset may declare at most one contract. Columns are ordered and use
  backend-neutral logical types. Grain may be structured with `by:` and/or
  descriptive when row identity cannot be expressed by output columns.
  Record column lineage with an explicit plain `from:` list. `renamed_from:`
  records evolution intent for semantic diffing; emit the new column name in
  the query output.

      contract do
        grain by: [:record_id], description: "one normalized record"

        column :record_id, :integer,
          null: false,
          from: [{SourceAsset, :source_id}],
          via: :transformation

        column :payload, :string, null: true
        unique [:record_id]

        row_count min: 1,
          when: :target_exists,
          on_violation: :skip_materialization
      end

  `grain by:` and `unique` generate transactional uniqueness checks;
  non-null columns generate non-null checks; and each ordered `row_count`
  declaration generates a normal policy-controlled row-count check. An earlier
  failed claim cannot be hidden by a later no-op claim. Candidate column names, order,
  types, and observable nullability are hard contract requirements checked
  before target mutation. Write the query and select list explicitly. See the
  HexDocs guide `guides/sql-output-contracts.md` for the complete option,
  fragment, runtime parameter, type, enforcement, bounds, and result reference.
  """
  defmacro contract(do: body) do
    raw = parse_contract!(body, __CALLER__)

    fragment_dependencies =
      Enum.map(raw.definition.compositions, fn composition ->
        quote do
          require unquote(composition.module)
          unquote(composition.module).__favn_sql_contract_dependency__()
        end
      end)

    quote do
      unquote_splicing(fragment_dependencies)
      raw = unquote(Macro.escape(raw))
      @favn_sql_contracts raw
      :ok
    end
  end

  @doc """
  References one normal runtime-bound SQL parameter from a contract claim.

  Use this marker as the `equals:` value in a row-count declaration:
  `row_count equals: param(:parameter_name)`.
  """
  defmacro param(name) do
    quote do
      Favn.SQL.Contract.Param.new!(unquote(name))
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    arity = length(args || [])

    generated_definition? =
      Module.get_attribute(env.module, :favn_sql_asset_generating) == true and
        kind == :def and
        name in [
          :__favn_asset_compiler__,
          :__favn_assets_raw__,
          :__favn_sql_asset_definition__,
          :__favn_single_asset__,
          :asset
        ]

    if generated_definition? do
      :ok
    else
      case {kind, name, arity} do
        {:defp, :__favn_sql_query_marker__, 0} ->
          capture_query_declarations!(env)

        {kind, :asset, _arity} when kind in [:def, :defp] ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "Favn.SQLAsset reserves asset/1 and generates it automatically"
          )

        {kind, _name, _arity} when kind in [:def, :defp] ->
          validate_no_stray_asset_attributes!(env, kind, name, arity)

        _ ->
          :ok
      end
    end
  end

  @doc """
  Declares the SQL body for a `Favn.SQLAsset`.

  Use either an inline `~SQL` body or `file: "..."`. Each module may declare
  exactly one query.

  Supported forms:

  - `query do ... end`
  - `query file: "path/to/query.sql"`

  ## Examples

      query do
        ~SQL\"""
        select *
        from raw.sales.orders
        \"""
      end

      query file: "sql/fct_orders.sql"
  """
  defmacro query(do: body) do
    raw_definition = Module.get_attribute(__CALLER__.module, :favn_sql_asset_raw)

    if raw_definition do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "Favn.SQLAsset modules can define only one query body"
      )
    end

    sql = extract_sql!(body, __CALLER__)

    raw = %{
      module: __CALLER__.module,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: DSLCompiler.normalize_file(__CALLER__.file),
      sql_line: __CALLER__.line,
      sql: sql
    }

    Module.put_attribute(__CALLER__.module, :favn_sql_asset_raw, raw)

    quote do
      defp __favn_sql_query_marker__(), do: :ok
      :ok
    end
  end

  defmacro query(file: path) when is_binary(path) do
    raw_definition = Module.get_attribute(__CALLER__.module, :favn_sql_asset_raw)

    if raw_definition do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "Favn.SQLAsset modules can define only one query body"
      )
    end

    source = Source.load_file!(__CALLER__, path, owner: "query")

    raw = %{
      module: __CALLER__.module,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: source.sql_file,
      sql_line: source.sql_line,
      sql: source.sql
    }

    Module.put_attribute(__CALLER__.module, :favn_sql_asset_raw, raw)

    quote do
      defp __favn_sql_query_marker__(), do: :ok
      :ok
    end
  end

  defmacro query(opts) do
    DSLCompiler.compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "query expects either a do block or file: \"path/to/query.sql\", got: #{Macro.to_string(opts)}"
    )
  end

  @doc false
  defmacro __before_compile__(env) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    base_definition = Module.get_attribute(env.module, :favn_sql_asset_raw)

    if is_nil(base_definition) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.SQLAsset modules must define exactly one query body"
      )
    end

    late_declarations =
      Enum.flat_map(
        [
          :settings,
          :meta,
          :depends,
          :window,
          :freshness,
          :retry,
          :execution_pool,
          :relation,
          :runtime_config,
          :materialized,
          :runtime_inputs,
          :resources
        ],
        &AssetDeclarations.values(env.module, &1)
      )

    if late_declarations != [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "SQL asset declarations must appear before query"
      )
    end

    raw_definition =
      base_definition
      |> Map.put(
        :sql_imports,
        env.module |> DSLCompiler.fetch_accum_attribute(:favn_sql_imports) |> Enum.reverse()
      )
      |> Map.put(
        :checks,
        env.module |> DSLCompiler.fetch_accum_attribute(:favn_sql_checks) |> Enum.reverse()
      )
      |> Map.put(
        :contracts,
        env.module |> DSLCompiler.fetch_accum_attribute(:favn_sql_contracts) |> Enum.reverse()
      )

    Module.put_attribute(env.module, :favn_sql_asset_generating, true)

    quote do
      @doc false
      @spec __favn_asset_compiler__() :: module()
      def __favn_asset_compiler__, do: Favn.SQLAsset.Compiler

      @doc false
      def __favn_assets_raw__, do: [unquote(Macro.escape(raw_definition))]

      @doc false
      @spec __favn_sql_asset_definition__() :: Favn.SQLAsset.Definition.t()
      def __favn_sql_asset_definition__ do
        # Intentionally finalizes outside @before_compile so namespace inheritance
        # does not depend on same-batch parent module compile order.
        # Do not call from DSL macros during compilation.
        Favn.SQLAsset.finalize_raw_definition(unquote(Macro.escape(raw_definition)))
      end

      @doc false
      def __favn_single_asset__, do: true

      @doc unquote(raw_definition.doc || false)
      @spec asset(map()) :: Favn.Asset.return_value()
      def asset(ctx), do: Favn.SQLAsset.runtime_asset(__MODULE__, ctx)
    end
  end

  defp capture_query_declarations!(env) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    resources = AssetDeclarations.take(env.module, :resources)
    validate_resource_declarations!(resources, env)

    declarations = %{
      doc: DSLCompiler.normalize_doc(Module.get_attribute(env.module, :doc)),
      settings: AssetDeclarations.take(env.module, :settings),
      meta: AssetDeclarations.take(env.module, :meta),
      depends: AssetDeclarations.take(env.module, :depends),
      window: AssetDeclarations.take(env.module, :window),
      freshness: AssetDeclarations.take(env.module, :freshness),
      retry: AssetDeclarations.take(env.module, :retry),
      execution_pool: AssetDeclarations.take(env.module, :execution_pool),
      relation: AssetDeclarations.take(env.module, :relation),
      runtime_config: AssetDeclarations.take(env.module, :runtime_config),
      materialized: AssetDeclarations.take(env.module, :materialized),
      runtime_inputs: AssetDeclarations.take(env.module, :runtime_inputs),
      resources: resources
    }

    raw = Module.get_attribute(env.module, :favn_sql_asset_raw)
    Module.put_attribute(env.module, :favn_sql_asset_raw, Map.merge(raw, declarations))
    Module.put_attribute(env.module, :doc, {env.line, false})
    :ok
  end

  @doc false
  def validate_resource_declarations!(declarations, env) do
    Enum.each(declarations, fn
      resources when is_list(resources) ->
        try do
          SessionRequirements.normalize_resources!(resources)
        rescue
          error in ArgumentError -> DSLCompiler.compile_error!(env.file, env.line, error.message)
        end

      other ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "resources must be a list of atom or string names, got: #{inspect(other)}"
        )
    end)
  end

  @doc false
  @spec runtime_asset(module(), map()) :: Favn.Asset.return_value()
  def runtime_asset(module, ctx) when is_atom(module) and is_map(ctx) do
    runtime_module = Favn.SQLAsset.Runtime

    with {:module, ^runtime_module} <- Code.ensure_loaded(runtime_module),
         true <- function_exported?(runtime_module, :run, 2) do
      :erlang.apply(runtime_module, :run, [module, struct(Favn.Run.Context, ctx)])
    else
      _ -> {:error, :runtime_not_available}
    end
  end

  @doc false
  @spec finalize_raw_definition(map()) :: Definition.t()
  def finalize_raw_definition(raw_definition) when is_map(raw_definition) do
    build_definition!(raw_definition)
  end

  defp build_definition!(raw_definition) do
    namespace = Namespace.resolve(raw_definition.module)
    depends_on = normalize_depends!(raw_definition.depends, raw_definition)

    meta =
      namespace
      |> Namespace.effective_declarations(:meta, raw_definition.meta)
      |> normalize_meta!(raw_definition)

    settings =
      namespace
      |> Namespace.effective_declarations(:settings, raw_definition.settings)
      |> normalize_settings!(raw_definition)

    window_spec =
      namespace
      |> Namespace.effective_declarations(:window, raw_definition.window)
      |> normalize_window!(raw_definition)

    freshness =
      namespace
      |> Namespace.effective_declarations(:freshness, raw_definition.freshness)
      |> normalize_freshness!(window_spec, raw_definition)

    retry_policy = normalize_retry!(raw_definition.retry, raw_definition)

    materialization =
      namespace
      |> Namespace.effective_declarations(:materialized, raw_definition.materialized)
      |> normalize_materialized!(window_spec, raw_definition)

    runtime_inputs =
      namespace
      |> Namespace.effective_declarations(
        :runtime_inputs,
        Map.get(raw_definition, :runtime_inputs, [])
      )
      |> normalize_runtime_inputs!(raw_definition)

    runtime_config =
      normalize_runtime_config!(raw_definition, namespace.runtime_config, runtime_inputs)

    execution_pool = normalize_execution_pool!(raw_definition.execution_pool, raw_definition)

    session_requirements = normalize_session_requirements!(raw_definition, namespace.resources)
    contract = normalize_contract!(Map.get(raw_definition, :contracts, []), raw_definition)

    validate_checked_materialization!(
      materialization,
      raw_definition.checks,
      contract,
      raw_definition
    )

    relation =
      normalize_relation!(
        raw_definition,
        RelationResolver.inferred_relation_name_for_module(raw_definition.module),
        namespace.relation
      )

    known_definitions = fetch_sql_definitions!(raw_definition)

    checks = compile_checks!(raw_definition, known_definitions, contract)

    template =
      Template.compile!(raw_definition.sql,
        known_definitions: known_definitions,
        file: raw_definition.sql_file,
        line: raw_definition.sql_line,
        module: raw_definition.module,
        scope: :query,
        local_args: [],
        enforce_query_root: true
      )

    sql_definitions = Map.values(known_definitions)
    validate_query_runtime_relations!(template, sql_definitions, raw_definition)

    relation_inputs =
      RelationUsage.collect(raw_definition.module, template, sql_definitions) ++
        Enum.flat_map(checks, fn %Check{template: check_template} ->
          RelationUsage.collect(raw_definition.module, check_template, sql_definitions)
        end)

    asset = %Asset{
      module: raw_definition.module,
      name: :asset,
      entrypoint: :asset,
      ref: Ref.new(raw_definition.module, :asset),
      arity: 1,
      type: :sql,
      doc: raw_definition.doc,
      file: raw_definition.file,
      line: raw_definition.line,
      meta: meta,
      depends_on: depends_on,
      settings: settings,
      relation_inputs: relation_inputs,
      session_requirements: session_requirements,
      window_spec: window_spec,
      freshness: freshness,
      retry_policy: retry_policy,
      execution_pool: execution_pool,
      relation: relation,
      materialization: materialization,
      runtime_config: runtime_config
    }

    definition = %Definition{
      module: raw_definition.module,
      asset: asset,
      sql: raw_definition.sql,
      template: template,
      relation_inputs: relation_inputs,
      runtime_inputs: runtime_inputs,
      session_requirements: session_requirements,
      contract: contract,
      sql_definitions: Map.values(known_definitions),
      checks: checks,
      materialization: materialization,
      raw_asset: raw_definition
    }

    try do
      _ = Asset.validate!(asset)
      definition
    rescue
      error in ArgumentError ->
        DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
    end
  end

  defp compile_checks!(raw_definition, known_definitions, contract) do
    raw_checks =
      generated_contract_checks(contract, raw_definition) ++ Map.get(raw_definition, :checks, [])

    ensure_check_count!(raw_checks, raw_definition)
    ensure_unique_check_names!(raw_checks)
    sql_definitions = Map.values(known_definitions)

    raw_checks
    |> Enum.map(fn raw_check ->
      try do
        template =
          Template.compile!(raw_check.sql,
            known_definitions: known_definitions,
            file: raw_check.sql_file,
            line: raw_check.sql_line,
            module: raw_definition.module,
            scope: :query,
            local_args: [],
            enforce_query_root: true
          )

        usages = RelationUsage.runtime_relations(template, sql_definitions)

        Check.new!(%{
          name: raw_check.name,
          at: Keyword.fetch!(raw_check.opts, :at),
          on_violation: Keyword.fetch!(raw_check.opts, :on_violation),
          when: Keyword.get(raw_check.opts, :when),
          message: Keyword.get(raw_check.opts, :message),
          sql: raw_check.sql,
          template: template,
          file: raw_check.sql_file,
          line: raw_check.sql_line,
          origin: Map.get(raw_check, :origin, :authored),
          claim_id: Map.get(raw_check, :claim_id),
          uses_query?: MapSet.member?(usages, :query),
          uses_target?: MapSet.member?(usages, :target)
        })
      rescue
        error in ArgumentError ->
          DSLCompiler.compile_error!(raw_check.file, raw_check.line, error.message)
      end
    end)
    |> then(&Contract.validate_generated_checks!(contract, &1))
  end

  defp generated_contract_checks(nil, _raw_definition), do: []

  defp generated_contract_checks(%Contract{} = contract, raw_definition) do
    Enum.map(Contract.generated_check_specs(contract), fn spec ->
      %{
        name: spec.name,
        opts: [
          at: spec.at,
          on_violation: spec.on_violation,
          when: spec.when,
          message: spec.message
        ],
        sql: spec.sql,
        file: raw_definition.file,
        line: raw_definition.line,
        sql_file: raw_definition.file,
        sql_line: raw_definition.line,
        origin: :contract,
        claim_id: spec.claim_id
      }
    end)
  end

  defp ensure_check_count!(raw_checks, raw_definition) do
    authored_count = Enum.count(raw_checks, &(Map.get(&1, :origin, :authored) == :authored))
    contract_count = Enum.count(raw_checks, &(Map.get(&1, :origin, :authored) == :contract))

    if authored_count > Check.max_per_asset() do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "SQL assets support at most #{Check.max_per_asset()} authored checks"
      )
    end

    if contract_count > Check.max_contract_per_asset() do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "SQL contracts support at most #{Check.max_contract_per_asset()} generated checks"
      )
    end
  end

  defp ensure_unique_check_names!(raw_checks) do
    raw_checks
    |> Enum.group_by(& &1.name)
    |> Enum.each(fn
      {_name, [_check]} ->
        :ok

      {name, [check | _rest]} ->
        DSLCompiler.compile_error!(check.file, check.line, "duplicate SQL check #{inspect(name)}")
    end)
  end

  defp validate_checked_materialization!(:view, [_check | _rest], _contract, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "SQL checks do not support :view materialization; use a snapshot table"
    )
  end

  defp validate_checked_materialization!(:view, [], %Contract{}, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "SQL output contracts do not support :view materialization; use a snapshot table"
    )
  end

  defp validate_checked_materialization!(_materialization, _checks, _contract, _raw_definition),
    do: :ok

  defp normalize_contract!([], _raw_definition), do: nil

  defp normalize_contract!([raw_contract], _raw_definition) do
    try do
      Contract.new!(raw_contract.definition)
    rescue
      error in ArgumentError ->
        DSLCompiler.compile_error!(raw_contract.file, raw_contract.line, error.message)
    end
  end

  defp normalize_contract!([_first, second | _rest], _raw_definition) do
    DSLCompiler.compile_error!(
      second.file,
      second.line,
      "Favn.SQLAsset modules can declare at most one contract"
    )
  end

  defp normalize_depends!(depends, raw_definition) do
    Enum.map(depends, fn
      module when is_atom(module) ->
        Ref.new(module, :asset)

      {module, name} when is_atom(module) and is_atom(name) ->
        if DSLCompiler.module_atom?(module) do
          Ref.new(module, name)
        else
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid depends entry #{inspect({module, name})}; expected Module or {Module, :asset_name}"
          )
        end

      dependency ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "invalid depends entry #{inspect(dependency)}; expected Module or {Module, :asset_name}"
        )
    end)
  end

  defp normalize_meta!(meta, raw_definition) do
    Enum.reduce(meta, %{}, fn declaration, acc ->
      Map.merge(acc, Asset.normalize_meta!(declaration))
    end)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_settings!(settings, raw_definition) do
    Favn.Settings.merge_all!(settings)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_runtime_config!(raw_definition, inherited, runtime_inputs) do
    requirements =
      inherited
      |> Kernel.++(raw_definition.runtime_config)
      |> Requirements.merge_all!(consumer: raw_definition.module)

    if map_size(requirements) > 0 and is_nil(runtime_inputs) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "SQLAsset runtime_config requires runtime_inputs so the resolved values have an explicit consumer"
      )
    end

    requirements
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_execution_pool!([], _raw_definition), do: nil

  defp normalize_execution_pool!([value], _raw_definition)
       when is_atom(value) and not is_nil(value),
       do: value

  defp normalize_execution_pool!([_first, _second | _rest], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple execution_pool declarations are not allowed"
    )
  end

  defp normalize_execution_pool!(value, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid execution_pool value #{inspect(value)}; expected a non-nil atom"
    )
  end

  defp normalize_window!([], _raw_definition), do: nil
  defp normalize_window!([nil], _raw_definition), do: nil
  defp normalize_window!([%Spec{} = spec], _raw_definition), do: spec

  defp normalize_window!([_a, _b | _rest], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple window attributes are not allowed; use at most one window before query"
    )
  end

  defp normalize_window!(value, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp normalize_freshness!(freshness, window_spec, raw_definition) do
    Asset.normalize_freshness!(freshness, window_spec, "before query")
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_retry!([], _raw_definition), do: nil
  defp normalize_retry!([value], _raw_definition), do: Favn.Retry.Policy.new!(value)

  defp normalize_retry!([_first, _second | _rest], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple retry attributes are not allowed; use at most one retry before query"
    )
  end

  defp normalize_retry!(value, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid retry value #{inspect(value)}; expected a retry keyword list"
    )
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_materialized!([value], window_spec, raw_definition) do
    value
    |> Materialization.normalize!()
    |> validate_incremental_materialized!(window_spec, raw_definition)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_materialized!([], _window_spec, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "Favn.SQLAsset requires one materialized attribute"
    )
  end

  defp normalize_materialized!([_a, _b | _rest], _window_spec, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple materialized attributes are not allowed; use exactly one materialized before query"
    )
  end

  defp validate_incremental_materialized!(
         {:incremental, opts} = materialization,
         window_spec,
         raw_definition
       ) do
    strategy = Keyword.fetch!(opts, :strategy)

    if is_nil(window_spec) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "incremental SQL materialization requires window"
      )
    end

    if Keyword.has_key?(opts, :unique_key) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "incremental materialization unique_key is reserved for future :merge semantics " <>
          "and is not currently supported"
      )
    end

    case strategy do
      :append ->
        if Keyword.has_key?(opts, :window_column) do
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "incremental :append does not accept :window_column"
          )
        end

      :delete_insert ->
        case Keyword.fetch(opts, :window_column) do
          {:ok, _column} ->
            :ok

          :error ->
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "incremental :delete_insert requires :window_column"
            )
        end

      :merge ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "incremental strategy :merge is not currently supported"
        )

      :replace ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "incremental strategy :replace is not currently supported"
        )
    end

    materialization
  end

  defp validate_incremental_materialized!(materialization, _window_spec, _raw_definition),
    do: materialization

  defp normalize_runtime_inputs!([], _raw_definition), do: nil
  defp normalize_runtime_inputs!([nil], _raw_definition), do: nil

  defp normalize_runtime_inputs!([module], raw_definition) when is_atom(module) do
    expected = "runtime_inputs MyApp.Inputs"

    unless DSLCompiler.module_atom?(module) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "invalid runtime_inputs value #{inspect(module)}; expected #{expected}"
      )
    end

    case Code.ensure_compiled(module) do
      {:module, ^module} ->
        :ok

      {:error, _reason} ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "runtime input resolver #{inspect(module)} could not be resolved; expected #{expected}"
        )
    end

    behaviours =
      module
      |> apply(:__info__, [:attributes])
      |> Keyword.get_values(:behaviour)
      |> List.flatten()

    unless Favn.SQLAsset.RuntimeInputs in behaviours do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "runtime input resolver #{inspect(module)} must explicitly declare @behaviour Favn.SQLAsset.RuntimeInputs"
      )
    end

    unless function_exported?(module, :resolve, 1) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "runtime input resolver #{inspect(module)} must export public resolve/1"
      )
    end

    RuntimeInputResolverRef.new!(module)
  end

  defp normalize_runtime_inputs!([value], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid runtime_inputs value #{inspect(value)}; expected runtime_inputs MyApp.Inputs"
    )
  end

  defp normalize_runtime_inputs!([_first, _second | _rest], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple runtime_inputs declarations are not allowed; use at most one runtime_inputs MyApp.Inputs before query"
    )
  end

  defp normalize_session_requirements!(raw_definition, inherited) do
    declared =
      raw_definition
      |> Map.get(:resources, [])
      |> Enum.flat_map(fn
        resources when is_list(resources) ->
          resources

        other ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "resources expects a list of atom or string names, got: #{inspect(other)}"
          )
      end)

    SessionRequirements.new!(inherited ++ declared)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_relation!(raw_definition, inferred_name, defaults) do
    relation_attrs =
      case raw_definition.relation do
        [] ->
          %{}

        [true] ->
          %{}

        [attrs] when is_list(attrs) ->
          if Keyword.keyword?(attrs) do
            Map.new(attrs)
          else
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "invalid relation value #{inspect(attrs)}; expected true, a keyword list, or a map"
            )
          end

        [attrs] when is_map(attrs) ->
          attrs

        [_a, _b | _rest] ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "multiple relation attributes are not allowed; use at most one relation before query do ... end"
          )

        [other] ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid relation value #{inspect(other)}; expected true, a keyword list, or a map"
          )
      end

    relation_attrs
    |> RelationResolver.resolve_relation_attrs!(defaults, inferred_name)
    |> ensure_sql_relation!(raw_definition)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp ensure_sql_relation!(%RelationRef{connection: nil}, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "SQL assets require a connection through Favn.Namespace or relation"
    )
  end

  defp ensure_sql_relation!(%RelationRef{} = relation_ref, _raw_definition),
    do: relation_ref

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    declarations =
      Enum.flat_map(
        [
          :settings,
          :meta,
          :depends,
          :window,
          :freshness,
          :retry,
          :execution_pool,
          :relation,
          :runtime_config,
          :materialized,
          :runtime_inputs,
          :resources
        ],
        &AssetDeclarations.values(env.module, &1)
      )

    if declarations != [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "SQL asset declarations before #{kind} #{name}/#{arity} require query immediately below them"
      )
    else
      :ok
    end
  end

  defp extract_sql!(body, env) do
    SQL.extract_sql!(body, env, "query body must contain a ~SQL literal")
  end

  defp parse_contract!(body, env) do
    statements =
      case body do
        {:__block__, _meta, statements} -> statements
        statement -> [statement]
      end

    definition =
      Enum.reduce(
        statements,
        %{grain: nil, columns: [], compositions: [], unique_keys: [], row_counts: []},
        &parse_contract_statement!(&1, &2, env)
      )

    %{
      definition: definition,
      file: DSLCompiler.normalize_file(env.file),
      line: env.line
    }
  end

  defp parse_contract_statement!({:grain, meta, [opts_ast]}, definition, env) do
    if definition.grain do
      contract_compile_error!(env, meta, "contract can declare grain only once")
    end

    opts = contract_keyword!(opts_ast, env, meta, :grain, [:by, :description])
    %{definition | grain: opts}
  end

  defp parse_contract_statement!({:column, meta, [name_ast, type_ast]}, definition, env) do
    parse_contract_column!(name_ast, type_ast, [], meta, definition, env)
  end

  defp parse_contract_statement!(
         {:column, meta, [name_ast, type_ast, opts_ast]},
         definition,
         env
       ) do
    parse_contract_column!(name_ast, type_ast, opts_ast, meta, definition, env)
  end

  defp parse_contract_statement!({:unique, meta, [columns_ast]}, definition, env) do
    columns = contract_literal!(columns_ast, env, meta, "contract unique columns")
    %{definition | unique_keys: definition.unique_keys ++ [columns]}
  end

  defp parse_contract_statement!({:include, meta, [module_ast]}, definition, env) do
    module = contract_module!(module_ast, env, meta)

    if Enum.any?(definition.compositions, &(&1.module == module)) do
      contract_compile_error!(
        env,
        meta,
        "contract fragment #{inspect(module)} is already included"
      )
    end

    fragment = fetch_contract_fragment!(module, env, meta)
    existing_names = MapSet.new(definition.columns, &contract_column_name/1)

    case Enum.find(fragment.columns, &MapSet.member?(existing_names, &1.name)) do
      nil ->
        :ok

      column ->
        contract_compile_error!(
          env,
          meta,
          "contract fragment #{inspect(module)} conflicts with existing column #{inspect(column.name)}"
        )
    end

    start_index = length(definition.columns)
    column_names = Enum.map(fragment.columns, & &1.name)
    composition = Composition.new!(module, start_index, column_names)

    %{
      definition
      | columns: definition.columns ++ fragment.columns,
        compositions: definition.compositions ++ [composition]
    }
  end

  defp parse_contract_statement!({:row_count, meta, [opts_ast]}, definition, env) do
    opts = contract_row_count_opts!(opts_ast, env, meta)

    unless Enum.any?([:equals, :min, :max], &Keyword.has_key?(opts, &1)),
      do: contract_compile_error!(env, meta, "contract row_count requires equals:, min:, or max:")

    %{definition | row_counts: definition.row_counts ++ [opts]}
  end

  defp parse_contract_statement!(statement, _definition, env) do
    DSLCompiler.compile_error!(
      env.file,
      env.line,
      "unsupported contract declaration: #{Macro.to_string(statement)}"
    )
  end

  defp parse_contract_column!(name_ast, type_ast, opts_ast, meta, definition, env) do
    name = contract_literal!(name_ast, env, meta, "contract column name")
    type = contract_literal!(type_ast, env, meta, "contract column type")

    opts =
      contract_keyword!(
        opts_ast,
        env,
        meta,
        :column,
        [:null, :description, :tags, :from, :via, :renamed_from]
      )

    column = %{name: name, type: type, opts: opts}

    case Enum.find(definition.columns, &(contract_column_name(&1) == name)) do
      nil -> :ok
      _column -> contract_compile_error!(env, meta, "duplicate contract column #{inspect(name)}")
    end

    %{definition | columns: definition.columns ++ [column]}
  end

  defp contract_row_count_opts!(ast, env, meta) do
    unless is_list(ast) and Keyword.keyword?(ast) do
      contract_compile_error!(env, meta, "contract row_count options must be a keyword list")
    end

    allowed = [:equals, :min, :max, :when, :on_violation]
    keys = Keyword.keys(ast)

    case keys -- Enum.uniq(keys) do
      [] ->
        :ok

      [key | _rest] ->
        contract_compile_error!(env, meta, "duplicate contract row_count option #{inspect(key)}")
    end

    case Enum.find(keys, &(&1 not in allowed)) do
      nil ->
        :ok

      key ->
        contract_compile_error!(env, meta, "unknown contract row_count option #{inspect(key)}")
    end

    Enum.map(ast, fn
      {:equals, {:param, param_meta, [name_ast]}} ->
        name = contract_literal!(name_ast, env, param_meta, "contract param name")

        try do
          {:equals, Param.new!(name)}
        rescue
          error in ArgumentError -> contract_compile_error!(env, param_meta, error.message)
        end

      {key, value_ast} ->
        {key, contract_literal!(value_ast, env, meta, "contract row_count #{key}")}
    end)
  end

  defp contract_module!(ast, env, meta) do
    module = Macro.expand(ast, env)

    if is_atom(module) and DSLCompiler.module_atom?(module) do
      module
    else
      contract_compile_error!(
        env,
        meta,
        "contract include expects a module, got: #{Macro.to_string(ast)}"
      )
    end
  end

  defp fetch_contract_fragment!(module, env, meta) do
    ensure_contract_fragment_compiled!(module, env, meta)

    unless function_exported?(module, :__favn_sql_contract_fragment__, 0) do
      contract_compile_error!(
        env,
        meta,
        "contract fragment #{inspect(module)} must use Favn.SQL.ContractFragment"
      )
    end

    case module.__favn_sql_contract_fragment__() do
      %Fragment{} = fragment ->
        Fragment.validate!(fragment)

      _other ->
        contract_compile_error!(env, meta, "contract fragment #{inspect(module)} is invalid")
    end
  rescue
    error in ArgumentError -> contract_compile_error!(env, meta, error.message)
  end

  defp ensure_contract_fragment_compiled!(module, env, meta) do
    Code.ensure_compiled!(module)
  rescue
    _error in ArgumentError ->
      contract_compile_error!(
        env,
        meta,
        "contract fragment #{inspect(module)} could not be resolved"
      )
  end

  defp contract_column_name(%Favn.SQL.Contract.Column{name: name}), do: name
  defp contract_column_name(%{name: name}), do: name

  defp contract_keyword!(ast, env, meta, declaration, allowed_keys) do
    value = contract_literal!(ast, env, meta, "contract #{declaration} options")

    unless is_list(value) and Keyword.keyword?(value) do
      contract_compile_error!(env, meta, "contract #{declaration} options must be a keyword list")
    end

    duplicate_keys =
      value
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(&(elem(&1, 1) > 1))

    if duplicate_keys != [] do
      keys = duplicate_keys |> Enum.map(&elem(&1, 0)) |> Enum.map_join(", ", &inspect/1)

      contract_compile_error!(
        env,
        meta,
        "duplicate contract #{declaration} options: #{keys}"
      )
    end

    case Enum.find(Keyword.keys(value), &(&1 not in allowed_keys)) do
      nil ->
        value

      key ->
        contract_compile_error!(
          env,
          meta,
          "unknown contract #{declaration} option #{inspect(key)}"
        )
    end
  end

  defp contract_literal!(ast, env, meta, label) do
    expanded =
      Macro.prewalk(ast, fn
        {:__aliases__, _alias_meta, _parts} = alias_ast -> Macro.expand(alias_ast, env)
        node -> node
      end)

    if Macro.quoted_literal?(expanded) do
      {value, _binding} = Code.eval_quoted(expanded, [], env)
      value
    else
      contract_compile_error!(env, meta, "#{label} must be literal, got: #{Macro.to_string(ast)}")
    end
  end

  defp contract_compile_error!(env, meta, message) do
    DSLCompiler.compile_error!(env.file, Keyword.get(meta, :line, env.line), message)
  end

  defp normalize_check_opts!(opts, env) do
    allowed = [:at, :on_violation, :when, :message]

    unless is_list(opts) and Keyword.keyword?(opts) do
      DSLCompiler.compile_error!(env.file, env.line, "SQL check options must be a keyword list")
    end

    duplicate_keys =
      opts |> Keyword.keys() |> Enum.frequencies() |> Enum.filter(&(elem(&1, 1) > 1))

    if duplicate_keys != [] do
      keys = duplicate_keys |> Enum.map(&elem(&1, 0)) |> Enum.map_join(", ", &inspect/1)
      DSLCompiler.compile_error!(env.file, env.line, "duplicate SQL check options: #{keys}")
    end

    Enum.each(Keyword.keys(opts), fn key ->
      unless key in allowed do
        DSLCompiler.compile_error!(env.file, env.line, "unknown SQL check option #{inspect(key)}")
      end
    end)

    unless Keyword.has_key?(opts, :at),
      do: DSLCompiler.compile_error!(env.file, env.line, "SQL check requires at:")

    unless Keyword.has_key?(opts, :on_violation),
      do: DSLCompiler.compile_error!(env.file, env.line, "SQL check requires on_violation:")

    Enum.map(opts, fn {key, value} ->
      {key, expand_literal!(value, env, "check option #{key}")}
    end)
  end

  defp validate_query_runtime_relations!(template, sql_definitions, raw_definition) do
    case RelationUsage.runtime_relations(template, sql_definitions) |> MapSet.to_list() do
      [] ->
        :ok

      relations ->
        names = relations |> Enum.sort() |> Enum.map_join(", ", &"#{&1}()")

        DSLCompiler.compile_error!(
          raw_definition.sql_file,
          raw_definition.sql_line,
          "#{names} may only be used inside SQL check bodies"
        )
    end
  end

  defp expand_literal!(ast, env, label) do
    value = Macro.expand(ast, env)

    if is_atom(value) or is_binary(value) do
      value
    else
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "#{label} must be a literal atom or string, got: #{Macro.to_string(ast)}"
      )
    end
  end

  defp fetch_sql_definitions!(raw_definition) do
    raw_definition
    |> Map.get(:sql_imports, [])
    |> Enum.uniq()
    |> Enum.flat_map(fn module ->
      case Code.ensure_compiled(module) do
        {:module, _} ->
          if function_exported?(module, :__favn_sql_definitions__, 0) do
            module.__favn_sql_definitions__()
          else
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "imported SQL provider #{inspect(module)} does not define reusable SQL"
            )
          end

        {:error, _reason} ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "imported SQL provider #{inspect(module)} could not be resolved"
          )
      end
    end)
    |> Enum.group_by(fn
      %SQLDefinition{name: name, arity: arity} -> {name, arity}
      other -> other
    end)
    |> Enum.map(fn
      {{name, arity}, [%SQLDefinition{} = definition]} ->
        {{name, arity}, definition}

      {{name, arity}, definitions} ->
        providers = definitions |> Enum.map(&inspect(&1.module)) |> Enum.sort() |> Enum.join(", ")

        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "duplicate visible defsql #{name}/#{arity}; conflicting providers: #{providers}"
        )

      {other, _definitions} ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "invalid SQL definition import #{inspect(other)}"
        )
    end)
    |> Map.new()
  end
end
