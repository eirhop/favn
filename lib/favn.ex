defmodule Favn do
  @moduledoc """
  Public facade for inspecting assets, planning runs, and executing workflows.

  `Favn` is the operator-facing entrypoint for the library. Use it to discover
  authored assets, inspect dependency relationships, work with pipelines,
  submit runs, await completion, inspect history, and access SQL helper APIs.

  This moduledoc is intentionally compact. Authoring rules live on the DSL
  modules such as `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`,
  `Favn.Assets`, `Favn.Pipeline`, and `Favn.Source`.

  For AI-first routing, start with `Favn.AgentGuide`.

  ## Core concepts

  - Asset: a compiled `%Favn.Asset{}` discovered from a public DSL module
  - Asset ref: `{Module, :asset_name}` or `{Module, :asset}` for single-asset modules
  - Pipeline: a named asset selection that still delegates dependency planning to the asset graph
  - Run: one submitted execution with plan, state, and history
  - Window: optional runtime time bounds shared by Elixir and SQL assets

  ## Authoring pointers

  - Use `Favn.Asset` for a single Elixir asset module
  - Use `Favn.SQLAsset` for a single SQL asset module
  - Use `Favn.MultiAsset` for repetitive extraction assets with shared runtime logic
  - Use `Favn.Assets` for compact multi-asset function modules when that style is intentional
  - Use `Favn.Namespace` for inherited relation defaults
  - Use `Favn.Connection`, `Favn.Window`, and `Favn.Triggers.Schedules` for supporting runtime definitions

  ## Inspect assets

      Favn.list_assets()
      Favn.list_assets(MyApp.Raw)
      Favn.get_asset(MyApp.Raw.Sales.Orders)
      Favn.get_asset({MyApp.Raw.Shopify, :orders})

  ## Run and await

      {:ok, run_id} = Favn.run_asset(MyApp.Raw.Sales.Orders)
      {:ok, run} = Favn.await_run(run_id)

      {:ok, run_id} =
        Favn.run_asset({MyApp.Raw.Shopify, :orders}, dependencies: :none)

  ## Pipelines

      {:ok, plan} = Favn.plan_pipeline(MyApp.Pipelines.DailySales)
      {:ok, run_id} = Favn.run_pipeline(MyApp.Pipelines.DailySales)

  ## Runs and history

      Favn.get_run(run_id)
      Favn.list_runs()
      Favn.list_runs(status: :running)

  ## SQL helpers

  SQL assets can be inspected and executed through the same runtime, and also
  support direct helper APIs:

      Favn.render(MyApp.Gold.Sales.FctOrders, params: %{limit: 100})
      Favn.preview(MyApp.Gold.Sales.FctOrders, params: %{limit: 100})
      Favn.explain(MyApp.Gold.Sales.FctOrders)
      Favn.materialize(MyApp.Gold.Sales.FctOrders)

  ## Minimal host setup

      import Config

      config :favn,
        asset_modules: [MyApp.Raw.Sales.Orders],
        pipeline_modules: [MyApp.Pipelines.DailySales]

  Add `connections`, `scheduler`, `storage_adapter`, and `storage_adapter_opts`
  as needed by your runtime.
  """

  @typedoc """
  Canonical reference to an asset.

  The public API should consistently use `{module, asset_name}` references.
  """
  @type asset_ref :: Favn.Ref.t()

  @typedoc """
  Canonical asset metadata returned by Favn inspection APIs.
  """
  @type asset :: Favn.Asset.t()

  @typedoc """
  Asset inspection errors returned by lookup APIs.
  """
  @type asset_error :: :not_asset_module | :asset_not_found

  @typedoc """
  Identifier for a single run.

  Favn currently generates UUID-like string identifiers, but callers should
  treat run IDs as opaque values.
  """
  @type run_id :: term()

  @typedoc """
  Dependency execution mode for `run_asset/2`.

    * `:all` - run the target asset and all of its upstream dependencies
    * `:none` - run only the requested target asset
  """
  @type dependencies_mode :: :all | :none

  @typedoc """
  Filter options for `list_runs/1`.
  """
  @type list_runs_opts :: [
          status: :running | :ok | :error | :cancelled | :timed_out,
          limit: pos_integer()
        ]

  @typedoc """
  Run retrieval/listing errors returned by storage-backed APIs.
  """
  @type run_error :: :not_found | :invalid_opts | {:store_error, term()}

  @typedoc """
  Stable run event payload delivered to subscribers.

  Event envelope fields:

    * `schema_version` - event schema version (currently `1`)
    * `event_type` - lifecycle event atom
    * `entity` - `:run` or `:step`
    * `run_id` - run identifier
    * `sequence` - monotonic per-run event sequence
    * `emitted_at` - event timestamp
    * `status` - internal runtime run/step status at emission time
    * `data` - event-specific details
    * optional `ref`/`stage` for step events
  """
  @type run_event :: Favn.Runtime.Events.event()

  @typedoc """
  Retry failure classes accepted by run retry policy.
  """
  @type retry_class :: :exception | :exit | :throw | :timeout | :executor_error | :error_return

  @typedoc """
  Retry policy for `run_asset/2`.

    * `max_attempts` includes the first attempt
    * `delay_ms` is a fixed delay between attempts
    * `retry_on` controls which failure classes are retried
  """
  @type retry_policy :: [
          max_attempts: pos_integer(),
          delay_ms: non_neg_integer(),
          retry_on: [retry_class()]
        ]

  @typedoc """
  Options for `run_asset/2`.
  """
  @type run_opts :: [
          dependencies: dependencies_mode(),
          params: map(),
          pipeline_context: map(),
          anchor_window: Favn.Window.Anchor.t(),
          max_concurrency: pos_integer(),
          timeout_ms: pos_integer(),
          retry: boolean() | retry_policy() | map()
        ]

  @typedoc """
  Options for `run_pipeline/2`.
  """
  @type run_pipeline_opts :: [
          params: map(),
          trigger: map(),
          anchor_window: Favn.Window.Anchor.t(),
          max_concurrency: pos_integer(),
          timeout_ms: pos_integer(),
          retry: boolean() | retry_policy() | map()
        ]

  @typedoc """
  Backfill anchor range definition.

  `end_at` is exclusive and will be expanded into contiguous anchors.
  """
  @type backfill_anchor_range :: %{
          required(:kind) => Favn.Window.Anchor.kind(),
          required(:start_at) => DateTime.t(),
          required(:end_at) => DateTime.t(),
          optional(:timezone) => String.t()
        }

  @typedoc """
  Options for `backfill_asset/2`.
  """
  @type backfill_asset_opts :: [
          dependencies: dependencies_mode(),
          params: map(),
          range: backfill_anchor_range(),
          max_concurrency: pos_integer(),
          timeout_ms: pos_integer(),
          retry: boolean() | retry_policy() | map()
        ]

  @typedoc """
  Options for `backfill_pipeline/2`.
  """
  @type backfill_pipeline_opts :: [
          params: map(),
          trigger: map(),
          range: backfill_anchor_range(),
          max_concurrency: pos_integer(),
          timeout_ms: pos_integer(),
          retry: boolean() | retry_policy() | map()
        ]

  @typedoc """
  Options for `await_run/2`.
  """
  @type await_run_opts :: [
          timeout: non_neg_integer() | :infinity,
          poll_interval_ms: pos_integer()
        ]

  @typedoc """
  Options for `check_asset_freshness/2`.
  """
  @type check_asset_freshness_opts :: [
          window_key: Favn.Window.Key.t(),
          max_age_seconds: non_neg_integer(),
          now: DateTime.t(),
          limit: pos_integer()
        ]

  @typedoc """
  Options for `plan_asset_run/2`.
  """
  @type plan_asset_run_opts :: [
          dependencies: dependencies_mode(),
          anchor_window: Favn.Window.Anchor.t(),
          anchor_windows: [Favn.Window.Anchor.t()],
          anchor_ranges: [backfill_anchor_range()]
        ]

  @typedoc """
  Pipeline module that exposes `__favn_pipeline__/0`.
  """
  @type pipeline_module :: module()

  alias Favn.Assets.Compiler
  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Assets.Registry
  alias Favn.Connection.NotFoundError
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Connection.Sanitizer
  alias Favn.Pipeline.Resolver
  alias Favn.Runtime.Engine
  alias Favn.Runtime.Events
  alias Favn.SQLAsset.Error, as: SQLAssetError
  alias Favn.SQLAsset.Input, as: SQLAssetInput
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias Favn.Window.Anchor

  @doc """
  List all registered assets.

  Global discovery is scoped to modules configured under
  `config :favn, asset_modules: [...]`.

  Deterministic behavior:

    * returned assets are sorted by canonical ref (`{module, name}` ascending)

  Returns:

    * `{:ok, assets}` where `assets` is a list of `%Favn.Asset{}`
    * `{:error, reason}` when the registry is unavailable or invalid

  ## Examples

      iex> Favn.list_assets()
      {:ok, []}
  """
  @spec list_assets() :: {:ok, [asset()]} | {:error, term()}
  def list_assets do
    with {:ok, assets} <- Registry.list_assets() do
      {:ok, Enum.sort_by(assets, & &1.ref)}
    end
  end

  @doc """
  List all assets for a specific module.

  Accepted input:

    * `module` - module atom

  Returns:

    * `{:ok, assets}` where `assets` contains `%Favn.Asset{}` entries for
      `module`
    * `{:error, :not_asset_module}` when `module` does not expose Favn asset
      metadata

  ## Examples

      iex> Favn.list_assets(Unknown.Module)
      {:error, :not_asset_module}
  """
  @spec list_assets(module()) :: {:ok, [asset()]} | {:error, asset_error()}
  def list_assets(module) when is_atom(module) do
    case Compiler.compile_module_assets(module) do
      {:ok, assets} -> {:ok, Enum.sort_by(assets, & &1.ref)}
      {:error, _reason} -> {:error, :not_asset_module}
    end
  end

  @doc """
  Fetch a single asset by reference.

  Accepted input:

    * `{module, name}` where both values are atoms
     * `module` for single-asset modules authored with `use Favn.Asset` or `use Favn.SQLAsset`

  Returns:

    * `{:ok, %Favn.Asset{}}` for a registered asset
    * `{:error, :not_asset_module}` when `module` is not a Favn asset module
    * `{:error, :asset_not_found}` when no asset named `name` exists

  ## Examples

      iex> Favn.get_asset({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec get_asset(asset_ref()) :: {:ok, asset()} | {:error, asset_error()}
  def get_asset({module, name}) when is_atom(module) and is_atom(name) do
    if asset_module?(module) do
      case Registry.get_asset({module, name}) do
        {:ok, asset} ->
          {:ok, asset}

        {:error, {:duplicate_asset, _ref}} ->
          {:error, :asset_not_found}

        {:error, :asset_not_found} ->
          {:error, :asset_not_found}
      end
    else
      {:error, :not_asset_module}
    end
  end

  @spec get_asset(module()) :: {:ok, asset()} | {:error, asset_error()}
  def get_asset(module) when is_atom(module) do
    cond do
      not asset_module?(module) ->
        {:error, :not_asset_module}

      function_exported?(module, :__favn_single_asset__, 0) ->
        Registry.get_asset({module, :asset})

      true ->
        {:error, :asset_not_found}
    end
  end

  @typedoc """
  Accepted SQL asset helper target input.
  """
  @type sql_asset_input :: module() | asset_ref() | asset()

  @typedoc """
  Common options for SQL helper APIs.

  `:params` contains query parameter values for non-reserved `@name` placeholders.
  `:runtime` contains reserved runtime SQL inputs such as `:window_start`, `:window_end`,
  or `:window` (`%Favn.Window.Runtime{}`).
  """
  @type sql_helper_opts :: [params: map(), runtime: map(), timeout_ms: pos_integer()]

  @typedoc """
  SQL preview options.
  """
  @type sql_preview_opts :: keyword()

  @typedoc """
  SQL explain options.
  """
  @type sql_explain_opts :: keyword()

  @doc """
  Render one SQL asset into canonical executable SQL and canonical bound params.

  This helper is backend/session free: it resolves compiled asset metadata and SQL
  definitions but does not open a SQL connection.
  """
  @spec render(sql_asset_input(), sql_helper_opts()) ::
          {:ok, Favn.SQL.Render.t()} | {:error, SQLAssetError.t()}
  def render(asset_input, opts \\ []) when is_list(opts) do
    case SQLAssetInput.normalize(asset_input) do
      {:ok, asset} -> SQLAssetRuntime.render(asset, opts)
      {:error, %SQLAssetError{} = error} -> {:error, error}
    end
  end

  @doc """
  Preview one SQL asset query result.

  `preview.statement` is the actual SQL executed for preview. `preview.render.sql`
  remains the canonical rendered SQL query.
  """
  @spec preview(sql_asset_input(), sql_preview_opts()) ::
          {:ok, Favn.SQL.Preview.t()} | {:error, SQLAssetError.t()}
  def preview(asset_input, opts \\ []) when is_list(opts) do
    case SQLAssetInput.normalize(asset_input) do
      {:ok, asset} -> SQLAssetRuntime.preview(asset, opts)
      {:error, %SQLAssetError{} = error} -> {:error, error}
    end
  end

  @doc """
  Explain one SQL asset query.
  """
  @spec explain(sql_asset_input(), sql_explain_opts()) ::
          {:ok, Favn.SQL.Explain.t()} | {:error, SQLAssetError.t()}
  def explain(asset_input, opts \\ []) when is_list(opts) do
    case SQLAssetInput.normalize(asset_input) do
      {:ok, asset} -> SQLAssetRuntime.explain(asset, opts)
      {:error, %SQLAssetError{} = error} -> {:error, error}
    end
  end

  @doc """
  Materialize one SQL asset.
  """
  @spec materialize(sql_asset_input(), sql_helper_opts()) ::
          {:ok, Favn.SQL.MaterializationResult.t()} | {:error, SQLAssetError.t()}
  def materialize(asset_input, opts \\ []) when is_list(opts) do
    case SQLAssetInput.normalize(asset_input) do
      {:ok, asset} -> SQLAssetRuntime.materialize(asset, opts)
      {:error, %SQLAssetError{} = error} -> {:error, error}
    end
  end

  @typedoc """
  Direction used by dependency graph inspection APIs.
  """
  @type dependency_direction :: Favn.Assets.GraphIndex.direction()

  @typedoc """
  Public connection lookup errors.
  """
  @type connection_error :: :not_found

  @typedoc """
  Public redacted connection inspection payload.
  """
  @type connection_info :: %{
          name: atom(),
          adapter: module(),
          module: module(),
          config: map(),
          required_keys: [atom()],
          secret_fields: [atom()],
          schema_keys: [atom()],
          metadata: map()
        }

  @doc """
  List all registered connections with secrets redacted.

  Returns:

    * list of maps with stable connection metadata and redacted config
  """
  @spec list_connections() :: [connection_info()]
  def list_connections do
    ConnectionRegistry.list()
    |> Enum.map(&Sanitizer.redact/1)
  end

  @doc """
  Fetch one registered connection by name with secrets redacted.
  """
  @spec get_connection(atom()) :: {:ok, connection_info()} | {:error, connection_error()}
  def get_connection(name) when is_atom(name) do
    case ConnectionRegistry.fetch(name) do
      {:ok, resolved} -> {:ok, Sanitizer.redact(resolved)}
      :error -> {:error, :not_found}
    end
  end

  @doc """
  Fetch one registered connection by name and raise when missing.
  """
  @spec get_connection!(atom()) :: connection_info()
  def get_connection!(name) when is_atom(name) do
    case get_connection(name) do
      {:ok, connection} -> connection
      {:error, :not_found} -> raise NotFoundError, name: name
    end
  end

  @doc """
  Return true when a connection with the given name is registered.
  """
  @spec connection_registered?(atom()) :: boolean()
  def connection_registered?(name) when is_atom(name) do
    ConnectionRegistry.registered?(name)
  end

  @typedoc """
  Options for dependency graph inspection APIs.
  """
  @type graph_opts :: [
          direction: dependency_direction(),
          include_target: boolean(),
          transitive: boolean(),
          tags: [atom() | String.t()],
          modules: [module()],
          names: [atom()]
        ]

  @doc """
  List upstream assets for a target reference.

  Accepted input:

    * `{module, name}` target ref
    * optional `opts`:
      `:include_target`, `:transitive`, `:tags`, `:modules`, `:names`

  Deterministic behavior:

    * default direction is `:upstream`
    * default `include_target` is `false`
    * results are in canonical ref order

  Returns:

    * `{:ok, assets}` where each entry is `%Favn.Asset{}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.Assets.GraphIndex`

  ## Examples

      iex> Favn.upstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec upstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def upstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      GraphIndex.related_assets(
        {module, name},
        opts |> Keyword.put_new(:direction, :upstream) |> Keyword.put_new(:include_target, false)
      )
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  List downstream assets for a target reference.

  Accepted input:

    * `{module, name}` target ref
    * optional `opts`:
      `:include_target`, `:transitive`, `:tags`, `:modules`, `:names`

  Deterministic behavior:

    * default direction is `:downstream`
    * results are in canonical ref order

  Returns:

    * `{:ok, assets}` where each entry is `%Favn.Asset{}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.Assets.GraphIndex`

  ## Examples

      iex> Favn.downstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec downstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def downstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      GraphIndex.related_assets(
        {module, name},
        Keyword.put_new(opts, :direction, :downstream)
      )
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  Build a filtered dependency subgraph for a target reference.

  Accepted input:

    * `{module, name}` target ref
    * optional `opts`:
      `:direction`, `:include_target`, `:transitive`, `:tags`,
      `:modules`, `:names`

  Deterministic behavior:

    * equivalent input and options produce equivalent graph index output

  Returns:

    * `{:ok, %Favn.Assets.GraphIndex{}}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.Assets.GraphIndex`

  ## Examples

      iex> Favn.dependency_graph({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec dependency_graph(asset_ref(), graph_opts()) ::
          {:ok, Favn.Assets.GraphIndex.t()} | {:error, asset_error() | term()}
  def dependency_graph({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      GraphIndex.subgraph({module, name}, opts)
    else
      {:error, :not_asset_module}
    end
  end

  @doc false
  @spec asset_module?(module()) :: boolean()
  def asset_module?(module) when is_atom(module) do
    match?({:ok, _}, Compiler.compile_module_assets(module))
  end

  @doc """
  Build a deterministic execution plan for one or more targets.

  This API returns a run-once plan shape where nodes are deduplicated by
  canonical ref and grouped into topological stages for parallel execution.
  Planning is deterministic:

    * target refs are normalized, deduplicated, and sorted
    * node refs inside each stage are sorted
    * stage number is computed as topological depth from source assets

  Accepted input:

    * one target ref `{module, name}` or a non-empty list of refs
    * `opts`:
      * `dependencies: :all | :none` (default `:all`)
      * `anchor_window: %Favn.Window.Anchor{}` optional anchor for windowed node expansion

  Returns:

    * `{:ok, %Favn.Plan{}}` for valid targets/options
    * `{:error, :empty_targets}` for `[]`
    * `{:error, :invalid_target_ref}` for malformed refs
    * `{:error, :asset_not_found}` when any target ref is unknown
    * `{:error, {:invalid_dependencies_mode, value}}` for unsupported
      dependency mode values

  ## Examples

      iex> Favn.plan_asset_run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

      iex> Favn.plan_asset_run([])
      {:error, :empty_targets}

  ## Output shape

      %Favn.Plan{
        target_refs: [{MyApp.GoldETL, :fact_sales}],
        dependencies: :all,
        topo_order: [
          {MyApp.SourceETL, :raw_orders},
          {MyApp.WarehouseETL, :normalize_orders},
          {MyApp.GoldETL, :fact_sales}
        ],
        stages: [
          [{MyApp.SourceETL, :raw_orders}],
          [{MyApp.WarehouseETL, :normalize_orders}],
          [{MyApp.GoldETL, :fact_sales}]
        ],
        nodes: %{
          {MyApp.WarehouseETL, :normalize_orders} => %{
            ref: {MyApp.WarehouseETL, :normalize_orders},
            upstream: [{MyApp.SourceETL, :raw_orders}],
            downstream: [{MyApp.GoldETL, :fact_sales}],
            stage: 1,
            action: :run
          }
        }
      }
  """
  @spec plan_asset_run(asset_ref() | [asset_ref()], plan_asset_run_opts()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_asset_run(targets, opts \\ []) when is_list(opts) do
    Planner.plan(targets, opts)
  end

  @doc """
  Resolve and plan a code-defined pipeline.

  Pipelines are a composition layer. This API resolves pipeline selectors to
  deterministic target refs and then delegates graph planning to `plan_asset_run/2`.
  """
  @spec plan_pipeline(pipeline_module(), keyword()) :: {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_pipeline(pipeline_module, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, definition} <- Favn.Pipeline.fetch(pipeline_module),
         {:ok, resolution} <- Resolver.resolve(definition, opts) do
      Favn.plan_asset_run(resolution.target_refs,
        dependencies: resolution.dependencies,
        anchor_window: resolution.pipeline_ctx.anchor_window
      )
    end
  end

  @doc """
  Submit an asynchronous run for the given asset.

  This is the low-level asset execution primitive.
  For operator-facing execution, prefer `run_pipeline/2`.

  Accepted input:

    * target ref `{module, name}`
    * `opts`:
      * `dependencies: :all | :none` (default `:all`)
      * `params: map()` (default `%{}`)
      * `anchor_window: %Favn.Window.Anchor{}` optional run anchor for window-aware planning
      * `pipeline_context: map()` optional manual pipeline provenance/context payload
        projected to `ctx.pipeline` and persisted `%Favn.Run{}.pipeline`
        (passed through as provided; unlike `run_pipeline/2`, no schedule normalization
        is applied to manually injected `pipeline_context`)
      * `max_concurrency: pos_integer()` (default from runtime config, fallback `1`)
      * `timeout_ms: pos_integer()` timeout counted from run start
      * `retry: false | true | keyword() | map()` (default from app config, fallback disabled)
        * `max_attempts: pos_integer()` (includes first attempt)
        * `delay_ms: non_neg_integer()` (fixed delay between attempts)
        * `retry_on: [:exception | :exit | :throw | :timeout | :executor_error | :error_return]`

  Deterministic behavior:

    * planning and stage ordering are deterministic for identical inputs
    * runnable refs are admitted in canonical ref order when multiple refs are ready together

  Runtime semantics:

    * returns immediately with a generated `run_id`
    * orchestration is owned by supervised runtime processes
    * independent ready steps may execute in parallel up to `max_concurrency`
    * retries are evaluated per-step and re-admitted under the same bounded concurrency
    * callers can observe progress through `get_run/1`, `list_runs/1`,
      `await_run/2`, and run events

  Asset invocation contract:

    * assets are invoked as public arity-1 functions receiving `ctx`
    * success may return `:ok` or `{:ok, map()}`
    * failure must be `{:error, reason}`

  ## Examples

      iex> Favn.run_asset({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

  Returns:

    * `{:ok, run_id}` when submission succeeds
    * `{:error, reason}` for validation/planning/storage submission failures
  """
  @spec run_asset(asset_ref(), run_opts()) :: {:ok, run_id()} | {:error, term()}
  def run_asset({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    opts =
      opts
      |> Keyword.put_new(:_submit_kind, :asset)
      |> Keyword.put_new(:_submit_ref, {module, name})
      |> normalize_pipeline_context_opt()

    Engine.submit_run({module, name}, opts)
  end

  @doc """
  Submit an asynchronous manual run for a pipeline module.

  Pipeline selection resolves target refs and then delegates dependency planning
  to the existing planner/runtime execution flow.

  This is the primary operator-facing execution entrypoint.
  In production flows where source/output/schedule/window context matters,
  prefer `run_pipeline/2` over direct `run_asset/2`.

  Accepted options:

    * `params: map()` runtime params (available on both `ctx.params` and
      `ctx.pipeline.params` for pipeline-triggered runs)
    * `trigger: map()` trigger metadata exposed through `ctx.pipeline.trigger`
    * `anchor_window: %Favn.Window.Anchor{}` explicit anchor for window-aware planning
    * runtime context exposes `ctx.window` and `ctx.pipeline.anchor_window`
    * runtime context/persisted run pipeline metadata also includes:
      `run_kind`, `resolved_refs`, and `deps`
    * `max_concurrency`, `timeout_ms`, `retry` (same semantics as `run_asset/2`)
  """
  @spec run_pipeline(pipeline_module(), run_pipeline_opts()) ::
          {:ok, run_id()} | {:error, term()}
  def run_pipeline(pipeline_module, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, definition} <- Favn.Pipeline.fetch(pipeline_module),
         {:ok, resolution} <- Resolver.resolve(definition, opts) do
      run_opts =
        opts
        |> Keyword.put(:dependencies, resolution.dependencies)
        |> Keyword.put(:pipeline_context, resolution.pipeline_ctx)
        |> Keyword.put(:_submit_kind, :pipeline)
        |> Keyword.put(:_submit_ref, pipeline_module)
        |> normalize_pipeline_context_opt()

      Engine.submit_run(resolution.target_refs, run_opts)
    end
  end

  @doc """
  Submit one asynchronous backfill run for an asset over a time range.

  Backfill expands the supplied `range` into contiguous anchors, plans once
  across all anchors, unions/deduplicates resulting node keys, and submits one
  run over that deduplicated plan.
  """
  @spec backfill_asset(asset_ref(), backfill_asset_opts()) ::
          {:ok, run_id()} | {:error, term()}
  def backfill_asset({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    dependencies = Keyword.get(opts, :dependencies, :all)

    with {:ok, range, anchor_ranges} <- fetch_backfill_range(opts),
         {:ok, plan} <-
           Favn.plan_asset_run({module, name},
             dependencies: dependencies,
             anchor_ranges: [range]
           ) do
      run_opts =
        opts
        |> drop_backfill_range_opt()
        |> Keyword.put(:dependencies, dependencies)
        |> Keyword.put(:_plan_override, plan)
        |> Keyword.put(:_submit_kind, :backfill_asset)
        |> Keyword.put(:_submit_ref, {module, name})
        |> Keyword.put(:_backfill, %{range: range, anchor_ranges: anchor_ranges})
        |> normalize_pipeline_context_opt()

      Engine.submit_run({module, name}, run_opts)
    end
  end

  @doc """
  Submit one asynchronous backfill run for a pipeline over a time range.

  Pipeline selection resolves first, then planner backfill expansion builds one
  deduplicated node-key plan across all anchors from the provided range.
  """
  @spec backfill_pipeline(pipeline_module(), backfill_pipeline_opts()) ::
          {:ok, run_id()} | {:error, term()}
  def backfill_pipeline(pipeline_module, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, range, anchor_ranges} <- fetch_backfill_range(opts),
         {:ok, definition} <- Favn.Pipeline.fetch(pipeline_module),
         {:ok, resolution} <- Resolver.resolve(definition, opts),
         {:ok, plan} <-
           Favn.plan_asset_run(resolution.target_refs,
             dependencies: resolution.dependencies,
             anchor_ranges: [range]
           ) do
      pipeline_context =
        resolution.pipeline_ctx
        |> Map.put(:run_kind, :pipeline_backfill)
        |> Map.put(:backfill_range, range)
        |> Map.put(:anchor_ranges, anchor_ranges)

      run_opts =
        opts
        |> drop_backfill_range_opt()
        |> Keyword.put(:dependencies, resolution.dependencies)
        |> Keyword.put(:pipeline_context, pipeline_context)
        |> Keyword.put(:_plan_override, plan)
        |> Keyword.put(:_submit_kind, :backfill_pipeline)
        |> Keyword.put(:_submit_ref, pipeline_module)
        |> Keyword.put(:_backfill, %{range: range, anchor_ranges: anchor_ranges})
        |> normalize_pipeline_context_opt()

      Engine.submit_run(resolution.target_refs, run_opts)
    end
  end

  defp normalize_pipeline_context_opt(opts) when is_list(opts) do
    case Keyword.fetch(opts, :pipeline_context) do
      :error ->
        opts

      {:ok, context} ->
        opts |> Keyword.delete(:pipeline_context) |> Keyword.put(:_pipeline_context, context)
    end
  end

  defp fetch_backfill_range(opts) when is_list(opts) do
    case Keyword.fetch(opts, :range) do
      {:ok, %{kind: kind, start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at} = range} ->
        timezone = Map.get(range, :timezone, "Etc/UTC")
        normalized = %{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}

        case Anchor.expand_range(kind, start_at, end_at, timezone: timezone) do
          {:ok, [_ | _] = anchor_ranges} -> {:ok, normalized, anchor_ranges}
          {:ok, []} -> {:error, :empty_backfill_range}
          {:error, _} = error -> error
        end

      {:ok, _invalid} ->
        {:error, :invalid_backfill_range}

      :error ->
        {:error, :backfill_range_required}
    end
  end

  defp drop_backfill_range_opt(opts) when is_list(opts), do: Keyword.delete(opts, :range)

  @doc """
  Evaluate freshness policy for one asset/window against persisted node results.

  Freshness is intentionally a thin policy layer over persisted run state.
  """
  @spec check_asset_freshness(asset_ref(), check_asset_freshness_opts()) ::
          {:ok, map()} | {:error, term()}
  def check_asset_freshness({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    Favn.Freshness.check({module, name}, opts)
  end

  @doc """
  Return missing target windows for an asset over a requested backfill range.
  """
  @spec missing_asset_windows(asset_ref(), backfill_anchor_range(), keyword()) ::
          {:ok, [Favn.Plan.node_key()]} | {:error, term()}
  def missing_asset_windows({module, name}, range, opts \\ [])
      when is_atom(module) and is_atom(name) and is_map(range) and is_list(opts) do
    Favn.Freshness.missing_windows({module, name}, range, opts)
  end

  @doc """
  Request cancellation of a submitted run.

  Returns:

    * `{:ok, :cancelling}` when cancellation request is accepted
    * `{:ok, :cancelled}` when run is already cancelled
    * `{:ok, :already_terminal}` when run already reached a non-cancel terminal status
    * `{:error, :not_found}` when run ID does not exist
    * `{:error, :coordinator_unavailable}` when run appears running but no live coordinator is available
    * `{:error, :timeout_in_progress}` when timeout terminalization is already in progress
  """
  @spec cancel_run(run_id()) ::
          {:ok, :cancelling | :cancelled | :already_terminal}
          | {:error,
             :not_found
             | :invalid_run_id
             | :coordinator_unavailable
             | :timeout_in_progress
             | term()}
  def cancel_run(run_id) do
    Engine.cancel_run(run_id)
  end

  @doc """
  Submit a rerun for a terminal run id.

  Rerun supports two modes:

    * `:resume_from_failure` (default) resumes from incomplete/failed/cancelled/timed-out
      parts while reusing successful completed steps from the source run.
    * `:exact_replay` replays the whole persisted run plan regardless of prior success.

  Common semantics:

    * both modes reuse persisted execution intent from the source run
    * supports source statuses `:ok | :error | :cancelled | :timed_out`
    * returns `{:error, :run_not_terminal}` for still-running source runs
    * returns `{:error, :resume_from_failure_requires_node_key_results}` when
      `:resume_from_failure` is requested for plans containing duplicate refs
      across multiple node keys (window-expanded plans)
    * persists lineage links in the new run (`rerun_of_run_id`, `parent_run_id`, `root_run_id`)

  Options:

    * `mode: :resume_from_failure | :exact_replay` (default `:resume_from_failure`)
    * `reason: term()` optional operator reason persisted on the rerun
  """
  @spec rerun_run(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun_run(run_id, opts \\ [])

  def rerun_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    Engine.rerun_run(run_id, opts)
  end

  def rerun_run(_run_id, _opts), do: {:error, :invalid_run_id}

  @doc """
  Block until one submitted run reaches a terminal state.

  Accepted options:

    * `timeout: non_neg_integer() | :infinity` (default `:infinity`)
    * `poll_interval_ms: pos_integer()` (default `50`)

  Returns:

    * `{:ok, %Favn.Run{status: :ok}}` on successful completion
    * `{:error, %Favn.Run{status: :cancelled}}` when cancellation completes
    * `{:error, %Favn.Run{status: :error}}` when the run fails
    * `{:error, %Favn.Run{status: :timed_out}}` when the run times out
    * `{:error, :not_found}` when the run ID does not exist
    * `{:error, :timeout}` when timeout elapses before terminal state
    * `{:error, reason}` for storage retrieval failures
  """
  @spec await_run(run_id(), await_run_opts()) ::
          {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}
  def await_run(run_id, opts \\ []) when is_list(opts) do
    Engine.await_run(run_id, opts)
  end

  @doc """
  Fetch one run by ID.

  Accepted input:

    * `run_id` as an opaque identifier

  Returns:

    * `{:ok, %Favn.Run{}}` when a run exists
    * `{:error, :not_found}` when no run exists
    * `{:error, :invalid_opts}` for adapter option validation failures
    * `{:error, {:store_error, reason}}` for storage adapter/internal failures

  ## Examples

      iex> Favn.get_run("run_123")
      {:error, :not_found}
  """
  @spec get_run(run_id()) :: {:ok, Favn.Run.t()} | {:error, run_error()}
  def get_run(run_id) do
    Favn.Storage.get_run(run_id)
  end

  @doc """
  List runs.

  Accepted options:

    * `status: :running | :ok | :error | :cancelled | :timed_out`
    * `limit: positive_integer()`

  Deterministic behavior:

    * results are returned newest-first

  Returns:

    * `{:ok, [run]}` where each entry is `%Favn.Run{}`
    * `{:error, :invalid_opts}` for unsupported filters
    * `{:error, {:store_error, reason}}` for storage adapter/internal failures

  ## Examples

      iex> {:ok, runs} = Favn.list_runs()
      iex> is_list(runs)
      true

      iex> {:ok, running_runs} = Favn.list_runs(status: :running)
      iex> is_list(running_runs)
      true
  """
  @spec list_runs(list_runs_opts()) :: {:ok, [Favn.Run.t()]} | {:error, run_error()}
  def list_runs(opts \\ []) when is_list(opts) do
    Favn.Storage.list_runs(opts)
  end

  @doc """
  Subscribe to live events for a single run.

  Accepted input:

    * `run_id` as an opaque identifier

  Delivery scope:

    * events are broadcast on `"favn:run:<run_id>"`
    * delivery is best-effort and observability-only
    * subscription state does not affect execution/persistence semantics
    * each event follows `t:run_event/0` stable schema envelope

  Returns:

    * `:ok` when subscribed
    * `{:error, reason}` when PubSub returns an error

  ## Examples

      iex> Favn.subscribe_run("run_123")
      :ok
  """
  @spec subscribe_run(run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) do
    Events.subscribe_run(run_id)
  end

  @doc """
  Unsubscribe from live events for a single run.

  Accepted input:

    * `run_id` as an opaque identifier

  Returns `:ok`.

  Unsubscribing is observability-only and does not affect run execution,
  persistence, or final status outcomes.

  ## Examples

      iex> Favn.unsubscribe_run("run_123")
      :ok
  """
  @spec unsubscribe_run(run_id()) :: :ok
  def unsubscribe_run(run_id) do
    Events.unsubscribe_run(run_id)
  end
end
