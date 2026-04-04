defmodule Favn do
  @moduledoc """
  Favn is an asset-first ETL/ELT orchestration library for Elixir.

  The public authoring model is centered on assets with explicit lineage metadata,
  deterministic dependency-based execution, and orchestration config provided through runtime context.

  ## Canonical authoring model

  Favn authoring is defined by two canonical contracts:

    * DSL attributes: `@asset`, `@depends`, `@uses`, `@freshness`, `@meta`
    * Asset function shape: `def asset(ctx)`

  Dependencies are used for **ordering and lineage**. Assets materialize externally as part of
  business logic and are not modeled as direct in-memory value passing between asset functions.

  Orchestration configuration remains outside function attributes and is accessible from `ctx`.

  ## Example

      defmodule MyApp.SalesAssets do
        use Favn.Assets

        @asset :extract_orders
        @uses [:warehouse_api]
        @freshness [max_age: :timer.hours(1)]
        @meta [owner: "data-platform", domain: :sales]
        def extract_orders(ctx) do
          source = ctx.config[:source] || :warehouse_api
          _ = source

          # Read source system and materialize externally.
          :ok
        end

        @asset :daily_sales_report
        @depends [:extract_orders]
        @uses [:warehouse]
        @freshness [max_age: :timer.hours(2)]
        @meta [owner: "finance-analytics", tier: :gold]
        def daily_sales_report(ctx) do
          report_date = ctx.runtime[:date]
          _ = report_date

          # Read upstream materializations and write final report externally.
          :ok
        end
      end

  ## What Favn provides

    * asset discovery and registry inspection
    * dependency and upstream graph inspection
    * deterministic run planning and execution
    * run lifecycle APIs (`run`, `await_run`, `cancel_run`, run lookup/listing)
    * live run-event subscriptions for operator tooling

  ## Scope notes

  Favn targets asset-first orchestration. Scheduling and polling concerns are v1 orchestration
  features, not function-level authoring attributes.
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
  Dependency execution mode for `run/2`.

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
  Retry policy for `run/2`.

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
  Options for `run/2`.
  """
  @type run_opts :: [
          dependencies: dependencies_mode(),
          params: map(),
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
  Options for `plan_run/2`.
  """
  @type plan_run_opts :: [
          dependencies: dependencies_mode()
        ]

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
    with {:ok, assets} <- Favn.Registry.list_assets() do
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
    if asset_module?(module) do
      {:ok, module.__favn_assets__()}
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  Fetch a single asset by reference.

  Accepted input:

    * `{module, name}` where both values are atoms

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
      with {:ok, asset} <- Favn.Registry.get_asset({module, name}) do
        {:ok, asset}
      else
        {:error, {:duplicate_asset, _ref}} -> {:error, :asset_not_found}
        {:error, :asset_not_found} -> {:error, :asset_not_found}
      end
    else
      {:error, :not_asset_module}
    end
  end

  @typedoc """
  Direction used by dependency graph inspection APIs.
  """
  @type dependency_direction :: Favn.GraphIndex.direction()

  @typedoc """
  Options for dependency graph inspection APIs.
  """
  @type graph_opts :: [
          direction: dependency_direction(),
          include_target: boolean(),
          transitive: boolean(),
          tags: [Favn.Asset.tag()],
          kinds: [Favn.Asset.kind()],
          modules: [module()],
          names: [atom()]
        ]

  @doc """
  List upstream assets for a target reference.

  Accepted input:

    * `{module, name}` target ref
    * optional `opts`:
      `:include_target`, `:transitive`, `:tags`, `:kinds`, `:modules`, `:names`

  Deterministic behavior:

    * default direction is `:upstream`
    * default `include_target` is `false`
    * results are in canonical ref order

  Returns:

    * `{:ok, assets}` where each entry is `%Favn.Asset{}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.GraphIndex`

  ## Examples

      iex> Favn.upstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec upstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def upstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Favn.GraphIndex.related_assets(
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
      `:include_target`, `:transitive`, `:tags`, `:kinds`, `:modules`, `:names`

  Deterministic behavior:

    * default direction is `:downstream`
    * results are in canonical ref order

  Returns:

    * `{:ok, assets}` where each entry is `%Favn.Asset{}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.GraphIndex`

  ## Examples

      iex> Favn.downstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec downstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def downstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Favn.GraphIndex.related_assets(
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
      `:direction`, `:include_target`, `:transitive`, `:tags`, `:kinds`,
      `:modules`, `:names`

  Deterministic behavior:

    * equivalent input and options produce equivalent graph index output

  Returns:

    * `{:ok, %Favn.GraphIndex{}}`
    * `{:error, :not_asset_module}` for invalid target modules
    * graph/filter validation errors forwarded from `Favn.GraphIndex`

  ## Examples

      iex> Favn.dependency_graph({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}
  """
  @spec dependency_graph(asset_ref(), graph_opts()) ::
          {:ok, Favn.GraphIndex.t()} | {:error, asset_error() | term()}
  def dependency_graph({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Favn.GraphIndex.subgraph({module, name}, opts)
    else
      {:error, :not_asset_module}
    end
  end

  @doc false
  @spec asset_module?(module()) :: boolean()
  def asset_module?(module) when is_atom(module) do
    function_exported?(module, :__favn_asset_module__, 0) and
      function_exported?(module, :__favn_assets__, 0) and
      module.__favn_asset_module__() == true
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
    * `opts` with `dependencies: :all | :none` (default `:all`)

  Returns:

    * `{:ok, %Favn.Plan{}}` for valid targets/options
    * `{:error, :empty_targets}` for `[]`
    * `{:error, :invalid_target_ref}` for malformed refs
    * `{:error, :asset_not_found}` when any target ref is unknown
    * `{:error, {:invalid_dependencies_mode, value}}` for unsupported
      dependency mode values

  ## Examples

      iex> Favn.plan_run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

      iex> Favn.plan_run([])
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
  @spec plan_run(asset_ref() | [asset_ref()], plan_run_opts()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_run(targets, opts \\ []) when is_list(opts) do
    Favn.Planner.plan(targets, opts)
  end

  @doc """
  Submit an asynchronous run for the given asset.

  Accepted input:

    * target ref `{module, name}`
    * `opts`:
      * `dependencies: :all | :none` (default `:all`)
      * `params: map()` (default `%{}`)
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

    * canonical authoring contract is `def asset(ctx)`
    * dependencies provide ordering/lineage and assets materialize externally
    * return values are interpreted by runtime/executor boundaries; failures are surfaced as run/step errors

  ## Examples

      iex> Favn.run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

  Returns:

    * `{:ok, run_id}` when submission succeeds
    * `{:error, reason}` for validation/planning/storage submission failures
  """
  @spec run(asset_ref(), run_opts()) :: {:ok, run_id()} | {:error, term()}
  def run({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    Favn.Runtime.Engine.submit_run({module, name}, opts)
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
    Favn.Runtime.Engine.cancel_run(run_id)
  end

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
    Favn.Runtime.Engine.await_run(run_id, opts)
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
    Favn.Runtime.Events.subscribe_run(run_id)
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
    Favn.Runtime.Events.unsubscribe_run(run_id)
  end
end
