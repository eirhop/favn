defmodule Flux do
  @moduledoc """
  Flux is a library for defining, inspecting, and orchestrating asset-based workflows in Elixir.

  It is built around the idea that each step in a workflow should be expressed as a normal,
  business-oriented Elixir function, while Flux adds the metadata and orchestration needed to
  discover assets, understand their dependencies, and execute them as part of a run.

  Flux is intended to be the core library behind orchestrator applications, operator dashboards,
  scheduling systems, and other tools that need a stable API for working with assets and runs.

  ## What Flux provides

  Flux is designed for applications that need to:

    * define assets as plain Elixir functions
    * attach documentation and metadata directly to those assets through @asset annotations
    * declare dependencies between assets
    * inspect assets and their relationships at runtime
    * execute one asset or a dependency-derived workflow
    * inspect active and completed runs
    * subscribe to live events from a run

  ## Core concepts

  ### Assets

  An asset is a function that represents a meaningful unit of work in a workflow, such as
  extracting data, transforming a dataset, or producing a modeled output.

  Assets are intended to be authored in modules that `use Flux.Assets`. At compile time,
  Flux will collect metadata such as:

    * asset name
    * documentation
    * dependency references
    * source file and line
    * kind
    * tags

  This keeps the workflow definition close to the business logic while still allowing Flux to
  introspect and orchestrate the workflow later.

  ### Dependencies

  Assets can depend on other assets. Flux uses those dependency declarations to derive the
  execution graph automatically.

  In practice, this means users generally do not need to manually define pipelines. When an
  asset is run, Flux can compute the dependency graph for the requested target and form the
  execution pipeline from that graph.

  ### Runs

  A run is a single execution of a target asset.

  Depending on the runtime options, a run may include:

    * only the requested asset
    * the requested asset and its upstream dependencies

  Runs are intended to be observable and inspectable so that orchestrator applications can show
  progress, history, and live operational state.

  ### Live events

  Flux is intended to expose structured run events so that consumers can subscribe to the
  progress of an active run.

  This is designed for use cases such as:

    * live operator dashboards
    * streaming logs and status in a web UI
    * audit trails
    * hooks into telemetry or other observability systems

  ## Authoring assets

  Assets are defined in normal modules using `Flux.Assets`.

  A simplified example:

      defmodule MyApp.SalesETL do
        use Flux.Assets

        @doc "Extract raw orders from the sales source"
        @asset true
        def extract_orders do
          [%{id: 1, total: 100}]
        end

        @doc "Normalize extracted orders"
        @asset depends_on: [:extract_orders]
        def normalize_orders(orders) do
          Enum.map(orders, fn order ->
            Map.put(order, :normalized, true)
          end)
        end
      end

  Cross-module dependencies are also expected to be supported:

      defmodule MyApp.GoldETL do
        use Flux.Assets

        alias MyApp.SalesETL

        @doc "Build the fact table for sales"
        @asset depends_on: [{SalesETL, :normalize_orders}]
        def fact_sales(normalized_orders) do
          %{rows: normalized_orders}
        end
      end

  In this model, workflow structure is derived from the dependencies rather than from a
  separately maintained pipeline definition.

  ## Using the library

  `Flux` is the main public entrypoint for the library.

  Typical usage from an orchestrator app or operator tool:

      Flux.list_assets()

      Flux.list_assets(MyApp.SalesETL)

      Flux.get_asset({MyApp.SalesETL, :normalize_orders})

      Flux.run({MyApp.GoldETL, :fact_sales})

      Flux.run({MyApp.GoldETL, :fact_sales}, dependencies: :none)

      Flux.get_run(run_id)

      Flux.list_runs()

      Flux.list_runs(status: :running)

      Flux.subscribe_run(run_id)

  ## Public API responsibilities

  The intended user-facing responsibilities of this module are:

    * asset discovery
    * asset inspection
    * run execution
    * run inspection
    * listing historical runs
    * subscribing to live run events

  This module should stay a thin public facade. Core implementation details should live in
  dedicated modules underneath.

  ## Design goals

  Flux aims to keep workflow code close to the business domain while still providing the
  operational features needed by orchestration and observability tooling.

  In practice, that means:

    * business logic remains plain Elixir
    * documentation lives close to the asset definition
    * dependencies are simple and explicit
    * orchestration is derived from asset metadata
    * the public API stays small and stable
    * implementation details are pushed into focused modules

  ## Status

  Flux is currently being rebuilt from the asset authoring layer upward.

  The current direction is:

    * `Flux` defines the intended public API first
    * asset authoring and compile-time metadata collection now back module-level inspection APIs
    * dependencies declared by assets will define the execution graph
    * runtime execution, eventing, and storage will be implemented underneath that API

  This module therefore acts as both:

    * the public contract for users of the library
    * the roadmap boundary for what still needs to be implemented underneath

  The first concrete deliverable is intentionally small and now available: define assets,
  inspect assets for a module, and fetch assets by canonical reference through this public
  facade before building planning and runtime layers.

  Global asset discovery now starts from an explicit registry scope configured through
  `config :flux, asset_modules: [...]`. Flux uses that configured module list to build a
  global asset catalog without scanning arbitrary loaded modules in the VM, and it loads
  that catalog into memory during application startup for fast read-heavy lookups.

  ## Roadmap

  The planned implementation work is roughly:

    1. asset authoring DSL with `use Flux.Assets` and `@asset` - done
    2. canonical asset metadata and asset references - done
    3. per-module asset introspection through `Flux` - done
    4. global registry and configured asset discovery - done
    5. startup registry loading and caching - done
    6. dependency resolution and graph construction
    7. execution planning
    8. run model and in-memory execution
    9. run storage and retrieval
    10. live run event subscriptions

  """

  @typedoc """
  Canonical reference to an asset.

  The public API should consistently use `{module, asset_name}` references.
  """
  @type asset_ref :: Flux.Ref.t()

  @typedoc """
  Canonical asset metadata returned by Flux inspection APIs.
  """
  @type asset :: Flux.Asset.t()

  @typedoc """
  Asset inspection errors returned by lookup APIs.
  """
  @type asset_error :: :not_asset_module | :asset_not_found

  @typedoc """
  Identifier for a single run.

  The exact representation is intentionally left open for now. A future
  implementation may use UUIDs, monotonic integers, or another stable ID type.
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
          status: :pending | :running | :ok | :error,
          limit: pos_integer()
        ]

  @typedoc """
  Options for `run/2`.
  """
  @type run_opts :: [
          dependencies: dependencies_mode()
        ]

  @doc """
  List all registered assets.

  This is the main discovery function for orchestrator applications that need
  to render an asset catalog, search UI, or operator dashboard.

  Global discovery is scoped to modules configured under
  `config :flux, asset_modules: [...]`.

  ## Examples

      iex> Flux.list_assets()
      {:ok, []}

  ## TODO

    * Keep `Flux.Registry` as the canonical global discovery layer
    * Keep startup-loaded caching read-only unless a real dynamic loading use case appears
    * Expand discovery beyond explicit module lists only when a concrete use case appears
    * Define stable ordering for returned assets
    * Consider filtering and pagination later
  """
  @spec list_assets() :: {:ok, [asset()]} | {:error, term()}
  def list_assets do
    Flux.Registry.list_assets()
  end

  @doc """
  List all assets for a specific module.

  This is a targeted variant of `list_assets/0` and is useful when an
  orchestrator wants to browse one asset module at a time.

  ## Examples

      iex> Flux.list_assets(Unknown.Module)
      {:error, :not_asset_module}

  ## TODO

    * Keep this backed by module-level introspection as the targeted inspection API
    * Consider whether richer filtering belongs here or in a registry layer later
  """
  @spec list_assets(module()) :: {:ok, [asset()]} | {:error, asset_error()}
  def list_assets(module) when is_atom(module) do
    if asset_module?(module) do
      {:ok, module.__flux_assets__()}
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  Fetch a single asset by reference.

  This should return the full asset metadata, including documentation,
  dependencies, source file, line number, kind, tags, and other compile-time
  metadata captured for the asset.

  ## Examples

      iex> Flux.get_asset({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}

  ## TODO

    * Keep the registry-backed lookup as the default global path for canonical refs
    * Preserve module-level metadata as the source of truth underneath the registry
    * Add richer dependency validation once graph construction is introduced
  """
  @spec get_asset(asset_ref()) :: {:ok, asset()} | {:error, asset_error()}
  def get_asset({module, name}) when is_atom(module) and is_atom(name) do
    if asset_module?(module) do
      with {:ok, asset} <- Flux.Registry.get_asset({module, name}) do
        {:ok, asset}
      else
        {:error, {:duplicate_asset, _ref}} -> {:error, :asset_not_found}
        {:error, :asset_not_found} -> {:error, :asset_not_found}
      end
    else
      {:error, :not_asset_module}
    end
  end

  @doc false
  @spec asset_module?(module()) :: boolean()
  def asset_module?(module) when is_atom(module) do
    function_exported?(module, :__flux_asset_module__, 0) and
      function_exported?(module, :__flux_assets__, 0) and
      module.__flux_asset_module__() == true
  end

  @doc """
  Start a run for the given asset.

  By default, a run should include the full upstream dependency chain so that
  asset dependencies automatically form the execution pipeline.

  ## Examples

      iex> Flux.run({MyApp.GoldETL, :fact_sales})
      ** (RuntimeError) TODO: implement Flux.run/2

      iex> Flux.run({MyApp.GoldETL, :fact_sales}, dependencies: :none)
      ** (RuntimeError) TODO: implement Flux.run/2

  ## Expected implementation flow

    1. Resolve the target asset
    2. Build the dependency graph or subgraph
    3. Produce an execution plan
    4. Create and persist a run record
    5. Emit run and asset lifecycle events
    6. Execute the plan

  ## TODO

    * Implement after asset authoring, introspection, and dependency graph construction exist
    * Delegate to `Flux.Runner.run/2`
    * Define the return contract, likely `{:ok, run}` or `{:error, reason}`
    * Support `dependencies: :all | :none` first
    * Add more advanced inclusion or exclusion rules only if truly needed
  """
  @spec run(asset_ref(), run_opts()) :: term()
  def run({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    todo!("Flux.run/2")
  end

  @doc """
  Fetch one run by ID.

  This should return the current or final run state, including status,
  timestamps, execution details, errors, and possibly recent emitted events.

  ## Examples

      iex> Flux.get_run("run_123")
      ** (RuntimeError) TODO: implement Flux.get_run/1

  ## TODO

    * Implement after the run model and runner exist
    * Delegate to `Flux.Storage` or a dedicated run store
    * Define the canonical run shape
    * Decide how much execution detail belongs in the run record itself
  """
  @spec get_run(run_id()) :: term()
  def get_run(run_id) do
    _ = run_id
    todo!("Flux.get_run/1")
  end

  @doc """
  List runs.

  This is intended for orchestrator screens that need to show historical and
  currently executing runs.

  ## Examples

      iex> Flux.list_runs()
      ** (RuntimeError) TODO: implement Flux.list_runs/1

      iex> Flux.list_runs(status: :running)
      ** (RuntimeError) TODO: implement Flux.list_runs/1

  ## TODO

    * Implement after the run model and storage layer exist
    * Delegate to `Flux.Storage`
    * Define default ordering, likely newest first
    * Consider pagination rather than large unbounded lists
  """
  @spec list_runs(list_runs_opts()) :: [term()]
  def list_runs(opts \\ []) when is_list(opts) do
    todo!("Flux.list_runs/1")
  end

  @doc """
  Subscribe to live events for a single run.

  This function is intentionally specific to runs so that the public API stays
  explicit and can later grow with other subscription types if needed.

  The expected event model is structured run events rather than raw log lines.

  ## Examples

      iex> Flux.subscribe_run("run_123")
      ** (RuntimeError) TODO: implement Flux.subscribe_run/1

  ## TODO

    * Implement after run execution exists
    * Delegate to `Flux.Events`
    * Decide whether to use Phoenix.PubSub directly or hide it behind an adapter
    * Define the event envelope and topic naming convention
    * Return `:ok` or a subscription handle if needed later
  """
  @spec subscribe_run(run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) do
    _ = run_id
    todo!("Flux.subscribe_run/1")
  end

  @doc """
  Unsubscribe from live events for a single run.

  ## Examples

      iex> Flux.unsubscribe_run("run_123")
      ** (RuntimeError) TODO: implement Flux.unsubscribe_run/1

  ## TODO

    * Implement after run execution exists
    * Delegate to `Flux.Events`
    * Mirror the behavior and return contract of `subscribe_run/1`
  """
  @spec unsubscribe_run(run_id()) :: :ok | {:error, term()}
  def unsubscribe_run(run_id) do
    _ = run_id
    todo!("Flux.unsubscribe_run/1")
  end

  defp todo!(function_name) do
    raise RuntimeError, "TODO: implement #{function_name}"
  end
end
