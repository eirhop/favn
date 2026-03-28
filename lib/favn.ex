defmodule Favn do
  @moduledoc """
  Favn is a library for defining, inspecting, and orchestrating asset-based workflows in Elixir.

  It is built around the idea that each step in a workflow should be expressed as a normal,
  business-oriented Elixir function, while Favn adds the metadata and orchestration needed to
  discover assets, understand their dependencies, and execute them as part of a run.

  Favn is intended to be the core library behind orchestrator applications, operator dashboards,
  scheduling systems, and other tools that need a stable API for working with assets and runs.

  ## What Favn provides

  Favn is designed for applications that need to:

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

  Assets are intended to be authored in modules that `use Favn.Assets`. At compile time,
  Favn will collect metadata such as:

    * asset name
    * documentation
    * dependency references
    * source file and line
    * kind
    * tags

  This keeps the workflow definition close to the business logic while still allowing Favn to
  introspect and orchestrate the workflow later.

  ### Dependencies

  Assets can depend on other assets. Favn uses those dependency declarations to derive the
  execution graph automatically.

  Favn models those relationships as a directed acyclic graph (DAG), not as a strict tree.
  Shared upstream assets are therefore allowed, cycles are rejected, and a later runtime
  planner can ensure an asset runs at most once in a single run even when multiple downstream
  assets depend on it.

  In practice, this means users generally do not need to manually define pipelines. When an
  asset is run, Favn can compute the dependency graph for the requested target and form the
  execution pipeline from that graph.

  ### Runs

  A run is a single execution of a target asset.

  Depending on the runtime options, a run may include:

    * only the requested asset
    * the requested asset and its upstream dependencies

  Runs are intended to be observable and inspectable so that orchestrator applications can show
  progress, history, and live operational state.

  ### Live events

  Favn is intended to expose structured run events so that consumers can subscribe to the
  progress of an active run.

  This is designed for use cases such as:

    * live operator dashboards
    * streaming logs and status in a web UI
    * audit trails
    * hooks into telemetry or other observability systems

  ## Authoring assets

  Assets are defined in normal modules using `Favn.Assets`.

  A simplified example:

      defmodule MyApp.SalesETL do
        use Favn.Assets

        @doc "Extract raw orders from the sales source"
        @asset true
        def extract_orders(_ctx, _deps) do
          {:ok, %Favn.Asset.Output{output: [%{id: 1, total: 100}], meta: %{source: :sales}}}
        end

        @doc "Normalize extracted orders"
        @asset depends_on: [:extract_orders]
        def normalize_orders(_ctx, deps) do
          orders = Map.fetch!(deps, {__MODULE__, :extract_orders})

          normalized =
            Enum.map(orders, fn order ->
              Map.put(order, :normalized, true)
            end)

          {:ok, %Favn.Asset.Output{output: normalized, meta: %{normalized_count: length(normalized)}}}
        end
      end

  Cross-module dependencies are also expected to be supported:

      defmodule MyApp.GoldETL do
        use Favn.Assets

        alias MyApp.SalesETL

        @doc "Build the fact table for sales"
        @asset depends_on: [{SalesETL, :normalize_orders}]
        def fact_sales(_ctx, deps) do
          normalized_orders = Map.fetch!(deps, {SalesETL, :normalize_orders})
          {:ok, %Favn.Asset.Output{output: %{rows: normalized_orders}}}
        end
      end

  In this model, workflow structure is derived from the dependencies rather than from a
  separately maintained pipeline definition.

  ## Using the library

  `Favn` is the main public entrypoint for the library.

  Typical usage from an orchestrator app or operator tool:

      Favn.list_assets()

      Favn.list_assets(MyApp.SalesETL)

      Favn.get_asset({MyApp.SalesETL, :normalize_orders})

      Favn.upstream_assets({MyApp.GoldETL, :fact_sales})

      Favn.dependency_graph({MyApp.GoldETL, :fact_sales}, tags: [:warehouse])

      Favn.run({MyApp.GoldETL, :fact_sales})

      Favn.run({MyApp.GoldETL, :fact_sales}, dependencies: :none)

      Favn.get_run(run_id)

      Favn.list_runs()

      Favn.list_runs(status: :running)

      Favn.subscribe_run(run_id)

  ## Setup in a host application

  Favn is an OTP application, not a standalone server you start with a
  dedicated `favn` command.

  A consumer application typically sets Favn up in four steps:

    1. add `:favn` as a dependency in `mix.exs`
    2. define one or more asset modules with `use Favn.Assets`
    3. register those modules under `config :favn, asset_modules: [...]`
    4. start the host application normally

  Favn is not published on Hex yet, so it should be installed directly
  from the repository. A minimal dependency declaration looks like this:

      defp deps do
        [
          {:favn, git: "https://github.com/eirhop/favn.git", tag: "v0.1.1"}
        ]
      end

  Once Hex publishing exists, the dependency can move to a normal versioned
  package declaration:

      defp deps do
        [
          {:favn, "~> 0.1.1"}
        ]
      end

  Asset modules usually live in the host application's normal source tree,
  for example under `lib/my_app/`.

  Once those modules exist, register them in the host application's config.
  For most projects that means `config/config.exs`, optionally overridden from
  environment-specific files such as `config/dev.exs`, `config/test.exs`, or
  `config/runtime.exs` if the application needs environment-dependent module
  lists.

      import Config

      config :favn,
        asset_modules: [
          MyApp.SalesETL,
          MyApp.GoldETL
        ]

  The configured module list is the global discovery scope used by
  `Favn.list_assets/0` and `Favn.get_asset/1`.

  Run event subscriptions use Phoenix PubSub and default to `Favn.PubSub`.
  Host applications can override the pubsub server name:

      import Config

      config :favn,
        pubsub_name: MyApp.PubSub

  ## Starting Favn

  There is no separate Favn server process that operators start manually.

  When the host application boots, Mix starts the `:favn` application as a
  dependency, and `Favn.Application` loads the configured asset registry during
  application startup.

  In practice, that means the right startup command is the normal startup
  command for the host application:

    * `iex -S mix` for interactive local development
    * `mix run --no-halt` for a long-running non-interactive OTP process
    * framework-specific commands such as `mix phx.server` when Favn is used
      inside a Phoenix application

  If the host application is already running under releases, supervision, or a
  framework entrypoint, Favn starts automatically as part of that boot process.

  ## Configuration lifecycle

  The global asset registry is loaded from application config during startup and
  then kept in memory for fast lookups. Favn also builds a global dependency
  graph index during startup from that same canonical asset catalog.

  The graph index is intended to stay read-only for the lifetime of the booted
  node and supports DAG validation, upstream and downstream inspection,
  transitive dependency queries, and deterministic topological ordering for
  later execution planning.

  That means changes to `config :favn, asset_modules: [...]` are not picked up
  automatically by an already-running node. In normal usage, update the config
  and restart the host application so Favn reloads the configured registry and
  dependency graph during boot.

  ## Public API responsibilities

  The intended user-facing responsibilities of this module are:

    * asset discovery
    * asset inspection
    * dependency graph inspection
    * run execution
    * run inspection
    * listing historical runs
    * subscribing to live run events

  This module should stay a thin public facade. Core implementation details should live in
  dedicated modules underneath.

  ## Design goals

  Favn aims to keep workflow code close to the business domain while still providing the
  operational features needed by orchestration and observability tooling.

  In practice, that means:

    * business logic remains plain Elixir
    * documentation lives close to the asset definition
    * dependencies are simple and explicit
    * orchestration is derived from asset metadata
    * the public API stays small and stable
    * implementation details are pushed into focused modules

  ## Documentation policy

  Documentation ownership is split intentionally:

    * `README.md` is canonical for release status, roadmap planning, and the
      feature/limitation matrix.
    * `Favn` moduledoc is canonical for API behavior, contracts, and examples.

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
          status: :running | :ok | :error,
          limit: pos_integer()
        ]

  @typedoc """
  Run retrieval/listing errors returned by storage-backed APIs.
  """
  @type run_error :: :not_found | :invalid_opts | {:store_error, term()}

  @typedoc """
  Options for `run/2`.
  """
  @type run_opts :: [
          dependencies: dependencies_mode(),
          params: map()
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
  Start a run for the given asset.

  Accepted input:

    * target ref `{module, name}`
    * `opts`:
      * `dependencies: :all | :none` (default `:all`)
      * `params: map()` (default `%{}`)

  Deterministic behavior:

    * planning and stage ordering are deterministic for identical inputs
    * runnable refs are selected in canonical ref order

  Runtime semantics:

    * orchestration is owned by an internal run-scoped coordinator process
    * step invocation happens through an isolated executor boundary
    * first asset failure halts the run and sets `run.status` to `:error`
    * asset failures populate both `run.error` and `run.asset_results[ref].error`
    * unresolved pending/ready steps are finalized explicitly when a run fails
    * terminal result persistence is attempted even if execution failed
    * run events are best-effort observability and do not affect correctness

  Asset invocation contract:

    * assets are invoked as `def asset(ctx, deps)`
    * success must be `{:ok, %Favn.Asset.Output{}}`
    * failure must be `{:error, reason}`

  ## Examples

      iex> Favn.run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

  Returns:

    * `{:ok, %Favn.Run{status: :ok}}` on success
    * `{:error, %Favn.Run{status: :error}}` for execution failures
    * `{:error, reason}` for preflight planning/storage validation failures
  """
  @spec run(asset_ref(), run_opts()) :: {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}
  def run({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    Favn.Runtime.Runner.run({module, name}, opts)
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

    * `status: :running | :ok | :error`
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
