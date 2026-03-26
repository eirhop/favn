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

  Flux models those relationships as a directed acyclic graph (DAG), not as a strict tree.
  Shared upstream assets are therefore allowed, cycles are rejected, and a later runtime
  planner can ensure an asset runs at most once in a single run even when multiple downstream
  assets depend on it.

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
        def extract_orders(_ctx, _deps) do
          {:ok, %Flux.Asset.Output{output: [%{id: 1, total: 100}], meta: %{source: :sales}}}
        end

        @doc "Normalize extracted orders"
        @asset depends_on: [:extract_orders]
        def normalize_orders(_ctx, deps) do
          orders = Map.fetch!(deps, {__MODULE__, :extract_orders})

          normalized =
            Enum.map(orders, fn order ->
              Map.put(order, :normalized, true)
            end)

          {:ok, %Flux.Asset.Output{output: normalized, meta: %{normalized_count: length(normalized)}}}
        end
      end

  Cross-module dependencies are also expected to be supported:

      defmodule MyApp.GoldETL do
        use Flux.Assets

        alias MyApp.SalesETL

        @doc "Build the fact table for sales"
        @asset depends_on: [{SalesETL, :normalize_orders}]
        def fact_sales(_ctx, deps) do
          normalized_orders = Map.fetch!(deps, {SalesETL, :normalize_orders})
          {:ok, %Flux.Asset.Output{output: %{rows: normalized_orders}}}
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

      Flux.upstream_assets({MyApp.GoldETL, :fact_sales})

      Flux.dependency_graph({MyApp.GoldETL, :fact_sales}, tags: [:warehouse])

      Flux.run({MyApp.GoldETL, :fact_sales})

      Flux.run({MyApp.GoldETL, :fact_sales}, dependencies: :none)

      Flux.get_run(run_id)

      Flux.list_runs()

      Flux.list_runs(status: :running)

      Flux.subscribe_run(run_id)

  ## Setup in a host application

  Flux is an OTP application, not a standalone server you start with a
  dedicated `flux` command.

  A consumer application typically sets Flux up in four steps:

    1. add `:flux` as a dependency in `mix.exs`
    2. define one or more asset modules with `use Flux.Assets`
    3. register those modules under `config :flux, asset_modules: [...]`
    4. start the host application normally

  Flux is not published on Hex yet, so today it should be installed directly
  from the repository. A minimal dependency declaration looks like this:

      defp deps do
        [
          {:flux, git: "https://github.com/eirhop/flux.git", branch: "main"}
        ]
      end

  Once Hex publishing exists, the dependency can move to a normal versioned
  package declaration:

      defp deps do
        [
          {:flux, "~> 0.1.0"}
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

      config :flux,
        asset_modules: [
          MyApp.SalesETL,
          MyApp.GoldETL
        ]

  The configured module list is the global discovery scope used by
  `Flux.list_assets/0` and `Flux.get_asset/1`.

  Run event subscriptions use Phoenix PubSub and default to `Flux.PubSub`.
  Host applications can override the pubsub server name:

      import Config

      config :flux,
        pubsub_name: MyApp.PubSub

  ## Starting Flux

  There is no separate Flux server process that operators start manually.

  When the host application boots, Mix starts the `:flux` application as a
  dependency, and `Flux.Application` loads the configured asset registry during
  application startup.

  In practice, that means the right startup command is the normal startup
  command for the host application:

    * `iex -S mix` for interactive local development
    * `mix run --no-halt` for a long-running non-interactive OTP process
    * framework-specific commands such as `mix phx.server` when Flux is used
      inside a Phoenix application

  If the host application is already running under releases, supervision, or a
  framework entrypoint, Flux starts automatically as part of that boot process.

  ## Configuration lifecycle

  The global asset registry is loaded from application config during startup and
  then kept in memory for fast lookups. Flux also builds a global dependency
  graph index during startup from that same canonical asset catalog.

  The graph index is intended to stay read-only for the lifetime of the booted
  node and supports DAG validation, upstream and downstream inspection,
  transitive dependency queries, and deterministic topological ordering for
  later execution planning.

  That means changes to `config :flux, asset_modules: [...]` are not picked up
  automatically by an already-running node. In normal usage, update the config
  and restart the host application so Flux reloads the configured registry and
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

  Flux aims to keep workflow code close to the business domain while still providing the
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
    * `Flux` moduledoc is canonical for API behavior, contracts, and examples.

  ## Status

  Flux is currently being rebuilt from the asset authoring layer upward.

  The current direction is:

    * `Flux` defines the intended public API first
    * asset authoring and compile-time metadata collection now back module-level inspection APIs
    * dependencies declared by assets now build a startup-loaded global DAG index
    * runtime execution, eventing, and storage will be implemented underneath that API

  This module therefore acts as both:

    * the public contract for users of the library
    * the roadmap boundary for what still needs to be implemented underneath

  The first concrete deliverable is intentionally small and now available: define assets,
  inspect assets for a module, and fetch assets by canonical reference through this public
  facade before building planning and runtime layers.

  Global asset discovery now starts from an explicit registry scope configured through
  `config :flux, asset_modules: [...]` in the host application. Flux uses that configured
  module list to build a global asset catalog without scanning arbitrary loaded modules in
  the VM, and it loads that catalog into memory during application startup for fast
  read-heavy lookups.

  ## Roadmap

  The planned implementation work is roughly:

    1. asset authoring DSL with `use Flux.Assets` and `@asset` - done
    2. canonical asset metadata and asset references - done
    3. per-module asset introspection through `Flux` - done
    4. global registry and configured asset discovery - done
    5. startup registry loading and caching - done
    6. dependency resolution and graph construction - done
    7. graph inspection queries - done
    8. execution planning - done
    9. run model and in-memory execution - done
    10. run storage and retrieval - done
    11. live run event subscriptions - done
    12. pre-release refactor pass for shared helpers and consistent module patterns
    13. pre-release documentation and test hardening for v0.1
    14. storage adapter boundary for memory/SQLite/Postgres and third-party plugins

  ## v0.1 release focus

  In addition to finishing the remaining runtime APIs, the first release should include a
  deliberate quality pass over internals, docs, and tests so the public facade stays small and
  predictable as usage grows.

  The v0.1 closeout checklist is:

    * harden run lifecycle APIs (`get_run/1`, `list_runs/1`, `subscribe_run/1`, `unsubscribe_run/1`)
    * refactor reusable logic into small shared helpers where duplication exists across modules
    * align coding patterns across modules (naming, return shapes, and error conventions)
    * tighten and refresh user-facing docs in this module and related public interfaces
    * consolidate test fixtures so one canonical test asset module set is reused across suites
    * keep tests deterministic and focused on public contracts

  Production deployment note:

  Flux is expected to run on multiple BEAM nodes in clustered environments (for example
  Kubernetes). The in-memory run store is intentionally node-local and non-durable for
  development. Production and multi-node consistency are expected to use durable shared
  adapters (Postgres first, with SQLite and third-party adapters as follow-up options).

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

  @typedoc """
  Direction used by dependency graph inspection APIs.
  """
  @type dependency_direction :: Flux.GraphIndex.direction()

  @typedoc """
  Options for dependency graph inspection APIs.
  """
  @type graph_opts :: [
          direction: dependency_direction(),
          include_target: boolean(),
          transitive: boolean(),
          tags: [Flux.Asset.tag()],
          kinds: [Flux.Asset.kind()],
          modules: [module()],
          names: [atom()]
        ]

  @doc """
  List upstream assets for a target reference.

  This query delegates to the startup-built global DAG index and returns asset
  metadata rather than bare refs so operator tooling can inspect tags, docs,
  source locations, and kinds together with dependency shape.

  ## Examples

      iex> Flux.upstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}

  ## TODO

    * Keep this backed by `Flux.GraphIndex.related_assets/2`
    * Keep the default query transitive so planners and UIs get the full closure
    * Add richer view-specific formatting later rather than here
  """
  @spec upstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def upstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Flux.GraphIndex.related_assets(
        {module, name},
        opts |> Keyword.put_new(:direction, :upstream) |> Keyword.put_new(:include_target, false)
      )
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  List downstream assets for a target reference.

  ## Examples

      iex> Flux.downstream_assets({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}

  ## TODO

    * Keep this backed by `Flux.GraphIndex.related_assets/2`
    * Support immediate and transitive traversal through `transitive: false | true`
    * Keep filtering in the graph layer rather than duplicating it in the facade
  """
  @spec downstream_assets(asset_ref(), graph_opts()) ::
          {:ok, [asset()]} | {:error, asset_error() | term()}
  def downstream_assets({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Flux.GraphIndex.related_assets(
        {module, name},
        Keyword.put_new(opts, :direction, :downstream)
      )
    else
      {:error, :not_asset_module}
    end
  end

  @doc """
  Build a filtered dependency subgraph for a target reference.

  This returns another `%Flux.GraphIndex{}` limited to the selected asset set,
  which keeps graph queries and planner inputs on one canonical shape.

  ## Examples

      iex> Flux.dependency_graph({Unknown.Module, :normalize_orders})
      {:error, :not_asset_module}

  ## TODO

    * Keep this backed by `Flux.GraphIndex.subgraph/2`
    * Use this as the input boundary for execution planning
    * Extend filtering only when real UI or planner needs appear
  """
  @spec dependency_graph(asset_ref(), graph_opts()) ::
          {:ok, Flux.GraphIndex.t()} | {:error, asset_error() | term()}
  def dependency_graph({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    if asset_module?(module) do
      Flux.GraphIndex.subgraph({module, name}, opts)
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
  Build a deterministic execution plan for one or more targets.

  This API returns a run-once plan shape where nodes are deduplicated by
  canonical ref and grouped into topological stages for parallel execution.
  Planning is deterministic:

    * target refs are normalized, deduplicated, and sorted
    * node refs inside each stage are sorted
    * stage number is computed as topological depth from source assets

  ## Examples

      iex> Flux.plan_run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

      iex> Flux.plan_run([])
      {:error, :empty_targets}

  ## Output shape

      %Flux.Plan{
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

  ## TODO

    * Keep this planner deterministic and graph-derived
    * Keep plan nodes deduplicated by canonical ref
    * Add freshness-aware actions after run storage exists
    * Reuse planner normalization and sorting helpers from one shared internal module
  """
  @spec plan_run(asset_ref() | [asset_ref()], plan_run_opts()) ::
          {:ok, Flux.Plan.t()} | {:error, term()}
  def plan_run(targets, opts \\ []) when is_list(opts) do
    Flux.Planner.plan(targets, opts)
  end

  @doc """
  Start a run for the given asset.

  By default, a run should include the full upstream dependency chain so that
  asset dependencies automatically form the execution pipeline.

  Asset invocation and return contract for this first runner:

    * assets are invoked as `def asset(ctx, deps)`
    * success must be `{:ok, %Flux.Asset.Output{}}`
    * failure must be `{:error, reason}`

  ## Examples

      iex> Flux.run({Unknown.Module, :fact_sales})
      {:error, :asset_not_found}

  ## Expected implementation flow

    1. Resolve the target asset
    2. Build the dependency graph or subgraph
    3. Produce an execution plan
    4. Execute stage-by-stage in deterministic order
    5. Return run status, outputs, timings, and errors

  ## TODO

    * Implement on top of the startup-built DAG index in `Flux.GraphIndex`
    * Delegate to `Flux.Runner.run/2`
    * Keep return contract `{:ok, run}` or `{:error, run | reason}`
    * Keep `dependencies: :all | :none` as the first execution mode toggle
    * Keep execution in-memory and synchronous for the first cut
    * Add run process supervision and stage-level parallelism in follow-up work
    * Add freshness-aware skipping after the planner can reason about materialized assets
    * Share common execution result-shaping helpers with planner and storage layers
  """
  @spec run(asset_ref(), run_opts()) :: {:ok, Flux.Run.t()} | {:error, Flux.Run.t() | term()}
  def run({module, name}, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    Flux.Runner.run({module, name}, opts)
  end

  @doc """
  Fetch one run by ID.

  This should return the current or final run state, including status,
  timestamps, execution details, errors, and possibly recent emitted events.

  ## Examples

      iex> Flux.get_run("run_123")
      {:error, :not_found}

  ## TODO

    * Delegate to `Flux.Storage`
    * Keep return contract stable across storage adapters
    * Preserve the canonical `%Flux.Run{}` shape in storage
  """
  @spec get_run(run_id()) :: {:ok, Flux.Run.t()} | {:error, run_error()}
  def get_run(run_id) do
    Flux.Storage.get_run(run_id)
  end

  @doc """
  List runs.

  This is intended for orchestrator screens that need to show historical and
  currently executing runs.

  ## Examples

      iex> Flux.list_runs()
      {:ok, []}

      iex> Flux.list_runs(status: :running)
      {:ok, []}

  ## TODO

    * Delegate to `Flux.Storage`
    * Keep default ordering newest-first
    * Keep filter semantics stable across storage adapters
    * Consider pagination rather than large unbounded lists
  """
  @spec list_runs(list_runs_opts()) :: {:ok, [Flux.Run.t()]} | {:error, run_error()}
  def list_runs(opts \\ []) when is_list(opts) do
    Flux.Storage.list_runs(opts)
  end

  @doc """
  Subscribe to live events for a single run.

  This function is intentionally specific to runs so that the public API stays
  explicit and can later grow with other subscription types if needed.

  The expected event model is structured run events rather than raw log lines.
  Live event delivery is observability-only (best-effort) and is not part of
  run correctness semantics.

  ## Examples

      iex> Flux.subscribe_run("run_123")
      :ok

  ## TODO

    * Delegate to `Flux.Events`
    * Keep topic naming run-scoped by canonical run ID
    * Keep the event envelope stable for UI consumers
    * Extend to broader event streams only after a concrete need appears
  """
  @spec subscribe_run(run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) do
    Flux.Events.subscribe_run(run_id)
  end

  @doc """
  Unsubscribe from live events for a single run.

  Live subscriptions are observability-only and do not affect run execution,
  persistence, or final status outcomes.

  ## Examples

      iex> Flux.unsubscribe_run("run_123")
      :ok

  ## TODO

    * Delegate to `Flux.Events`
    * Mirror the behavior and return contract of `subscribe_run/1`
    * Keep unsubscribe idempotent for callers
  """
  @spec unsubscribe_run(run_id()) :: :ok
  def unsubscribe_run(run_id) do
    Flux.Events.unsubscribe_run(run_id)
  end
end
