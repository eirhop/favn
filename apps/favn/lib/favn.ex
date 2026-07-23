defmodule Favn do
  @moduledoc """
  Stable public facade for the `favn` package.

  `Favn` is the supported v1 entrypoint for authoring-time inspection,
  manifest work, pipeline resolution, and deterministic planning. These
  functions operate on authored modules and canonical manifest data without
  requiring the runtime applications to be running.

  ## Stable v1 APIs

  - `asset_module?/1`: identify modules that compile as Favn assets
  - `list_assets/0,1`: compile and inspect assets
  - `get_asset/1`: fetch one compiled asset
  - `get_pipeline/1`: fetch one compiled pipeline definition
  - `generate_manifest/1`: build the canonical manifest
  - `build_manifest/1`: build the manifest plus diagnostics metadata
  - `serialize_manifest/1`, `hash_manifest/1`,
    `validate_manifest_compatibility/1`, `pin_manifest_version/2`,
    `prepare_manifest_publication/2`: work with manifest indexes, versions, and
    immutable execution packages
  - `resolve_pipeline/2`: resolve one pipeline to concrete targets and context
  - `plan_asset_run/2`: build a deterministic execution plan

  Compiled assets include normalized freshness policies when authored with
  `freshness`; read `Favn.Freshness` and `Favn.Freshness.Policy` for the
  authoring contract.

  ## Runner lifecycle extensions

  Consumer-owned services that must run inside the isolated execution runtime
  use the public `Favn.Runner.Plugin` lifecycle. The no-boilerplate path is
  `Favn.Runner.SupervisedChildren`. Both are configured with `config :favn,
  runner_plugins: [...]`; consumers do not depend on the internal
  `:favn_runner` application. Custom plugins can explicitly declare packaged
  OTP applications that the isolated runner must start before their children.

  Plugin processes are suitable for rebuildable runner-local caches, sessions,
  pools, and rate limiters. They are not durable storage or a correctness-safe
  way to pass data between asset runs. Read
  [Runner Plugins And Runner-Local Services](runner-plugins.html).
  That guide also documents cached Azure managed-identity injection for
  DuckLake metadata backed by Azure Database for PostgreSQL.

  ## Runtime-dependent helpers

  This module also keeps callable helpers for local SQL runtime operations:
  `render/2`, `preview/2`, `explain/2`, and `materialize/2`.

  Those helpers are intentionally not documented as stable ordinary user APIs.
  They delegate to the optional SQL runtime when available and return
  `{:error, :runtime_not_available}` when the relevant runtime boundary is not
  loaded or running.

  ## Public authoring workflow

  A typical non-runtime inspection flow is:

      {:ok, assets} = Favn.list_assets()
      {:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
      {:ok, resolution} = Favn.resolve_pipeline(MyApp.Pipelines.DailySales)
      {:ok, manifest} = Favn.generate_manifest(runner_release_id: runner_release_id)
      {:ok, version} = Favn.pin_manifest_version(manifest)

  Deployment tooling should preserve the complete execution artifact set:

      {:ok, build} = Favn.build_manifest(runner_release_id: runner_release_id)
      {:ok, publication} = Favn.prepare_manifest_publication(build)

  `publication.version` is the compact schema-12 manifest index. It is bound to
  the operator-selected runner image through `required_runner_release_id`.
  SQL assets point to immutable entries in `publication.execution_packages` by
  content hash. In these examples, `runner_release_id` is the immutable identity
  chosen by the user or CI system for the customer runner image.

  ## Retries, replay, and runtime-input stability

  Favn deliberately has no global "retry everything" switch. Several
  mechanisms can repeat work, but they have different scopes:

  - SQL safety retries cover only proven-safe session bootstrap and read-only
    inspection/query operations. They do not consume an asset attempt and never
    blindly retry a write, materialization, or transaction.
  - Persistence retries repeat a failed control-plane state write. They do not
    rerun asset code or consume an asset attempt.
  - A node-attempt retry repeats one failed asset node inside the same run. It
    consumes an attempt and preserves successful sibling nodes.
  - Opt-in resource recovery creates a linked new run after a shared resource's
    exclusive probe succeeds. It includes circuit-blocked nodes and only failed
    nodes explicitly classified as safe to repeat.
  - A rerun, replay, backfill child, or admitted schedule occurrence creates a
    new run with independent attempt counts. Schedule overlap and missed-run
    policy decide whether such a run exists; they are not execution retries.
  - HTTP command idempotency prevents duplicate commands. It does not retry or
    prove the success of asset execution.

  `max_attempts` includes the initial attempt. The effective node policy is
  frozen into the run plan using this precedence:

      explicit operator submission override
      -> asset retry
      -> pipeline retry
      -> max_attempts: 1

  Pipeline and asset policies share `%Favn.Retry.Policy{}` and support fixed or
  bounded exponential `Favn.Retry.Backoff`. A policy answers how often and when
  a node may repeat; it never makes a failure safe. Another attempt is scheduled
  only when the normalized runner failure explicitly says both that it is
  retryable and that its outcome is a known safe failure. Unknown write,
  transaction, materialization, and external-side-effect outcomes remain
  terminal regardless of `max_attempts`. A typed `retry_after_ms` raises the
  policy delay but cannot exceed the global delay bound.

  Public submissions accept one typed `retry_policy` override. HTTP JSON uses
  the backoff fields `strategy`, `initial_ms`, `max_ms`, and `jitter`; the local
  Mix tasks expose only fixed `--retry-max-attempts`/`--retry-backoff-ms`
  shorthand. A backoff with the default `max_attempts: 1` does not cause a
  retry.

  SQL assets with `runtime_inputs` use a resolve/pin/execute handshake. Before
  SQL work starts, the selected parameters are atomically stored under
  `{run_id, planned_node_key}`; retries and safe orchestrator recovery reuse that
  exact pin. Sensitive pins require protected storage and fail closed when no
  valid protection key is configured. Generic metadata, events, logs, and
  telemetry expose only safe pin identity and lineage.

  New-run input behavior is explicit: normal runs, schedules, and backfill
  children default to `:fresh`; exact replay uses `:pinned` and fails if a
  required source pin is missing; resume and retry-remaining use `:inherit`,
  copying existing pins and resolving only nodes the source run never reached.
  Choosing fresh input for a rerun is valid, but it is not exact replay.

  Cancellation during backoff prevents dispatch. Safe pending retry state and
  its absolute `next_retry_at` are durable across an orchestrator restart.
  Recovery does not dispatch replacement work when an earlier side effect may
  have succeeded.

  Resource circuit breakers are configured on execution pools or named SQL
  connections. They count consecutive explicit resource failures, block only
  affected nodes, and grant one half-open probe after their delay. Independent
  DAG siblings keep running; required downstream nodes become durably blocked.
  Pipelines opt into linked recovery with `resource_recovery :retry_remaining`.
  The terminal source run is immutable.

  Read [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html) before
  setting retry policy at more than one level. It includes the complete
  precedence rules, safety boundary, schedule timeline, replay input matrix,
  transaction caveats, and operator checklist. Runtime-input authors should
  also read [Runtime Inputs For SQL Assets](sql-runtime-inputs.html).

  ## See also

  - `Favn.AI`
  - `Favn.SQLClient`
  - `Favn.Runner.Plugin`
  - `Favn.Runner.SupervisedChildren`
  - `Favn.Freshness`
  - task-specific DSL modules such as `Favn.Asset`, `Favn.SQLAsset`,
    `Favn.Pipeline`, and `Favn.Dev`
  """

  alias Favn.SQLAsset.Input, as: SQLAssetInput

  @type asset_ref :: Favn.Ref.t()
  @type asset :: Favn.Asset.t()
  @type asset_error :: :not_asset_module | :asset_not_found
  @type dependencies_mode :: :all | :none
  @type list_runs_opts :: [
          status: :running | :ok | :error | :cancelled | :timed_out,
          manifest_version_id: String.t(),
          pipeline_module: module(),
          limit: pos_integer()
        ]

  @type backfill_anchor_range :: %{
          required(:kind) => Favn.Window.Anchor.kind(),
          required(:start_at) => DateTime.t(),
          required(:end_at) => DateTime.t(),
          optional(:timezone) => String.t()
        }

  @type manifest_opts :: [
          asset_modules: [module()] | :all,
          pipeline_modules: [module()] | :all,
          schedule_modules: [module()] | :all,
          connection_modules: [module()] | :all,
          runner_release_id: String.t()
        ]

  @type run_id :: term()

  @doc """
  Returns `true` when the module compiles as a Favn asset module.

  Use this when introspection code needs to distinguish plain Elixir modules
  from modules authored with `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`,
  or `Favn.Source`.
  """
  @spec asset_module?(module()) :: boolean()
  defdelegate asset_module?(module), to: FavnAuthoring

  @doc """
  Compiles and returns all configured or discovered asset modules from
  `config :favn`.

  This is the normal entrypoint when the current project declares
  `:asset_modules` explicitly or opts into app-scoped discovery with
  `config :favn, discovery: [apps: [:my_app], assets: :all]`.

  ## Example

      {:ok, assets} = Favn.list_assets()
  """
  @spec list_assets() :: {:ok, [Favn.Asset.t()]} | {:error, term()}
  defdelegate list_assets(), to: FavnAuthoring

  @doc """
  Compiles and returns assets for one module or a list of modules.

  Use this when you want targeted inspection instead of compiling the full
  configured catalog.

  Module shorthand such as `MyApp.Sales.Orders` is treated as the canonical
  single-asset ref `{MyApp.Sales.Orders, :asset}`.
  """
  @spec list_assets(module() | [module()]) :: {:ok, [Favn.Asset.t()]} | {:error, term()}
  defdelegate list_assets(module_or_modules), to: FavnAuthoring

  @doc """
  Fetches one compiled asset by module shorthand or canonical ref.

  Use this when you want the normalized `%Favn.Asset{}` for one asset, including
  metadata, dependencies, relation ownership, window spec, freshness policy, and
  compiled config.

  ## Examples

      {:ok, asset} = Favn.get_asset(MyApp.Lakehouse.Raw.Sales.Orders)
      {:ok, asset} = Favn.get_asset({MyApp.Raw.Shopify, :orders})
  """
  @spec get_asset(module() | Favn.Ref.t()) :: {:ok, Favn.Asset.t()} | {:error, term()}
  defdelegate get_asset(asset_input), to: FavnAuthoring

  @doc """
  Fetches a compiled pipeline definition for one pipeline module.

  Read `Favn.Pipeline` for the DSL itself. Use this function when authored code
  already exists and you want the normalized `%Favn.Pipeline.Definition{}`.
  """
  @spec get_pipeline(module()) :: {:ok, Favn.Pipeline.Definition.t()} | {:error, term()}
  defdelegate get_pipeline(module), to: FavnAuthoring

  @doc """
  Generates the canonical `%Favn.Manifest{}` from authored modules.

  Use this when you need the stable graph payload used by planning and runtime,
  but do not need build-only metadata such as diagnostics. An immutable
  `:runner_release_id` is required and becomes the manifest's exact
  `required_runner_release_id`.

  If no explicit modules are passed, `Favn` reads `:asset_modules`,
  `:pipeline_modules`, and `:schedule_modules` from `config :favn`. Projects can
  also opt into release-safe app-scoped discovery with `config :favn, discovery:
  [apps: [:my_app], assets: :all, pipelines: :all, schedules: :all]`.

  ## Example

      # With config :favn, discovery: [apps: [:my_app], assets: :all, pipelines: :all, schedules: :all]
      {:ok, manifest} = Favn.generate_manifest(runner_release_id: runner_release_id)

      {:ok, manifest} = Favn.generate_manifest(
        runner_release_id: runner_release_id,
        asset_modules: [MyApp.Lakehouse.Raw.Sales.Orders, MyApp.Lakehouse.Mart.Sales.OrderSummary],
        pipeline_modules: [MyApp.Pipelines.DailySales]
      )
  """
  @spec generate_manifest(manifest_opts()) :: {:ok, Favn.Manifest.t()} | {:error, term()}
  def generate_manifest(opts \\ []) when is_list(opts) do
    FavnAuthoring.generate_manifest(opts)
  end

  @doc """
  Builds a manifest plus build-only metadata such as diagnostics.

  Use this instead of `generate_manifest/1` when tooling needs both the
  canonical manifest payload and non-runtime build information. Pass the same
  required `:runner_release_id` selected for the customer runner image.
  """
  @spec build_manifest(manifest_opts()) :: {:ok, Favn.Manifest.Build.t()} | {:error, term()}
  def build_manifest(opts \\ []) when is_list(opts) do
    FavnAuthoring.build_manifest(opts)
  end

  @doc """
  Serializes a canonical manifest payload to stable JSON bytes.

  Use this when persisting, hashing, or transferring a manifest outside the VM.
  """
  @spec serialize_manifest(map() | struct()) :: {:ok, binary()} | {:error, term()}
  defdelegate serialize_manifest(manifest), to: FavnAuthoring

  @doc """
  Computes the stable content hash for a canonical manifest payload.

  Use this when identity should depend on manifest content rather than build
  timestamps or local environment details.
  """
  @spec hash_manifest(map() | struct()) :: {:ok, String.t()} | {:error, term()}
  defdelegate hash_manifest(manifest), to: FavnAuthoring

  @doc """
  Validates that a manifest matches the current schema and runner contract.

  Use this before packaging or registering a manifest into a runtime that must
  reject incompatible payloads.
  """
  @spec validate_manifest_compatibility(map() | struct()) :: :ok | {:error, term()}
  defdelegate validate_manifest_compatibility(manifest), to: FavnAuthoring

  @doc """
  Wraps a manifest in an immutable manifest-version envelope.

  Use this when the manifest will be registered, activated, or attached to runs.
  The result keeps the canonical manifest together with stable version identity.
  """
  @spec pin_manifest_version(map() | struct(), keyword()) ::
          {:ok, Favn.Manifest.Version.t()} | {:error, term()}
  def pin_manifest_version(manifest, opts \\ []) when is_list(opts) do
    FavnAuthoring.pin_manifest_version(manifest, opts)
  end

  @doc """
  Prepares a manifest build for scalable publication.

  The returned publication contains one compact pinned manifest index and the
  exact immutable SQL execution packages referenced by it. Runtime publishers
  upload only packages the orchestrator does not already have, then register the
  compact index.
  """
  @spec prepare_manifest_publication(Favn.Manifest.Build.t(), keyword()) ::
          {:ok, Favn.Manifest.Publication.t()} | {:error, term()}
  def prepare_manifest_publication(%Favn.Manifest.Build{} = build, opts \\ [])
      when is_list(opts) do
    FavnAuthoring.prepare_manifest_publication(build, opts)
  end

  @doc """
  Builds a deterministic run plan for one or more target asset refs.

  Use this when you already know the target assets and want dependency expansion,
  topological stages, and optional backfill window expansion without first going
  through a pipeline.

  Read `Favn.Assets.Planner` if you need the exact planning semantics.
  """
  @spec plan_asset_run(Favn.Ref.t() | [Favn.Ref.t()], keyword()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_asset_run(target_refs, opts \\ []) when is_list(opts) do
    FavnAuthoring.plan_asset_run(target_refs, opts)
  end

  @doc """
  Resolves a pipeline module into concrete selected refs and pipeline context.

  Use this when you need to know what a pipeline selects, which dependency mode
  it uses, and what schedule/config/metadata it contributes before execution.

  ## Example

      {:ok, resolution} = Favn.resolve_pipeline(MyApp.Pipelines.DailySales)
  """
  @spec resolve_pipeline(module(), keyword()) ::
          {:ok, Favn.Pipeline.Resolution.t()} | {:error, term()}
  def resolve_pipeline(pipeline_module, opts \\ []) when is_list(opts) do
    FavnAuthoring.resolve_pipeline(pipeline_module, opts)
  end

  @doc false
  @spec render(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def render(asset_input, opts \\ []) when is_list(opts) do
    normalized_sql_asset_call(:render, asset_input, opts)
  end

  @doc false
  @spec preview(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def preview(asset_input, opts \\ []) when is_list(opts) do
    normalized_sql_asset_call(:preview, asset_input, opts)
  end

  @doc false
  @spec explain(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def explain(asset_input, opts \\ []) when is_list(opts) do
    normalized_sql_asset_call(:explain, asset_input, opts)
  end

  @doc false
  @spec materialize(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def materialize(asset_input, opts \\ []) when is_list(opts) do
    normalized_sql_asset_call(:materialize, asset_input, opts)
  end

  defp normalized_sql_asset_call(runtime_function, asset_input, opts)
       when is_atom(runtime_function) and is_list(opts) do
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(runtime_function, [asset, opts])
    end
  end

  defp sql_runtime_call(runtime_function, runtime_args)
       when is_atom(runtime_function) and is_list(runtime_args) do
    runtime_module = Favn.SQLAsset.Runtime
    arity = length(runtime_args)

    if runtime_function_exported?(runtime_module, runtime_function, arity) do
      apply(runtime_module, runtime_function, runtime_args)
    else
      {:error, :runtime_not_available}
    end
  end

  defp runtime_function_exported?(runtime_module, runtime_function, arity)
       when is_atom(runtime_module) and is_atom(runtime_function) and is_integer(arity) do
    case Code.ensure_loaded(runtime_module) do
      {:module, ^runtime_module} -> function_exported?(runtime_module, runtime_function, arity)
      _other -> false
    end
  end
end
