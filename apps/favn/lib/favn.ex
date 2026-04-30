defmodule Favn do
  @moduledoc """
  Public Favn facade.

  This module collects the main public helper functions in one place.

  It preserves the public `Favn.*` contract while delegating:

  - authoring and manifest compilation to `FavnAuthoring`
  - local lifecycle and packaging tasks to `Favn.Dev` through public
    `mix favn.*` tasks
  - runtime/orchestrator calls to the runtime apps when they are available

  ## Main Functions

  - `list_assets/0,1`: compile and inspect assets
  - `get_asset/1`: fetch one compiled asset
  - `get_pipeline/1`: fetch one compiled pipeline definition
  - `generate_manifest/1`: build the canonical manifest
  - `build_manifest/1`: build the manifest plus diagnostics metadata
  - `serialize_manifest/1`, `hash_manifest/1`,
    `validate_manifest_compatibility/1`, `pin_manifest_version/2`: work with
    manifest payloads and versions
  - `resolve_pipeline/2`: resolve one pipeline to concrete targets and context
  - `plan_asset_run/2`: build a deterministic execution plan

  ## Public authoring workflow

  A typical non-runtime inspection flow is:

      {:ok, assets} = Favn.list_assets()
      {:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
      {:ok, resolution} = Favn.resolve_pipeline(MyApp.Pipelines.DailySales)
      {:ok, manifest} = Favn.generate_manifest()
      {:ok, version} = Favn.pin_manifest_version(manifest)

  ## See also

  - `Favn.AI`
  - `Favn.SQLClient`
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
          limit: pos_integer()
        ]

  @type backfill_anchor_range :: %{
          required(:kind) => Favn.Window.Anchor.kind(),
          required(:start_at) => DateTime.t(),
          required(:end_at) => DateTime.t(),
          optional(:timezone) => String.t()
        }

  @type manifest_opts :: [
          asset_modules: [module()],
          pipeline_modules: [module()],
          schedule_modules: [module()]
        ]

  @type run_id :: term()

  @doc """
  Returns `true` when the module compiles as a Favn asset module.

  Use this when introspection code needs to distinguish plain Elixir modules
  from modules authored with `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`,
  `Favn.Assets`, or `Favn.Source`.
  """
  @spec asset_module?(module()) :: boolean()
  defdelegate asset_module?(module), to: FavnAuthoring

  @doc """
  Compiles and returns all configured asset modules from `config :favn`.

  This is the normal entrypoint when the current project already declares its
  `:asset_modules` in application config.

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
  metadata, dependencies, relation ownership, window spec, and compiled config.

  ## Examples

      {:ok, asset} = Favn.get_asset(MyApp.Warehouse.Raw.Sales.Orders)
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
  but do not need build-only metadata such as diagnostics.

  If no explicit modules are passed, `Favn` reads `:asset_modules`,
  `:pipeline_modules`, and `:schedule_modules` from `config :favn`.

  ## Example

      {:ok, manifest} = Favn.generate_manifest(
        asset_modules: [MyApp.Warehouse.Raw.Orders, MyApp.Warehouse.Mart.OrderSummary],
        pipeline_modules: [MyApp.Pipelines.DailySales]
      )
  """
  @spec generate_manifest(keyword()) :: {:ok, Favn.Manifest.t()} | {:error, term()}
  def generate_manifest(opts \\ []) when is_list(opts) do
    FavnAuthoring.generate_manifest(opts)
  end

  @doc """
  Builds a manifest plus build-only metadata such as diagnostics.

  Use this instead of `generate_manifest/1` when tooling needs both the
  canonical manifest payload and non-runtime build information.
  """
  @spec build_manifest(keyword()) :: {:ok, Favn.Manifest.Build.t()} | {:error, term()}
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
  @spec run_pipeline(module(), keyword()) :: {:ok, term()} | {:error, term()}
  def run_pipeline(pipeline_module, opts \\ [])

  def run_pipeline(pipeline_module, opts) when is_atom(pipeline_module) and is_list(opts) do
    orchestrator_runtime_call(:submit_pipeline_run, [pipeline_module, opts])
  end

  def run_pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

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

  @doc false
  @spec get_run(term()) :: {:ok, term()} | {:error, term()}
  def get_run(run_id) do
    orchestrator_runtime_call(:get_run, [run_id])
  end

  @doc false
  @spec list_runs(keyword()) :: {:ok, [term()]} | {:error, term()}
  def list_runs(opts \\ []) when is_list(opts) do
    orchestrator_runtime_call(:list_runs, [opts])
  end

  @doc false
  @spec list_run_events(run_id(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []) when is_list(opts) do
    orchestrator_runtime_call(:list_run_events, [run_id, opts])
  end

  @doc false
  @spec rerun(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun(run_id, opts \\ []) when is_list(opts) do
    orchestrator_runtime_call(:rerun, [run_id, opts])
  end

  @doc false
  @spec cancel_run(run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_map(reason) do
    orchestrator_runtime_call(:cancel_run, [run_id, reason])
  end

  @doc false
  @spec reload_scheduler() :: :ok | {:error, term()}
  def reload_scheduler do
    scheduler_runtime_call(:reload)
  end

  @doc false
  @spec tick_scheduler() :: :ok | {:error, term()}
  def tick_scheduler do
    scheduler_runtime_call(:tick)
  end

  @doc false
  @spec list_scheduled_pipelines() :: [map()] | {:error, term()}
  def list_scheduled_pipelines do
    scheduler_runtime_call(:list_scheduled_pipelines)
  end

  defp normalized_sql_asset_call(runtime_function, asset_input, opts)
       when is_atom(runtime_function) and is_list(opts) do
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(runtime_function, [asset, opts])
    end
  end

  defp orchestrator_runtime_call(runtime_function, runtime_args)
       when is_atom(runtime_function) and is_list(runtime_args) do
    orchestrator_module = FavnOrchestrator
    arity = length(runtime_args)

    if runtime_function_exported?(orchestrator_module, runtime_function, arity) do
      try do
        orchestrator_module
        |> apply(runtime_function, runtime_args)
        |> normalize_orchestrator_result()
      rescue
        UndefinedFunctionError ->
          {:error, :runtime_not_available}
      catch
        :exit, {:noproc, _detail} ->
          {:error, :runtime_not_available}

        :exit, {:normal, _detail} ->
          {:error, :runtime_not_available}

        :exit, reason ->
          {:error, {:runtime_call_exited, reason}}
      end
    else
      {:error, :runtime_not_available}
    end
  end

  defp normalize_orchestrator_result({:error, {:exited, {:noproc, _detail}}}),
    do: {:error, :runtime_not_available}

  defp normalize_orchestrator_result({:error, {:exited, {:normal, _detail}}}),
    do: {:error, :runtime_not_available}

  defp normalize_orchestrator_result(other), do: other

  defp scheduler_runtime_call(runtime_function) when is_atom(runtime_function) do
    scheduler_module = Favn.Scheduler

    if runtime_function_exported?(scheduler_module, runtime_function, 0) do
      try do
        scheduler_module
        |> apply(runtime_function, [])
        |> normalize_scheduler_result()
      rescue
        UndefinedFunctionError ->
          {:error, :runtime_not_available}
      catch
        :exit, {:noproc, _detail} ->
          {:error, :runtime_not_available}

        :exit, {:normal, _detail} ->
          {:error, :runtime_not_available}

        :exit, reason ->
          {:error, {:runtime_call_exited, reason}}
      end
    else
      {:error, :runtime_not_available}
    end
  end

  defp normalize_scheduler_result({:error, :scheduler_not_running}),
    do: {:error, :runtime_not_available}

  defp normalize_scheduler_result(other), do: other

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
