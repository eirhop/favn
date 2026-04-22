defmodule Favn do
  @moduledoc """
  Public Favn facade.

  This module is a thin wrapper that preserves the public `Favn.*` surface while
  delegating authoring implementation to `FavnAuthoring`.
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

  @spec asset_module?(module()) :: boolean()
  defdelegate asset_module?(module), to: FavnAuthoring

  @spec list_assets() :: {:ok, [Favn.Asset.t()]} | {:error, term()}
  defdelegate list_assets(), to: FavnAuthoring

  @spec list_assets(module() | [module()]) :: {:ok, [Favn.Asset.t()]} | {:error, term()}
  defdelegate list_assets(module_or_modules), to: FavnAuthoring

  @spec get_asset(module() | Favn.Ref.t()) :: {:ok, Favn.Asset.t()} | {:error, term()}
  defdelegate get_asset(asset_input), to: FavnAuthoring

  @spec get_pipeline(module()) :: {:ok, Favn.Pipeline.Definition.t()} | {:error, term()}
  defdelegate get_pipeline(module), to: FavnAuthoring

  @spec generate_manifest(keyword()) :: {:ok, Favn.Manifest.t()} | {:error, term()}
  def generate_manifest(opts \\ []) when is_list(opts) do
    FavnAuthoring.generate_manifest(opts)
  end

  @spec build_manifest(keyword()) :: {:ok, Favn.Manifest.Build.t()} | {:error, term()}
  def build_manifest(opts \\ []) when is_list(opts) do
    FavnAuthoring.build_manifest(opts)
  end

  @spec serialize_manifest(map() | struct()) :: {:ok, binary()} | {:error, term()}
  defdelegate serialize_manifest(manifest), to: FavnAuthoring

  @spec hash_manifest(map() | struct()) :: {:ok, String.t()} | {:error, term()}
  defdelegate hash_manifest(manifest), to: FavnAuthoring

  @spec validate_manifest_compatibility(map() | struct()) :: :ok | {:error, term()}
  defdelegate validate_manifest_compatibility(manifest), to: FavnAuthoring

  @spec pin_manifest_version(map() | struct(), keyword()) ::
          {:ok, Favn.Manifest.Version.t()} | {:error, term()}
  def pin_manifest_version(manifest, opts \\ []) when is_list(opts) do
    FavnAuthoring.pin_manifest_version(manifest, opts)
  end

  @spec plan_asset_run(Favn.Ref.t() | [Favn.Ref.t()], keyword()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_asset_run(target_refs, opts \\ []) when is_list(opts) do
    FavnAuthoring.plan_asset_run(target_refs, opts)
  end

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
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(:render, [asset, opts])
    end
  end

  @doc false
  @spec preview(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def preview(asset_input, opts \\ []) when is_list(opts) do
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(:preview, [asset, opts])
    end
  end

  @doc false
  @spec explain(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def explain(asset_input, opts \\ []) when is_list(opts) do
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(:explain, [asset, opts])
    end
  end

  @doc false
  @spec materialize(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def materialize(asset_input, opts \\ []) when is_list(opts) do
    with {:ok, asset} <- SQLAssetInput.normalize(asset_input) do
      sql_runtime_call(:materialize, [asset, opts])
    end
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

  defp orchestrator_runtime_call(function_name, args)
       when is_atom(function_name) and is_list(args) do
    orchestrator = FavnOrchestrator

    with {:module, ^orchestrator} <- Code.ensure_loaded(orchestrator),
         arity <- length(args),
         true <- function_exported?(orchestrator, function_name, arity) do
      try do
        orchestrator
        |> apply(function_name, args)
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
      _ -> {:error, :runtime_not_available}
    end
  end

  defp normalize_orchestrator_result({:error, {:exited, {:noproc, _detail}}}),
    do: {:error, :runtime_not_available}

  defp normalize_orchestrator_result({:error, {:exited, {:normal, _detail}}}),
    do: {:error, :runtime_not_available}

  defp normalize_orchestrator_result(other), do: other

  defp scheduler_runtime_call(function_name) when is_atom(function_name) do
    scheduler = Favn.Scheduler

    with {:module, ^scheduler} <- Code.ensure_loaded(scheduler),
         true <- function_exported?(scheduler, function_name, 0) do
      try do
        scheduler
        |> apply(function_name, [])
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
      _ -> {:error, :runtime_not_available}
    end
  end

  defp normalize_scheduler_result({:error, :scheduler_not_running}),
    do: {:error, :runtime_not_available}

  defp normalize_scheduler_result(other), do: other

  defp sql_runtime_call(function_name, args)
       when is_atom(function_name) and is_list(args) do
    runtime_module = Favn.SQLAsset.Runtime

    with {:module, ^runtime_module} <- Code.ensure_loaded(runtime_module),
         arity <- length(args),
         true <- function_exported?(runtime_module, function_name, arity) do
      apply(runtime_module, function_name, args)
    else
      _ -> {:error, :runtime_not_available}
    end
  end
end
