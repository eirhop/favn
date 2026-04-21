defmodule Favn do
  @moduledoc """
  Public Favn facade.

  This module is a thin wrapper that preserves the public `Favn.*` surface while
  delegating authoring implementation to `FavnAuthoring`.
  """

  @type asset_ref :: Favn.Ref.t()
  @type asset :: Favn.Asset.t()
  @type list_runs_opts :: [
          status: :running | :ok | :error | :cancelled | :timed_out,
          limit: pos_integer()
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

  @spec run_pipeline(module(), keyword()) :: {:ok, term()} | {:error, term()}
  def run_pipeline(pipeline_module, opts \\ []) when is_list(opts) do
    FavnAuthoring.run_pipeline(pipeline_module, opts)
  end

  @spec render(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def render(asset_input, opts \\ []) when is_list(opts) do
    FavnAuthoring.render(asset_input, opts)
  end

  @spec preview(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def preview(asset_input, opts \\ []) when is_list(opts) do
    FavnAuthoring.preview(asset_input, opts)
  end

  @spec explain(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def explain(asset_input, opts \\ []) when is_list(opts) do
    FavnAuthoring.explain(asset_input, opts)
  end

  @spec materialize(module() | Favn.Ref.t() | Favn.Asset.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def materialize(asset_input, opts \\ []) when is_list(opts) do
    FavnAuthoring.materialize(asset_input, opts)
  end

  @spec get_run(term()) :: {:ok, term()} | {:error, term()}
  defdelegate get_run(run_id), to: FavnAuthoring

  @spec list_runs(keyword()) :: {:ok, [term()]} | {:error, term()}
  def list_runs(opts \\ []) when is_list(opts) do
    FavnAuthoring.list_runs(opts)
  end

  @spec list_run_events(run_id(), keyword()) ::
          {:ok, [FavnOrchestrator.RunEvent.t()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []) when is_list(opts) do
    FavnAuthoring.list_run_events(run_id, opts)
  end

  @spec rerun(run_id(), keyword()) :: {:ok, run_id()} | {:error, term()}
  def rerun(run_id, opts \\ []) when is_list(opts) do
    FavnAuthoring.rerun(run_id, opts)
  end

  @spec cancel_run(run_id(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_map(reason) do
    FavnAuthoring.cancel_run(run_id, reason)
  end

  @spec reload_scheduler() :: :ok | {:error, term()}
  defdelegate reload_scheduler(), to: FavnAuthoring

  @spec tick_scheduler() :: :ok | {:error, term()}
  defdelegate tick_scheduler(), to: FavnAuthoring

  @spec list_scheduled_pipelines() :: [map()] | {:error, term()}
  defdelegate list_scheduled_pipelines(), to: FavnAuthoring
end
