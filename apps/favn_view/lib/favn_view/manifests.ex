defmodule FavnView.Manifests do
  @moduledoc """
  Thin view-side context over orchestrator manifest APIs.
  """

  alias FavnOrchestrator

  @spec list_manifest_summaries() ::
          {:ok, [FavnOrchestrator.manifest_summary()]} | {:error, term()}
  def list_manifest_summaries, do: FavnOrchestrator.list_manifest_summaries()

  @spec get_manifest_summary(String.t()) ::
          {:ok, FavnOrchestrator.manifest_summary()} | {:error, term()}
  def get_manifest_summary(manifest_version_id),
    do: FavnOrchestrator.get_manifest_summary(manifest_version_id)

  @spec active_manifest_targets() :: {:ok, FavnOrchestrator.manifest_targets()} | {:error, term()}
  def active_manifest_targets, do: FavnOrchestrator.active_manifest_targets()

  @spec manifest_targets(String.t()) ::
          {:ok, FavnOrchestrator.manifest_targets()} | {:error, term()}
  def manifest_targets(manifest_version_id),
    do: FavnOrchestrator.manifest_targets(manifest_version_id)

  @spec submit_asset_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_asset_for_manifest(manifest_version_id, target_id, opts \\ []) do
    FavnOrchestrator.submit_asset_run_for_manifest(manifest_version_id, target_id, opts)
  end

  @spec submit_pipeline_for_manifest(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_pipeline_for_manifest(manifest_version_id, target_id, opts \\ []) do
    FavnOrchestrator.submit_pipeline_run_for_manifest(manifest_version_id, target_id, opts)
  end

  @spec active_manifest() :: {:ok, String.t()} | {:error, term()}
  def active_manifest, do: FavnOrchestrator.active_manifest()
end
