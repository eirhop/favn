defmodule FavnView.Manifests do
  @moduledoc """
  Thin view-side context over orchestrator manifest APIs.
  """

  alias FavnOrchestrator

  @spec list_manifests() :: {:ok, [Favn.Manifest.Version.t()]} | {:error, term()}
  def list_manifests, do: FavnOrchestrator.list_manifests()

  @spec get_manifest(String.t()) :: {:ok, Favn.Manifest.Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id), do: FavnOrchestrator.get_manifest(manifest_version_id)

  @spec active_manifest() :: {:ok, String.t()} | {:error, term()}
  def active_manifest, do: FavnOrchestrator.active_manifest()
end
