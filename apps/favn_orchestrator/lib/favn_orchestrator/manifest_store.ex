defmodule FavnOrchestrator.ManifestStore do
  @moduledoc """
  Manifest persistence and activation facade for orchestrator runtime.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage

  @spec register_manifest(Version.t()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version), do: Storage.put_manifest_version(version)

  @spec get_manifest(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    Storage.get_manifest_version(manifest_version_id)
  end

  @spec list_manifests() :: {:ok, [Version.t()]} | {:error, term()}
  def list_manifests, do: Storage.list_manifest_versions()

  @spec set_active_manifest(String.t()) :: :ok | {:error, term()}
  def set_active_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    Storage.set_active_manifest_version(manifest_version_id)
  end

  @spec get_active_manifest() :: {:ok, String.t()} | {:error, term()}
  def get_active_manifest, do: Storage.get_active_manifest_version()
end
