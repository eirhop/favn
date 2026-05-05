defmodule FavnOrchestrator.ManifestStore do
  @moduledoc """
  Manifest persistence and activation facade for orchestrator runtime.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage

  @spec register_manifest(Version.t()) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version) do
    with {:ok, verified} <- Version.verify(version) do
      Storage.put_manifest_version(verified)
    end
  end

  @spec publish_manifest(Version.t()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish_manifest(%Version{} = version) do
    with {:ok, verified} <- Version.verify(version) do
      case Storage.get_manifest_version_by_content_hash(verified.content_hash) do
        {:ok, %Version{} = existing} ->
          {:ok, :already_published, existing}

        {:error, reason} when reason in [:manifest_version_not_found, :not_found] ->
          case Storage.put_manifest_version(verified) do
            :ok -> {:ok, :published, verified}
            {:error, :manifest_version_conflict} -> resolve_publish_conflict(verified)
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp resolve_publish_conflict(%Version{} = version) do
    case Storage.get_manifest_version_by_content_hash(version.content_hash) do
      {:ok, %Version{} = existing} -> {:ok, :already_published, existing}
      {:error, _reason} -> {:error, :manifest_version_conflict}
    end
  end

  @spec get_manifest(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest(manifest_version_id) when is_binary(manifest_version_id) do
    Storage.get_manifest_version(manifest_version_id)
  end

  @spec get_manifest_by_content_hash(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_by_content_hash(content_hash) when is_binary(content_hash) do
    Storage.get_manifest_version_by_content_hash(content_hash)
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
