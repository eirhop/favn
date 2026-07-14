defmodule FavnOrchestrator.Storage.Adapter.Memory.Manifests do
  @moduledoc """
  Pure manifest-version storage operations for the in-memory adapter.

  Content hashes are indexed explicitly because they are both a uniqueness key
  and a supported lookup path.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @doc false
  @spec put(State.t(), Version.t()) :: {:ok | {:error, :manifest_version_conflict}, State.t()}
  def put(%State{} = state, %Version{} = version) do
    case Map.fetch(state.manifest_versions, version.manifest_version_id) do
      {:ok, %Version{content_hash: content_hash}} when content_hash != version.content_hash ->
        {{:error, :manifest_version_conflict}, state}

      {:ok, %Version{}} ->
        {:ok, state}

      :error ->
        put_new_content(state, version)
    end
  end

  @doc false
  @spec get(State.t(), String.t()) :: {:ok, Version.t()} | {:error, :manifest_version_not_found}
  def get(%State{} = state, manifest_version_id) when is_binary(manifest_version_id) do
    fetch_version(state.manifest_versions, manifest_version_id)
  end

  @doc false
  @spec get_by_content_hash(State.t(), String.t()) ::
          {:ok, Version.t()} | {:error, :manifest_version_not_found}
  def get_by_content_hash(%State{} = state, content_hash) when is_binary(content_hash) do
    case Map.fetch(state.manifest_version_ids_by_content_hash, content_hash) do
      {:ok, manifest_version_id} -> fetch_version(state.manifest_versions, manifest_version_id)
      :error -> {:error, :manifest_version_not_found}
    end
  end

  @doc false
  @spec list(State.t()) :: [Version.t()]
  def list(%State{} = state) do
    state.manifest_versions
    |> Map.values()
    |> Enum.sort_by(& &1.manifest_version_id)
  end

  @doc false
  @spec activate(State.t(), String.t()) ::
          {:ok, State.t()} | {{:error, :manifest_version_not_found}, State.t()}
  def activate(%State{} = state, manifest_version_id) when is_binary(manifest_version_id) do
    if Map.has_key?(state.manifest_versions, manifest_version_id) do
      {:ok, %{state | active_manifest_version_id: manifest_version_id}}
    else
      {{:error, :manifest_version_not_found}, state}
    end
  end

  defp put_new_content(state, version) do
    if Map.has_key?(state.manifest_version_ids_by_content_hash, version.content_hash) do
      {:ok, state}
    else
      {:ok,
       %{
         state
         | manifest_versions:
             Map.put(state.manifest_versions, version.manifest_version_id, version),
           manifest_version_ids_by_content_hash:
             Map.put(
               state.manifest_version_ids_by_content_hash,
               version.content_hash,
               version.manifest_version_id
             )
       }}
    end
  end

  defp fetch_version(versions, manifest_version_id) do
    case Map.fetch(versions, manifest_version_id) do
      {:ok, %Version{} = version} -> {:ok, version}
      :error -> {:error, :manifest_version_not_found}
    end
  end
end
