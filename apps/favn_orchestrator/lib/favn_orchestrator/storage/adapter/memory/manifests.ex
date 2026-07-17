defmodule FavnOrchestrator.Storage.Adapter.Memory.Manifests do
  @moduledoc """
  Pure manifest-version storage operations for the in-memory adapter.

  Content hashes are indexed explicitly because they are both a uniqueness key
  and a supported lookup path.
  """

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @doc false
  @spec put(State.t(), Version.t()) :: {:ok | {:error, term()}, State.t()}
  def put(%State{} = state, %Version{} = version) do
    required_refs = Publication.required_package_refs(version)
    required_hashes = Enum.map(required_refs, &elem(&1, 0))

    case validate_package_refs(state, required_refs) do
      :ok -> put_verified_index(state, version, required_hashes)
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  @doc false
  @spec put_packages(State.t(), [ExecutionPackage.t()]) :: {:ok, State.t()}
  def put_packages(%State{} = state, packages) when is_list(packages) do
    execution_packages =
      Enum.reduce(packages, state.execution_packages, fn package, acc ->
        Map.put_new(acc, package.content_hash, package)
      end)

    {:ok, %{state | execution_packages: execution_packages}}
  end

  @doc false
  @spec missing_package_hashes(State.t(), [String.t()]) :: [String.t()]
  def missing_package_hashes(%State{} = state, hashes) when is_list(hashes) do
    Enum.reject(hashes, &Map.has_key?(state.execution_packages, &1))
  end

  @doc false
  @spec get_package(State.t(), String.t()) ::
          {:ok, ExecutionPackage.t()} | {:error, :execution_package_not_found}
  def get_package(%State{} = state, content_hash) when is_binary(content_hash) do
    case Map.fetch(state.execution_packages, content_hash) do
      {:ok, package} -> {:ok, package}
      :error -> {:error, :execution_package_not_found}
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

  defp validate_package_refs(state, required_refs) do
    missing = missing_package_hashes(state, Enum.map(required_refs, &elem(&1, 0)))

    case missing do
      [] -> validate_package_ownership(state, required_refs)
      hashes -> {:error, {:missing_execution_packages, hashes}}
    end
  end

  defp validate_package_ownership(state, required_refs) do
    Enum.reduce_while(required_refs, :ok, fn {hash, expected_ref}, :ok ->
      actual_ref = state.execution_packages |> Map.fetch!(hash) |> Map.fetch!(:asset_ref)
      stored_actual_ref = stored_ref(actual_ref)

      if actual_ref == expected_ref do
        {:cont, :ok}
      else
        {:halt,
         {:error, {:execution_package_asset_mismatch, hash, expected_ref, stored_actual_ref}}}
      end
    end)
  end

  defp stored_ref({module, name}) do
    %{module: Atom.to_string(module), name: Atom.to_string(name)}
  end

  defp put_verified_index(state, version, required_hashes) do
    case Map.fetch(state.manifest_versions, version.manifest_version_id) do
      {:ok, %Version{content_hash: content_hash}} when content_hash != version.content_hash ->
        {{:error, :manifest_version_conflict}, state}

      {:ok, %Version{}} ->
        {:ok, state}

      :error ->
        case put_new_content(state, version) do
          {:ok, next_state} ->
            canonical_id =
              Map.fetch!(next_state.manifest_version_ids_by_content_hash, version.content_hash)

            {:ok,
             %{
               next_state
               | manifest_package_hashes:
                   Map.put(
                     next_state.manifest_package_hashes,
                     canonical_id,
                     MapSet.new(required_hashes)
                   )
             }}
        end
    end
  end

  defp fetch_version(versions, manifest_version_id) do
    case Map.fetch(versions, manifest_version_id) do
      {:ok, %Version{} = version} -> {:ok, version}
      :error -> {:error, :manifest_version_not_found}
    end
  end
end
