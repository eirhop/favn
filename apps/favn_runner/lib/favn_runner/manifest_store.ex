defmodule FavnRunner.ManifestStore do
  @moduledoc """
  In-memory manifest version registry for runner execution.
  """

  use GenServer

  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Version

  @type state :: %{versions: %{required(String.t()) => Version.t()}}
  @type fetch_error :: :manifest_not_found | :manifest_hash_mismatch

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec register(Version.t(), keyword()) :: :ok | {:error, term()}
  def register(%Version{} = version, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:register, version})
  end

  @spec fetch(String.t(), String.t() | nil, keyword()) ::
          {:ok, Version.t()} | {:error, fetch_error()}
  def fetch(manifest_version_id, expected_hash \\ nil, opts \\ [])

  def fetch(manifest_version_id, expected_hash, opts)
      when is_binary(manifest_version_id) and (is_binary(expected_hash) or is_nil(expected_hash)) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:fetch, manifest_version_id, expected_hash})
  end

  def fetch(_manifest_version_id, _expected_hash, _opts), do: {:error, :manifest_not_found}

  @spec delete(String.t(), keyword()) :: :ok
  def delete(manifest_version_id, opts \\ [])
      when is_binary(manifest_version_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete, manifest_version_id})
  end

  @impl true
  def init(_args), do: {:ok, %{versions: %{}}}

  @impl true
  def handle_call({:register, %Version{} = version}, _from, state) do
    reply = register_version(version, state)

    case reply do
      {:ok, next_state} -> {:reply, :ok, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fetch, manifest_version_id, expected_hash}, _from, state) do
    reply = fetch_version(state, manifest_version_id, expected_hash)
    {:reply, reply, state}
  end

  def handle_call({:delete, manifest_version_id}, _from, state) do
    next_state = %{state | versions: Map.delete(state.versions, manifest_version_id)}
    {:reply, :ok, next_state}
  end

  defp register_version(%Version{} = version, state) do
    with :ok <- Compatibility.validate_manifest(version.manifest) do
      case Map.fetch(state.versions, version.manifest_version_id) do
        {:ok, %Version{content_hash: existing_hash}} when existing_hash == version.content_hash ->
          {:ok, state}

        {:ok, %Version{content_hash: existing_hash}} ->
          {:error,
           {:manifest_version_conflict, version.manifest_version_id, existing_hash,
            version.content_hash}}

        :error ->
          {:ok,
           %{state | versions: Map.put(state.versions, version.manifest_version_id, version)}}
      end
    end
  end

  defp fetch_version(state, manifest_version_id, expected_hash) do
    with {:ok, version} <- Map.fetch(state.versions, manifest_version_id),
         :ok <- match_expected_hash(version, expected_hash) do
      {:ok, version}
    else
      :error -> {:error, :manifest_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp match_expected_hash(_version, nil), do: :ok

  defp match_expected_hash(%Version{content_hash: content_hash}, expected_hash)
       when content_hash == expected_hash,
       do: :ok

  defp match_expected_hash(_version, _expected_hash), do: {:error, :manifest_hash_mismatch}
end
