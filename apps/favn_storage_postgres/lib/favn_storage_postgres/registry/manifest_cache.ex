defmodule FavnStoragePostgres.Registry.ManifestCache do
  @moduledoc false

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById

  @entries_table __MODULE__.Entries
  @order_table __MODULE__.Order
  @default_max_entries 1_024

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options \\ []) do
    GenServer.start_link(__MODULE__, options, name: Keyword.get(options, :name, __MODULE__))
  end

  @spec get(ById.t() | ByContentHash.t()) :: {:ok, Version.t()} | :miss
  def get(%ById{manifest_version_id: id}), do: lookup({:id, id})
  def get(%ByContentHash{content_hash: hash}), do: lookup({:content_hash, hash})

  @spec put(Version.t()) :: :ok
  def put(%Version{} = version) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.call(__MODULE__, {:put, version})
    end
  end

  @spec diagnostics() :: map()
  def diagnostics do
    case Process.whereis(__MODULE__) do
      nil -> %{running?: false, entries: 0, max_entries: configured_max_entries()}
      _pid -> GenServer.call(__MODULE__, :diagnostics)
    end
  end

  @impl true
  def init(options) do
    max_entries = Keyword.get(options, :max_entries, configured_max_entries())

    if not is_integer(max_entries) or max_entries < 1 or max_entries > 100_000 do
      {:stop, :invalid_manifest_cache_size}
    else
      :ets.new(@entries_table, [
        :named_table,
        :set,
        :protected,
        read_concurrency: true,
        write_concurrency: false
      ])

      :ets.new(@order_table, [:named_table, :ordered_set, :protected])
      {:ok, %{max_entries: max_entries, count: 0, sequence: 0}}
    end
  end

  @impl true
  def handle_call({:put, %Version{} = version}, _from, state) do
    case :ets.lookup(@entries_table, {:id, version.manifest_version_id}) do
      [{_key, _cached}] ->
        {:reply, :ok, state}

      [] ->
        sequence = state.sequence + 1
        id_key = {:id, version.manifest_version_id}
        hash_key = {:content_hash, version.content_hash}
        true = :ets.insert(@entries_table, [{id_key, version}, {hash_key, version}])
        true = :ets.insert(@order_table, {sequence, id_key, hash_key})

        next_state =
          %{state | count: state.count + 1, sequence: sequence}
          |> evict_if_needed()

        {:reply, :ok, next_state}
    end
  end

  def handle_call(:diagnostics, _from, state) do
    {:reply, %{running?: true, entries: state.count, max_entries: state.max_entries}, state}
  end

  defp lookup(key) do
    case :ets.lookup(@entries_table, key) do
      [{^key, %Version{} = version}] -> {:ok, version}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp evict_if_needed(%{count: count, max_entries: max_entries} = state)
       when count <= max_entries,
       do: state

  defp evict_if_needed(state) do
    case :ets.first(@order_table) do
      sequence when is_integer(sequence) ->
        [{^sequence, id_key, hash_key}] = :ets.lookup(@order_table, sequence)
        true = :ets.delete(@order_table, sequence)
        true = :ets.delete(@entries_table, id_key)
        true = :ets.delete(@entries_table, hash_key)
        evict_if_needed(%{state | count: state.count - 1})

      :"$end_of_table" ->
        %{state | count: 0}
    end
  end

  defp configured_max_entries do
    Application.get_env(
      :favn_storage_postgres,
      :manifest_cache_max_entries,
      @default_max_entries
    )
  end
end
