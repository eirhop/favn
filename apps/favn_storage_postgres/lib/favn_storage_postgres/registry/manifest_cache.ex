defmodule FavnStoragePostgres.Registry.ManifestCache do
  @moduledoc false

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById

  @entries_table __MODULE__.Entries
  @order_table __MODULE__.Order
  @default_max_entries 1_024
  @default_max_bytes 64 * 1_024 * 1_024
  @term_budget_multiplier 4

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options \\ []) do
    GenServer.start_link(__MODULE__, options, name: Keyword.get(options, :name, __MODULE__))
  end

  @spec get(ById.t() | ByContentHash.t()) :: {:ok, Version.t()} | :miss
  def get(%ById{manifest_version_id: id}), do: lookup_version(id)

  def get(%ByContentHash{content_hash: hash}) do
    with [{_key, id}] <- safe_lookup({:content_hash, hash}) do
      lookup_version(id)
    else
      _missing -> :miss
    end
  end

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
      nil ->
        %{
          running?: false,
          entries: 0,
          bytes: 0,
          max_entries: configured_max_entries(),
          max_bytes: configured_max_bytes(),
          oversized_skips: 0
        }

      _pid ->
        GenServer.call(__MODULE__, :diagnostics)
    end
  end

  @impl true
  def init(options) do
    max_entries = Keyword.get(options, :max_entries, configured_max_entries())
    max_bytes = Keyword.get(options, :max_bytes, configured_max_bytes())

    if not valid_max_entries?(max_entries) or not valid_max_bytes?(max_bytes) do
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

      {:ok,
       %{
         max_entries: max_entries,
         max_bytes: max_bytes,
         count: 0,
         bytes: 0,
         sequence: 0,
         oversized_skips: 0
       }}
    end
  end

  @impl true
  def handle_call({:put, %Version{} = version}, _from, state) do
    case :ets.lookup(@entries_table, {:version, version.manifest_version_id}) do
      [{_key, _cached}] ->
        {:reply, :ok, state}

      [] ->
        id_key = {:version, version.manifest_version_id}
        hash_key = {:content_hash, version.content_hash}
        entry_bytes = cache_entry_bytes(id_key, hash_key, version)

        if entry_bytes > state.max_bytes do
          {:reply, :ok, %{state | oversized_skips: state.oversized_skips + 1}}
        else
          sequence = state.sequence + 1

          true =
            :ets.insert(@entries_table, [
              {id_key, version},
              {hash_key, version.manifest_version_id}
            ])

          true = :ets.insert(@order_table, {sequence, id_key, hash_key, entry_bytes})

          next_state =
            %{
              state
              | count: state.count + 1,
                bytes: state.bytes + entry_bytes,
                sequence: sequence
            }
            |> evict_if_needed()

          {:reply, :ok, next_state}
        end
    end
  end

  def handle_call(:diagnostics, _from, state) do
    {:reply,
     %{
       running?: true,
       entries: state.count,
       bytes: state.bytes,
       max_entries: state.max_entries,
       max_bytes: state.max_bytes,
       oversized_skips: state.oversized_skips
     }, state}
  end

  defp lookup_version(id) do
    key = {:version, id}

    case safe_lookup(key) do
      [{^key, %Version{} = version}] -> {:ok, version}
      [] -> :miss
    end
  end

  defp safe_lookup(key) do
    :ets.lookup(@entries_table, key)
  rescue
    ArgumentError -> []
  end

  defp evict_if_needed(
         %{count: count, max_entries: max_entries, bytes: bytes, max_bytes: max_bytes} = state
       )
       when count <= max_entries and bytes <= max_bytes,
       do: state

  defp evict_if_needed(state) do
    case :ets.first(@order_table) do
      sequence when is_integer(sequence) ->
        [{^sequence, id_key, hash_key, entry_bytes}] = :ets.lookup(@order_table, sequence)
        true = :ets.delete(@order_table, sequence)
        true = :ets.delete(@entries_table, id_key)
        true = :ets.delete(@entries_table, hash_key)

        evict_if_needed(%{
          state
          | count: state.count - 1,
            bytes: max(state.bytes - entry_bytes, 0)
        })

      :"$end_of_table" ->
        %{state | count: 0, bytes: 0}
    end
  end

  # External term size is a stable budget unit. The multiplier accounts for decoded
  # terms and ETS bookkeeping without relying on VM-internal accounting APIs.
  defp cache_entry_bytes(id_key, hash_key, version) do
    @term_budget_multiplier * :erlang.external_size({id_key, version}) +
      :erlang.external_size({hash_key, version.manifest_version_id}) +
      :erlang.external_size({0, id_key, hash_key, 0})
  end

  defp valid_max_entries?(value), do: is_integer(value) and value in 1..100_000

  defp valid_max_bytes?(value),
    do: is_integer(value) and value >= 1 and value <= 16 * 1_024 * 1_024 * 1_024

  defp configured_max_entries do
    Application.get_env(
      :favn_storage_postgres,
      :manifest_cache_max_entries,
      @default_max_entries
    )
  end

  defp configured_max_bytes do
    Application.get_env(
      :favn_storage_postgres,
      :manifest_cache_max_bytes,
      @default_max_bytes
    )
  end
end
