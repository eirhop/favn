defmodule FavnOrchestrator.ManifestIndexCache do
  @moduledoc """
  Bounded node-local cache for compiled immutable manifest indexes.

  Indexes are keyed by manifest version, content hash, and the local index format
  version. A cache miss always remains correct: the index is compiled from the
  immutable manifest and returned even when it is too large to retain.
  """

  use GenServer

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version

  @index_format_version 1
  @default_max_entries 128
  @default_max_bytes 256 * 1_024 * 1_024
  @term_budget_multiplier 4

  @type diagnostics :: %{
          running?: boolean(),
          entries: non_neg_integer(),
          bytes: non_neg_integer(),
          max_entries: pos_integer(),
          max_bytes: pos_integer(),
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          evictions: non_neg_integer(),
          oversized_skips: non_neg_integer()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    init_opts = Keyword.drop(opts, [:name])

    if is_nil(name),
      do: GenServer.start_link(__MODULE__, init_opts),
      else: GenServer.start_link(__MODULE__, init_opts, name: name)
  end

  @doc "Compiles or returns the cached index for an immutable manifest version."
  @spec fetch(Version.t(), keyword()) :: {:ok, Index.t()} | {:error, Index.error()}
  def fetch(%Version{} = version, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 30_000)

    case resolve_server(server) do
      nil -> Index.build_from_version(version)
      pid -> fetch_or_compile(pid, version, timeout)
    end
  end

  @doc "Returns cache bounds and pressure counters."
  @spec diagnostics(keyword()) :: diagnostics()
  def diagnostics(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    case resolve_server(server) do
      nil -> stopped_diagnostics()
      pid -> GenServer.call(pid, :diagnostics)
    end
  end

  @impl true
  def init(opts) do
    max_entries = Keyword.get(opts, :max_entries, configured_max_entries())
    max_bytes = Keyword.get(opts, :max_bytes, configured_max_bytes())

    if valid_max_entries?(max_entries) and valid_max_bytes?(max_bytes) do
      {:ok,
       %{
         indexes: %{},
         sizes: %{},
         order: :queue.new(),
         entries: 0,
         bytes: 0,
         max_entries: max_entries,
         max_bytes: max_bytes,
         hits: 0,
         misses: 0,
         evictions: 0,
         oversized_skips: 0
       }}
    else
      {:stop, :invalid_manifest_index_cache_size}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case Map.fetch(state.indexes, key) do
      {:ok, %Index{} = index} ->
        {:reply, {:ok, index}, %{state | hits: state.hits + 1}}

      :error ->
        {:reply, {:miss, state.max_bytes}, %{state | misses: state.misses + 1}}
    end
  end

  def handle_call({:put, key, %Index{} = index, bytes}, _from, state) do
    cond do
      Map.has_key?(state.indexes, key) ->
        {:reply, :ok, state}

      bytes > state.max_bytes ->
        emit(:oversized, bytes)
        {:reply, :ok, %{state | oversized_skips: state.oversized_skips + 1}}

      true ->
        {:reply, :ok, put_index(state, key, index, bytes)}
    end
  end

  def handle_call(:diagnostics, _from, state) do
    {:reply, Map.put(cache_diagnostics(state), :running?, true), state}
  end

  defp fetch_or_compile(pid, version, timeout) do
    key = cache_key(version)

    case GenServer.call(pid, {:get, key}, timeout) do
      {:ok, %Index{} = index} ->
        {:ok, index}

      {:miss, _max_bytes} ->
        with {:ok, %Index{} = index} <- Index.build_from_version(version),
             bytes <- cache_entry_bytes(key, index),
             :ok <- GenServer.call(pid, {:put, key, index, bytes}, timeout) do
          {:ok, index}
        end
    end
  end

  defp put_index(state, key, index, bytes) do
    %{
      state
      | indexes: Map.put(state.indexes, key, index),
        sizes: Map.put(state.sizes, key, bytes),
        order: :queue.in(key, state.order),
        entries: state.entries + 1,
        bytes: state.bytes + bytes
    }
    |> evict_if_needed()
  end

  defp evict_if_needed(state)
       when state.entries <= state.max_entries and state.bytes <= state.max_bytes,
       do: state

  defp evict_if_needed(state) do
    {{:value, key}, order} = :queue.out(state.order)
    bytes = Map.fetch!(state.sizes, key)
    emit(:eviction, bytes)

    evict_if_needed(%{
      state
      | indexes: Map.delete(state.indexes, key),
        sizes: Map.delete(state.sizes, key),
        order: order,
        entries: state.entries - 1,
        bytes: state.bytes - bytes,
        evictions: state.evictions + 1
    })
  end

  defp cache_key(%Version{} = version) do
    {version.manifest_version_id, version.content_hash, @index_format_version}
  end

  defp cache_entry_bytes(key, index) do
    @term_budget_multiplier * :erlang.external_size({key, index})
  end

  defp emit(event, bytes) do
    :telemetry.execute(
      [:favn, :orchestrator, :manifest_index_cache, event],
      %{count: 1, bytes: bytes},
      %{}
    )
  end

  defp resolve_server(pid) when is_pid(pid) do
    if Process.alive?(pid), do: pid, else: nil
  end

  defp resolve_server(name) when is_atom(name), do: Process.whereis(name)

  defp cache_diagnostics(state) do
    Map.take(state, [
      :entries,
      :bytes,
      :max_entries,
      :max_bytes,
      :hits,
      :misses,
      :evictions,
      :oversized_skips
    ])
  end

  defp stopped_diagnostics do
    %{
      running?: false,
      entries: 0,
      bytes: 0,
      max_entries: configured_max_entries(),
      max_bytes: configured_max_bytes(),
      hits: 0,
      misses: 0,
      evictions: 0,
      oversized_skips: 0
    }
  end

  defp valid_max_entries?(value), do: is_integer(value) and value in 1..100_000

  defp valid_max_bytes?(value),
    do: is_integer(value) and value >= 1 and value <= 16 * 1_024 * 1_024 * 1_024

  defp configured_max_entries do
    :favn_orchestrator
    |> Application.get_env(:manifest_index_cache, [])
    |> Keyword.get(:max_entries, @default_max_entries)
  end

  defp configured_max_bytes do
    :favn_orchestrator
    |> Application.get_env(:manifest_index_cache, [])
    |> Keyword.get(:max_bytes, @default_max_bytes)
  end
end
