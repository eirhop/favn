defmodule FavnRunner.TaskResultBuffer do
  @moduledoc "Bounded in-memory delivery buffer for one-slot runner output."
  use GenServer

  @default_max_entries 200
  @default_flush_entries 50

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def reset(server \\ __MODULE__), do: GenServer.call(server, :reset)
  def append(server \\ __MODULE__, entry), do: GenServer.call(server, {:append, entry})
  def drain_logs(server \\ __MODULE__), do: GenServer.call(server, :drain_logs)

  @doc false
  @spec restore_logs(GenServer.server(), [term()]) :: :ok
  def restore_logs(server \\ __MODULE__, entries),
    do: GenServer.call(server, {:restore_logs, entries})

  def put_result(server \\ __MODULE__, result), do: GenServer.call(server, {:put_result, result})
  def pending_result(server \\ __MODULE__), do: GenServer.call(server, :pending_result)
  def acknowledge_result(server \\ __MODULE__), do: GenServer.call(server, :acknowledge_result)
  def stats(server \\ __MODULE__), do: GenServer.call(server, :stats)

  @impl true
  def init(opts) do
    {:ok,
     %{
       entries: [],
       count: 0,
       dropped: 0,
       flush_requested?: false,
       max_entries: Keyword.get(opts, :max_entries, @default_max_entries),
       flush_entries: Keyword.get(opts, :flush_entries, @default_flush_entries),
       telemetry_metadata: Keyword.get(opts, :telemetry_metadata, %{}),
       result: nil
     }}
  end

  @impl true
  def handle_call(:reset, _from, state),
    do:
      {:reply, :ok,
       %{state | entries: [], count: 0, dropped: 0, flush_requested?: false, result: nil}}

  def handle_call({:append, entry}, _from, state) do
    if state.count < state.max_entries do
      next = %{state | entries: [entry | state.entries], count: state.count + 1}
      flush? = next.count >= next.flush_entries and not state.flush_requested?
      next = if flush?, do: %{next | flush_requested?: true}, else: next
      reply = if flush?, do: :flush, else: :ok
      {:reply, reply, next}
    else
      :telemetry.execute(
        [:favn, :runner, :task_buffer, :dropped],
        %{count: 1},
        state.telemetry_metadata
      )

      {:reply, :dropped, %{state | dropped: state.dropped + 1}}
    end
  end

  def handle_call(:drain_logs, _from, state) do
    entries = Enum.reverse(state.entries)

    entries =
      if state.dropped > 0,
        do: entries ++ [%{type: :truncated, dropped_count: state.dropped}],
        else: entries

    {:reply, entries, %{state | entries: [], count: 0, dropped: 0, flush_requested?: false}}
  end

  def handle_call({:restore_logs, entries}, _from, state) when is_list(entries) do
    combined = entries ++ Enum.reverse(state.entries)
    {kept, dropped} = Enum.split(combined, state.max_entries)
    dropped_count = length(dropped)

    {:reply, :ok,
     %{
       state
       | entries: Enum.reverse(kept),
         count: length(kept),
         dropped: state.dropped + dropped_count,
         flush_requested?: state.flush_requested? or kept != []
     }}
  end

  def handle_call({:put_result, result}, _from, %{result: nil} = state),
    do: {:reply, :ok, %{state | result: result}}

  def handle_call({:put_result, result}, _from, %{result: result} = state),
    do: {:reply, :ok, state}

  def handle_call({:put_result, _result}, _from, state) do
    :telemetry.execute(
      [:favn, :runner, :task_buffer, :result_conflict],
      %{count: 1},
      state.telemetry_metadata
    )

    {:reply, {:error, :result_buffer_conflict}, state}
  end

  def handle_call(:pending_result, _from, state), do: {:reply, state.result, state}
  def handle_call(:acknowledge_result, _from, state), do: {:reply, :ok, %{state | result: nil}}

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       count: state.count,
       dropped: state.dropped,
       max_entries: state.max_entries,
       flush_requested?: state.flush_requested?
     }, state}
  end
end
