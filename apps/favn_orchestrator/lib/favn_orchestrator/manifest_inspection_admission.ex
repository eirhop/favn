defmodule FavnOrchestrator.ManifestInspectionAdmission do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      Keyword.fetch!(opts, :limit),
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def with_slot(fun, server \\ __MODULE__) when is_function(fun, 0) do
    case Process.whereis(server) do
      nil ->
        if Application.get_env(
             :favn_orchestrator,
             :manifest_inspection_admission_fallback?,
             false
           ) do
          fun.()
        else
          raise "manifest inspection admission is unavailable"
        end

      _pid ->
        {:ok, token} = GenServer.call(server, :acquire, :infinity)

        try do
          fun.()
        after
          GenServer.cast(server, {:release, token})
        end
    end
  end

  @impl true
  def init(limit) when is_integer(limit) and limit > 0 do
    {:ok, %{limit: limit, active: %{}, monitors: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, from, state) do
    monitor = Process.monitor(elem(from, 0))

    if map_size(state.active) < state.limit do
      {token, state} = grant(from, monitor, state)
      {:reply, {:ok, token}, state}
    else
      state = %{
        state
        | waiting: :queue.in({from, monitor}, state.waiting),
          monitors: Map.put(state.monitors, monitor, :waiting)
      }

      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:release, token}, state) do
    {:noreply, release(token, state)}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.get(state.monitors, monitor) do
      {:active, token} -> {:noreply, release(token, state, false)}
      :waiting -> {:noreply, %{state | monitors: Map.delete(state.monitors, monitor)}}
      nil -> {:noreply, state}
    end
  end

  defp grant(_from, monitor, state) do
    token = make_ref()

    state = %{
      state
      | active: Map.put(state.active, token, monitor),
        monitors: Map.put(state.monitors, monitor, {:active, token})
    }

    {token, state}
  end

  defp release(token, state, demonitor? \\ true) do
    case Map.pop(state.active, token) do
      {nil, _active} ->
        state

      {monitor, active} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])

        state = %{
          state
          | active: active,
            monitors: Map.delete(state.monitors, monitor)
        }

        grant_next(state)
    end
  end

  defp grant_next(state) do
    case :queue.out(state.waiting) do
      {:empty, _waiting} ->
        state

      {{:value, {from, monitor}}, waiting} ->
        state = %{state | waiting: waiting}

        if Map.has_key?(state.monitors, monitor) do
          {token, state} = grant(from, monitor, state)
          GenServer.reply(from, {:ok, token})
          state
        else
          grant_next(state)
        end
    end
  end
end
