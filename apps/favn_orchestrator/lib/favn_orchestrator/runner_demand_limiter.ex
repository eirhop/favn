defmodule FavnOrchestrator.RunnerDemandLimiter do
  @moduledoc "Process-local fixed-window limiter dedicated to infrastructure demand reads."
  use GenServer

  @default_limit 120
  @window_ms 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec allow?(String.t()) :: boolean()
  def allow?(identity), do: GenServer.call(__MODULE__, {:allow, identity})

  @impl true
  def init(opts) do
    {:ok, %{limit: Keyword.get(opts, :limit, @default_limit), windows: %{}}}
  end

  @impl true
  def handle_call({:allow, identity}, _from, state) do
    now = System.monotonic_time(:millisecond)
    {started_at, count} = Map.get(state.windows, identity, {now, 0})

    {started_at, count} =
      if now - started_at >= @window_ms, do: {now, 0}, else: {started_at, count}

    allowed? = count < state.limit
    windows = Map.put(state.windows, identity, {started_at, count + 1})

    windows =
      if map_size(windows) > 1_024, do: %{identity => {started_at, count + 1}}, else: windows

    {:reply, allowed?, %{state | windows: windows}}
  end
end
