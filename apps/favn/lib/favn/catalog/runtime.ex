defmodule Favn.Catalog.Runtime do
  @moduledoc false
  use GenServer

  # This owner survives termination of the deadline-bounded publication task.
  # Only this invocation's applications are stopped, including late startups.
  def start, do: GenServer.start(__MODULE__, self())
  def start_applications(pid, apps, timeout), do: GenServer.call(pid, {:start, apps}, timeout)

  def close(pid) do
    monitor = Process.monitor(pid)
    GenServer.cast(pid, :close)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _} -> :ok
    after
      1000 -> Process.demonitor(monitor, [:flush])
    end
  end

  @impl true
  def init(owner) do
    lock = {__MODULE__, self()}

    if :global.set_lock(lock, [node()], 0) do
      {:ok, %{owner: Process.monitor(owner), started: [], lock: lock}}
    else
      {:stop, :catalog_runtime_busy}
    end
  end

  @impl true
  def handle_call({:start, apps}, _from, state) do
    {result, started} =
      Enum.reduce_while(apps, {:ok, state.started}, fn app, {_, owned} ->
        case Application.ensure_all_started(app) do
          {:ok, started} -> {:cont, {:ok, Enum.reverse(started) ++ owned}}
          _ -> {:halt, {{:error, :catalog_runtime_start_failed}, owned}}
        end
      end)

    {:reply, result, %{state | started: started}}
  end

  @impl true
  def handle_cast(:close, state), do: {:stop, :normal, state}

  @impl true
  def handle_info({:DOWN, monitor, :process, _, _}, %{owner: monitor} = state),
    do: {:stop, :normal, state}

  @impl true
  def terminate(_, state) do
    try do
      Enum.each(state.started, &Application.stop/1)
    after
      :global.del_lock(state.lock, [node()])
    end
  end
end
