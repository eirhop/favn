defmodule FavnOrchestrator.RunnerQueueCoordinator do
  @moduledoc """
  In-memory wake hint coordinator for one exact runner pool and release.

  It never owns or assigns durable work. Every woken runner must still perform
  an atomic PostgreSQL claim.
  """

  use GenServer

  alias Favn.Contracts.RunnerTask.Wake

  def start_link({pool, release} = key),
    do: GenServer.start_link(__MODULE__, key, name: via(pool, release))

  def ensure_started(pool, release) do
    spec = {__MODULE__, {pool, release}}

    case DynamicSupervisor.start_child(FavnOrchestrator.RunnerQueueDynamicSupervisor, spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  def generation(pool, release) do
    {:ok, pid} = ensure_started(pool, release)
    GenServer.call(pid, :generation)
  end

  def wait(pool, release, observed_generation, session) do
    {:ok, pid} = ensure_started(pool, release)
    GenServer.call(pid, {:wait, observed_generation, session})
  end

  def notify(pool, release, count \\ 1) when is_integer(count) and count >= 0 do
    if Process.whereis(FavnOrchestrator.RunnerQueueDynamicSupervisor) do
      with {:ok, pid} <- ensure_started(pool, release) do
        GenServer.cast(pid, {:notify, count})
      end
    else
      :ok
    end
  end

  def child_spec(key) do
    %{
      id: {__MODULE__, key},
      start: {__MODULE__, :start_link, [key]},
      restart: :transient
    }
  end

  @impl true
  def init({pool, release}),
    do: {:ok, %{pool: pool, release: release, generation: 0, waiters: [], monitors: %{}}}

  @impl true
  def handle_call(:generation, _from, state), do: {:reply, state.generation, state}

  def handle_call({:wait, observed, session}, _from, state) do
    if observed == state.generation do
      state = put_waiter(state, session)
      {:reply, :waiting, state}
    else
      {:reply, :retry, state}
    end
  end

  @impl true
  def handle_cast({:notify, count}, state) do
    state = %{state | generation: state.generation + 1}
    {wake, keep} = Enum.split(state.waiters, count)

    Enum.each(wake, fn session ->
      message = %Wake{
        runner_instance_id: session.runner_instance_id,
        runner_session_generation: session.session_generation,
        runner_pool: state.pool,
        required_runner_release_id: state.release
      }

      send(session.agent_pid, {:favn_runner_task, message})
    end)

    monitors =
      Enum.reduce(wake, state.monitors, fn session, monitors ->
        Process.demonitor(session.waiter_monitor_ref, [:flush])
        Map.delete(monitors, session.waiter_monitor_ref)
      end)

    {:noreply, %{state | waiters: keep, monitors: monitors}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {runner_id, monitors} ->
        waiters = Enum.reject(state.waiters, &(&1.runner_instance_id == runner_id))
        {:noreply, %{state | waiters: waiters, monitors: monitors}}
    end
  end

  defp put_waiter(state, session) do
    {removed, waiters} =
      Enum.split_with(state.waiters, &(&1.runner_instance_id == session.runner_instance_id))

    monitors =
      Enum.reduce(removed, state.monitors, fn old, monitors ->
        Process.demonitor(old.waiter_monitor_ref, [:flush])
        Map.delete(monitors, old.waiter_monitor_ref)
      end)

    ref = Process.monitor(session.agent_pid)
    waiter = Map.put(session, :waiter_monitor_ref, ref)

    %{
      state
      | waiters: waiters ++ [waiter],
        monitors: Map.put(monitors, ref, session.runner_instance_id)
    }
  end

  defp via(pool, release),
    do: {:via, Registry, {FavnOrchestrator.RunnerQueueRegistry, {pool, release}}}
end
