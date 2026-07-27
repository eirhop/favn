defmodule FavnOrchestrator.RunnerTaskResultRouter do
  @moduledoc "Routes already-durable terminal task results to the owning run process."
  use GenServer

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.SystemContext

  @terminal [:succeeded, :failed, :cancelled, :unknown]

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))

  def subscribe(workspace_id, task_id, pid \\ self()) do
    case Process.whereis(__MODULE__) do
      nil -> {:error, :runner_task_result_router_unavailable}
      _router -> GenServer.call(__MODULE__, {:subscribe, workspace_id, task_id, pid})
    end
  catch
    :exit, reason -> {:error, {:runner_task_result_router_call_failed, reason}}
  end

  def notify(task), do: GenServer.cast(__MODULE__, {:notify, task})

  @doc false
  def await(workspace_id, task_id, parent) do
    parent_ref = Process.monitor(parent)
    do_await(workspace_id, task_id, parent, parent_ref)
  end

  defp do_await(workspace_id, task_id, parent, parent_ref) do
    case Process.whereis(__MODULE__) do
      router when is_pid(router) ->
        monitor_ref = Process.monitor(router)

        case subscribe(workspace_id, task_id, self()) do
          result when result in [:ready, :waiting] ->
            receive do
              {:runner_task_result, ^workspace_id, ^task_id, _task} = message ->
                Process.demonitor(monitor_ref, [:flush])
                Process.demonitor(parent_ref, [:flush])
                send(parent, message)

              {:DOWN, ^monitor_ref, :process, ^router, _reason} ->
                do_await(workspace_id, task_id, parent, parent_ref)

              {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
                :ok
            end

          {:error, _reason} ->
            Process.demonitor(monitor_ref, [:flush])
            retry_await(workspace_id, task_id, parent, parent_ref)
        end

      _unavailable ->
        retry_await(workspace_id, task_id, parent, parent_ref)
    end
  end

  defp retry_await(workspace_id, task_id, parent, parent_ref) do
    receive do
      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        :ok
    after
      50 -> do_await(workspace_id, task_id, parent, parent_ref)
    end
  end

  @impl true
  def init(_opts), do: {:ok, %{waiters: %{}, monitors: %{}}}

  @impl true
  def handle_call({:subscribe, workspace_id, task_id, pid}, _from, state) do
    key = {workspace_id, task_id}

    case get_task(workspace_id, task_id) do
      {:ok, %{status: status} = task} when status in @terminal ->
        send(pid, {:runner_task_result, workspace_id, task_id, task})
        {:reply, :ready, state}

      {:ok, _task} ->
        ref = Process.monitor(pid)
        waiters = Map.update(state.waiters, key, [{pid, ref}], &[{pid, ref} | &1])

        {:reply, :waiting,
         %{state | waiters: waiters, monitors: Map.put(state.monitors, ref, key)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:notify, %{workspace_id: workspace_id, task_id: task_id} = task}, state) do
    key = {workspace_id, task_id}
    {waiters, remaining} = Map.pop(state.waiters, key, [])

    Enum.each(waiters, fn {pid, _ref} ->
      send(pid, {:runner_task_result, workspace_id, task_id, task})
    end)

    monitors =
      Enum.reduce(waiters, state.monitors, fn {_pid, ref}, monitors ->
        Process.demonitor(ref, [:flush])
        Map.delete(monitors, ref)
      end)

    {:noreply, %{state | waiters: remaining, monitors: monitors}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {key, monitors} ->
        waiters =
          Map.update(state.waiters, key, [], fn entries ->
            Enum.reject(entries, fn {_pid, waiter_ref} -> waiter_ref == ref end)
          end)

        {:noreply, %{state | waiters: waiters, monitors: monitors}}
    end
  end

  defp get_task(workspace_id, task_id) do
    Persistence.stores().runner_tasks.get(%Q.GetRunnerTask{
      workspace_context: SystemContext.workspace(workspace_id, :runner_task_result_router),
      task_id: task_id
    })
  end
end
