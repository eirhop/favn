defmodule FavnOrchestrator.RunnerTaskResultRouter do
  @moduledoc """
  Routes already-durable terminal task results to the owning run process.

  Durable subscription checks run asynchronously behind a fixed concurrency
  ceiling. A provisional waiter is installed before each read so notifications
  racing the read remain deterministic and cannot be lost.

  Waiters subscribed with `notify_started?: true` additionally receive a
  `{:runner_task_started, workspace_id, task_id, task}` message when the
  assigned runner reports the task as started. Started notifications are
  advisory presence signals: they do not consume the waiter and carry no
  delivery guarantee beyond the subscription check.
  """
  use GenServer

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.SystemContext

  @terminal [:succeeded, :failed, :cancelled, :unknown]
  @default_max_concurrency 32

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Subscribes `pid` to the terminal result of one task.

  With `notify_started?: true` the subscriber also receives
  `{:runner_task_started, workspace_id, task_id, task}` messages.
  """
  @spec subscribe(String.t(), String.t(), pid(), keyword()) ::
          :ready | :waiting | {:error, term()}
  def subscribe(workspace_id, task_id, pid \\ self(), opts \\ []) do
    case Process.whereis(__MODULE__) do
      nil -> {:error, :runner_task_result_router_unavailable}
      _router -> GenServer.call(__MODULE__, {:subscribe, workspace_id, task_id, pid, opts})
    end
  catch
    :exit, reason -> {:error, {:runner_task_result_router_call_failed, reason}}
  end

  def notify(task), do: GenServer.cast(__MODULE__, {:notify, task})

  @doc "Delivers a started notification to opted-in waiters without consuming them."
  @spec notify_started(map()) :: :ok
  def notify_started(task), do: GenServer.cast(__MODULE__, {:notify_started, task})

  @doc false
  def await(workspace_id, task_id, parent, opts \\ []) do
    parent_ref = Process.monitor(parent)
    do_await(workspace_id, task_id, parent, parent_ref, opts)
  end

  defp do_await(workspace_id, task_id, parent, parent_ref, opts) do
    case Process.whereis(__MODULE__) do
      router when is_pid(router) ->
        monitor_ref = Process.monitor(router)

        case subscribe(workspace_id, task_id, self(), opts) do
          result when result in [:ready, :waiting] ->
            await_result(workspace_id, task_id, parent, parent_ref, router, monitor_ref, opts)

          {:error, _reason} ->
            Process.demonitor(monitor_ref, [:flush])
            retry_await(workspace_id, task_id, parent, parent_ref, opts)
        end

      _unavailable ->
        retry_await(workspace_id, task_id, parent, parent_ref, opts)
    end
  end

  defp await_result(workspace_id, task_id, parent, parent_ref, router, monitor_ref, opts) do
    receive do
      {:runner_task_result, ^workspace_id, ^task_id, _task} = message ->
        Process.demonitor(monitor_ref, [:flush])
        Process.demonitor(parent_ref, [:flush])
        send(parent, message)

      {:runner_task_started, ^workspace_id, ^task_id, _task} = message ->
        send(parent, message)
        await_result(workspace_id, task_id, parent, parent_ref, router, monitor_ref, opts)

      {:DOWN, ^monitor_ref, :process, ^router, _reason} ->
        do_await(workspace_id, task_id, parent, parent_ref, opts)

      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        :ok
    end
  end

  defp retry_await(workspace_id, task_id, parent, parent_ref, opts) do
    receive do
      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        :ok
    after
      50 -> do_await(workspace_id, task_id, parent, parent_ref, opts)
    end
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      {:ok,
       %{
         checks: %{},
         max_concurrency: max_concurrency,
         monitors: %{},
         task_supervisor:
           Keyword.get(opts, :task_supervisor, FavnOrchestrator.RunnerClaimSupervisor),
         waiters: %{}
       }}
    else
      {:stop, {:invalid_runner_task_result_router_max_concurrency, max_concurrency}}
    end
  end

  @impl true
  def handle_call({:subscribe, workspace_id, task_id, pid, opts}, from, state) do
    key = {workspace_id, task_id}

    if map_size(state.checks) < state.max_concurrency do
      monitor_ref = Process.monitor(pid)

      task =
        Task.Supervisor.async_nolink(state.task_supervisor, fn ->
          get_task(workspace_id, task_id)
        end)

      waiter = %{
        check_ref: task.ref,
        from: from,
        monitor_ref: monitor_ref,
        notify_started?: Keyword.get(opts, :notify_started?, false),
        pid: pid
      }

      {:noreply,
       %{
         state
         | checks:
             Map.put(state.checks, task.ref, %{
               key: key,
               monitor_ref: monitor_ref,
               task_pid: task.pid
             }),
           monitors: Map.put(state.monitors, monitor_ref, key),
           waiters: Map.update(state.waiters, key, [waiter], &[waiter | &1])
       }}
    else
      {:reply, {:error, :runner_task_result_router_overloaded}, state}
    end
  end

  @impl true
  def handle_cast({:notify, %{workspace_id: workspace_id, task_id: task_id} = task}, state) do
    key = {workspace_id, task_id}
    {waiters, remaining} = Map.pop(state.waiters, key, [])

    state = %{state | waiters: remaining}

    state =
      Enum.reduce(waiters, state, fn waiter, state ->
        send(waiter.pid, {:runner_task_result, workspace_id, task_id, task})
        if waiter.from, do: GenServer.reply(waiter.from, :ready)

        state
        |> demonitor_waiter(waiter)
        |> cancel_check(waiter.check_ref)
      end)

    {:noreply, state}
  end

  def handle_cast(
        {:notify_started, %{workspace_id: workspace_id, task_id: task_id} = task},
        state
      ) do
    state.waiters
    |> Map.get({workspace_id, task_id}, [])
    |> Enum.each(fn waiter ->
      if waiter.notify_started? do
        send(waiter.pid, {:runner_task_started, workspace_id, task_id, task})
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.checks, ref) do
      {nil, _checks} ->
        {:noreply, state}

      {%{key: key, monitor_ref: monitor_ref}, checks} ->
        state = %{state | checks: checks}
        {:noreply, finish_check(state, key, monitor_ref, result)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.checks, ref) do
      {%{key: key, monitor_ref: monitor_ref}, checks} ->
        state = %{state | checks: checks}

        {:noreply,
         fail_check(
           state,
           key,
           monitor_ref,
           {:runner_task_result_read_failed, reason}
         )}

      {nil, _checks} ->
        handle_waiter_down(state, ref)
    end
  end

  defp finish_check(state, key, monitor_ref, {:ok, %{status: status} = task})
       when status in @terminal do
    case take_waiter(state, key, monitor_ref) do
      {nil, state} ->
        state

      {waiter, state} ->
        {workspace_id, task_id} = key
        send(waiter.pid, {:runner_task_result, workspace_id, task_id, task})
        GenServer.reply(waiter.from, :ready)
        demonitor_waiter(state, waiter)
    end
  end

  # A task already running at subscription time gets its started notification
  # from the check itself, covering restarts and runners that raced ahead of
  # the subscription.
  defp finish_check(state, key, monitor_ref, {:ok, task}) do
    update_waiter(state, key, monitor_ref, fn waiter ->
      if waiter.notify_started? and task.status == :running do
        {workspace_id, task_id} = key
        send(waiter.pid, {:runner_task_started, workspace_id, task_id, task})
      end

      GenServer.reply(waiter.from, :waiting)
      %{waiter | check_ref: nil, from: nil}
    end)
  end

  defp finish_check(state, key, monitor_ref, {:error, reason}),
    do: fail_check(state, key, monitor_ref, reason)

  defp finish_check(state, key, monitor_ref, invalid),
    do: fail_check(state, key, monitor_ref, {:invalid_runner_task_read_result, invalid})

  defp fail_check(state, key, monitor_ref, reason) do
    case take_waiter(state, key, monitor_ref) do
      {nil, state} ->
        state

      {waiter, state} ->
        GenServer.reply(waiter.from, {:error, reason})
        demonitor_waiter(state, waiter)
    end
  end

  defp handle_waiter_down(state, ref) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {key, monitors} ->
        state = %{state | monitors: monitors}

        case take_waiter(state, key, ref) do
          {nil, state} ->
            {:noreply, state}

          {%{from: from} = waiter, state} when not is_nil(from) ->
            GenServer.reply(from, {:error, :runner_task_result_subscriber_down})
            {:noreply, cancel_check(state, waiter.check_ref)}

          {waiter, state} ->
            {:noreply, cancel_check(state, waiter.check_ref)}
        end
    end
  end

  defp take_waiter(state, key, monitor_ref) do
    {matching, remaining} =
      state.waiters
      |> Map.get(key, [])
      |> Enum.split_with(&(&1.monitor_ref == monitor_ref))

    waiters =
      case remaining do
        [] -> Map.delete(state.waiters, key)
        _entries -> Map.put(state.waiters, key, remaining)
      end

    {List.first(matching), %{state | waiters: waiters}}
  end

  defp update_waiter(state, key, monitor_ref, fun) do
    waiters =
      Map.update(state.waiters, key, [], fn entries ->
        Enum.map(entries, fn
          %{monitor_ref: ^monitor_ref} = waiter -> fun.(waiter)
          waiter -> waiter
        end)
      end)

    %{state | waiters: waiters}
  end

  defp demonitor_waiter(state, waiter) do
    Process.demonitor(waiter.monitor_ref, [:flush])
    %{state | monitors: Map.delete(state.monitors, waiter.monitor_ref)}
  end

  defp cancel_check(state, nil), do: state

  defp cancel_check(state, check_ref) do
    case Map.pop(state.checks, check_ref) do
      {nil, _checks} ->
        state

      {%{task_pid: task_pid}, checks} ->
        _ = Task.Supervisor.terminate_child(state.task_supervisor, task_pid)
        Process.demonitor(check_ref, [:flush])
        %{state | checks: checks}
    end
  end

  defp get_task(workspace_id, task_id) do
    Persistence.stores().runner_tasks.get(%Q.GetRunnerTask{
      workspace_context: SystemContext.workspace(workspace_id, :runner_task_result_router),
      task_id: task_id
    })
  end
end
