defmodule FavnRunner.Drain do
  @moduledoc """
  Coordinates bounded draining of the durable runner-task agent.

  The agent stops claiming immediately, finishes and reports its current task,
  then exits. At the configured deadline the active executor is cancelled
  through the same result path before the application stops.
  """

  alias FavnRunner.Lifecycle
  alias FavnRunner.OperationalEvents
  alias FavnRunner.RunnerAgent
  alias FavnRunner.TaskExecutor

  @default_poll_interval_ms 25

  @doc "Drains active runner admissions and the one durable task slot."
  @spec drain(keyword()) :: {:ok, map()}
  def drain(opts \\ []) when is_list(opts) do
    lifecycle = Keyword.get(opts, :lifecycle, Lifecycle)
    wait_deadline = System.monotonic_time(:millisecond) + shutdown_wait_budget(opts, lifecycle)

    case Lifecycle.begin_shutdown(lifecycle) do
      :leader -> do_drain(opts, lifecycle)
      {:complete, result} -> {:ok, result}
      :in_progress -> await_existing_shutdown(opts, lifecycle, wait_deadline)
    end
  end

  defp await_existing_shutdown(opts, lifecycle, wait_deadline) do
    remaining_ms = wait_deadline - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      unknown_wait_result(lifecycle, :shutdown_wait_timeout)
    else
      case Lifecycle.await_shutdown(remaining_ms, lifecycle) do
        {:complete, result} ->
          {:ok, result}

        {:error, :shutdown_coordinator_failed} ->
          case Lifecycle.begin_shutdown(lifecycle) do
            :leader -> do_drain(Keyword.put(opts, :timeout_ms, max(remaining_ms, 1)), lifecycle)
            {:complete, result} -> {:ok, result}
            :in_progress -> await_existing_shutdown(opts, lifecycle, wait_deadline)
          end

        {:error, :shutdown_wait_timeout} ->
          unknown_wait_result(lifecycle, :shutdown_wait_timeout)
      end
    end
  end

  defp shutdown_wait_budget(opts, lifecycle) do
    Keyword.get_lazy(opts, :timeout_ms, fn -> Lifecycle.shutdown_drain_timeout_ms(lifecycle) end) +
      Keyword.get(opts, :cancellation_timeout_ms, 5_000) + 1_000
  end

  defp unknown_wait_result(lifecycle, reason) do
    :ok = Lifecycle.stop(lifecycle)
    {:ok, %{status: :state_unknown, lifecycle: :stopping, reason: reason}}
  end

  defp do_drain(opts, lifecycle) do
    timeout_ms =
      Keyword.get_lazy(opts, :timeout_ms, fn ->
        Lifecycle.shutdown_drain_timeout_ms(lifecycle)
      end)

    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    agent = Keyword.get(opts, :agent, RunnerAgent)
    supervisor = Keyword.get(opts, :executor_supervisor, FavnRunner.TaskExecutorSupervisor)
    initial = active_count(supervisor)
    started_at = System.monotonic_time(:millisecond)

    OperationalEvents.emit(
      :drain_started,
      %{
        active_admissions: Lifecycle.diagnostics(lifecycle).active_admissions,
        active_executions: initial
      },
      %{}
    )

    signal_drain(agent)
    deadline = started_at + timeout_ms
    first_outcome = await_idle(supervisor, deadline, poll_interval_ms)

    {outcome, cancellation} =
      case first_outcome do
        :idle ->
          {:idle, %{status: :not_required, cancelled_executions: 0}}

        :deadline ->
          cancel_count = cancel_active(agent, supervisor)

          cancellation_deadline =
            System.monotonic_time(:millisecond) +
              Keyword.get(opts, :cancellation_timeout_ms, 5_000)

          {await_idle(supervisor, cancellation_deadline, poll_interval_ms),
           %{status: :recorded, cancelled_executions: cancel_count}}
      end

    final = active_count(supervisor)
    :ok = Lifecycle.stop(lifecycle)

    result = %{
      status: shutdown_status(first_outcome, outcome, final),
      duration_ms: System.monotonic_time(:millisecond) - started_at,
      active_admissions_at_start: 0,
      active_executions_at_start: initial,
      active_admissions_remaining: 0,
      active_executions_remaining: final,
      cancellation: cancellation,
      cancelled_executions: cancellation.cancelled_executions
    }

    OperationalEvents.emit(
      :drain_completed,
      %{duration_ms: result.duration_ms, cancelled_executions: result.cancelled_executions},
      %{status: result.status, cancellation_status: cancellation.status}
    )

    :ok = Lifecycle.complete_shutdown(result, lifecycle)
    {:ok, result}
  end

  defp signal_drain(agent) do
    if process_alive?(agent), do: RunnerAgent.drain(agent)
    :ok
  end

  defp cancel_active(agent, supervisor) do
    cond do
      process_alive?(agent) ->
        :ok = RunnerAgent.cancel_and_drain(agent)
        active_count(supervisor)

      true ->
        supervisor
        |> children()
        |> Enum.count(fn pid -> TaskExecutor.cancel(pid, :runner_shutdown_deadline) == :ok end)
    end
  end

  defp await_idle(supervisor, deadline, poll_interval_ms) do
    if active_count(supervisor) == 0 do
      :idle
    else
      remaining_ms = deadline - System.monotonic_time(:millisecond)

      if remaining_ms <= 0 do
        :deadline
      else
        Process.sleep(min(poll_interval_ms, remaining_ms))
        await_idle(supervisor, deadline, poll_interval_ms)
      end
    end
  end

  defp active_count(supervisor) do
    if process_alive?(supervisor) do
      DynamicSupervisor.count_children(supervisor).active
    else
      0
    end
  end

  defp children(supervisor) do
    if process_alive?(supervisor) do
      supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.flat_map(fn
        {_id, pid, _type, _modules} when is_pid(pid) -> [pid]
        _child -> []
      end)
    else
      []
    end
  end

  defp process_alive?(name) when is_atom(name), do: is_pid(Process.whereis(name))
  defp process_alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp process_alive?(_other), do: false

  defp shutdown_status(:deadline, _outcome, 0), do: :cancelled_at_deadline
  defp shutdown_status(_first, :idle, 0), do: :drained
  defp shutdown_status(_first, _outcome, _remaining), do: :state_unknown
end
