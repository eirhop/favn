defmodule FavnOrchestrator.Shutdown do
  @moduledoc """
  Coordinates bounded control-plane draining before OTP stops dependencies.

  The lifecycle transition rejects new mutations first. Existing admissions and
  run servers may settle until the drain deadline. Remaining runs then receive
  cancellation through the ordinary durable cancellation path; any interrupted
  or unknown outcome is left for the existing fenced recovery process.
  """

  require Logger

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager

  @default_poll_interval_ms 25
  @default_cancel_timeout_ms 15_000
  @default_settle_timeout_ms 5_000
  @post_drain_budget_ms 30_000
  @cancel_concurrency 8

  @doc "Drains active control-plane work within the frozen runtime timeout."
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
      await_existing_shutdown_result(opts, lifecycle, wait_deadline, remaining_ms)
    end
  end

  defp await_existing_shutdown_result(opts, lifecycle, wait_deadline, remaining_ms) do
    case Lifecycle.await_shutdown(remaining_ms, lifecycle) do
      {:complete, result} ->
        {:ok, result}

      {:error, :shutdown_coordinator_failed} ->
        case Lifecycle.begin_shutdown(lifecycle) do
          :leader -> do_drain(replacement_opts(opts, wait_deadline), lifecycle)
          {:complete, result} -> {:ok, result}
          :in_progress -> await_existing_shutdown(opts, lifecycle, wait_deadline)
        end

      {:error, :shutdown_wait_timeout} ->
        unknown_wait_result(lifecycle, :shutdown_wait_timeout)
    end
  end

  defp shutdown_wait_budget(opts, lifecycle) do
    Keyword.get_lazy(opts, :timeout_ms, fn -> Lifecycle.shutdown_drain_timeout_ms(lifecycle) end) +
      @post_drain_budget_ms + 1_000
  end

  defp replacement_opts(opts, wait_deadline) do
    remaining_ms =
      max(
        wait_deadline - System.monotonic_time(:millisecond) - @post_drain_budget_ms - 1_000,
        0
      )

    Keyword.put(opts, :timeout_ms, remaining_ms)
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
    cancel_timeout_ms = Keyword.get(opts, :cancel_timeout_ms, @default_cancel_timeout_ms)
    settle_timeout_ms = Keyword.get(opts, :settle_timeout_ms, @default_settle_timeout_ms)
    run_manager = Keyword.get(opts, :run_manager, RunManager)

    task_supervisor =
      Keyword.get(opts, :task_supervisor, FavnOrchestrator.RunManagerTaskSupervisor)

    started_at = System.monotonic_time(:millisecond)
    deadline = started_at + timeout_ms

    initial = snapshot(lifecycle, run_manager, deadline)

    OperationalEvents.emit(
      :orchestrator_drain_started,
      %{
        active_admissions: initial.active_admissions,
        active_runs: initial.active_runs
      },
      %{}
    )

    drain_outcome = await_idle(lifecycle, run_manager, deadline, poll_interval_ms)

    if drain_outcome != :idle do
      :ok = Lifecycle.stop(lifecycle)
    end

    post_drain_deadline = System.monotonic_time(:millisecond) + @post_drain_budget_ms

    cancellation =
      if drain_outcome == :idle do
        empty_cancellation()
      else
        cancel_active(run_manager, task_supervisor, cancel_timeout_ms, post_drain_deadline)
      end

    if drain_outcome != :idle do
      settle_deadline =
        min(System.monotonic_time(:millisecond) + settle_timeout_ms, post_drain_deadline)

      _ = await_idle(lifecycle, run_manager, settle_deadline, poll_interval_ms)
    end

    before_force_stop = snapshot(lifecycle, run_manager, post_drain_deadline)
    cancellation = summarize_settlement(cancellation, before_force_stop.active_runs)

    forced_stops =
      force_stop_remaining(run_manager, before_force_stop.active_runs, post_drain_deadline)

    final = snapshot(lifecycle, run_manager, post_drain_deadline)
    duration_ms = System.monotonic_time(:millisecond) - started_at

    result = %{
      status: shutdown_status(drain_outcome, initial, final, cancellation),
      duration_ms: duration_ms,
      active_admissions_at_start: initial.active_admissions,
      active_runs_at_start: initial.active_runs,
      active_admissions_remaining: final.active_admissions,
      active_runs_remaining: final.active_runs,
      forced_run_server_stops: forced_stops,
      cancellation: cancellation
    }

    OperationalEvents.emit(
      :orchestrator_shutdown_cancellation,
      %{
        requested: Map.get(cancellation, :requested, 0),
        request_failed: Map.get(cancellation, :request_failed, 0),
        settled: Map.get(cancellation, :settled_before_force_stop, 0),
        unknown_outcomes: Map.get(cancellation, :unknown_outcomes, 0)
      },
      %{status: result.status}
    )

    OperationalEvents.emit(:orchestrator_drain_completed, %{duration_ms: duration_ms}, result)

    Logger.info(
      "control-plane shutdown drain completed status=#{result.status} duration_ms=#{duration_ms}"
    )

    :ok = Lifecycle.complete_shutdown(result, lifecycle)
    {:ok, result}
  end

  defp await_idle(lifecycle, run_manager, deadline, poll_interval_ms) do
    current = snapshot(lifecycle, run_manager, deadline)

    cond do
      current.active_admissions == 0 and current.active_runs == 0 ->
        :idle

      current.active_admissions == :unknown or current.active_runs == :unknown ->
        :unknown

      System.monotonic_time(:millisecond) >= deadline ->
        :deadline

      true ->
        remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)
        Process.sleep(min(poll_interval_ms, remaining_ms))
        await_idle(lifecycle, run_manager, deadline, poll_interval_ms)
    end
  end

  defp snapshot(lifecycle, run_manager, deadline) do
    active_runs =
      case remaining_ms(deadline) do
        0 ->
          :unknown

        timeout_ms ->
          case active_runs(run_manager, timeout_ms) do
            {:ok, runs} -> length(runs)
            {:error, _reason} -> :unknown
          end
      end

    %{
      active_admissions: Lifecycle.diagnostics(lifecycle).active_admissions,
      active_runs: active_runs
    }
  end

  defp cancel_active(run_manager, task_supervisor, timeout_ms, post_drain_deadline) do
    deadline =
      min(System.monotonic_time(:millisecond) + timeout_ms, post_drain_deadline)

    case active_runs(run_manager, remaining_ms(deadline)) do
      {:ok, runs} ->
        runs
        |> cancel_batches(run_manager, task_supervisor, deadline, %{
          targeted: length(runs),
          requested: 0,
          request_failed: 0,
          timed_out_or_unstarted: 0
        })

      {:error, reason} ->
        %{
          targeted: :unknown,
          requested: 0,
          request_failed: :unknown,
          timed_out_or_unstarted: :unknown,
          enumeration_error: reason
        }
    end
  end

  defp cancel_batches([], _run_manager, _task_supervisor, _deadline, summary), do: summary

  defp cancel_batches(runs, run_manager, task_supervisor, deadline, summary) do
    remaining_ms = deadline - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      count = length(runs)

      %{
        summary
        | request_failed: summary.request_failed + count,
          timed_out_or_unstarted: summary.timed_out_or_unstarted + count
      }
    else
      {batch, rest} = Enum.split(runs, @cancel_concurrency)
      {tasks, start_failures} = start_cancel_tasks(batch, run_manager, task_supervisor)
      results = Task.yield_many(tasks, remaining_ms)

      batch_summary =
        Enum.reduce(results, summary, fn
          {_task, {:ok, :ok}}, acc ->
            Map.update!(acc, :requested, &(&1 + 1))

          {task, nil}, acc ->
            _ = Task.shutdown(task, :brutal_kill)

            acc
            |> Map.update!(:request_failed, &(&1 + 1))
            |> Map.update!(:timed_out_or_unstarted, &(&1 + 1))

          {_task, _failure}, acc ->
            Map.update!(acc, :request_failed, &(&1 + 1))
        end)
        |> Map.update!(:request_failed, &(&1 + start_failures))

      cancel_batches(rest, run_manager, task_supervisor, deadline, batch_summary)
    end
  end

  defp start_cancel_tasks(runs, run_manager, task_supervisor) do
    Enum.reduce(runs, {[], 0}, fn run, {tasks, failures} ->
      case start_cancel_task(run, run_manager, task_supervisor) do
        {:ok, task} -> {[task | tasks], failures}
        {:error, _reason} -> {tasks, failures + 1}
      end
    end)
  end

  defp start_cancel_task(run, run_manager, task_supervisor) do
    task =
      Task.Supervisor.async_nolink(task_supervisor, fn ->
        context = SystemContext.workspace(run.workspace_id, :shutdown_drain)

        run_manager.cancel_run(context, run.run_id, %{
          kind: :control_plane_shutdown_deadline,
          requested_by: :system
        })
      end)

    {:ok, task}
  catch
    :exit, reason -> {:error, reason}
  end

  defp active_runs(_run_manager, 0), do: {:error, :shutdown_deadline_reached}

  defp active_runs(run_manager, timeout_ms) do
    if function_exported?(run_manager, :active_runs, 1) do
      run_manager.active_runs(timeout_ms)
    else
      run_manager.active_runs()
    end
  catch
    :exit, _reason -> {:error, :run_manager_not_available}
  end

  defp stop_active_runs(_run_manager, 0), do: {:error, :shutdown_deadline_reached}

  defp stop_active_runs(run_manager, timeout_ms) do
    if function_exported?(run_manager, :stop_active_for_shutdown, 1) do
      run_manager.stop_active_for_shutdown(timeout_ms)
    else
      run_manager.stop_active_for_shutdown()
    end
  catch
    :exit, _reason -> {:error, :run_manager_not_available}
  end

  defp force_stop_remaining(run_manager, active_runs, deadline)
       when is_integer(active_runs) and active_runs > 0 do
    case stop_active_runs(run_manager, remaining_ms(deadline)) do
      {:ok, count} -> count
      {:error, _reason} -> :unknown
    end
  end

  defp force_stop_remaining(_run_manager, 0, _deadline), do: 0
  defp force_stop_remaining(_run_manager, :unknown, _deadline), do: :unknown

  defp remaining_ms(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp summarize_settlement(%{targeted: targeted} = cancellation, active_runs)
       when is_integer(targeted) and is_integer(active_runs) do
    cancellation
    |> Map.put(:locally_inactive_before_force_stop, max(targeted - active_runs, 0))
    |> Map.put(:settled_before_force_stop, 0)
    |> Map.put(:unknown_outcomes, targeted)
  end

  defp summarize_settlement(cancellation, _active_runs) do
    cancellation
    |> Map.put(:settled_before_force_stop, :unknown)
    |> Map.put(:unknown_outcomes, :unknown)
  end

  defp shutdown_status(outcome, initial, final, cancellation) do
    values = [
      initial.active_admissions,
      initial.active_runs,
      final.active_admissions,
      final.active_runs,
      Map.get(cancellation, :unknown_outcomes)
    ]

    cond do
      :unknown in values ->
        :state_unknown

      outcome == :idle and final.active_admissions == 0 and final.active_runs == 0 ->
        :drained

      outcome == :idle ->
        :state_unknown

      true ->
        :cancelled_at_deadline
    end
  end

  defp empty_cancellation do
    %{
      targeted: 0,
      requested: 0,
      request_failed: 0,
      timed_out_or_unstarted: 0,
      locally_inactive_before_force_stop: 0,
      settled_before_force_stop: 0,
      unknown_outcomes: 0
    }
  end
end
