defmodule FavnRunner.Shutdown do
  @moduledoc """
  Coordinates bounded runner draining before OTP stops the supervision tree.

  New work is rejected first. Existing workers may finish until the configured
  deadline; remaining executions are then cancelled through the ordinary runner
  result path so waiting control-plane callers receive an honest terminal result.
  """

  alias FavnRunner.Lifecycle
  alias FavnRunner.OperationalEvents
  alias FavnRunner.Server

  @default_poll_interval_ms 25

  @doc "Drains active runner admissions and executions within the frozen timeout."
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
      cancellation_timeout_ms(opts) + 1_000
  end

  defp replacement_opts(opts, wait_deadline) do
    remaining_ms =
      max(
        wait_deadline - System.monotonic_time(:millisecond) - cancellation_timeout_ms(opts) -
          1_000,
        0
      )

    Keyword.put(opts, :timeout_ms, remaining_ms)
  end

  defp cancellation_timeout_ms(opts) do
    opts |> Keyword.get(:server_opts, []) |> Keyword.get(:timeout, 5_000)
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
    server_opts = Keyword.get(opts, :server_opts, [])
    server = Keyword.get(opts, :server, Server)
    started_at = System.monotonic_time(:millisecond)
    deadline = started_at + timeout_ms

    initial = snapshot(lifecycle, server, server_opts, deadline)

    OperationalEvents.emit(
      :drain_started,
      %{
        active_admissions: initial.active_admissions,
        active_executions: initial.active_executions
      },
      %{}
    )

    drain_outcome = await_idle(lifecycle, server, server_opts, deadline, poll_interval_ms)

    if drain_outcome != :idle do
      :ok = Lifecycle.stop(lifecycle)
    end

    cancellation_timeout_ms = cancellation_timeout_ms(opts)
    cancellation_deadline = System.monotonic_time(:millisecond) + cancellation_timeout_ms
    post_drain_deadline = cancellation_deadline + 1_000

    cancellation =
      case drain_outcome do
        :idle ->
          %{status: :not_required, cancelled_executions: 0}

        _not_idle ->
          cancel_opts =
            bounded_server_opts(server_opts, cancellation_deadline, cancellation_timeout_ms)

          case server.cancel_active(%{kind: :runner_shutdown_deadline}, cancel_opts) do
            {:ok, count} -> %{status: :recorded, cancelled_executions: count}
            {:error, reason} -> %{status: :unknown, error: reason}
          end
      end

    final = snapshot(lifecycle, server, server_opts, post_drain_deadline)
    duration_ms = System.monotonic_time(:millisecond) - started_at

    result = %{
      status: shutdown_status(drain_outcome, initial, final, cancellation),
      duration_ms: duration_ms,
      active_admissions_at_start: initial.active_admissions,
      active_executions_at_start: initial.active_executions,
      active_admissions_remaining: final.active_admissions,
      active_executions_remaining: final.active_executions,
      cancellation: cancellation,
      cancelled_executions: Map.get(cancellation, :cancelled_executions, :unknown)
    }

    OperationalEvents.emit(
      :drain_completed,
      %{
        duration_ms: duration_ms,
        cancelled_executions: result.cancelled_executions
      },
      %{status: result.status, cancellation_status: cancellation.status}
    )

    :ok = Lifecycle.complete_shutdown(result, lifecycle)
    {:ok, result}
  end

  defp await_idle(lifecycle, server, server_opts, deadline, poll_interval_ms) do
    current = snapshot(lifecycle, server, server_opts, deadline)

    cond do
      current.active_admissions == 0 and current.active_executions == 0 ->
        :idle

      current.active_admissions == :unknown or current.active_executions == :unknown ->
        :unknown

      System.monotonic_time(:millisecond) >= deadline ->
        :deadline

      true ->
        remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)
        Process.sleep(min(poll_interval_ms, remaining_ms))
        await_idle(lifecycle, server, server_opts, deadline, poll_interval_ms)
    end
  end

  defp snapshot(lifecycle, server, server_opts, deadline) do
    lifecycle_diagnostics = Lifecycle.diagnostics(lifecycle)

    active_executions =
      case remaining_ms(deadline) do
        0 ->
          :unknown

        _remaining ->
          case server.active_execution_count(bounded_server_opts(server_opts, deadline, 1_000)) do
            {:ok, count} -> count
            {:error, _reason} -> :unknown
          end
      end

    %{
      active_admissions: lifecycle_diagnostics.active_admissions,
      active_executions: active_executions
    }
  end

  defp bounded_server_opts(server_opts, deadline, default_timeout_ms) do
    timeout_ms =
      min(
        Keyword.get(server_opts, :timeout, default_timeout_ms),
        max(remaining_ms(deadline), 1)
      )

    Keyword.put(server_opts, :timeout, timeout_ms)
  end

  defp remaining_ms(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp shutdown_status(outcome, initial, final, cancellation) do
    values = [
      initial.active_admissions,
      initial.active_executions,
      final.active_admissions,
      final.active_executions
    ]

    cond do
      :unknown in values ->
        :state_unknown

      outcome == :idle and final.active_admissions == 0 and final.active_executions == 0 ->
        :drained

      outcome == :idle ->
        :state_unknown

      outcome == :deadline and cancellation.status == :recorded ->
        :cancelled_at_deadline

      true ->
        :state_unknown
    end
  end
end
