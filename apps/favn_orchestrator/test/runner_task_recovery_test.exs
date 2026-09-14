defmodule FavnOrchestrator.RunnerTaskRecoveryTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RunnerTaskRecovery

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores

  defmodule Store do
    def release(command) do
      send(:persistent_term.get({__MODULE__, :owner}), {:release, command})
      {:error, Error.new(:internal, "release unavailable")}
    end

    def recover_expired(command) do
      send(:persistent_term.get({__MODULE__, :owner}), {:scan, self(), command})

      receive do
        {:scan_result, result} -> result
      after
        5_000 -> raise "test did not release recovery scan"
      end
    end
  end

  describe "timer ownership" do
    setup do
      :persistent_term.put({Store, :owner}, self())
      stores = struct(Stores, runner_tasks: Store)
      start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
      handler = {__MODULE__, make_ref()}

      :telemetry.attach(
        handler,
        [:favn, :runner_task_recovery, :tick],
        fn _, measures, meta, pid ->
          send(pid, {:tick, measures, meta})
        end,
        self()
      )

      on_exit(fn ->
        :persistent_term.erase({Store, :owner})
        :telemetry.detach(handler)
      end)

      :ok
    end

    test "startup, duplicate messages and disconnect churn keep one serial chain" do
      pid = start_supervised!({RunnerTaskRecovery, interval_ms: 60_000})
      assert_receive {:scan, ^pid, first}

      for _ <- 1..100 do
        send(pid, {:recover, make_ref()})
        send(pid, :recover)
        send(pid, {:runner_down, "runner", 1, :closed})
      end

      refute_receive {:scan, _, _}, 0
      send(pid, {:scan_result, {:ok, []}})
      first_state = :sys.get_state(pid)
      assert is_reference(first_state.tick_token)
      assert_receive {:tick, %{recovered_count: 0, error_count: 0, duration: duration}, %{}}
      assert duration >= 0

      token = first_state.tick_token
      send(pid, {:recover, token})
      assert_receive {:scan, ^pid, second}
      refute first.command_id == second.command_id
      send(pid, {:recover, token})
      send(pid, {:scan_result, {:ok, []}})
      next_state = :sys.get_state(pid)
      refute next_state.tick_token == token
      assert_receive {:tick, %{recovered_count: 0, error_count: 0}, %{}}
      refute_receive {:scan, _, _}, 0
      refute_receive {:tick, _, _}, 0
    end

    test "telemetry distinguishes claimed tasks from failed dispositions" do
      pid = start_supervised!({RunnerTaskRecovery, interval_ms: 60_000})
      assert_receive {:scan, ^pid, command}

      task = %{
        workspace_id: "workspace",
        task_id: "rt_task",
        status: :assigned,
        assignment_generation: 2,
        assigned_at: command.occurred_at
      }

      send(pid, {:scan_result, {:ok, [task]}})
      :sys.get_state(pid)
      assert_receive {:release, %{task_id: "rt_task", disposition: :requeue}}
      assert_receive {:tick, %{recovered_count: 1, error_count: 1}, %{}}
    end

    test "storage failure schedules one successor with a new command and restart discards old tokens" do
      pid = start_supervised!({RunnerTaskRecovery, interval_ms: 60_000})
      assert_receive {:scan, ^pid, first}
      send(pid, {:scan_result, {:error, Error.new(:internal, "unavailable")}})
      state = :sys.get_state(pid)
      assert state.last_failure == :storage
      assert_receive {:tick, %{recovered_count: 0, error_count: 1}, %{}}

      send(pid, {:recover, state.tick_token})
      assert_receive {:scan, ^pid, second}
      refute first.command_id == second.command_id
      send(pid, {:scan_result, {:ok, []}})
      healthy = :sys.get_state(pid)
      assert healthy.last_failure == nil
      assert_receive {:tick, %{error_count: 0}, %{}}

      stop_supervised!(RunnerTaskRecovery)
      replacement = start_supervised!({RunnerTaskRecovery, interval_ms: 60_000})
      assert_receive {:scan, ^replacement, _}
      send(replacement, {:recover, healthy.tick_token})
      send(replacement, {:scan_result, {:ok, []}})
      restarted = :sys.get_state(replacement)
      refute restarted.tick_token == healthy.tick_token
      assert_receive {:tick, %{error_count: 0}, %{}}
      refute_receive {:scan, _, _}, 0
    end
  end

  test "an acknowledged cancellation is terminal only when retry safety proves no write ambiguity" do
    acknowledged_at = ~U[2026-01-01 00:00:00Z]

    assert :cancelled ==
             RunnerTaskRecovery.recovery_disposition(%{
               status: :cancelling,
               cancellation_acknowledged_at: acknowledged_at,
               retry_class: :safe_to_retry
             })

    assert :unknown ==
             RunnerTaskRecovery.recovery_disposition(%{
               status: :cancelling,
               cancellation_acknowledged_at: acknowledged_at,
               retry_class: :unknown_do_not_retry
             })
  end

  test "pre-execution tasks remain safe to requeue" do
    assert :requeue ==
             RunnerTaskRecovery.recovery_disposition(%{
               status: :preparing,
               cancellation_acknowledged_at: nil,
               retry_class: :unknown_do_not_retry,
               assignment_generation: 2
             })
  end

  test "a task that exhausts its assignment budget is released as unknown, not requeued" do
    task = %{
      status: :preparing,
      cancellation_acknowledged_at: nil,
      retry_class: :unknown_do_not_retry,
      assignment_generation: 12
    }

    assert :unknown == RunnerTaskRecovery.recovery_disposition(task)

    assert :requeue ==
             RunnerTaskRecovery.recovery_disposition(%{task | assignment_generation: 11})

    assert :unknown == RunnerTaskRecovery.recovery_disposition(task, 12)
    assert :requeue == RunnerTaskRecovery.recovery_disposition(task, 13)
  end

  test "the assignment budget never rescues an unsafe status back to requeue" do
    assert :unknown ==
             RunnerTaskRecovery.recovery_disposition(%{
               status: :running,
               cancellation_acknowledged_at: nil,
               retry_class: :unknown_do_not_retry,
               assignment_generation: 1
             })
  end

  test "a budget-exhausted release names the budget in its error envelope" do
    reason = RunnerTaskRecovery.unknown_recovery_reason(%{assignment_generation: 12}, 12)

    assert reason.type == :runner_task_assignment_budget_exhausted
    assert reason.outcome == :unknown
    refute reason.retryable?
    assert reason.details.assignment_generation == 12
    assert reason.details.assignment_budget == 12
    assert :ok = Favn.Contracts.RunnerError.validate(reason)
  end

  test "an unknown release under budget keeps the generic envelope" do
    reason = RunnerTaskRecovery.unknown_recovery_reason(%{assignment_generation: 3}, 12)

    assert reason.type == :runner_error
    assert reason.outcome == :unknown
    refute reason.retryable?
    assert :ok = Favn.Contracts.RunnerError.validate(reason)
  end
end
