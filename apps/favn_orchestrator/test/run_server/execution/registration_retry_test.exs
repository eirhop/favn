defmodule FavnOrchestrator.RunServer.Execution.RegistrationRetryTest do
  use ExUnit.Case, async: true
  alias FavnOrchestrator.RunServer.Execution.RegistrationRetry, as: Retry
  alias FavnOrchestrator.Persistence.Error

  @now ~U[2026-09-23 13:41:00.000000Z]
  @pending %{entry: %{asset_step_id: "step", task_id: "task"}, attempt: 1, stage: 0}
  @step %{phase: :outcome, status: :ok, task_id: "task", attempt: 1, stage: 0}
  @error Error.new(:conflict, "database busy", retryable?: true)

  test "slots and absolute deadline survive restart; interrupted dispatch spends its slot" do
    {:ok, original} = Retry.next(nil, @pending, @error, @now, 100)
    assert {:ok, restored} = Retry.restore(Retry.event(original), @step, nil)
    assert restored.slots == 1
    assert restored.deadline_at == DateTime.add(@now, 30, :second)
    assert {:error, :registration_retry_exhausted} = Retry.next(restored, @pending, @error)

    assert {:error, :invalid_registration_retry_event} =
             Retry.restore(Retry.event(original), @step, restored)

    assert {:error, :invalid_registration_retry_event} =
             Retry.restore(Retry.event(original), %{@step | task_id: "other"}, nil)

    assert {:error, :invalid_registration_retry_event} =
             Retry.restore(Retry.event(original), %{@step | status: :error}, nil)
  end

  test "neither slots, wall time nor monotonic time can extend the budget" do
    retry =
      Enum.reduce(1..8, nil, fn slot, previous ->
        assert {:ok, next} = Retry.next(previous, @pending, @error, @now, 100)
        assert next.slots == slot
        next
      end)

    assert {:error, :registration_retry_exhausted} =
             Retry.next(retry, @pending, @error, @now, 100)

    assert {:ok, first} = Retry.next(nil, @pending, @error, @now, 100)

    assert {:error, :registration_retry_exhausted} =
             Retry.next(first, @pending, @error, DateTime.add(@now, 31, :second), 100)

    assert {:error, :registration_retry_exhausted} =
             Retry.next(first, @pending, @error, DateTime.add(@now, -20, :second), 30_101)
  end

  test "only explicitly retryable persistence causes qualify" do
    assert Retry.retryable?({:registration, {:marker, @error}})

    for reason <- [
          :runner_task_timeout,
          {:post_step_worker_down, :timeout},
          {:cleanup_read_requires_reconciliation, "task"},
          Error.new(:fenced, "lost", retryable?: true),
          Error.new(:unavailable, "missing evidence")
        ] do
      refute Retry.retryable?(reason)
    end
  end
end
