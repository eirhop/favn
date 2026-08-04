defmodule FavnOrchestrator.RunnerTaskRecoveryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunnerTaskRecovery

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
end
