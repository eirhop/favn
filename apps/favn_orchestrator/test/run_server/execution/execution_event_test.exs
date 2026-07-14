defmodule FavnOrchestrator.RunServer.Execution.ExecutionEventTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState

  test "stale await monitor and timeout messages do not remove the current await" do
    execution_id = "exec_1"
    monitor_ref = make_ref()
    timeout_token = make_ref()

    await = %{
      pid: self(),
      monitor_ref: monitor_ref,
      timeout_token: timeout_token,
      timeout_ref: make_ref(),
      entry: %{execution_id: execution_id},
      kind: :pipeline
    }

    state = %RunExecutionState{
      awaits: %{execution_id => await},
      await_monitors: %{monitor_ref => execution_id},
      await_timers: %{timeout_token => execution_id}
    }

    assert {:cont, ^state} =
             Execution.handle_event(
               state,
               {:runner_await_down, execution_id, make_ref(), :stale}
             )

    assert {:cont, ^state} =
             Execution.handle_event(state, {:attempt_timeout, execution_id, make_ref()})
  end

  test "stale admission generations do not remove the current waiter" do
    waiter = %{waiter_id: "waiter_1", wake_generation: 2}
    state = %RunExecutionState{admission_waiters: %{waiter.waiter_id => waiter}}

    assert {:cont, ^state} =
             Execution.handle_event(
               state,
               {:execution_admission_wakeup, waiter.waiter_id, 1}
             )
  end
end
