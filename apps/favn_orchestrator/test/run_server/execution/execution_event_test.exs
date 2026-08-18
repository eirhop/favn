defmodule FavnOrchestrator.RunServer.Execution.ExecutionEventTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunState

  test "stale await monitor and timeout messages do not remove the current await" do
    task_id = "rt_1"
    monitor_ref = make_ref()
    timeout_token = make_ref()

    await = %{
      pid: self(),
      monitor_ref: monitor_ref,
      timeout_token: timeout_token,
      timeout_ref: make_ref(),
      entry: %{task_id: task_id},
      kind: :pipeline
    }

    state = %RunExecutionState{
      awaits: %{task_id => await},
      await_monitors: %{monitor_ref => task_id},
      await_timers: %{timeout_token => task_id}
    }

    assert {:cont, ^state} =
             Execution.handle_event(
               state,
               {:runner_await_down, task_id, make_ref(), :stale}
             )

    assert {:cont, ^state} =
             Execution.handle_event(state, {:attempt_timeout, task_id, make_ref()})
  end

  test "runner started signals are ignored without a live, unpersisted await" do
    task_id = "rt_started"

    missing = %RunExecutionState{awaits: %{}}

    assert {:cont, ^missing} =
             Execution.handle_event(missing, {:runner_task_started, task_id, %{}})

    await = %{
      pid: self(),
      monitor_ref: make_ref(),
      timeout_token: make_ref(),
      timeout_ref: make_ref(),
      entry: %{task_id: task_id},
      kind: :pipeline,
      started_persisted?: true
    }

    persisted = %RunExecutionState{awaits: %{task_id => await}}

    assert {:cont, ^persisted} =
             Execution.handle_event(persisted, {:runner_task_started, task_id, %{}})
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

  test "stale deferred admission refill timers are ignored" do
    timer_token = make_ref()

    state = %RunExecutionState{
      status: :admission_wait,
      awaits: %{"rt_active" => %{}},
      admission_timers: %{
        timer_token => %{
          timer_ref: make_ref(),
          payload: %{kind: :deferred_refill, stage_index: 1}
        }
      }
    }

    assert {:cont, next} =
             Execution.handle_event(state, {:stage_admission_timeout, timer_token})

    assert next.status == :admission_wait
    assert next.awaits == state.awaits
    assert next.admission_timers == %{}

    assert {:cont, ^next} =
             Execution.handle_event(next, {:stage_admission_timeout, timer_token})
  end

  test "terminal sibling failure still refills deferred work and schedules safe retries" do
    run =
      RunState.new(
        id: "continue-independent-siblings",
        manifest_version_id: "manifest-version",
        manifest_content_hash: "manifest-hash",
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: {__MODULE__, :asset}
      )

    failed = %{status: :error, error: :terminal_failure}

    deferred =
      StageAttemptState.new(
        run,
        [],
        [],
        [{{__MODULE__, :later_sibling}, nil}],
        MapSet.new(),
        failed
      )

    assert :refill == Execution.pipeline_progress_action(deferred, 0, 0)

    retrying = %{deferred | deferred_node_keys: [], retry_refs: [{{__MODULE__, :retry}, nil}]}
    assert :retry == Execution.pipeline_progress_action(retrying, 0, 0)
  end
end
