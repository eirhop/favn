defmodule Favn.RuntimeTransitionsTest do
  use ExUnit.Case

  alias Favn.Runtime.State
  alias Favn.Runtime.StepState
  alias Favn.Runtime.Transitions.Run, as: RunTransitions
  alias Favn.Runtime.Transitions.Step, as: StepTransitions

  test "run transitions validate lifecycle" do
    state = %State{run_status: :pending}

    assert {:ok, state, [:run_started]} = RunTransitions.apply(state, :start)
    assert state.run_status == :running

    assert {:ok, terminal, [:run_finished]} = RunTransitions.apply(state, :mark_success)
    assert terminal.run_status == :success

    assert {:error, {:invalid_run_transition, :success, :start}} =
             RunTransitions.apply(terminal, :start)
  end

  test "run timeout transitions validate lifecycle" do
    state = %State{run_status: :pending}
    assert {:ok, state, [:run_started]} = RunTransitions.apply(state, :start)
    assert {:ok, state, [:run_timeout_triggered]} = RunTransitions.apply(state, :request_timeout)
    assert state.run_status == :timing_out
    assert {:ok, state, [:run_timed_out]} = RunTransitions.apply(state, :mark_timed_out)
    assert state.run_status == :timed_out
  end

  test "step transitions unlock downstream only after upstream success" do
    ref_a = {__MODULE__, :a}
    ref_b = {__MODULE__, :b}
    key_a = {ref_a, nil}
    key_b = {ref_b, nil}

    state = %State{
      steps: %{
        key_a => %StepState{ref: ref_a, node_key: key_a, status: :ready, downstream: [key_b]},
        key_b => %StepState{ref: ref_b, node_key: key_b, status: :pending, upstream: [key_a]}
      },
      ready_queue: [key_a]
    }

    assert {:ok, state, [{:step_started, ^key_a, _payload}]} =
             StepTransitions.start_step(state, key_a)

    assert {:ok, state, events} =
             StepTransitions.complete_success(state, key_a, %{})

    assert {:step_finished, ^key_a, _payload} = hd(events)
    assert {:step_ready, key_b} in events

    assert state.steps[key_b].status == :ready
    assert key_b in state.ready_queue
  end

  test "finalize unresolved marks pending and ready steps deterministically" do
    ref_pending = {__MODULE__, :pending_step}
    ref_ready = {__MODULE__, :ready_step}
    ref_done = {__MODULE__, :done_step}
    key_pending = {ref_pending, nil}
    key_ready = {ref_ready, nil}
    key_done = {ref_done, nil}

    state = %State{
      steps: %{
        key_pending => %StepState{ref: ref_pending, node_key: key_pending, status: :pending},
        key_ready => %StepState{ref: ref_ready, node_key: key_ready, status: :ready},
        key_done => %StepState{ref: ref_done, node_key: key_done, status: :success}
      },
      ready_queue: [key_ready]
    }

    {state, events} = StepTransitions.finalize_unresolved(state, :skipped)

    assert state.steps[key_pending].status == :skipped
    assert state.steps[key_ready].status == :skipped
    assert state.steps[key_done].status == :success
    assert state.ready_queue == []
    assert Enum.count(events, &(elem(&1, 0) == :step_skipped)) == 2
  end

  test "finalize unresolved can mark timed_out deterministically" do
    ref_pending = {__MODULE__, :pending_step}
    key_pending = {ref_pending, nil}

    state = %State{
      steps: %{
        key_pending => %StepState{ref: ref_pending, node_key: key_pending, status: :pending}
      }
    }

    {state, events} = StepTransitions.finalize_unresolved(state, :timed_out)
    assert state.steps[key_pending].status == :timed_out
    assert events == [{:step_timed_out, key_pending}]
  end

  test "retry scheduling moves running step to retrying and back to ready" do
    ref = {__MODULE__, :retry_step}
    key = {ref, nil}

    state = %State{
      steps: %{
        key => %StepState{ref: ref, node_key: key, status: :ready, max_attempts: 3}
      },
      ready_queue: [key]
    }

    assert {:ok, state, _} = StepTransitions.start_step(state, key)
    assert state.steps[key].attempt == 1

    assert {:ok, state, [{:step_retry_scheduled, ^key, _payload}]} =
             StepTransitions.schedule_retry(state, key, %{kind: :error, reason: :boom}, 0)

    assert state.steps[key].status == :retrying
    assert {:ok, state, [{:step_ready, ^key}]} = StepTransitions.requeue_retry(state, key)
    assert state.steps[key].status == :ready
  end
end
