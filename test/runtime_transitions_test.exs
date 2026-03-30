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

    state = %State{
      steps: %{
        ref_a => %StepState{ref: ref_a, status: :ready, downstream: [ref_b]},
        ref_b => %StepState{ref: ref_b, status: :pending, upstream: [ref_a]}
      },
      ready_queue: [ref_a]
    }

    assert {:ok, state, [{:step_started, ^ref_a}]} = StepTransitions.start_step(state, ref_a)

    assert {:ok, state, events} =
             StepTransitions.complete_success(state, ref_a, :ok, %{})

    assert {:step_finished, ref_a} in events
    assert {:step_ready, ref_b} in events

    assert state.steps[ref_b].status == :ready
    assert ref_b in state.ready_queue
  end

  test "finalize unresolved marks pending and ready steps deterministically" do
    ref_pending = {__MODULE__, :pending_step}
    ref_ready = {__MODULE__, :ready_step}
    ref_done = {__MODULE__, :done_step}

    state = %State{
      steps: %{
        ref_pending => %StepState{ref: ref_pending, status: :pending},
        ref_ready => %StepState{ref: ref_ready, status: :ready},
        ref_done => %StepState{ref: ref_done, status: :success}
      },
      ready_queue: [ref_ready]
    }

    {state, events} = StepTransitions.finalize_unresolved(state, :skipped)

    assert state.steps[ref_pending].status == :skipped
    assert state.steps[ref_ready].status == :skipped
    assert state.steps[ref_done].status == :success
    assert state.ready_queue == []
    assert Enum.count(events, &(elem(&1, 0) == :step_skipped)) == 2
  end

  test "finalize unresolved can mark timed_out deterministically" do
    ref_pending = {__MODULE__, :pending_step}

    state = %State{
      steps: %{
        ref_pending => %StepState{ref: ref_pending, status: :pending}
      }
    }

    {state, events} = StepTransitions.finalize_unresolved(state, :timed_out)
    assert state.steps[ref_pending].status == :timed_out
    assert events == [{:step_timed_out, ref_pending}]
  end
end
