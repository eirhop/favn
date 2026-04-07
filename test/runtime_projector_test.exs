defmodule Favn.Runtime.ProjectorTest do
  use ExUnit.Case, async: true

  alias Favn.Runtime.Projector
  alias Favn.Runtime.State
  alias Favn.Runtime.StepState

  test "asset_results use deterministic canonical step when multiple node_keys share one ref" do
    ref = {__MODULE__, :windowed_asset}
    started_at = DateTime.utc_now()
    finished_at = DateTime.add(started_at, 5, :millisecond)

    state = %State{
      run_id: "run-1",
      plan: %Favn.Plan{},
      target_refs: [ref],
      target_node_keys: [{ref, "w1"}],
      run_status: :running,
      steps: %{
        {ref, "w1"} => %StepState{
          ref: ref,
          node_key: {ref, "w1"},
          stage: 0,
          status: :retrying,
          attempt: 1,
          max_attempts: 3,
          started_at: started_at
        },
        {ref, "w2"} => %StepState{
          ref: ref,
          node_key: {ref, "w2"},
          stage: 0,
          status: :success,
          attempt: 2,
          max_attempts: 3,
          started_at: started_at,
          finished_at: finished_at,
          duration_ms: 5,
          attempts: [
            %{attempt: 1, status: :error},
            %{attempt: 2, status: :ok}
          ]
        }
      }
    }

    run = Projector.to_public_run(state)
    result = run.asset_results[ref]

    assert result.status == :ok
    assert result.attempt_count == 2
    assert result.duration_ms == 5
  end
end
