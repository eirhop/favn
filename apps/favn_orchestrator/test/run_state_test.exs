defmodule FavnOrchestrator.RunStateTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunState

  test "step snapshots remain non-terminal until a terminal event finalizes the run" do
    run = run_state()

    step_snapshot =
      run
      |> RunState.transition(status: :ok, result: %{status: :ok})
      |> RunState.for_step_persistence()

    assert step_snapshot.status == :running
    assert step_snapshot.result == nil
    refute RunState.finalized?(step_snapshot)

    finalized =
      RunState.transition(run,
        status: :ok,
        result: %{status: :ok},
        metadata: %{terminal_event_type: :run_finished}
      )

    assert RunState.for_step_persistence(finalized) == finalized
  end

  defp run_state do
    RunState.new(
      id: "run-step-persistence",
      workspace_id: "workspace-step-persistence",
      manifest_version_id: "manifest-version",
      manifest_content_hash: String.duplicate("a", 64),
      asset_ref: {__MODULE__, :asset}
    )
  end
end
