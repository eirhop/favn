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

  test "new runs require an immutable runner release identity" do
    opts = [
      id: "run-release-required",
      manifest_version_id: "manifest-version",
      manifest_content_hash: String.duplicate("a", 64),
      asset_ref: {__MODULE__, :asset}
    ]

    assert_raise KeyError, fn -> RunState.new(opts) end

    run = RunState.new(Keyword.put(opts, :required_runner_release_id, release_id()))

    assert_raise ArgumentError, ~r/release identity is immutable/, fn ->
      RunState.transition(run,
        required_runner_release_id: FavnTestSupport.runner_release_id(:alternate)
      )
    end
  end

  defp run_state do
    RunState.new(
      id: "run-step-persistence",
      workspace_id: "workspace-step-persistence",
      manifest_version_id: "manifest-version",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: release_id(),
      asset_ref: {__MODULE__, :asset}
    )
  end

  defp release_id, do: FavnTestSupport.runner_release_id()
end
