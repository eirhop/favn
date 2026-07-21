defmodule FavnOrchestrator.RunServer.Execution.StageAttemptStateTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageResult
  alias FavnOrchestrator.RunState

  test "keeps settlement lists as reverse accumulators until finalization" do
    run =
      RunState.new(
        id: "stage-attempt-state",
        manifest_version_id: "manifest-version",
        manifest_content_hash: "manifest-hash",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {MyApp.Assets.StageAttempt, :asset}
      )

    state = StageAttemptState.new(run, [:first, :second], [], [], MapSet.new())

    state =
      StageAttemptState.record_result(
        state,
        run,
        Enum.reverse([:third, :fourth], state.results),
        [:retry_two, :retry_one],
        %{},
        nil,
        MapSet.new()
      )

    assert state.results == [:fourth, :third, :second, :first]
    assert StageAttemptState.settled_results(state) == [:first, :second, :third, :fourth]
    assert StageAttemptState.retry_node_keys(state) == [:retry_one, :retry_two]

    assert {:ok, ^run, [:first, :second, :third, :fourth], [:retry_one, :retry_two], [], %{}} =
             StageResult.finalize(state)
  end

  test "keeps the first resource-admission failure while sibling work proceeds" do
    run =
      RunState.new(
        id: "stage-admission-failure",
        manifest_version_id: "manifest-version",
        manifest_content_hash: "manifest-hash",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {MyApp.Assets.StageAttempt, :asset}
      )

    first_key = {{__MODULE__, :first}, nil}
    second_key = {{__MODULE__, :second}, nil}
    first = %{status: :error, error: :circuit_open, node_statuses: %{first_key => :blocked}}
    second = %{status: :error, error: :another_circuit, node_statuses: %{second_key => :blocked}}

    state = StageAttemptState.new(run, [], [], [], MapSet.new(), first)
    next = StageAttemptState.add_admission_failure(state, second)

    assert next.terminal_failure == first
    assert next.node_statuses == %{first_key => :blocked, second_key => :blocked}
  end
end
