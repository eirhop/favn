defmodule FavnOrchestrator.RunServer.Execution.PreSubmitFailureTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerError
  alias FavnOrchestrator.RunServer.Execution.PreSubmitFailure

  test "classifies a local pre-submit error as a non-retryable safe failure" do
    assert %RunnerError{
             phase: :pre_submit,
             outcome: :safe_failure,
             retryable?: false
           } = PreSubmitFailure.normalize(:asset_not_found)
  end

  test "preserves an existing runner error without weakening its outcome" do
    error =
      RunnerError.new(
        reason: :ambiguous_dispatch,
        phase: :dispatch,
        outcome: :unknown,
        retryable?: false
      )

    assert PreSubmitFailure.normalize(error) == error
  end
end
