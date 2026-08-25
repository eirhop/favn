defmodule FavnOrchestrator.BackfillDispatcherTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.BackfillDispatcher
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunSubmission

  test "recovers a durable submission when enqueue committed but its reply was lost" do
    run_missing = {:error, Error.new(:not_found, "run not found")}
    submission = struct(RunSubmission)

    assert :reserved =
             BackfillDispatcher.reserved_identity_after_submit_error(
               run_missing,
               {:ok, submission}
             )
  end

  test "does not convert an unavailable recovery read into a safe failure" do
    run_missing = {:error, Error.new(:not_found, "run not found")}
    unavailable = Error.new(:unavailable, "submission read unavailable")

    assert {:unknown, ^unavailable} =
             BackfillDispatcher.reserved_identity_after_submit_error(
               run_missing,
               {:error, unavailable}
             )
  end
end
