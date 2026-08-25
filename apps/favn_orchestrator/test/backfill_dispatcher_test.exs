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

  test "unknown recovery state retries regardless of the original error" do
    assert BackfillDispatcher.submission_error_disposition(
             :invalid_run_submission_intent,
             :unavailable
           ) == :retry
  end

  test "proven-missing explicitly retryable submissions retry" do
    transient = Error.new(:unavailable, "submission unavailable", retryable?: true)

    assert BackfillDispatcher.submission_error_disposition(transient, :missing) == :retry
  end

  test "proven-missing deterministic submissions fail even for grouped work" do
    assert BackfillDispatcher.submission_error_disposition(
             :invalid_run_submission_intent,
             :missing
           ) == :fail

    non_retryable = Error.new(:invalid, "invalid submission")
    assert BackfillDispatcher.submission_error_disposition(non_retryable, :missing) == :fail
  end
end
