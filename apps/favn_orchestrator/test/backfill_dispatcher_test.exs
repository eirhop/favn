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

  describe "the error a failed window records" do
    # A window fails because its run failed far more often than for any other
    # reason, and a run's stored error is already string-keyed and JSON-safe.
    # Inspecting it again would put a printed map where a reason belongs, so
    # every window would carry its own blob and one shared failure would read as
    # N different ones on the run page that groups them.
    test "an already-safe error map is recorded as it stands" do
      error = %{
        "kind" => "error",
        "type" => "backend_execution_failed",
        "phase" => "materialize",
        "reason" => "Catalog Error: Table with name opportunity does not exist!",
        "details" => %{"type" => "backend_execution_failed"}
      }

      assert BackfillDispatcher.error_payload(error) == error
    end

    test "a payload carrying only a message is recorded as it stands" do
      error = %{"kind" => "error", "message" => "the runner pool was empty"}

      assert BackfillDispatcher.error_payload(error) == error
    end

    test "an atom reason becomes that atom's name, not its inspection" do
      assert BackfillDispatcher.error_payload(:window_lease_lost) ==
               %{"reason" => "window_lease_lost"}
    end

    test "a bare term is still inspected, because nothing else can carry it" do
      assert %{"reason" => reason} =
               BackfillDispatcher.error_payload(%{reason: :invalid_backfill_pipeline_identity})

      assert reason =~ "invalid_backfill_pipeline_identity"
    end

    test "a map whose reason is not a string is not mistaken for a safe payload" do
      assert %{"reason" => reason} =
               BackfillDispatcher.error_payload(%{"reason" => :not_a_string})

      assert reason =~ "not_a_string"
      refute reason == "not_a_string"
    end

    test "an admission decision keeps its structured details" do
      details = %{target_id: "crm.orders", compatibility_status: :drifted}

      assert %{
               "kind" => "admission",
               "reason" => "target_drift",
               "details" => %{"target_id" => "crm.orders"}
             } = BackfillDispatcher.error_payload({:target_drift, details})
    end
  end
end
