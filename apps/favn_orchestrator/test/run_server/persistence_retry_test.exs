defmodule FavnOrchestrator.RunServer.PersistenceRetryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunState

  test "retry budget keeps its first cause and does not extend on further failures" do
    first =
      Error.new(:conflict, "history busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    run = struct(RunState, id: "run")

    retry =
      PersistenceRetry.new(run, :resource_outcomes, %{asset_step_id: "step"}, nil)
      |> PersistenceRetry.rejected(first)

    retry = %{retry | started_ms: System.monotonic_time(:millisecond) - 30_001}

    again =
      PersistenceRetry.rejected(retry, Error.new(:unavailable, "reply lost", retryable?: true))

    assert again.started_ms == retry.started_ms
    assert again.original_error == first
    assert again.attempts == 2
    assert again.ambiguous?
    assert PersistenceRetry.exhausted?(again)
    error = PersistenceRetry.exhaustion(again)
    assert error.details.operation == :resource_outcomes
    assert error.details.asset_step_id == "step"
    assert error.details.original_error == first
    assert error.details.reason_code == "persistence_retry_exhausted"
  end

  test "cancellation retains completed bookkeeping at both retry and renewal gates" do
    retry = PersistenceRetry.new(struct(RunState), :resource_outcomes, %{}, nil)

    for pending <- [
          %{execution_persist_pending: %{retry: retry}},
          %{storage_renewal_pending: %{purpose: {:resume, retry}}}
        ] do
      message = {:favn_run_cancel_requested, :operator}
      assert {:noreply, retained} = RunServer.handle_info(message, pending)
      assert Map.drop(retained, [:deferred_execution_events]) == pending
      assert retained.deferred_execution_events == [message]
    end
  end
end
