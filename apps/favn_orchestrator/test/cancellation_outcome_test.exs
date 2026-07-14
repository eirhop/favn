defmodule FavnOrchestrator.CancellationOutcomeTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.CancellationOutcome

  test "normalizes and redacts untrusted runner cancellation failures" do
    outcome =
      CancellationOutcome.from_runner_result(
        "exec_safe",
        {:error, %{type: :transport_error, token: "secret", message: "password=hunter2"}}
      )

    assert outcome.status == :best_effort_failed
    assert outcome.reason_class == :transport_error
    assert outcome.error.token == "[REDACTED]"
    assert outcome.error.message == "password=[REDACTED]"
  end

  test "drops malformed optional runner facts" do
    outcome =
      CancellationOutcome.from_runner_result("exec_facts", {
        :ok,
        %{
          status: :acknowledged,
          native_status: %{unexpected: true},
          reason_class: "not-an-atom",
          correlation_id: %{unexpected: true}
        }
      })

    assert outcome.status == :acknowledged
    assert outcome.native_status == nil
    assert outcome.reason_class == nil
    assert outcome.correlation_id == nil
  end
end
