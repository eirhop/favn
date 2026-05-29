defmodule FavnOrchestrator.Audit.RedactorTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Audit.Redactor

  test "redacts nested sensitive operator payload keys" do
    payload = %{
      refresh_mode: :force_all,
      metadata: %{
        keep: "visible",
        service_token: "raw-token",
        connection: %{url: "postgres://user:pass@example/db"}
      }
    }

    redacted = Redactor.redact_payload(payload)

    assert redacted["refresh_mode"] == "force_all"
    assert redacted["metadata"]["keep"] == "visible"
    assert redacted["metadata"]["service_token"] == "[REDACTED]"
    assert redacted["metadata"]["connection"] == "[REDACTED]"
  end

  test "bounds oversized payloads" do
    metadata = Map.new(1..50, fn index -> {"safe_#{index}", String.duplicate("x", 1_000)} end)
    redacted = Redactor.redact_payload(%{metadata: metadata})

    assert redacted == %{"payload_truncated" => true}
  end
end
