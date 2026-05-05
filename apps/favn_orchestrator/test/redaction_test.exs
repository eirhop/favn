defmodule FavnOrchestrator.RedactionTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Redaction

  test "redact_untrusted redacts exception structs without requiring Enumerable" do
    assert "[REDACTED]" = Redaction.redact_untrusted(%RuntimeError{message: "password=secret"})
  end

  test "redact handles structs without requiring Enumerable" do
    assert %{message: "password=secret", type: RuntimeError} =
             Redaction.redact(%RuntimeError{message: "password=secret"})
  end

  test "redact_operational redacts exception structs in untrusted fields" do
    assert %{reason: "[REDACTED]"} =
             Redaction.redact_operational(%{
               reason: %RuntimeError{message: "password=secret"}
             })
  end
end
