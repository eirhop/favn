defmodule FavnOrchestrator.RedactionTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Redaction

  defmodule HTTPError do
    defstruct [:message, :status, :url, :token]
  end

  test "redact_untrusted redacts exception structs without requiring Enumerable" do
    assert "[REDACTED]" = Redaction.redact_untrusted(%RuntimeError{message: "password=secret"})
  end

  test "redact redacts exception structs without requiring Enumerable" do
    assert %{message: "[REDACTED]", type: RuntimeError} =
             Redaction.redact(%RuntimeError{message: "password=secret"})
  end

  test "redact_operational preserves sanitized exception messages in untrusted fields" do
    assert %{reason: %{type: RuntimeError, message: "password=[REDACTED]"}} =
             Redaction.redact_operational(%{
               reason: %RuntimeError{message: "password=secret"}
             })
  end

  test "redact_operational preserves useful error text while masking obvious secrets" do
    assert %{
             message:
               "request failed with token=[REDACTED] Authorization=[REDACTED] dsn=[REDACTED]"
           } =
             Redaction.redact_operational(%{
               message:
                 "request failed with token=abc123 Authorization=Bearer topsecret dsn=postgres://user:pass@example/db"
             })
  end

  test "redact_operational preserves untrusted maps but redacts sensitive keys" do
    assert %{reason: %{status: 401, body: "unauthorized", token: "[REDACTED]"}} =
             Redaction.redact_operational(%{
               reason: %{status: 401, body: "unauthorized", token: "abc123"}
             })
  end

  test "redact_operational preserves sanitized non-exception structs" do
    assert %{
             reason: %{
               type: HTTPError,
               message: "request failed token=[REDACTED]",
               status: 401,
               url: "[REDACTED]",
               token: "[REDACTED]"
             }
           } =
             Redaction.redact_operational(%{
               reason: %HTTPError{
                 message: "request failed token=abc123",
                 status: 401,
                 url: "https://user:pass@example.test/path",
                 token: "abc123"
               }
             })
  end
end
