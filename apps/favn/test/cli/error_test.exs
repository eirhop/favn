defmodule Favn.CLI.ErrorTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.Error

  test "formats bounded HTTP errors with allowlisted details and next action" do
    reason = %{
      operation: :submit_run,
      reason:
        {:http_error, 422,
         %{
           error_code: "window_kind_mismatch",
           message: "expected a monthly window",
           details: %{
             expected: "month",
             actual: "day",
             sql: "select secret from private",
             token: "hidden"
           }
         }}
    }

    message = Error.format(reason, context: "run", next: "use --window month:YYYY-MM")

    assert message =~ "run submission failed: HTTP 422 [window_kind_mismatch]"
    assert message =~ "expected=month"
    assert message =~ "actual=day"
    assert message =~ "Next: use --window month:YYYY-MM"
    refute message =~ "select secret"
    refute message =~ "hidden"
  end

  test "does not inspect arbitrary failure terms or secret assignments" do
    secret = "password=super-secret"

    refute Error.format({:unexpected, %{payload: secret}}, context: "run") =~ secret

    message =
      Error.format(
        {:http_error, 500, %{message: "failure token=abc123"}},
        context: "run"
      )

    assert message =~ "token=[REDACTED]"
    refute message =~ "abc123"
  end

  test "redacts SQL statements from server and terminal messages" do
    message = Error.safe_message("adapter failed: SELECT * FROM customers WHERE token = 'secret'")

    assert message == "adapter failed: [REDACTED SQL]"
    refute message =~ "customers"
    refute message =~ "secret"
  end

  test "redacts bearer tokens and URL userinfo completely" do
    message =
      Error.safe_message("authorization: Bearer abc123 database=postgres://alice:hunter2@host/db")

    assert message =~ "authorization=[REDACTED]"
    assert message =~ "database=[REDACTED]"
    refute message =~ "abc123"
    refute message =~ "alice"
    refute message =~ "hunter2"
  end

  test "bounds multibyte messages by bytes without producing invalid UTF-8" do
    message = Error.safe_message(String.duplicate("🦆", 240))

    assert byte_size(message) <= 240
    assert String.valid?(message)
    assert String.ends_with?(message, "...")
  end
end
