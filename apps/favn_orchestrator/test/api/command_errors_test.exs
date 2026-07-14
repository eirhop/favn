defmodule FavnOrchestrator.API.CommandErrorsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias FavnOrchestrator.API.CommandErrors

  test "redacts untrusted validation values in diagnostics" do
    secret = "command-error-secret"

    log =
      capture_log(fn ->
        assert {:error, 422, "validation_failed", "Invalid backfill range request", _details} =
                 CommandErrors.backfill(
                   {:invalid_backfill_range_request,
                    %{"password" => secret, "detail" => "password=#{secret}"}}
                 )
      end)

    refute log =~ secret
    assert log =~ "[REDACTED]"
  end
end
