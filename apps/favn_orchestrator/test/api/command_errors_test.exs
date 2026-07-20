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

  test "maps asset run context validation to stable fields" do
    assert {:error, 422, "validation_failed", "Invalid run_context_id",
            %{field: "run_context_id"}} =
             CommandErrors.operator({:invalid_operator_run_context_id, ""})

    assert {:error, 422, "validation_failed", "Run context timezone does not match the selection",
            %{
              field: "run_context_id",
              expected_timezone: "Europe/Oslo",
              actual_timezone: "Etc/UTC"
            }} =
             CommandErrors.operator(
               {:asset_run_context_timezone_mismatch, "Europe/Oslo", "Etc/UTC"}
             )
  end
end
