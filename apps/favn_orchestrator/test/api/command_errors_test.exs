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

  test "maps target admission failures to stable bounded conflicts" do
    path = ["asset:upstream", "asset:selected"]

    details = %{
      target_id: "asset:upstream",
      selected_target_id: "asset:selected",
      blocked_path: path,
      blocked_path_target_count: 2,
      blocked_path_truncated: false,
      compatibility_status: :rebuild_required,
      reason_code: "contract_changed",
      compatibility_diff: %{columns: String.duplicate("x", 10_000)}
    }

    expected_details = Map.delete(details, :compatibility_diff)

    assert {:error, 409, "rebuild_required",
            "Target requires a rebuild before this run can start", ^expected_details} =
             CommandErrors.admission({:rebuild_required, details})

    assert {:error, 409, "target_drift",
            "Target physical state differs from its recorded generation", drift_details} =
             CommandErrors.admission({
               :target_drift,
               %{details | compatibility_status: :unexpected_drift}
             })

    assert drift_details.compatibility_status == :unexpected_drift

    assert {:error, 409, "operator_decision_required",
            "Target requires an explicit operator decision before this run can start", _details} =
             CommandErrors.admission({
               :operator_decision_required,
               %{details | compatibility_status: :operator_decision}
             })
  end

  test "does not reinterpret unrelated tuple failures as admission conflicts" do
    assert CommandErrors.admission({:invalid_window_request, %{}}) == nil
    assert CommandErrors.admission(:rebuild_required) == nil
  end
end
