defmodule FavnOrchestrator.OperatorCommands.RequestTest do
  use ExUnit.Case, async: true

  alias Favn.Backfill.RangeRequest
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest

  test "asset run requests normalize browser-shaped operator intent" do
    assert {:ok, request} =
             AssetRunRequest.from_input(%{
               "dependencies" => "none",
               "refresh" => "force_selected_upstream",
               "selection" => %{
                 "source" => "refresh_timeline",
                 "kind" => "day",
                 "value" => "2026-05-10",
                 "timezone" => "Etc/UTC"
               }
             })

    assert request.dependency_mode == :none
    assert request.refresh_mode == :force_selected_upstream
    assert request.selection.source == :refresh_timeline
    assert request.selection.id == "refresh:day:2026-05-10"
  end

  test "asset run requests reject unknown dependency refresh and selection values" do
    assert {:error, {:invalid_operator_dependency_mode, "some"}} =
             AssetRunRequest.from_input(%{"dependency_mode" => "some"})

    assert {:error, {:invalid_operator_refresh_mode, "sometimes"}} =
             AssetRunRequest.from_input(%{"refresh_mode" => "sometimes"})

    assert {:error, {:invalid_operator_selection_source, "audit_log"}} =
             AssetRunRequest.from_input(%{
               selection: %{source: "audit_log", id: "window:day:2026-05-10"}
             })
  end

  test "asset backfill requests normalize ranges and selected refresh modes" do
    assert {:ok, request} =
             AssetBackfillRequest.from_input(%{
               dependency_mode: :all,
               refresh_mode: :force_selected,
               range: %{kind: "day", from: "2026-05-01", to: "2026-05-03", timezone: "Etc/UTC"}
             })

    assert request.dependency_mode == :all
    assert request.refresh_mode == :force_selected
    assert %RangeRequest{kind: :day, from: "2026-05-01", to: "2026-05-03"} = request.range
  end

  test "pipeline requests reject selected-asset refresh modes" do
    assert {:ok, request} = PipelineRunRequest.from_input(%{refresh_mode: :force})
    assert request.refresh_mode == :force_all

    assert {:error, {:invalid_operator_refresh_mode, "force_selected"}} =
             PipelineRunRequest.from_input(%{refresh_mode: "force_selected"})

    assert {:error, {:invalid_operator_refresh_mode, "force_selected_upstream"}} =
             PipelineBackfillRequest.from_input(%{
               refresh_mode: "force_selected_upstream",
               range: %{kind: "day", from: "2026-05-01", to: "2026-05-03"}
             })
  end

  test "prebuilt request structs are validated like map input" do
    assert {:error, {:invalid_operator_refresh_mode, :bogus}} =
             AssetRunRequest.from_input(%AssetRunRequest{refresh_mode: :bogus})

    assert {:error, {:invalid_operator_refresh_mode, :bogus}} =
             AssetBackfillRequest.from_input(%AssetBackfillRequest{refresh_mode: :bogus})

    assert {:error, {:invalid_operator_refresh_mode, :force_selected}} =
             PipelineRunRequest.from_input(%PipelineRunRequest{refresh_mode: :force_selected})

    assert {:error, {:invalid_operator_refresh_mode, :force_selected}} =
             PipelineBackfillRequest.from_input(%PipelineBackfillRequest{
               refresh_mode: :force_selected
             })
  end

  test "pipeline window maps normalize lower-level request errors" do
    assert {:error, {:invalid_operator_window, %{mode: "bad"}}} =
             PipelineRunRequest.from_input(%{window: %{mode: "bad"}})
  end

  test "pipeline backfill requests return stable range errors" do
    assert {:error, {:invalid_operator_range, %{kind: "day"}}} =
             PipelineBackfillRequest.from_input(%{range: %{kind: "day"}})
  end

  test "pipeline backfill requests validate optional DTO fields" do
    base = %{range: %{kind: "day", from: "2026-05-01", to: "2026-05-03"}}

    assert {:ok, request} =
             PipelineBackfillRequest.from_input(
               Map.merge(base, %{
                 max_attempts: 2,
                 retry_backoff_ms: 0,
                 timeout_ms: 1_000,
                 coverage_baseline_id: "baseline_1"
               })
             )

    assert request.max_attempts == 2
    assert request.retry_backoff_ms == 0
    assert request.timeout_ms == 1_000
    assert request.coverage_baseline_id == "baseline_1"

    assert {:error, {:invalid_operator_max_attempts, 0}} =
             PipelineBackfillRequest.from_input(Map.put(base, :max_attempts, 0))

    assert {:error, {:invalid_operator_retry_backoff_ms, -1}} =
             PipelineBackfillRequest.from_input(Map.put(base, :retry_backoff_ms, -1))

    assert {:error, {:invalid_operator_timeout_ms, 0}} =
             PipelineBackfillRequest.from_input(Map.put(base, :timeout_ms, 0))

    assert {:error, {:invalid_operator_coverage_baseline_id, ""}} =
             PipelineBackfillRequest.from_input(Map.put(base, :coverage_baseline_id, ""))
  end
end
