defmodule FavnOrchestrator.Backfill.ReadModelContractsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline

  @start_at ~U[2026-07-14 00:00:00Z]
  @end_at ~U[2026-07-15 00:00:00Z]
  @now ~U[2026-07-15 01:00:00Z]

  test "shared persisted vocabulary normalizes consistently" do
    assert {:ok, window} = BackfillWindow.new(backfill_window_attrs("daily", "running"))
    assert window.window_kind == :day
    assert window.status == :running

    assert {:ok, state} = AssetWindowState.new(asset_window_attrs("daily", "running"))
    assert state.window_kind == :day
    assert state.status == :running

    assert {:ok, baseline} = CoverageBaseline.new(coverage_attrs("daily", "running"))
    assert baseline.window_kind == :day
    assert baseline.status == :running
  end

  test "window records reject malformed collections and counts" do
    assert {:error, {:invalid_attempt_count, -1}} =
             BackfillWindow.new(Map.put(backfill_window_attrs(), :attempt_count, -1))

    assert {:error, {:invalid_errors, %{}}} =
             AssetWindowState.new(Map.put(asset_window_attrs(), :errors, %{}))

    assert {:error, {:invalid_metadata, []}} =
             CoverageBaseline.new(Map.put(coverage_attrs(), :metadata, []))
  end

  test "window records reject reversed time ranges" do
    assert {:error, {:invalid_window_range, @end_at, @start_at}} =
             BackfillWindow.new(
               backfill_window_attrs()
               |> Map.put(:window_start_at, @end_at)
               |> Map.put(:window_end_at, @start_at)
             )

    assert {:error, {:invalid_coverage_range, @end_at, @start_at}} =
             CoverageBaseline.new(
               coverage_attrs()
               |> Map.put(:coverage_start_at, @end_at)
               |> Map.put(:coverage_until, @start_at)
             )
  end

  defp backfill_window_attrs(window_kind \\ :day, status \\ :pending) do
    %{
      backfill_run_id: "backfill_1",
      pipeline_module: __MODULE__.Pipeline,
      manifest_version_id: "mv_1",
      window_kind: window_kind,
      window_start_at: @start_at,
      window_end_at: @end_at,
      timezone: "Etc/UTC",
      window_key: "day:2026-07-14",
      status: status,
      updated_at: @now
    }
  end

  defp asset_window_attrs(window_kind \\ :day, status \\ :pending) do
    %{
      asset_ref_module: __MODULE__.Asset,
      asset_ref_name: :orders,
      pipeline_module: __MODULE__.Pipeline,
      manifest_version_id: "mv_1",
      window_kind: window_kind,
      window_start_at: @start_at,
      window_end_at: @end_at,
      timezone: "Etc/UTC",
      window_key: "day:2026-07-14",
      status: status,
      latest_run_id: "run_1",
      updated_at: @now
    }
  end

  defp coverage_attrs(window_kind \\ :day, status \\ :pending) do
    %{
      baseline_id: "baseline_1",
      pipeline_module: __MODULE__.Pipeline,
      source_key: "orders",
      segment_key_hash: "sha256:abc",
      window_kind: window_kind,
      timezone: "Etc/UTC",
      coverage_start_at: @start_at,
      coverage_until: @end_at,
      created_by_run_id: "run_1",
      manifest_version_id: "mv_1",
      status: status,
      created_at: @now,
      updated_at: @now
    }
  end
end
