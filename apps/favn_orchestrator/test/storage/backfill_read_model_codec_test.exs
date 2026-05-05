defmodule FavnOrchestrator.Storage.BackfillReadModelCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Storage.Backfill.AssetWindowStateCodec
  alias FavnOrchestrator.Storage.Backfill.BackfillWindowCodec
  alias FavnOrchestrator.Storage.Backfill.CoverageBaselineCodec

  @now ~U[2026-04-01 12:00:00Z]
  @start_at ~U[2026-04-01 00:00:00Z]
  @end_at ~U[2026-04-02 00:00:00Z]

  test "coverage baseline codec stores full JSON-safe DTO records" do
    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: "baseline_codec",
        pipeline_module: __MODULE__.Pipeline,
        source_key: "orders",
        segment_key_hash: "sha256:abc",
        segment_key_redacted: "abc***",
        window_kind: :day,
        timezone: "Etc/UTC",
        coverage_until: @end_at,
        created_by_run_id: "run_baseline",
        manifest_version_id: "mv_1",
        status: :ok,
        errors: [%{reason: {:failed, :because}, password: "secret-password"}],
        metadata: %{row_count: 10, password: "secret-password", nested: %{kind: :atom_value}},
        created_at: @now,
        updated_at: @now
      })

    assert {:ok, payload} = CoverageBaselineCodec.encode(baseline)
    dto = Jason.decode!(payload)

    assert dto["format"] == "favn.backfill.coverage_baseline.storage.v1"
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ "secret-password"

    assert {:ok, restored} = CoverageBaselineCodec.decode(payload)
    assert restored.baseline_id == baseline.baseline_id
    assert restored.pipeline_module == baseline.pipeline_module
    assert restored.window_kind == :day
    assert restored.status == :ok
    assert restored.metadata["row_count"] == 10
    assert restored.metadata["nested"] == %{"kind" => "atom_value"}
    assert [%{"kind" => "error", "redacted" => true}] = restored.errors
  end

  test "backfill window codec normalizes error fields without BEAM reconstruction" do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: "backfill_codec",
        child_run_id: "child_codec",
        pipeline_module: __MODULE__.Pipeline,
        manifest_version_id: "mv_1",
        window_kind: :day,
        window_start_at: @start_at,
        window_end_at: @end_at,
        timezone: "Etc/UTC",
        window_key: "day:2026-04-01",
        status: :error,
        attempt_count: 2,
        latest_attempt_run_id: "child_codec",
        last_error: %RuntimeError{message: "boom password=secret"},
        errors: [{:error, :failed}],
        metadata: %{source: :backfill, password: "secret"},
        updated_at: @now
      })

    assert {:ok, payload} = BackfillWindowCodec.encode(window)
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ "password=secret"

    assert {:ok, restored} = BackfillWindowCodec.decode(payload)
    assert restored.pipeline_module == window.pipeline_module
    assert restored.last_error["kind"] == "error"
    assert restored.last_error["redacted"] == true
    assert [%{"kind" => "error"}] = restored.errors
    assert restored.metadata["source"] == "backfill"
  end

  test "asset window state codec preserves identity fields and JSON-shaped payloads" do
    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: __MODULE__.Asset,
        asset_ref_name: :orders,
        pipeline_module: __MODULE__.Pipeline,
        manifest_version_id: "mv_1",
        window_kind: :day,
        window_start_at: @start_at,
        window_end_at: @end_at,
        timezone: "Etc/UTC",
        window_key: "day:2026-04-01",
        status: :error,
        latest_run_id: "run_asset_codec",
        latest_error: {:error, :failed},
        errors: [%{reason: :failed}],
        rows_written: 42,
        metadata: %{partition: :daily, credentials: "secret"},
        updated_at: @now
      })

    assert {:ok, payload} = AssetWindowStateCodec.encode(state)
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ "secret"

    assert {:ok, restored} = AssetWindowStateCodec.decode(payload)
    assert restored.asset_ref_module == state.asset_ref_module
    assert restored.asset_ref_name == :orders
    assert restored.latest_error["type"] == "tuple"
    assert restored.metadata == %{"credentials" => "[REDACTED]", "partition" => "daily"}
  end
end
