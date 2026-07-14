defmodule FavnOrchestrator.TargetStatusTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.TargetStatus

  @valid_attrs %{
    manifest_version_id: "mv_1",
    target_kind: :asset,
    target_id: "asset:Orders:orders",
    target_ref_text: "Orders:orders",
    status: :healthy,
    updated_at: ~U[2026-07-14 12:00:00Z]
  }

  test "normalizes allowlisted persisted statuses" do
    assert {:ok, status} =
             TargetStatus.new(
               Map.merge(@valid_attrs, %{
                 latest_run_status: "skipped_fresh",
                 freshness_status: "ok"
               })
             )

    assert status.latest_run_status == :skipped_fresh
    assert status.freshness_status == :ok
  end

  test "rejects unknown persisted statuses" do
    assert {:error, {:invalid_target_status_field, :latest_run_status, "admin"}} =
             TargetStatus.new(Map.put(@valid_attrs, :latest_run_status, "admin"))

    assert {:error, {:invalid_target_status_field, :freshness_status, :unknown}} =
             TargetStatus.new(Map.put(@valid_attrs, :freshness_status, :unknown))
  end

  test "rejects malformed optional ids and durations" do
    assert {:error, {:invalid_target_status_field, :latest_run_id, ""}} =
             TargetStatus.new(Map.put(@valid_attrs, :latest_run_id, ""))

    assert {:error, {:invalid_target_status_field, :latest_run_duration_ms, -1}} =
             TargetStatus.new(Map.put(@valid_attrs, :latest_run_duration_ms, -1))
  end
end
