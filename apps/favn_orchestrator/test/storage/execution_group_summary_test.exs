defmodule FavnOrchestrator.Storage.ExecutionGroupSummaryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.ExecutionGroupSummary

  @now ~U[2026-07-13 12:00:00Z]

  describe "storage codec" do
    test "round-trips summaries through versioned JSON" do
      summary = summary_fixture()

      assert {:ok, payload} = ExecutionGroupSummary.encode(summary)
      assert {:ok, decoded} = Jason.decode(payload)
      assert decoded["format"] == "json-v1"
      assert {:ok, ^summary} = ExecutionGroupSummary.decode(payload)
    end

    test "rejects Erlang term payloads" do
      payload = :erlang.term_to_binary(summary_fixture())

      assert {:error, :invalid_execution_group_summary} =
               ExecutionGroupSummary.decode(payload)
    end

    test "rejects JSON values that are not execution-group summaries" do
      assert {:ok, payload} = FavnOrchestrator.Storage.PayloadCodec.encode(%{id: "run-1"})

      assert {:error, :invalid_execution_group_summary} =
               ExecutionGroupSummary.decode(payload)
    end

    test "rejects summaries with empty persisted identities" do
      assert {:ok, payload} =
               summary_fixture()
               |> Map.put(:root_execution_group_id, "")
               |> ExecutionGroupSummary.encode()

      assert {:error, :invalid_execution_group_summary} =
               ExecutionGroupSummary.decode(payload)
    end
  end

  defp summary_fixture do
    %{
      id: "run-1",
      root_execution_group_id: "run-1",
      status: :running,
      health: :active,
      active?: true,
      trigger_type: :manual,
      target_assets: ["Example.users"],
      root_status: :running,
      started_at: @now,
      finished_at: nil,
      duration_ms: nil,
      total_windows: 0,
      completed_windows: 0,
      failed_windows: 0,
      total_asset_attempts: 1,
      completed_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 1,
      queued_asset_attempts: 0,
      failure_count: 0,
      progress: %{
        unit: :assets,
        label: "0 / 1 asset attempts",
        counts: %{total: 1, completed: 0, failed: 0, running: 1, queued: 0}
      },
      summary_totals: %{
        windows: %{total: 0, completed: 0, failed: 0},
        asset_attempts: %{total: 1, completed: 0, failed: 0, running: 1, queued: 0}
      },
      last_activity_at: @now,
      currently_running_asset_attempts: [],
      child_run_ids: []
    }
  end
end
