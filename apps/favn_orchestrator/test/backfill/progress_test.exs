defmodule FavnOrchestrator.Backfill.ProgressTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Backfill.Progress

  @now ~U[2026-07-13 12:00:00Z]

  test "normalizes persisted status names and numeric counts" do
    assert {:ok, progress} = Progress.from_counts("backfill-1", %{"ok" => "2"}, @now)

    assert progress.status == :ok
    assert progress.total_count == 2
    assert progress.ok_count == 2
  end

  test "rejects unknown persisted statuses without creating atoms" do
    status = "unknown_#{System.unique_integer([:positive])}"
    refute_existing_atom(status)

    assert {:error, {:invalid_status, ^status}} =
             Progress.from_counts("backfill-1", %{status => 1}, @now)

    refute_existing_atom(status)
  end

  test "rejects malformed persisted counts instead of raising or coercing them" do
    assert {:error, {:invalid_count, "ok", "not-a-count"}} =
             Progress.from_counts("backfill-1", %{"ok" => "not-a-count"}, @now)

    assert {:error, {:invalid_count, :ok_count, :invalid}} =
             Progress.new(%{
               backfill_run_id: "backfill-1",
               total_count: 1,
               ok_count: :invalid,
               status: :ok,
               updated_at: @now
             })
  end

  test "applies status changes without carrying a synthetic total key" do
    assert {:ok, progress} = Progress.from_counts("backfill-1", %{pending: 1}, @now)

    assert {:ok, updated} =
             Progress.apply_status_change(progress, :pending, :running, @now)

    assert updated.total_count == 1
    assert updated.pending_count == 0
    assert updated.running_count == 1
  end

  defp refute_existing_atom(value) do
    assert_raise ArgumentError, fn -> String.to_existing_atom(value) end
  end
end
