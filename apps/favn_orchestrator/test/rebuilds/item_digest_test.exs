defmodule FavnOrchestrator.Rebuilds.ItemDigestTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Rebuilds.ItemDigest

  test "is independent of persistence ordering and DateTime precision metadata" do
    {:ok, coarse_start, 0} = DateTime.from_iso8601("2026-07-24T00:00:00Z")
    {:ok, precise_start, 0} = DateTime.from_iso8601("2026-07-24T00:00:00.000000Z")
    {:ok, coarse_end, 0} = DateTime.from_iso8601("2026-07-25T00:00:00Z")
    {:ok, precise_end, 0} = DateTime.from_iso8601("2026-07-25T00:00:00.000000Z")

    first = item("asset:b", "item:b", coarse_start, coarse_end)
    second = item("asset:a", "item:a", precise_start, precise_end)

    assert ItemDigest.hash([first, second]) ==
             ItemDigest.hash([
               %{second | window_start: coarse_start, window_end: coarse_end},
               %{first | window_start: precise_start, window_end: precise_end}
             ])
  end

  defp item(target_id, item_id, window_start, window_end) do
    %{
      target_id: target_id,
      item_id: item_id,
      ordinal: 0,
      work_kind: :window,
      window_key: "window",
      window_start: window_start,
      window_end: window_end,
      runtime_input_expectation: nil,
      candidate_generation_id: nil
    }
  end
end
