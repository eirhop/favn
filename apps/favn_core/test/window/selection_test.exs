defmodule Favn.Window.SelectionTest do
  use ExUnit.Case, async: true

  alias Favn.Window.{Anchor, Selection}

  test "scheduled selection applies pipeline lookback once" do
    anchor = Anchor.new!(:month, ~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z])

    assert {:ok, selection} = Selection.scheduled(anchor, 1, "Etc/UTC")
    assert selection.requested_anchors == [anchor]
    assert selection.expansion == {:lookback, 1}

    assert Enum.map(selection.effective_anchors, & &1.start_at) == [
             ~U[2026-06-01 00:00:00Z],
             ~U[2026-07-01 00:00:00Z]
           ]
  end

  test "manual and backfill selections remain exact" do
    first = Anchor.new!(:day, ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])
    second = Anchor.new!(:day, ~U[2026-07-02 00:00:00Z], ~U[2026-07-03 00:00:00Z])

    assert {:ok, manual} = Selection.manual(first, "Etc/UTC")
    assert manual.expansion == :none
    assert manual.effective_anchors == [first]

    assert {:ok, backfill} = Selection.backfill([second, first], "Etc/UTC")
    assert backfill.expansion == :none
    assert backfill.effective_anchors == [first, second]
  end

  test "manual lookback and mixed timezones are rejected" do
    anchor = Anchor.new!(:day, ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])

    assert {:error, {:invalid_selection_expansion, :manual, {:lookback, 1}}} =
             Selection.new(:manual, [anchor], {:lookback, 1}, "Etc/UTC")

    assert {:error, {:anchor_timezone_mismatch, "Etc/UTC"}} =
             Selection.manual(anchor, "Europe/Oslo")
  end
end
