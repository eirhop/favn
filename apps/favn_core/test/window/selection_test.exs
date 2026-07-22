defmodule Favn.Window.SelectionTest do
  use ExUnit.Case, async: true

  alias Favn.Window.{Anchor, Policy, Request, Selection}

  test "pipeline policy expands scheduled July but keeps manual July exact" do
    policy =
      Policy.new!(:monthly,
        anchor: :current_period,
        lookback: 1,
        timezone: "Etc/UTC"
      )

    assert {:ok, scheduled} =
             Policy.select_scheduled(policy, ~U[2026-07-17 02:00:00Z], "Europe/Oslo")

    assert Enum.map(scheduled.effective_anchors, & &1.start_at) == [
             ~U[2026-06-01 00:00:00Z],
             ~U[2026-07-01 00:00:00Z]
           ]

    assert {:ok, request} = Request.parse("month:2026-07")
    assert {:ok, manual} = Policy.select_manual(policy, request)
    assert Enum.map(manual.effective_anchors, & &1.start_at) == [~U[2026-07-01 00:00:00Z]]
    assert manual.expansion == :none
  end

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

  test "backfill selections reject mixed anchor kinds" do
    day = Anchor.new!(:day, ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])
    month = Anchor.new!(:month, ~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z])

    assert {:error, :mixed_selection_anchor_kinds} =
             Selection.backfill([day, month], "Etc/UTC")
  end
end
