defmodule FavnView.RunWindowRailTest do
  use ExUnit.Case, async: true

  alias FavnView.RunWindowRail

  @zone "Etc/UTC"

  describe "flat layout" do
    test "sorts window runs into calendar order regardless of read order" do
      choices = [
        choice("run-c", :day, ~U[2026-08-03 00:00:00Z]),
        choice("run-a", :day, ~U[2026-08-01 00:00:00Z]),
        choice("run-b", :day, ~U[2026-08-02 00:00:00Z])
      ]

      rail = RunWindowRail.build(choices, "run-b", @zone)

      assert rail.layout == :flat
      assert Enum.map(rail.cells, & &1.run_id) == ["run-a", "run-b", "run-c"]
      assert Enum.map(rail.cells, & &1.label) == ["1", "2", "3"]
      assert Enum.map(rail.cells, & &1.selected?) == [false, true, false]
    end

    test "labels each window kind by its calendar position" do
      for {kind, expected} <- [
            {:hour, "14:00"},
            {:day, "3"},
            {:month, "Aug"},
            {:year, "2026"}
          ] do
        rail = RunWindowRail.build([choice("run", kind, ~U[2026-08-03 14:00:00Z])], "run", @zone)
        assert [%{label: ^expected}] = rail.cells
      end
    end

    test "labels a mixed or undecodable list by start timestamp" do
      mixed = [
        choice("run-a", :day, ~U[2026-08-03 00:00:00Z]),
        choice("run-b", :month, ~U[2026-09-01 00:00:00Z])
      ]

      assert %{cells: [first, second]} = RunWindowRail.build(mixed, "run-a", @zone)
      assert first.label == "Aug 3, 2026 00:00"
      assert second.label == "Sep 1, 2026 00:00"

      undecodable = [%{choice("run-c", :day, ~U[2026-08-03 00:00:00Z]) | kind: nil}]
      assert %{cells: [only]} = RunWindowRail.build(undecodable, "run-c", @zone)
      assert only.label == "Aug 3, 2026 00:00"
    end

    test "buckets and labels in the window's own timezone, not the viewer's" do
      # 22:00 UTC is already the next day in Oslo, which is the day the
      # operator scheduled.
      oslo = %{choice("run", :day, ~U[2026-05-31 22:00:00Z]) | timezone: "Europe/Oslo"}

      assert %{cells: [%{label: "1"}]} = RunWindowRail.build([oslo], "run", @zone)

      assert %{cells: [%{label: "31"}]} =
               RunWindowRail.build([%{oslo | timezone: nil}], "r", @zone)
    end

    test "an empty list produces an empty flat rail" do
      rail = RunWindowRail.build([], nil, @zone)

      assert rail.layout == :flat
      assert rail.cells == []
      assert rail.buckets == []
    end

    test "a selected run absent from the choices leaves every cell unselected" do
      choices = [choice("run-a", :day, ~U[2026-08-01 00:00:00Z])]
      rail = RunWindowRail.build(choices, "run-missing", @zone)

      assert Enum.all?(rail.cells, &(not &1.selected?))
    end
  end

  describe "banded layout" do
    test "groups days by month and shows only the selected bucket's cells" do
      choices = day_choices(200, ~U[2026-01-01 00:00:00Z])
      selected = Enum.at(choices, 100)

      rail = RunWindowRail.build(choices, selected.run_id, @zone)

      assert rail.layout == :banded
      assert length(rail.buckets) == 7
      assert Enum.map(rail.buckets, & &1.label) |> hd() == "Jan 2026"
      assert Enum.count(rail.buckets, & &1.selected?) == 1

      # The fine band holds exactly the selected month, and the coarse band's
      # count for that month agrees with it.
      selected_bucket = Enum.find(rail.buckets, & &1.selected?)
      assert selected_bucket.count == length(rail.cells)
      assert Enum.any?(rail.cells, &(&1.run_id == selected.run_id))
      assert Enum.sum(Enum.map(rail.buckets, & &1.count)) == 200
    end

    test "groups hours by day" do
      choices = hour_choices(200, ~U[2026-08-01 00:00:00Z])
      rail = RunWindowRail.build(choices, "hour-0", @zone)

      assert rail.layout == :banded
      assert Enum.map(rail.buckets, & &1.label) |> hd() == "Aug 1"
      assert length(rail.cells) == 24
    end

    test "years stay flat because no coarser unit exists" do
      choices =
        Enum.map(0..(RunWindowRail.flat_threshold() + 10), fn index ->
          choice(
            "year-#{index}",
            :year,
            DateTime.new!(Date.new!(1900 + index, 1, 1), ~T[00:00:00])
          )
        end)

      rail = RunWindowRail.build(choices, "year-0", @zone)

      assert rail.layout == :flat
      assert rail.buckets == []
      assert length(rail.cells) == length(choices)
    end

    test "a mixed list buckets by the smallest unit that fits the flat threshold" do
      choices =
        Enum.map(0..199, fn index ->
          kind = if rem(index, 2) == 0, do: :day, else: :hour

          %{
            choice("mixed-#{index}", kind, DateTime.add(~U[2026-01-01 00:00:00Z], index, :day))
            | kind: kind
          }
        end)

      rail = RunWindowRail.build(choices, "mixed-0", @zone)

      # 200 distinct days exceeds the threshold, so it falls through to months.
      assert rail.layout == :banded
      assert length(rail.buckets) == 7
    end

    test "with no selected run the rail opens on the newest bucket" do
      choices = day_choices(200, ~U[2026-01-01 00:00:00Z])
      rail = RunWindowRail.build(choices, nil, @zone)

      assert rail.layout == :banded
      assert List.last(rail.buckets).selected?
      refute hd(rail.buckets).selected?
    end
  end

  # Logical windows describe coverage; child runs describe executions. The rail
  # navigates between executions, so these three cases are the whole space.
  describe "coverage windows against executed child runs" do
    test "case 1: a single window is one cell" do
      rail =
        RunWindowRail.build([choice("run-a", :day, ~U[2026-08-01 00:00:00Z])], "run-a", @zone)

      assert [%{run_id: "run-a", window_count: 1}] = rail.cells
    end

    test "case 2: separate windows are one cell per child run" do
      choices = [
        choice("run-a", :month, ~U[2026-01-01 00:00:00Z]),
        choice("run-b", :month, ~U[2026-02-01 00:00:00Z]),
        choice("run-c", :month, ~U[2026-03-01 00:00:00Z])
      ]

      rail = RunWindowRail.build(choices, "run-b", @zone)

      assert Enum.map(rail.cells, & &1.run_id) == ["run-a", "run-b", "run-c"]
      assert Enum.all?(rail.cells, &(&1.window_count == 1))
    end

    test "case 3: combined windows collapse into the one run that executed them" do
      combined =
        Enum.map(0..3, fn index ->
          %{
            choice("run-combined", :month, oslo_month(index))
            | timezone: "Europe/Oslo"
          }
        end)

      rail = RunWindowRail.build(combined, "run-combined", @zone)

      # Four coverage windows, one execution, so one place to navigate to.
      assert [cell] = rail.cells
      assert cell.run_id == "run-combined"
      assert cell.window_count == 4
      assert cell.selected?

      # The cell spans the whole combined coverage rather than only its first
      # window, and is labelled by where that coverage starts.
      assert cell.start_at == oslo_month(0)
      assert DateTime.compare(cell.end_at, oslo_month(3)) == :gt
      assert cell.label == "Jan"
    end

    test "case 3: a combined run mixed with separate runs keeps both readable" do
      choices =
        [
          choice("run-combined", :month, ~U[2026-01-01 00:00:00Z]),
          choice("run-combined", :month, ~U[2026-02-01 00:00:00Z]),
          choice("run-later", :month, ~U[2026-03-01 00:00:00Z])
        ]

      rail = RunWindowRail.build(choices, "run-later", @zone)

      assert Enum.map(rail.cells, & &1.run_id) == ["run-combined", "run-later"]
      assert Enum.map(rail.cells, & &1.window_count) == [2, 1]
      assert [%{selected?: false}, %{selected?: true}] = rail.cells
    end
  end

  describe "in-progress and truncation markers" do
    test "a non-terminal backfill marks the set as still growing" do
      choices = [choice("run-a", :day, ~U[2026-08-01 00:00:00Z])]

      for status <- [:planning, :ready, :running] do
        rail = RunWindowRail.build(choices, "run-a", @zone, backfill_status: status)
        assert rail.in_progress?
      end

      for status <- [:completed, :failed, :cancelled] do
        rail = RunWindowRail.build(choices, "run-a", @zone, backfill_status: status)
        refute rail.in_progress?
      end
    end

    test "an unknown backfill status claims no progress" do
      rail = RunWindowRail.build([], nil, @zone, backfill_status: nil)
      refute rail.in_progress?
    end

    test "truncation is carried through from the read" do
      assert RunWindowRail.build([], nil, @zone, truncated?: true).truncated?
      refute RunWindowRail.build([], nil, @zone).truncated?
    end
  end

  defp choice(run_id, kind, start_at) do
    %{
      run_id: run_id,
      window_start_at: start_at,
      window_end_at: DateTime.add(start_at, 3_600, :second),
      status: :succeeded,
      kind: kind,
      timezone: @zone
    }
  end

  # Oslo month boundaries in UTC, as the projection stores them: 23:00 the
  # previous day under CET, 22:00 once CEST begins.
  defp oslo_month(index) do
    Enum.at(
      [
        ~U[2025-12-31 23:00:00Z],
        ~U[2026-01-31 23:00:00Z],
        ~U[2026-02-28 23:00:00Z],
        ~U[2026-03-31 22:00:00Z]
      ],
      index
    )
  end

  defp day_choices(count, from) do
    Enum.map(0..(count - 1), fn index ->
      choice("day-#{index}", :day, DateTime.add(from, index, :day))
    end)
  end

  defp hour_choices(count, from) do
    Enum.map(0..(count - 1), fn index ->
      choice("hour-#{index}", :hour, DateTime.add(from, index * 3_600, :second))
    end)
  end
end
