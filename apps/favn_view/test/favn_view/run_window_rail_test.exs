defmodule FavnView.RunWindowRailTest do
  use ExUnit.Case, async: true

  alias FavnView.RunWindowRail

  @zone "Etc/UTC"
  @from ~U[2026-08-01 00:00:00Z]

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

      # One cell is one place to navigate to, and it is the page the operator is
      # already on, so there is no calendar to move through — only a span to
      # state. The rail reports it and stands down.
      # It carries the unit those windows were planned in and the zone they were
      # keyed in, because "four" and a pair of instants cannot be turned into
      # "Jan 2026 – Apr 2026 · 4 months" without both.
      assert rail.combined == %{
               start_at: cell.start_at,
               end_at: cell.end_at,
               window_count: 4,
               kind: :month,
               timezone: "Europe/Oslo"
             }
    end

    test "case 3: a combined run still growing keeps its rail" do
      combined =
        Enum.map(0..1, fn index ->
          %{choice("run-combined", :month, oslo_month(index)) | timezone: "Europe/Oslo"}
        end)

      # A running backfill may yet create another run, and the rail is the only
      # thing that says the set is still growing.
      running = RunWindowRail.build(combined, "run-combined", @zone, backfill_status: :running)
      assert running.combined == nil
      assert running.in_progress?

      done = RunWindowRail.build(combined, "run-combined", @zone, backfill_status: :completed)
      assert done.combined
    end

    test "separate runs are a calendar, so the rail never stands down" do
      choices = [
        choice("run-a", :month, ~U[2026-01-01 00:00:00Z]),
        choice("run-b", :month, ~U[2026-02-01 00:00:00Z])
      ]

      assert RunWindowRail.build(choices, "run-a", @zone).combined == nil
    end

    test "one plain window is one window, not a combined span" do
      rail =
        RunWindowRail.build([choice("run-a", :day, ~U[2026-08-01 00:00:00Z])], "run-a", @zone)

      assert rail.combined == nil
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

    test "counts every window the read returned, not the band on screen" do
      rail =
        RunWindowRail.build(hour_choices(200, ~U[2026-08-01 00:00:00Z]), "hour-0", @zone,
          truncated?: true
        )

      # Banding narrows the cells to one day. The notice speaks for the read, so
      # a capped backfill cannot claim it returned a day's worth of windows.
      assert rail.layout == :banded
      assert length(rail.cells) < 200
      assert rail.loaded_count == 200
    end
  end

  describe "daylight saving" do
    test "buckets a 25-hour day as one day in the window's own zone" do
      # Oslo returns to CET at 03:00 on 2026-10-25, so that local day is 25
      # hours long and spans two UTC dates. The first window starts at local
      # midnight; 125 of them carry the rail past its flat threshold into bands.
      choices =
        Enum.map(0..124, fn index ->
          start_at = DateTime.add(~U[2026-10-24 22:00:00Z], index * 3_600, :second)

          %{
            choice("dst-#{index}", :hour, start_at)
            | timezone: "Europe/Oslo"
          }
        end)

      rail = RunWindowRail.build(choices, "dst-0", @zone)

      assert rail.layout == :banded
      assert %{count: 25} = Enum.find(rail.buckets, &(&1.id == "2026-10-25"))
    end
  end

  describe "comparison" do
    test "marks compared cells with the track their bars occupy" do
      rail =
        RunWindowRail.build(day_choices(4, @from), "day-0", @zone, compare_run_ids: compared())

      assert Enum.map(rail.cells, &{&1.run_id, &1.compared?, &1.track}) == [
               {"day-0", true, 1},
               {"day-1", false, nil},
               {"day-2", true, 2},
               {"day-3", false, nil}
             ]
    end

    test "takes track order from the caller, not from the rail's own order" do
      # The page orders the selection; the rail must not renumber it, or opening
      # a different band would move a window's bars to another track.
      rail =
        RunWindowRail.build(day_choices(3, @from), "day-0", @zone,
          compare_run_ids: ["day-2", "day-0"]
        )

      assert %{track: 2} = Enum.find(rail.cells, &(&1.run_id == "day-0"))
      assert %{track: 1} = Enum.find(rail.cells, &(&1.run_id == "day-2"))
    end

    test "reports a full comparison at the stated limit" do
      choices = day_choices(RunWindowRail.compare_limit() + 1, @from)
      ids = choices |> Enum.map(& &1.run_id) |> Enum.take(RunWindowRail.compare_limit())

      assert RunWindowRail.build(choices, "day-0", @zone, compare_run_ids: ids).compare_full?

      refute RunWindowRail.build(choices, "day-0", @zone, compare_run_ids: tl(ids)).compare_full?
    end

    test "compares nothing by default" do
      rail = RunWindowRail.build(day_choices(2, @from), "day-0", @zone)

      assert rail.compare_run_ids == []
      refute rail.compare_full?
      assert Enum.all?(rail.cells, &(&1.compared? == false and &1.track == nil))
    end
  end

  defp compared, do: ["day-0", "day-2"]

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
