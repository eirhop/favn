defmodule FavnView.RunTimelineTest do
  use ExUnit.Case, async: true

  alias FavnView.RunTimeline

  doctest RunTimeline

  @start ~U[2026-08-25 10:00:00Z]

  describe "the axis" do
    test "fits the run rather than the clock" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 30),
            row("b", started: 10, finished: 60)
          ],
          now: at(9_999)
        )

      assert chart.axis.start_at == @start
      assert chart.axis.end_at == at(60)
      assert chart.axis.span_ms == 60_000
      assert chart.axis.now_offset == nil
      assert chart.axis.advance_ms == nil
    end

    test "reaches past now while anything still runs, so the line has room to advance" do
      chart = RunTimeline.build([row("a", started: 0)], now: at(100))

      # 100s elapsed plus 8% headroom: the now line sits inside the axis rather
      # than on its right edge, and CSS animates into what is left.
      assert chart.axis.span_ms == 108_000
      assert chart.axis.now_offset == 92.593
      assert chart.axis.now_offset < 100.0

      # The headroom lasts eight seconds of real time, which is how long the
      # animation has to cross it before the next read redraws the axis.
      assert chart.axis.advance_ms == 8_000
    end

    test "is absent when no attempt has started" do
      chart = RunTimeline.build([row("a", []), row("b", [])], now: at(30))

      assert chart.axis == nil
      assert chart.ghost_count == 2
      assert Enum.all?(lanes(chart), &(&1.bar == nil))
    end

    test "survives a run whose whole span is one instant" do
      chart = RunTimeline.build([row("a", started: 0, finished: 0)], now: at(5))

      assert chart.axis.span_ms == 1
      assert [%{bar: %{offset: +0.0, width: 1.0}}] = lanes(chart)
    end
  end

  describe "ticks" do
    test "land on round intervals and start at the origin" do
      chart = RunTimeline.build([row("a", started: 0, finished: 120)], now: at(120))

      assert Enum.map(chart.axis.ticks, & &1.label) == ["0s", "30s", "1m", "1m 30s", "2m"]
      assert Enum.map(chart.axis.ticks, & &1.offset) == [0.0, 25.0, 50.0, 75.0, 100.0]
    end

    test "stay under seven gridlines at every span" do
      for seconds <- [1, 7, 45, 300, 3_600, 86_400, 864_000] do
        chart = RunTimeline.build([row("a", started: 0, finished: seconds)], now: at(seconds))

        assert length(chart.axis.ticks) <= 7, "#{seconds}s produced too many ticks"
        assert hd(chart.axis.ticks).offset == 0.0
      end
    end
  end

  describe "bars" do
    test "are positioned and sized as a fraction of the axis" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 25),
            row("b", started: 50, finished: 100)
          ],
          now: at(100)
        )

      assert [%{bar: first}, %{bar: second}] = lanes(chart)
      assert first == %{offset: 0.0, width: 25.0, running?: false}
      assert second == %{offset: 50.0, width: 50.0, running?: false}
    end

    test "keep a floor so a short attempt stays visible and clickable" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 600),
            row("b", started_ms: 1, finished_ms: 2)
          ],
          now: at(600)
        )

      assert %{bar: %{width: 1.0}} = lane(chart, "b")
    end

    test "shift back rather than overflow when the floor lands on the right edge" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 600),
            row("b", started: 600, finished: 600)
          ],
          now: at(600)
        )

      assert %{bar: %{offset: 99.0, width: 1.0}} = lane(chart, "b")
    end

    test "extend to now and say so while the attempt is unfinished" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 20),
            row("b", started: 40)
          ],
          now: at(100)
        )

      assert %{bar: %{running?: true} = bar} = lane(chart, "b")
      # Ends at now, which is short of the axis end by the live headroom.
      assert bar.offset + bar.width < 100.0
      assert %{bar: %{running?: false}} = lane(chart, "a")
    end

    test "clamp rather than draw backwards when a finish precedes its own start" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 100),
            row("b", started: 60, finished: 20)
          ],
          now: at(100)
        )

      assert %{bar: %{offset: 60.0, width: 1.0}} = lane(chart, "b")
    end

    test "are absent on a row that has not started, which stays a labelled ghost" do
      chart = RunTimeline.build([row("a", started: 0, finished: 30), row("b", [])], now: at(30))

      assert %{bar: nil, name: "b", state: :pending} = lane(chart, "b")
      assert chart.ghost_count == 1
      assert chart.lane_count == 2
    end
  end

  describe "stage bands" do
    test "run in dependency order with the unstaged band last" do
      chart =
        RunTimeline.build(
          [
            row("c", started: 20, finished: 30, stage: nil),
            row("b", started: 10, finished: 20, stage: 2),
            row("a", started: 0, finished: 10, stage: 1)
          ],
          now: at(30)
        )

      assert Enum.map(chart.bands, & &1.id) == ["stage-1", "stage-2", "unstaged"]
      assert Enum.map(chart.bands, & &1.label) == ["Stage 1", "Stage 2", "Unstaged"]
    end

    test "show every row exactly once, whatever its stage" do
      rows = [
        row("a", started: 0, finished: 10, stage: 1),
        row("b", started: 5, finished: 15, stage: nil),
        row("c", stage: 4)
      ]

      chart = RunTimeline.build(rows, now: at(15))

      assert chart |> lanes() |> Enum.map(& &1.id) |> Enum.sort() == ["a", "b", "c"]
      assert chart.lane_count == 3
    end

    test "order lanes by start, with the not-yet-started last" do
      chart =
        RunTimeline.build(
          [
            row("later", started: 20, finished: 30),
            row("ghost", []),
            row("earlier", started: 5, finished: 10)
          ],
          now: at(30)
        )

      assert Enum.map(lanes(chart), & &1.id) == ["earlier", "later", "ghost"]
    end

    test "order lanes by name on request, which is how a wide stage is scanned" do
      rows = [
        row("later", started: 20, finished: 30),
        row("ghost", []),
        row("earlier", started: 5, finished: 10)
      ]

      chart = RunTimeline.build(rows, now: at(30), sort: :name)

      assert Enum.map(lanes(chart), & &1.id) == ["earlier", "ghost", "later"]
    end
  end

  describe "band summaries" do
    test "count outcomes and span everything the band covers" do
      chart =
        RunTimeline.build(
          [
            row("a", started: 0, finished: 25, state: :ok, stage: 1),
            row("b", started: 50, finished: 75, state: :error, stage: 1),
            row("c", started: 80, state: :running, stage: 1),
            row("d", state: :pending, stage: 1)
          ],
          now: at(100)
        )

      assert [%{summary: summary}] = chart.bands
      assert summary.total == 4
      assert summary.succeeded == 1
      assert summary.failed == 1
      assert summary.running == 1
      assert summary.waiting == 1
      assert summary.bar.offset == 0.0
      assert summary.bar.running? == true
    end

    test "have no bar when nothing in the band has started" do
      chart =
        RunTimeline.build([row("a", started: 0, finished: 10, stage: 1), row("b", stage: 2)],
          now: at(10)
        )

      assert [_first, %{id: "stage-2", summary: %{bar: nil, waiting: 1}}] = chart.bands
    end
  end

  describe "density" do
    test "picks a lane height mode from the lane count" do
      assert RunTimeline.density(0) == :comfortable
      assert RunTimeline.density(60) == :comfortable
      assert RunTimeline.density(61) == :compact
      assert RunTimeline.density(200) == :compact
      assert RunTimeline.density(201) == :dense
    end

    test "is chosen from the rows the chart was built with" do
      assert RunTimeline.build(many(61), now: at(1)).density == :compact
      assert RunTimeline.build(many(201), now: at(1)).density == :dense
    end

    test "collapses every band in dense mode except the ones asked for" do
      rows = Enum.map(many(201), &Map.put(&1, :stage, rem(String.to_integer(&1.id), 2)))
      chart = RunTimeline.build(rows, now: at(1), expanded: ["stage-1"])

      assert Enum.map(chart.bands, &{&1.id, &1.collapsed?}) == [
               {"stage-0", true},
               {"stage-1", false}
             ]
    end

    test "never collapses a band a reader can already see in full" do
      chart = RunTimeline.build(many(60), now: at(1), expanded: [])

      assert Enum.all?(chart.bands, &(&1.collapsed? == false))
    end
  end

  describe "outcome/1" do
    test "reads a state as done, failed, going, or not yet" do
      for state <- [:ok, :succeeded, :skipped_fresh],
          do: assert(RunTimeline.outcome(state) == :succeeded)

      for state <- [:error, :failed, :timed_out, :blocked, :cancelled],
          do: assert(RunTimeline.outcome(state) == :failed)

      for state <- [:running, :retrying], do: assert(RunTimeline.outcome(state) == :running)

      for state <- [:pending, :queued, :planned, nil],
          do: assert(RunTimeline.outcome(state) == :waiting)
    end
  end

  defp lanes(chart), do: Enum.flat_map(chart.bands, & &1.lanes)

  defp lane(chart, id), do: chart |> lanes() |> Enum.find(&(&1.id == id))

  defp many(count) do
    Enum.map(1..count, fn index ->
      row(Integer.to_string(index), started_ms: index, finished_ms: index + 1)
    end)
  end

  defp row(id, opts) do
    %{
      id: id,
      run_id: "run-1",
      name: id,
      asset_ref: "crm.#{id}",
      state: Keyword.get(opts, :state, default_state(opts)),
      stage: Keyword.get(opts, :stage),
      started_at: instant(opts, :started, :started_ms),
      finished_at: instant(opts, :finished, :finished_ms)
    }
  end

  defp default_state(opts) do
    cond do
      Keyword.has_key?(opts, :finished) or Keyword.has_key?(opts, :finished_ms) -> :ok
      Keyword.has_key?(opts, :started) or Keyword.has_key?(opts, :started_ms) -> :running
      true -> :pending
    end
  end

  defp instant(opts, seconds_key, milliseconds_key) do
    cond do
      seconds = Keyword.get(opts, seconds_key) -> at(seconds)
      milliseconds = Keyword.get(opts, milliseconds_key) -> at_ms(milliseconds)
      true -> nil
    end
  end

  defp at(seconds), do: DateTime.add(@start, seconds, :second)
  defp at_ms(milliseconds), do: DateTime.add(@start, milliseconds, :millisecond)
end
