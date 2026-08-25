defmodule FavnView.RunComparisonTest do
  use ExUnit.Case, async: true

  alias FavnView.RunComparison

  doctest RunComparison

  @start ~U[2026-08-25 10:00:00Z]

  describe "lanes and tracks" do
    test "draws one lane per asset and one track per window, in track order" do
      chart =
        RunComparison.build(
          [
            window(2, "run-b", [row("orders", started: 0, finished: 10)]),
            window(1, "run-a", [row("orders", started: 0, finished: 20)])
          ],
          now: at(20)
        )

      assert [%{id: "orders", tracks: tracks}] = lanes(chart)
      assert Enum.map(tracks, & &1.track) == [1, 2]
      assert Enum.map(tracks, & &1.run_id) == ["run-a", "run-b"]
      assert chart.lane_count == 1
      assert chart.track_count == 2
    end

    test "takes the lane set from every window, not from the first one" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [
              row("orders", started: 0, finished: 10),
              row("refunds", started: 0, finished: 10)
            ])
          ],
          now: at(10)
        )

      assert chart |> lanes() |> Enum.map(& &1.id) |> Enum.sort() == ["orders", "refunds"]
    end

    test "an asset a window never planned leaves an empty track in its position" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [
              row("orders", started: 0, finished: 10),
              row("refunds", started: 0, finished: 10)
            ])
          ],
          now: at(10)
        )

      # The plan difference is visible in place rather than hidden by dropping
      # the lane or by silently shifting the remaining tracks left.
      assert [%{presence: :absent, bar: nil, track: 1}, %{presence: :drawn, track: 2}] =
               track_list(chart, "refunds")
    end

    test "a row present but not yet started waits rather than drawing at the origin" do
      chart =
        RunComparison.build(
          [window(1, "run-a", [row("orders", started: 0, finished: 10), row("total", [])])],
          now: at(10)
        )

      assert [%{presence: :waiting, bar: nil, state: :pending}] = track_list(chart, "total")
    end

    test "an unreadable window draws unavailable tracks, never gaps that read as skipped" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            %{
              run_id: "run-b",
              track: 2,
              state: :unavailable,
              label: nil,
              reason: :unavailable,
              assets: [],
              selected?: false
            }
          ],
          now: at(10)
        )

      assert [%{presence: :drawn}, %{presence: :unavailable, bar: nil}] =
               track_list(chart, "orders")

      assert [_first, %{state: :unavailable, reason: :unavailable}] = chart.tracks
    end

    test "a window still loading says so on every lane" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            %{
              run_id: "run-b",
              track: 2,
              state: :loading,
              label: nil,
              reason: nil,
              assets: [],
              selected?: false
            }
          ],
          now: at(10)
        )

      assert [_drawn, %{presence: :loading}] = track_list(chart, "orders")
    end
  end

  describe "alignment" do
    test "compares windows at their own starts by default" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [row("orders", started: 3_600, finished: 3_620)])
          ],
          now: at(3_620)
        )

      assert chart.alignment == :window
      assert chart.axis.span_ms == 20_000

      # An hour apart in wall-clock time, but both start their own window at the
      # axis origin, which is what makes the two runs comparable at all.
      assert [%{bar: first}, %{bar: second}] = track_list(chart, "orders")
      assert first == %{offset: 0.0, width: 50.0, running?: false}
      assert second == %{offset: 0.0, width: 100.0, running?: false}
    end

    test "offers wall clock only when the windows ran closely enough together" do
      near =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [row("orders", started: 5, finished: 25)])
          ],
          now: at(25)
        )

      assert near.wall_clock?
      assert near.span_ratio == 1.3
    end

    test "refuses wall clock across distant windows and says by how much" do
      far =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [row("orders", started: 3_600, finished: 3_620)])
          ],
          now: at(3_620),
          alignment: :wall_clock
        )

      refute far.wall_clock?
      assert far.span_ratio > RunComparison.wall_clock_max_ratio()

      # Asking for an alignment the comparison cannot honour falls back rather
      # than drawing a chart of hairlines.
      assert far.alignment == :window
    end

    test "shares one real timeline when asked and allowed" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 10)]),
            window(2, "run-b", [row("orders", started: 10, finished: 20)])
          ],
          now: at(20),
          alignment: :wall_clock
        )

      assert chart.alignment == :wall_clock
      assert chart.axis.span_ms == 20_000

      assert [%{bar: %{offset: +0.0}}, %{bar: %{offset: 50.0}}] = track_list(chart, "orders")
    end

    test "a single window has nothing to align against" do
      chart = RunComparison.build([window(1, "run-a", [row("orders", started: 0, finished: 10)])])

      refute chart.wall_clock?
      assert chart.span_ratio == nil
    end

    test "measures a running bar against its own window's elapsed time" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [row("orders", started: 0, finished: 60)]),
            window(2, "run-b", [row("orders", started: 3_600)])
          ],
          now: at(3_630)
        )

      assert [%{bar: %{running?: false, width: 100.0}}, %{bar: running}] =
               track_list(chart, "orders")

      # run-b started 30s ago in its own window, not an hour ago on the clock.
      assert running.running? == true
      assert running.width < 60.0
    end
  end

  describe "bands and density" do
    test "groups lanes by stage with the unstaged band last" do
      chart =
        RunComparison.build(
          [
            window(1, "run-a", [
              row("late", started: 10, finished: 20, stage: 2),
              row("loose", started: 0, finished: 5, stage: nil),
              row("early", started: 0, finished: 10, stage: 1)
            ])
          ],
          now: at(20)
        )

      assert Enum.map(chart.bands, & &1.id) == ["stage-1", "stage-2", "unstaged"]
      assert Enum.map(chart.bands, & &1.label) == ["Stage 1", "Stage 2", "Unstaged"]
    end

    test "picks a lane height from the tracks it draws, not from the lanes" do
      # 40 assets is comfortable on its own and compact across three windows.
      rows = Enum.map(1..40, &row("asset-#{&1}", started: 0, finished: 1))

      assert RunComparison.build([window(1, "run-a", rows)], now: at(1)).density == :comfortable

      wide =
        RunComparison.build(
          Enum.map(1..3, &window(&1, "run-#{&1}", rows)),
          now: at(1)
        )

      assert wide.track_count == 120
      assert wide.density == :compact
    end

    test "has no axis when nothing in any window has started" do
      chart = RunComparison.build([window(1, "run-a", [row("orders", [])])], now: at(10))

      assert chart.axis == nil
      assert [%{presence: :waiting, bar: nil}] = track_list(chart, "orders")
    end
  end

  defp lanes(chart), do: Enum.flat_map(chart.bands, & &1.lanes)

  defp track_list(chart, id) do
    chart |> lanes() |> Enum.find(&(&1.id == id)) |> Map.fetch!(:tracks)
  end

  defp window(track, run_id, assets) do
    %{
      run_id: run_id,
      track: track,
      state: :loaded,
      label: "Window #{track}",
      reason: nil,
      selected?: track == 1,
      assets: assets
    }
  end

  defp row(ref, opts) do
    %{
      id: ref,
      name: ref,
      asset_ref: ref,
      state: Keyword.get(opts, :state, default_state(opts)),
      stage: Keyword.get(opts, :stage, 1),
      started_at: opts |> Keyword.get(:started) |> at_or_nil(),
      finished_at: opts |> Keyword.get(:finished) |> at_or_nil()
    }
  end

  defp default_state(opts) do
    cond do
      Keyword.has_key?(opts, :finished) -> :ok
      Keyword.has_key?(opts, :started) -> :running
      true -> :pending
    end
  end

  defp at_or_nil(nil), do: nil
  defp at_or_nil(seconds), do: at(seconds)

  defp at(seconds), do: DateTime.add(@start, seconds, :second)
end
