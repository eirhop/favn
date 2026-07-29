defmodule FavnView.RunFlowTest do
  use ExUnit.Case, async: true

  alias FavnView.RunFlow

  doctest RunFlow

  @anchor ~U[2026-07-23 10:00:00Z]

  defp attempt(overrides) do
    Map.merge(
      %{
        id: "attempt-1",
        asset_key: "crm.orders",
        short_asset_name: "Orders",
        stage: 1,
        raw_status: :ok,
        status: "Succeeded",
        status_tone: :success,
        started_at_raw: @anchor,
        finished_at_raw: DateTime.add(@anchor, 30, :second),
        duration: "30s",
        window_label: "Jul 23",
        error_summary: nil
      },
      Map.new(overrides)
    )
  end

  defp lanes(flow), do: Enum.flat_map(flow.stages, & &1.lanes)
  defp lane(flow, key), do: Enum.find(lanes(flow), &(&1.key == key))

  describe "stages" do
    test "groups lanes by stage in dependency order" do
      flow =
        RunFlow.build([
          attempt(id: "b", asset_key: "crm.revenue", short_asset_name: "Revenue", stage: 2),
          attempt(id: "a", asset_key: "crm.orders", stage: 1)
        ])

      assert Enum.map(flow.stages, & &1.label) == ["Stage 1", "Stage 2"]
    end

    test "an attempt with no stage lands in one ungrouped stage" do
      flow = RunFlow.build([attempt(stage: nil)])

      assert [%{id: "stage-unknown", label: "Ungrouped"}] = flow.stages
    end

    test "a stage hint counts its failures" do
      flow =
        RunFlow.build([
          attempt(id: "a", asset_key: "crm.orders"),
          attempt(
            id: "b",
            asset_key: "crm.revenue",
            short_asset_name: "Revenue",
            status_tone: :error,
            raw_status: :error,
            status: "Failed"
          )
        ])

      assert [%{hint: "2 assets · 1 failed"}] = flow.stages
    end
  end

  describe "lanes" do
    test "one lane per asset, whatever its window count" do
      flow =
        RunFlow.build([
          attempt(id: "a", window_label: "Feb 2026"),
          attempt(id: "b", window_label: "Mar 2026")
        ])

      assert [lane] = lanes(flow)
      assert lane.detail == "2 windows"
      assert length(lane.bars) == 2
    end

    test "a lane escalates to its worst outcome" do
      flow =
        RunFlow.build([
          attempt(id: "a", window_label: "Feb 2026"),
          attempt(
            id: "b",
            window_label: "Mar 2026",
            status_tone: :error,
            raw_status: :error,
            status: "Failed"
          )
        ])

      assert [%{tone: :error, status: "Failed"}] = lanes(flow)
    end

    test "a failure carries its own summary and the attempt to open" do
      flow =
        RunFlow.build([
          attempt(
            id: "failed-attempt",
            status_tone: :error,
            raw_status: :error,
            error_summary: "Contract check failed"
          )
        ])

      assert [%{error: %{summary: "Contract check failed", attempt_id: "failed-attempt"}}] =
               lanes(flow)
    end

    test "a failure with no reported reason still says something" do
      flow = RunFlow.build([attempt(status_tone: :error, raw_status: :error, error_summary: nil)])

      assert [%{error: %{summary: "Failed without a reported reason"}}] = lanes(flow)
    end
  end

  describe "bars" do
    test "a queued attempt gets no bar and says why the lane is empty" do
      flow =
        RunFlow.build(
          [
            attempt(
              raw_status: :pending,
              status: "Queued",
              status_tone: :neutral,
              started_at_raw: nil,
              finished_at_raw: nil,
              duration: "-"
            )
          ],
          active?: true
        )

      assert [%{bars: [], empty_label: "Waiting to start"}] = lanes(flow)
    end

    test "a settled run's empty lane reports the attempt status rather than waiting" do
      flow =
        RunFlow.build([
          attempt(
            raw_status: :skipped,
            status: "Skipped",
            status_tone: :neutral,
            started_at_raw: nil,
            finished_at_raw: nil
          )
        ])

      assert [%{bars: [], empty_label: "Skipped"}] = lanes(flow)
    end

    test "a running attempt extends to now and is marked as running" do
      now = DateTime.to_unix(DateTime.add(@anchor, 90, :second), :millisecond)

      flow =
        RunFlow.build(
          [
            attempt(
              raw_status: :running,
              status: "Running",
              status_tone: :info,
              finished_at_raw: nil
            )
          ],
          active?: true,
          now_ms: now
        )

      assert [%{bars: [bar]} = lane] = lanes(flow)
      assert bar.running?
      assert bar.left == 0.0

      # The axis keeps headroom past now, so the bar and the now-marker both stay
      # inside the track instead of sitting on its right edge.
      assert bar.width > 85.0 and bar.width < 100.0
      assert flow.axis.now_offset > 85.0 and flow.axis.now_offset < 100.0

      # A running attempt's `duration` is "-", so the elapsed time computed from
      # the clock is the only number the lane can honestly show.
      assert bar.elapsed_ms == 90_000
      assert lane.detail == "Jul 23 · 1m 30s elapsed"
      assert bar.title =~ "1m 30s elapsed"
    end

    test "the elapsed label advances with the clock, not with events" do
      base = DateTime.to_unix(@anchor, :millisecond)

      attempts = [
        attempt(raw_status: :running, status: "Running", status_tone: :info, finished_at_raw: nil)
      ]

      labels =
        for offset <- [5_000, 20_000, 65_000] do
          flow = RunFlow.build(attempts, active?: true, now_ms: base + offset)
          [lane] = lanes(flow)
          lane.detail
        end

      assert labels == [
               "Jul 23 · 5.0 s elapsed",
               "Jul 23 · 20.0 s elapsed",
               "Jul 23 · 1m 5s elapsed"
             ]
    end

    test "a settled run's axis ends with its last attempt, not with now" do
      much_later = DateTime.to_unix(DateTime.add(@anchor, 86_400, :second), :millisecond)

      flow = RunFlow.build([attempt([])], active?: false, now_ms: much_later)

      assert flow.axis.now_offset == nil
      assert flow.axis.end_ms - flow.axis.start_ms == 30_000
    end

    test "a very short attempt keeps a clickable minimum width" do
      flow =
        RunFlow.build([
          attempt(id: "long", finished_at_raw: DateTime.add(@anchor, 600, :second)),
          attempt(
            id: "brief",
            asset_key: "crm.brief",
            short_asset_name: "Brief",
            started_at_raw: DateTime.add(@anchor, 100, :second),
            finished_at_raw: DateTime.add(@anchor, 100, :second)
          )
        ])

      assert [bar] = lane(flow, "crm.brief").bars
      assert bar.width == 2.5
    end

    test "the last attempt's bar stays inside the track" do
      flow =
        RunFlow.build([
          attempt(id: "first", finished_at_raw: DateTime.add(@anchor, 600, :second)),
          attempt(
            id: "last",
            asset_key: "crm.last",
            short_asset_name: "Last",
            started_at_raw: DateTime.add(@anchor, 600, :second),
            finished_at_raw: DateTime.add(@anchor, 600, :second)
          )
        ])

      assert [bar] = lane(flow, "crm.last").bars
      assert bar.left + bar.width <= 100.0
    end

    test "concurrent windows for one asset get their own tracks" do
      flow =
        RunFlow.build([
          attempt(
            id: "a",
            window_label: "Feb 2026",
            finished_at_raw: DateTime.add(@anchor, 60, :second)
          ),
          attempt(
            id: "b",
            window_label: "Mar 2026",
            started_at_raw: DateTime.add(@anchor, 10, :second),
            finished_at_raw: DateTime.add(@anchor, 70, :second)
          )
        ])

      assert [lane] = lanes(flow)
      assert Enum.map(lane.bars, & &1.track) == [0, 1]
      assert lane.tracks == 2
    end

    test "sequential windows for one asset share a track" do
      flow =
        RunFlow.build([
          attempt(
            id: "a",
            window_label: "Feb 2026",
            finished_at_raw: DateTime.add(@anchor, 30, :second)
          ),
          attempt(
            id: "b",
            window_label: "Mar 2026",
            started_at_raw: DateTime.add(@anchor, 40, :second),
            finished_at_raw: DateTime.add(@anchor, 70, :second)
          )
        ])

      assert [lane] = lanes(flow)
      assert Enum.map(lane.bars, & &1.track) == [0, 0]
      assert lane.tracks == 1
    end
  end

  describe "axis" do
    test "an attempt-free run still returns a usable axis" do
      flow = RunFlow.build([])

      assert flow.stages == []
      assert length(flow.axis.ticks) == 5
      assert flow.axis.end_ms > flow.axis.start_ms
    end

    test "a sub-second run is padded so its ticks are not all the same label" do
      flow =
        RunFlow.build([
          attempt(finished_at_raw: DateTime.add(@anchor, 200, :millisecond))
        ])

      assert flow.axis.end_ms - flow.axis.start_ms == 2_000
      assert flow.axis.ticks |> Enum.map(& &1.label) |> Enum.uniq() |> length() > 1
    end

    test "ticks are aligned so the first and last labels stay inside the track" do
      flow = RunFlow.build([attempt([])])

      assert Enum.map(flow.axis.ticks, & &1.align) == [:start, :center, :center, :center, :end]
    end
  end
end
