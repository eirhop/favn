defmodule FavnView.Components.RunDetailPage.TimelineTest do
  @moduledoc """
  Covers what the chart draws, not where the bars land.

  Geometry has its own tests in `FavnView.RunTimelineTest`. What matters here is
  that a bar leads to its attempt, that a lane with no attempt still reads as a
  lane, and that a live run is visibly live.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage.Timeline
  alias FavnView.RunTimeline

  @start ~U[2026-08-25 10:00:00Z]

  test "a bar is the way into the attempt it draws" do
    html = render(rows(), now: at(60))

    assert html =~ ~s(data-testid="run-timeline-bar")
    assert html =~ ~s(href="/runs/run-1/assets/orders")
    assert html =~ "bg-success"
    assert html =~ "bg-error"
  end

  test "a lane with no attempt yet says what it is waiting as" do
    html = render(rows(), now: at(60))

    assert html =~ ~s(data-testid="run-timeline-ghost")
    assert html =~ "Pending"

    # A ghost is a labelled track, never a zero-length bar at the origin.
    assert count(html, ~s(data-testid="run-timeline-bar")) == 3
  end

  test "a live run is visibly live and carries how long it has to advance" do
    html = render(rows(), now: at(60))

    assert html =~ ~s(data-running="true")
    assert html =~ ~s(data-testid="run-timeline-now")
    assert html =~ "--favn-timeline-advance:"
  end

  test "a finished run has no now line and nothing to advance" do
    finished = Enum.map(rows(), &Map.put(&1, :finished_at, at(30)))
    html = render(finished, now: at(90))

    refute html =~ ~s(data-testid="run-timeline-now")
    refute html =~ ~s(data-running="true")
  end

  test "stage bands name the stage and summarise what happened in it" do
    html = render(rows(), now: at(60))

    assert html =~ "Stage 1"
    assert html =~ "Stage 2"
    assert html =~ "1 succeeded"
  end

  test "a dense chart drops the label column and collapses its bands" do
    html = render(many(220), now: at(60))

    assert html =~ ~s(data-density="dense")
    assert html =~ ~s(data-collapsed="true")
    assert html =~ ~s(data-testid="run-timeline-band-strip")
    refute html =~ ~s(data-testid="run-timeline-lane")
  end

  test "a run with nothing started keeps its lanes and drops the axis" do
    html = render([row("orders", "Orders", :pending, nil, nil)], now: at(60))

    assert html =~ ~s(data-testid="run-timeline-lane")
    assert html =~ ~s(data-testid="run-timeline-ghost")
    refute html =~ ~s(data-testid="run-timeline-axis")
  end

  test "a filter that matches nothing says so rather than drawing an empty chart" do
    html = render_component(&Timeline.timeline/1, chart: RunTimeline.build([], now: at(60)))

    assert html =~ "No assets match the filter"
    refute html =~ ~s(data-testid="run-timeline-lane")
  end

  test "lanes sort by name on request and by start otherwise" do
    by_start = render(rows(), now: at(60))
    by_name = render(rows(), now: at(60), sort: :name)

    # Both assets are in stage 1, so only the order within the band moves.
    assert position(by_start, "Orders") < position(by_start, "Engagement")
    assert position(by_name, "Engagement") < position(by_name, "Orders")
  end

  defp render(rows, opts) do
    render_component(&Timeline.timeline/1, chart: RunTimeline.build(rows, opts))
  end

  defp rows do
    [
      row("orders", "Orders", :ok, at(0), at(20), 1),
      row("engagement", "Engagement", :error, at(5), at(15), 1),
      row("revenue", "Revenue", :running, at(25), nil, 2),
      row("summary", "Summary", :pending, nil, nil, 2)
    ]
  end

  defp many(count) do
    Enum.map(1..count, fn index ->
      row("a#{index}", "Asset #{index}", :ok, at(index), at(index + 1), rem(index, 2))
    end)
  end

  defp row(id, name, state, started_at, finished_at, stage \\ nil) do
    %{
      id: id,
      run_id: "run-1",
      name: name,
      asset_ref: "crm.#{id}",
      state: state,
      stage: stage,
      started_at: started_at,
      finished_at: finished_at
    }
  end

  defp at(seconds), do: DateTime.add(@start, seconds, :second)

  defp count(html, needle), do: length(String.split(html, needle)) - 1

  # A lane's label precedes its own bar, so first occurrence is lane order.
  defp position(html, needle), do: html |> String.split(needle) |> hd() |> String.length()
end
