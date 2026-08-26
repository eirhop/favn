defmodule FavnView.Components.RunDetailPage.ComparisonTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage.Comparison
  alias FavnView.RunComparison

  @start ~U[2026-08-25 10:00:00Z]

  test "draws one track per window, each row naming its own window" do
    html = draw(windows())

    assert html =~ ~s(data-testid="run-comparison")
    assert count(html, ~s(data-testid="run-comparison-lane")) == 2

    # Two windows and two lanes: four tracks, whatever each window planned.
    assert count(html, ~s(data-testid="run-comparison-track")) == 4
    assert html =~ ~s(data-track="1")
    assert html =~ ~s(data-track="2")

    # No legend of its own: the rail that picked the windows already lists them
    # against these numbers, so every row carries its number and its window is
    # one hover away.
    refute html =~ ~s(data-testid="run-comparison-legend")
    assert count(html, ~s(class="favn-comparison-index favn-text-subtle")) == 4
    assert count(html, ~s(title="orders · Window 1 · Succeeded")) == 1
    assert count(html, ~s(title="orders · Window 2 · Succeeded")) == 1
  end

  test "a bar links to the attempt in its own window's run" do
    html = draw(windows())

    assert html =~ ~s(href="/runs/run-a/assets/orders")
    assert html =~ ~s(href="/runs/run-b/assets/orders")
    assert html =~ ~s(data-testid="run-comparison-bar")
  end

  test "each kind of empty track says which kind it is" do
    html = draw(windows())

    # run-a never planned refunds; the gap says so rather than reading as a
    # skip, a failure, or a window that has not got there yet.
    assert html =~ ~s(data-presence="absent")
    assert html =~ "Not planned"
    assert html =~ "This window did not plan this asset"
  end

  test "an unreadable window is named as unavailable on every track it owns" do
    html =
      draw([
        window(1, "run-a", [row("orders", started: 0, finished: 10)]),
        %{
          run_id: "run-b",
          track: 2,
          state: :unavailable,
          label: "Aug 2",
          reason: :unavailable,
          selected?: false,
          assets: []
        }
      ])

    assert html =~ ~s(data-presence="unavailable")
    assert html =~ "This window could not be read"

    # The window is named on the track rather than only in a legend, so the
    # failure says which window failed wherever the operator is looking.
    assert html =~ ~s(title="orders · Aug 2 · This window could not be read")
  end

  test "offers both alignments and disables wall clock when it would show nothing" do
    near = draw(windows())
    assert count(near, ~s(data-testid="run-comparison-alignment-option")) == 2
    refute near =~ "disabled"
    refute near =~ ~s(data-testid="run-comparison-alignment-unavailable")

    far =
      draw([
        window(1, "run-a", [row("orders", started: 0, finished: 10)]),
        window(2, "run-b", [row("orders", started: 3_600, finished: 3_620)])
      ])

    # The control explains itself rather than simply being dead.
    assert far =~ "disabled"
    assert far =~ ~s(data-testid="run-comparison-alignment-unavailable")
    assert far =~ "the longest run"
  end

  test "the alignment in force is the one the chart reports" do
    html = draw(windows(), alignment: :wall_clock)

    assert html =~ ~s(data-alignment="wall_clock")
  end

  defp draw(windows, opts \\ []) do
    chart = RunComparison.build(windows, Keyword.put_new(opts, :now, at(30)))

    render_component(&Comparison.comparison/1, chart: chart)
  end

  defp windows do
    [
      window(1, "run-a", [row("orders", started: 0, finished: 10)]),
      window(2, "run-b", [
        row("orders", started: 5, finished: 25),
        row("refunds", started: 10, finished: 20)
      ])
    ]
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
      state: :ok,
      stage: 1,
      started_at: at(Keyword.fetch!(opts, :started)),
      finished_at: at(Keyword.fetch!(opts, :finished))
    }
  end

  defp at(seconds), do: DateTime.add(@start, seconds, :second)

  defp count(html, fragment), do: html |> String.split(fragment) |> length() |> Kernel.-(1)
end
