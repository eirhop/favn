defmodule FavnView.Components.RunDetailPage.WindowSemanticsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage
  alias FavnView.Components.RunDetailPage.WindowRail
  alias FavnView.Dev.DesignSystem.Fixtures.Runs
  alias FavnView.RunWindowRail

  defp render_page(run, opts \\ []) do
    render_component(
      &RunDetailPage.run_detail_page/1,
      Keyword.merge(
        [
          run: run,
          run_id: run.id,
          current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}}
        ],
        opts
      )
    )
  end

  test "Flow draws one exact run as lanes on its own axis" do
    html = render_page(Runs.single_window())

    assert html =~ ~s(data-testid="asset-progress")
    assert html =~ ~s(data-testid="run-flow")
    assert html =~ ~s(data-testid="run-timeline")
    assert html =~ ~s(data-testid="run-timeline-bar")
    assert html =~ "Orders"

    # The chart is the default reading, so the table is not also rendered.
    refute html =~ ~s(data-testid="run-asset-row")
    refute html =~ "Output metadata"
    refute html =~ ~s(data-testid="window-progress")
  end

  test "Flow keeps the lean asset list one click away" do
    html = render_page(Runs.single_window(), flow_view: :table)

    assert html =~ ~s(data-testid="run-asset-row")
    assert html =~ "Orders"
    assert html =~ "Started"
    assert html =~ "Finished"
    refute html =~ ~s(data-testid="run-timeline")
  end

  test "a run outside a backfill shows no rail and no window controls" do
    html = render_page(Runs.single_window())

    refute html =~ ~s(data-testid="window-rail")
    refute html =~ ~s(data-testid="load-run-windows")
    refute html =~ ~s(data-testid="run-window-selector")
  end

  test "sibling window runs render as one selectable calendar rail" do
    run = Runs.single_window()

    rail =
      rail(
        [
          window("run_daily_orders_2026_07_22", ~U[2026-07-22 00:00:00Z]),
          window(run.id, ~U[2026-07-23 00:00:00Z]),
          window("run_daily_orders_2026_07_24", ~U[2026-07-24 00:00:00Z])
        ],
        run.id
      )

    html = render_page(run, rail: rail)

    assert html =~ ~s(data-testid="window-rail")
    assert count(html, ~s(data-testid="window-rail-cell")) == 3

    # Every cell is selectable, including the one already open: the rail is a
    # calendar, not a list of somewhere-else links.
    assert count(html, ~s(phx-click="select_window")) == 3
    assert html =~ ~s(aria-current="true")
    refute html =~ ~s(data-testid="window-rail-buckets")
  end

  test "the rail says the set is still growing while the backfill runs" do
    run = Runs.single_window()
    windows = [window(run.id, ~U[2026-07-23 00:00:00Z])]

    running = render_page(run, rail: rail(windows, run.id, backfill_status: :running))
    assert running =~ ~s(data-testid="window-rail-in-progress")

    completed = render_page(run, rail: rail(windows, run.id, backfill_status: :completed))
    refute completed =~ ~s(data-testid="window-rail-in-progress")
  end

  test "a truncated window read says so without claiming a total" do
    run = Runs.single_window()
    windows = [window(run.id, ~U[2026-07-23 00:00:00Z])]

    html = render_page(run, rail: rail(windows, run.id, truncated?: true))

    assert html =~ ~s(data-testid="window-rail-truncated")
    assert html =~ "older windows exist"
  end

  test "a bounded Flow slice is named precisely" do
    run = Map.put(Runs.single_window(), :asset_attempts_truncated?, true)
    html = render_page(run)

    assert html =~ "run-detail-truncated-warning"
    assert html =~ "first 1,000 in stable order"
  end

  test "a backfill parent explains its grouping role instead of claiming there is no work" do
    run =
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        total_windows: 3,
        completed_windows: 2,
        failed_windows: 1,
        total_asset_attempts: 3,
        completed_asset_attempts: 2,
        succeeded_asset_attempts: 1,
        failed_asset_attempts: 1,
        running_asset_attempts: 1,
        queued_asset_attempts: 0,
        planned_asset_attempts: 0
      })

    html = render_page(run)

    assert html =~ ~s(data-testid="backfill-parent-explanation")
    assert html =~ ~s(data-testid="window-progress")
    assert html =~ "Asset work runs in the windows"
    refute html =~ "No asset work yet"
  end

  test "a backfill parent's rail offers its children with none of them current" do
    run = Map.merge(Runs.single_window(), %{backfill_parent?: true, window: nil})

    children = [
      window("run-child-one", ~U[2026-07-01 00:00:00Z]),
      window("run-child-two", ~U[2026-08-01 00:00:00Z])
    ]

    html = render_page(run, rail: rail(children, run.id))

    assert count(html, ~s(data-testid="window-rail-cell")) == 2
    assert html =~ ~s(phx-value-run_id="run-child-one")
    assert html =~ ~s(phx-value-run_id="run-child-two")

    # The parent is not one of its own windows, so no cell is current.
    refute html =~ ~s(aria-current="true")
  end

  test "the rail offers a comparison and picks windows instead of opening them" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    off = render_page(run, rail: rail(windows, run.id))
    assert off =~ ~s(data-testid="window-rail-compare-toggle")
    assert off =~ ~s(aria-pressed="false")
    assert count(off, ~s(phx-click="select_window")) == 3
    refute off =~ ~s(phx-click="toggle_compare_window")

    on =
      render_page(run,
        rail: rail(windows, run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    # In compare mode a cell picks a window to draw, so nothing in the rail
    # navigates and every cell reports whether it is in the comparison.
    assert count(on, ~s(phx-click="toggle_compare_window")) == 3
    refute on =~ ~s(phx-click="select_window")
    assert count(on, ~s(data-compared="true")) == 2
    assert count(on, ~s(data-compared="false")) == 1
    assert on =~ ~s(data-track="1")
    assert on =~ ~s(data-track="2")
  end

  test "the rail explains a full comparison rather than silently ignoring a click" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    html =
      render_page(run,
        rail: rail(windows, run.id),
        compare?: true,
        compare_limit_reached?: true
      )

    assert html =~ ~s(data-testid="window-rail-compare-limit")
    assert html =~ "at most #{RunWindowRail.compare_limit()} windows"

    # The refusal is a response to a click, not a standing part of the rail.
    refute render_page(run, rail: rail(windows, run.id), compare?: true) =~
             ~s(data-testid="window-rail-compare-limit")
  end

  test "a comparison takes the chart's place rather than sitting beside it" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    html =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(windows, run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    assert html =~ ~s(data-testid="run-comparison")
    assert html =~ ~s(data-testid="run-comparison-track")

    # One chart at a time: the single-run timeline and its filters belong to a
    # single window and would narrow only one track of the comparison.
    refute html =~ ~s(data-testid="run-timeline")
    refute html =~ ~s(data-testid="run-flow-controls")
  end

  test "the rail is one surface whose cells light up inside it" do
    run = Runs.single_window()

    # Rendered alone, because the page also carries a mode rail built from the
    # same element and counting both would say nothing about this one.
    html =
      render_component(&WindowRail.window_rail/1, rail: rail(compare_windows(run.id), run.id))

    # A window run is a position on a calendar, so the calendar is the object on
    # screen. Cells that each carried their own border and status fill read as
    # three separate controls and drowned the one that was selected.
    assert html =~ ~s(favn-surface-rail)
    assert count(html, "favn-mode-item h-9") == 3
    assert count(html, "favn-mode-item-active") == 1
    refute html =~ "rounded-md border px-2 py-1 text-center"

    # Status still shows, on a dot, so six statuses cannot fight the selected
    # state for the same pixels.
    assert count(html, ~s(class="status status-xs)) == 3
  end

  test "a compared window is numbered, and the number is what the chart repeats" do
    run = Runs.single_window()

    html =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(compare_windows(run.id), run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    # The rail is the only list of compared windows: it numbers the two it
    # picked, and the chart repeats those numbers on every track row. A second
    # copy of the list inside the chart said nothing the rail did not.
    assert count(html, ~s(data-compared="true")) == 2
    assert count(html, ~s(class="favn-track-index")) == 2
    refute html =~ ~s(data-testid="run-comparison-legend")
    assert html =~ ~s(class="favn-comparison-index favn-text-subtle")

    # "T1" named a track in a vocabulary the page never introduced.
    refute html =~ "T1"
  end

  test "a combined run states its span in its header instead of offering a rail" do
    run = Runs.single_window()

    combined =
      Enum.map(0..4, fn index ->
        window("run-combined", DateTime.add(~U[2026-07-10 00:00:00Z], index, :day))
      end)

    rail = rail(combined, "run-combined", backfill_status: :completed)

    html =
      render_page(
        Map.put(run, :combined_window, %{
          label: "Jul 10 00:00 – Jul 15 00:00, 2026",
          window_count: 5
        }),
        rail: rail
      )

    # One run covering five windows has one place to navigate to, and it is this
    # page. A rail of one cell is not a calendar, so the span is a property of
    # the run and sits with the run's other properties.
    refute html =~ ~s(data-testid="window-rail")
    assert html =~ "Combined window"
    assert html =~ "Jul 10 00:00 – Jul 15 00:00, 2026 · 5 windows"
  end

  test "a panel's title keeps its inset even when its body owns the spacing" do
    run = Runs.single_window()
    html = render_page(run, rail: rail(compare_windows(run.id), run.id))

    # `padding={:none}` says the body owns its spacing — a chart that wants the
    # width, a table with its own cells. It never meant the heading should sit
    # on the card's border.
    assert html =~ ~s(px-5 pt-5 sm:px-6 sm:pt-6)
  end

  test "compare is offered only where the chart it draws exists" do
    run = Runs.single_window()
    html = render_page(run, rail: rail(compare_windows(run.id), run.id))

    # Below `lg` the page shows the card list, so the toggle would enter a mode
    # that changes nothing. A comparison already open says where its chart went
    # rather than showing this window's rows as if they were the comparison.
    assert html =~ ~s(hidden lg:inline-flex)

    comparing =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(compare_windows(run.id), run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    assert comparing =~ ~s(data-testid="run-comparison-narrow")
    assert comparing =~ "needs a wider screen"
  end

  test "a failed window read is stated on the page, not just absent from it" do
    run = Runs.single_window()

    html =
      render_page(run,
        rail: nil,
        windows_error: "Window runs could not be loaded. The page will try again."
      )

    assert html =~ ~s(data-testid="window-read-warning")
    assert html =~ "could not be loaded"

    # The rail is gone and the rest of the run page is untouched.
    refute html =~ ~s(data-testid="window-rail")
    assert html =~ ~s(data-testid="run-flow")
  end

  test "a comparison that lost every window says so where the chart was" do
    run = Runs.single_window()

    html =
      render_page(run,
        compare_error: "No compared window could be read. Showing this window on its own."
      )

    assert html =~ ~s(data-testid="compare-warning")
    assert html =~ "Showing this window on its own"

    # It has fallen back, so the single-run chart is what is drawn.
    assert html =~ ~s(data-testid="run-timeline")
    refute html =~ ~s(data-testid="run-comparison")
  end

  defp compare_windows(run_id) do
    [
      window("run-earlier", ~U[2026-07-22 00:00:00Z]),
      window(run_id, ~U[2026-07-23 00:00:00Z]),
      window("run-later", ~U[2026-07-24 00:00:00Z])
    ]
  end

  defp rail(windows, selected_run_id, opts \\ []) do
    RunWindowRail.build(windows, selected_run_id, "Etc/UTC", opts)
  end

  defp window(run_id, start_at, opts \\ []) do
    %{
      run_id: run_id,
      window_start_at: start_at,
      window_end_at: DateTime.add(start_at, 1, :day),
      status: Keyword.get(opts, :status, :succeeded),
      kind: :day,
      timezone: "Etc/UTC"
    }
  end

  defp count(html, fragment), do: html |> String.split(fragment) |> length() |> Kernel.-(1)
end
