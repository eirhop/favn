defmodule FavnView.Components.RunDetailPage.WindowSemanticsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage
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
    assert html =~ ~s(data-testid="run-comparison-track-head")

    # One chart at a time: the single-run timeline and its filters belong to a
    # single window and would narrow only one track of the comparison.
    refute html =~ ~s(data-testid="run-timeline")
    refute html =~ ~s(data-testid="run-flow-controls")
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
