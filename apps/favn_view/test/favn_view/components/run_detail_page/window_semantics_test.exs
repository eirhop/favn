defmodule FavnView.Components.RunDetailPage.WindowSemanticsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage
  alias FavnView.Dev.DesignSystem.Fixtures.Runs

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

  test "Flow renders one exact run as a lean asset list" do
    html = render_page(Runs.single_window())

    assert html =~ ~s(data-testid="asset-progress")
    assert html =~ ~s(data-testid="run-flow")
    assert html =~ ~s(data-testid="run-asset-row")
    assert html =~ "Orders"
    assert html =~ "Started"
    assert html =~ "Finished"
    refute html =~ "Output metadata"
    refute html =~ ~s(data-testid="window-progress")
  end

  test "window choices are absent until explicitly loaded" do
    html = render_page(Runs.single_window())

    assert html =~ ~s(data-testid="load-run-windows")
    refute html =~ ~s(data-testid="run-window-selector")
  end

  test "loaded sibling windows replace the button with one compact selector" do
    windows = [
      %{run_id: "run_daily_orders_2026_05_19", label: "Jul 22 – Jul 23"},
      %{run_id: "run_daily_orders_2026_05_20", label: "Jul 23 – Jul 24"},
      %{run_id: "run_daily_orders_2026_05_21", label: "Jul 24 – Jul 25"}
    ]

    html = render_page(Runs.single_window(), windows: windows)

    assert html =~ ~s(data-testid="run-window-selector")
    assert html =~ "Select a window run"
    refute html =~ "Jul 22 – Jul 23"
    assert html =~ "Jul 23 – Jul 24"
    refute html =~ ~s(data-testid="load-run-windows")
  end

  test "a one-window child does not link back to itself" do
    run = Runs.single_window()
    windows = [%{run_id: run.id, label: "Jul 22 – Jul 23"}]

    html = render_page(run, windows: windows)

    refute html =~ ~s(data-testid="open-run-window")
    refute html =~ ~s(data-testid="run-window-selector")
    refute html =~ ~s(data-testid="load-run-windows")
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
    assert html =~ ~s(data-testid="load-run-windows")
    assert html =~ "Asset work runs in the windows"
    refute html =~ "No asset work yet"
  end

  test "a multi-window parent starts its selector on an explicit prompt" do
    run =
      Runs.single_window()
      |> Map.merge(%{backfill_parent?: true, window: nil})

    windows = [
      %{run_id: "run-child-one", label: "Jul 1 – Aug 1"},
      %{run_id: "run-child-two", label: "Aug 1 – Sep 1"}
    ]

    html = render_page(run, windows: windows)

    assert html =~ ~s(data-testid="run-window-selector")
    assert html =~ "Select a window run"
    assert html =~ ~s(value="run-child-one")
    assert html =~ ~s(value="run-child-two")
  end
end
