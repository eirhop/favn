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
      %{run_id: "run_daily_orders_2026_05_20", label: "Jul 23 – Jul 24"}
    ]

    html = render_page(Runs.single_window(), windows: windows)

    assert html =~ ~s(data-testid="run-window-selector")
    assert html =~ "Jul 22 – Jul 23"
    refute html =~ ~s(data-testid="load-run-windows")
  end

  test "a bounded Flow slice is named precisely" do
    run = Map.put(Runs.single_window(), :asset_attempts_truncated?, true)
    html = render_page(run)

    assert html =~ "run-detail-truncated-warning"
    assert html =~ "first 1,000 in stable order"
  end
end
