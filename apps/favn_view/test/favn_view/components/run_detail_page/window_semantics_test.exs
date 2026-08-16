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

  test "a multi-window run reports window progress separately from asset progress" do
    html = render_page(Runs.backfill(:running))

    assert html =~ ~s(data-testid="asset-progress")
    assert html =~ ~s(data-testid="window-progress")
    assert html =~ "2 windows"
  end

  test "a single-window run says nothing about window progress" do
    html = render_page(Runs.single_window())

    assert html =~ ~s(data-testid="asset-progress")
    refute html =~ ~s(data-testid="window-progress")
  end

  test "an outcome that did not occur is absent rather than shown as zero" do
    html = render_page(Runs.single_window())

    assert html =~ "2 ran"
    refute html =~ "0 failed"
    refute html =~ "0 running"
  end

  test "already-fresh work is named separately from work that ran" do
    html = render_page(Runs.backfill(:ok))

    assert html =~ "already fresh"
    assert html =~ ~s(data-visual="marker")
  end

  test "a bounded detail slice is marked without implying the meters are partial" do
    run = Map.put(Runs.backfill(:running), :asset_attempts_truncated?, true)
    html = render_page(run)

    assert html =~ "run-detail-truncated-warning"
    assert html =~ "meters above are exact"
  end

  test "window runs are only counted in the rail when there is more than one" do
    assert render_page(Runs.backfill(:running)) =~ "Window runs"
    refute render_page(Runs.single_window()) =~ ~s(data-testid="window-progress")
  end
end
