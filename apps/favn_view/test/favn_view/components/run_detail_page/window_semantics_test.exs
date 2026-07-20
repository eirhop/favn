defmodule FavnView.Components.RunDetailPage.WindowSemanticsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage
  alias FavnView.Components.RunDetailPage.Overview
  alias FavnView.Components.RunDetailPage.Stats

  test "labels requested anchors separately from effective runtime windows" do
    run =
      :running
      |> RunDetailPage.sample_execution_group()
      |> Map.put(:effective_window_count, 7)

    stats = render_component(&Stats.execution_group_stats/1, run: run)
    overview = render_component(&Overview.overview_panel/1, run: run)

    assert stats =~ "Requested windows"
    assert stats =~ "anchors complete"
    assert stats =~ "Effective windows"
    assert stats =~ "asset runtime windows"
    assert overview =~ "Effective asset windows"
    assert overview =~ "lookback expansion"
  end

  test "visibly marks a bounded detail slice while retaining exact header totals" do
    run =
      :running
      |> RunDetailPage.sample_execution_group()
      |> Map.put(:asset_attempts_truncated?, true)

    html =
      render_component(&RunDetailPage.run_detail_page/1,
        run: run,
        run_id: run.id,
        active_mode: :overview
      )

    assert html =~ "run-detail-truncated-warning"
    assert html =~ "Header totals are exact"
    assert html =~ "some detail rows are omitted"
  end
end
