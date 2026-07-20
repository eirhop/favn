defmodule FavnView.Components.AssetDetailPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetDetailPage
  alias FavnView.AssetDetailLive

  test "renders calendar freshness separately from actionable run and data timelines" do
    html =
      render_component(&AssetDetailPage.window_timeline_panel/1,
        window_kind_label: "Monthly windows",
        refresh_timeline_label: "Monthly run anchors",
        refresh_cadence_label: "Monthly run anchors Europe/Oslo",
        freshness_timeline_label: "Daily freshness periods",
        freshness_cadence_label: "Daily freshness Europe/Oslo",
        data_coverage_timeline_label: "Monthly data windows",
        window_range: "Jun 2026 - Jul 2026",
        refresh_window_range: "Jun 2026 - Jul 2026",
        freshness_window_range: "Jul 16 - Jul 17",
        data_coverage_window_range: "Jun 2026 - Jul 2026",
        active_timeline: :freshness,
        has_freshness_timeline?: true,
        has_data_windows?: true,
        refresh_timeline: [],
        freshness_timeline: [freshness_window()],
        data_coverage_timeline: [],
        freshness: nil
      )

    assert html =~ "Freshness timeline"
    assert html =~ "Daily freshness Europe/Oslo"
    assert html =~ ~s(data-testid="freshness-timeline-toggle")
    assert html =~ ~s(data-testid="refresh-timeline-toggle")
    assert html =~ ~s(data-testid="data-coverage-timeline-toggle")
    assert html =~ ~s(disabled)
    refute html =~ ~s(data-testid="selected-window-actions")
    refute html =~ ~s(phx-click="select_window")
  end

  test "rejects forged run events while the read-only freshness timeline is active" do
    socket = %Phoenix.LiveView.Socket{
      assigns: %{__changed__: %{}, active_timeline: :freshness}
    }

    assert {:noreply, open_socket} =
             AssetDetailLive.handle_event("open_run_config", %{}, socket)

    assert open_socket.assigns.selected_window_error == "Freshness periods are read-only."

    assert {:noreply, submit_socket} =
             AssetDetailLive.handle_event("run_selected_window", %{}, socket)

    assert submit_socket.assigns.selected_window_error == "Freshness periods are read-only."
    refute submit_socket.assigns.run_config_open?
    refute submit_socket.assigns.submitting_window_run?
  end

  defp freshness_window do
    %{
      id: "freshness:day:2026-07-17",
      label: "Jul 17",
      date_label: "Jul 17, 2026",
      status: :success
    }
  end
end
