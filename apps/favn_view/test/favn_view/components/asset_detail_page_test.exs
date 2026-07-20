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

  test "surfaces ambiguous pipeline context and renders stable selection links" do
    contexts = [
      %{
        id: "pipeline:manual",
        label: "Manual pipeline",
        href: "/assets/orders?run_context=pipeline%3Amanual",
        timezone: "Etc/UTC",
        policy: %{kind: :month, anchor: :previous_complete_period}
      },
      %{
        id: "pipeline:scheduled",
        label: "Scheduled pipeline",
        href: "/assets/orders?run_context=pipeline%3Ascheduled",
        timezone: "Europe/Oslo",
        policy: %{kind: :month, anchor: :current_period}
      }
    ]

    html =
      render_component(&AssetDetailPage.run_context_selector/1,
        contexts: contexts,
        selected: nil,
        status: :ambiguous
      )

    assert html =~ ~s(data-testid="asset-run-context-selector")
    assert html =~ ~s(data-testid="asset-run-context-required")
    assert html =~ "Select one before running it"
    assert html =~ "run_context=pipeline%3Amanual"
    assert html =~ "run_context=pipeline%3Ascheduled"
  end

  test "renders every ordered row-count claim in the assurance contract" do
    html =
      render_component(&AssetDetailPage.assurance_panel/1,
        assurance: %{
          quality_status: :passed,
          write_outcome: :written,
          latest_run_id: nil,
          contract_validation: nil,
          checks: [],
          contract: %{
            grain: nil,
            unique_keys: [],
            columns: [],
            row_counts: [
              %{
                claim_id: "row_count.equals.param.expected_rows",
                equals: %{source: :param, name: :expected_rows},
                min: nil,
                max: nil,
                when: nil,
                on_violation: :fail,
                latest_result: %{outcome: :passed}
              },
              %{
                claim_id: "row_count.min.1",
                equals: nil,
                min: 1,
                max: nil,
                when: :target_exists,
                on_violation: :skip_materialization,
                latest_result: %{outcome: :condition_skipped}
              }
            ]
          }
        }
      )

    assert html =~ ~s(data-claim-id="row_count.equals.param.expected_rows")
    assert html =~ ~s(data-claim-id="row_count.min.1")
    assert html =~ "Exactly @expected_rows"
    assert html =~ "At least 1"
    assert html =~ "passed"
    assert html =~ "condition skipped"
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
