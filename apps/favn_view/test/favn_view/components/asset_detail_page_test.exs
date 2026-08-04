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

  test "ignores duplicate missing-coverage submissions while one is in flight" do
    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        submitting_coverage?: true,
        coverage_plan: %{plan_hash: String.duplicate("a", 64)}
      }
    }

    assert {:noreply, ^socket} =
             AssetDetailLive.handle_event("submit_missing_coverage", %{}, socket)
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

  test "renders structured target compatibility and a blocking explanation" do
    html =
      render_component(&AssetDetailPage.compatibility_panel/1,
        compatibility: %{
          status: :rebuild_required,
          reason_code: "incompatible_descriptor",
          diff: %{
            descriptor: [
              %{field: :window_identity, active: %{kind: :day}, desired: %{kind: :month}}
            ]
          },
          active_generation_id: "generation-orders-v1",
          desired_descriptor_hash: String.duplicate("a", 64),
          physical_fingerprint: String.duplicate("b", 64),
          persisted?: true,
          blocks_writes?: true
        }
      )

    assert html =~ ~s(data-testid="asset-compatibility-panel")
    assert html =~ "Rebuild required"
    assert html =~ "generation-orders-v1"
    assert html =~ "Compatibility differences"
    assert html =~ ~s(data-testid="asset-compatibility-blocked")
    assert html =~ "Runs and backfills are blocked"
  end

  test "keeps ordinary writes available when only a rebuild is optional" do
    html =
      render_component(&AssetDetailPage.compatibility_panel/1,
        compatibility: %{
          status: :rebuild_available,
          reason_code: "execution_package_changed",
          diff: %{},
          active_generation_id: "generation-orders-v1",
          persisted?: true,
          blocks_writes?: false
        }
      )

    assert html =~ "Rebuild available"
    assert html =~ "ordinary writes remain allowed"
    refute html =~ ~s(data-testid="asset-compatibility-blocked")
  end

  test "contract claims and hand-written checks share one table" do
    html =
      render_component(&AssetDetailPage.assurance_panel/1,
        assurance: %{
          quality_status: :passed,
          write_outcome: :written,
          latest_run_id: nil,
          contract_validation: nil,
          checks: [
            %{
              name: :orders_have_customer,
              origin: :asset,
              claim_id: nil,
              phase: :after_materialize,
              when: nil,
              on_violation: :fail,
              message: "every order names a customer",
              latest_result: %{outcome: :passed, metrics: %{actual: 0}}
            },
            # A generated check restates a row-count claim, so it must not appear
            # twice under two names.
            %{
              name: :row_count_min_1,
              origin: :contract,
              claim_id: "row_count.min.1",
              phase: :after_materialize,
              when: nil,
              on_violation: :fail,
              message: nil,
              latest_result: %{outcome: :passed}
            }
          ],
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
                latest_result: %{outcome: :passed, metrics: %{actual: 1_284}}
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

    rows = html |> String.split(~s(data-testid="asset-quality-check")) |> length()

    # Two claims plus one hand-written check. The generated duplicate of
    # row_count.min.1 is dropped rather than listed beside the claim it restates.
    assert rows - 1 == 3

    assert html =~ "Exactly @expected_rows"
    assert html =~ "At least 1"
    assert html =~ "every order names a customer"
    assert html =~ "1,284"

    # A check whose condition never held is not a pass, and must not read as one.
    assert html =~ "not needed"
    assert html =~ "2 of 3 passed"
  end

  test "columns stay shut when they matched and open themselves when they did not" do
    matched =
      render_component(&AssetDetailPage.assurance_panel/1, assurance: columns_assurance(:matched))

    assert matched =~ "3 of 3 matched"
    refute matched =~ "<details open"

    drifted =
      render_component(&AssetDetailPage.assurance_panel/1, assurance: columns_assurance(:drifted))

    assert drifted =~ "2 of 3 matched"
    assert drifted =~ "<details open"
    assert drifted =~ "does not match what the asset promises"
  end

  test "says once that a run recorded no columns instead of once per column" do
    html =
      render_component(&AssetDetailPage.assurance_panel/1,
        assurance: %{
          quality_status: nil,
          write_outcome: nil,
          latest_run_id: nil,
          contract_validation: nil,
          checks: [],
          contract: %{
            grain: %{by: [:order_id], description: "one customer order"},
            unique_keys: [[:order_id]],
            row_counts: [],
            columns: [
              contract_column(:order_id, :integer),
              contract_column(:total, :decimal),
              contract_column(:placed_at, :datetime)
            ]
          }
        }
      )

    assert html =~ "did not record the table's actual columns"
    assert html =~ "3 promised"
    refute html =~ "not found"
    assert html =~ "One row per"
    assert html =~ "order_id · one customer order"
  end

  defp columns_assurance(state) do
    observed =
      case state do
        :matched ->
          [observed_column(:order_id, :integer), observed_column(:total, :decimal)]

        :drifted ->
          [observed_column(:order_id, :integer), observed_column(:total, :string)]
      end

    %{
      quality_status: :passed,
      write_outcome: :written,
      latest_run_id: "run-1",
      checks: [],
      contract_validation: %{
        status: (state == :matched && :passed) || :failed,
        expected_columns: [:order_id, :total, :placed_at],
        observed_columns: observed ++ [observed_column(:placed_at, :datetime)],
        differences:
          (state == :drifted &&
             [%{kind: :type_mismatch, column: :total, expected: :decimal, observed: :string}]) ||
            [],
        observed_column_count: 3,
        observed_truncated?: false
      },
      contract: %{
        grain: nil,
        unique_keys: [],
        row_counts: [],
        columns: [
          contract_column(:order_id, :integer),
          contract_column(:total, :decimal),
          contract_column(:placed_at, :datetime)
        ]
      }
    }
  end

  defp contract_column(name, type) do
    %{
      name: name,
      type: type,
      nullable?: false,
      description: nil,
      tags: [],
      via: nil,
      sources: [],
      origin: %{kind: :local}
    }
  end

  defp observed_column(name, type) do
    %{
      name: name,
      type: type,
      native_type: to_string(type),
      nullable?: false,
      nullability_observed?: true
    }
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
