defmodule FavnView.Components.AssetDetailPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetDetailPage
  alias FavnView.AssetDetailLive

  # A disabled button is a hint, not a guard: the event can still arrive from a forged
  # message, so both events check the permission themselves rather than trusting that
  # the control that sends them was rendered enabled.
  test "refuses a forged run event from someone who cannot submit runs" do
    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        can_submit_runs?: false,
        run_config: %{},
        asset: %{can_run_asset?: true, default_run_config: nil}
      }
    }

    assert {:noreply, open_socket} = AssetDetailLive.handle_event("open_run_config", %{}, socket)
    assert open_socket.assigns.run_error == "Operator role required to submit runs."
    refute open_socket.assigns[:run_config_open?]

    assert {:noreply, submit_socket} = AssetDetailLive.handle_event("submit_run", %{}, socket)
    assert submit_socket.assigns.run_error == "Operator role required to submit runs."
  end

  test "refuses a forged run event for an asset that cannot be run" do
    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        can_submit_runs?: true,
        run_config: %{},
        asset: %{can_run_asset?: false, default_run_config: nil}
      }
    }

    assert {:noreply, open_socket} = AssetDetailLive.handle_event("open_run_config", %{}, socket)
    assert open_socket.assigns.run_error == "This asset cannot be run."

    assert {:noreply, submit_socket} = AssetDetailLive.handle_event("submit_run", %{}, socket)
    assert submit_socket.assigns.run_error == "This asset cannot be run."
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

  test "names what changed in the asset's own terms and offers the fix beside it" do
    html =
      render_component(&AssetDetailPage.diagnostics_panel/1,
        rebuild_target_id: "asset:orders",
        manifest_version_id: "mv_0001",
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

    assert html =~ ~s(data-testid="asset-diagnostics")
    assert html =~ "Rebuild required"
    assert html =~ "no longer fits the one it has"
    assert html =~ ~s(data-testid="asset-compatibility-blocked")
    assert html =~ "Nothing can run until this is resolved."

    # A `descriptor` diff holds a list of per-field changes, and the field name is
    # Favn's rather than the operator's. Both are translated.
    assert html =~ "Period shape"
    refute html =~ "window_identity"

    assert html =~ ~s(data-testid="plan-asset-rebuild")

    # The identifiers the verdict was computed from are still reachable, one
    # disclosure down, because they are what someone comparing two deployments needs.
    assert html =~ "Identifiers Favn matched on"
    assert html =~ "generation-orders-v1"
    assert html =~ "mv_0001"
  end

  test "an optional rebuild says writes still work and still offers one" do
    html =
      render_component(&AssetDetailPage.diagnostics_panel/1,
        rebuild_target_id: "asset:orders",
        compatibility: %{
          status: :rebuild_available,
          reason_code: "execution_package_changed",
          diff: %{execution_package_hash: %{active: "aaaa", desired: "bbbb"}},
          active_generation_id: "generation-orders-v1",
          persisted?: true,
          blocks_writes?: false
        }
      )

    assert html =~ "Rebuild available"
    assert html =~ "Runs still work"
    assert html =~ "How it is built"
    assert html =~ ~s(data-testid="plan-asset-rebuild")
    refute html =~ ~s(data-testid="asset-compatibility-blocked")
  end

  test "a healthy target offers no rebuild, because there is nothing to rebuild" do
    html =
      render_component(&AssetDetailPage.diagnostics_panel/1,
        rebuild_target_id: "asset:orders",
        compatibility: %{
          status: :ready,
          reason_code: "compatible",
          diff: %{},
          active_generation_id: "generation-orders-v1",
          persisted?: true,
          blocks_writes?: false
        }
      )

    assert html =~ "Nothing needs doing."
    refute html =~ ~s(data-testid="plan-asset-rebuild")
    refute html =~ "What changed"
  end

  test "an asset with no table of its own says so instead of showing a verdict" do
    html =
      render_component(&AssetDetailPage.diagnostics_panel/1,
        rebuild_target_id: "asset:orders",
        compatibility: %{status: :ready, reason_code: "compatible", diff: %{}, persisted?: false}
      )

    assert html =~ "does not manage a table of its own"
    refute html =~ "Compatible"
    refute html =~ ~s(data-testid="plan-asset-rebuild")
  end

  test "the coverage rules explain where the calendar's range came from" do
    html =
      render_component(&AssetDetailPage.diagnostics_panel/1,
        compatibility: %{status: :ready, reason_code: "compatible", diff: %{}, persisted?: true},
        coverage: %{
          evaluated_at: ~U[2026-07-22 12:00:00Z],
          last_expected_window: %{start_at: ~U[2026-07-21 00:00:00Z]}
        },
        coverage_policy: %{
          timezone: "Europe/Oslo",
          declared_from: ~U[2026-06-01 00:00:00Z],
          effective_from: ~U[2026-07-01 00:00:00Z],
          availability_delay_seconds: 21_600
        }
      )

    assert html =~ "How periods are counted"
    assert html =~ "Europe/Oslo"
    assert html =~ "Expected 6 hours after the window closes"

    # Coverage starts at the later of the declared date and the first build, so the
    # two are only told apart when they disagree, and here they do.
    assert html =~ "declared Jun 1, 2026"
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
end
