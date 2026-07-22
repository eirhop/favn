defmodule FavnView.Components.RebuildPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RebuildPage

  test "requires separate review and start for a planned rebuild" do
    html =
      render_component(&RebuildPage.rebuilds_page/1,
        operations: [],
        plan: %{
          plan_id: "rebuild_plan_1",
          plan_hash: String.duplicate("a", 64),
          expires_at: ~U[2026-07-22 14:00:00Z],
          permissions: %{start: true},
          payload: %{
            evaluated_at: ~U[2026-07-22 13:00:00Z],
            root_target_id: "asset:orders",
            coverage: %{
              declared_from: %{
                start_at: ~U[2026-01-01 00:00:00Z],
                end_at: ~U[2026-02-01 00:00:00Z]
              },
              effective_from: %{
                start_at: ~U[2026-03-01 00:00:00Z],
                end_at: ~U[2026-04-01 00:00:00Z]
              },
              through: :latest_closed,
              availability_delay_seconds: 21_600
            },
            evaluated_range: %{
              start_at: ~U[2026-03-01 00:00:00Z],
              end_at: ~U[2026-07-01 00:00:00Z]
            },
            binding_snapshot: %{
              "asset:orders" => %{
                compatibility_status: :rebuild_required,
                reason_code: "schema_changed",
                compatibility_diff: %{added_columns: ["country"]}
              }
            },
            capabilities: %{
              "asset:orders" => %{atomic_generation_activation: true}
            },
            actions: [
              %{
                ordinal: 0,
                target_id: "asset:orders",
                action: :rebuild,
                reason: %{reason_code: "schema_changed"},
                mapping_proof: %{kind: :identity},
                pinned_input_generation_ids: [%{generation_id: "generation_input"}],
                candidate_generation: %{target_generation_id: "generation_new"}
              }
            ]
          }
        },
        target_id: "asset:orders",
        has_more?: false,
        planning?: false
      )

    assert html =~ ~s(data-testid="rebuild-plan")
    assert html =~ "Plan ready for review"
    assert html =~ ~s(data-testid="start-rebuild")
    assert html =~ "Approve and start"
    assert html =~ "Declared coverage"
    assert html =~ "Rebuild required"
    assert html =~ "schema_changed"
    assert html =~ "atomic_generation_activation"
    assert html =~ "generation_input"
  end

  test "does not render plan approval when the server denies start permission" do
    html =
      render_component(&RebuildPage.rebuilds_page/1,
        operations: [],
        plan: %{
          plan_id: "rebuild_plan_1",
          plan_hash: String.duplicate("a", 64),
          expires_at: ~U[2026-07-22 14:00:00Z],
          permissions: %{start: false}
        },
        target_id: "asset:orders",
        has_more?: false,
        planning?: false
      )

    assert html =~ ~s(data-testid="rebuild-plan")
    refute html =~ ~s(data-testid="start-rebuild")
  end

  test "renders only server-authorized operation actions" do
    html =
      render_component(&RebuildPage.rebuild_detail_page/1,
        operation: operation(%{start: false, cancel: false, retry: false, reconcile: true}),
        items: [item()],
        items_has_more?: false
      )

    assert html =~ ~s(data-testid="reconcile-rebuild")
    refute html =~ ~s(data-testid="start-rebuild")
    refute html =~ ~s(data-testid="retry-rebuild")
    refute html =~ ~s(data-testid="cancel-rebuild")
    assert html =~ "month:2026-07"
    assert html =~ "Outcome needs reconciliation"
    assert html =~ "Downstream actions"
    assert html =~ "candidate_ready"
  end

  defp operation(permissions) do
    %{
      operation_id: "rebuild_1",
      root_target_id: "asset:orders",
      state: :activation_unknown,
      phase: :activation,
      progress: %{completed: 1, total: 1},
      action_count: 1,
      window_count: 1,
      plan_hash: String.duplicate("a", 64),
      permissions: permissions,
      terminal_error: nil,
      unknown_outcome: %{reason_code: "activation_reply_lost"},
      actions: [
        %{
          target_id: "asset:orders",
          action: :rebuild,
          status: :activating,
          progress: %{completed: 1, total: 1},
          validation_result: %{status: "candidate_ready"},
          cleanup_state: :not_started
        }
      ]
    }
  end

  defp item do
    %{
      target_id: "asset:orders",
      window_key: "month:2026-07",
      status: :outcome_unknown,
      attempt_count: 1,
      row_count: nil
    }
  end
end
