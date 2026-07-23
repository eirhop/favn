defmodule FavnOrchestrator.Operator.RebuildsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Operator.Rebuilds
  alias FavnOrchestrator.Persistence.Results.RebuildAction
  alias FavnOrchestrator.Persistence.Results.RebuildLease
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Rebuild.Plan

  test "detail DTO excludes dispatcher fences and activation tokens" do
    operation = %RebuildOperation{
      operation_id: "rebuild-1",
      root_target_id: "asset:orders",
      state: :activation_unknown,
      phase: :reconciling,
      cleanup_state: :not_started,
      dispatcher: %RebuildLease{owner: "node-secret", fencing_token: 42},
      activation_token: "activation-secret",
      plan_payload: %{
        "binding_snapshot" => %{
          "asset:orders" => %{
            "active_data_plane_marker" => %{"activation_token" => "activation-secret"}
          }
        }
      },
      result_marker: %{
        "active_generation_id" => "generation-2",
        "activation_token" => "activation-secret"
      },
      terminal_error: %{
        "code" => "activation_unknown",
        "message" => "Activation outcome is unknown; token=activation-secret",
        "stacktrace" => "must-not-escape"
      },
      unknown_outcome: %{"reason_code" => "reply_lost", "token" => "activation-secret"},
      validation_result: %{"status" => "failed", "error" => "token=activation-secret"},
      actions: [
        %RebuildAction{
          target_id: "asset:orders",
          action: :rebuild,
          status: :outcome_unknown,
          activation_intent: %{
            "activation_token" => "activation-secret",
            "previous_generation_id" => "generation-1",
            "candidate_generation_id" => "generation-2"
          },
          pinned_input_generation_ids: [
            %{
              "target_id" => "asset:upstream",
              "data_plane_marker" => %{"activation_token" => "activation-secret"}
            }
          ],
          validation_result: %{"status" => "unknown", "token" => "activation-secret"},
          terminal_error: %{
            "message" => "Unknown outcome token=activation-secret",
            "debug" => "must-not-escape"
          }
        }
      ]
    }

    dto = Rebuilds.operation(operation, true)

    refute Map.has_key?(dto, :activation_token)
    refute Map.has_key?(dto, :dispatcher_owner)
    refute Map.has_key?(dto, :dispatcher_fencing_token)
    refute Map.has_key?(dto.result_marker, :activation_token)
    refute Map.has_key?(hd(dto.actions).activation_intent, :activation_token)
    refute Map.has_key?(dto.terminal_error, :stacktrace)
    refute Map.has_key?(hd(dto.actions).terminal_error, :debug)
    refute inspect(dto) =~ "activation-secret"

    assert get_in(dto.plan, [
             "binding_snapshot",
             "asset:orders",
             "active_data_plane_marker",
             "activation_token"
           ]) ==
             "[REDACTED]"

    assert dto.permissions.reconcile
  end

  test "plan DTO exposes server-derived start permission and redacts its payload" do
    plan =
      Plan.new("rebuild-plan-1", ~U[2026-07-23 00:00:00Z], %{
        binding_snapshot: %{
          "asset:orders" => %{
            active_data_plane_marker: %{activation_token: "activation-secret"}
          }
        }
      })

    operator_dto = Rebuilds.plan(plan, false)
    admin_dto = Rebuilds.plan(plan, true)

    refute operator_dto.permissions.start
    assert admin_dto.permissions.start
    refute inspect(operator_dto) =~ "activation-secret"
  end

  test "workspace admins and platform operators receive administrator permissions" do
    refute Rebuilds.admin?([:customer_operator])
    assert Rebuilds.admin?([:workspace_admin])
    assert Rebuilds.admin?([:platform_operator])
  end
end
