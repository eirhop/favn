defmodule FavnOrchestrator.ManifestActivationDiagnosticsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestActivationDiagnostics
  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility

  test "returns bounded unresolved inspection diagnostics and retry guidance" do
    compatibilities =
      for index <- 1..25 do
        compatibility(
          "asset:#{String.pad_leading(to_string(index), 2, "0")}",
          :operator_decision,
          "physical_inspection_unavailable"
        )
      end

    diagnostics = ManifestActivationDiagnostics.from_compatibilities(compatibilities)

    assert diagnostics.unresolved_inspection_count == 25
    assert length(diagnostics.unresolved_inspections) == 20
    assert diagnostics.truncated?
    assert hd(diagnostics.unresolved_inspections).target_id == "asset:01"

    assert diagnostics.recovery == %{
             action: :repeat_manifest_activation,
             requires_new_idempotency_key: true,
             message:
               "Resolve the target decisions, then repeat manifest activation with a new idempotency key."
           }
  end

  test "includes every operator decision and ignores non-operator outcomes" do
    diagnostics =
      ManifestActivationDiagnostics.from_compatibilities([
        compatibility("asset:ready", :ready, "compatible"),
        compatibility("asset:rebuild", :rebuild_required, "incompatible_descriptor"),
        compatibility("asset:ownership", :operator_decision, "unmanaged_physical_relation"),
        compatibility("asset:generation", :operator_decision, "inconsistent_generation_state"),
        compatibility(
          "asset:fingerprint",
          :operator_decision,
          "active_physical_fingerprint_missing"
        )
      ])

    assert ManifestActivationDiagnostics.to_map(diagnostics) == %{
             unresolved_inspection_count: 3,
             unresolved_inspections: [
               %{
                 target_id: "asset:fingerprint",
                 reason_code: "active_physical_fingerprint_missing"
               },
               %{target_id: "asset:generation", reason_code: "inconsistent_generation_state"},
               %{target_id: "asset:ownership", reason_code: "unmanaged_physical_relation"}
             ],
             truncated: false,
             recovery: %{
               action: :repeat_manifest_activation,
               requires_new_idempotency_key: true,
               message:
                 "Resolve the target decisions, then repeat manifest activation with a new idempotency key."
             }
           }
  end

  test "round-trips the bounded durable receipt shape" do
    diagnostics =
      ManifestActivationDiagnostics.from_compatibilities([
        compatibility(
          "asset:unresolved",
          :operator_decision,
          "physical_inspection_unavailable"
        )
      ])

    encoded =
      diagnostics
      |> ManifestActivationDiagnostics.to_map()
      |> Jason.encode!()
      |> Jason.decode!()

    assert {:ok, ^diagnostics} = ManifestActivationDiagnostics.from_map(encoded)
  end

  defp compatibility(target_id, status, reason_code) do
    %DeploymentTargetCompatibility{
      target_id: target_id,
      desired_descriptor_hash: String.duplicate("a", 64),
      compatibility_status: status,
      reason_code: reason_code,
      compatibility_diff: %{},
      expected_binding_version: nil,
      expected_active_generation_id: nil,
      active_physical_fingerprint: nil
    }
  end
end
