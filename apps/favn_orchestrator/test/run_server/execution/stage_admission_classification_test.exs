defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionClassificationTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunServer.Execution.StageAdmission

  @asset_ref {__MODULE__.Asset, :orders}

  describe "node-specific claim failures" do
    test "a claim-store conflict fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :materialization_claim,
               Error.new(:conflict, "target operation is in progress",
                 details: %{reason_code: "target_operation_in_progress"}
               )
             )
    end

    test "a mismatched target generation pin fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :materialization_claim,
               {:target_generation_pin_mismatch, @asset_ref}
             )
    end

    test "an unpinned target generation fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :materialization_claim,
               {:target_generation_not_pinned, @asset_ref}
             )
    end

    test "an unexpected materialization decision fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :materialization_claim,
               {:unexpected_materialization_decision, :expired}
             )
    end

    test "an asset missing from the manifest index fails only its node" do
      assert StageAdmission.node_specific_failure?(:materialization_claim, :asset_not_found)
      assert StageAdmission.node_specific_failure?(:execution_package, :asset_not_found)
    end
  end

  describe "node-specific execution-package failures" do
    for reason <- [
          :execution_package_required,
          :execution_package_deployment_required,
          :execution_package_materialization_mismatch,
          :invalid_execution_package
        ] do
      test "#{inspect(reason)} fails only its node" do
        assert StageAdmission.node_specific_failure?(:execution_package, unquote(reason))
      end
    end

    test "a package that should not exist fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:execution_package_not_required, @asset_ref}
             )
    end

    test "mismatched package relation inputs fail only their node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:execution_package_relation_inputs_mismatch, @asset_ref}
             )
    end

    test "an invalid package hash fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:invalid_execution_package_hash, "not-a-hash"}
             )
    end

    test "a package hash mismatch fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:execution_package_hash_mismatch, "expected", "computed"}
             )
    end

    test "a package built for another asset fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:execution_package_asset_mismatch, @asset_ref, {__MODULE__.Asset, :other}}
             )
    end

    test "an unsupported package schema fails only its node" do
      assert StageAdmission.node_specific_failure?(
               :execution_package,
               {:unsupported_execution_package_schema, 99, 1}
             )
    end
  end

  describe "run-wide failures" do
    test "missing run authority for materialization stops the stage" do
      refute StageAdmission.node_specific_failure?(
               :materialization_claim,
               :materialization_run_authority_required
             )
    end

    for kind <- [:unavailable, :internal, :forbidden] do
      test "a #{kind} persistence error stops the stage at either call site" do
        error = Error.new(unquote(kind), "store failure")

        refute StageAdmission.node_specific_failure?(:materialization_claim, error)
        refute StageAdmission.node_specific_failure?(:execution_package, error)
      end
    end

    test "an attempt-start persistence failure stops the stage" do
      refute StageAdmission.node_specific_failure?(
               :attempt_start,
               Error.new(:conflict, "run transition rejected")
             )

      refute StageAdmission.node_specific_failure?(:attempt_start, :asset_not_found)
    end

    test "external cancellation stops the stage" do
      refute StageAdmission.node_specific_failure?(:materialization_claim, :external_cancel)
      refute StageAdmission.node_specific_failure?(:execution_package, :external_cancel)
    end

    test "a run that is no longer admissible stops the stage" do
      refute StageAdmission.node_specific_failure?(
               :materialization_claim,
               {:run_not_admissible, "run-1", :cancelled}
             )
    end

    test "an unrecognized term stops the stage" do
      refute StageAdmission.node_specific_failure?(:materialization_claim, :something_new)
      refute StageAdmission.node_specific_failure?(:execution_package, {:something_new, 1, 2})
    end

    test "a conflict outside the claim call site stops the stage" do
      refute StageAdmission.node_specific_failure?(
               :execution_package,
               Error.new(:conflict, "package registry conflict")
             )
    end
  end
end

defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionClassificationTest.Asset do
end
