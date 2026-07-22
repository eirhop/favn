defmodule Favn.Contracts.ContractLockTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.{
    GenerationActivationRequest,
    GenerationActivationResult,
    GenerationDiscardRequest,
    GenerationDiscardResult,
    GenerationMarker,
    GenerationMarkerInitializationRequest,
    GenerationMarkerInitializationResult,
    GenerationReconciliationRequest,
    GenerationReconciliationResult,
    RelationInspectionRequest,
    RelationInspectionResult,
    RunnerAssetResult,
    RunnerEvent,
    RunnerResult,
    RunnerWork,
    TargetGenerationPin
  }

  test "runner work contract keys are locked" do
    assert_runner_keys(
      %RunnerWork{},
      [
        :asset_ref,
        :asset_refs,
        :asset_step_id,
        :attempt,
        :active_relation,
        :deadline_at,
        :execution_package,
        :execution_id,
        :manifest_content_hash,
        :manifest_lease_id,
        :manifest_version_id,
        :max_attempts,
        :metadata,
        :node_identity,
        :params,
        :pipeline,
        :planned_asset_refs,
        :rebuild_action_id,
        :rebuild_item_id,
        :rebuild_operation_id,
        :required_runner_release_id,
        :run_id,
        :run_started_at,
        :runtime_input_pin,
        :stage,
        :logical_target_id,
        :target_descriptor_hash,
        :target_generation_id,
        :target_operation,
        :trigger,
        :upstream_generation_pins,
        :write_relation
      ]
    )
  end

  test "runner result contract keys are locked" do
    assert_runner_keys(
      %RunnerResult{},
      [
        :asset_results,
        :error,
        :manifest_content_hash,
        :manifest_version_id,
        :metadata,
        :required_runner_release_id,
        :resource_outcomes,
        :run_id,
        :status
      ]
    )
  end

  test "runner asset result generation keys are locked" do
    assert_runner_keys(
      %RunnerAssetResult{},
      [
        :asset_step_id,
        :attempt_count,
        :attempts,
        :duration_ms,
        :error,
        :finished_at,
        :logical_target_id,
        :max_attempts,
        :meta,
        :ref,
        :started_at,
        :status,
        :target_generation_id,
        :target_operation,
        :write_outcome,
        :write_relation
      ]
    )
  end

  test "generation operation contract keys are locked" do
    assert_runner_keys(
      struct(TargetGenerationPin),
      [:asset_ref, :descriptor_hash, :relation, :target_generation_id, :target_id]
    )

    assert_runner_keys(
      struct(GenerationMarker),
      [
        :activated_at,
        :activation_operation_id,
        :activation_token,
        :active_generation_id,
        :active_relation,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationMarkerInitializationRequest),
      [
        :active_relation,
        :expected_physical_fingerprint,
        :initialization_operation_id,
        :initialization_token,
        :manifest_content_hash,
        :manifest_version_id,
        :required_runner_release_id,
        :target_generation_id,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationMarkerInitializationResult),
      [
        :completed_at,
        :error,
        :initialization_token,
        :observed_marker,
        :outcome,
        :physical_fingerprint,
        :required_runner_release_id,
        :target_generation_id,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationActivationRequest),
      [
        :activation_token,
        :active_relation,
        :candidate_generation_id,
        :expected_candidate_fingerprint,
        :candidate_relation,
        :expected_marker,
        :manifest_content_hash,
        :manifest_version_id,
        :previous_generation_id,
        :rebuild_action_id,
        :rebuild_operation_id,
        :required_runner_release_id,
        :retired_relation,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationActivationResult),
      [
        :activation_token,
        :candidate_fingerprint,
        :candidate_generation_id,
        :completed_at,
        :error,
        :observed_marker,
        :outcome,
        :physical_fingerprint,
        :required_runner_release_id,
        :retired_relation,
        :target_id
      ]
    )

    assert_runner_keys(struct(GenerationReconciliationRequest), [:activation])

    assert_runner_keys(
      struct(GenerationReconciliationResult),
      [
        :activation_token,
        :candidate_generation_id,
        :candidate_present,
        :disposition,
        :error,
        :observed_marker,
        :physical_fingerprint,
        :reconciled_at,
        :required_runner_release_id,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationDiscardRequest),
      [
        :active_relation,
        :candidate_generation_id,
        :candidate_relation,
        :discard_token,
        :manifest_content_hash,
        :manifest_version_id,
        :rebuild_action_id,
        :rebuild_operation_id,
        :required_runner_release_id,
        :target_id
      ]
    )

    assert_runner_keys(
      struct(GenerationDiscardResult),
      [
        :candidate_generation_id,
        :candidate_present,
        :completed_at,
        :discard_token,
        :error,
        :observed_marker,
        :outcome,
        :required_runner_release_id,
        :target_id
      ]
    )
  end

  test "runner event contract keys are locked" do
    assert_runner_keys(
      %RunnerEvent{},
      [
        :event_type,
        :manifest_content_hash,
        :manifest_version_id,
        :occurred_at,
        :payload,
        :required_runner_release_id,
        :run_id
      ]
    )
  end

  test "relation inspection request contract keys are locked" do
    assert_runner_keys(
      %RelationInspectionRequest{},
      [
        :asset_ref,
        :include,
        :manifest_content_hash,
        :manifest_version_id,
        :relation,
        :required_runner_release_id,
        :sample_limit
      ]
    )
  end

  test "relation inspection result contract keys are locked" do
    assert_runner_keys(
      %RelationInspectionResult{},
      [
        :adapter,
        :asset_ref,
        :columns,
        :error,
        :inspected_at,
        :relation,
        :relation_ref,
        :required_runner_release_id,
        :row_count,
        :sample,
        :table_metadata,
        :warnings
      ]
    )
  end

  test "all runner contracts validate one canonical release binding" do
    release_id = FavnTestSupport.runner_release_id()

    for contract <- [
          %RunnerWork{required_runner_release_id: release_id},
          %RunnerResult{required_runner_release_id: release_id},
          %RunnerEvent{required_runner_release_id: release_id},
          %RelationInspectionRequest{required_runner_release_id: release_id},
          %RelationInspectionResult{required_runner_release_id: release_id}
        ] do
      assert :ok = apply(contract.__struct__, :validate_release_binding, [contract])
    end
  end

  test "runner contracts reject missing and malformed release bindings" do
    assert {:error, {:invalid_required_runner_release_id, nil}} =
             RunnerWork.validate_release_binding(%RunnerWork{})

    assert {:error, {:invalid_required_runner_release_id, "rr_INVALID"}} =
             RunnerResult.validate_release_binding(%RunnerResult{
               required_runner_release_id: "rr_INVALID"
             })
  end

  defp assert_runner_keys(struct, expected_keys) when is_list(expected_keys) do
    keys =
      struct
      |> Map.from_struct()
      |> Map.keys()
      |> Enum.sort()

    assert keys == Enum.sort(expected_keys)
  end
end
