defmodule Favn.Contracts.GenerationContractsTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerWork
  alias Favn.Contracts.TargetGenerationPin
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.SQL.Template

  @previous_generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
  @candidate_generation_id "018f47a0-7b0d-4b1a-9d8b-e18a9a987655"
  @input_generation_id "018f47a0-7b0d-4b1a-ad8b-e18a9a987656"
  @manifest_hash String.duplicate("a", 64)
  @fingerprint String.duplicate("b", 64)

  test "normal and candidate runner work bind exact generation relations" do
    normal = runner_work(:normal_materialization)
    assert :ok = RunnerWork.validate_generation_contract(normal)

    assert :ok =
             RunnerAssetResult.validate_generation_result(
               runner_result(normal, :ok, :succeeded),
               normal
             )

    candidate = runner_work(:rebuild_candidate)
    assert :ok = RunnerWork.validate_generation_contract(candidate)

    assert {:error, :normal_materialization_relation_mismatch} =
             normal
             |> Map.put(:write_relation, candidate.write_relation)
             |> RunnerWork.validate_generation_contract()

    assert {:error, {:runner_result_identity_mismatch, :target_generation_id, _, _}} =
             candidate
             |> runner_result(:ok, :succeeded)
             |> Map.put(:target_generation_id, @previous_generation_id)
             |> RunnerAssetResult.validate_generation_result(candidate)
  end

  test "runner work and input pins match manifest descriptors and packages" do
    {descriptor, package} = descriptor_and_package()

    work = %{
      runner_work(:normal_materialization)
      | execution_package: package,
        target_descriptor_hash: descriptor.descriptor_hash
    }

    assert :ok = RunnerWork.validate_target_identity(work, descriptor)

    changed = %{work | logical_target_id: "asset:other"}

    assert {:error, {:target_identity_mismatch, :logical_target_id, _, _}} =
             RunnerWork.validate_target_identity(changed, descriptor)

    pin = %TargetGenerationPin{
      asset_ref: {MyApp.Target, :asset},
      target_id: descriptor.target_id,
      target_generation_id: @input_generation_id,
      relation: active_relation(),
      descriptor_hash: descriptor.descriptor_hash
    }

    assert :ok = TargetGenerationPin.validate_target_identity(pin, descriptor)

    assert {:error, {:target_generation_pin_mismatch, :descriptor_hash, _, _}} =
             pin
             |> Map.put(:descriptor_hash, String.duplicate("f", 64))
             |> TargetGenerationPin.validate_target_identity(descriptor)
  end

  test "non-persisted output work may still pin persisted SQL inputs" do
    {descriptor, _package} = descriptor_and_package()

    pin = %TargetGenerationPin{
      asset_ref: {MyApp.Target, :asset},
      target_id: descriptor.target_id,
      target_generation_id: @input_generation_id,
      relation: active_relation(),
      descriptor_hash: descriptor.descriptor_hash
    }

    assert :ok =
             RunnerWork.validate_generation_contract(%RunnerWork{
               target_operation: nil,
               upstream_generation_pins: [pin]
             })
  end

  test "activation success requires the candidate marker and physical fingerprint" do
    request = activation_request()
    assert :ok = GenerationActivationRequest.validate(request)

    result = %GenerationActivationResult{
      required_runner_release_id: release_id(),
      target_id: request.target_id,
      candidate_generation_id: request.candidate_generation_id,
      activation_token: request.activation_token,
      outcome: :succeeded,
      observed_marker: candidate_marker(request),
      candidate_fingerprint: @fingerprint,
      physical_fingerprint: @fingerprint,
      retired_relation: request.retired_relation,
      completed_at: now()
    }

    assert :ok = GenerationActivationResult.validate(result, request)

    wrong_marker = %{result.observed_marker | active_generation_id: @previous_generation_id}

    assert {:error, {:generation_activation_result_mismatch, :marker_generation_id, _, _}} =
             GenerationActivationResult.validate(
               %{result | observed_marker: wrong_marker},
               request
             )

    assert {:error, {:generation_activation_result_mismatch, :candidate_fingerprint, _, _}} =
             GenerationActivationResult.validate(
               %{result | candidate_fingerprint: String.duplicate("d", 64)},
               request
             )
  end

  test "initial generation marker results preserve exact materialization identity" do
    request = %GenerationMarkerInitializationRequest{
      manifest_version_id: "mv_generation",
      manifest_content_hash: @manifest_hash,
      required_runner_release_id: release_id(),
      target_id: "asset:Elixir.MyApp.Target:asset",
      target_generation_id: @previous_generation_id,
      active_relation: active_relation(),
      expected_physical_fingerprint: @fingerprint,
      initialization_operation_id: "initial-materialization-1",
      initialization_token: "initial-marker-token-1"
    }

    marker = %GenerationMarker{
      target_id: request.target_id,
      active_relation: request.active_relation,
      active_generation_id: request.target_generation_id,
      activation_operation_id: request.initialization_operation_id,
      activation_token: request.initialization_token,
      activated_at: now()
    }

    result = %GenerationMarkerInitializationResult{
      required_runner_release_id: release_id(),
      target_id: request.target_id,
      target_generation_id: request.target_generation_id,
      initialization_token: request.initialization_token,
      outcome: :succeeded,
      observed_marker: marker,
      physical_fingerprint: @fingerprint,
      completed_at: now()
    }

    assert :ok = GenerationMarkerInitializationResult.validate(result, request)
  end

  test "reconciliation distinguishes candidate, previous, and unknown states" do
    activation = activation_request()
    request = %GenerationReconciliationRequest{activation: activation}

    candidate = %GenerationReconciliationResult{
      required_runner_release_id: release_id(),
      target_id: activation.target_id,
      candidate_generation_id: activation.candidate_generation_id,
      activation_token: activation.activation_token,
      disposition: :candidate_active,
      observed_marker: candidate_marker(activation),
      candidate_present: false,
      physical_fingerprint: @fingerprint,
      reconciled_at: now()
    }

    previous = %{
      candidate
      | disposition: :previous_active,
        observed_marker: previous_marker(activation),
        candidate_present: true,
        physical_fingerprint: nil
    }

    unknown = %{
      candidate
      | disposition: :unknown,
        observed_marker: nil,
        candidate_present: nil,
        physical_fingerprint: nil,
        error: RunnerError.new(outcome: :unknown)
    }

    assert :ok = GenerationReconciliationResult.validate(candidate, request)
    assert :ok = GenerationReconciliationResult.validate(previous, request)
    assert :ok = GenerationReconciliationResult.validate(unknown, request)

    mismatched_markers = [
      %{previous.observed_marker | activation_operation_id: "previous-rebuild-stale"},
      %{previous.observed_marker | activation_token: "previous-token-stale"}
    ]

    for mismatched_marker <- mismatched_markers do
      assert {:error, {:generation_reconciliation_result_mismatch, _, _, _}} =
               GenerationReconciliationResult.validate(
                 %{previous | observed_marker: mismatched_marker},
                 request
               )
    end
  end

  test "discard is idempotent and never accepts a safe failure with the candidate active" do
    request = discard_request()

    absent = %GenerationDiscardResult{
      required_runner_release_id: release_id(),
      target_id: request.target_id,
      candidate_generation_id: request.candidate_generation_id,
      discard_token: request.discard_token,
      outcome: :already_absent,
      candidate_present: false,
      completed_at: now()
    }

    assert :ok = GenerationDiscardResult.validate(absent, request)

    active = %{
      absent
      | outcome: :safe_failure,
        observed_marker: candidate_marker(activation_request()),
        candidate_present: true,
        error: RunnerError.new(outcome: :safe_failure)
    }

    assert {:error, :cannot_discard_active_candidate_generation} =
             GenerationDiscardResult.validate(active, request)

    unknown = %{
      active
      | outcome: :outcome_unknown,
        candidate_present: nil,
        error: RunnerError.new(outcome: :unknown)
    }

    assert :ok = GenerationDiscardResult.validate(unknown, request)

    assert :ok = GenerationDiscardRequest.validate(%{request | relation_kind: :retired})

    assert {:error, {:invalid_discard_relation_kind, :temporary}} =
             GenerationDiscardRequest.validate(%{request | relation_kind: :temporary})
  end

  defp runner_work(operation) do
    active = active_relation()

    %RunnerWork{
      manifest_version_id: "mv_generation",
      manifest_content_hash: @manifest_hash,
      required_runner_release_id: release_id(),
      asset_ref: {MyApp.Target, :asset},
      target_operation: operation,
      logical_target_id: "asset:Elixir.MyApp.Target:asset",
      target_descriptor_hash: String.duplicate("c", 64),
      target_generation_id: @candidate_generation_id,
      active_relation: active,
      write_relation:
        if(operation == :rebuild_candidate,
          do: relation("target__favn_candidate"),
          else: active
        ),
      upstream_generation_pins: [],
      rebuild_operation_id: if(operation == :rebuild_candidate, do: "rebuild_1"),
      rebuild_action_id: if(operation == :rebuild_candidate, do: "action_1"),
      rebuild_item_id: if(operation == :rebuild_candidate, do: "item_1")
    }
  end

  defp runner_result(work, status, outcome) do
    %RunnerAssetResult{
      ref: work.asset_ref,
      status: status,
      target_operation: work.target_operation,
      logical_target_id: work.logical_target_id,
      target_generation_id: work.target_generation_id,
      write_relation: work.write_relation,
      write_outcome: outcome
    }
  end

  defp descriptor_and_package do
    ref = {MyApp.Target, :asset}
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/generation_contracts_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})

    descriptor =
      TargetDescriptor.from_asset(
        %{
          ref: ref,
          type: :sql,
          relation: active_relation(),
          materialization: :table,
          execution_package_hash: package.content_hash,
          assurance: nil,
          window: nil,
          coverage: nil
        },
        connection_definitions: %{
          warehouse: %{adapter: MyApp.Adapter, module: MyApp.Warehouse}
        },
        manifest_schema_version: 11,
        runner_contract_version: 11
      )

    {descriptor, package}
  end

  defp activation_request do
    %GenerationActivationRequest{
      manifest_version_id: "mv_generation",
      manifest_content_hash: @manifest_hash,
      required_runner_release_id: release_id(),
      rebuild_operation_id: "rebuild_1",
      rebuild_action_id: "action_1",
      target_id: "asset:Elixir.MyApp.Target:asset",
      previous_generation_id: @previous_generation_id,
      candidate_generation_id: @candidate_generation_id,
      active_relation: active_relation(),
      candidate_relation: relation("target__favn_candidate"),
      retired_relation: relation("target__favn_retired"),
      expected_candidate_fingerprint: @fingerprint,
      activation_token: "activation_1",
      expected_marker: previous_marker_fields()
    }
  end

  defp discard_request do
    %GenerationDiscardRequest{
      manifest_version_id: "mv_generation",
      manifest_content_hash: @manifest_hash,
      required_runner_release_id: release_id(),
      rebuild_operation_id: "rebuild_1",
      rebuild_action_id: "action_1",
      target_id: "asset:Elixir.MyApp.Target:asset",
      candidate_generation_id: @candidate_generation_id,
      active_relation: active_relation(),
      candidate_relation: relation("target__favn_candidate"),
      discard_token: "discard_1"
    }
  end

  defp previous_marker_fields do
    %GenerationMarker{
      target_id: "asset:Elixir.MyApp.Target:asset",
      active_relation: active_relation(),
      active_generation_id: @previous_generation_id,
      activation_operation_id: "previous_rebuild",
      activation_token: "previous_token",
      activated_at: now()
    }
  end

  defp previous_marker(_request), do: previous_marker_fields()

  defp candidate_marker(request) do
    %GenerationMarker{
      target_id: request.target_id,
      active_relation: request.active_relation,
      active_generation_id: request.candidate_generation_id,
      activation_operation_id: request.rebuild_operation_id,
      activation_token: request.activation_token,
      activated_at: now()
    }
  end

  defp active_relation, do: relation("target")

  defp relation(name),
    do: RelationRef.new!(connection: :warehouse, schema: "mart", name: name)

  defp release_id, do: FavnTestSupport.runner_release_id()
  defp now, do: ~U[2026-07-22 10:00:00Z]
end
