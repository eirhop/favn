defmodule Favn.Contracts.RunnerTask.PersistenceSchema do
  @moduledoc false

  alias Favn.Contracts
  alias Favn.Contracts.RunnerWork
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Plan.NodeIdentity

  @work_ids ~w(run_id asset_step_id manifest_lease_id logical_target_id
    target_descriptor_hash target_generation_id rebuild_operation_id rebuild_action_id rebuild_item_id)a

  def payload(:asset_attempt, %RunnerWork{} = work) do
    with true <- identity?(work.manifest_version_id) and hash?(work.manifest_content_hash),
         :ok <- RunnerWork.validate_release_binding(work),
         true <- is_atom(work.runner_pool),
         true <-
           Enum.all?(
             @work_ids,
             &(is_nil(Map.fetch!(work, &1)) or identity?(Map.fetch!(work, &1)))
           ),
         true <-
           positive?(work.attempt) and positive?(work.max_attempts) and
             work.attempt <= work.max_attempts,
         true <- is_integer(work.stage) and work.stage >= 0,
         true <- is_map(work.params) and is_map(work.metadata) and is_map(work.trigger),
         true <- is_boolean(work.rebuild_empty_generation) and is_boolean(work.rebuild_final_item),
         true <- optional_datetime?(work.run_started_at) and optional_datetime?(work.deadline_at),
         true <- ref?(RunnerWork.asset_ref(work)),
         true <- refs?(work.asset_refs) and refs?(work.planned_asset_refs),
         :ok <- node_identity(work.node_identity),
         :ok <- package(work.execution_package),
         :ok <- RunnerWork.validate_generation_contract(work) do
      :ok
    else
      _invalid -> {:error, :invalid_runner_work_fields}
    end
  end

  def payload(:relation_inspection, %Contracts.RelationInspectionRequest{} = request) do
    with true <- identity?(request.manifest_version_id) and hash?(request.manifest_content_hash),
         :ok <- Contracts.RelationInspectionRequest.validate_release_binding(request),
         true <- is_nil(request.asset_ref) or ref?(request.asset_ref),
         true <- is_nil(request.relation) or is_struct(request.relation, Favn.RelationRef),
         true <-
           is_list(request.include) and
             Enum.all?(
               request.include,
               &(&1 in [:relation, :columns, :row_count, :sample, :table_metadata])
             ),
         true <- is_integer(request.sample_limit) and request.sample_limit >= 0 do
      :ok
    else
      _invalid -> {:error, :invalid_relation_inspection_fields}
    end
  end

  def payload(:generation_capabilities, request),
    do: Contracts.GenerationCapabilitiesRequest.validate(request)

  def payload(:generation_marker_read, request),
    do: Contracts.GenerationMarkerReadRequest.validate(request)

  def payload(:generation_marker_initialize, request),
    do: Contracts.GenerationMarkerInitializationRequest.validate(request)

  def payload(:generation_activate, request),
    do: Contracts.GenerationActivationRequest.validate(request)

  def payload(:generation_reconcile, request),
    do: Contracts.GenerationReconciliationRequest.validate(request)

  def payload(:generation_discard, request),
    do: Contracts.GenerationDiscardRequest.validate(request)

  def payload(_kind, _request), do: {:error, :invalid_runner_task_fields}

  def result(:asset_attempt, outcome, %RunnerResult{} = result) do
    expected =
      case outcome do
        :succeeded -> :ok
        :cancelled -> :cancelled
        _failure -> :error
      end

    with true <- identity?(result.manifest_version_id) and hash?(result.manifest_content_hash),
         :ok <- RunnerResult.validate_release_binding(result),
         true <-
           result.status == expected or (outcome != :succeeded and result.status == :timed_out),
         true <-
           is_list(result.asset_results) and
             Enum.all?(result.asset_results, &is_struct(&1, Contracts.RunnerAssetResult)),
         true <- is_list(result.resource_outcomes) and is_map(result.metadata),
         true <-
           outcome != :succeeded or
             (is_nil(result.error) and
                Enum.all?(result.asset_results, &(&1.status == :ok and is_nil(&1.error)))) do
      :ok
    else
      _invalid -> {:error, :invalid_runner_result_fields}
    end
  end

  def result(kind, :succeeded, %{outcome: outcome})
      when kind in [:generation_marker_initialize, :generation_activate] and outcome != :succeeded,
      do: {:error, :inconsistent_runner_task_outcome}

  def result(:generation_discard, :succeeded, %{outcome: outcome})
      when outcome not in [:discarded, :already_absent],
      do: {:error, :inconsistent_runner_task_outcome}

  def result(:generation_reconcile, :succeeded, %{disposition: disposition})
      when disposition not in [:candidate_active, :previous_active],
      do: {:error, :inconsistent_runner_task_outcome}

  def result(_kind, _outcome, _value), do: :ok

  def safe_resolution?(nil), do: true

  def safe_resolution?(%RunnerResult{} = result) do
    safe_error?(result.error) and
      Enum.all?(result.asset_results, fn asset ->
        asset.write_outcome in [nil, :safe_failure] and safe_error?(asset.error)
      end)
  end

  def safe_resolution?(%{outcome: outcome} = result),
    do: outcome == :safe_failure and safe_error?(Map.get(result, :error))

  def safe_resolution?(_result), do: false
  defp safe_error?(nil), do: true
  defp safe_error?(%Contracts.RunnerError{outcome: :safe_failure}), do: true
  defp safe_error?(_error), do: false

  # Resolving a write requires a result for the exact dispatched request.
  def completion(_kind, _request, nil, outcome) when outcome != :succeeded, do: :ok

  def completion(:generation_marker_initialize, request, result, _outcome),
    do: Contracts.GenerationMarkerInitializationResult.validate(result, request)

  def completion(:generation_activate, request, result, _outcome),
    do: Contracts.GenerationActivationResult.validate(result, request)

  def completion(:generation_discard, request, result, _outcome),
    do: Contracts.GenerationDiscardResult.validate(result, request)

  def completion(:generation_reconcile, request, result, _outcome),
    do: Contracts.GenerationReconciliationResult.validate(result, request)

  def completion(:asset_attempt, work, %RunnerResult{} = result, outcome) do
    with true <-
           Enum.all?(
             [:run_id, :manifest_version_id, :manifest_content_hash, :required_runner_release_id],
             &(Map.fetch!(work, &1) == Map.fetch!(result, &1))
           ),
         true <-
           outcome != :succeeded or
             length(result.asset_results) ==
               if(RunnerWork.runtime_input_resolution_only?(work), do: 0, else: 1),
         true <-
           Enum.all?(result.asset_results, fn asset ->
             asset.ref == RunnerWork.asset_ref(work) and asset.asset_step_id == work.asset_step_id and
               asset.attempt_count == work.attempt and
               Contracts.RunnerAssetResult.validate_generation_result(asset, work) == :ok
           end) do
      :ok
    else
      _invalid -> {:error, :runner_task_result_identity_mismatch}
    end
  end

  def completion(:relation_inspection, request, result, _outcome) do
    if result.required_runner_release_id == request.required_runner_release_id and
         result.asset_ref == request.asset_ref and
         (is_nil(request.relation) or result.relation_ref == request.relation),
       do: :ok,
       else: {:error, :runner_task_result_identity_mismatch}
  end

  def completion(:generation_marker_read, request, result, _outcome) do
    if is_nil(result.marker) or
         result.marker.target_id == Favn.TargetIdentity.for_asset(request.asset_ref),
       do: :ok,
       else: {:error, :runner_task_result_identity_mismatch}
  end

  def completion(:generation_capabilities, _request, _result, _outcome), do: :ok
  def completion(_kind, _request, _result, _outcome), do: {:error, :invalid_runner_task_result}

  defp node_identity(nil), do: :ok

  defp node_identity(%NodeIdentity{} = node) do
    case NodeIdentity.new(Map.from_struct(node)) do
      {:ok, ^node} -> :ok
      _invalid -> {:error, :invalid_node_identity}
    end
  end

  defp node_identity(_node), do: {:error, :invalid_node_identity}
  defp package(nil), do: :ok

  defp package(%ExecutionPackage{} = package) do
    case ExecutionPackage.verify(package) do
      {:ok, _verified} -> :ok
      _invalid -> {:error, :invalid_execution_package}
    end
  end

  defp package(_package), do: {:error, :invalid_execution_package}
  defp identity?(value), do: is_binary(value) and byte_size(value) in 1..255
  defp hash?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp positive?(value), do: is_integer(value) and value > 0

  defp ref?({module, name}),
    do: is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name)

  defp ref?(_value), do: false
  defp refs?(values), do: is_list(values) and Enum.all?(values, &ref?/1)
  defp optional_datetime?(value), do: is_nil(value) or is_struct(value, DateTime)
end
