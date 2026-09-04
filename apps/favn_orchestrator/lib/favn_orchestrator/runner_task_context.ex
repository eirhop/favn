defmodule FavnOrchestrator.RunnerTaskContext do
  @moduledoc """
  Current task-local settlement continuation. Executable task data belongs to
  Core; claim and permit restoration stays with the orchestrator.
  """

  alias Favn.Contracts.RunnerTask.PersistenceData
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitPermit
  alias FavnOrchestrator.Persistence.Results.TargetOperationLock
  alias FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation

  @limit 4 * 1_048_576
  @atoms ~w(runtime_input_resolution rebuild_operation_id rebuild_action_id rebuild_item_id purpose ownership_only released kind sequential pipeline decision freshness_checkpoint freshness_key
    materialization_claim resource_circuit_permits claim_key fencing_token
    workspace_id deployment_id run_id asset_step_id node_key asset_ref_module
    asset_ref_name input_fingerprint input_versions input_generations
    manifest_version_id manifest_content_hash execution_package_hash runtime_input_lineage
    logical_window_range empty_generation target_operation target_operation_lock
    target_generation_id evidence_generation_id owner_id status claimed_at heartbeat_at
    expires_at claimed succeeded failed completed_at result error attempt payload_hash
    revision sequence stage version resource probe? target_id operation_id operation_type
    lease_owner lease_expires_at inserted_at updated_at materialization rebuild recovery
    stale fresh missing forced reason reasons upstream_versions freshness_version
    consumed_version ref policy refresh forced? materialize? skip? decision_at
    window_start window_end start end from to connection storage source
    target_kind partition_key upstream_ref upstream_node_key success_run_id
    current_version current_success_run_id stale_reasons run skipped_fresh blocked
    upstream_incomplete upstream_blocked upstream_refreshed no_freshness_policy
    freshness_expired window_success calendar_period max_age existing_success
    missing_consumed_version missing_upstream_version upstream_version_changed blocking_upstream)a
  @claim_keys ~w(purpose claim_key workspace_id deployment_id run_id asset_step_id node_key
    asset_ref_module asset_ref_name freshness_key input_fingerprint input_versions
    input_generations manifest_version_id manifest_content_hash execution_package_hash
    runtime_input_lineage logical_window_range empty_generation target_operation
    target_operation_lock target_generation_id evidence_generation_id owner_id status
    claimed_at heartbeat_at expires_at fencing_token version target_kind target_id
    partition_key completed_at result error)a

  @spec encode(map()) :: {:ok, map()} | {:error, term()}
  def encode(context) do
    if valid?(context) do
      context |> lower() |> PersistenceData.encode(@limit)
    else
      {:error, :invalid_runner_task_orchestration_context}
    end
  rescue
    _invalid -> {:error, :invalid_runner_task_orchestration_context}
  end

  @spec decode(map(), Version.t()) :: {:ok, map()} | {:error, atom()}
  def decode(envelope, version, packages \\ []) do
    with {:ok, context} <- PersistenceData.decode(envelope, @limit, version, @atoms, packages),
         true <- valid?(context) do
      {:ok, restore(context)}
    else
      _invalid -> {:error, :invalid_runner_task_orchestration_context}
    end
  rescue
    _invalid -> {:error, :invalid_runner_task_orchestration_context}
  end

  defp valid?(context) when context == %{}, do: true

  defp valid?(%{kind: :sequential, materialization_claim: claim} = context),
    do: map_size(context) == 2 and valid_claim?(claim)

  defp valid?(%{kind: :pipeline} = context) do
    PipelineTaskContinuation.valid?(context) and valid_claim?(context.materialization_claim) and
      Enum.all?(context.resource_circuit_permits, &valid_permit?/1) and
      is_binary(context.freshness_key) and valid_decision?(context.decision)
  end

  defp valid?(%{purpose: :runtime_input_resolution} = context) do
    Enum.sort(Map.keys(context)) == [
      :purpose,
      :rebuild_action_id,
      :rebuild_item_id,
      :rebuild_operation_id
    ] and
      Enum.all?(
        [:rebuild_operation_id, :rebuild_action_id, :rebuild_item_id],
        &identifier?(context[&1])
      )
  end

  defp valid?(_context), do: false

  defp valid_claim?(nil), do: true

  defp valid_claim?(claim) when is_map(claim) and not is_struct(claim) do
    Map.keys(claim) -- @claim_keys == [] and
      Enum.all?(
        [:claim_key, :workspace_id, :deployment_id, :run_id, :owner_id],
        &identifier?(Map.get(claim, &1))
      ) and
      is_integer(claim[:fencing_token]) and claim.fencing_token > 0 and
      is_integer(claim[:version]) and claim.version > 0 and
      claim[:status] in [:claimed, :succeeded, :failed] and
      is_struct(claim[:expires_at], DateTime) and valid_lock?(claim[:target_operation_lock])
  end

  defp valid_claim?(_claim), do: false

  defp valid_lock?(nil), do: true
  defp valid_lock?(%TargetOperationLock{} = lock), do: valid_lock?(Map.from_struct(lock))

  defp valid_lock?(lock) when is_map(lock) do
    Enum.sort(Map.keys(lock)) == Enum.sort(Map.keys(Map.from_struct(%TargetOperationLock{}))) and
      Enum.all?([:workspace_id, :target_id, :operation_id, :lease_owner], &identifier?(lock[&1])) and
      lock[:operation_type] in [:rebuild, :materialization, :target_recovery] and
      is_integer(lock[:fencing_token]) and lock.fencing_token > 0 and
      is_integer(lock[:version]) and lock.version > 0 and
      is_struct(lock[:lease_expires_at], DateTime)
  end

  defp valid_lock?(_lock), do: false

  defp valid_permit?(%ResourceCircuitPermit{} = permit),
    do: valid_permit?(Map.from_struct(permit))

  defp valid_permit?(permit) when is_map(permit) do
    Enum.sort(Map.keys(permit)) == [:owner_id, :probe?, :resource] and
      identifier?(permit.owner_id) and is_boolean(permit.probe?) and
      Favn.Resource.Ref.from_value(permit.resource) == {:ok, permit.resource}
  end

  defp valid_permit?(_permit), do: false

  defp valid_decision?(
         %{
           decision: :run,
           reason: reason,
           node_key: {{module, name}, _window},
           freshness_key: key
         } = decision
       ) do
    Map.keys(decision) -- [:decision, :reason, :node_key, :freshness_key, :stale_reasons] == [] and
      is_atom(reason) and is_atom(module) and is_atom(name) and is_binary(key) and
      is_list(Map.get(decision, :stale_reasons, []))
  end

  defp valid_decision?(_decision), do: false
  defp identifier?(value), do: is_binary(value) and byte_size(value) in 1..255

  @doc false
  def matches_task?(%{} = context, command) when map_size(context) == 0,
    do: command.task_kind != :asset_attempt

  def matches_task?(%{materialization_claim: nil}, command), do: is_nil(command.write_claim_key)

  def matches_task?(%{materialization_claim: claim}, command) do
    claim.workspace_id == command.workspace_context.workspace_id and
      claim.run_id == command.run_id and
      claim.claim_key == command.write_claim_key and
      claim.fencing_token == command.write_claim_fence
  end

  def matches_task?(%{purpose: :runtime_input_resolution}, command),
    do:
      command.task_kind == :asset_attempt and is_nil(command.write_claim_key) and
        is_nil(command.write_target_id)

  def matches_task?(_context, _command), do: false

  defp lower(%{kind: :pipeline} = context) do
    context
    |> Map.update!(
      :resource_circuit_permits,
      &Enum.map(&1, fn %ResourceCircuitPermit{} = permit -> Map.from_struct(permit) end)
    )
    |> Map.update!(:materialization_claim, &lower_claim/1)
  end

  defp lower(%{kind: :sequential} = context),
    do: Map.update!(context, :materialization_claim, &lower_claim/1)

  defp lower(context), do: context

  defp lower_claim(%{target_operation_lock: %TargetOperationLock{} = lock} = claim),
    do: %{claim | target_operation_lock: Map.from_struct(lock)}

  defp lower_claim(claim), do: claim

  defp restore(%{kind: :pipeline} = context) do
    context
    |> Map.update!(
      :resource_circuit_permits,
      &Enum.map(&1, fn permit ->
        if Enum.sort(Map.keys(permit)) != [:owner_id, :probe?, :resource],
          do: raise(ArgumentError)

        struct!(ResourceCircuitPermit, permit)
      end)
    )
    |> Map.update!(:materialization_claim, &restore_claim/1)
  end

  defp restore(%{kind: :sequential} = context),
    do: Map.update!(context, :materialization_claim, &restore_claim/1)

  defp restore(context), do: context

  defp restore_claim(%{target_operation_lock: lock} = claim) when is_map(lock),
    do: %{claim | target_operation_lock: struct!(TargetOperationLock, lock)}

  defp restore_claim(claim), do: claim
end
