defmodule FavnStoragePostgres.TargetRecoveries.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.TargetRecoveryStore

  import Ecto.Query

  alias Favn.GenerationDataPlaneMarker
  alias Favn.TargetGeneration
  alias Favn.TargetGenerationRelation
  alias FavnOrchestrator.Persistence.Commands.ActivateRecoveredTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.BeginTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.CreateTargetRecoveryIntent
  alias FavnOrchestrator.Persistence.Commands.FailTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.FinalizeTargetRecoveryPlan
  alias FavnOrchestrator.Persistence.Commands.MarkTargetRecoveryUnknown
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Queries.GetTargetRecovery
  alias FavnOrchestrator.Persistence.Results.InitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Results.TargetRecoveryOperation
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AssetTargetBinding
  alias FavnStoragePostgres.Schemas.AssetTargetGeneration
  alias FavnStoragePostgres.Schemas.Materialization
  alias FavnStoragePostgres.Schemas.TargetOperationLock
  alias FavnStoragePostgres.Schemas.TargetRecoveryOperation
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment

  @impl true
  def get_initial_candidate(%GetInitialTargetRecoveryCandidate{} = query) do
    with :ok <- validate_context_and_id(query.workspace_context, query.target_id) do
      transaction(fn -> initial_candidate!(query) end)
    end
  end

  @impl true
  def create_intent(%CreateTargetRecoveryIntent{} = command) do
    with :ok <- validate_intent(command) do
      transaction(fn -> create_intent!(command) end)
    end
  end

  @impl true
  def finalize_plan(%FinalizeTargetRecoveryPlan{} = command) do
    with :ok <- validate_finalize(command) do
      transaction(fn -> finalize_plan!(command) end)
    end
  end

  @impl true
  def begin_recovery(%BeginTargetRecovery{} = command) do
    with :ok <- validate_begin(command) do
      transaction(fn -> begin_recovery!(command) end)
    end
  end

  @impl true
  def activate_generation(%ActivateRecoveredTargetGeneration{} = command) do
    with :ok <- validate_activate(command) do
      transaction(fn -> activate_generation!(command) end)
    end
  end

  @impl true
  def mark_unknown(%MarkTargetRecoveryUnknown{} = command) do
    with :ok <- validate_transition(command, :unknown_outcome) do
      transaction(fn -> mark_unknown!(command) end)
    end
  end

  @impl true
  def fail_recovery(%FailTargetRecovery{} = command) do
    with :ok <- validate_transition(command, :terminal_error) do
      transaction(fn -> fail_recovery!(command) end)
    end
  end

  @impl true
  def get(%GetTargetRecovery{} = query) do
    with :ok <- validate_context_and_id(query.workspace_context, query.operation_id) do
      case Repo.get_by(TargetRecoveryOperation,
             workspace_id: query.workspace_context.workspace_id,
             operation_id: query.operation_id
           ) do
        nil -> {:error, Error.new(:not_found, "target recovery not found")}
        operation -> {:ok, operation_result(operation)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp initial_candidate!(query) do
    workspace_id = query.workspace_context.workspace_id

    binding =
      from(binding in AssetTargetBinding,
        where: binding.workspace_id == ^workspace_id and binding.target_id == ^query.target_id,
        lock: "FOR SHARE"
      )
      |> Repo.one()

    if is_nil(binding) or not is_nil(binding.active_generation_id) do
      Repo.rollback(Error.new(:conflict, "target has no interrupted initial generation"))
    end

    evidence =
      from(generation in AssetTargetGeneration,
        join: materialization in Materialization,
        on:
          materialization.workspace_id == generation.workspace_id and
            materialization.target_id == generation.target_id and
            materialization.target_generation_id == generation.target_generation_id and
            materialization.evidence_generation_id ==
              fragment("?::text", generation.target_generation_id),
        join: deployment in WorkspaceDeployment,
        on:
          deployment.workspace_id == materialization.workspace_id and
            deployment.deployment_id == materialization.deployment_id and
            deployment.manifest_version_id == generation.creating_manifest_id,
        where:
          generation.workspace_id == ^workspace_id and generation.target_id == ^query.target_id and
            materialization.target_kind == "asset" and
            generation.status == "building" and
            is_nil(generation.creating_rebuild_operation_id),
        order_by: [asc: materialization.inserted_at, asc: materialization.materialization_id],
        limit: 1,
        lock: "FOR SHARE",
        select: {generation, materialization.materialization_id}
      )
      |> Repo.one()

    case evidence do
      {%AssetTargetGeneration{} = generation, materialization_id} ->
        %InitialTargetRecoveryCandidate{
          binding: binding_result(binding),
          generation: generation_result(generation),
          materialization_id: materialization_id
        }

      nil ->
        Repo.rollback(
          Error.new(:conflict, "target has no successful initial materialization evidence",
            details: %{reason_code: "target_recovery_evidence_missing"}
          )
        )
    end
  end

  defp create_intent!(command) do
    workspace_id = command.workspace_context.workspace_id

    existing =
      from(operation in TargetRecoveryOperation,
        where:
          operation.workspace_id == ^workspace_id and
            operation.idempotency_key == ^command.idempotency_key,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    case existing do
      %TargetRecoveryOperation{} = operation ->
        if operation.target_id == command.target_id and operation.reason == command.reason and
             operation.recovery_kind == Atom.to_string(command.recovery_kind) do
          operation |> operation_result() |> Map.put(:idempotency_replay?, true)
        else
          Repo.rollback(
            Error.new(:conflict, "target recovery idempotency key has different request content",
              details: %{reason_code: "idempotency_conflict"}
            )
          )
        end

      nil ->
        %TargetRecoveryOperation{
          workspace_id: workspace_id,
          operation_id: command.operation_id,
          target_id: command.target_id,
          recovery_kind: Atom.to_string(command.recovery_kind),
          desired_manifest_id: command.desired_manifest_id,
          source_manifest_id: command.source_manifest_id,
          target_generation_id: command.target_generation_id,
          materialization_id: command.materialization_id,
          plan_hash: nil,
          plan_version: 1,
          plan_payload: %{},
          state: "planning",
          phase: "collecting_evidence",
          actor_id: command.actor_id,
          session_id: command.session_id,
          reason: command.reason,
          idempotency_key: command.idempotency_key,
          expected_binding_version: command.expected_binding_version,
          expected_physical_fingerprint: nil,
          evaluated_at: command.evaluated_at,
          last_command_id: command.command_id,
          version: 1,
          inserted_at: command.occurred_at,
          updated_at: command.occurred_at
        }
        |> Repo.insert!()
        |> operation_result()
    end
  end

  defp finalize_plan!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)

    cond do
      operation.state == "planned" and operation.plan_hash == command.plan_hash and
          operation.expected_physical_fingerprint == command.expected_physical_fingerprint ->
        operation_result(operation)

      operation.state != "planning" or operation.version != command.expected_version ->
        stale_recovery!()

      true ->
        operation
        |> Ecto.Changeset.change(%{
          plan_hash: command.plan_hash,
          plan_payload: command.plan_payload,
          state: "planned",
          phase: "planned",
          expected_physical_fingerprint: command.expected_physical_fingerprint,
          last_command_id: command.command_id,
          version: operation.version + 1,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
        |> operation_result()
    end
  end

  defp begin_recovery!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)

    cond do
      operation.state == "applying" and operation.recovery_token == command.recovery_token ->
        operation_result(operation)

      operation.state != "planned" ->
        Repo.rollback(Error.new(:conflict, "target recovery is no longer planned"))

      operation.plan_hash != command.plan_hash or operation.version != command.expected_version ->
        stale_recovery!()

      true ->
        operation
        |> Ecto.Changeset.change(%{
          state: "applying",
          phase: "marker_intent",
          recovery_token: command.recovery_token,
          last_command_id: command.command_id,
          version: operation.version + 1,
          started_at: command.occurred_at,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
        |> operation_result()
    end
  end

  defp mark_unknown!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)

    cond do
      operation.state == "outcome_unknown" and
          operation.unknown_outcome == command.unknown_outcome ->
        operation_result(operation)

      operation.state != "applying" or operation.version != command.expected_version ->
        stale_recovery!()

      true ->
        operation
        |> Ecto.Changeset.change(%{
          state: "outcome_unknown",
          phase: "reconciling",
          unknown_outcome: command.unknown_outcome,
          last_command_id: command.command_id,
          version: operation.version + 1,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
        |> operation_result()
    end
  end

  defp fail_recovery!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)

    cond do
      operation.state == "failed" and operation.terminal_error == command.terminal_error ->
        operation_result(operation)

      operation.state not in ["planning", "planned", "applying"] or
          operation.version != command.expected_version ->
        stale_recovery!()

      true ->
        operation
        |> Ecto.Changeset.change(%{
          state: "failed",
          phase: "terminal",
          terminal_error: command.terminal_error,
          last_command_id: command.command_id,
          version: operation.version + 1,
          completed_at: command.occurred_at,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
        |> operation_result()
    end
  end

  defp activate_generation!(command) do
    workspace_id = command.workspace_context.workspace_id
    operation = lock_operation!(workspace_id, command.operation_id)
    lock = lock_target_operation!(workspace_id, command.target_id)
    binding = lock_binding!(workspace_id, command.target_id)

    generation =
      from(generation in AssetTargetGeneration,
        where:
          generation.workspace_id == ^workspace_id and
            generation.target_id == ^command.target_id and
            generation.target_generation_id == ^command.target_generation_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    materialization =
      from(materialization in Materialization,
        join: deployment in WorkspaceDeployment,
        on:
          deployment.workspace_id == materialization.workspace_id and
            deployment.deployment_id == materialization.deployment_id,
        where:
          materialization.workspace_id == ^workspace_id and
            materialization.materialization_id == ^command.materialization_id,
        lock: "FOR SHARE",
        select: {materialization, deployment.manifest_version_id}
      )
      |> Repo.one()

    validate_activation_state!(
      operation,
      lock,
      binding,
      generation,
      materialization,
      command
    )

    generation
    |> Ecto.Changeset.change(%{
      active_descriptor_hash: generation.creating_descriptor_hash,
      physical_schema_fingerprint: command.physical_schema_fingerprint,
      data_plane_marker: canonical_map(command.data_plane_marker),
      activation_token: operation.recovery_token,
      status: "active",
      version: generation.version + 1,
      activated_at: command.occurred_at,
      updated_at: command.occurred_at
    })
    |> Repo.update!()

    binding
    |> Ecto.Changeset.change(%{
      active_generation_id: generation.target_generation_id,
      compatibility_status: Atom.to_string(command.compatibility_status),
      reason_code: command.reason_code,
      compatibility_diff: command.compatibility_diff,
      active_physical_fingerprint: command.physical_schema_fingerprint,
      version: binding.version + 1,
      updated_at: command.occurred_at
    })
    |> Repo.update!()

    compatibility_result = %{
      status: command.compatibility_status,
      reason_code: command.reason_code,
      diff: command.compatibility_diff
    }

    operation
    |> Ecto.Changeset.change(%{
      state: "succeeded",
      phase: "terminal",
      result_marker: canonical_map(command.data_plane_marker),
      compatibility_result: compatibility_result,
      unknown_outcome: nil,
      last_command_id: command.command_id,
      version: operation.version + 1,
      completed_at: command.occurred_at,
      updated_at: command.occurred_at
    })
    |> Repo.update!()
    |> operation_result()
  end

  defp validate_activation_state!(
         operation,
         lock,
         binding,
         generation,
         materialization,
         command
       ) do
    now = database_now!()
    source_manifest_id = command.source_manifest_id

    valid? =
      operation.version == command.expected_operation_version and
        operation.state in ["applying", "outcome_unknown"] and
        operation.target_id == command.target_id and
        operation.target_generation_id == command.target_generation_id and
        operation.materialization_id == command.materialization_id and
        operation.source_manifest_id == command.source_manifest_id and
        operation.expected_physical_fingerprint == command.physical_schema_fingerprint and
        lock.operation_id == command.operation_id and
        lock.operation_type == "target_recovery" and lock.lease_owner == command.lease_owner and
        lock.fencing_token == command.fencing_token and
        DateTime.compare(lock.lease_expires_at, now) == :gt and
        binding.version == command.expected_binding_version and
        is_nil(binding.active_generation_id) and
        binding.desired_manifest_id == command.expected_desired_manifest_id and
        binding.desired_descriptor_hash == command.expected_desired_descriptor_hash and
        match?(%AssetTargetGeneration{}, generation) and generation.status == "building" and
        is_nil(generation.creating_rebuild_operation_id) and
        generation.creating_manifest_id == command.source_manifest_id and
        match?({%Materialization{}, ^source_manifest_id}, materialization) and
        elem(materialization, 0).target_kind == "asset" and
        elem(materialization, 0).target_id == command.target_id and
        elem(materialization, 0).target_generation_id == command.target_generation_id and
        elem(materialization, 0).evidence_generation_id == command.target_generation_id and
        valid_compatibility?(command.compatibility_status) and
        marker_matches?(command.data_plane_marker, command, operation, generation)

    unless valid?, do: stale_recovery!()
  end

  defp marker_matches?(marker, command, operation, generation) when is_map(marker) do
    GenerationDataPlaneMarker.validate(
      marker,
      command.target_id,
      command.target_generation_id
    ) == :ok and
      field(marker, :activation_operation_id) == command.expected_marker_operation_id and
      same_relation?(field(marker, :active_relation), generation.physical_relation) and
      field(operation.plan_payload, :physical_relation_instance_id) ==
        TargetGenerationRelation.instance_id(field(marker, :activation_token)) and
      marker_identity(marker) ==
        marker_identity(field(operation.plan_payload, :data_plane_marker))
  end

  defp marker_matches?(_marker, _command, _operation, _generation), do: false

  defp valid_compatibility?(status),
    do: status in [:ready, :rebuild_available, :rebuild_required]

  defp lock_operation!(workspace_id, operation_id) do
    from(operation in TargetRecoveryOperation,
      where: operation.workspace_id == ^workspace_id and operation.operation_id == ^operation_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "target recovery not found"))
      operation -> operation
    end
  end

  defp lock_target_operation!(workspace_id, target_id) do
    from(lock in TargetOperationLock,
      where: lock.workspace_id == ^workspace_id and lock.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:fenced, "target recovery lock no longer exists"))
      lock -> lock
    end
  end

  defp lock_binding!(workspace_id, target_id) do
    from(binding in AssetTargetBinding,
      where: binding.workspace_id == ^workspace_id and binding.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "target binding not found"))
      binding -> binding
    end
  end

  defp binding_result(binding) do
    %TargetBinding{
      workspace_id: binding.workspace_id,
      target_id: binding.target_id,
      active_generation_id: binding.active_generation_id,
      active_manifest_id: nil,
      active_descriptor_hash: nil,
      active_physical_relation: nil,
      active_data_plane_marker: nil,
      desired_manifest_id: binding.desired_manifest_id,
      desired_descriptor_hash: binding.desired_descriptor_hash,
      compatibility_status: String.to_existing_atom(binding.compatibility_status),
      reason_code: binding.reason_code,
      compatibility_diff: binding.compatibility_diff,
      active_physical_fingerprint: binding.active_physical_fingerprint,
      version: binding.version,
      updated_at: binding.updated_at
    }
  end

  defp generation_result(generation) do
    {:ok, result} =
      TargetGeneration.new(%{
        workspace_id: generation.workspace_id,
        target_id: generation.target_id,
        target_generation_id: generation.target_generation_id,
        creating_manifest_id: generation.creating_manifest_id,
        creating_descriptor_hash: generation.creating_descriptor_hash,
        active_descriptor_hash: generation.active_descriptor_hash,
        logical_relation: canonical_map(generation.logical_relation),
        physical_relation: canonical_map(generation.physical_relation),
        physical_schema_fingerprint: generation.physical_schema_fingerprint,
        data_plane_marker: canonical_map(generation.data_plane_marker),
        status: String.to_existing_atom(generation.status),
        rebuild_operation_id: generation.creating_rebuild_operation_id,
        version: generation.version,
        created_at: generation.created_at,
        activated_at: generation.activated_at,
        retired_at: generation.retired_at,
        updated_at: generation.updated_at
      })

    result
  end

  defp operation_result(operation) do
    %TargetRecoveryOperation{
      workspace_id: operation.workspace_id,
      operation_id: operation.operation_id,
      target_id: operation.target_id,
      recovery_kind: recovery_kind(operation.recovery_kind),
      desired_manifest_id: operation.desired_manifest_id,
      source_manifest_id: operation.source_manifest_id,
      target_generation_id: operation.target_generation_id,
      materialization_id: operation.materialization_id,
      plan_hash: operation.plan_hash,
      plan_version: operation.plan_version,
      plan_payload: operation.plan_payload,
      state: operation_state(operation.state),
      phase: operation_phase(operation.phase),
      actor_id: operation.actor_id,
      session_id: operation.session_id,
      reason: operation.reason,
      idempotency_key: operation.idempotency_key,
      expected_binding_version: operation.expected_binding_version,
      expected_physical_fingerprint: operation.expected_physical_fingerprint,
      evaluated_at: operation.evaluated_at,
      recovery_token: operation.recovery_token,
      result_marker: operation.result_marker,
      compatibility_result: operation.compatibility_result,
      unknown_outcome: operation.unknown_outcome,
      terminal_error: operation.terminal_error,
      version: operation.version,
      started_at: operation.started_at,
      completed_at: operation.completed_at,
      inserted_at: operation.inserted_at,
      updated_at: operation.updated_at
    }
  end

  defp validate_intent(command) do
    values = [
      command.command_id,
      command.operation_id,
      command.target_id,
      command.desired_manifest_id,
      command.source_manifest_id,
      command.target_generation_id,
      command.materialization_id,
      command.actor_id,
      command.idempotency_key
    ]

    if WorkspaceContext.valid?(command.workspace_context) and
         Enum.all?(values, &valid_id?/1) and
         command.recovery_kind == :reconcile_initial_generation and
         command.expected_binding_version > 0 and
         is_binary(command.reason) and byte_size(command.reason) in 1..4096 and
         match?(%DateTime{}, command.evaluated_at) and match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target recovery intent")}
    end
  end

  defp validate_finalize(command) do
    if WorkspaceContext.valid?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.operation_id) and command.expected_version > 0 and
         valid_hash?(command.plan_hash) and valid_hash?(command.expected_physical_fingerprint) and
         is_map(command.plan_payload) and match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target recovery plan finalization")}
    end
  end

  defp validate_begin(command) do
    if WorkspaceContext.valid?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.operation_id) and valid_hash?(command.plan_hash) and
         command.expected_version > 0 and valid_id?(command.recovery_token) and
         match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target recovery start")}
    end
  end

  defp validate_activate(command) do
    ids = [
      command.command_id,
      command.operation_id,
      command.target_id,
      command.target_generation_id,
      command.materialization_id,
      command.source_manifest_id,
      command.expected_desired_manifest_id,
      command.expected_marker_operation_id,
      command.lease_owner
    ]

    if WorkspaceContext.valid?(command.workspace_context) and Enum.all?(ids, &valid_id?/1) and
         command.expected_operation_version > 0 and command.expected_binding_version > 0 and
         valid_hash?(command.expected_desired_descriptor_hash) and
         valid_hash?(command.physical_schema_fingerprint) and is_map(command.data_plane_marker) and
         is_map(command.compatibility_diff) and is_binary(command.reason_code) and
         command.fencing_token > 0 and match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target recovery activation")}
    end
  end

  defp validate_transition(command, payload_field) do
    payload = Map.fetch!(command, payload_field)

    if WorkspaceContext.valid?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.operation_id) and command.expected_version > 0 and is_map(payload) and
         map_size(payload) > 0 and match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target recovery transition")}
    end
  end

  defp validate_context_and_id(context, id) do
    if WorkspaceContext.valid?(context) and valid_id?(id),
      do: :ok,
      else: {:error, Error.new(:invalid, "invalid target recovery query")}
  end

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp database_now! do
    %{rows: [[now]]} = Ecto.Adapters.SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp stale_recovery! do
    Repo.rollback(
      Error.new(:conflict, "target recovery plan is stale",
        details: %{reason_code: "target_recovery_plan_stale"}
      )
    )
  end

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255
  defp valid_hash?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp recovery_kind("reconcile_initial_generation"), do: :reconcile_initial_generation
  defp operation_state("planning"), do: :planning
  defp operation_state("planned"), do: :planned
  defp operation_state("applying"), do: :applying
  defp operation_state("outcome_unknown"), do: :outcome_unknown
  defp operation_state("succeeded"), do: :succeeded
  defp operation_state("failed"), do: :failed
  defp operation_phase("collecting_evidence"), do: :collecting_evidence
  defp operation_phase("planned"), do: :planned
  defp operation_phase("marker_intent"), do: :marker_intent
  defp operation_phase("reconciling"), do: :reconciling
  defp operation_phase("terminal"), do: :terminal

  defp same_relation?(left, right) when is_map(left) and is_map(right),
    do: relation_identity(left) == relation_identity(right)

  defp same_relation?(_left, _right), do: false

  defp relation_identity(relation) when is_map(relation) do
    %{
      connection: relation_value(relation, :connection),
      catalog: relation_value(relation, :catalog),
      schema: relation_value(relation, :schema),
      name: relation_value(relation, :name)
    }
  end

  defp relation_identity(_relation), do: nil

  defp marker_identity(marker) when is_map(marker) do
    {
      field(marker, :target_id),
      relation_identity(field(marker, :active_relation)),
      field(marker, :active_generation_id),
      field(marker, :activation_operation_id),
      field(marker, :activation_token)
    }
  end

  defp marker_identity(_marker), do: nil

  defp relation_value(relation, key) do
    case field(relation, key) do
      value when is_atom(value) -> Atom.to_string(value)
      value -> value
    end
  end

  defp canonical_map(nil), do: nil
  defp canonical_map(map) when is_map(map), do: Map.new(map)
  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
