defmodule FavnOrchestrator.TargetRecovery do
  @moduledoc """
  Plans and executes evidence-backed recovery of interrupted initial target generations.

  Recovery never adopts an arbitrary physical relation. It requires a matching successful
  materialization, building generation, historical descriptor, physical fingerprint, and
  exact generation marker before restoring the active binding.
  """

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.GenerationMarkerReadResult
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Asset
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Contract
  alias Favn.TargetCompatibility
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TargetGenerationRelation
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.ActivateRecoveredTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.BeginTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.CreateTargetRecoveryIntent
  alias FavnOrchestrator.Persistence.Commands.FailTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.FinalizeTargetRecoveryPlan
  alias FavnOrchestrator.Persistence.Commands.MarkTargetRecoveryUnknown
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Queries.GetTargetRecovery
  alias FavnOrchestrator.Persistence.Results.InitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Results.TargetOperationLock
  alias FavnOrchestrator.Persistence.Results.TargetRecoveryOperation
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.TargetRecovery.Plan

  @plan_ttl_seconds 3_600
  @default_lease_ms 30_000
  @required_capabilities [:physical_inspection, :marker_reconciliation]

  @doc "Creates and persists an immutable recovery plan for one ownership-unknown target."
  @spec plan(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, Plan.t()} | {:error, term()}
  def plan(%WorkspaceContext{} = context, target_id, reason, opts \\ [])
      when is_binary(target_id) and is_binary(reason) and is_list(opts) do
    with :ok <- authorize_operator(context),
         :ok <- validate_reason(reason),
         :ok <- validate_plan_options(opts),
         evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
         :ok <- validate_datetime(evaluated_at),
         operation_id <- Keyword.get(opts, :operation_id, recovery_id()),
         {:ok, durable} <- recovery_context(context, target_id) do
      try do
        with {:ok, intent} <-
               store().create_intent(%CreateTargetRecoveryIntent{
                 workspace_context: context,
                 command_id: command_id("plan", operation_id),
                 operation_id: operation_id,
                 target_id: target_id,
                 recovery_kind: :reconcile_initial_generation,
                 desired_manifest_id: durable.version.manifest_version_id,
                 source_manifest_id: durable.candidate.generation.creating_manifest_id,
                 target_generation_id: durable.candidate.generation.target_generation_id,
                 materialization_id: durable.candidate.materialization_id,
                 actor_id: context.principal_id,
                 session_id: Keyword.get(opts, :session_id),
                 reason: reason,
                 idempotency_key: Keyword.get(opts, :idempotency_key, operation_id),
                 expected_binding_version: durable.candidate.binding.version,
                 evaluated_at: evaluated_at,
                 occurred_at: Keyword.get(opts, :occurred_at, evaluated_at)
               }),
             {:ok, plan} <- resume_planning(context, intent, durable, opts) do
          {:ok, plan}
        end
      after
        release_recovery_context(durable)
      end
    end
  end

  @doc "Starts an exact recovery plan and returns its authoritative durable state."
  @spec start(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, TargetRecoveryOperation.t()} | {:error, term()}
  def start(%WorkspaceContext{} = context, operation_id, plan_hash, opts \\ [])
      when is_binary(operation_id) and is_binary(plan_hash) and is_list(opts) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts),
         {:ok, operation} <- get(context, operation_id) do
      start_operation(context, operation, plan_hash, opts)
    end
  end

  defp start_operation(context, operation, plan_hash, opts) do
    with :ok <- ensure_startable(operation, plan_hash),
         {:ok, evidence} <-
           current_recovery_evidence(
             context,
             operation.target_id,
             {:start, operation.operation_id, operation.version}
           ) do
      with_recovery_evidence(evidence, fn ->
        with :ok <- revalidate_operation(operation, evidence),
             {:ok, [lock]} <- acquire_lock(context, operation, opts),
             {:ok, begun} <- begin_recovery_with_release(context, operation, lock, opts) do
          execute_started(context, begun, evidence, lock, opts)
        end
      end)
    else
      {:ok, :idempotent_replay} -> {:ok, %{operation | idempotency_replay?: true}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Reads marker evidence for an inconclusive recovery and completes it when proven."
  @spec reconcile(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, TargetRecoveryOperation.t()} | {:error, term()}
  def reconcile(%WorkspaceContext{} = context, operation_id, opts \\ [])
      when is_binary(operation_id) and is_list(opts) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts),
         {:ok, operation} <- get(context, operation_id) do
      reconcile_operation(context, operation, opts)
    end
  end

  defp reconcile_operation(
         _context,
         %TargetRecoveryOperation{state: :succeeded} = operation,
         _opts
       ),
       do: {:ok, %{operation | idempotency_replay?: true}}

  defp reconcile_operation(context, operation, opts) do
    occurred_at = Keyword.get(opts, :occurred_at, DateTime.utc_now())

    with :ok <- ensure_reconcilable(operation),
         {:ok, evidence} <-
           current_recovery_evidence(
             context,
             operation.target_id,
             {:reconcile, operation.operation_id, operation.version, occurred_at}
           ) do
      with_recovery_evidence(evidence, fn ->
        with :ok <- revalidate_operation(operation, evidence),
             {:ok, [lock]} <- acquire_lock(context, operation, opts) do
          reconcile_marker(context, operation, evidence, lock, opts)
        end
      end)
    end
  end

  @doc "Returns one authoritative recovery operation."
  @spec get(WorkspaceContext.t(), String.t()) ::
          {:ok, TargetRecoveryOperation.t()} | {:error, term()}
  def get(%WorkspaceContext{} = context, operation_id) when is_binary(operation_id) do
    with :ok <- authorize_read(context) do
      store().get(%GetTargetRecovery{
        workspace_context: context,
        operation_id: operation_id
      })
    end
  end

  defp recovery_context(context, target_id) do
    with {:ok, candidate} <-
           store().get_initial_candidate(%GetInitialTargetRecoveryCandidate{
             workspace_context: context,
             target_id: target_id
           }),
         {:ok, runtime, version, asset, manifest_lease} <- active_asset(context, target_id) do
      result =
        with :ok <- current_binding_matches(candidate, version, asset),
             {:ok, source_descriptor} <- source_descriptor(candidate),
             :ok <- source_generation_matches(candidate, source_descriptor),
             :ok <- recoverable_contract(asset, source_descriptor),
             :ok <- same_physical_relation(asset, candidate) do
          {:ok,
           %{
             runtime: runtime,
             version: version,
             asset: asset,
             manifest_lease: manifest_lease,
             candidate: candidate,
             source_descriptor: source_descriptor
           }}
        end

      case result do
        {:ok, _durable} = success ->
          success

        {:error, _reason} = error ->
          release_recovery_context(%{manifest_lease: manifest_lease}, error)
      end
    end
  end

  defp current_recovery_evidence(context, target_id, task_scope) do
    with {:ok, durable} <- recovery_context(context, target_id) do
      case collect_runner_evidence(context, durable, task_scope) do
        {:error, {:terminal_target_recovery_evidence, reason}} ->
          release_recovery_context(durable, {:error, reason})

        {:error, _reason} = error ->
          release_recovery_context(durable, error)

        result ->
          result
      end
    end
  end

  defp collect_runner_evidence(context, durable, task_scope) do
    with :ok <- required_capabilities(context, durable.version, durable.asset, task_scope),
         {:ok, physical, relation_instance_id} <-
           inspect_physical(
             context,
             durable.version,
             durable.asset,
             durable.asset.relation,
             task_scope
           ),
         :ok <-
           source_physical_identity(
             durable.source_descriptor,
             target_contract(durable.asset),
             physical
           ),
         {:ok, marker} <-
           existing_marker(
             context,
             durable.version,
             durable.asset,
             durable.candidate.generation,
             relation_instance_id,
             task_scope
           ) do
      {:ok,
       Map.merge(durable, %{
         physical: physical,
         relation_instance_id: relation_instance_id,
         marker: marker,
         task_scope: task_scope
       })}
    end
  end

  defp resume_planning(
         context,
         %TargetRecoveryOperation{state: :planning} = intent,
         durable,
         opts
       ) do
    case revalidate_intent(intent, durable) do
      :ok -> complete_plan(context, intent, durable, opts)
      {:error, reason} -> persist_terminal_planning_failure(context, intent, reason, opts)
    end
  end

  defp resume_planning(context, operation, durable, opts),
    do: complete_plan(context, operation, durable, opts)

  defp complete_plan(context, %TargetRecoveryOperation{state: :planning} = intent, durable, opts) do
    task_scope = {:plan, intent.operation_id, intent.evaluated_at}

    case collect_runner_evidence(context, durable, task_scope) do
      {:ok, evidence} ->
        expires_at = DateTime.add(intent.evaluated_at, @plan_ttl_seconds, :second)
        plan = build_plan(intent.operation_id, expires_at, intent.evaluated_at, evidence)

        with {:ok, operation} <-
               store().finalize_plan(%FinalizeTargetRecoveryPlan{
                 workspace_context: context,
                 command_id: command_id("finalize-plan", intent.operation_id),
                 operation_id: intent.operation_id,
                 expected_version: intent.version,
                 plan_hash: plan.plan_hash,
                 plan_payload: plan.payload,
                 expected_physical_fingerprint: evidence.physical.fingerprint,
                 occurred_at: Keyword.get(opts, :occurred_at, intent.evaluated_at)
               }) do
          {:ok, persisted_plan(plan, operation)}
        end

      {:error, {:terminal_target_recovery_evidence, reason}} ->
        persist_planning_failure(context, intent, reason, opts)

      {:error, reason} ->
        persist_terminal_planning_failure(context, intent, reason, opts)
    end
  end

  defp complete_plan(
         _context,
         %TargetRecoveryOperation{plan_hash: plan_hash} = operation,
         _durable,
         _opts
       )
       when is_binary(plan_hash),
       do: {:ok, persisted_plan(operation)}

  defp complete_plan(_context, _operation, _durable, _opts),
    do: {:error, Error.new(:conflict, "target recovery plan cannot be resumed")}

  defp persist_terminal_planning_failure(
         context,
         %TargetRecoveryOperation{state: :planning} = intent,
         reason,
         opts
       ) do
    if terminal_planning_failure?(reason) do
      persist_planning_failure(context, intent, reason, opts)
    else
      {:error, reason}
    end
  end

  defp persist_planning_failure(
         context,
         %TargetRecoveryOperation{state: :planning} = intent,
         reason,
         opts
       ) do
    command = %FailTargetRecovery{
      workspace_context: context,
      command_id: command_id("fail-plan", intent.operation_id),
      operation_id: intent.operation_id,
      expected_version: intent.version,
      terminal_error: JsonSafe.error(reason),
      occurred_at: Keyword.get(opts, :occurred_at, intent.evaluated_at)
    }

    case store().fail_recovery(command) do
      {:ok, _failed} ->
        {:error, reason}

      {:error, persist_reason} ->
        {:error,
         Error.new(:internal, "failed to persist terminal target recovery planning error",
           retryable?: true,
           details: %{
             reason_code: "target_recovery_planning_failure_not_persisted",
             persistence_error: inspect_reason(persist_reason)
           }
         )}
    end
  end

  defp terminal_planning_failure?(%Error{retryable?: false}), do: true

  defp terminal_planning_failure?({:unsupported_runner_task_kind, _task_kind}), do: true
  defp terminal_planning_failure?({:runner_task_release_mismatch, _expected, _actual}), do: true
  defp terminal_planning_failure?({:invalid_runner_task_binding, _binding}), do: true
  defp terminal_planning_failure?(_reason), do: false

  defp revalidate_intent(operation, durable) do
    candidate = durable.candidate

    if operation.target_id == candidate.binding.target_id and
         operation.desired_manifest_id == durable.version.manifest_version_id and
         operation.source_manifest_id == candidate.generation.creating_manifest_id and
         operation.target_generation_id == candidate.generation.target_generation_id and
         operation.materialization_id == candidate.materialization_id and
         operation.expected_binding_version == candidate.binding.version do
      :ok
    else
      {:error, stale_error()}
    end
  end

  defp active_asset(context, target_id) do
    with {:ok, {runtime, grants}} <- ManifestStore.get_active_deployment(context),
         true <- Enum.any?(grants, &(&1.target_kind == :asset and &1.target_id == target_id)),
         {:ok, lease} <-
           ManifestStore.checkout_deployment_manifest(
             context,
             runtime.deployment_id,
             runtime.manifest_version_id
           ) do
      case ManifestTarget.resolve_asset(lease.version, target_id) do
        {:ok, %Asset{target_descriptor: %TargetDescriptor{}} = asset} ->
          {:ok, runtime, lease.version, asset, lease}

        {:ok, %Asset{}} ->
          release_recovery_context(
            %{manifest_lease: lease},
            {:error, Error.new(:conflict, "target is not a persisted SQL asset")}
          )

        {:error, _reason} = error ->
          release_recovery_context(%{manifest_lease: lease}, error)
      end
    else
      false -> {:error, Error.new(:not_found, "target is not in the active deployment")}
      {:error, _reason} = error -> error
    end
  end

  defp with_recovery_evidence(evidence, fun) do
    try do
      fun.()
    after
      release_recovery_context(evidence)
    end
  end

  defp release_recovery_context(%{manifest_lease: lease}) do
    ManifestStore.release_manifest(lease)
  end

  defp release_recovery_context(context, result) do
    :ok = release_recovery_context(context)
    result
  end

  defp source_descriptor(%InitialTargetRecoveryCandidate{} = candidate) do
    platform_context = SystemContext.platform(:target_recovery_historical_descriptor)
    manifest_id = candidate.generation.creating_manifest_id
    target_id = candidate.binding.target_id

    case ManifestStore.with_manifest(platform_context, manifest_id, fn version ->
           with {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
                %TargetDescriptor{} = descriptor <- asset.target_descriptor do
             {:ok, descriptor}
           else
             _invalid ->
               {:error, Error.new(:conflict, "historical target descriptor is missing")}
           end
         end) do
      {:ok, %TargetDescriptor{} = descriptor} ->
        {:ok, descriptor}

      {:error, %{details: %{reason: :historical_manifest_not_activatable}}} ->
        historical_descriptor(platform_context, manifest_id, target_id)

      {:error, _reason} = error ->
        error
    end
  end

  defp historical_descriptor(platform_context, manifest_id, target_id) do
    case ManifestStore.get_manifest_target_descriptors(platform_context, manifest_id, [target_id]) do
      {:ok, [%TargetDescriptor{} = descriptor]} -> {:ok, descriptor}
      {:ok, _other} -> {:error, Error.new(:conflict, "historical target descriptor is missing")}
      {:error, _reason} = error -> error
    end
  end

  defp current_binding_matches(candidate, version, asset) do
    binding = candidate.binding

    if binding.desired_manifest_id == version.manifest_version_id and
         binding.desired_descriptor_hash == asset.target_descriptor.descriptor_hash and
         is_nil(binding.active_generation_id) and
         binding.compatibility_status in [:uninitialized, :operator_decision] do
      :ok
    else
      {:error, stale_error()}
    end
  end

  defp source_generation_matches(candidate, descriptor) do
    generation = candidate.generation

    if generation.status == :building and
         generation.creating_descriptor_hash == descriptor.descriptor_hash do
      :ok
    else
      {:error, stale_error()}
    end
  end

  defp recoverable_contract(asset, source_descriptor) do
    if asset.target_descriptor.contract_fingerprint == source_descriptor.contract_fingerprint do
      :ok
    else
      {:error,
       Error.new(:conflict, "target contract changed after the interrupted materialization",
         details: %{reason_code: "target_recovery_contract_changed"}
       )}
    end
  end

  defp same_physical_relation(asset, candidate) do
    desired = relation_identity(asset.relation)
    physical = relation_identity(candidate.generation.physical_relation)

    if desired == physical do
      :ok
    else
      {:error,
       Error.new(:conflict, "target relation changed after the interrupted materialization",
         details: %{reason_code: "target_recovery_relation_changed"}
       )}
    end
  end

  defp required_capabilities(context, version, asset, task_scope) do
    request = %GenerationCapabilitiesRequest{
      manifest: Version.identity(version),
      asset_ref: asset.ref
    }

    case OperationRunnerTasks.ensure_and_await(
           context,
           version,
           asset.ref,
           :generation_capabilities,
           request,
           {:target_recovery_capabilities, task_scope}
         ) do
      {:ok, %GenerationCapabilitiesResult{capabilities: capabilities}} ->
        missing =
          Enum.reject(@required_capabilities, &(Map.get(capabilities, &1) == :supported))

        if missing == [] do
          :ok
        else
          terminal_evidence_error(
            Error.new(:conflict, "target adapter cannot safely reconcile ownership",
              details: %{
                reason_code: "target_recovery_not_supported",
                missing_capabilities: missing
              }
            )
          )
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        terminal_evidence_error(:invalid_generation_capabilities)
    end
  end

  defp inspect_physical(context, version, asset, relation, task_scope) do
    {:ok, binding} = OperationRunnerTasks.binding(version, asset)

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: binding.required_runner_release_id,
      relation: RelationRef.new!(relation),
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    case OperationRunnerTasks.ensure_and_await(
           context,
           version,
           asset.ref,
           :relation_inspection,
           request,
           {:target_recovery_inspection, task_scope},
           runner_binding: binding
         ) do
      {:ok, result} -> validate_physical_inspection(binding, result)
      {:error, _reason} = error -> error
    end
  end

  defp validate_physical_inspection(
         binding,
         %RelationInspectionResult{} = result
       ) do
    with :ok <-
           RunnerIdentityVerifier.verify_inspection_result(
             binding.required_runner_release_id,
             result
           ),
         {:ok, %PhysicalFingerprint{} = physical} <-
           PhysicalFingerprint.from_inspection(result),
         relation_instance_id when is_binary(relation_instance_id) <-
           field(result.table_metadata, :relation_instance_id) do
      {:ok, physical, relation_instance_id}
    else
      nil ->
        terminal_evidence_error(
          Error.new(:conflict, "physical relation has no durable Favn instance identity",
            details: %{reason_code: "target_recovery_relation_identity_missing"}
          )
        )

      {:error, reason} ->
        terminal_evidence_error(reason)

      _invalid ->
        terminal_evidence_error(:invalid_runner_inspection_result)
    end
  end

  defp validate_physical_inspection(_binding, :not_found) do
    terminal_evidence_error(
      Error.new(:conflict, "interrupted target relation no longer exists",
        details: %{reason_code: "target_recovery_relation_missing"}
      )
    )
  end

  defp validate_physical_inspection(_binding, _invalid),
    do: terminal_evidence_error(:invalid_runner_inspection_result)

  defp source_physical_identity(descriptor, contract, physical) do
    case PhysicalFingerprint.identity_diff(descriptor, physical, contract) do
      [] ->
        :ok

      diff ->
        {:error,
         Error.new(:conflict, "physical relation does not match its historical descriptor",
           details: %{reason_code: "target_recovery_identity_mismatch", diff: diff}
         )}
    end
  end

  defp build_plan(operation_id, expires_at, evaluated_at, evidence) do
    candidate = evidence.candidate

    Plan.new(operation_id, expires_at, %{
      plan_version: 1,
      recovery_kind: :reconcile_initial_generation,
      target_id: candidate.binding.target_id,
      desired_manifest_id: evidence.version.manifest_version_id,
      desired_descriptor_hash: evidence.asset.target_descriptor.descriptor_hash,
      source_manifest_id: candidate.generation.creating_manifest_id,
      source_descriptor_hash: evidence.source_descriptor.descriptor_hash,
      target_generation_id: candidate.generation.target_generation_id,
      materialization_id: candidate.materialization_id,
      expected_binding_version: candidate.binding.version,
      physical_relation: candidate.generation.physical_relation,
      physical_fingerprint: evidence.physical.fingerprint,
      physical_relation_instance_id: evidence.relation_instance_id,
      data_plane_marker: marker_map(evidence.marker),
      evaluated_at: evaluated_at
    })
  end

  defp ensure_startable(operation, plan_hash) do
    cond do
      operation.plan_hash != plan_hash ->
        {:error, stale_error()}

      operation.state in [:outcome_unknown, :succeeded] ->
        {:ok, :idempotent_replay}

      operation.state not in [:planned, :applying] ->
        {:error, Error.new(:conflict, "target recovery is no longer planned")}

      plan_expired?(operation.plan_payload) ->
        {:error, stale_error()}

      true ->
        :ok
    end
  end

  defp ensure_reconcilable(%TargetRecoveryOperation{state: state})
       when state in [:applying, :outcome_unknown],
       do: :ok

  defp ensure_reconcilable(_operation),
    do: {:error, Error.new(:conflict, "target recovery has no unknown outcome to reconcile")}

  defp revalidate_operation(operation, evidence) do
    payload = operation.plan_payload
    candidate = evidence.candidate

    valid? =
      operation.target_id == candidate.binding.target_id and
        operation.desired_manifest_id == evidence.version.manifest_version_id and
        operation.source_manifest_id == candidate.generation.creating_manifest_id and
        operation.target_generation_id == candidate.generation.target_generation_id and
        operation.materialization_id == candidate.materialization_id and
        operation.expected_binding_version == candidate.binding.version and
        operation.expected_physical_fingerprint == evidence.physical.fingerprint and
        field(payload, :physical_relation_instance_id) == evidence.relation_instance_id and
        field(payload, :desired_descriptor_hash) ==
          evidence.asset.target_descriptor.descriptor_hash and
        field(payload, :source_descriptor_hash) == evidence.source_descriptor.descriptor_hash and
        marker_plan_identity(field(payload, :data_plane_marker)) ==
          marker_identity(evidence.marker)

    if valid?, do: :ok, else: {:error, stale_error()}
  end

  defp acquire_lock(context, operation, opts) do
    lock_store().acquire_many(%AcquireTargetOperationLocks{
      workspace_context: context,
      command_id: command_id("lock", operation.operation_id),
      target_ids: [operation.target_id],
      operation_id: operation.operation_id,
      operation_type: :target_recovery,
      lease_owner: operation.operation_id,
      lease_duration_ms: Keyword.get(opts, :lease_duration_ms, @default_lease_ms),
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
    })
  end

  defp begin_recovery(context, operation, opts) do
    if operation.state == :applying do
      {:ok, operation}
    else
      persist_recovery_intent(context, operation, opts)
    end
  end

  defp persist_recovery_intent(context, operation, opts) do
    token = recovery_token(operation)

    store().begin_recovery(%BeginTargetRecovery{
      workspace_context: context,
      command_id: command_id("begin", operation.operation_id),
      operation_id: operation.operation_id,
      plan_hash: operation.plan_hash,
      expected_version: operation.version,
      recovery_token: token,
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
    })
  end

  defp begin_recovery_with_release(context, operation, lock, opts) do
    case begin_recovery(context, operation, opts) do
      {:ok, begun} ->
        {:ok, begun}

      {:error, _reason} = error ->
        release_lock(context, operation, lock)
        error
    end
  end

  defp execute_started(context, operation, evidence, lock, opts) do
    activate(context, operation, evidence, lock, evidence.marker, opts)
  end

  defp reconcile_marker(context, operation, evidence, lock, opts) do
    case read_marker(context, evidence) do
      {:ok, %GenerationMarker{} = marker} ->
        if marker_identity(marker) == marker_identity(evidence.marker) do
          activate(context, operation, evidence, lock, marker, opts)
        else
          persist_reconciliation_unknown(
            context,
            operation,
            lock,
            "marker_mismatch",
            opts
          )
        end

      {:ok, nil} ->
        persist_reconciliation_unknown(context, operation, lock, "marker_not_found", opts)

      {:ok, _invalid} ->
        persist_reconciliation_unknown(context, operation, lock, "invalid_marker", opts)

      {:error, reason} ->
        persist_reconciliation_unknown(
          context,
          operation,
          lock,
          inspect_reason(reason),
          opts
        )
    end
  end

  defp persist_reconciliation_unknown(
         context,
         %TargetRecoveryOperation{state: :applying} = operation,
         lock,
         reason,
         opts
       ),
       do: mark_unknown(context, operation, lock, reason, opts)

  defp persist_reconciliation_unknown(
         context,
         %TargetRecoveryOperation{state: :outcome_unknown} = operation,
         lock,
         _reason,
         _opts
       ) do
    release_lock(context, operation, lock)
    {:ok, operation}
  end

  defp read_marker(context, evidence) do
    request = %GenerationMarkerReadRequest{
      manifest: Version.identity(evidence.version),
      asset_ref: evidence.asset.ref,
      require_relation_instance?: true
    }

    case OperationRunnerTasks.ensure_and_await(
           context,
           evidence.version,
           evidence.asset.ref,
           :generation_marker_read,
           request,
           {:target_recovery_reconcile_marker, evidence.task_scope}
         ) do
      {:ok, %GenerationMarkerReadResult{marker: marker}} -> {:ok, marker}
      {:ok, _invalid} -> {:error, :invalid_generation_marker_read_result}
      {:error, _reason} = error -> error
    end
  end

  defp existing_marker(context, version, asset, generation, relation_instance_id, task_scope) do
    request = %GenerationMarkerReadRequest{
      manifest: Version.identity(version),
      asset_ref: asset.ref,
      require_relation_instance?: true
    }

    case OperationRunnerTasks.ensure_and_await(
           context,
           version,
           asset.ref,
           :generation_marker_read,
           request,
           {:target_recovery_evidence_marker, task_scope}
         ) do
      {:ok, %GenerationMarkerReadResult{marker: %GenerationMarker{} = marker}} ->
        expected = {
          generation.target_id,
          RelationRef.new!(asset.relation),
          generation.target_generation_id
        }

        actual = {marker.target_id, marker.active_relation, marker.active_generation_id}

        with :ok <- GenerationMarker.validate(marker),
             true <- actual == expected,
             true <-
               relation_instance_id ==
                 TargetGenerationRelation.instance_id(marker.activation_token) do
          {:ok, marker}
        else
          _invalid -> terminal_marker_conflict("target_recovery_marker_mismatch")
        end

      {:ok, %GenerationMarkerReadResult{marker: nil}} ->
        terminal_marker_conflict("target_recovery_marker_missing")

      {:ok, %GenerationMarkerReadResult{}} ->
        terminal_marker_conflict("target_recovery_marker_invalid")

      {:ok, _invalid} ->
        terminal_marker_conflict("target_recovery_marker_invalid")

      {:error, _reason} = error ->
        error
    end
  end

  defp terminal_marker_conflict(reason_code) do
    case marker_conflict(reason_code) do
      {:error, reason} -> terminal_evidence_error(reason)
    end
  end

  defp marker_conflict(reason_code) do
    {:error,
     Error.new(:conflict, "target has no matching Favn generation marker",
       details: %{reason_code: reason_code}
     )}
  end

  defp terminal_evidence_error(reason),
    do: {:error, {:terminal_target_recovery_evidence, reason}}

  defp activate(context, operation, evidence, lock, marker, opts) do
    physical = evidence.physical
    result = compatibility_result(evidence)

    command = %ActivateRecoveredTargetGeneration{
      workspace_context: context,
      command_id: command_id("activate", operation.operation_id),
      operation_id: operation.operation_id,
      expected_operation_version: operation.version,
      target_id: operation.target_id,
      target_generation_id: operation.target_generation_id,
      materialization_id: operation.materialization_id,
      source_manifest_id: operation.source_manifest_id,
      expected_binding_version: evidence.candidate.binding.version,
      expected_desired_manifest_id: evidence.version.manifest_version_id,
      expected_desired_descriptor_hash: evidence.asset.target_descriptor.descriptor_hash,
      physical_schema_fingerprint: physical.fingerprint,
      expected_marker_operation_id: evidence.marker.activation_operation_id,
      data_plane_marker: marker_map(marker),
      compatibility_status: result.status,
      reason_code: Atom.to_string(result.reason_code),
      compatibility_diff: result.diff,
      lease_owner: operation.operation_id,
      fencing_token: lock.fencing_token,
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
    }

    case store().activate_generation(command) do
      {:ok, completed} ->
        release_lock(context, operation, lock)
        {:ok, completed}

      {:error, _reason} = error ->
        release_lock(context, operation, lock)
        error
    end
  end

  defp compatibility_result(evidence) do
    TargetCompatibility.classify(
      evidence.asset.target_descriptor,
      evidence.source_descriptor,
      evidence.physical.fingerprint,
      evidence.physical,
      target_contract(evidence.asset)
    )
  end

  defp target_contract(%Asset{assurance: %{contract: %Contract{} = contract}}), do: contract
  defp target_contract(%Asset{}), do: nil

  defp mark_unknown(context, operation, lock, reason, opts) do
    result =
      store().mark_unknown(%MarkTargetRecoveryUnknown{
        workspace_context: context,
        command_id: command_id("unknown", operation.operation_id),
        operation_id: operation.operation_id,
        expected_version: operation.version,
        unknown_outcome: %{reason_code: reason},
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
      })

    release_lock(context, operation, lock)
    result
  end

  defp release_lock(context, operation, %TargetOperationLock{} = lock) do
    lock_store().release_many(%ReleaseTargetOperationLocks{
      workspace_context: context,
      command_id: command_id("release", operation.operation_id),
      operation_id: operation.operation_id,
      lease_owner: operation.operation_id,
      locks: [%{target_id: lock.target_id, fencing_token: lock.fencing_token}],
      occurred_at: DateTime.utc_now()
    })
  end

  defp marker_identity(marker) do
    {
      marker.target_id,
      relation_identity(marker.active_relation),
      marker.active_generation_id,
      marker.activation_operation_id,
      marker.activation_token
    }
  end

  defp marker_plan_identity(marker) when is_map(marker) do
    {
      field(marker, :target_id),
      relation_identity(field(marker, :active_relation)),
      field(marker, :active_generation_id),
      field(marker, :activation_operation_id),
      field(marker, :activation_token)
    }
  end

  defp marker_plan_identity(_marker), do: nil

  defp marker_map(marker) do
    %{
      target_id: marker.target_id,
      active_relation: Map.from_struct(marker.active_relation),
      active_generation_id: marker.active_generation_id,
      activation_operation_id: marker.activation_operation_id,
      activation_token: marker.activation_token,
      activated_at: DateTime.to_iso8601(marker.activated_at)
    }
  end

  defp recovery_token(operation),
    do: "target-recovery-token:" <> digest([operation.operation_id, operation.plan_hash])

  defp command_id(action, value), do: "target-recovery:#{action}:" <> digest([value])

  defp digest(parts) do
    parts
    |> Enum.intersperse(<<0>>)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp recovery_id,
    do: "target_recovery_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

  defp plan_expired?(payload) do
    case field(payload, :expires_at) do
      %DateTime{} = expires_at -> DateTime.compare(expires_at, DateTime.utc_now()) != :gt
      value when is_binary(value) -> expired_iso8601?(value)
      _invalid -> true
    end
  end

  defp persisted_plan(plan, %{idempotency_replay?: false}), do: plan

  defp persisted_plan(_plan, %TargetRecoveryOperation{} = operation) do
    persisted_plan(operation)
  end

  defp persisted_plan(%TargetRecoveryOperation{} = operation) do
    %Plan{
      plan_id: operation.operation_id,
      plan_hash: operation.plan_hash,
      expires_at: plan_expiry(operation.plan_payload),
      payload: operation.plan_payload,
      idempotency_replay?: true
    }
  end

  defp plan_expiry(payload) do
    case field(payload, :expires_at) do
      %DateTime{} = expires_at ->
        expires_at

      value when is_binary(value) ->
        {:ok, expires_at, 0} = DateTime.from_iso8601(value)
        expires_at
    end
  end

  defp expired_iso8601?(value) do
    case DateTime.from_iso8601(value) do
      {:ok, expires_at, 0} -> DateTime.compare(expires_at, DateTime.utc_now()) != :gt
      _invalid -> true
    end
  end

  defp authorize_operator(%WorkspaceContext{roles: roles}) do
    if Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator])),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace operator authority required")}
  end

  defp authorize_admin(%WorkspaceContext{roles: roles}) do
    if Enum.any?(roles, &(&1 in [:workspace_admin, :platform_operator])),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace admin authority required")}
  end

  defp authorize_read(%WorkspaceContext{roles: []}),
    do: {:error, Error.new(:forbidden, "workspace authority required")}

  defp authorize_read(%WorkspaceContext{}), do: :ok

  defp validate_reason(reason) do
    if byte_size(reason) in 1..4096,
      do: :ok,
      else: {:error, :target_recovery_reason_required}
  end

  defp validate_datetime(%DateTime{}), do: :ok
  defp validate_datetime(_invalid), do: {:error, :invalid_target_recovery_evaluated_at}

  defp validate_plan_options(opts) do
    allowed = [:evaluated_at, :operation_id, :idempotency_key, :session_id, :occurred_at]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_target_recovery_options}
  end

  defp validate_command_options(opts) do
    allowed = [:lease_duration_ms, :occurred_at]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_target_recovery_options}
  end

  defp stale_error do
    Error.new(:conflict, "target recovery plan is stale",
      details: %{reason_code: "target_recovery_plan_stale"}
    )
  end

  defp inspect_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp inspect_reason({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  defp inspect_reason(%{type: type}) when is_atom(type), do: Atom.to_string(type)
  defp inspect_reason(_reason), do: "target_recovery_failed"

  defp store, do: Persistence.stores().target_recovery
  defp lock_store, do: Persistence.stores().target_operation_locks
  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp relation_identity(relation) when is_map(relation) do
    %{
      connection: relation_value(relation, :connection),
      catalog: relation_value(relation, :catalog),
      schema: relation_value(relation, :schema),
      name: relation_value(relation, :name)
    }
  end

  defp relation_identity(_relation), do: nil

  defp relation_value(relation, key) do
    case field(relation, key) do
      value when is_atom(value) -> Atom.to_string(value)
      value -> value
    end
  end
end
