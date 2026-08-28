defmodule FavnOrchestrator.Rebuilds do
  require Logger

  @moduledoc "Manual, generation-safe rebuild planning and lifecycle commands."

  alias Favn.Coverage.Expected
  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.GenerationMarkerReadResult
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index, as: ManifestIndex
  alias Favn.Manifest.PlanningIndex
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.TargetGenerationRelation
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TimePeriod
  alias Favn.Window.Anchor
  alias Favn.Window.Selection
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestStore.Lease
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.MemoryCapacity.Error, as: MemoryError
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.BeginRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.CreateRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanAction
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanItem
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildReconciliation
  alias FavnOrchestrator.Persistence.Commands.RetryRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.StartRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems
  alias FavnOrchestrator.Persistence.Queries.PageRebuildOperations
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuild.Plan
  alias FavnOrchestrator.Rebuild.RuntimeInputs, as: RebuildRuntimeInputs
  alias FavnOrchestrator.Rebuilds.ItemDigest
  alias FavnOrchestrator.Rebuild.Telemetry
  alias FavnOrchestrator.TargetGenerations

  @plan_ttl_seconds 3_600
  @page_size 500
  @required_capabilities [
    :transactional_ddl,
    :isolated_candidates,
    :physical_inspection,
    :atomic_swap,
    :marker_reconciliation,
    :idempotent_discard
  ]

  @doc "Creates and persists an immutable rebuild plan for operator review."
  @spec plan(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, Plan.t()} | {:error, term()}
  def plan(%WorkspaceContext{} = context, target_id, reason, opts \\ [])
      when is_binary(target_id) and is_binary(reason) and is_list(opts) do
    Telemetry.plan(context, target_id, fn ->
      with :ok <- authorize_plan(context),
           :ok <- validate_plan_options(opts),
           :ok <- validate_reason(reason) do
        operation_id = plan_operation_id(context, opts)

        case existing_plan(context, operation_id, target_id, reason, opts) do
          {:ok, plan} ->
            {:ok, plan}

          {:resume, operation} ->
            planning_worker().ensure_and_await(context, operation)

          :missing ->
            create_plan(
              context,
              target_id,
              reason,
              Keyword.put(opts, :operation_id, operation_id)
            )

          {:error, _reason} = error ->
            error
        end
      end
    end)
  end

  defp create_plan(context, target_id, reason, opts) do
    with evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
         :ok <- validate_datetime(evaluated_at),
         {:ok, runtime, version, root, lease} <- active_asset(context, target_id) do
      try do
        with :ok <- persisted_sql_target(root) do
          with_rebuild_indexes(version, lease, fn index, _execution_index ->
            with {:ok, descendants} <- PlanningIndex.transitive_downstream_of(index, root.ref),
                 :ok <- validate_empty_rebuild(root, opts),
                 refs <- rebuild_refs(index, root.ref, descendants, opts),
                 {:ok, bindings} <- target_bindings(context, index, refs),
                 :ok <- validate_affected_bindings(index, refs, bindings),
                 {:ok, root_binding} <- rebuildable_binding(bindings, target_id),
                 {:ok, operation} <-
                   persist_planning_continuation(
                     context,
                     runtime,
                     version,
                     root,
                     root_binding,
                     reason,
                     evaluated_at,
                     opts
                   ) do
              planning_worker().ensure_and_await(context, operation)
            end
          end)
        end
      after
        ManifestStore.release_manifest(lease)
      end
    end
  end

  @doc false
  @spec resume_planning(WorkspaceContext.t(), RebuildOperation.t()) ::
          {:ok, Plan.t()} | {:error, term()}
  def resume_planning(
        %WorkspaceContext{} = _context,
        %RebuildOperation{state: :planned} = operation
      ),
      do: {:ok, plan_from_operation(operation, true)}

  def resume_planning(
        %WorkspaceContext{} = context,
        %RebuildOperation{state: :planning} = operation
      ) do
    opts = [
      operation_id: operation.operation_id,
      idempotency_key: operation.idempotency_key,
      evaluated_at: operation.evaluated_at,
      actor_id: operation.actor_id,
      session_id: operation.session_id,
      occurred_at: DateTime.utc_now(),
      combine_windows: field(operation.plan_payload, :combine_windows, true),
      empty: field(operation.plan_payload, :empty, false)
    ]

    with {:ok, runtime, version, root, lease} <- frozen_planning_asset(context, operation) do
      try do
        with :ok <- persisted_sql_target(root) do
          with_rebuild_indexes(version, lease, fn index, execution_index ->
            with {:ok, descendants} <- PlanningIndex.transitive_downstream_of(index, root.ref),
                 :ok <- validate_empty_rebuild(root, opts),
                 refs <- rebuild_refs(index, root.ref, descendants, opts),
                 {:ok, bindings} <- target_bindings(context, index, refs),
                 {:ok, identities} <- TargetGenerations.for_reads(context, index.assets_by_ref),
                 :ok <- validate_affected_bindings(index, refs, bindings),
                 {:ok, root_binding} <- rebuildable_binding(bindings, operation.root_target_id),
                 :ok <- exact_planning_binding(operation, root_binding),
                 {:ok, capability_snapshots} <-
                   capabilities(context, version, index, refs, operation.operation_id),
                 :ok <-
                   validate_live_bindings(
                     context,
                     version,
                     index,
                     refs,
                     bindings,
                     operation.operation_id
                   ),
                 {:ok, draft} <-
                   build_draft(
                     context,
                     runtime,
                     version,
                     index,
                     execution_index,
                     root,
                     root_binding,
                     bindings,
                     identities,
                     capability_snapshots,
                     refs,
                     operation.reason,
                     operation.evaluated_at,
                     opts
                   ),
                 {:ok, persisted} <- persist_plan(context, draft, opts) do
              {:ok, plan_from_operation(persisted, false)}
            end
          end)
        end
      after
        ManifestStore.release_manifest(lease)
      end
    end
  end

  def resume_planning(%WorkspaceContext{}, %RebuildOperation{}),
    do: {:error, Error.new(:conflict, "rebuild operation is not awaiting planning")}

  defp existing_plan(context, operation_id, target_id, reason, opts) do
    requested_idempotency_key = Keyword.get(opts, :idempotency_key, operation_id)
    requested_evaluated_at = Keyword.get(opts, :evaluated_at)

    case store().get(%GetRebuild{workspace_context: context, operation_id: operation_id}) do
      {:ok, operation} ->
        if operation.root_target_id == target_id and operation.reason == reason and
             operation.idempotency_key == requested_idempotency_key and
             field(operation.plan_payload, :combine_windows, true) ==
               Keyword.get(opts, :combine_windows, true) and
             field(operation.plan_payload, :empty, false) == Keyword.get(opts, :empty, false) and
             (is_nil(requested_evaluated_at) or
                operation.evaluated_at == requested_evaluated_at) do
          if operation.state == :planning do
            {:resume, operation}
          else
            {:ok, plan_from_operation(operation, true)}
          end
        else
          {:error,
           Error.new(:conflict, "rebuild plan identity has different request content",
             details: %{reason_code: "idempotency_conflict"}
           )}
        end

      {:error, %Error{kind: :not_found}} ->
        :missing

      {:error, _reason} = error ->
        error
    end
  end

  defp persist_planning_continuation(
         context,
         runtime,
         version,
         root,
         root_binding,
         reason,
         evaluated_at,
         opts
       ) do
    operation_id = Keyword.fetch!(opts, :operation_id)
    expires_at = DateTime.add(evaluated_at, @plan_ttl_seconds, :second)

    planning =
      Plan.new(operation_id, expires_at, %{
        schema_version: 1,
        status: :planning,
        workspace_id: context.workspace_id,
        operation_id: operation_id,
        root_target_id: root.target_descriptor.target_id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        deployment_id: runtime.deployment_id,
        evaluated_at: evaluated_at,
        reason: reason,
        active_generation_id: root_binding.active_generation_id,
        combine_windows: Keyword.get(opts, :combine_windows, true),
        empty: Keyword.get(opts, :empty, false)
      })

    store().begin_plan(%BeginRebuildPlan{
      workspace_context: context,
      command_id: command_id("begin-plan", operation_id <> ":" <> planning.plan_hash),
      operation_id: operation_id,
      root_target_id: root.target_descriptor.target_id,
      manifest_version_id: version.manifest_version_id,
      active_generation_id: root_binding.active_generation_id,
      planning_hash: planning.plan_hash,
      planning_payload: planning.payload,
      actor_id: Keyword.get(opts, :actor_id, context.principal_id),
      session_id: Keyword.get(opts, :session_id, context.request_id),
      reason: reason,
      idempotency_key: Keyword.get(opts, :idempotency_key, operation_id),
      evaluated_at: evaluated_at,
      occurred_at: Keyword.get(opts, :occurred_at, evaluated_at),
      idempotency: Keyword.get(opts, :idempotency)
    })
  end

  defp frozen_planning_asset(context, operation) do
    payload = operation.plan_payload
    deployment_id = field(payload, :deployment_id)

    result =
      if is_binary(deployment_id) do
        with {:ok, lease} <-
               ManifestStore.checkout_deployment_manifest(
                 context,
                 deployment_id,
                 operation.manifest_version_id
               ) do
          result =
            with true <- lease.version.content_hash == field(payload, :manifest_content_hash),
                 {:ok, root} <-
                   ManifestTarget.resolve_asset(lease.version, operation.root_target_id) do
              runtime = %RuntimeState{
                workspace_id: context.workspace_id,
                deployment_id: deployment_id,
                manifest_version_id: operation.manifest_version_id,
                revision: 0,
                manifest_content_hash: lease.version.content_hash
              }

              {:ok, runtime, lease.version, root, lease}
            else
              false ->
                {:error,
                 Error.new(:conflict, "frozen rebuild planning manifest identity changed")}

              {:error, _reason} = error ->
                error
            end

          case result do
            {:ok, _runtime, _version, _root, _lease} = success -> success
            {:error, _reason} = error -> release_manifest_error(lease, error)
          end
        end
      else
        {:error, Error.new(:conflict, "frozen rebuild planning manifest identity changed")}
      end

    case result do
      {:ok, _runtime, _version, _root, _lease} = success ->
        success

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, %MemoryError{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:conflict, "frozen rebuild planning snapshot is unavailable",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp exact_planning_binding(operation, root_binding) do
    if root_binding.active_generation_id == operation.active_generation_id do
      :ok
    else
      {:error, Error.new(:conflict, "rebuild planning target binding changed")}
    end
  end

  defp plan_from_operation(operation, replay?) do
    %Plan{
      plan_id: operation.operation_id,
      plan_hash: operation.plan_hash,
      expires_at: decode_datetime!(field(operation.plan_payload, :expires_at)),
      payload: operation.plan_payload,
      idempotency_replay?: replay?
    }
  end

  @doc "Approves an exact plan after revalidating every pinned control/data-plane input."
  @spec start(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, RebuildOperation.t()} | {:error, term()}
  def start(%WorkspaceContext{} = context, plan_id, plan_hash, opts \\ [])
      when is_binary(plan_id) and is_binary(plan_hash) and is_list(opts) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts),
         {:ok, operation} <- get(context, plan_id) do
      start_or_replay(context, operation, plan_hash, opts)
    end
  end

  defp start_or_replay(
         context,
         %RebuildOperation{state: state} = operation,
         plan_hash,
         opts
       )
       when state != :planned do
    case Keyword.get(opts, :idempotency) do
      %CommandIdempotency{} -> persist_start(context, operation, plan_hash, opts)
      nil -> {:error, Error.new(:conflict, "rebuild operation is no longer planned")}
    end
  end

  defp start_or_replay(context, operation, plan_hash, opts) do
    with :ok <- exact_plan(operation, plan_hash),
         :ok <- plan_not_expired(operation),
         :ok <- revalidate_plan(context, operation),
         {:ok, locks} <- acquire_plan_locks(context, operation, opts) do
      case persist_start(context, operation, plan_hash, opts) do
        {:ok, started} -> {:ok, started}
        {:error, reason} -> release_plan_locks(context, operation, locks, reason)
      end
    end
  end

  defp persist_start(context, operation, plan_hash, opts) do
    store().start_operation(%StartRebuildOperation{
      workspace_context: context,
      command_id: command_id("start", operation.operation_id <> ":" <> plan_hash),
      operation_id: operation.operation_id,
      plan_hash: plan_hash,
      expected_version: operation.version,
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
      idempotency: Keyword.get(opts, :idempotency)
    })
  end

  @doc "Returns one authoritative rebuild operation."
  @spec get(WorkspaceContext.t(), String.t()) :: {:ok, RebuildOperation.t()} | {:error, term()}
  def get(%WorkspaceContext{} = context, operation_id) when is_binary(operation_id) do
    with :ok <- authorize_read(context) do
      store().get(%GetRebuild{workspace_context: context, operation_id: operation_id})
    end
  end

  @doc "Pages authoritative rebuild operations newest first."
  @spec page(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with :ok <- authorize_read(context),
         :ok <- validate_page_options(opts) do
      store().page_operations(%PageRebuildOperations{
        workspace_context: context,
        state: Keyword.get(opts, :state),
        after: Keyword.get(opts, :after),
        limit: Keyword.get(opts, :limit, 100)
      })
    end
  end

  @doc "Pages immutable logical items for one rebuild operation."
  @spec page_items(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_items(%WorkspaceContext{} = context, operation_id, opts \\ [])
      when is_binary(operation_id) and is_list(opts) do
    with :ok <- authorize_read(context),
         :ok <- validate_item_page_options(opts) do
      store().page_items(%PageRebuildItems{
        workspace_context: context,
        operation_id: operation_id,
        target_id: Keyword.get(opts, :target_id),
        status: Keyword.get(opts, :status),
        after: Keyword.get(opts, :after),
        limit: Keyword.get(opts, :limit, 100)
      })
    end
  end

  @doc "Requests cancellation without assuming that a dispatched activation rolled back."
  @spec cancel(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, RebuildOperation.t()} | {:error, term()}
  def cancel(%WorkspaceContext{} = context, operation_id, reason, opts \\ []) do
    with :ok <- authorize_admin(context),
         :ok <- validate_reason(reason),
         :ok <- validate_command_options(opts) do
      store().request_cancellation(%RequestRebuildCancellation{
        workspace_context: context,
        command_id:
          Keyword.get(opts, :command_id, command_id("cancel", operation_id <> ":" <> reason)),
        operation_id: operation_id,
        reason: reason,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
        idempotency: Keyword.get(opts, :idempotency)
      })
    end
  end

  @doc "Requeues only explicitly safe failed work from the same immutable plan."
  @spec retry(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, RebuildOperation.t()} | {:error, term()}
  def retry(%WorkspaceContext{} = context, operation_id, plan_hash, opts \\ []) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts),
         {:ok, operation} <- get(context, operation_id),
         :ok <- exact_plan(operation, plan_hash),
         :ok <- reject_combined_append_retry(operation),
         :ok <- revalidate_plan(context, operation) do
      store().retry_operation(%RetryRebuildOperation{
        workspace_context: context,
        command_id:
          Keyword.get(opts, :command_id, command_id("retry", operation_id <> ":" <> plan_hash)),
        operation_id: operation_id,
        plan_hash: plan_hash,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
        idempotency: Keyword.get(opts, :idempotency)
      })
    end
  end

  @doc "Requests explicit reconciliation of one durable unknown rebuild outcome."
  @spec reconcile(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, RebuildOperation.t()} | {:error, term()}
  def reconcile(%WorkspaceContext{} = context, operation_id, opts \\ [])
      when is_binary(operation_id) and is_list(opts) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts) do
      store().request_reconciliation(%RequestRebuildReconciliation{
        workspace_context: context,
        command_id: Keyword.get(opts, :command_id, command_id("reconcile", operation_id)),
        operation_id: operation_id,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
        idempotency: Keyword.get(opts, :idempotency)
      })
    end
  end

  defp with_rebuild_indexes(version, fun), do: with_rebuild_indexes(version, [], fun)

  defp with_rebuild_indexes(version, %Lease{} = lease, fun) do
    with_rebuild_indexes(
      version,
      [memory_capacity_token: lease.capacity_token],
      fun
    )
  end

  defp with_rebuild_indexes(version, opts, fun) when is_list(opts) do
    ManifestStore.with_index(version, opts, fn
      %ManifestIndex{planning_index: %PlanningIndex{} = index} = execution_index ->
        fun.(index, execution_index)
    end)
  end

  defp build_draft(
         context,
         runtime,
         version,
         index,
         execution_index,
         root,
         root_binding,
         bindings,
         identities,
         capabilities,
         refs,
         reason,
         evaluated_at,
         opts
       ) do
    operation_id = Keyword.fetch!(opts, :operation_id)
    expires_at = DateTime.add(evaluated_at, @plan_ttl_seconds, :second)

    with {:ok, actions, items} <-
           build_actions(
             version,
             index,
             root,
             bindings,
             identities,
             capabilities,
             refs,
             operation_id,
             evaluated_at,
             opts
           ),
         {:ok, items} <-
           freeze_runtime_inputs(
             context,
             runtime,
             version,
             execution_index,
             index,
             actions,
             items,
             bindings,
             capabilities,
             evaluated_at,
             operation_id
           ) do
      payload = %{
        schema_version: 1,
        workspace_id: context.workspace_id,
        operation_id: operation_id,
        root_target_id: root.target_descriptor.target_id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        deployment_id: runtime.deployment_id,
        evaluated_at: evaluated_at,
        reason: reason,
        combine_windows: Keyword.get(opts, :combine_windows, true),
        empty: Keyword.get(opts, :empty, false),
        execution_mode: rebuild_execution_mode(opts),
        combined_append: combined_append?(index, actions, items),
        logical_window_count: logical_window_count(root, evaluated_at),
        physical_execution_count:
          Enum.count(items, &(&1.target_id == root.target_descriptor.target_id)),
        coverage: coverage_payload(root.coverage),
        evaluated_range:
          if(Keyword.get(opts, :empty, false),
            do: %{start_at: nil, end_at: nil},
            else: evaluated_range(items, root.target_descriptor.target_id)
          ),
        active_generation_id: root_binding.active_generation_id,
        candidate_generation_id: root_candidate_id(actions, root.target_descriptor.target_id),
        binding_snapshot: binding_snapshot(bindings),
        capabilities: capabilities,
        execution_packages:
          Map.new(actionable_refs(index, refs), fn ref ->
            asset = Map.fetch!(index.assets_by_ref, ref)
            {target_id(asset), asset.execution_package_hash}
          end),
        assurance_expectations:
          Map.new(actionable_refs(index, refs), fn ref ->
            asset = Map.fetch!(index.assets_by_ref, ref)
            {target_id(asset), assurance_expectation(asset)}
          end),
        actions: Enum.map(actions, &action_payload/1),
        item_count: length(items),
        items_digest: ItemDigest.hash(items)
      }

      plan = Plan.new(operation_id, expires_at, payload)
      {:ok, %{plan: plan, actions: actions, items: items, root_binding: root_binding}}
    else
      {:error, _reason} = error -> error
    end
  end

  defp assurance_expectation(%Asset{assurance: assurance}) when is_map(assurance) do
    %{
      contract_required: not is_nil(field(assurance, :contract)),
      checks:
        assurance
        |> field(:checks)
        |> List.wrap()
        |> Enum.map(fn check ->
          %{
            name: check |> field(:name) |> to_string(),
            origin: check |> field(:origin) |> to_string(),
            claim_id: field(check, :claim_id),
            phase: check |> field(:at) |> to_string()
          }
        end)
    }
  end

  defp assurance_expectation(%Asset{}), do: %{contract_required: false, checks: []}

  defp logical_window_count(%Asset{window: nil}, _evaluated_at), do: 1

  defp logical_window_count(%Asset{} = asset, evaluated_at) do
    case expected_anchors(asset, evaluated_at) do
      {:ok, anchors} -> length(anchors)
      {:error, _reason} -> 0
    end
  end

  defp combined_append?(index, actions, items) do
    ranged_targets =
      items
      |> Enum.filter(&combined_item?/1)
      |> MapSet.new(&field(&1, :target_id))

    Enum.any?(actions, fn action ->
      target_id = field(action, :target_id)

      with true <- MapSet.member?(ranged_targets, target_id),
           {_ref, asset} <-
             Enum.find(index.assets_by_ref, fn {_ref, asset} ->
               asset.target_descriptor && asset.target_descriptor.target_id == target_id
             end) do
        field(asset.target_descriptor.write_semantics, :strategy) in [:append, "append"]
      else
        _other -> false
      end
    end)
  end

  defp reject_combined_append_retry(operation) do
    if field(operation.plan_payload, :combined_append, false),
      do:
        {:error,
         Error.new(:conflict, "combined append rebuild requires a new plan and candidate")},
      else: :ok
  end

  defp rebuild_execution_mode(opts) do
    cond do
      Keyword.get(opts, :empty, false) -> :empty
      Keyword.get(opts, :combine_windows, true) -> :combined
      true -> :separate
    end
  end

  defp build_actions(
         version,
         index,
         root,
         bindings,
         identities,
         capabilities,
         refs,
         operation_id,
         evaluated_at,
         opts
       ) do
    Enum.reduce_while(Enum.with_index(actionable_refs(index, refs)), {:ok, [], [], identities}, fn
      {ref, ordinal}, {:ok, actions, items, planned_identities} ->
        asset = Map.fetch!(index.assets_by_ref, ref)
        action_kind = action_kind(index, root, asset)

        with {:ok, runner_binding} <- OperationRunnerTasks.binding(version, asset.ref),
             {:ok, candidate} <- candidate(action_kind, asset, capabilities, operation_id),
             {:ok, action_items} <-
               action_items(action_kind, root, asset, candidate, evaluated_at, opts),
             {:ok, input_pins} <- input_pins(asset, planned_identities, bindings),
             true <- length(action_items) <= Expected.max_windows() do
          action = %RebuildPlanAction{
            target_id: target_id(asset),
            ordinal: ordinal,
            action: action_kind,
            reason: action_reason(action_kind, root, asset, Map.get(bindings, target_id(asset))),
            upstream_impact: upstream_impact(index, ref),
            mapping_proof: mapping_proof(action_kind, root, asset),
            pinned_input_generation_ids: input_pins,
            runner_pool: runner_binding.runner_pool,
            required_runner_release_id: runner_binding.required_runner_release_id,
            candidate_generation: candidate,
            status: :planned
          }

          {:cont,
           {:ok, [action | actions], items ++ action_items,
            put_planned_output(
              planned_identities,
              asset,
              action_kind,
              candidate,
              operation_id
            )}}
        else
          false -> {:halt, {:error, :coverage_window_limit_exceeded}}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
    |> then(fn
      {:ok, actions, items, _planned_identities} -> {:ok, Enum.reverse(actions), items}
      error -> error
    end)
  end

  defp action_kind(_index, %Asset{ref: ref}, %Asset{ref: ref}), do: :rebuild
  defp action_kind(_index, _root, %Asset{target_descriptor: nil}), do: :no_action

  defp action_kind(index, root, asset) do
    direct? = MapSet.member?(Map.fetch!(index.upstream, asset.ref), root.ref)
    same_window? = same_window_identity?(root, asset)
    replay_safe? = replay_safe_window_write?(asset.target_descriptor.write_semantics)

    if direct? and same_window? and replay_safe?, do: :backfill, else: :rebuild
  end

  defp candidate(:rebuild, asset, capabilities, operation_id) do
    target_id = target_id(asset)
    candidate_generation_id = candidate_generation_id(operation_id, target_id)
    max_identifier_bytes = get_in(capabilities, [target_id, :max_identifier_bytes])
    logical_relation = RelationRef.new!(asset.relation)

    candidate_relation =
      TargetGenerationRelation.candidate(
        logical_relation,
        candidate_generation_id,
        max_identifier_bytes
      )

    {:ok,
     %{
       target_generation_id: candidate_generation_id,
       descriptor_hash: asset.target_descriptor.descriptor_hash,
       logical_relation: Map.from_struct(logical_relation),
       physical_relation: Map.from_struct(candidate_relation)
     }}
  rescue
    ArgumentError -> {:error, :invalid_rebuild_relation}
  end

  defp candidate(_action, _asset, _capabilities, _operation_id), do: {:ok, nil}

  defp action_items(:no_action, _root, _asset, _candidate, _evaluated_at, _opts), do: {:ok, []}

  defp action_items(:backfill, root, asset, _candidate, evaluated_at, opts) do
    with {:ok, anchors} <- expected_anchors(root, evaluated_at) do
      {:ok, build_window_items(asset, anchors, nil, Keyword.get(opts, :combine_windows, true))}
    end
  end

  defp action_items(
         :rebuild,
         _root,
         %Asset{window: nil} = asset,
         candidate,
         _evaluated_at,
         _opts
       ) do
    {:ok,
     [
       %RebuildPlanItem{
         target_id: target_id(asset),
         item_id: item_id(target_id(asset), "full_load"),
         ordinal: 0,
         work_kind: :full_load,
         window_key: "full_load",
         window_start: nil,
         window_end: nil,
         candidate_generation_id: candidate.target_generation_id
       }
     ]}
  end

  defp action_items(:rebuild, root, asset, candidate, evaluated_at, opts) do
    with {:ok, anchors} <- expected_anchors(asset, evaluated_at) do
      items =
        cond do
          Keyword.get(opts, :empty, false) and asset.ref == root.ref ->
            [empty_generation_item(asset, candidate.target_generation_id)]

          anchors == [] ->
            [empty_generation_item(asset, candidate.target_generation_id)]

          true ->
            build_window_items(
              asset,
              anchors,
              candidate.target_generation_id,
              Keyword.get(opts, :combine_windows, true)
            )
        end

      {:ok, items}
    end
  end

  defp expected_anchors(%Asset{coverage: nil}, _evaluated_at),
    do: {:error, :coverage_required_for_windowed_rebuild}

  defp expected_anchors(%Asset{coverage: coverage}, evaluated_at) do
    with {:ok, evaluation} <- Expected.evaluate(coverage, evaluated_at) do
      collect_expected(evaluation, nil, [])
    end
  end

  defp collect_expected(evaluation, after_key, acc) do
    with {:ok, page} <- Expected.page(evaluation, after_key, @page_size) do
      next = acc ++ page.items

      if page.has_more?,
        do: collect_expected(evaluation, page.next_after, next),
        else: {:ok, next}
    end
  end

  defp build_window_items(
         asset,
         [_first, _second | _rest] = anchors,
         candidate_generation_id,
         true
       ) do
    first = hd(anchors)
    last = List.last(anchors)

    range_key =
      Favn.Window.Key.new_range!(first.kind, first.start_at, last.end_at, first.timezone)

    window_key = FreshnessKey.window!(range_key)

    [
      %RebuildPlanItem{
        target_id: target_id(asset),
        item_id: item_id(target_id(asset), window_key),
        ordinal: 0,
        work_kind: :window,
        window_key: window_key,
        window_start: first.start_at,
        window_end: last.end_at,
        candidate_generation_id: candidate_generation_id
      }
    ]
  end

  defp build_window_items(asset, anchors, candidate_generation_id, _combine_windows) do
    anchors
    |> Enum.with_index()
    |> Enum.map(fn {anchor, ordinal} ->
      window_key = FreshnessKey.window!(anchor.key)

      %RebuildPlanItem{
        target_id: target_id(asset),
        item_id: item_id(target_id(asset), window_key),
        ordinal: ordinal,
        work_kind: :window,
        window_key: window_key,
        window_start: anchor.start_at,
        window_end: anchor.end_at,
        candidate_generation_id: candidate_generation_id
      }
    end)
  end

  defp empty_generation_item(asset, candidate_generation_id) do
    period = asset.coverage.effective_from
    start_at = TimePeriod.shift!(period.start_at, period.kind, -1)
    end_at = period.start_at
    anchor = Anchor.new!(period.kind, start_at, end_at, timezone: period.timezone)

    %RebuildPlanItem{
      target_id: target_id(asset),
      item_id: item_id(target_id(asset), "empty_generation:" <> inspect(anchor.key)),
      ordinal: 0,
      work_kind: :empty_generation,
      window_key: FreshnessKey.window!(anchor.key),
      window_start: start_at,
      window_end: end_at,
      candidate_generation_id: candidate_generation_id
    }
  end

  defp persist_plan(context, draft, opts) do
    payload = draft.plan.payload
    items = draft.items
    coverage_items = Enum.filter(items, &(&1.target_id == payload.root_target_id))
    coverage_start = coverage_items |> List.first() |> item_boundary(:window_start)
    coverage_end = coverage_items |> List.last() |> item_boundary(:window_end)

    store().create_plan(%CreateRebuildPlan{
      workspace_context: context,
      command_id: command_id("plan", draft.plan.plan_id <> ":" <> draft.plan.plan_hash),
      operation_id: draft.plan.plan_id,
      root_target_id: payload.root_target_id,
      manifest_version_id: payload.manifest_version_id,
      active_generation_id: payload.active_generation_id,
      candidate_generation_id: payload.candidate_generation_id,
      plan_hash: draft.plan.plan_hash,
      plan_payload: draft.plan.payload,
      actor_id: Keyword.get(opts, :actor_id, context.principal_id),
      session_id: Keyword.get(opts, :session_id, context.request_id),
      reason: payload.reason,
      idempotency_key: Keyword.get(opts, :idempotency_key, draft.plan.plan_id),
      evaluated_at: payload.evaluated_at,
      coverage_start: coverage_start,
      coverage_end: coverage_end,
      actions: draft.actions,
      items: items,
      occurred_at: Keyword.get(opts, :occurred_at, payload.evaluated_at),
      idempotency: nil
    })
  end

  defp revalidate_plan(context, operation) do
    payload = operation.plan_payload

    with {:ok, runtime} <- ManifestStore.get_runtime_state(context),
         :ok <-
           ensure_current(
             runtime.manifest_version_id == field(payload, :manifest_version_id),
             :active_manifest
           ),
         true <- true do
      ManifestStore.with_manifest(context, runtime.manifest_version_id, fn version ->
        with :ok <-
               ensure_current(
                 version.content_hash == field(payload, :manifest_content_hash),
                 :manifest_content
               ),
             {:ok, current_bindings} <-
               target_bindings_by_ids(context, snapshot_target_ids(payload)),
             :ok <-
               ensure_current(
                 binding_snapshot_matches?(current_bindings, payload, operation),
                 :target_bindings
               ),
             {:ok, current_capabilities} <-
               capabilities_for_payload(context, version, payload, operation.operation_id),
             :ok <-
               ensure_current(
                 canonical(current_capabilities) == field(payload, :capabilities),
                 :runner_capabilities
               ) do
          with_rebuild_indexes(version, fn index, execution_index ->
            with :ok <-
                   validate_live_bindings(
                     context,
                     version,
                     index,
                     payload_refs(index, payload),
                     current_bindings,
                     operation.operation_id
                   ),
                 {:ok, frozen_items} <- operation_items(context, operation.operation_id),
                 {:ok, current_items} <-
                   freeze_runtime_inputs(
                     context,
                     runtime,
                     version,
                     execution_index,
                     index,
                     operation.actions,
                     frozen_items,
                     current_bindings,
                     current_capabilities,
                     operation.evaluated_at,
                     operation.operation_id
                   ),
                 :ok <-
                   ensure_current(
                     ItemDigest.hash(current_items) == field(payload, :items_digest),
                     :runtime_inputs
                   ) do
              :ok
            end
          end)
        end
      end)
    else
      {:error, {:stale_rebuild_plan, reason}} ->
        Logger.warning("rebuild plan revalidation failed: #{reason}",
          operation_id: operation.operation_id,
          reason: reason
        )

        {:error, stale_plan_error()}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp ensure_current(true, _reason), do: :ok
  defp ensure_current(false, reason), do: {:error, {:stale_rebuild_plan, reason}}

  defp acquire_plan_locks(context, operation, opts) do
    target_ids = write_target_ids(operation.plan_payload)

    lock_store().acquire_many(%AcquireTargetOperationLocks{
      workspace_context: context,
      command_id: command_id("locks", operation.operation_id <> ":" <> operation.plan_hash),
      target_ids: target_ids,
      operation_id: operation.operation_id,
      operation_type: :rebuild,
      lease_owner: operation.operation_id,
      lease_duration_ms: Keyword.get(opts, :lease_duration_ms, 30_000),
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
    })
  end

  defp release_plan_locks(context, operation, locks, original_error) do
    refs = Enum.map(locks, &%{target_id: &1.target_id, fencing_token: &1.fencing_token})

    _ =
      lock_store().release_many(%ReleaseTargetOperationLocks{
        workspace_context: context,
        command_id: command_id("release", operation.operation_id <> ":start-failed"),
        operation_id: operation.operation_id,
        lease_owner: operation.operation_id,
        locks: refs,
        occurred_at: DateTime.utc_now()
      })

    {:error, original_error}
  end

  defp capabilities(context, version, index, refs, operation_id) do
    refs
    |> Enum.filter(&(Map.fetch!(index.assets_by_ref, &1).target_descriptor != nil))
    |> Enum.reduce_while({:ok, %{}}, fn ref, {:ok, acc} ->
      asset = Map.fetch!(index.assets_by_ref, ref)

      case generation_capabilities(context, version, asset, operation_id) do
        {:ok, snapshot} -> {:cont, {:ok, Map.put(acc, target_id(asset), snapshot)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp capabilities_for_payload(context, version, payload, operation_id) do
    target_ids = payload |> field(:capabilities) |> Map.keys()

    Enum.reduce_while(target_ids, {:ok, %{}}, fn target_id, {:ok, acc} ->
      with {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
           {:ok, snapshot} <- generation_capabilities(context, version, asset, operation_id) do
        {:cont, {:ok, Map.put(acc, target_id, snapshot)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp generation_capabilities(context, version, asset, operation_id) do
    payload = %GenerationCapabilitiesRequest{
      manifest: Version.identity(version),
      asset_ref: asset.ref
    }

    case OperationRunnerTasks.ensure_and_await(
           context,
           version,
           asset.ref,
           :generation_capabilities,
           payload,
           {:rebuild_capabilities, operation_id, target_id(asset)},
           operation_id: operation_id,
           rebuild_operation_id: operation_id
         ) do
      {:ok, %GenerationCapabilitiesResult{capabilities: capabilities}}
      when is_map(capabilities) ->
        missing = Enum.reject(@required_capabilities, &(field(capabilities, &1) == :supported))
        max_identifier_bytes = field(capabilities, :max_identifier_bytes)

        if missing == [] and is_integer(max_identifier_bytes) and max_identifier_bytes >= 48 do
          {:ok,
           Map.new(@required_capabilities ++ [:snapshots, :max_identifier_bytes], fn key ->
             {key, field(capabilities, key)}
           end)}
        else
          {:error,
           Error.new(:conflict, "target adapter does not support safe rebuilds",
             details: %{reason_code: "rebuild_not_supported", missing_capabilities: missing}
           )}
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_generation_capabilities}
    end
  end

  defp target_bindings(context, index, refs) do
    target_ids =
      refs
      |> Enum.flat_map(fn ref -> [ref | MapSet.to_list(Map.fetch!(index.upstream, ref))] end)
      |> Enum.uniq()
      |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
      |> Enum.filter(& &1.target_descriptor)
      |> Enum.map(&target_id/1)

    target_bindings_by_ids(context, target_ids)
  end

  defp target_bindings_by_ids(context, target_ids) do
    case Persistence.stores().target_generations.get_bindings(%GetTargetBindings{
           workspace_context: context,
           target_ids: Enum.sort(Enum.uniq(target_ids))
         }) do
      {:ok, bindings} -> {:ok, Map.new(bindings, &{&1.target_id, &1})}
      {:error, _reason} = error -> error
    end
  end

  defp rebuildable_binding(bindings, target_id) do
    case Map.get(bindings, target_id) do
      %{
        active_generation_id: generation_id,
        active_data_plane_marker: marker,
        active_physical_relation: relation,
        active_physical_fingerprint: fingerprint
      } = binding
      when is_binary(generation_id) and is_map(marker) and is_map(relation) and
             is_binary(fingerprint) ->
        if binding.compatibility_status in [:ready, :rebuild_available, :rebuild_required] do
          {:ok, binding}
        else
          {:error,
           Error.new(:conflict, "target state cannot be rebuilt safely",
             details: %{
               reason_code: rebuild_conflict_code(binding.compatibility_status),
               target_id: target_id
             }
           )}
        end

      _missing ->
        {:error, Error.new(:conflict, "target has no active generation marker")}
    end
  end

  defp validate_affected_bindings(index, refs, bindings) do
    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
    |> Enum.filter(& &1.target_descriptor)
    |> Enum.reduce_while(:ok, fn asset, :ok ->
      case rebuildable_binding(bindings, target_id(asset)) do
        {:ok, _binding} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp rebuild_conflict_code(:unexpected_drift), do: "target_drift"
  defp rebuild_conflict_code(:operator_decision), do: "operator_decision_required"
  defp rebuild_conflict_code(status), do: Atom.to_string(status)

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
        {:ok, asset} -> {:ok, runtime, lease.version, asset, lease}
        {:error, _reason} = error -> release_manifest_error(lease, error)
      end
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp release_manifest_error(lease, error) do
    :ok = ManifestStore.release_manifest(lease)
    error
  end

  defp persisted_sql_target(%Asset{type: :sql, target_descriptor: %TargetDescriptor{}}), do: :ok
  defp persisted_sql_target(_asset), do: {:error, :rebuild_target_not_supported}

  defp affected_refs(index, root_ref, descendants) do
    affected = MapSet.put(descendants, root_ref)
    Enum.filter(index.topo_order, &MapSet.member?(affected, &1))
  end

  defp input_pins(asset, identities, bindings) do
    Enum.reduce_while(asset.depends_on, {:ok, []}, fn ref, {:ok, pins} ->
      identity = Map.get(identities, ref)
      binding = identity && Map.get(bindings, identity.target_id)

      cond do
        is_nil(identity) ->
          {:halt, {:error, missing_input_generation_error(ref)}}

        Map.get(identity, :planned_rebuild_operation_id) ->
          pin =
            identity
            |> Map.put(:data_plane_marker, %{
              active_generation_id: identity.target_generation_id,
              activation_operation_id: identity.planned_rebuild_operation_id,
              source: "planned_rebuild_output"
            })
            |> Map.put(:binding_version, nil)

          {:cont, {:ok, [pin | pins]}}

        is_binary(identity.target_generation_id) and
            (is_nil(binding) or not is_map(binding.active_data_plane_marker) or
               not is_integer(binding.version) or not is_map(identity.physical_relation)) ->
          {:halt, {:error, missing_input_generation_error(ref)}}

        true ->
          pin =
            identity
            |> Map.put(:data_plane_marker, binding && binding.active_data_plane_marker)
            |> Map.put(:binding_version, binding && binding.version)

          {:cont, {:ok, [pin | pins]}}
      end
    end)
    |> then(fn
      {:ok, pins} -> {:ok, Enum.sort_by(pins, & &1.target_id)}
      {:error, _reason} = error -> error
    end)
  end

  defp put_planned_output(identities, asset, :rebuild, candidate, operation_id) do
    Map.put(identities, asset.ref, %{
      target_id: target_id(asset),
      evidence_generation_id: candidate.target_generation_id,
      target_generation_id: candidate.target_generation_id,
      physical_relation: candidate.logical_relation,
      planned_rebuild_operation_id: operation_id
    })
  end

  defp put_planned_output(identities, _asset, _action, _candidate, _operation_id),
    do: identities

  defp missing_input_generation_error(ref) do
    Error.new(:conflict, "rebuild input has no complete active generation",
      details: %{reason_code: "rebuild_input_generation_unavailable", asset_ref: ref_value(ref)}
    )
  end

  defp upstream_impact(index, ref) do
    %{
      direct_upstream_refs:
        index.upstream
        |> Map.fetch!(ref)
        |> Enum.map(&ref_value/1)
        |> Enum.sort_by(&inspect/1)
    }
  end

  defp mapping_proof(:backfill, root, asset) do
    %{
      kind: "direct_equal_window",
      source_target_id: target_id(root),
      destination_target_id: target_id(asset),
      window_identity: asset.target_descriptor.window_identity,
      write_semantics: asset.target_descriptor.write_semantics,
      partition_local: true
    }
  end

  defp mapping_proof(_action, _root, _asset), do: nil

  defp action_reason(:rebuild, root, asset, binding) when root.ref == asset.ref,
    do: %{reason_code: binding.reason_code, compatibility_diff: binding.compatibility_diff}

  defp action_reason(:rebuild, _root, _asset, _binding),
    do: %{reason_code: "conservative_downstream_rebuild"}

  defp action_reason(:backfill, _root, _asset, _binding),
    do: %{reason_code: "proven_direct_equal_window_impact"}

  defp action_reason(:no_action, _root, _asset, _binding),
    do: %{reason_code: "non_persisted_descendant_has_no_durable_target"}

  defp same_window_identity?(root, asset) do
    root.target_descriptor.window_identity != nil and
      root.target_descriptor.window_identity == asset.target_descriptor.window_identity
  end

  defp replay_safe_window_write?(write_semantics) when is_map(write_semantics) do
    field(write_semantics, :mode) == "incremental" and
      field(write_semantics, :strategy) == "delete_insert" and
      is_binary(field(write_semantics, :window_column)) and
      field(write_semantics, :window_column) != ""
  end

  defp replay_safe_window_write?(_write_semantics), do: false

  defp binding_snapshot_matches?(current_bindings, payload, operation) do
    frozen = field(payload, :binding_snapshot)
    actions = Map.new(operation.actions, &{&1.target_id, &1})

    Enum.all?(current_bindings, fn {target_id, binding} ->
      frozen_binding = field(frozen, target_id)
      action = Map.get(actions, target_id)

      if activated_by_operation?(binding, action, operation.operation_id, frozen_binding) do
        true
      else
        canonical(canonical_binding_snapshot(%{target_id => binding})) ==
          canonical(%{target_id => frozen_binding})
      end
    end) and map_size(current_bindings) == map_size(frozen)
  end

  defp activated_by_operation?(binding, action, operation_id, frozen) do
    marker = binding.active_data_plane_marker

    match?(%{action: :rebuild, status: :succeeded}, action) and
      not is_nil(action.activated_at) and
      binding.active_generation_id == action.candidate_generation_id and
      binding.desired_manifest_id == field(frozen, :desired_manifest_id) and
      binding.desired_descriptor_hash == field(frozen, :desired_descriptor_hash) and
      binding.compatibility_status == :ready and
      canonical(binding.active_physical_relation) ==
        canonical(field(frozen, :active_physical_relation)) and
      is_binary(binding.active_physical_fingerprint) and
      is_integer(binding.version) and binding.version > field(frozen, :version) and
      is_map(marker) and field(marker, :active_generation_id) == action.candidate_generation_id and
      field(marker, :activation_operation_id) == operation_id and
      field(marker, :activation_token) == field(action.activation_intent, :activation_token)
  end

  defp payload_refs(index, payload) do
    payload
    |> field(:binding_snapshot)
    |> Map.keys()
    |> MapSet.new()
    |> then(fn target_ids ->
      Enum.filter(index.topo_order, fn ref ->
        asset = Map.fetch!(index.assets_by_ref, ref)
        asset.target_descriptor && MapSet.member?(target_ids, target_id(asset))
      end)
    end)
  end

  defp validate_live_bindings(context, version, index, refs, bindings, operation_id) do
    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
    |> Enum.filter(& &1.target_descriptor)
    |> Enum.reduce_while(:ok, fn asset, :ok ->
      binding = Map.fetch!(bindings, target_id(asset))

      if is_binary(binding.active_generation_id) do
        payload = %GenerationMarkerReadRequest{
          manifest: Version.identity(version),
          asset_ref: asset.ref,
          require_relation_instance?: false
        }

        with {:ok, %GenerationMarkerReadResult{marker: marker}} <-
               OperationRunnerTasks.ensure_and_await(
                 context,
                 version,
                 asset.ref,
                 :generation_marker_read,
                 payload,
                 {:rebuild_marker_read, operation_id, target_id(asset)},
                 operation_id: operation_id,
                 rebuild_operation_id: operation_id
               ),
             :ok <- validate_live_marker(marker, binding),
             {:ok, fingerprint} <-
               inspect_active_fingerprint(
                 context,
                 version,
                 asset,
                 binding,
                 operation_id
               ),
             true <- fingerprint.fingerprint == binding.active_physical_fingerprint do
          {:cont, :ok}
        else
          _stale -> {:halt, {:error, stale_plan_error()}}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp validate_live_marker(%GenerationMarker{} = marker, binding) do
    expected = binding.active_data_plane_marker

    observed =
      marker
      |> Map.from_struct()
      |> Map.update!(:active_relation, &Map.from_struct/1)

    if marker.active_generation_id == binding.active_generation_id and
         canonical(observed) == canonical(expected),
       do: :ok,
       else: {:error, :active_generation_marker_changed}
  end

  defp validate_live_marker(_marker, _binding), do: {:error, :active_generation_marker_missing}

  defp inspect_active_fingerprint(context, version, asset, binding, operation_id) do
    {:ok, runner_binding} = OperationRunnerTasks.binding(version, asset.ref)

    with {:ok, relation} <- persisted_physical_relation(asset, binding),
         request <- %RelationInspectionRequest{
           manifest_version_id: version.manifest_version_id,
           manifest_content_hash: version.content_hash,
           required_runner_release_id: runner_binding.required_runner_release_id,
           asset_ref: asset.ref,
           relation: relation,
           include: [:relation, :columns, :table_metadata],
           sample_limit: 0
         },
         {:ok, inspection} <-
           OperationRunnerTasks.ensure_and_await(
             context,
             version,
             asset.ref,
             :relation_inspection,
             request,
             {:rebuild_active_inspection, operation_id, target_id(asset)},
             operation_id: operation_id,
             rebuild_operation_id: operation_id
           ) do
      PhysicalFingerprint.from_inspection(inspection)
    end
  end

  defp persisted_physical_relation(asset, binding) do
    logical_relation = RelationRef.new!(asset.relation)
    physical_relation = binding.active_physical_relation
    persisted_connection = field(physical_relation, :connection)

    if persisted_connection in persisted_connection_values(logical_relation.connection) do
      {:ok,
       RelationRef.new!(
         connection: logical_relation.connection,
         catalog: field(physical_relation, :catalog),
         schema: field(physical_relation, :schema),
         name: field(physical_relation, :name)
       )}
    else
      {:error, :active_physical_relation_connection_changed}
    end
  rescue
    ArgumentError -> {:error, :invalid_active_physical_relation}
  end

  defp persisted_connection_values(nil), do: [nil]

  defp persisted_connection_values(connection) when is_atom(connection),
    do: [connection, Atom.to_string(connection)]

  defp binding_snapshot(bindings), do: bindings |> canonical_binding_snapshot() |> canonical()

  defp canonical_binding_snapshot(bindings) do
    bindings
    |> Enum.map(fn {target_id, binding} ->
      {target_id,
       %{
         active_generation_id: binding.active_generation_id,
         active_physical_fingerprint: binding.active_physical_fingerprint,
         active_physical_relation: binding.active_physical_relation,
         active_data_plane_marker: binding.active_data_plane_marker,
         desired_manifest_id: binding.desired_manifest_id,
         desired_descriptor_hash: binding.desired_descriptor_hash,
         compatibility_status: binding.compatibility_status,
         reason_code: binding.reason_code,
         compatibility_diff: binding.compatibility_diff,
         version: binding.version
       }}
    end)
    |> Map.new()
  end

  defp action_payload(action) do
    action
    |> Map.from_struct()
    |> Map.update!(:candidate_generation, fn
      nil -> nil
      candidate -> candidate
    end)
  end

  defp coverage_payload(nil), do: nil

  defp coverage_payload(coverage) do
    %{
      declared_from: period_payload(coverage.declared_from),
      effective_from: period_payload(coverage.effective_from),
      through:
        case coverage.through do
          through when through in [:latest_closed, :current] -> through
          through -> period_payload(through)
        end,
      availability_delay_seconds: coverage.availability_delay_seconds,
      kind: coverage.kind,
      timezone: coverage.timezone,
      timezone_source: coverage.timezone_source,
      scope_source: coverage.scope_source
    }
  end

  defp period_payload(period) do
    %{
      kind: period.kind,
      start_at: period.start_at,
      end_at: period.end_at,
      timezone: period.timezone
    }
  end

  defp evaluated_range(items, root_target_id) do
    root_items = Enum.filter(items, &(&1.target_id == root_target_id))

    %{
      start_at: root_items |> List.first() |> item_boundary(:window_start),
      end_at: root_items |> List.last() |> item_boundary(:window_end)
    }
  end

  defp freeze_runtime_inputs(
         context,
         runtime,
         version,
         execution_index,
         planning_index,
         actions,
         items,
         bindings,
         capabilities,
         evaluated_at,
         operation_id
       ) do
    actions_by_target = Map.new(actions, &{field(&1, :target_id), &1})

    assets_by_target =
      planning_index.assets_by_ref
      |> Enum.filter(fn {_ref, asset} -> asset.target_descriptor != nil end)
      |> Map.new(fn {_ref, asset} -> {target_id(asset), asset} end)

    totals = Enum.frequencies_by(items, &field(&1, :target_id))

    specs =
      Enum.map(items, fn item ->
        target_id = field(item, :target_id)
        action = Map.fetch!(actions_by_target, target_id)
        asset = Map.fetch!(assets_by_target, target_id)

        %{
          item: item,
          asset: asset,
          run_id:
            command_id(
              "run-rebuild",
              operation_id <> ":" <> target_id <> ":" <> field(item, :item_id)
            ),
          evaluated_at: evaluated_at,
          combine_windows: combined_item?(item),
          window_selection: item_window_selection(item, asset),
          runner_binding: %{
            runner_pool: field(action, :runner_pool),
            required_runner_release_id: field(action, :required_runner_release_id)
          },
          rebuild:
            runtime_rebuild(
              operation_id,
              action,
              item,
              asset,
              bindings,
              capabilities,
              Map.fetch!(totals, target_id)
            )
        }
      end)

    RebuildRuntimeInputs.freeze(
      context,
      version,
      execution_index,
      runtime.deployment_id,
      specs
    )
  end

  defp runtime_rebuild(operation_id, action, item, asset, bindings, capabilities, total) do
    target_id = target_id(asset)
    active_relation = RelationRef.new!(asset.relation)
    action_kind = field(action, :action)

    generation_id =
      candidate_generation_id(action) || Map.fetch!(bindings, target_id).active_generation_id

    write_relation =
      if action_kind in [:rebuild, "rebuild"] do
        TargetGenerationRelation.candidate(
          active_relation,
          generation_id,
          get_in(capabilities, [target_id, :max_identifier_bytes]) ||
            get_in(capabilities, [target_id, "max_identifier_bytes"])
        )
      else
        active_relation
      end

    %{
      target_id: target_id,
      candidate_generation_id: generation_id,
      active_relation: active_relation,
      candidate_relation: write_relation,
      input_generations: planning_input_generations(action, bindings),
      operation_id: operation_id,
      action_id: target_id,
      item_id: field(item, :item_id),
      target_operation:
        if(action_kind in [:rebuild, "rebuild"],
          do: :rebuild_candidate,
          else: :normal_materialization
        ),
      empty_generation: field(item, :work_kind) in [:empty_generation, "empty_generation"],
      allow_combined_append: true,
      final_item: field(item, :ordinal) == total - 1
    }
  end

  defp candidate_generation_id(action) do
    field(action, :candidate_generation_id) ||
      action |> field(:candidate_generation) |> field(:target_generation_id)
  end

  defp planning_input_generations(action, bindings) do
    action
    |> field(:pinned_input_generation_ids)
    |> Enum.map(fn pin ->
      target_id = field(pin, :target_id)

      case Map.get(bindings, target_id) do
        nil ->
          %{
            target_id: target_id,
            target_generation_id: field(pin, :target_generation_id),
            evidence_generation_id: field(pin, :evidence_generation_id),
            physical_relation: field(pin, :physical_relation)
          }

        binding ->
          %{
            target_id: binding.target_id,
            target_generation_id: binding.active_generation_id,
            evidence_generation_id: binding.active_generation_id,
            physical_relation: binding.active_physical_relation
          }
      end
    end)
  end

  defp item_window_selection(item, asset) do
    if field(item, :work_kind) in [:full_load, "full_load"] do
      nil
    else
      {:ok, anchors} =
        Anchor.expand_range(
          asset.window.kind,
          field(item, :window_start),
          field(item, :window_end),
          timezone: asset.window.timezone
        )

      {:ok, selection} = Selection.backfill(anchors, asset.window.timezone)
      selection
    end
  end

  defp operation_items(context, operation_id),
    do: operation_items(context, operation_id, nil, [])

  defp operation_items(context, operation_id, after_cursor, acc) do
    case store().page_items(%FavnOrchestrator.Persistence.Queries.PageRebuildItems{
           workspace_context: context,
           operation_id: operation_id,
           after: after_cursor,
           limit: @page_size
         }) do
      {:ok, %{items: items, has_more?: true, next_cursor: next_cursor}} ->
        operation_items(context, operation_id, next_cursor, acc ++ items)

      {:ok, %{items: items}} ->
        {:ok, acc ++ items}

      {:error, _reason} = error ->
        error
    end
  end

  defp root_candidate_id(actions, target_id) do
    actions
    |> Enum.find(&(&1.target_id == target_id))
    |> then(& &1.candidate_generation.target_generation_id)
  end

  defp snapshot_target_ids(payload), do: payload |> field(:binding_snapshot) |> Map.keys()

  defp write_target_ids(payload) do
    payload
    |> field(:actions)
    |> Enum.filter(&(field(&1, :action) in ["rebuild", "backfill", :rebuild, :backfill]))
    |> Enum.map(&field(&1, :target_id))
    |> Enum.sort()
  end

  defp exact_plan(%RebuildOperation{plan_hash: hash}, hash), do: :ok
  defp exact_plan(_operation, _hash), do: {:error, stale_plan_error()}

  defp plan_not_expired(operation) do
    case field(operation.plan_payload, :expires_at) do
      %DateTime{} = expires_at ->
        if DateTime.compare(expires_at, DateTime.utc_now()) == :gt,
          do: :ok,
          else: {:error, stale_plan_error()}

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, expires_at, 0} ->
            if DateTime.compare(expires_at, DateTime.utc_now()) == :gt,
              do: :ok,
              else: {:error, stale_plan_error()}

          _invalid ->
            {:error, stale_plan_error()}
        end

      _invalid ->
        {:error, stale_plan_error()}
    end
  end

  defp stale_plan_error,
    do:
      Error.new(:conflict, "rebuild plan is stale", details: %{reason_code: "rebuild_plan_stale"})

  defp authorize_plan(%WorkspaceContext{roles: roles}) do
    if Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator])),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace operator authority required")}
  end

  defp authorize_admin(%WorkspaceContext{roles: roles}) do
    if Enum.any?(roles, &(&1 in [:workspace_admin, :platform_operator])),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace admin authority required")}
  end

  defp authorize_read(%WorkspaceContext{roles: roles}) do
    if roles == [], do: {:error, Error.new(:forbidden, "workspace authority required")}, else: :ok
  end

  defp validate_plan_options(opts) do
    allowed = [
      :evaluated_at,
      :operation_id,
      :idempotency_key,
      :occurred_at,
      :idempotency,
      :combine_windows,
      :empty
    ]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [] and
         Keyword.get(opts, :combine_windows, true) in [true, false] and
         Keyword.get(opts, :empty, false) in [true, false],
       do: :ok,
       else: {:error, :invalid_rebuild_options}
  end

  defp validate_command_options(opts) do
    allowed = [:command_id, :occurred_at, :lease_duration_ms, :idempotency]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_rebuild_options}
  end

  defp validate_page_options(opts) do
    allowed = [:state, :after, :limit]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [] and
         Keyword.get(opts, :limit, 100) in 1..200,
       do: :ok,
       else: {:error, :invalid_rebuild_page}
  end

  defp validate_item_page_options(opts) do
    allowed = [:target_id, :status, :after, :limit]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [] and
         Keyword.get(opts, :limit, 100) in 1..200,
       do: :ok,
       else: {:error, :invalid_rebuild_item_page}
  end

  defp validate_reason(reason) when byte_size(reason) in 1..4096 do
    if String.trim(reason) == "", do: {:error, :rebuild_reason_required}, else: :ok
  end

  defp validate_reason(_reason), do: {:error, :rebuild_reason_required}
  defp validate_datetime(%DateTime{}), do: :ok
  defp validate_datetime(_value), do: {:error, :invalid_rebuild_evaluated_at}

  defp item_boundary(nil, _field), do: nil
  defp item_boundary(item, field), do: Map.fetch!(item, field)

  defp validate_empty_rebuild(%Asset{window: %{}}, _opts), do: :ok

  defp validate_empty_rebuild(%Asset{}, opts) do
    if Keyword.get(opts, :empty, false),
      do: {:error, :empty_rebuild_requires_windowed_asset},
      else: :ok
  end

  defp rebuild_refs(index, root_ref, descendants, opts) do
    if Keyword.get(opts, :empty, false),
      do: [root_ref],
      else: affected_refs(index, root_ref, descendants)
  end

  defp actionable_refs(index, refs) do
    Enum.filter(refs, &(Map.fetch!(index.assets_by_ref, &1).target_descriptor != nil))
  end

  defp target_id(asset), do: asset.target_descriptor.target_id

  defp ref_value({module, name}),
    do: %{module: Atom.to_string(module), name: Atom.to_string(name)}

  defp item_id(target_id, key), do: command_id("item", target_id <> ":" <> key)
  defp rebuild_id, do: "rebuild_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

  defp plan_operation_id(context, opts) do
    Keyword.get(opts, :operation_id) ||
      case Keyword.get(opts, :idempotency_key) do
        key when is_binary(key) and key != "" ->
          command_id("rebuild", context.workspace_id <> ":" <> key)

        _missing ->
          rebuild_id()
      end
  end

  defp candidate_generation_id(operation_id, target_id) do
    bytes =
      :crypto.hash(:sha256, "rebuild-candidate:" <> operation_id <> ":" <> target_id)
      |> binary_part(0, 16)
      |> :binary.bin_to_list()
      |> List.update_at(6, &Bitwise.bor(Bitwise.band(&1, 0x0F), 0x40))
      |> List.update_at(8, &Bitwise.bor(Bitwise.band(&1, 0x3F), 0x80))
      |> :binary.list_to_bin()
      |> Base.encode16(case: :lower)

    <<a::binary-size(8), b::binary-size(4), c::binary-size(4), d::binary-size(4),
      e::binary-size(12)>> = bytes

    Enum.join([a, b, c, d, e], "-")
  end

  defp command_id(prefix, identity) do
    digest = :crypto.hash(:sha256, identity) |> Base.url_encode64(padding: false)
    prefix <> ":" <> String.slice(digest, 0, 40)
  end

  defp canonical(value) do
    value
    |> Favn.Manifest.Serializer.encode_canonical!()
    |> Jason.decode!()
  end

  defp decode_datetime!(%DateTime{} = value), do: value

  defp decode_datetime!(value) when is_binary(value) do
    {:ok, datetime, 0} = DateTime.from_iso8601(value)
    datetime
  end

  defp field(map, key) when is_map(map) and is_atom(key),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp field(map, key) when is_map(map) and is_binary(key), do: Map.get(map, key)

  defp field(map, key, default) when is_map(map) and is_atom(key),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp combined_item?(item) do
    with {:ok, {:window, key}} <- FreshnessKey.parse(field(item, :window_key)) do
      Favn.Window.Key.range?(key)
    else
      _other -> false
    end
  end

  defp store, do: Persistence.stores().rebuilds
  defp lock_store, do: Persistence.stores().target_operation_locks

  defp planning_worker do
    Application.get_env(
      :favn_orchestrator,
      :rebuild_planning_worker,
      FavnOrchestrator.RebuildPlanningWorker
    )
  end
end
