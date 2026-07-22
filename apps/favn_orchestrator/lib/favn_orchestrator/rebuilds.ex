defmodule FavnOrchestrator.Rebuilds do
  @moduledoc "Manual, generation-safe rebuild planning and lifecycle commands."

  alias Favn.Coverage.Expected
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index, as: ManifestIndex
  alias Favn.Manifest.PlanningIndex
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.TargetGenerationRelation
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TimePeriod
  alias Favn.Window.Anchor
  alias Favn.Window.Selection
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.CreateRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanAction
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanItem
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation
  alias FavnOrchestrator.Persistence.Commands.RetryRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.StartRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuild.Plan
  alias FavnOrchestrator.Rebuild.RuntimeInputs, as: RebuildRuntimeInputs
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.RuntimeConfig
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
    with :ok <- authorize_plan(context),
         :ok <- validate_plan_options(opts),
         :ok <- validate_reason(reason) do
      operation_id = plan_operation_id(context, opts)

      case existing_plan(context, operation_id, target_id, reason, opts) do
        {:ok, plan} ->
          {:ok, plan}

        :missing ->
          create_plan(context, target_id, reason, Keyword.put(opts, :operation_id, operation_id))

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp create_plan(context, target_id, reason, opts) do
    with evaluated_at <- Keyword.get(opts, :evaluated_at, DateTime.utc_now()),
         :ok <- validate_datetime(evaluated_at),
         {:ok, runtime, version, root} <- active_asset(context, target_id),
         :ok <- persisted_sql_target(root),
         {:ok, index} <- PlanningIndex.build(version.manifest),
         {:ok, descendants} <- PlanningIndex.transitive_downstream_of(index, root.ref),
         refs <- affected_refs(index, root.ref, descendants),
         {:ok, bindings} <- target_bindings(context, index, refs),
         {:ok, identities} <- TargetGenerations.for_reads(context, index.assets_by_ref),
         :ok <- validate_affected_bindings(index, refs, bindings),
         {:ok, root_binding} <- rebuildable_binding(bindings, target_id),
         {:ok, capability_snapshots} <- capabilities(version, index, refs),
         :ok <- validate_live_bindings(version, index, refs, bindings),
         {:ok, draft} <-
           build_draft(
             context,
             runtime,
             version,
             index,
             root,
             root_binding,
             bindings,
             identities,
             capability_snapshots,
             refs,
             reason,
             evaluated_at,
             opts
           ),
         {:ok, persisted} <- persist_plan(context, draft, opts) do
      {:ok,
       %Plan{
         plan_id: persisted.operation_id,
         plan_hash: persisted.plan_hash,
         expires_at: decode_datetime!(field(persisted.plan_payload, :expires_at)),
         payload: persisted.plan_payload
       }}
    end
  end

  defp existing_plan(context, operation_id, target_id, reason, opts) do
    requested_idempotency_key = Keyword.get(opts, :idempotency_key, operation_id)
    requested_evaluated_at = Keyword.get(opts, :evaluated_at)

    case store().get(%GetRebuild{workspace_context: context, operation_id: operation_id}) do
      {:ok, operation}
      when operation.root_target_id == target_id and operation.reason == reason and
             operation.idempotency_key == requested_idempotency_key and
             (is_nil(requested_evaluated_at) or
                operation.evaluated_at == requested_evaluated_at) ->
        {:ok,
         %Plan{
           plan_id: operation.operation_id,
           plan_hash: operation.plan_hash,
           expires_at: decode_datetime!(field(operation.plan_payload, :expires_at)),
           payload: operation.plan_payload
         }}

      {:ok, _operation} ->
        {:error,
         Error.new(:conflict, "rebuild plan identity has different request content",
           details: %{reason_code: "idempotency_conflict"}
         )}

      {:error, %Error{kind: :not_found}} ->
        :missing

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Approves an exact plan after revalidating every pinned control/data-plane input."
  @spec start(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, RebuildOperation.t()} | {:error, term()}
  def start(%WorkspaceContext{} = context, plan_id, plan_hash, opts \\ [])
      when is_binary(plan_id) and is_binary(plan_hash) and is_list(opts) do
    with :ok <- authorize_admin(context),
         :ok <- validate_command_options(opts),
         {:ok, operation} <- get(context, plan_id),
         :ok <- exact_plan(operation, plan_hash),
         :ok <- plan_not_expired(operation),
         :ok <- revalidate_plan(context, operation),
         {:ok, locks} <- acquire_plan_locks(context, operation, opts) do
      case store().start_operation(%StartRebuildOperation{
             workspace_context: context,
             command_id: command_id("start", plan_id <> ":" <> plan_hash),
             operation_id: plan_id,
             plan_hash: plan_hash,
             expected_version: operation.version,
             occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
           }) do
        {:ok, started} -> {:ok, started}
        {:error, reason} -> release_plan_locks(context, operation, locks, reason)
      end
    end
  end

  @doc "Returns one authoritative rebuild operation."
  @spec get(WorkspaceContext.t(), String.t()) :: {:ok, RebuildOperation.t()} | {:error, term()}
  def get(%WorkspaceContext{} = context, operation_id) when is_binary(operation_id) do
    with :ok <- authorize_read(context) do
      store().get(%GetRebuild{workspace_context: context, operation_id: operation_id})
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
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
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
         :ok <- revalidate_plan(context, operation) do
      store().retry_operation(%RetryRebuildOperation{
        workspace_context: context,
        command_id:
          Keyword.get(opts, :command_id, command_id("retry", operation_id <> ":" <> plan_hash)),
        operation_id: operation_id,
        plan_hash: plan_hash,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
      })
    end
  end

  defp build_draft(
         context,
         runtime,
         version,
         index,
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
             index,
             root,
             bindings,
             identities,
             capabilities,
             refs,
             operation_id,
             evaluated_at
           ),
         {:ok, execution_index} <- ManifestIndex.build(version.manifest),
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
        required_runner_release_id: version.required_runner_release_id,
        deployment_id: runtime.deployment_id,
        evaluated_at: evaluated_at,
        reason: reason,
        active_generation_id: root_binding.active_generation_id,
        candidate_generation_id: root_candidate_id(actions, root.target_descriptor.target_id),
        binding_snapshot: binding_snapshot(bindings),
        capabilities: capabilities,
        execution_packages:
          Map.new(refs, fn ref ->
            asset = Map.fetch!(index.assets_by_ref, ref)
            {target_id(asset), asset.execution_package_hash}
          end),
        assurance_expectations:
          Map.new(refs, fn ref ->
            asset = Map.fetch!(index.assets_by_ref, ref)
            {target_id(asset), assurance_expectation(asset)}
          end),
        actions: Enum.map(actions, &action_payload/1),
        item_count: length(items),
        items_digest: item_digest(items)
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

  defp build_actions(
         index,
         root,
         bindings,
         identities,
         capabilities,
         refs,
         operation_id,
         evaluated_at
       ) do
    Enum.reduce_while(Enum.with_index(refs), {:ok, [], [], identities}, fn {ref, ordinal},
                                                                           {:ok, actions, items,
                                                                            planned_identities} ->
      asset = Map.fetch!(index.assets_by_ref, ref)
      action_kind = action_kind(index, root, asset)

      with {:ok, candidate} <- candidate(action_kind, asset, capabilities, operation_id),
           {:ok, action_items} <-
             action_items(action_kind, root, asset, candidate, evaluated_at),
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

  defp action_items(:no_action, _root, _asset, _candidate, _evaluated_at), do: {:ok, []}

  defp action_items(:backfill, root, asset, _candidate, evaluated_at) do
    with {:ok, anchors} <- expected_anchors(root, evaluated_at) do
      {:ok, build_window_items(asset, anchors, nil)}
    end
  end

  defp action_items(:rebuild, _root, %Asset{window: nil} = asset, candidate, _evaluated_at) do
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

  defp action_items(:rebuild, _root, asset, candidate, evaluated_at) do
    with {:ok, anchors} <- expected_anchors(asset, evaluated_at) do
      items =
        case anchors do
          [] -> [empty_generation_item(asset, candidate.target_generation_id)]
          anchors -> build_window_items(asset, anchors, candidate.target_generation_id)
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

  defp build_window_items(asset, anchors, candidate_generation_id) do
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
      actor_id: context.principal_id,
      session_id: context.request_id,
      reason: payload.reason,
      idempotency_key: Keyword.get(opts, :idempotency_key, draft.plan.plan_id),
      evaluated_at: payload.evaluated_at,
      coverage_start: coverage_start,
      coverage_end: coverage_end,
      actions: draft.actions,
      items: items,
      occurred_at: Keyword.get(opts, :occurred_at, payload.evaluated_at)
    })
  end

  defp revalidate_plan(context, operation) do
    payload = operation.plan_payload

    with {:ok, runtime} <- ManifestStore.get_runtime_state(context),
         true <- runtime.manifest_version_id == field(payload, :manifest_version_id),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         true <- version.content_hash == field(payload, :manifest_content_hash),
         {:ok, current_bindings} <- target_bindings_by_ids(context, snapshot_target_ids(payload)),
         true <- binding_snapshot_matches?(current_bindings, payload, operation),
         {:ok, current_capabilities} <- capabilities_for_payload(version, payload),
         true <- canonical(current_capabilities) == field(payload, :capabilities),
         {:ok, index} <- PlanningIndex.build(version.manifest),
         :ok <-
           validate_live_bindings(
             version,
             index,
             payload_refs(index, payload),
             current_bindings
           ),
         {:ok, execution_index} <- ManifestIndex.build(version.manifest),
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
         true <- item_digest(current_items) == field(payload, :items_digest) do
      :ok
    else
      false -> {:error, stale_plan_error()}
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} -> {:error, stale_plan_error()}
    end
  end

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

  defp capabilities(version, index, refs) do
    refs
    |> Enum.filter(&(Map.fetch!(index.assets_by_ref, &1).target_descriptor != nil))
    |> Enum.reduce_while({:ok, %{}}, fn ref, {:ok, acc} ->
      asset = Map.fetch!(index.assets_by_ref, ref)

      case generation_capabilities(version, asset) do
        {:ok, snapshot} -> {:cont, {:ok, Map.put(acc, target_id(asset), snapshot)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp capabilities_for_payload(version, payload) do
    target_ids = payload |> field(:capabilities) |> Map.keys()

    Enum.reduce_while(target_ids, {:ok, %{}}, fn target_id, {:ok, acc} ->
      with {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
           {:ok, snapshot} <- generation_capabilities(version, asset) do
        {:cont, {:ok, Map.put(acc, target_id, snapshot)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp generation_capabilities(version, asset) do
    runtime = RuntimeConfig.current()

    case RunnerDispatch.generation_capabilities(
           runtime.runner_client,
           version,
           asset.ref,
           runtime.runner_client_opts
         ) do
      {:ok, capabilities} when is_map(capabilities) ->
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
               reason_code: Atom.to_string(binding.compatibility_status),
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

  defp active_asset(context, target_id) do
    with {:ok, {runtime, grants}} <- ManifestStore.get_active_deployment(context),
         true <- Enum.any?(grants, &(&1.target_kind == :asset and &1.target_id == target_id)),
         {:ok, version} <- ManifestStore.get_manifest(context, runtime.manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      {:ok, runtime, version, asset}
    else
      false -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
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

  defp validate_live_bindings(version, index, refs, bindings) do
    runtime = RuntimeConfig.current()

    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
    |> Enum.filter(& &1.target_descriptor)
    |> Enum.reduce_while(:ok, fn asset, :ok ->
      binding = Map.fetch!(bindings, target_id(asset))

      with {:ok, marker} <-
             RunnerDispatch.generation_marker(
               runtime.runner_client,
               version,
               asset.ref,
               runtime.runner_client_opts
             ),
           :ok <- validate_live_marker(marker, binding),
           {:ok, fingerprint} <- inspect_active_fingerprint(runtime, version, asset, binding),
           true <- fingerprint.fingerprint == binding.active_physical_fingerprint do
        {:cont, :ok}
      else
        _stale -> {:halt, {:error, stale_plan_error()}}
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

  defp inspect_active_fingerprint(runtime, version, asset, binding) do
    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      asset_ref: asset.ref,
      relation: RelationRef.new!(binding.active_physical_relation),
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    with {:ok, inspection} <-
           RunnerDispatch.inspect_relation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ) do
      PhysicalFingerprint.from_inspection(inspection)
    end
  end

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

  defp item_payload(item) do
    Map.take(item, [
      :target_id,
      :item_id,
      :ordinal,
      :work_kind,
      :window_key,
      :window_start,
      :window_end,
      :runtime_input_expectation,
      :candidate_generation_id
    ])
  end

  defp item_digest(items), do: Plan.hash(%{items: Enum.map(items, &item_payload/1)})

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
      Map.new(planning_index.assets_by_ref, fn {_ref, asset} -> {target_id(asset), asset} end)

    totals = Enum.frequencies_by(items, &field(&1, :target_id))

    specs =
      Enum.map(items, fn item ->
        target_id = field(item, :target_id)
        action = Map.fetch!(actions_by_target, target_id)
        asset = Map.fetch!(assets_by_target, target_id)

        %{
          item: item,
          asset: asset,
          run_id: command_id("run-rebuild", operation_id <> ":" <> field(item, :item_id)),
          evaluated_at: evaluated_at,
          window_selection: item_window_selection(item, asset),
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

    runner = RuntimeConfig.current()

    RebuildRuntimeInputs.freeze(
      context,
      version,
      execution_index,
      runtime.deployment_id,
      specs,
      runner.runner_client,
      runner.runner_client_opts
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
      binding = Map.fetch!(bindings, field(pin, :target_id))

      %{
        target_id: binding.target_id,
        target_generation_id: binding.active_generation_id,
        evidence_generation_id: binding.active_generation_id,
        physical_relation: binding.active_physical_relation
      }
    end)
  end

  defp item_window_selection(item, asset) do
    if field(item, :work_kind) in [:full_load, "full_load"] do
      nil
    else
      {:ok, anchor} =
        Anchor.new(asset.window.kind, field(item, :window_start), field(item, :window_end),
          timezone: asset.window.timezone
        )

      {:ok, selection} = Selection.backfill([anchor], asset.window.timezone)
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
    allowed = [:evaluated_at, :operation_id, :idempotency_key, :occurred_at]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_rebuild_options}
  end

  defp validate_command_options(opts) do
    allowed = [:command_id, :occurred_at, :lease_duration_ms]

    if Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
      do: :ok,
      else: {:error, :invalid_rebuild_options}
  end

  defp validate_reason(reason) when byte_size(reason) in 1..4096, do: :ok
  defp validate_reason(_reason), do: {:error, :rebuild_reason_required}
  defp validate_datetime(%DateTime{}), do: :ok
  defp validate_datetime(_value), do: {:error, :invalid_rebuild_evaluated_at}

  defp item_boundary(nil, _field), do: nil
  defp item_boundary(item, field), do: Map.fetch!(item, field)
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

  defp field(map, key) when is_map(map) do
    string_key = if is_atom(key), do: Atom.to_string(key), else: key
    Map.get(map, key, Map.get(map, string_key))
  end

  defp store, do: Persistence.stores().rebuilds
  defp lock_store, do: Persistence.stores().target_operation_locks
end
