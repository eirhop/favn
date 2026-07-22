defmodule FavnStoragePostgres.Rebuilds.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RebuildStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.GenerationDataPlaneMarker
  alias Favn.Manifest.Serializer
  alias FavnOrchestrator.Persistence.Commands.ActivateRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildItems
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.CreateRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation
  alias FavnOrchestrator.Persistence.Commands.RetryRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.StartRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildAction
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildItem
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RebuildAction, as: ActionResult
  alias FavnOrchestrator.Persistence.Results.RebuildItem, as: ItemResult
  alias FavnOrchestrator.Persistence.Results.RebuildOperation, as: OperationResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AssetTargetBinding
  alias FavnStoragePostgres.Schemas.AssetTargetGeneration
  alias FavnStoragePostgres.Schemas.RebuildOperation
  alias FavnStoragePostgres.Schemas.RebuildPlanAction
  alias FavnStoragePostgres.Schemas.RebuildWindow

  @max_actions 10_000
  @max_items 1_000_000
  @max_items_per_target 100_000
  @insert_batch 500
  @max_lease_ms 3_600_000
  @operation_states ~w(planned queued building validating activating activation_unknown reconciling cancelling succeeded failed cancelled)a
  @claimable_states ~w(queued building validating activating activation_unknown reconciling cancelling)a
  @operation_phases ~w(planned locking building validating activating reconciling repairing cleanup terminal)a
  @action_statuses ~w(planned running succeeded failed cancelled outcome_unknown)a
  @item_statuses ~w(planned ready claimed running succeeded failed cancelled outcome_unknown)a

  @impl true
  def create_plan(%CreateRebuildPlan{} = command) do
    with :ok <- validate_create(command) do
      transaction(fn -> create_plan!(command) end)
    end
  end

  @impl true
  def start_operation(%StartRebuildOperation{} = command) do
    with :ok <- validate_start(command) do
      transaction(fn -> start_operation!(command) end)
    end
  end

  @impl true
  def request_cancellation(%RequestRebuildCancellation{} = command) do
    with :ok <- validate_cancel(command) do
      transaction(fn -> request_cancellation!(command) end)
    end
  end

  @impl true
  def retry_operation(%RetryRebuildOperation{} = command) do
    with :ok <- validate_retry(command) do
      transaction(fn -> retry_operation!(command) end)
    end
  end

  @impl true
  def claim_operation(%ClaimRebuildOperation{} = command) do
    with :ok <- validate_claim_operation(command) do
      transaction(fn -> claim_operation!(command) end)
    end
  end

  @impl true
  def transition_operation(%TransitionRebuildOperation{} = command) do
    with :ok <- validate_transition_operation(command) do
      transaction(fn -> transition_operation!(command) end)
    end
  end

  @impl true
  def claim_items(%ClaimRebuildItems{} = command) do
    with :ok <- validate_claim_items(command) do
      transaction(fn -> claim_items!(command) end)
    end
  end

  @impl true
  def transition_item(%TransitionRebuildItem{} = command) do
    with :ok <- validate_transition_item(command) do
      transaction(fn -> transition_item!(command) end)
    end
  end

  @impl true
  def transition_action(%TransitionRebuildAction{} = command) do
    with :ok <- validate_transition_action(command) do
      transaction(fn -> transition_action!(command) end)
    end
  end

  @impl true
  def activate_generation(%ActivateRebuildGeneration{} = command) do
    with :ok <- validate_activation(command) do
      transaction(fn -> activate_generation!(command) end)
    end
  end

  @impl true
  def transition_generation(%TransitionRebuildGeneration{} = command) do
    with :ok <- validate_generation_transition(command) do
      case transaction(fn -> transition_generation!(command) end) do
        {:ok, :ok} -> :ok
        {:error, _reason} = error -> error
      end
    end
  end

  @impl true
  def get(%GetRebuild{} = query) do
    with :ok <- validate_get(query) do
      case operation_with_actions(query.workspace_context.workspace_id, query.operation_id) do
        nil -> {:error, ErrorMapper.map(:not_found)}
        operation -> {:ok, operation}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_items(%PageRebuildItems{} = page) do
    with :ok <- validate_page(page) do
      query =
        RebuildWindow
        |> where(
          [item],
          item.workspace_id == ^page.workspace_context.workspace_id and
            item.operation_id == ^page.operation_id
        )
        |> maybe_target(page.target_id)
        |> maybe_status(page.status)
        |> after_item(page.after)
        |> order_by([item], asc: item.ordinal, asc: item.target_id, asc: item.item_id)
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)
      items = rows |> Enum.take(page.limit) |> Enum.map(&item_result/1)
      last = List.last(items)

      {:ok,
       %CursorPage{
         items: items,
         limit: page.limit,
         has_more?: length(rows) > page.limit,
         next_cursor:
           if(length(rows) > page.limit and last,
             do: %{ordinal: last.ordinal, target_id: last.target_id, item_id: last.item_id},
             else: nil
           )
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp create_plan!(command) do
    workspace_id = command.workspace_context.workspace_id

    existing =
      from(operation in RebuildOperation,
        where:
          operation.workspace_id == ^workspace_id and
            (operation.operation_id == ^command.operation_id or
               operation.idempotency_key == ^command.idempotency_key),
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      existing && exact_plan_replay?(existing, command) ->
        operation_result(existing, load_actions(existing), progress(existing))

      existing ->
        Repo.rollback(Error.new(:conflict, "rebuild plan identity has different content"))

      true ->
        insert_plan!(command)
    end
  end

  defp insert_plan!(command) do
    workspace_id = command.workspace_context.workspace_id
    now = database_datetime(command.occurred_at)

    operation =
      %RebuildOperation{
        workspace_id: workspace_id,
        operation_id: command.operation_id,
        root_target_id: command.root_target_id,
        manifest_version_id: command.manifest_version_id,
        active_generation_id: command.active_generation_id,
        candidate_generation_id: command.candidate_generation_id,
        plan_hash: command.plan_hash,
        plan_version: command.plan_version,
        plan_payload: canonical_map(command.plan_payload),
        trigger: "manual",
        actor_id: command.actor_id,
        session_id: command.session_id,
        reason: command.reason,
        idempotency_key: command.idempotency_key,
        evaluated_at: database_datetime(command.evaluated_at),
        coverage_start: optional_datetime(command.coverage_start),
        coverage_end: optional_datetime(command.coverage_end),
        action_count: length(command.actions),
        window_count: length(command.items),
        state: "planned",
        phase: "planned",
        cleanup_state: "not_started",
        last_command_id: command.command_id,
        dispatcher_fencing_token: 0,
        cancel_requested: false,
        version: 1,
        inserted_at: now,
        updated_at: now
      }
      |> Repo.insert!()

    insert_candidate_generations!(command, now)
    insert_actions!(command, now)
    insert_items!(command, now)
    operation_result(operation, load_actions(operation), progress(operation))
  end

  defp insert_candidate_generations!(command, now) do
    rows =
      command.actions
      |> Enum.filter(&(&1.action == :rebuild))
      |> Enum.map(fn action ->
        generation = action.candidate_generation

        %{
          workspace_id: command.workspace_context.workspace_id,
          target_id: action.target_id,
          target_generation_id: generation.target_generation_id,
          creating_manifest_id: command.manifest_version_id,
          creation_command_id: command.command_id <> ":" <> action.target_id,
          creating_descriptor_hash: generation.descriptor_hash,
          logical_relation: canonical_map(generation.logical_relation),
          physical_relation: canonical_map(generation.physical_relation),
          status: "building",
          creating_rebuild_operation_id: command.operation_id,
          version: 1,
          created_at: now,
          updated_at: now
        }
      end)

    insert_all_batched!(AssetTargetGeneration, rows, "candidate generation")
  end

  defp insert_actions!(command, now) do
    rows =
      Enum.map(command.actions, fn action ->
        %{
          workspace_id: command.workspace_context.workspace_id,
          operation_id: command.operation_id,
          target_id: action.target_id,
          ordinal: action.ordinal,
          action: Atom.to_string(action.action),
          reason: canonical_map(action.reason),
          upstream_impact: canonical_map(action.upstream_impact),
          mapping_proof: optional_map(action.mapping_proof),
          pinned_input_generation_ids: canonical_value(action.pinned_input_generation_ids),
          candidate_generation_id: candidate_id(action.candidate_generation),
          status: Atom.to_string(action.status),
          cleanup_state: "not_started",
          version: 1,
          inserted_at: now,
          updated_at: now
        }
      end)

    insert_all_batched!(RebuildPlanAction, rows, "rebuild action")
  end

  defp insert_items!(command, now) do
    rows =
      Enum.map(command.items, fn item ->
        %{
          workspace_id: command.workspace_context.workspace_id,
          operation_id: command.operation_id,
          target_id: item.target_id,
          item_id: item.item_id,
          ordinal: item.ordinal,
          work_kind: Atom.to_string(item.work_kind),
          window_key: item.window_key,
          window_start: optional_datetime(item.window_start),
          window_end: optional_datetime(item.window_end),
          runtime_input_expectation: optional_map(item.runtime_input_expectation),
          status: "planned",
          fencing_token: 0,
          attempt_count: 0,
          candidate_generation_id: item.candidate_generation_id,
          version: 1,
          inserted_at: now,
          updated_at: now
        }
      end)

    insert_all_batched!(RebuildWindow, rows, "rebuild item")
  end

  defp insert_all_batched!(_schema, [], _kind), do: :ok

  defp insert_all_batched!(schema, rows, kind) do
    Enum.each(Enum.chunk_every(rows, @insert_batch), fn batch ->
      case Repo.insert_all(schema, batch) do
        {count, _} when count == length(batch) -> :ok
        _other -> Repo.rollback(Error.new(:conflict, "duplicate #{kind} identity"))
      end
    end)
  end

  defp start_operation!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)
    now = database_datetime(command.occurred_at)

    cond do
      operation.last_command_id == command.command_id and operation.state == "queued" ->
        operation_result(operation, load_actions(operation), progress(operation))

      operation.plan_hash != command.plan_hash ->
        Repo.rollback(stale_plan_error())

      operation.state != "planned" or operation.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "rebuild plan can no longer be started"))

      operator_decision?(operation) ->
        Repo.rollback(
          Error.new(:conflict, "rebuild plan requires an operator decision",
            details: %{reason_code: "operator_decision_required"}
          )
        )

      true ->
        from(item in RebuildWindow,
          where:
            item.workspace_id == ^operation.workspace_id and
              item.operation_id == ^operation.operation_id and item.status == "planned"
        )
        |> Repo.update_all(set: [status: "ready", updated_at: now])

        operation
        |> Ecto.Changeset.change(%{
          state: "queued",
          phase: "locking",
          last_command_id: command.command_id,
          version: operation.version + 1,
          started_at: now,
          updated_at: now
        })
        |> Repo.update!()
        |> then(&operation_result(&1, load_actions(&1), progress(&1)))
    end
  end

  defp request_cancellation!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)
    now = database_datetime(command.occurred_at)

    cond do
      operation.last_command_id == command.command_id and operation.cancel_requested ->
        operation_result(operation, load_actions(operation), progress(operation))

      operation.state in ["succeeded", "cancelled"] ->
        Repo.rollback(Error.new(:conflict, "terminal rebuild cannot be cancelled"))

      true ->
        {state, phase, cancelled_at} = cancellation_state(operation.state, now)

        from(item in RebuildWindow,
          where:
            item.workspace_id == ^operation.workspace_id and
              item.operation_id == ^operation.operation_id and
              item.status in ["planned", "ready"]
        )
        |> Repo.update_all(set: [status: "cancelled", updated_at: now], inc: [version: 1])

        operation
        |> Ecto.Changeset.change(%{
          cancel_requested: true,
          state: state,
          phase: phase,
          cleanup_state: "pending",
          terminal_error: %{
            "reason_code" => "operator_cancel_requested",
            "reason" => command.reason
          },
          last_command_id: command.command_id,
          version: operation.version + 1,
          cancelled_at: cancelled_at,
          updated_at: now
        })
        |> Repo.update!()
        |> then(&operation_result(&1, load_actions(&1), progress(&1)))
    end
  end

  defp cancellation_state("planned", now), do: {"cancelled", "terminal", now}

  defp cancellation_state(state, _now)
       when state in ["activating", "activation_unknown", "reconciling"],
       do: {state, if(state == "activating", do: "activating", else: "reconciling"), nil}

  defp cancellation_state(_state, _now), do: {"cancelling", "cleanup", nil}

  defp retry_operation!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)
    now = database_datetime(command.occurred_at)

    cond do
      operation.last_command_id == command.command_id and operation.state == "queued" ->
        operation_result(operation, load_actions(operation), progress(operation))

      operation.plan_hash != command.plan_hash ->
        Repo.rollback(stale_plan_error())

      operation.state != "failed" ->
        Repo.rollback(Error.new(:conflict, "only a failed rebuild can be retried"))

      operation.cleanup_state != "not_started" ->
        Repo.rollback(
          Error.new(:conflict, "failed rebuild candidate cleanup has started",
            details: %{reason_code: "rebuild_candidate_cleanup_started"}
          )
        )

      is_map(operation.unknown_outcome) and map_size(operation.unknown_outcome) > 0 ->
        Repo.rollback(
          Error.new(:conflict, "unknown rebuild outcome requires reconciliation",
            details: %{reason_code: "activation_outcome_unknown"}
          )
        )

      true ->
        reset_safe_items!(operation, now)
        reset_failed_generations!(operation, now)
        reset_failed_actions!(operation, now)

        operation
        |> Ecto.Changeset.change(%{
          state: "queued",
          phase: "locking",
          terminal_error: nil,
          validation_result: nil,
          cancel_requested: false,
          last_command_id: command.command_id,
          dispatcher_owner: nil,
          dispatcher_expires_at: nil,
          version: operation.version + 1,
          completed_at: nil,
          updated_at: now
        })
        |> Repo.update!()
        |> then(&operation_result(&1, load_actions(&1), progress(&1)))
    end
  end

  defp reset_safe_items!(operation, now) do
    from(item in RebuildWindow,
      where:
        item.workspace_id == ^operation.workspace_id and
          item.operation_id == ^operation.operation_id and item.status == "failed" and
          fragment("?->>'outcome' = 'safe_failure'", item.last_error)
    )
    |> Repo.update_all(
      set: [
        status: "ready",
        claim_owner: nil,
        claim_command_id: nil,
        claim_expires_at: nil,
        last_command_id: nil,
        last_error: nil,
        updated_at: now
      ],
      inc: [version: 1]
    )
  end

  defp reset_failed_actions!(operation, now) do
    from(action in RebuildPlanAction,
      where:
        action.workspace_id == ^operation.workspace_id and
          action.operation_id == ^operation.operation_id and action.status == "failed"
    )
    |> Repo.update_all(
      set: [status: "planned", terminal_error: nil, updated_at: now],
      inc: [version: 1]
    )
  end

  defp reset_failed_generations!(operation, now) do
    candidate_ids =
      from(action in RebuildPlanAction,
        where:
          action.workspace_id == ^operation.workspace_id and
            action.operation_id == ^operation.operation_id and action.status == "failed" and
            not is_nil(action.candidate_generation_id) and is_nil(action.activated_at),
        select: action.candidate_generation_id
      )
      |> Repo.all()

    from(generation in AssetTargetGeneration,
      where:
        generation.workspace_id == ^operation.workspace_id and
          generation.creating_rebuild_operation_id == ^operation.operation_id and
          generation.target_generation_id in ^candidate_ids and generation.status == "failed"
    )
    |> Repo.update_all(set: [status: "building", updated_at: now], inc: [version: 1])
  end

  defp claim_operation!(command) do
    now = database_now!()

    query =
      from(operation in RebuildOperation,
        where:
          operation.workspace_id == ^command.workspace_context.workspace_id and
            (operation.state in ^Enum.map(@claimable_states, &Atom.to_string/1) or
               (operation.state in ["succeeded", "failed", "cancelled"] and
                  operation.cleanup_state in ["pending", "failed"])) and
            (is_nil(operation.dispatcher_expires_at) or operation.dispatcher_expires_at <= ^now or
               operation.dispatcher_owner == ^command.owner_id),
        order_by: [asc: operation.updated_at, asc: operation.operation_id],
        limit: 1,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> maybe_operation(command.operation_id)

    case Repo.one(query) do
      nil ->
        nil

      operation ->
        same_live_owner? =
          (operation.dispatcher_owner == command.owner_id and operation.dispatcher_expires_at) &&
            DateTime.compare(operation.dispatcher_expires_at, now) == :gt

        operation
        |> Ecto.Changeset.change(%{
          dispatcher_owner: command.owner_id,
          dispatcher_fencing_token:
            if(same_live_owner?,
              do: operation.dispatcher_fencing_token,
              else: operation.dispatcher_fencing_token + 1
            ),
          dispatcher_expires_at: DateTime.add(now, command.lease_duration_ms, :millisecond),
          version: operation.version + 1,
          updated_at: now
        })
        |> Repo.update!()
        |> then(&operation_result(&1, load_actions(&1), progress(&1)))
    end
  end

  defp transition_operation!(command) do
    operation = lock_operation!(command.workspace_context.workspace_id, command.operation_id)
    now = database_now!()

    cond do
      operation.last_command_id == command.command_id and
          operation.state == Atom.to_string(command.state) ->
        operation_result(operation, load_actions(operation), progress(operation))

      not exact_operation_fence?(operation, command, now) ->
        Repo.rollback(Error.new(:fenced, "rebuild dispatcher fence is stale"))

      operation.version != command.expected_version or
          operation.state not in Enum.map(command.expected_states, &Atom.to_string/1) ->
        Repo.rollback(Error.new(:conflict, "rebuild operation state changed"))

      not legal_operation_transition?(operation.state, Atom.to_string(command.state)) ->
        Repo.rollback(Error.new(:conflict, "illegal rebuild operation transition"))

      true ->
        state = Atom.to_string(command.state)

        operation
        |> Ecto.Changeset.change(%{
          state: state,
          phase: Atom.to_string(command.phase),
          activation_token: choose(command.activation_token, operation.activation_token),
          result_marker: choose_canonical(command.result_marker, operation.result_marker),
          unknown_outcome: choose_canonical(command.unknown_outcome, operation.unknown_outcome),
          validation_result:
            choose_canonical(command.validation_result, operation.validation_result),
          terminal_error: choose_canonical(command.terminal_error, operation.terminal_error),
          cleanup_state:
            if(command.cleanup_state,
              do: Atom.to_string(command.cleanup_state),
              else: operation.cleanup_state
            ),
          last_command_id: command.command_id,
          version: operation.version + 1,
          completed_at:
            if(state in ["succeeded", "failed"],
              do: operation.completed_at || command.occurred_at
            ),
          cancelled_at: if(state == "cancelled", do: command.occurred_at),
          dispatcher_owner:
            if(state in ["succeeded", "failed", "cancelled"],
              do: nil,
              else: operation.dispatcher_owner
            ),
          dispatcher_expires_at:
            if(state in ["succeeded", "failed", "cancelled"],
              do: nil,
              else: operation.dispatcher_expires_at
            ),
          updated_at: database_datetime(command.occurred_at)
        })
        |> Repo.update!()
        |> then(&operation_result(&1, load_actions(&1), progress(&1)))
    end
  end

  defp claim_items!(command) do
    workspace_id = command.workspace_context.workspace_id
    now = database_now!()

    operation =
      ensure_dispatch_operation!(workspace_id, command.operation_id, command.owner_id, now)

    replay =
      from(item in RebuildWindow,
        where:
          item.workspace_id == ^workspace_id and item.operation_id == ^command.operation_id and
            item.target_id == ^command.target_id and item.claim_command_id == ^command.batch_id,
        order_by: [asc: item.ordinal, asc: item.item_id]
      )
      |> Repo.all()

    if replay != [] do
      Enum.map(replay, &item_result/1)
    else
      expires_at = DateTime.add(now, command.lease_duration_ms, :millisecond)

      base_query =
        from(item in RebuildWindow,
          where:
            item.workspace_id == ^workspace_id and item.operation_id == ^command.operation_id and
              item.target_id == ^command.target_id,
          order_by: [asc: item.ordinal, asc: item.item_id],
          limit: ^command.limit,
          lock: "FOR UPDATE SKIP LOCKED"
        )

      claim_query =
        if operation.cancel_requested do
          where(
            base_query,
            [item],
            item.status in ["claimed", "running"] and item.claim_expires_at <= ^now
          )
        else
          where(
            base_query,
            [item],
            item.status == "ready" or
              (item.status in ["claimed", "running"] and item.claim_expires_at <= ^now)
          )
        end

      items = Repo.all(claim_query)

      Enum.map(items, fn item ->
        new_attempt? = item.status == "ready"

        item
        |> Ecto.Changeset.change(%{
          status: "claimed",
          claim_owner: command.owner_id,
          fencing_token: item.fencing_token + 1,
          claim_command_id: command.batch_id,
          claim_expires_at: expires_at,
          attempt_count: item.attempt_count + if(new_attempt?, do: 1, else: 0),
          version: item.version + 1,
          updated_at: now
        })
        |> Repo.update!()
        |> item_result()
      end)
    end
  end

  defp transition_item!(command) do
    now = database_now!()

    ensure_item_transition_owner!(
      command.workspace_context.workspace_id,
      command.operation_id,
      command.owner_id,
      command.status,
      now
    )

    item = lock_item!(command)

    cond do
      item.last_command_id == command.command_id and item.status == Atom.to_string(command.status) ->
        item_result(item)

      item.fencing_token != command.fencing_token or
          (item.status != "outcome_unknown" and item.claim_owner != command.owner_id) ->
        Repo.rollback(Error.new(:fenced, "rebuild item fence is stale"))

      item.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "rebuild item version changed"))

      not legal_item_transition?(item.status, Atom.to_string(command.status)) ->
        Repo.rollback(Error.new(:conflict, "illegal rebuild item transition"))

      true ->
        status = Atom.to_string(command.status)
        terminal? = status in ["succeeded", "failed", "cancelled", "outcome_unknown"]

        item
        |> Ecto.Changeset.change(%{
          status: status,
          child_run_id: choose(command.child_run_id, item.child_run_id),
          materialization_id: choose(command.materialization_id, item.materialization_id),
          row_count: command.row_count,
          last_error: optional_canonical(command.last_error),
          last_command_id: command.command_id,
          claim_expires_at: if(terminal?, do: nil, else: item.claim_expires_at),
          version: item.version + 1,
          updated_at: database_datetime(command.occurred_at)
        })
        |> Repo.update!()
        |> item_result()
    end
  end

  defp transition_action!(command) do
    now = database_now!()

    ensure_operation_fence!(
      command.workspace_context.workspace_id,
      command.operation_id,
      command,
      now
    )

    action =
      lock_action!(
        command.workspace_context.workspace_id,
        command.operation_id,
        command.target_id
      )

    cond do
      action.last_command_id == command.command_id and
          action.status == Atom.to_string(command.status) ->
        action_result(action)

      action.version != command.expected_version or
          action.status not in Enum.map(command.expected_statuses, &Atom.to_string/1) ->
        Repo.rollback(Error.new(:conflict, "rebuild action state changed"))

      not legal_action_transition?(action.status, Atom.to_string(command.status)) ->
        Repo.rollback(Error.new(:conflict, "illegal rebuild action transition"))

      true ->
        action
        |> Ecto.Changeset.change(%{
          status: Atom.to_string(command.status),
          child_operation_id: choose(command.child_operation_id, action.child_operation_id),
          child_run_id: choose(command.child_run_id, action.child_run_id),
          activation_intent:
            choose_canonical(command.activation_intent, action.activation_intent),
          validation_result:
            choose_canonical(command.validation_result, action.validation_result),
          terminal_error: choose_canonical(command.terminal_error, action.terminal_error),
          cleanup_state:
            if(command.cleanup_state,
              do: Atom.to_string(command.cleanup_state),
              else: action.cleanup_state
            ),
          activated_at: choose(command.activated_at, action.activated_at),
          last_command_id: command.command_id,
          version: action.version + 1,
          updated_at: database_datetime(command.occurred_at)
        })
        |> Repo.update!()
        |> action_result()
    end
  end

  defp activate_generation!(command) do
    workspace_id = command.workspace_context.workspace_id
    ensure_operation_fence!(workspace_id, command.operation_id, command, database_now!())
    action = lock_action!(workspace_id, command.operation_id, command.target_id)
    binding = lock_binding!(workspace_id, command.target_id)
    previous = lock_generation!(workspace_id, command.target_id, command.previous_generation_id)
    candidate = lock_generation!(workspace_id, command.target_id, command.candidate_generation_id)
    marker = canonical_map(command.data_plane_marker)

    cond do
      exact_activation_replay?(action, binding, previous, candidate, command, marker) ->
        action_result(action)

      action.candidate_generation_id != command.candidate_generation_id or
          candidate.creating_rebuild_operation_id != command.operation_id ->
        Repo.rollback(
          Error.new(:conflict, "candidate generation does not belong to rebuild action")
        )

      binding.active_generation_id != command.previous_generation_id or
        previous.status != "active" or
          candidate.status != "building" ->
        Repo.rollback(Error.new(:conflict, "target generation binding changed before activation"))

      not activation_intent_matches?(action.activation_intent, command) ->
        Repo.rollback(Error.new(:conflict, "activation intent was not durably persisted"))

      true ->
        validate_marker!(marker, command)
        now = database_datetime(command.occurred_at)

        previous
        |> Ecto.Changeset.change(%{
          status: "retired",
          physical_relation: canonical_map(command.retired_relation),
          version: previous.version + 1,
          retired_at: now,
          updated_at: now
        })
        |> Repo.update!()

        candidate
        |> Ecto.Changeset.change(%{
          active_descriptor_hash: binding.desired_descriptor_hash,
          physical_relation: canonical_map(command.active_relation),
          physical_schema_fingerprint: command.physical_schema_fingerprint,
          data_plane_marker: marker,
          activation_token: command.activation_token,
          status: "active",
          version: candidate.version + 1,
          activated_at: now,
          updated_at: now
        })
        |> Repo.update!()

        binding
        |> Ecto.Changeset.change(%{
          active_generation_id: command.candidate_generation_id,
          compatibility_status: "ready",
          reason_code: "rebuild_activated",
          compatibility_diff: %{},
          active_physical_fingerprint: command.physical_schema_fingerprint,
          version: binding.version + 1,
          updated_at: now
        })
        |> Repo.update!()

        action
        |> Ecto.Changeset.change(%{
          status: "succeeded",
          validation_result: %{"data_plane_marker" => marker},
          terminal_error: nil,
          activated_at: now,
          version: action.version + 1,
          updated_at: now
        })
        |> Repo.update!()
        |> action_result()
    end
  end

  defp transition_generation!(command) do
    workspace_id = command.workspace_context.workspace_id
    ensure_operation_fence!(workspace_id, command.operation_id, command, database_now!())
    action = lock_action!(workspace_id, command.operation_id, command.target_id)

    generation =
      lock_generation!(workspace_id, command.target_id, command.candidate_generation_id)

    status = Atom.to_string(command.status)

    cond do
      generation.status == status ->
        :ok

      action.candidate_generation_id != command.candidate_generation_id or
          generation.creating_rebuild_operation_id != command.operation_id ->
        Repo.rollback(
          Error.new(:conflict, "candidate generation does not belong to rebuild action")
        )

      generation.status not in ["building", "failed"] ->
        Repo.rollback(Error.new(:conflict, "candidate generation is no longer building"))

      true ->
        generation
        |> Ecto.Changeset.change(%{
          status: status,
          version: generation.version + 1,
          updated_at: database_datetime(command.occurred_at)
        })
        |> Repo.update!()

        :ok
    end
  end

  defp validate_marker!(marker, command) do
    with :ok <-
           GenerationDataPlaneMarker.validate(
             marker,
             command.target_id,
             command.candidate_generation_id
           ),
         true <- marker["activation_operation_id"] == command.operation_id,
         true <- marker["activation_token"] == command.activation_token do
      :ok
    else
      _invalid -> Repo.rollback(Error.new(:conflict, "data-plane activation marker is invalid"))
    end
  end

  defp exact_activation_replay?(action, binding, previous, candidate, command, marker) do
    action.status == "succeeded" and
      binding.active_generation_id == command.candidate_generation_id and
      previous.status == "retired" and candidate.status == "active" and
      candidate.activation_token == command.activation_token and
      candidate.data_plane_marker == marker and
      candidate.physical_schema_fingerprint == command.physical_schema_fingerprint and
      candidate.physical_relation == canonical_map(command.active_relation) and
      previous.physical_relation == canonical_map(command.retired_relation)
  end

  defp activation_intent_matches?(intent, command) when is_map(intent) do
    (intent["activation_token"] || intent[:activation_token]) == command.activation_token and
      (intent["previous_generation_id"] || intent[:previous_generation_id]) ==
        command.previous_generation_id and
      (intent["candidate_generation_id"] || intent[:candidate_generation_id]) ==
        command.candidate_generation_id
  end

  defp activation_intent_matches?(_intent, _command), do: false

  defp operation_with_actions(workspace_id, operation_id) do
    case Repo.get_by(RebuildOperation, workspace_id: workspace_id, operation_id: operation_id) do
      nil -> nil
      operation -> operation_result(operation, load_actions(operation), progress(operation))
    end
  end

  defp load_actions(operation) do
    progress_by_target = action_progress(operation)

    from(action in RebuildPlanAction,
      where:
        action.workspace_id == ^operation.workspace_id and
          action.operation_id == ^operation.operation_id,
      order_by: [asc: action.ordinal, asc: action.target_id]
    )
    |> Repo.all()
    |> Enum.map(&action_result(&1, Map.get(progress_by_target, &1.target_id, %{total: 0})))
  end

  defp action_progress(operation) do
    from(item in RebuildWindow,
      where:
        item.workspace_id == ^operation.workspace_id and
          item.operation_id == ^operation.operation_id,
      group_by: [item.target_id, item.status],
      select: {item.target_id, item.status, count(item.item_id)}
    )
    |> Repo.all()
    |> Enum.reduce(%{}, fn {target_id, status, count}, acc ->
      Map.update(
        acc,
        target_id,
        %{String.to_existing_atom(status) => count, total: count},
        fn current ->
          current
          |> Map.put(String.to_existing_atom(status), count)
          |> Map.update!(:total, &(&1 + count))
        end
      )
    end)
  end

  defp progress(operation) do
    counts =
      from(item in RebuildWindow,
        where:
          item.workspace_id == ^operation.workspace_id and
            item.operation_id == ^operation.operation_id,
        group_by: item.status,
        select: {item.status, count(item.item_id)}
      )
      |> Repo.all()
      |> Map.new(fn {status, count} -> {String.to_existing_atom(status), count} end)

    Map.put(counts, :total, Enum.sum(Map.values(counts)))
  end

  defp operation_result(operation, actions, progress) do
    %OperationResult{
      workspace_id: operation.workspace_id,
      operation_id: operation.operation_id,
      root_target_id: operation.root_target_id,
      manifest_version_id: operation.manifest_version_id,
      active_generation_id: operation.active_generation_id,
      candidate_generation_id: operation.candidate_generation_id,
      plan_hash: operation.plan_hash,
      plan_version: operation.plan_version,
      plan_payload: operation.plan_payload || %{},
      actor_id: operation.actor_id,
      session_id: operation.session_id,
      reason: operation.reason,
      idempotency_key: operation.idempotency_key,
      evaluated_at: operation.evaluated_at,
      coverage_start: operation.coverage_start,
      coverage_end: operation.coverage_end,
      action_count: operation.action_count,
      window_count: operation.window_count,
      state: String.to_existing_atom(operation.state),
      phase: String.to_existing_atom(operation.phase),
      activation_token: operation.activation_token,
      result_marker: operation.result_marker,
      unknown_outcome: operation.unknown_outcome,
      validation_result: operation.validation_result,
      terminal_error: operation.terminal_error,
      cleanup_state: String.to_existing_atom(operation.cleanup_state),
      cancel_requested: operation.cancel_requested,
      dispatcher_owner: operation.dispatcher_owner,
      dispatcher_fencing_token: operation.dispatcher_fencing_token,
      dispatcher_expires_at: operation.dispatcher_expires_at,
      version: operation.version,
      started_at: operation.started_at,
      completed_at: operation.completed_at,
      cancelled_at: operation.cancelled_at,
      inserted_at: operation.inserted_at,
      updated_at: operation.updated_at,
      actions: actions,
      progress: progress
    }
  end

  defp action_result(action, progress \\ %{}) do
    %ActionResult{
      workspace_id: action.workspace_id,
      operation_id: action.operation_id,
      target_id: action.target_id,
      ordinal: action.ordinal,
      action: String.to_existing_atom(action.action),
      reason: action.reason,
      upstream_impact: action.upstream_impact,
      mapping_proof: action.mapping_proof,
      pinned_input_generation_ids: action.pinned_input_generation_ids,
      candidate_generation_id: action.candidate_generation_id,
      status: String.to_existing_atom(action.status),
      child_operation_id: action.child_operation_id,
      child_run_id: action.child_run_id,
      activation_intent: action.activation_intent,
      validation_result: action.validation_result,
      terminal_error: action.terminal_error,
      cleanup_state: String.to_existing_atom(action.cleanup_state),
      activated_at: action.activated_at,
      version: action.version,
      inserted_at: action.inserted_at,
      updated_at: action.updated_at,
      progress: progress
    }
  end

  defp item_result(item) do
    %ItemResult{
      workspace_id: item.workspace_id,
      operation_id: item.operation_id,
      target_id: item.target_id,
      item_id: item.item_id,
      ordinal: item.ordinal,
      work_kind: String.to_existing_atom(item.work_kind),
      window_key: item.window_key,
      window_start: item.window_start,
      window_end: item.window_end,
      runtime_input_expectation: item.runtime_input_expectation,
      status: String.to_existing_atom(item.status),
      claim_owner: item.claim_owner,
      fencing_token: item.fencing_token,
      claim_expires_at: item.claim_expires_at,
      child_run_id: item.child_run_id,
      materialization_id: item.materialization_id,
      attempt_count: item.attempt_count,
      row_count: item.row_count,
      last_error: item.last_error,
      candidate_generation_id: item.candidate_generation_id,
      version: item.version,
      inserted_at: item.inserted_at,
      updated_at: item.updated_at
    }
  end

  defp lock_operation!(workspace_id, operation_id) do
    from(operation in RebuildOperation,
      where: operation.workspace_id == ^workspace_id and operation.operation_id == ^operation_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(ErrorMapper.map(:not_found))
      operation -> operation
    end
  end

  defp lock_action!(workspace_id, operation_id, target_id) do
    from(action in RebuildPlanAction,
      where:
        action.workspace_id == ^workspace_id and action.operation_id == ^operation_id and
          action.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(ErrorMapper.map(:not_found))
      action -> action
    end
  end

  defp lock_item!(command) do
    from(item in RebuildWindow,
      where:
        item.workspace_id == ^command.workspace_context.workspace_id and
          item.operation_id == ^command.operation_id and item.target_id == ^command.target_id and
          item.item_id == ^command.item_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(ErrorMapper.map(:not_found))
      item -> item
    end
  end

  defp lock_binding!(workspace_id, target_id) do
    from(binding in AssetTargetBinding,
      where: binding.workspace_id == ^workspace_id and binding.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(ErrorMapper.map(:not_found))
      binding -> binding
    end
  end

  defp lock_generation!(workspace_id, target_id, generation_id) do
    from(generation in AssetTargetGeneration,
      where:
        generation.workspace_id == ^workspace_id and generation.target_id == ^target_id and
          generation.target_generation_id == ^generation_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(ErrorMapper.map(:not_found))
      generation -> generation
    end
  end

  defp ensure_dispatch_operation!(workspace_id, operation_id, owner_id, now) do
    operation = lock_operation!(workspace_id, operation_id)

    unless (operation.dispatcher_owner == owner_id and operation.dispatcher_expires_at) &&
             DateTime.compare(operation.dispatcher_expires_at, now) == :gt do
      Repo.rollback(Error.new(:fenced, "rebuild dispatcher fence is stale"))
    end

    operation
  end

  defp ensure_item_transition_owner!(workspace_id, operation_id, owner_id, status, now) do
    operation = ensure_dispatch_operation!(workspace_id, operation_id, owner_id, now)

    allowed? =
      if operation.state == "cancelling" do
        operation.cancel_requested and
          status in [:cancelled, :succeeded, :failed, :outcome_unknown]
      else
        operation.state in ["queued", "building", "validating", "activating", "reconciling"] and
          not operation.cancel_requested
      end

    unless allowed?,
      do: Repo.rollback(Error.new(:fenced, "rebuild operation no longer permits item outcome"))
  end

  defp validate_generation_transition(command) do
    valid =
      valid_context?(command.workspace_context) and valid_id?(command.command_id) and
        valid_id?(command.operation_id) and valid_id?(command.target_id) and
        valid_uuid?(command.candidate_generation_id) and valid_id?(command.owner_id) and
        valid_version?(command.operation_fencing_token) and
        command.status in [:failed, :discarded] and
        match?(%DateTime{}, command.occurred_at)

    valid |> valid_or_error()
  end

  defp ensure_operation_fence!(workspace_id, operation_id, command, now) do
    operation = lock_operation!(workspace_id, operation_id)

    unless (operation.dispatcher_owner == command.owner_id and
              operation.dispatcher_fencing_token == command.operation_fencing_token and
              operation.dispatcher_expires_at) &&
             DateTime.compare(operation.dispatcher_expires_at, now) == :gt do
      Repo.rollback(Error.new(:fenced, "rebuild dispatcher fence is stale"))
    end
  end

  defp exact_operation_fence?(operation, command, now) do
    (operation.dispatcher_owner == command.owner_id and
       operation.dispatcher_fencing_token == command.fencing_token and
       operation.dispatcher_expires_at) &&
      DateTime.compare(operation.dispatcher_expires_at, now) == :gt
  end

  defp exact_plan_replay?(operation, command) do
    operation.operation_id == command.operation_id and
      operation.idempotency_key == command.idempotency_key and
      operation.plan_hash == command.plan_hash and
      operation.manifest_version_id == command.manifest_version_id and
      operation.root_target_id == command.root_target_id and
      operation.active_generation_id == command.active_generation_id and
      operation.candidate_generation_id == command.candidate_generation_id and
      operation.actor_id == command.actor_id and operation.session_id == command.session_id and
      operation.reason == command.reason and operation.plan_version == command.plan_version and
      operation.plan_payload == canonical_map(command.plan_payload)
  end

  defp operator_decision?(operation) do
    from(action in RebuildPlanAction,
      where:
        action.workspace_id == ^operation.workspace_id and
          action.operation_id == ^operation.operation_id and action.action == "operator_decision",
      select: true,
      limit: 1
    )
    |> Repo.exists?()
  end

  defp legal_operation_transition?(state, state), do: true

  defp legal_operation_transition?("queued", next),
    do: next in ["building", "cancelling", "failed"]

  defp legal_operation_transition?("building", next),
    do: next in ["validating", "cancelling", "failed"]

  defp legal_operation_transition?("validating", next),
    do: next in ["activating", "cancelling", "failed"]

  defp legal_operation_transition?("activating", next),
    do: next in ["building", "succeeded", "failed", "activation_unknown"]

  defp legal_operation_transition?("activation_unknown", next),
    do: next in ["reconciling", "cancelling", "failed"]

  defp legal_operation_transition?("reconciling", next),
    do: next in ["building", "activating", "cancelling", "succeeded", "failed"]

  defp legal_operation_transition?("cancelling", next),
    do: next in ["cancelled", "activation_unknown", "failed"]

  defp legal_operation_transition?(_state, _next), do: false

  defp legal_item_transition?("claimed", next),
    do: next in ["running", "failed", "cancelled", "outcome_unknown"]

  defp legal_item_transition?("running", next),
    do: next in ["succeeded", "failed", "cancelled", "outcome_unknown"]

  defp legal_item_transition?("outcome_unknown", next),
    do: next in ["succeeded", "failed", "cancelled"]

  defp legal_item_transition?(_state, _next), do: false

  defp legal_action_transition?("planned", next), do: next in ["running", "cancelled"]
  defp legal_action_transition?(state, state), do: true

  defp legal_action_transition?("running", next),
    do: next in ["succeeded", "failed", "cancelled", "outcome_unknown"]

  defp legal_action_transition?("failed", "running"), do: true
  defp legal_action_transition?("outcome_unknown", next), do: next in ["running", "cancelled"]
  defp legal_action_transition?(_state, _next), do: false

  defp validate_create(command) do
    actions = command.actions
    items = command.items

    if valid_context?(command.workspace_context) and
         Enum.all?(
           [
             command.command_id,
             command.operation_id,
             command.root_target_id,
             command.manifest_version_id,
             command.candidate_generation_id,
             command.actor_id,
             command.idempotency_key
           ],
           &valid_id?/1
         ) and (is_nil(command.active_generation_id) or valid_uuid?(command.active_generation_id)) and
         valid_uuid?(command.candidate_generation_id) and valid_hash?(command.plan_hash) and
         command.plan_version == 1 and is_map(command.plan_payload) and
         Payload.validate(command.plan_payload, 1_048_576) == :ok and
         plan_hash(command.plan_payload) == command.plan_hash and valid_reason?(command.reason) and
         match?(%DateTime{}, command.evaluated_at) and
         valid_range?(command.coverage_start, command.coverage_end) and
         is_list(actions) and length(actions) in 1..@max_actions and
         valid_actions?(actions, command) and
         is_list(items) and length(items) in 1..@max_items and valid_items?(items, actions) and
         exact_item_set?(command.plan_payload, items) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_actions?(actions, command) do
    ordinals = Enum.map(actions, & &1.ordinal)
    target_ids = Enum.map(actions, & &1.target_id)

    ordinals == Enum.to_list(0..(length(actions) - 1)) and
      length(target_ids) == length(Enum.uniq(target_ids)) and
      Enum.all?(actions, &valid_action?(&1, command)) and
      Enum.any?(actions, fn action ->
        action.target_id == command.root_target_id and action.action == :rebuild and
          candidate_id(action.candidate_generation) == command.candidate_generation_id
      end)
  end

  defp valid_action?(action, command) do
    valid_id?(action.target_id) and is_integer(action.ordinal) and action.ordinal >= 0 and
      action.action in [:no_action, :backfill, :rebuild, :operator_decision] and
      action.status == :planned and
      is_map(action.reason) and Payload.validate(action.reason, 65_536) == :ok and
      is_map(action.upstream_impact) and Payload.validate(action.upstream_impact, 65_536) == :ok and
      (is_nil(action.mapping_proof) or
         (is_map(action.mapping_proof) and Payload.validate(action.mapping_proof, 65_536) == :ok)) and
      is_list(action.pinned_input_generation_ids) and
      Payload.validate(action.pinned_input_generation_ids, 262_144) == :ok and
      valid_candidate_for_action?(action, command)
  end

  defp valid_candidate_for_action?(%{action: :rebuild, candidate_generation: candidate}, _command)
       when is_map(candidate) do
    valid_uuid?(candidate[:target_generation_id]) and valid_hash?(candidate[:descriptor_hash]) and
      is_map(candidate[:logical_relation]) and is_map(candidate[:physical_relation])
  end

  defp valid_candidate_for_action?(%{candidate_generation: nil}, _command), do: true
  defp valid_candidate_for_action?(_action, _command), do: false

  defp valid_items?(items, actions) do
    action_targets = MapSet.new(actions, & &1.target_id)
    identities = Enum.map(items, &{&1.target_id, &1.item_id})
    grouped = Enum.group_by(items, & &1.target_id)

    length(identities) == length(Enum.uniq(identities)) and
      Enum.all?(items, &valid_item?(&1, action_targets)) and
      Enum.all?(grouped, fn {_target_id, target_items} ->
        length(target_items) <= @max_items_per_target and
          Enum.map(target_items, & &1.ordinal) == Enum.to_list(0..(length(target_items) - 1))
      end)
  end

  defp exact_item_set?(payload, items) do
    payload = canonical_map(payload)

    payload["item_count"] == length(items) and
      payload["items_digest"] ==
        plan_hash(%{items: Enum.map(items, &canonical_plan_item/1)})
  end

  defp canonical_plan_item(item) do
    item
    |> Map.from_struct()
    |> canonical_map()
  end

  defp valid_item?(item, action_targets) do
    MapSet.member?(action_targets, item.target_id) and valid_id?(item.item_id) and
      valid_id?(item.window_key) and
      is_integer(item.ordinal) and item.ordinal >= 0 and
      item.work_kind in [:window, :full_load, :empty_generation] and
      (is_nil(item.candidate_generation_id) or valid_uuid?(item.candidate_generation_id)) and
      (is_nil(item.runtime_input_expectation) or
         (is_map(item.runtime_input_expectation) and
            Payload.validate(item.runtime_input_expectation, 8_192) == :ok)) and
      valid_item_window?(item)
  end

  defp valid_item_window?(%{
         work_kind: :full_load,
         window_key: "full_load",
         window_start: nil,
         window_end: nil
       }),
       do: true

  defp valid_item_window?(%{
         work_kind: :window,
         window_start: %DateTime{} = start_at,
         window_end: %DateTime{} = end_at
       }),
       do: DateTime.compare(start_at, end_at) == :lt

  defp valid_item_window?(%{
         work_kind: :empty_generation,
         window_start: %DateTime{} = start_at,
         window_end: %DateTime{} = end_at
       }),
       do: DateTime.compare(start_at, end_at) == :lt

  defp valid_item_window?(_item), do: false

  defp validate_start(command) do
    basic_command?(command) and valid_hash?(command.plan_hash) and
      valid_version?(command.expected_version)
      |> valid_or_error()
  end

  defp validate_cancel(command) do
    (basic_command?(command) and valid_reason?(command.reason)) |> valid_or_error()
  end

  defp validate_retry(command) do
    (basic_command?(command) and valid_hash?(command.plan_hash)) |> valid_or_error()
  end

  defp validate_claim_operation(command) do
    (valid_context?(command.workspace_context) and valid_id?(command.command_id) and
       valid_id?(command.owner_id) and
       valid_duration?(command.lease_duration_ms) and
       (is_nil(command.operation_id) or valid_id?(command.operation_id)))
    |> valid_or_error()
  end

  defp validate_transition_operation(command) do
    payloads = [
      command.result_marker,
      command.unknown_outcome,
      command.validation_result,
      command.terminal_error
    ]

    (basic_command?(command) and valid_id?(command.owner_id) and
       valid_positive?(command.fencing_token) and
       valid_version?(command.expected_version) and command.expected_states != [] and
       Enum.all?(command.expected_states, &(&1 in @operation_states)) and
       command.state in @operation_states and
       command.phase in @operation_phases and
       (is_nil(command.cleanup_state) or
          command.cleanup_state in ~w(not_started pending running complete failed)a) and
       Enum.all?(payloads, &(is_nil(&1) or (is_map(&1) and Payload.validate(&1, 262_144) == :ok))))
    |> valid_or_error()
  end

  defp validate_claim_items(command) do
    (valid_context?(command.workspace_context) and
       Enum.all?(
         [command.batch_id, command.operation_id, command.target_id, command.owner_id],
         &valid_id?/1
       ) and
       valid_duration?(command.lease_duration_ms) and command.limit in 1..500)
    |> valid_or_error()
  end

  defp validate_transition_item(command) do
    (basic_command?(command) and
       Enum.all?([command.target_id, command.item_id, command.owner_id], &valid_id?/1) and
       valid_positive?(command.fencing_token) and valid_version?(command.expected_version) and
       command.status in [:running, :succeeded, :failed, :cancelled, :outcome_unknown] and
       (is_nil(command.child_run_id) or valid_id?(command.child_run_id)) and
       (is_nil(command.materialization_id) or valid_id?(command.materialization_id)) and
       (is_nil(command.row_count) or (is_integer(command.row_count) and command.row_count >= 0)) and
       (is_nil(command.last_error) or
          (is_map(command.last_error) and Payload.validate(command.last_error, 65_536) == :ok)))
    |> valid_or_error()
  end

  defp validate_transition_action(command) do
    (basic_command?(command) and valid_id?(command.target_id) and valid_id?(command.owner_id) and
       valid_positive?(command.operation_fencing_token) and
       valid_version?(command.expected_version) and
       command.expected_statuses != [] and
       Enum.all?(command.expected_statuses, &(&1 in @action_statuses)) and
       command.status in @action_statuses and
       Enum.all?(
         [command.activation_intent, command.validation_result, command.terminal_error],
         fn value ->
           is_nil(value) or (is_map(value) and Payload.validate(value, 262_144) == :ok)
         end
       ))
    |> valid_or_error()
  end

  defp validate_activation(command) do
    (basic_command?(command) and valid_id?(command.target_id) and valid_id?(command.owner_id) and
       valid_positive?(command.operation_fencing_token) and
       valid_uuid?(command.previous_generation_id) and
       valid_uuid?(command.candidate_generation_id) and valid_id?(command.activation_token) and
       is_map(command.active_relation) and
       Payload.validate(command.active_relation, 16_384) == :ok and
       is_map(command.retired_relation) and
       Payload.validate(command.retired_relation, 16_384) == :ok and
       is_map(command.data_plane_marker) and
       Payload.validate(command.data_plane_marker, 65_536) == :ok and
       valid_hash?(command.physical_schema_fingerprint))
    |> valid_or_error()
  end

  defp validate_get(query) do
    (valid_context?(query.workspace_context) and valid_id?(query.operation_id))
    |> valid_or_error()
  end

  defp validate_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{ordinal: ordinal, target_id: target_id, item_id: item_id}
          when is_integer(ordinal) and
                 is_binary(target_id) and is_binary(item_id),
          page.after
        )

    (valid_context?(page.workspace_context) and valid_id?(page.operation_id) and
       (is_nil(page.target_id) or valid_id?(page.target_id)) and
       (is_nil(page.status) or page.status in @item_statuses) and cursor? and page.limit in 1..500)
    |> valid_or_error()
  end

  defp basic_command?(command) do
    valid_context?(command.workspace_context) and valid_id?(command.command_id) and
      valid_id?(command.operation_id) and match?(%DateTime{}, command.occurred_at)
  end

  defp valid_or_error(true), do: :ok
  defp valid_or_error(false), do: {:error, ErrorMapper.map(:invalid)}
  defp valid_context?(context), do: WorkspaceContext.valid?(context)
  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255
  defp valid_reason?(value), do: is_binary(value) and byte_size(value) in 1..4096
  defp valid_version?(value), do: is_integer(value) and value > 0
  defp valid_positive?(value), do: is_integer(value) and value > 0
  defp valid_uuid?(value), do: match?({:ok, _}, Ecto.UUID.cast(value))
  defp valid_hash?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp valid_duration?(value), do: is_integer(value) and value > 0 and value <= @max_lease_ms

  defp valid_range?(nil, nil), do: true

  defp valid_range?(%DateTime{} = start_at, %DateTime{} = end_at),
    do: DateTime.compare(start_at, end_at) == :lt

  defp valid_range?(_start_at, _end_at), do: false

  defp plan_hash(payload) do
    payload
    |> Serializer.encode_canonical!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp stale_plan_error do
    Error.new(:conflict, "rebuild plan is stale", details: %{reason_code: "rebuild_plan_stale"})
  end

  defp candidate_id(nil), do: nil
  defp candidate_id(candidate), do: candidate.target_generation_id
  defp choose(nil, current), do: current
  defp choose(value, _current), do: value
  defp optional_datetime(nil), do: nil
  defp optional_datetime(datetime), do: database_datetime(datetime)
  defp database_datetime(%DateTime{} = datetime), do: DateTime.add(datetime, 0, :microsecond)

  defp canonical_map(value) do
    value |> canonical_value() |> Map.new()
  end

  defp canonical_value(value) do
    {:ok, encoded} = CanonicalJSON.encode(value)
    Jason.decode!(encoded)
  end

  defp optional_map(nil), do: nil
  defp optional_map(value), do: canonical_map(value)
  defp optional_canonical(nil), do: nil
  defp optional_canonical(value), do: canonical_map(value)
  defp choose_canonical(nil, current), do: current
  defp choose_canonical(value, _current), do: canonical_map(value)

  defp maybe_operation(query, nil), do: query

  defp maybe_operation(query, operation_id),
    do: where(query, [operation], operation.operation_id == ^operation_id)

  defp maybe_target(query, nil), do: query
  defp maybe_target(query, target_id), do: where(query, [item], item.target_id == ^target_id)
  defp maybe_status(query, nil), do: query

  defp maybe_status(query, status),
    do: where(query, [item], item.status == ^Atom.to_string(status))

  defp after_item(query, nil), do: query

  defp after_item(query, %{ordinal: ordinal, target_id: target_id, item_id: item_id}) do
    where(
      query,
      [item],
      item.ordinal > ^ordinal or
        (item.ordinal == ^ordinal and item.target_id > ^target_id) or
        (item.ordinal == ^ordinal and item.target_id == ^target_id and item.item_id > ^item_id)
    )
  end

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end
end
