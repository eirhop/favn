defmodule FavnOrchestrator.RebuildDispatcher do
  @moduledoc "Distributed, fenced dispatcher for immutable rebuild operations."

  use GenServer

  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Manifest.Asset
  alias Favn.RelationRef
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TargetGenerationRelation
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Window.Anchor
  alias Favn.Window.Selection
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.ActivateRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildItems
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildAction
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildItem
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems
  alias FavnOrchestrator.Persistence.Queries.GetRebuildMaterialization
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision
  alias FavnOrchestrator.Persistence.Results.RebuildAction
  alias FavnOrchestrator.Persistence.Results.RebuildItem
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Rebuild.ItemPager
  alias FavnOrchestrator.Rebuild.Reconciliation
  alias FavnOrchestrator.Rebuild.Telemetry

  @default_interval_ms 1_000
  @default_lease_ms 30_000
  @default_batch_size 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      workspace_ids:
        case Keyword.get(opts, :workspace_ids) do
          nil -> nil
          workspace_ids -> Enum.uniq(workspace_ids)
        end,
      owner_id: Keyword.get(opts, :owner_id, owner_id()),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lease_ms: Keyword.get(opts, :lease_duration_ms, @default_lease_ms),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: dispatch(state)

  @impl true
  def handle_info(:dispatch, state), do: dispatch(state)

  defp dispatch(state) do
    _ =
      Lifecycle.with_admission(fn ->
        Enum.each(workspace_ids(state), &dispatch_workspace(&1, state))
      end)

    Process.send_after(self(), :dispatch, state.interval_ms)
    {:noreply, state}
  end

  defp workspace_ids(%{workspace_ids: workspace_ids}) when is_list(workspace_ids),
    do: workspace_ids

  defp workspace_ids(_state) do
    context = SystemContext.platform(:rebuild_dispatcher_workspace_discovery)

    case page_workspace_ids(context, nil, []) do
      {:ok, workspace_ids} ->
        workspace_ids

      {:error, reason} ->
        emit_error(nil, nil, :workspace_discovery, reason)
        []
    end
  end

  defp page_workspace_ids(context, cursor, acc) do
    case ManifestStore.page_workspaces(context, after: cursor, limit: 500) do
      {:ok, %{items: items, has_more?: true, next_cursor: next_cursor}} ->
        page_workspace_ids(context, next_cursor, [items | acc])

      {:ok, %{items: items}} ->
        {:ok, acc |> Enum.reverse() |> List.flatten() |> Kernel.++(items)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp dispatch_workspace(workspace_id, state) do
    context = SystemContext.workspace(workspace_id, :rebuild_dispatcher)

    case rebuild_store().claim_operation(%ClaimRebuildOperation{
           workspace_context: context,
           command_id: command_id("claim-operation", workspace_id <> ":" <> unique_identity()),
           owner_id: state.owner_id,
           lease_duration_ms: state.lease_ms
         }) do
      {:ok, nil} ->
        :ok

      {:ok, operation} ->
        with {:ok, locks} <- ensure_target_locks(context, operation, state),
             :ok <- process_operation(context, operation, locks, state) do
          :ok
        else
          {:error, %Error{kind: kind}} when kind in [:conflict, :fenced] ->
            Telemetry.execute(:lock_contention, %{count: 1}, %{
              workspace_id: workspace_id,
              operation_id: operation.operation_id,
              reason: kind
            })

            :ok

          {:error, reason} ->
            emit_error(workspace_id, operation.operation_id, :dispatch, reason)
        end

      {:error, %Error{kind: kind}} when kind in [:conflict, :fenced] ->
        Telemetry.execute(:lock_contention, %{count: 1}, %{
          workspace_id: workspace_id,
          operation_id: nil,
          reason: kind
        })

        :ok

      {:error, reason} ->
        emit_error(workspace_id, nil, :claim, reason)
    end
  end

  defp process_operation(context, %RebuildOperation{state: :queued} = operation, _locks, state) do
    with {:ok, _building} <- transition_operation(operation, context, state, :building, :building) do
      :ok
    end
  end

  defp process_operation(context, %RebuildOperation{state: :building} = operation, locks, state) do
    case Enum.find(operation.actions, &(&1.status != :succeeded)) do
      nil ->
        complete_operation(context, operation, locks, state)

      %RebuildAction{status: :failed} = action ->
        fail_operation(
          context,
          operation,
          locks,
          state,
          action.terminal_error || %{reason: "action_failed"}
        )

      %RebuildAction{status: :outcome_unknown} ->
        transition_operation(operation, context, state, :activation_unknown, :reconciling)
        |> ok_only()

      %RebuildAction{action: :no_action, status: :planned} = action ->
        transition_action(operation, action, context, state, :succeeded, []) |> ok_only()

      %RebuildAction{status: :planned} = action ->
        transition_action(operation, action, context, state, :running, []) |> ok_only()

      %RebuildAction{status: :running} = action ->
        process_action(context, operation, action, locks, state)
    end
  end

  defp process_operation(
         context,
         %RebuildOperation{state: :validating} = operation,
         locks,
         state
       ),
       do:
         timed_phase(:validation, context, operation, fn ->
           validate_current_action(context, operation, locks, state)
         end)

  defp process_operation(
         context,
         %RebuildOperation{state: :activating} = operation,
         locks,
         state
       ),
       do:
         timed_phase(:activation, context, operation, fn ->
           activate_current_action(context, operation, locks, state)
         end)

  defp process_operation(
         context,
         %RebuildOperation{state: :activation_unknown} = operation,
         _locks,
         state
       ) do
    transition_operation(operation, context, state, :reconciling, :reconciling) |> ok_only()
  end

  defp process_operation(
         context,
         %RebuildOperation{state: :reconciling} = operation,
         locks,
         state
       ),
       do:
         timed_phase(:reconciliation, context, operation, fn ->
           reconcile_activation(context, operation, locks, state)
         end)

  defp process_operation(
         context,
         %RebuildOperation{state: :cancelling} = operation,
         locks,
         state
       ),
       do: process_cancellation(context, operation, locks, state)

  defp process_operation(
         context,
         %RebuildOperation{state: :succeeded, cleanup_state: cleanup_state} = operation,
         locks,
         state
       )
       when cleanup_state in [:pending, :failed],
       do:
         timed_phase(:cleanup, context, operation, fn ->
           cleanup_retired_relations(context, operation, locks, state)
         end)

  defp process_operation(
         context,
         %RebuildOperation{state: terminal_state, cleanup_state: cleanup_state} = operation,
         locks,
         state
       )
       when terminal_state in [:failed, :cancelled] and cleanup_state in [:pending, :failed],
       do:
         timed_phase(:cleanup, context, operation, fn ->
           cleanup_abandoned_candidates(context, operation, locks, state)
         end)

  defp process_operation(_context, _operation, _locks, _state), do: :ok

  defp process_action(context, operation, action, locks, state) do
    with {:ok, claimed} <- claim_items(context, operation, action, state),
         :ok <- process_items(context, operation, action, claimed, state),
         :ok <- reconcile_running_items(context, operation, action, state),
         {:ok, current} <- reload(context, operation.operation_id),
         %RebuildAction{} = current_action <- action(current, action.target_id) do
      cond do
        Map.get(current_action.progress, :outcome_unknown, 0) > 0 ->
          unknown_action(context, current, current_action, locks, state)

        Map.get(current_action.progress, :failed, 0) > 0 ->
          fail_action(context, current, current_action, locks, state, %{
            reason: "candidate_item_failed"
          })

        current_action.progress.total > 0 and
            Map.get(current_action.progress, :succeeded, 0) == current_action.progress.total ->
          finish_built_action(context, current, current_action, state)

        true ->
          :ok
      end
    else
      nil -> {:error, :rebuild_action_not_found}
      {:error, _reason} = error -> error
    end
  end

  defp finish_built_action(context, operation, %RebuildAction{action: :backfill} = action, state) do
    transition_action(operation, action, context, state, :succeeded, []) |> ok_only()
  end

  defp finish_built_action(context, operation, %RebuildAction{action: :rebuild}, state) do
    transition_operation(operation, context, state, :validating, :validating) |> ok_only()
  end

  defp validate_current_action(context, operation, locks, state) do
    action = current_generation_action(operation)

    with %RebuildAction{} <- action,
         :ok <- validate_candidate_materializations(context, operation, action),
         {:ok, version, asset} <- version_asset(context, operation, action),
         {:ok, relation} <- candidate_relation(operation, action, asset),
         {:ok, fingerprint} <- inspect_candidate(version, asset, relation),
         :ok <- validate_candidate_identity(asset, relation, fingerprint),
         {:ok, checked_action} <-
           transition_action(operation, action, context, state, :running,
             validation_result: %{
               outcome: "succeeded",
               candidate_fingerprint: fingerprint.fingerprint,
               inspected_at: DateTime.utc_now()
             }
           ),
         activation_token <- activation_token(operation, action),
         {:ok, intended_action} <-
           transition_action(operation, checked_action, context, state, :running,
             activation_intent: activation_intent(operation, action, activation_token)
           ),
         {:ok, current} <- reload(context, operation.operation_id),
         {:ok, _activating} <-
           transition_operation(current, context, state, :activating, :activating,
             activation_token: activation_token,
             validation_result: intended_action.validation_result
           ) do
      :ok
    else
      nil ->
        fail_operation(context, operation, locks, state, %{reason: "rebuild_action_not_found"})

      {:error, reason} ->
        fail_validation(context, operation, action, locks, state, reason)
    end
  end

  defp activate_current_action(context, operation, locks, state) do
    action = current_generation_action(operation)

    with %RebuildAction{} <- action,
         {:ok, version, asset} <- version_asset(context, operation, action),
         {:ok, request} <- activation_request(operation, action, version, asset),
         runtime <- RuntimeConfig.current() do
      case RunnerDispatch.activate_generation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ) do
        {:ok, %GenerationActivationResult{outcome: :succeeded} = result} ->
          with :ok <- GenerationActivationResult.validate(result, request),
               {:ok, _action} <-
                 persist_activation(context, operation, action, request, result, state),
               {:ok, current} <- reload(context, operation.operation_id) do
            after_activation(context, current, locks, state)
          else
            {:error, reason} -> unknown_activation(context, operation, action, state, reason)
          end

        {:ok, %GenerationActivationResult{outcome: :safe_failure} = result} ->
          fail_action(context, operation, action, locks, state, error_payload(result.error))

        {:ok, %GenerationActivationResult{outcome: :outcome_unknown} = result} ->
          unknown_activation(context, operation, action, state, error_payload(result.error))

        {:error, reason} ->
          unknown_activation(context, operation, action, state, reason)

        invalid ->
          unknown_activation(
            context,
            operation,
            action,
            state,
            {:invalid_activation_result, invalid}
          )
      end
    else
      nil ->
        fail_operation(context, operation, locks, state, %{reason: "activation_action_not_found"})

      {:error, reason} ->
        unknown_activation(context, operation, action, state, reason)
    end
  end

  defp after_activation(context, operation, locks, state) do
    attrs = if operation.state == :reconciling, do: [unknown_outcome: %{}], else: []

    cond do
      operation.cancel_requested ->
        transition_operation(operation, context, state, :cancelling, :cleanup, attrs) |> ok_only()

      Enum.all?(operation.actions, &(&1.status == :succeeded)) ->
        complete_operation(context, operation, locks, state, attrs)

      true ->
        transition_operation(operation, context, state, :building, :repairing, attrs) |> ok_only()
    end
  end

  defp reconcile_activation(context, operation, locks, state) do
    case activation_action(operation) do
      %RebuildAction{} = action ->
        reconcile_activation_action(context, operation, action, locks, state)

      nil ->
        reconcile_unknown_action(context, operation, locks, state)
    end
  end

  defp reconcile_activation_action(context, operation, action, locks, state) do
    with {:ok, version, asset} <- version_asset(context, operation, action),
         {:ok, activation} <- activation_request(operation, action, version, asset),
         request <- %GenerationReconciliationRequest{activation: activation},
         runtime <- RuntimeConfig.current() do
      case RunnerDispatch.reconcile_generation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ) do
        {:ok, %GenerationReconciliationResult{disposition: :candidate_active} = result} ->
          with :ok <- GenerationReconciliationResult.validate(result, request),
               {:ok, _action} <-
                 persist_reconciled_activation(
                   context,
                   operation,
                   action,
                   activation,
                   result,
                   state
                 ),
               {:ok, current} <- reload(context, operation.operation_id) do
            after_activation(context, current, locks, state)
          end

        {:ok,
         %GenerationReconciliationResult{disposition: :previous_active, candidate_present: true} =
             result} ->
          with :ok <- GenerationReconciliationResult.validate(result, request) do
            case Reconciliation.next(operation.cancel_requested, :activation) do
              :cancel ->
                cancel_after_reconciliation(context, operation, action, state)

              :activate ->
                with {:ok, _running} <-
                       transition_action(operation, action, context, state, :running,
                         terminal_error: %{}
                       ),
                     {:ok, current} <- reload(context, operation.operation_id),
                     {:ok, _activating} <-
                       transition_operation(current, context, state, :activating, :activating,
                         unknown_outcome: %{},
                         activation_token: activation.activation_token
                       ) do
                  :ok
                end
            end
          end

        {:ok, %GenerationReconciliationResult{disposition: :previous_active}} ->
          fail_action(context, operation, action, locks, state, %{
            reason: "candidate_relation_missing"
          })

        {:ok, %GenerationReconciliationResult{disposition: :unknown}} ->
          :ok

        {:error, _reason} ->
          :ok

        _invalid ->
          :ok
      end
    else
      _missing -> :ok
    end
  end

  defp reconcile_unknown_action(context, operation, _locks, state) do
    case Enum.find(operation.actions, &(&1.status == :outcome_unknown)) do
      nil ->
        :ok

      action ->
        with :ok <- reconcile_unknown_items(context, operation, action, state),
             {:ok, current} <- reload(context, operation.operation_id),
             %RebuildAction{} = current_action <- action(current, action.target_id) do
          cond do
            Map.get(current_action.progress, :outcome_unknown, 0) > 0 ->
              :ok

            Reconciliation.next(current.cancel_requested, :items) == :cancel ->
              cancel_after_reconciliation(context, current, current_action, state)

            true ->
              with {:ok, _running} <-
                     transition_action(current, current_action, context, state, :running,
                       terminal_error: %{}
                     ),
                   {:ok, reloaded} <- reload(context, operation.operation_id),
                   {:ok, _building} <-
                     transition_operation(reloaded, context, state, :building, :repairing,
                       unknown_outcome: %{}
                     ) do
                :ok
              end
          end
        else
          nil -> {:error, :rebuild_action_not_found}
          {:error, _reason} = error -> error
        end
    end
  end

  defp cancel_after_reconciliation(context, operation, action, state) do
    with {:ok, _cancelled_action} <-
           transition_action(operation, action, context, state, :cancelled, terminal_error: %{}),
         {:ok, current} <- reload(context, operation.operation_id),
         {:ok, _cancelling} <-
           transition_operation(current, context, state, :cancelling, :cleanup,
             unknown_outcome: %{},
             cleanup_state: :pending
           ) do
      :ok
    end
  end

  defp process_cancellation(context, operation, locks, state) do
    with :ok <- claim_expired_active_items(context, operation, state),
         :ok <- cancel_active_items(context, operation, state),
         :ok <- reconcile_all_unknown_items(context, operation, state),
         {:ok, current} <- reload(context, operation.operation_id) do
      active_count =
        Map.get(current.progress, :claimed, 0) + Map.get(current.progress, :running, 0) +
          Map.get(current.progress, :outcome_unknown, 0)

      if active_count == 0 do
        with :ok <- discard_inactive_candidates(context, current, state),
             {:ok, cancelled} <-
               transition_operation(current, context, state, :cancelled, :terminal,
                 cleanup_state: :complete
               ) do
          release_locks(context, cancelled, locks)
        end
      else
        :ok
      end
    end
  end

  defp reconcile_all_unknown_items(context, operation, state) do
    operation.actions
    |> Enum.reduce_while(:ok, fn action, :ok ->
      case reconcile_unknown_items(context, operation, action, state) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp reconcile_unknown_items(context, operation, action, state) do
    with {:ok, items} <-
           ItemPager.all(
             rebuild_store(),
             context,
             operation.operation_id,
             target_id: action.target_id,
             status: :outcome_unknown
           ) do
      Enum.reduce_while(items, :ok, fn item, :ok ->
        case reconcile_materialization(context, operation, item, state, :outcome_unknown) do
          :pending -> {:cont, :ok}
          {:ok, _item} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp claim_expired_active_items(context, operation, state) do
    operation.actions
    |> Enum.filter(&(&1.status in [:running, :planned]))
    |> Enum.reduce_while(:ok, fn action, :ok ->
      case claim_items(context, operation, action, state) do
        {:ok, _items} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp cancel_active_items(context, operation, state) do
    [:claimed, :running]
    |> Enum.reduce_while(:ok, fn status, :ok ->
      case page_all_items(context, operation.operation_id, status) do
        {:ok, items} ->
          items
          |> Enum.filter(&(&1.claim_owner == state.owner_id))
          |> Enum.reduce_while(:ok, fn item, :ok ->
            case cancel_item(context, operation, item, state) do
              :ok -> {:cont, :ok}
              {:ok, _item} -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp cancel_item(context, operation, item, state) do
    case item.child_run_id do
      run_id when is_binary(run_id) ->
        _ = RunManager.cancel_run(context, run_id, %{reason: "rebuild_cancelled"})
        reconcile_run(context, operation, item, state)

      _missing ->
        transition_item(context, operation, item, state, :cancelled, nil, nil)
    end
  end

  defp discard_inactive_candidates(context, operation, state) do
    Enum.reduce_while(operation.actions, :ok, fn
      %RebuildAction{action: :rebuild, activated_at: nil} = action, :ok ->
        case discard_candidate(context, operation, action) do
          :ok ->
            case transition_candidate_generation(context, operation, action, state, :discarded) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          {:error, reason} ->
            {:halt, {:error, reason}}
        end

      _action, :ok ->
        {:cont, :ok}
    end)
  end

  defp discard_candidate(context, operation, action) do
    with {:ok, version, asset} <- version_asset(context, operation, action),
         {:ok, candidate} <- candidate_relation(operation, action, asset),
         request <- %GenerationDiscardRequest{
           manifest_version_id: version.manifest_version_id,
           manifest_content_hash: version.content_hash,
           required_runner_release_id: version.required_runner_release_id,
           rebuild_operation_id: operation.operation_id,
           rebuild_action_id: action.target_id,
           target_id: action.target_id,
           candidate_generation_id: action.candidate_generation_id,
           active_relation: RelationRef.new!(asset.relation),
           candidate_relation: candidate,
           discard_token: command_id("discard", operation.operation_id <> ":" <> action.target_id)
         },
         runtime <- RuntimeConfig.current(),
         {:ok, %GenerationDiscardResult{} = result} <-
           RunnerDispatch.discard_generation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ),
         :ok <- GenerationDiscardResult.validate(result, request),
         true <- result.outcome in [:discarded, :already_absent] do
      :ok
    else
      false -> {:error, :candidate_discard_failed}
      {:error, _reason} = error -> error
    end
  end

  defp claim_items(context, operation, action, state) do
    case ensure_candidate_item_serial(context, operation, action) do
      :busy ->
        {:ok, []}

      :ok ->
        result =
          rebuild_store().claim_items(%ClaimRebuildItems{
            workspace_context: context,
            batch_id:
              command_id(
                "claim-items",
                operation.operation_id <> ":" <> action.target_id <> ":" <> unique_identity()
              ),
            operation_id: operation.operation_id,
            target_id: action.target_id,
            owner_id: state.owner_id,
            lease_duration_ms: state.lease_ms,
            limit: if(action.action == :rebuild, do: 1, else: state.batch_size)
          })

        case result do
          {:ok, items} ->
            Telemetry.execute(:item_dispatch, %{count: length(items)}, %{
              workspace_id: context.workspace_id,
              operation_id: operation.operation_id,
              target_id: action.target_id,
              action: action.action,
              outcome: :claimed
            })

          {:error, reason} ->
            Telemetry.execute(:item_dispatch, %{count: 0}, %{
              workspace_id: context.workspace_id,
              operation_id: operation.operation_id,
              target_id: action.target_id,
              action: action.action,
              outcome: :error,
              reason: Telemetry.reason_kind(reason)
            })
        end

        result

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_candidate_item_serial(_context, _operation, %RebuildAction{action: action})
       when action != :rebuild,
       do: :ok

  defp ensure_candidate_item_serial(context, operation, action) do
    with {:ok, claimed} <- page_items(context, operation.operation_id, action.target_id, :claimed),
         {:ok, running} <- page_items(context, operation.operation_id, action.target_id, :running) do
      if claimed == [] and running == [], do: :ok, else: :busy
    end
  end

  defp process_items(context, operation, action, items, state) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case process_item(context, operation, action, item, state) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp process_item(context, operation, action, %RebuildItem{status: :claimed} = item, state) do
    run_id = item.child_run_id || child_run_id(operation, item)

    result =
      case Runs.get(context, run_id) do
        {:ok, %RunState{}} ->
          {:ok, run_id}

        {:error, %Error{kind: :not_found}} ->
          submit_item(context, operation, action, item, run_id)

        {:error, reason} ->
          {:error, reason}
      end

    case result do
      {:ok, ^run_id} ->
        with {:ok, running} <-
               transition_item(context, operation, item, state, :running, run_id, nil) do
          reconcile_run(context, operation, running, state)
        end

      {:error, reason} ->
        case Runs.get(context, run_id) do
          {:ok, %RunState{}} ->
            with {:ok, running} <-
                   transition_item(context, operation, item, state, :running, run_id, nil) do
              reconcile_run(context, operation, running, state)
            end

          {:error, %Error{kind: :not_found}} ->
            transition_item(
              context,
              operation,
              item,
              state,
              :failed,
              nil,
              safe_failure(reason)
            )
            |> ok_only()

          {:error, lookup_error} ->
            {:error, lookup_error}
        end
    end
  end

  defp process_item(context, operation, _action, %RebuildItem{status: :running} = item, state),
    do: reconcile_run(context, operation, item, state)

  defp process_item(_context, _operation, _action, _item, _state), do: :ok

  defp reconcile_running_items(context, operation, action, state) do
    with {:ok, running} <- page_items(context, operation.operation_id, action.target_id, :running) do
      running
      |> Enum.filter(&(&1.claim_owner == state.owner_id))
      |> Enum.reduce_while(:ok, fn item, :ok ->
        case reconcile_run(context, operation, item, state) do
          :ok -> {:cont, :ok}
          {:ok, _item} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp reconcile_run(context, operation, %RebuildItem{child_run_id: run_id} = item, state)
       when is_binary(run_id) do
    case Runs.get(context, run_id) do
      {:ok, %RunState{status: :ok}} ->
        reconcile_materialization(context, operation, item, state, :run_succeeded)

      {:ok, %RunState{status: :cancelled}} ->
        case reconcile_materialization(context, operation, item, state, :run_cancelled) do
          :pending -> transition_item(context, operation, item, state, :cancelled, run_id, nil)
          result -> result
        end

      {:ok, %RunState{status: status, error: error}}
      when status in [:error, :partial, :timed_out] ->
        case reconcile_materialization(context, operation, item, state, status) do
          :pending ->
            case classified_failure(error || status) do
              {:failed, payload} ->
                transition_item(context, operation, item, state, :failed, run_id, payload)

              {:outcome_unknown, _payload} ->
                transition_item(
                  context,
                  operation,
                  item,
                  state,
                  :outcome_unknown,
                  run_id,
                  unknown_failure(error || status)
                )
            end

          result ->
            result
        end

      {:ok, %RunState{}} ->
        :ok

      {:error, %Error{kind: :not_found}} ->
        case reconcile_materialization(context, operation, item, state, :run_not_found) do
          :pending ->
            transition_item(
              context,
              operation,
              item,
              state,
              :outcome_unknown,
              run_id,
              unknown_failure(:run_not_found)
            )

          result ->
            result
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reconcile_run(_context, _operation, _item, _state), do: :ok

  defp reconcile_materialization(context, operation, item, state, _run_status) do
    generation_id =
      item.candidate_generation_id || active_generation_id(operation, item.target_id)

    query = %GetRebuildMaterialization{
      workspace_context: context,
      operation_id: operation.operation_id,
      target_id: item.target_id,
      run_id: item.child_run_id,
      target_generation_id: generation_id,
      partition_key: item_partition_key(item)
    }

    case Persistence.stores().materialization.get_rebuild(query) do
      {:ok, %MaterializationDecision{status: :materialized, materialization: materialization}} ->
        with :ok <- validate_materialization(operation, item, materialization) do
          transition_item(
            context,
            operation,
            item,
            state,
            :succeeded,
            item.child_run_id,
            nil,
            materialization_id: materialization.materialization_id,
            row_count: field(materialization.payload, :rows_affected)
          )
        end

      {:ok, %MaterializationDecision{status: :failed, claim: claim}} ->
        case classified_failure(claim.error) do
          {:failed, payload} ->
            transition_item(
              context,
              operation,
              item,
              state,
              :failed,
              item.child_run_id,
              payload
            )

          {:outcome_unknown, _payload} when item.status == :outcome_unknown ->
            :pending

          {:outcome_unknown, payload} ->
            transition_item(
              context,
              operation,
              item,
              state,
              :outcome_unknown,
              item.child_run_id,
              payload
            )
        end

      {:ok, %MaterializationDecision{status: status}}
      when status in [:claimed, :competing, :missing] ->
        :pending

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_materialization(operation, item, materialization) do
    payload = materialization.payload
    action = action(operation, item.target_id)
    expected_package = field(operation.plan_payload, :execution_packages) |> field(item.target_id)

    assurance =
      field(operation.plan_payload, :assurance_expectations)
      |> field(item.target_id) || %{contract_required: false, checks: []}

    valid_identity? =
      materialization.run_id == item.child_run_id and
        materialization.target_id == item.target_id and
        materialization.target_generation_id ==
          (item.candidate_generation_id || active_generation_id(operation, item.target_id)) and
        materialization.partition_key == item_partition_key(item) and
        field(payload, :manifest_content_hash) ==
          field(operation.plan_payload, :manifest_content_hash) and
        (is_nil(expected_package) or field(payload, :execution_package_hash) == expected_package)

    checks_valid? =
      not final_candidate_item?(action, item) or
        (valid_check_results?(field(payload, :check_results), field(assurance, :checks)) and
           valid_contract_validation?(
             field(payload, :contract_validation),
             field(assurance, :contract_required)
           ))

    if valid_identity? and checks_valid?,
      do: :ok,
      else: {:error, :rebuild_materialization_evidence_mismatch}
  end

  defp valid_check_results?(results, expected) when is_list(results) and is_list(expected) do
    identities = Enum.map(results, &check_identity/1)
    expected_identities = Enum.map(expected, &check_identity/1)

    identities == expected_identities and
      Enum.all?(results, fn result ->
        field(result, :outcome) in [
          "passed",
          "warned",
          "condition_skipped",
          :passed,
          :warned,
          :condition_skipped
        ]
      end)
  end

  defp valid_check_results?(_results, _expected), do: false

  defp check_identity(check) do
    {
      check |> field(:name) |> normalize_token(),
      check |> field(:origin) |> normalize_token(),
      field(check, :claim_id),
      check |> field(:phase) |> normalize_token()
    }
  end

  defp normalize_token(nil), do: nil
  defp normalize_token(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_token(value) when is_binary(value), do: value
  defp normalize_token(value), do: to_string(value)

  defp valid_contract_validation?(validation, false),
    do: is_nil(validation) or field(validation, :status) in [:passed, "passed"]

  defp valid_contract_validation?(validation, true),
    do: field(validation, :status) in [:passed, "passed"]

  defp final_candidate_item?(%RebuildAction{action: :rebuild} = action, item),
    do: item.ordinal == action.progress.total - 1

  defp final_candidate_item?(_action, _item), do: false

  defp active_generation_id(operation, target_id) do
    operation.plan_payload
    |> field(:binding_snapshot)
    |> field(target_id)
    |> field(:active_generation_id)
  end

  defp item_partition_key(%RebuildItem{work_kind: :full_load}), do: FreshnessKey.latest()
  defp item_partition_key(%RebuildItem{window_key: window_key}), do: window_key

  defp submit_item(context, operation, action, item, run_id) do
    with {:ok, version, asset} <- version_asset(context, operation, action),
         {:ok, options} <- submission_options(operation, action, item, version, asset, run_id) do
      RunManager.submit_asset_run(context, asset.ref, options)
    end
  end

  defp submission_options(operation, action, item, version, asset, run_id) do
    with {:ok, window_selection} <- item_selection(item, asset),
         {:ok, rebuild} <- rebuild_submission(operation, action, item, asset) do
      options = [
        run_id: run_id,
        manifest_version_id: version.manifest_version_id,
        dependencies: :none,
        rebuild: rebuild,
        refresh: :force,
        metadata: %{
          rebuild_operation_id: operation.operation_id,
          rebuild_action_id: action.target_id,
          rebuild_item_id: item.item_id,
          rebuild_evaluated_at: operation.evaluated_at,
          runtime_input_expectation: item.runtime_input_expectation
        }
      ]

      {:ok,
       if(window_selection,
         do: Keyword.put(options, :window_selection, window_selection),
         else: options
       )}
    end
  end

  defp item_selection(%RebuildItem{work_kind: :full_load}, _asset), do: {:ok, nil}

  defp item_selection(%RebuildItem{} = item, %Asset{window: window}) do
    with {:ok, anchor} <-
           Anchor.new(window.kind, item.window_start, item.window_end, timezone: window.timezone),
         {:ok, selection} <- Selection.backfill([anchor], window.timezone) do
      {:ok, selection}
    end
  end

  defp rebuild_submission(operation, %RebuildAction{action: :rebuild} = action, item, asset) do
    with {:ok, candidate} <- candidate_relation(operation, action, asset) do
      {:ok,
       %{
         target_id: action.target_id,
         candidate_generation_id: action.candidate_generation_id,
         active_relation: RelationRef.new!(asset.relation),
         candidate_relation: candidate,
         input_generations: runtime_input_pins(action.pinned_input_generation_ids),
         operation_id: operation.operation_id,
         action_id: action.target_id,
         item_id: item.item_id,
         target_operation: :rebuild_candidate,
         empty_generation: item.work_kind == :empty_generation,
         final_item: item.ordinal == action.progress.total - 1
       }}
    end
  end

  defp rebuild_submission(operation, %RebuildAction{action: :backfill} = action, item, asset) do
    snapshot = field(operation.plan_payload, :binding_snapshot) |> field(action.target_id)
    generation_id = field(snapshot, :active_generation_id)
    relation = RelationRef.new!(asset.relation)

    {:ok,
     %{
       target_id: action.target_id,
       candidate_generation_id: generation_id,
       active_relation: relation,
       candidate_relation: relation,
       input_generations: runtime_input_pins(action.pinned_input_generation_ids),
       operation_id: operation.operation_id,
       action_id: action.target_id,
       item_id: item.item_id,
       target_operation: :normal_materialization
     }}
  end

  defp runtime_input_pins(pins) do
    Enum.map(pins, fn pin ->
      %{
        target_id: field(pin, :target_id),
        evidence_generation_id: field(pin, :evidence_generation_id),
        target_generation_id: field(pin, :target_generation_id),
        physical_relation: field(pin, :physical_relation)
      }
    end)
  end

  defp inspect_candidate(version, asset, candidate_relation) do
    runtime = RuntimeConfig.current()

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      relation: candidate_relation,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    with {:ok, result} <-
           RunnerDispatch.inspect_relation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ),
         {:ok, %PhysicalFingerprint{} = fingerprint} <-
           PhysicalFingerprint.from_inspection(result) do
      _ = asset
      {:ok, fingerprint}
    end
  end

  defp validate_candidate_identity(asset, candidate_relation, fingerprint) do
    diffs = PhysicalFingerprint.identity_diff(asset.target_descriptor, fingerprint)

    non_relation_diffs = Enum.reject(diffs, &(Map.get(&1, :field) == :relation))
    observed = fingerprint.relation

    relation_matches? =
      observed.catalog == candidate_relation.catalog and
        observed.schema == candidate_relation.schema and
        observed.name == candidate_relation.name and observed.kind == "table"

    if relation_matches? and non_relation_diffs == [],
      do: :ok,
      else: {:error, {:candidate_validation_failed, non_relation_diffs}}
  end

  defp validate_candidate_materializations(context, operation, action) do
    with {:ok, count} <-
           ItemPager.count(
             rebuild_store(),
             context,
             operation.operation_id,
             [
               target_id: action.target_id,
               status: :succeeded
             ],
             &(is_binary(&1.materialization_id) and &1.materialization_id != "")
           ) do
      if count == action.progress.total,
        do: :ok,
        else: {:error, :candidate_materialization_evidence_incomplete}
    end
  end

  defp activation_request(operation, action, version, asset) do
    with {:ok, candidate} <- candidate_relation(operation, action, asset),
         {:ok, previous_marker} <- previous_marker(operation, action, asset),
         fingerprint when is_binary(fingerprint) <-
           field(action.validation_result || %{}, :candidate_fingerprint),
         intent when is_map(intent) <- action.activation_intent,
         token when is_binary(token) <- field(intent, :activation_token) do
      max_identifier_bytes = capability(operation, action.target_id, :max_identifier_bytes)
      active = RelationRef.new!(asset.relation)

      {:ok,
       %GenerationActivationRequest{
         manifest_version_id: version.manifest_version_id,
         manifest_content_hash: version.content_hash,
         required_runner_release_id: version.required_runner_release_id,
         rebuild_operation_id: operation.operation_id,
         rebuild_action_id: action.target_id,
         target_id: action.target_id,
         previous_generation_id: previous_marker.active_generation_id,
         candidate_generation_id: action.candidate_generation_id,
         active_relation: active,
         candidate_relation: candidate,
         retired_relation:
           TargetGenerationRelation.retired(
             active,
             previous_marker.active_generation_id,
             max_identifier_bytes
           ),
         expected_candidate_fingerprint: fingerprint,
         activation_token: token,
         expected_marker: previous_marker
       }}
    else
      _missing -> {:error, :incomplete_activation_checkpoint}
    end
  end

  defp previous_marker(operation, action, asset) do
    snapshot = field(operation.plan_payload, :binding_snapshot) |> field(action.target_id)
    marker = field(snapshot, :active_data_plane_marker)

    with marker when is_map(marker) <- marker,
         {:ok, activated_at} <- datetime(field(marker, :activated_at)) do
      {:ok,
       %GenerationMarker{
         target_id: action.target_id,
         active_relation: RelationRef.new!(asset.relation),
         active_generation_id: field(marker, :active_generation_id),
         activation_operation_id: field(marker, :activation_operation_id),
         activation_token: field(marker, :activation_token),
         activated_at: activated_at
       }}
    else
      _invalid -> {:error, :invalid_previous_generation_marker}
    end
  end

  defp candidate_relation(operation, action, asset) do
    max_identifier_bytes = capability(operation, action.target_id, :max_identifier_bytes)

    if is_integer(max_identifier_bytes) do
      {:ok,
       TargetGenerationRelation.candidate(
         RelationRef.new!(asset.relation),
         action.candidate_generation_id,
         max_identifier_bytes
       )}
    else
      {:error, :missing_generation_capabilities}
    end
  rescue
    ArgumentError -> {:error, :invalid_candidate_relation}
  end

  defp activation_intent(operation, action, token) do
    snapshot = field(operation.plan_payload, :binding_snapshot) |> field(action.target_id)

    %{
      activation_token: token,
      previous_generation_id: field(snapshot, :active_generation_id),
      candidate_generation_id: action.candidate_generation_id,
      dispatched: false
    }
  end

  defp persist_activation(context, operation, action, request, result, state) do
    marker = marker_map(result.observed_marker)

    rebuild_store().activate_generation(%ActivateRebuildGeneration{
      workspace_context: context,
      command_id:
        command_id("activate-binding", operation.operation_id <> ":" <> action.target_id),
      operation_id: operation.operation_id,
      target_id: action.target_id,
      owner_id: state.owner_id,
      operation_fencing_token: operation.dispatcher.fencing_token,
      previous_generation_id: field(action.activation_intent, :previous_generation_id),
      candidate_generation_id: action.candidate_generation_id,
      activation_token: field(action.activation_intent, :activation_token),
      active_relation: Map.from_struct(request.active_relation),
      retired_relation: Map.from_struct(request.retired_relation),
      data_plane_marker: marker,
      physical_schema_fingerprint: result.physical_fingerprint,
      occurred_at: result.completed_at
    })
  end

  defp persist_reconciled_activation(context, operation, action, request, result, state) do
    rebuild_store().activate_generation(%ActivateRebuildGeneration{
      workspace_context: context,
      command_id:
        command_id("reconcile-binding", operation.operation_id <> ":" <> action.target_id),
      operation_id: operation.operation_id,
      target_id: action.target_id,
      owner_id: state.owner_id,
      operation_fencing_token: operation.dispatcher.fencing_token,
      previous_generation_id: field(action.activation_intent, :previous_generation_id),
      candidate_generation_id: action.candidate_generation_id,
      activation_token: field(action.activation_intent, :activation_token),
      active_relation: Map.from_struct(request.active_relation),
      retired_relation: Map.from_struct(request.retired_relation),
      data_plane_marker: marker_map(result.observed_marker),
      physical_schema_fingerprint: result.physical_fingerprint,
      occurred_at: result.reconciled_at
    })
  end

  defp marker_map(marker) do
    marker
    |> Map.from_struct()
    |> Map.update!(:active_relation, &Map.from_struct/1)
  end

  defp unknown_activation(context, operation, action, state, reason) do
    with {:ok, _unknown_action} <-
           transition_action(operation, action, context, state, :outcome_unknown,
             terminal_error: unknown_failure(reason)
           ),
         {:ok, current} <- reload(context, operation.operation_id),
         {:ok, _unknown_operation} <-
           transition_operation(current, context, state, :activation_unknown, :reconciling,
             unknown_outcome: unknown_failure(reason)
           ) do
      :ok
    end
  end

  defp unknown_action(context, operation, action, _locks, state) do
    transition_action(operation, action, context, state, :outcome_unknown,
      terminal_error: unknown_failure(:candidate_item_outcome_unknown)
    )
    |> ok_only()
  end

  defp fail_validation(context, operation, action, locks, state, reason) do
    if action do
      _ = transition_candidate_generation(context, operation, action, state, :failed)

      _ =
        transition_action(operation, action, context, state, :failed,
          validation_result: %{outcome: "failed", reason: inspect_reason(reason)},
          terminal_error: safe_failure(reason)
        )
    end

    fail_operation(context, operation, locks, state, safe_failure(reason), :pending)
  end

  defp fail_action(context, operation, action, locks, state, reason) do
    with :ok <- maybe_fail_candidate_generation(context, operation, action, state),
         {:ok, _failed_action} <-
           transition_action(operation, action, context, state, :failed,
             terminal_error: canonical_error(reason)
           ) do
      fail_operation(context, operation, locks, state, reason)
    end
  end

  defp maybe_fail_candidate_generation(
         context,
         operation,
         %RebuildAction{action: :rebuild, activated_at: nil} = action,
         state
       ),
       do: transition_candidate_generation(context, operation, action, state, :failed)

  defp maybe_fail_candidate_generation(_context, _operation, _action, _state), do: :ok

  defp transition_candidate_generation(context, operation, action, state, status) do
    rebuild_store().transition_generation(%TransitionRebuildGeneration{
      workspace_context: context,
      command_id:
        command_id(
          "candidate-#{status}",
          operation.operation_id <> ":" <> action.target_id
        ),
      operation_id: operation.operation_id,
      target_id: action.target_id,
      candidate_generation_id: action.candidate_generation_id,
      owner_id: state.owner_id,
      operation_fencing_token: operation.dispatcher.fencing_token,
      status: status,
      occurred_at: DateTime.utc_now()
    })
  end

  defp fail_operation(context, operation, locks, state, reason, cleanup_state \\ :not_started) do
    with {:ok, failed} <-
           transition_operation(operation, context, state, :failed, :terminal,
             terminal_error: canonical_error(reason),
             cleanup_state: cleanup_state
           ) do
      release_locks(context, failed, locks)
    end
  end

  defp cleanup_abandoned_candidates(context, operation, locks, state) do
    cleanup_state =
      case discard_inactive_candidates(context, operation, state) do
        :ok -> :complete
        {:error, _reason} -> :failed
      end

    with {:ok, terminal} <-
           transition_operation(
             operation,
             context,
             state,
             operation.state,
             :terminal,
             cleanup_state: cleanup_state
           ) do
      release_locks(context, terminal, locks)
    end
  end

  defp cleanup_retired_relations(context, operation, locks, state) do
    {current, cleanup_state} =
      operation.actions
      |> Enum.filter(fn action ->
        action.action == :rebuild and action.activated_at != nil and
          action.cleanup_state != :complete
      end)
      |> Enum.reduce({operation, :complete}, fn planned_action, {current, overall} ->
        current_action = action(current, planned_action.target_id)

        case discard_retired_relation(context, current, current_action) do
          :ok ->
            case transition_action(current, current_action, context, state, :succeeded,
                   cleanup_state: :complete,
                   terminal_error: %{}
                 ) do
              {:ok, _updated_action} ->
                case reload(context, current.operation_id) do
                  {:ok, reloaded} -> {reloaded, overall}
                  {:error, _reason} -> {current, :failed}
                end

              {:error, _reason} ->
                {current, :failed}
            end

          {:error, reason} ->
            _ =
              transition_action(current, current_action, context, state, :succeeded,
                cleanup_state: :failed,
                terminal_error: safe_failure(reason)
              )

            reloaded =
              case reload(context, current.operation_id) do
                {:ok, value} -> value
                {:error, _reason} -> current
              end

            {reloaded, :failed}
        end
      end)

    with {:ok, terminal} <-
           transition_operation(current, context, state, :succeeded, :terminal,
             cleanup_state: cleanup_state
           ) do
      release_locks(context, terminal, locks)
    end
  end

  defp discard_retired_relation(context, operation, action) do
    with {:ok, version, asset} <- version_asset(context, operation, action),
         previous_generation_id when is_binary(previous_generation_id) <-
           field(action.activation_intent, :previous_generation_id),
         max_identifier_bytes when is_integer(max_identifier_bytes) <-
           capability(operation, action.target_id, :max_identifier_bytes),
         active <- RelationRef.new!(asset.relation),
         retired <-
           TargetGenerationRelation.retired(
             active,
             previous_generation_id,
             max_identifier_bytes
           ),
         request <- %GenerationDiscardRequest{
           manifest_version_id: version.manifest_version_id,
           manifest_content_hash: version.content_hash,
           required_runner_release_id: version.required_runner_release_id,
           rebuild_operation_id: operation.operation_id,
           rebuild_action_id: action.target_id,
           target_id: action.target_id,
           candidate_generation_id: previous_generation_id,
           active_relation: active,
           candidate_relation: retired,
           relation_kind: :retired,
           discard_token:
             command_id("cleanup-retired", operation.operation_id <> ":" <> action.target_id)
         },
         runtime <- RuntimeConfig.current(),
         {:ok, %GenerationDiscardResult{} = result} <-
           RunnerDispatch.discard_generation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ),
         :ok <- GenerationDiscardResult.validate(result, request),
         true <- result.outcome in [:discarded, :already_absent] do
      :ok
    else
      false -> {:error, :retired_relation_cleanup_failed}
      {:error, _reason} = error -> error
      _missing -> {:error, :incomplete_retired_relation_cleanup_checkpoint}
    end
  rescue
    ArgumentError -> {:error, :invalid_retired_relation_cleanup_checkpoint}
  end

  defp complete_operation(context, operation, locks, state, attrs \\ []) do
    with {:ok, succeeded} <-
           transition_operation(
             operation,
             context,
             state,
             :succeeded,
             :terminal,
             Keyword.put(attrs, :cleanup_state, :pending)
           ) do
      release_locks(context, succeeded, locks)
    end
  end

  defp transition_operation(operation, context, state, next_state, phase, attrs \\ []) do
    result =
      rebuild_store().transition_operation(%TransitionRebuildOperation{
        workspace_context: context,
        command_id:
          command_id(
            "operation-#{next_state}-#{phase}",
            operation.operation_id <> ":" <> Integer.to_string(operation.version)
          ),
        operation_id: operation.operation_id,
        owner_id: state.owner_id,
        fencing_token: operation.dispatcher.fencing_token,
        expected_version: operation.version,
        expected_states: [operation.state],
        state: next_state,
        phase: phase,
        activation_token: Keyword.get(attrs, :activation_token),
        result_marker: Keyword.get(attrs, :result_marker),
        unknown_outcome: Keyword.get(attrs, :unknown_outcome),
        validation_result: Keyword.get(attrs, :validation_result),
        terminal_error: Keyword.get(attrs, :terminal_error),
        cleanup_state: Keyword.get(attrs, :cleanup_state),
        occurred_at: DateTime.utc_now()
      })

    case result do
      {:ok, updated} ->
        Telemetry.execute(:transition, %{count: 1}, %{
          workspace_id: context.workspace_id,
          operation_id: operation.operation_id,
          from_state: operation.state,
          state: updated.state,
          phase: updated.phase,
          cleanup_state: updated.cleanup_state,
          outcome:
            if(updated.state in [:succeeded, :failed, :cancelled],
              do: updated.state,
              else: :progress
            )
        })

      {:error, _reason} ->
        :ok
    end

    result
  end

  defp transition_action(operation, action, context, state, status, attrs) do
    rebuild_store().transition_action(%TransitionRebuildAction{
      workspace_context: context,
      command_id:
        command_id(
          "action-#{status}",
          :erlang.term_to_binary(
            {operation.operation_id, action.target_id, action.version, status, attrs},
            [:deterministic]
          )
        ),
      operation_id: operation.operation_id,
      target_id: action.target_id,
      owner_id: state.owner_id,
      operation_fencing_token: operation.dispatcher.fencing_token,
      expected_version: action.version,
      expected_statuses: [action.status],
      status: status,
      activation_intent: Keyword.get(attrs, :activation_intent),
      validation_result: Keyword.get(attrs, :validation_result),
      terminal_error: Keyword.get(attrs, :terminal_error),
      cleanup_state: Keyword.get(attrs, :cleanup_state),
      activated_at: Keyword.get(attrs, :activated_at),
      occurred_at: DateTime.utc_now()
    })
  end

  defp transition_item(context, operation, item, state, status, run_id, error, attrs \\ []) do
    result =
      rebuild_store().transition_item(%TransitionRebuildItem{
        workspace_context: context,
        command_id:
          command_id(
            "item-#{status}",
            operation.operation_id <>
              ":" <> item.item_id <> ":" <> Integer.to_string(item.version)
          ),
        operation_id: operation.operation_id,
        target_id: item.target_id,
        item_id: item.item_id,
        owner_id: state.owner_id,
        fencing_token: item.fencing_token,
        expected_version: item.version,
        status: status,
        child_run_id: run_id,
        materialization_id: Keyword.get(attrs, :materialization_id),
        row_count: Keyword.get(attrs, :row_count),
        last_error: error,
        occurred_at: DateTime.utc_now()
      })

    if match?({:ok, _item}, result) do
      Telemetry.execute(:item_outcome, %{count: 1}, %{
        workspace_id: context.workspace_id,
        operation_id: operation.operation_id,
        target_id: item.target_id,
        from_status: item.status,
        status: status
      })
    end

    result
  end

  defp timed_phase(event, context, operation, execute) do
    started_at = System.monotonic_time()
    result = execute.()

    metadata = %{
      workspace_id: context.workspace_id,
      operation_id: operation.operation_id,
      outcome: if(match?({:error, _reason}, result), do: :error, else: :ok)
    }

    metadata =
      case result do
        {:error, reason} -> Map.put(metadata, :reason, Telemetry.reason_kind(reason))
        _result -> metadata
      end

    Telemetry.execute(event, %{duration: System.monotonic_time() - started_at}, metadata)
    result
  end

  defp ensure_target_locks(context, operation, state) do
    target_ids = write_target_ids(operation)

    lock_store().acquire_many(%AcquireTargetOperationLocks{
      workspace_context: context,
      command_id: command_id("renew-locks", operation.operation_id <> ":" <> unique_identity()),
      target_ids: target_ids,
      operation_id: operation.operation_id,
      operation_type: :rebuild,
      lease_owner: operation.operation_id,
      lease_duration_ms: state.lease_ms,
      occurred_at: DateTime.utc_now()
    })
  end

  defp release_locks(context, operation, locks) do
    result =
      lock_store().release_many(%ReleaseTargetOperationLocks{
        workspace_context: context,
        command_id: command_id("release-locks", operation.operation_id <> ":" <> operation.state),
        operation_id: operation.operation_id,
        lease_owner: operation.operation_id,
        locks: Enum.map(locks, &%{target_id: &1.target_id, fencing_token: &1.fencing_token}),
        occurred_at: DateTime.utc_now()
      })

    case result do
      :ok -> :ok
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp write_target_ids(operation) do
    operation.actions
    |> Enum.filter(&(&1.action in [:rebuild, :backfill]))
    |> Enum.map(& &1.target_id)
    |> Enum.sort()
  end

  defp page_items(context, operation_id, target_id, status) do
    rebuild_store().page_items(%PageRebuildItems{
      workspace_context: context,
      operation_id: operation_id,
      target_id: target_id,
      status: status,
      limit: 500
    })
    |> case do
      {:ok, page} -> {:ok, page.items}
      {:error, _reason} = error -> error
    end
  end

  defp page_all_items(context, operation_id, status),
    do: ItemPager.all(rebuild_store(), context, operation_id, status: status)

  defp version_asset(context, operation, action) do
    with {:ok, version} <- ManifestStore.get_manifest(context, operation.manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, action.target_id) do
      {:ok, version, asset}
    end
  end

  defp reload(context, operation_id) do
    rebuild_store().get(%FavnOrchestrator.Persistence.Queries.GetRebuild{
      workspace_context: context,
      operation_id: operation_id
    })
  end

  defp action(operation, target_id),
    do: Enum.find(operation.actions, &(&1.target_id == target_id))

  defp current_generation_action(operation) do
    Enum.find(operation.actions, fn action ->
      action.action == :rebuild and action.status == :running
    end)
  end

  defp activation_action(operation) do
    Enum.find(operation.actions, fn action ->
      action.action == :rebuild and action.status in [:running, :outcome_unknown] and
        is_map(action.activation_intent)
    end)
  end

  defp capability(operation, target_id, key) do
    operation.plan_payload
    |> field(:capabilities)
    |> field(target_id)
    |> field(key)
  end

  defp activation_token(operation, action),
    do: command_id("activation", operation.operation_id <> ":" <> action.target_id)

  defp child_run_id(operation, item),
    do: command_id("run-rebuild", operation.operation_id <> ":" <> item.item_id)

  defp classified_failure(error) do
    cond do
      contains_unknown_outcome?(error) -> {:outcome_unknown, unknown_failure(error)}
      contains_safe_failure?(error) -> {:failed, safe_failure(error)}
      true -> {:outcome_unknown, unknown_failure(error)}
    end
  end

  defp contains_unknown_outcome?(value) when is_map(value) do
    Enum.any?(value, fn {key, child} ->
      (key in [:outcome, "outcome"] and
         child in [:unknown, "unknown", :outcome_unknown, "outcome_unknown"]) or
        contains_unknown_outcome?(child)
    end)
  end

  defp contains_unknown_outcome?(values) when is_list(values),
    do: Enum.any?(values, &contains_unknown_outcome?/1)

  defp contains_unknown_outcome?(_value), do: false

  defp contains_safe_failure?(value) when is_map(value) do
    Enum.any?(value, fn {key, child} ->
      (key in [:outcome, "outcome"] and child in [:safe_failure, "safe_failure"]) or
        contains_safe_failure?(child)
    end)
  end

  defp contains_safe_failure?(values) when is_list(values),
    do: Enum.any?(values, &contains_safe_failure?/1)

  defp contains_safe_failure?(_value), do: false

  defp safe_failure(reason),
    do: %{"outcome" => "safe_failure", "reason" => inspect_reason(reason)}

  defp unknown_failure(reason),
    do: %{"outcome" => "unknown", "reason" => inspect_reason(reason)}

  defp canonical_error(reason) when is_map(reason), do: reason
  defp canonical_error(reason), do: safe_failure(reason)
  defp error_payload(nil), do: %{"reason" => "runner_error"}
  defp error_payload(error), do: canonical_error(error)

  defp inspect_reason(reason),
    do: reason |> inspect(limit: 20, printable_limit: 1_000) |> String.slice(0, 2_000)

  defp datetime(%DateTime{} = value), do: {:ok, value}

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> {:ok, datetime}
      _invalid -> {:error, :invalid_datetime}
    end
  end

  defp datetime(_value), do: {:error, :invalid_datetime}

  defp ok_only({:ok, _value}), do: :ok
  defp ok_only({:error, _reason} = error), do: error

  defp field(nil, _key), do: nil

  defp field(map, key) when is_map(map),
    do: Map.get(map, key, Map.get(map, to_string(key)))

  defp emit_error(workspace_id, operation_id, operation, reason) do
    :telemetry.execute(
      [:favn, :orchestrator, :rebuild_dispatch, :error],
      %{count: 1},
      %{
        workspace_id: workspace_id,
        operation_id: operation_id,
        operation: operation,
        reason: error_kind(reason)
      }
    )
  end

  defp error_kind(%Error{kind: kind}), do: kind
  defp error_kind(_reason), do: :unknown

  defp command_id(prefix, identity) do
    digest = :crypto.hash(:sha256, identity) |> Base.url_encode64(padding: false)
    prefix <> ":" <> String.slice(digest, 0, 40)
  end

  defp unique_identity,
    do: Integer.to_string(System.unique_integer([:positive, :monotonic]))

  defp owner_id do
    instance = RuntimeConfig.instance_id() |> String.slice(0, 160)
    instance <> ":rebuilds:" <> String.slice(unique_identity(), 0, 40)
  end

  defp rebuild_store, do: Persistence.stores().rebuilds
  defp lock_store, do: Persistence.stores().target_operation_locks
end
