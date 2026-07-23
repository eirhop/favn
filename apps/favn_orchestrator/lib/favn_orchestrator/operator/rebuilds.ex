defmodule FavnOrchestrator.Operator.Rebuilds do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RebuildAction
  alias FavnOrchestrator.Persistence.Results.RebuildItem
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Rebuild.Plan
  alias FavnOrchestrator.Redaction

  @spec plan(Plan.t(), boolean()) :: map()
  def plan(%Plan{} = plan, admin?) when is_boolean(admin?) do
    %{
      plan_id: plan.plan_id,
      plan_hash: plan.plan_hash,
      expires_at: plan.expires_at,
      payload: Redaction.redact(plan.payload),
      permissions: %{start: admin?}
    }
  end

  @spec admin?([atom()]) :: boolean()
  def admin?(roles) when is_list(roles),
    do: Enum.any?(roles, &(&1 in [:workspace_admin, :platform_operator]))

  @spec operation(RebuildOperation.t(), boolean(), :summary | :detail) :: map()
  def operation(%RebuildOperation{} = operation, admin?, mode \\ :detail) do
    summary = %{
      operation_id: operation.operation_id,
      root_target_id: operation.root_target_id,
      reason: operation.reason,
      action_count: operation.action_count,
      window_count: operation.window_count,
      state: operation.state,
      phase: operation.phase,
      progress: operation.progress,
      cleanup_state: operation.cleanup_state,
      cancel_requested: operation.cancel_requested,
      started_at: operation.timestamps.started_at,
      completed_at: operation.timestamps.completed_at,
      updated_at: operation.timestamps.updated_at
    }

    if mode == :detail do
      Map.merge(summary, %{
        manifest_version_id: operation.manifest_version_id,
        active_generation_id: operation.active_generation_id,
        candidate_generation_id: operation.candidate_generation_id,
        plan_hash: operation.plan_hash,
        evaluated_at: operation.evaluated_at,
        coverage_start: operation.coverage_start,
        coverage_end: operation.coverage_end,
        unknown_outcome: safe_diagnostic(operation.unknown_outcome),
        validation_result: safe_diagnostic(operation.validation_result),
        terminal_error: safe_error(operation.terminal_error),
        cancelled_at: operation.timestamps.cancelled_at,
        inserted_at: operation.timestamps.inserted_at,
        permissions: permissions(operation, admin?)
      })
      |> Map.put(:plan, Redaction.redact(operation.plan_payload))
      |> Map.put(:actions, Enum.map(operation.actions, &action/1))
      |> Map.put(:result_marker, safe_marker(operation.result_marker))
    else
      summary
    end
  end

  @spec item(RebuildItem.t()) :: map()
  def item(%RebuildItem{} = item) do
    item
    |> Map.take([
      :target_id,
      :item_id,
      :ordinal,
      :work_kind,
      :window_key,
      :window_start,
      :window_end,
      :status,
      :child_run_id,
      :materialization_id,
      :attempt_count,
      :row_count,
      :last_error,
      :candidate_generation_id,
      :inserted_at,
      :updated_at
    ])
    |> Map.update(:last_error, nil, &safe_error/1)
  end

  @spec page(CursorPage.t(term()), (term() -> term())) :: CursorPage.t(term())
  def page(%CursorPage{} = page, mapper) when is_function(mapper, 1) do
    %{page | items: Enum.map(page.items, mapper)}
  end

  defp action(%RebuildAction{} = action) do
    action
    |> Map.take([
      :target_id,
      :ordinal,
      :action,
      :reason,
      :upstream_impact,
      :mapping_proof,
      :pinned_input_generation_ids,
      :candidate_generation_id,
      :status,
      :child_operation_id,
      :child_run_id,
      :activation_intent,
      :validation_result,
      :terminal_error,
      :cleanup_state,
      :activated_at,
      :progress,
      :inserted_at,
      :updated_at
    ])
    |> Map.update(:pinned_input_generation_ids, [], &Redaction.redact/1)
    |> Map.update(:activation_intent, nil, &safe_activation_intent/1)
    |> Map.update(:validation_result, nil, &safe_diagnostic/1)
    |> Map.update(:terminal_error, nil, &safe_error/1)
  end

  defp safe_activation_intent(intent) when is_map(intent) do
    take_fields(intent, [:previous_generation_id, :candidate_generation_id, :dispatched])
  end

  defp safe_activation_intent(_intent), do: nil

  defp safe_marker(marker) when is_map(marker) do
    take_fields(marker, [
      :target_id,
      :active_generation_id,
      :previous_generation_id,
      :operation_id,
      :physical_fingerprint,
      :committed_at
    ])
  end

  defp safe_marker(_marker), do: nil

  defp safe_error(error) when is_map(error) do
    error
    |> take_fields([:code, :message, :outcome, :reason, :reason_code])
    |> Redaction.redact_operational_bounded()
  end

  defp safe_error(error) when is_binary(error) do
    %{error: sanitized} =
      Redaction.redact_operational_bounded(%{error: String.slice(error, 0, 2_000)})

    sanitized
  end

  defp safe_error(_error), do: nil

  defp safe_diagnostic(nil), do: nil
  defp safe_diagnostic(value), do: Redaction.redact_operational_bounded(value)

  defp take_fields(map, fields) do
    Map.new(fields, fn field ->
      {field, Map.get(map, field, Map.get(map, Atom.to_string(field)))}
    end)
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp permissions(operation, admin?) do
    %{
      start: admin? and operation.state == :planned,
      cancel:
        admin? and
          operation.state not in [:succeeded, :cancelled] and operation.cancel_requested != true,
      retry:
        admin? and operation.state == :failed and operation.cleanup_state == :not_started and
          empty?(operation.unknown_outcome),
      reconcile:
        admin? and
          (operation.state in [:activation_unknown, :reconciling] or
             not empty?(operation.unknown_outcome))
    }
  end

  defp empty?(nil), do: true
  defp empty?(map) when is_map(map), do: map_size(map) == 0
  defp empty?(_value), do: false
end
