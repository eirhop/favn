defmodule FavnOrchestrator.Operator.TargetRecovery do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Results.TargetRecoveryOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.TargetRecovery.Plan

  @spec plan(Plan.t(), boolean()) :: map()
  def plan(%Plan{} = plan, admin?) when is_boolean(admin?) do
    %{
      plan_id: plan.plan_id,
      plan_hash: plan.plan_hash,
      expires_at: plan.expires_at,
      payload: Redaction.redact_operational_bounded(plan.payload),
      permissions: %{start: admin?}
    }
  end

  @spec operation(TargetRecoveryOperation.t(), boolean()) :: map()
  def operation(%TargetRecoveryOperation{} = operation, admin?) when is_boolean(admin?) do
    %{
      operation_id: operation.operation_id,
      target_id: operation.target_id,
      recovery_kind: operation.recovery_kind,
      desired_manifest_id: operation.desired_manifest_id,
      source_manifest_id: operation.source_manifest_id,
      target_generation_id: operation.target_generation_id,
      materialization_id: operation.materialization_id,
      plan_hash: operation.plan_hash,
      state: operation.state,
      phase: operation.phase,
      reason: operation.reason,
      expected_physical_fingerprint: operation.expected_physical_fingerprint,
      compatibility_result: safe(operation.compatibility_result),
      unknown_outcome: safe(operation.unknown_outcome),
      terminal_error: safe(operation.terminal_error),
      evaluated_at: operation.evaluated_at,
      started_at: operation.started_at,
      completed_at: operation.completed_at,
      inserted_at: operation.inserted_at,
      updated_at: operation.updated_at,
      permissions: %{
        start: admin? and operation.state == :planned,
        reconcile: admin? and operation.state in [:applying, :outcome_unknown]
      }
    }
  end

  @spec admin?([atom()]) :: boolean()
  def admin?(roles) when is_list(roles),
    do: Enum.any?(roles, &(&1 in [:workspace_admin, :platform_operator]))

  @doc false
  @spec error_code(term()) :: String.t()
  def error_code(%Error{kind: kind, details: details}) do
    Map.get(details, :reason_code) || Map.get(details, "reason_code") || Atom.to_string(kind)
  end

  def error_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  def error_code(_reason), do: "target_recovery_failed"

  defp safe(nil), do: nil
  defp safe(value), do: Redaction.redact_operational_bounded(value)
end
