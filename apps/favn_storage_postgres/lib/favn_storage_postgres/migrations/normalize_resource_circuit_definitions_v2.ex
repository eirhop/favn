defmodule FavnStoragePostgres.Migrations.NormalizeResourceCircuitDefinitionsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    drop_resource_definitions()
    create_resource_definitions(:canonical)
  end

  def down do
    drop_resource_definitions()
    create_resource_definitions(:legacy)
  end

  defp drop_resource_definitions do
    drop(
      index(
        :resource_recovery_candidates,
        [:workspace_id, :resource_kind, :resource_name, :status, :expires_at],
        prefix: @prefix,
        name: :resource_recovery_candidates_claim_idx
      )
    )

    drop(
      index(:resource_circuits, [:workspace_id, :state, :next_probe_at],
        prefix: @prefix,
        name: :resource_circuits_probe_idx
      )
    )

    drop(
      constraint(
        :resource_recovery_candidates,
        :resource_recovery_candidates_values_valid,
        prefix: @prefix
      )
    )

    drop(
      constraint(
        :resource_circuit_outcomes,
        :resource_circuit_outcomes_values_valid,
        prefix: @prefix
      )
    )

    drop(constraint(:resource_circuits, :resource_circuits_values_valid, prefix: @prefix))
  end

  defp create_resource_definitions(:canonical) do
    create_constraints(
      "resource_kind::text IN ('execution_pool'::text, 'connection'::text)",
      "state::text IN ('closed'::text, 'open'::text, 'half_open'::text)",
      "status::text IN ('success'::text, 'failure'::text)",
      "reason::text IN ('blocked'::text, 'safe_failure'::text)",
      "status::text IN ('pending'::text, 'claimed'::text, 'submitted'::text)"
    )

    create_indexes(
      "state::text IN ('open'::text, 'half_open'::text)",
      "status::text IN ('pending'::text, 'claimed'::text)"
    )
  end

  defp create_resource_definitions(:legacy) do
    create_constraints(
      "resource_kind IN ('execution_pool', 'connection')",
      "state IN ('closed', 'open', 'half_open')",
      "status IN ('success', 'failure')",
      "reason IN ('blocked', 'safe_failure')",
      "status IN ('pending', 'claimed', 'submitted')"
    )

    create_indexes(
      "state IN ('open', 'half_open')",
      "status IN ('pending', 'claimed')"
    )
  end

  defp create_constraints(resource_kind, circuit_state, outcome_status, reason, recovery_status) do
    create(
      constraint(:resource_circuits, :resource_circuits_values_valid,
        prefix: @prefix,
        check:
          "#{resource_kind} AND #{circuit_state} AND consecutive_failures >= 0 AND failure_threshold > 0 AND probe_after_ms >= 0 AND version > 0 AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:resource_circuit_outcomes, :resource_circuit_outcomes_values_valid,
        prefix: @prefix,
        check:
          "#{resource_kind} AND #{outcome_status} AND attempt > 0 AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(outcome_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(
        :resource_recovery_candidates,
        :resource_recovery_candidates_values_valid,
        prefix: @prefix,
        check:
          "#{resource_kind} AND #{reason} AND #{recovery_status} AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(candidate_id) BETWEEN 1 AND 255 AND octet_length(source_run_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255 AND octet_length(node_key) BETWEEN 1 AND 65536"
      )
    )
  end

  defp create_indexes(circuit_predicate, recovery_predicate) do
    create(
      index(:resource_circuits, [:workspace_id, :state, :next_probe_at],
        prefix: @prefix,
        name: :resource_circuits_probe_idx,
        where: circuit_predicate
      )
    )

    create(
      index(
        :resource_recovery_candidates,
        [:workspace_id, :resource_kind, :resource_name, :status, :expires_at],
        prefix: @prefix,
        name: :resource_recovery_candidates_claim_idx,
        where: recovery_predicate
      )
    )
  end
end
