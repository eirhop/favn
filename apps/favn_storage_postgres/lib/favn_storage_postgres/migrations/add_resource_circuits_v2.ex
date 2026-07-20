defmodule FavnStoragePostgres.Migrations.AddResourceCircuitsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:resource_circuits, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :string, null: false, primary_key: true)
      add(:resource_kind, :string, null: false, primary_key: true)
      add(:resource_name, :string, null: false, primary_key: true)
      add(:state, :string, null: false, default: "closed")
      add(:consecutive_failures, :integer, null: false, default: 0)
      add(:failure_threshold, :integer, null: false)
      add(:probe_after_ms, :bigint, null: false)
      add(:opened_at, :utc_datetime_usec)
      add(:next_probe_at, :utc_datetime_usec)
      add(:probe_owner_id, :string)
      add(:probe_expires_at, :utc_datetime_usec)
      add(:last_category, :string)
      add(:last_outcome_at, :utc_datetime_usec)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      constraint(:resource_circuits, :resource_circuits_values_valid,
        prefix: @prefix,
        check:
          "resource_kind::text IN ('execution_pool'::text, 'connection'::text) AND state::text IN ('closed'::text, 'open'::text, 'half_open'::text) AND consecutive_failures >= 0 AND failure_threshold > 0 AND probe_after_ms >= 0 AND version > 0 AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:resource_circuits, :resource_circuits_probe_shape_valid,
        prefix: @prefix,
        check:
          "(state = 'half_open' AND probe_owner_id IS NOT NULL AND probe_expires_at IS NOT NULL) OR (state <> 'half_open')"
      )
    )

    create(
      index(:resource_circuits, [:workspace_id, :state, :next_probe_at],
        prefix: @prefix,
        name: :resource_circuits_probe_idx,
        where: "state::text IN ('open'::text, 'half_open'::text)"
      )
    )

    create table(:resource_circuit_outcomes, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :string, null: false, primary_key: true)
      add(:outcome_id, :string, null: false, primary_key: true)
      add(:resource_kind, :string, null: false)
      add(:resource_name, :string, null: false)
      add(:run_id, :string, null: false)
      add(:asset_step_id, :string, null: false)
      add(:attempt, :integer, null: false)
      add(:status, :string, null: false)
      add(:category, :string)
      add(:occurred_at, :utc_datetime_usec, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
    end

    create(
      constraint(:resource_circuit_outcomes, :resource_circuit_outcomes_values_valid,
        prefix: @prefix,
        check:
          "resource_kind::text IN ('execution_pool'::text, 'connection'::text) AND status::text IN ('success'::text, 'failure'::text) AND attempt > 0 AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(outcome_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255"
      )
    )

    create(
      index(
        :resource_circuit_outcomes,
        [:workspace_id, :resource_kind, :resource_name, :occurred_at],
        prefix: @prefix,
        name: :resource_circuit_outcomes_resource_idx
      )
    )

    create table(:resource_recovery_candidates, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :string, null: false, primary_key: true)
      add(:candidate_id, :string, null: false, primary_key: true)
      add(:source_run_id, :string, null: false)
      add(:node_key, :text, null: false)
      add(:resource_kind, :string, null: false)
      add(:resource_name, :string, null: false)
      add(:reason, :string, null: false)
      add(:status, :string, null: false, default: "pending")
      add(:expires_at, :utc_datetime_usec, null: false)
      add(:claim_owner, :string)
      add(:claim_expires_at, :utc_datetime_usec)
      add(:recovery_run_id, :string)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      constraint(:resource_recovery_candidates, :resource_recovery_candidates_values_valid,
        prefix: @prefix,
        check:
          "resource_kind::text IN ('execution_pool'::text, 'connection'::text) AND reason::text IN ('blocked'::text, 'safe_failure'::text) AND status::text IN ('pending'::text, 'claimed'::text, 'submitted'::text) AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(candidate_id) BETWEEN 1 AND 255 AND octet_length(source_run_id) BETWEEN 1 AND 255 AND octet_length(resource_name) BETWEEN 1 AND 255 AND octet_length(node_key) BETWEEN 1 AND 65536"
      )
    )

    create(
      index(
        :resource_recovery_candidates,
        [:workspace_id, :resource_kind, :resource_name, :status, :expires_at],
        prefix: @prefix,
        name: :resource_recovery_candidates_claim_idx,
        where: "status::text IN ('pending'::text, 'claimed'::text)"
      )
    )
  end

  def down do
    drop(table(:resource_recovery_candidates, prefix: @prefix))
    drop(table(:resource_circuit_outcomes, prefix: @prefix))
    drop(table(:resource_circuits, prefix: @prefix))
  end
end
