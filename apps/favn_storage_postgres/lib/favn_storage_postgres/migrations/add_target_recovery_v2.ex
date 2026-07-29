defmodule FavnStoragePostgres.Migrations.AddTargetRecoveryV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    execute(
      "ALTER TABLE #{@prefix}.target_operation_locks " <>
        "DROP CONSTRAINT target_operation_locks_values_valid"
    )

    execute("""
    ALTER TABLE #{@prefix}.target_operation_locks
    ADD CONSTRAINT target_operation_locks_values_valid
    CHECK (
      operation_type IN ('materialization', 'rebuild', 'target_recovery')
      AND fencing_token > 0
      AND version > 0
    )
    """)

    create table(:target_recovery_operations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:operation_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false)
      add(:recovery_kind, :text, null: false)
      add(:desired_manifest_id, :text, null: false)
      add(:source_manifest_id, :text, null: false)
      add(:target_generation_id, :uuid, null: false)
      add(:materialization_id, :text, null: false)
      add(:plan_hash, :text)
      add(:plan_version, :integer, null: false, default: 1)
      add(:plan_payload, :map, null: false)
      add(:state, :text, null: false)
      add(:phase, :text, null: false)
      add(:actor_id, :text, null: false)
      add(:session_id, :text)
      add(:reason, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:expected_binding_version, :bigint, null: false)
      add(:expected_physical_fingerprint, :text)
      add(:evaluated_at, :timestamptz, null: false)
      add(:recovery_token, :text)
      add(:result_marker, :map)
      add(:compatibility_result, :map)
      add(:unknown_outcome, :map)
      add(:terminal_error, :map)
      add(:last_command_id, :text, null: false)
      add(:version, :bigint, null: false, default: 1)
      add(:started_at, :timestamptz)
      add(:completed_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_desired_manifest_fk
    FOREIGN KEY (desired_manifest_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_source_manifest_fk
    FOREIGN KEY (source_manifest_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_generation_fk
    FOREIGN KEY (workspace_id, target_id, target_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_materialization_fk
    FOREIGN KEY (workspace_id, materialization_id)
    REFERENCES #{@prefix}.materializations(workspace_id, materialization_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:target_recovery_operations, [:workspace_id, :idempotency_key],
        prefix: @prefix,
        name: :target_recovery_operations_idempotency_uidx
      )
    )

    create(
      index(:target_recovery_operations, [:workspace_id, :state, :updated_at, :operation_id],
        prefix: @prefix,
        name: :target_recovery_operations_recovery_idx,
        where: "state NOT IN ('succeeded', 'failed')"
      )
    )

    create(
      constraint(:target_recovery_operations, :target_recovery_operations_values_valid,
        prefix: @prefix,
        check:
          "recovery_kind = 'reconcile_initial_generation' " <>
            "AND (plan_hash IS NULL OR plan_hash ~ '^[0-9a-f]{64}$') AND plan_version > 0 " <>
            "AND expected_binding_version > 0 " <>
            "AND (expected_physical_fingerprint IS NULL " <>
            "OR expected_physical_fingerprint ~ '^[0-9a-f]{64}$') " <>
            "AND state IN ('planning', 'planned', 'applying', 'outcome_unknown', 'succeeded', 'failed') " <>
            "AND phase IN ('collecting_evidence', 'planned', 'marker_intent', 'reconciling', 'terminal') " <>
            "AND octet_length(reason) BETWEEN 1 AND 4096 AND version > 0"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_identifiers_bounded
    CHECK (
      octet_length(workspace_id) BETWEEN 1 AND 255
      AND octet_length(operation_id) BETWEEN 1 AND 255
      AND octet_length(target_id) BETWEEN 1 AND 255
      AND octet_length(desired_manifest_id) BETWEEN 1 AND 255
      AND octet_length(source_manifest_id) BETWEEN 1 AND 255
      AND octet_length(materialization_id) BETWEEN 1 AND 255
      AND octet_length(actor_id) BETWEEN 1 AND 255
      AND (session_id IS NULL OR octet_length(session_id) BETWEEN 1 AND 255)
      AND octet_length(idempotency_key) BETWEEN 1 AND 255
      AND (recovery_token IS NULL OR octet_length(recovery_token) BETWEEN 1 AND 255)
      AND octet_length(last_command_id) BETWEEN 1 AND 255
    )
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_payload_bounded
    CHECK (
      pg_column_size(plan_payload) <= 262144
      AND (result_marker IS NULL OR pg_column_size(result_marker) <= 65536)
      AND (compatibility_result IS NULL OR pg_column_size(compatibility_result) <= 65536)
      AND (unknown_outcome IS NULL OR pg_column_size(unknown_outcome) <= 65536)
      AND (terminal_error IS NULL OR pg_column_size(terminal_error) <= 65536)
    )
    """)

    execute("""
    ALTER TABLE #{@prefix}.target_recovery_operations
    ADD CONSTRAINT target_recovery_operations_state_shape
    CHECK (
      (state = 'planning' AND phase = 'collecting_evidence'
       AND plan_hash IS NULL AND expected_physical_fingerprint IS NULL
       AND plan_payload = '{}'::jsonb AND recovery_token IS NULL
       AND result_marker IS NULL AND compatibility_result IS NULL
       AND unknown_outcome IS NULL AND terminal_error IS NULL
       AND started_at IS NULL AND completed_at IS NULL)
      OR
      (state = 'planned' AND phase = 'planned'
       AND plan_hash IS NOT NULL AND expected_physical_fingerprint IS NOT NULL
       AND recovery_token IS NULL
       AND result_marker IS NULL AND compatibility_result IS NULL
       AND unknown_outcome IS NULL AND terminal_error IS NULL
       AND started_at IS NULL AND completed_at IS NULL)
      OR
      (state = 'applying' AND phase IN ('marker_intent', 'reconciling')
       AND plan_hash IS NOT NULL AND expected_physical_fingerprint IS NOT NULL
       AND recovery_token IS NOT NULL
       AND result_marker IS NULL AND compatibility_result IS NULL
       AND unknown_outcome IS NULL AND terminal_error IS NULL
       AND started_at IS NOT NULL AND completed_at IS NULL)
      OR
      (state = 'outcome_unknown' AND phase = 'reconciling'
       AND plan_hash IS NOT NULL AND expected_physical_fingerprint IS NOT NULL
       AND recovery_token IS NOT NULL AND unknown_outcome IS NOT NULL
       AND result_marker IS NULL AND compatibility_result IS NULL
       AND terminal_error IS NULL
       AND started_at IS NOT NULL AND completed_at IS NULL)
      OR
      (state = 'succeeded' AND phase = 'terminal'
       AND plan_hash IS NOT NULL AND expected_physical_fingerprint IS NOT NULL
       AND recovery_token IS NOT NULL
       AND result_marker IS NOT NULL AND compatibility_result IS NOT NULL
       AND unknown_outcome IS NULL AND terminal_error IS NULL
       AND started_at IS NOT NULL AND completed_at IS NOT NULL)
      OR
      (state = 'failed' AND phase = 'terminal' AND terminal_error IS NOT NULL
       AND result_marker IS NULL AND compatibility_result IS NULL
       AND unknown_outcome IS NULL AND completed_at IS NOT NULL
       AND (
         (plan_hash IS NULL AND expected_physical_fingerprint IS NULL
          AND plan_payload = '{}'::jsonb AND recovery_token IS NULL
          AND started_at IS NULL)
         OR
         (plan_hash IS NOT NULL AND expected_physical_fingerprint IS NOT NULL
          AND (
            (recovery_token IS NULL AND started_at IS NULL)
            OR
            (recovery_token IS NOT NULL AND started_at IS NOT NULL)
          ))
       ))
    )
    """)
  end

  def down do
    drop(table(:target_recovery_operations, prefix: @prefix))

    execute(
      "DELETE FROM #{@prefix}.target_operation_locks " <>
        "WHERE operation_type = 'target_recovery'"
    )

    execute(
      "ALTER TABLE #{@prefix}.target_operation_locks " <>
        "DROP CONSTRAINT target_operation_locks_values_valid"
    )

    execute("""
    ALTER TABLE #{@prefix}.target_operation_locks
    ADD CONSTRAINT target_operation_locks_values_valid
    CHECK (
      operation_type IN ('materialization', 'rebuild')
      AND fencing_token > 0
      AND version > 0
    )
    """)
  end
end
