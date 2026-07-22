defmodule FavnStoragePostgres.Migrations.AddTargetGenerationFoundationV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create_target_generations()
    create_target_bindings()
    create_rebuild_operations()
    add_generation_operation_reference()
    create_rebuild_plan_actions()
    create_rebuild_windows()
    create_target_operation_locks()
    add_materialization_generations()
    replace_evidence_projection_identity()
  end

  def down do
    restore_projection_identity()
    remove_materialization_generations()
    drop(table(:target_operation_locks, prefix: @prefix))
    drop(table(:rebuild_windows, prefix: @prefix))
    drop(table(:rebuild_plan_actions, prefix: @prefix))

    execute("""
    ALTER TABLE #{@prefix}.asset_target_generations
    DROP CONSTRAINT asset_target_generations_rebuild_operation_fk
    """)

    drop(table(:rebuild_operations, prefix: @prefix))
    drop(table(:asset_target_bindings, prefix: @prefix))
    drop(table(:asset_target_generations, prefix: @prefix))
  end

  defp create_target_generations do
    create table(:asset_target_generations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:target_generation_id, :uuid, null: false, primary_key: true)
      add(:creating_manifest_id, :text, null: false)
      add(:creation_command_id, :text, null: false)
      add(:creating_descriptor_hash, :text, null: false)
      add(:active_descriptor_hash, :text)
      add(:logical_relation, :map, null: false)
      add(:physical_relation, :map, null: false)
      add(:physical_schema_fingerprint, :text)
      add(:data_plane_marker, :map)
      add(:activation_token, :text)
      add(:status, :text, null: false)
      add(:creating_rebuild_operation_id, :text)
      add(:version, :bigint, null: false, default: 1)
      add(:created_at, :timestamptz, null: false)
      add(:activated_at, :timestamptz)
      add(:retired_at, :timestamptz)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.asset_target_generations
    ADD CONSTRAINT asset_target_generations_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_target_generations
    ADD CONSTRAINT asset_target_generations_manifest_fk
    FOREIGN KEY (creating_manifest_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:asset_target_generations, [:workspace_id, :creation_command_id],
        prefix: @prefix,
        name: :asset_target_generations_command_uidx
      )
    )

    create(
      index(:asset_target_generations, [:workspace_id, :target_id, :status, :updated_at],
        prefix: @prefix,
        name: :asset_target_generations_status_idx
      )
    )

    create(
      constraint(:asset_target_generations, :asset_target_generations_values_valid,
        prefix: @prefix,
        check:
          "status IN ('building', 'active', 'retired', 'failed', 'discarded') " <>
            "AND creating_descriptor_hash ~ '^[0-9a-f]{64}$' " <>
            "AND (active_descriptor_hash IS NULL OR active_descriptor_hash ~ '^[0-9a-f]{64}$') " <>
            "AND (physical_schema_fingerprint IS NULL OR physical_schema_fingerprint ~ '^[0-9a-f]{64}$') " <>
            "AND version > 0 AND jsonb_typeof(logical_relation) = 'object' " <>
            "AND jsonb_typeof(physical_relation) = 'object'"
      )
    )

    identifier_constraint(:asset_target_generations, [
      :workspace_id,
      :target_id,
      :creating_manifest_id,
      :creation_command_id,
      :activation_token,
      :creating_rebuild_operation_id
    ])

    payload_constraint(:asset_target_generations,
      logical_relation: 65_536,
      physical_relation: 65_536,
      data_plane_marker: 65_536
    )
  end

  defp create_target_bindings do
    create table(:asset_target_bindings, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:active_generation_id, :uuid)
      add(:desired_manifest_id, :text, null: false)
      add(:desired_descriptor_hash, :text, null: false)
      add(:compatibility_status, :text, null: false)
      add(:reason_code, :text, null: false)
      add(:compatibility_diff, :map, null: false, default: %{})
      add(:active_physical_fingerprint, :text)
      add(:version, :bigint, null: false, default: 1)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.asset_target_bindings
    ADD CONSTRAINT asset_target_bindings_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_target_bindings
    ADD CONSTRAINT asset_target_bindings_manifest_fk
    FOREIGN KEY (desired_manifest_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_target_bindings
    ADD CONSTRAINT asset_target_bindings_active_generation_fk
    FOREIGN KEY (workspace_id, target_id, active_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:asset_target_bindings, [:workspace_id, :compatibility_status, :target_id],
        prefix: @prefix,
        name: :asset_target_bindings_status_idx
      )
    )

    create(
      constraint(:asset_target_bindings, :asset_target_bindings_values_valid,
        prefix: @prefix,
        check:
          "compatibility_status IN ('ready', 'uninitialized', 'rebuild_available', " <>
            "'rebuild_required', 'unexpected_drift', 'operator_decision') " <>
            "AND desired_descriptor_hash ~ '^[0-9a-f]{64}$' " <>
            "AND (active_physical_fingerprint IS NULL OR " <>
            "active_physical_fingerprint ~ '^[0-9a-f]{64}$') " <>
            "AND jsonb_typeof(compatibility_diff) = 'object' AND version > 0"
      )
    )

    identifier_constraint(:asset_target_bindings, [
      :workspace_id,
      :target_id,
      :desired_manifest_id,
      :reason_code
    ])

    payload_constraint(:asset_target_bindings, compatibility_diff: 262_144)
  end

  defp create_rebuild_operations do
    create table(:rebuild_operations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:operation_id, :text, null: false, primary_key: true)
      add(:root_target_id, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:active_generation_id, :uuid)
      add(:candidate_generation_id, :uuid, null: false)
      add(:plan_hash, :text, null: false)
      add(:plan_version, :integer, null: false)
      add(:trigger, :text, null: false)
      add(:actor_id, :text, null: false)
      add(:session_id, :text)
      add(:reason, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:evaluated_at, :timestamptz, null: false)
      add(:coverage_start, :timestamptz)
      add(:coverage_end, :timestamptz)
      add(:action_count, :integer, null: false)
      add(:window_count, :integer, null: false)
      add(:state, :text, null: false)
      add(:phase, :text, null: false)
      add(:activation_token, :text)
      add(:dispatched_at, :timestamptz)
      add(:result_marker, :map)
      add(:unknown_outcome, :map)
      add(:validation_result, :map)
      add(:terminal_error, :map)
      add(:cleanup_state, :text, null: false, default: "not_started")
      add(:version, :bigint, null: false, default: 1)
      add(:started_at, :timestamptz)
      add(:completed_at, :timestamptz)
      add(:cancelled_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.rebuild_operations
    ADD CONSTRAINT rebuild_operations_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_operations
    ADD CONSTRAINT rebuild_operations_manifest_fk
    FOREIGN KEY (manifest_version_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_operations
    ADD CONSTRAINT rebuild_operations_active_generation_fk
    FOREIGN KEY (workspace_id, root_target_id, active_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_operations
    ADD CONSTRAINT rebuild_operations_candidate_generation_fk
    FOREIGN KEY (workspace_id, root_target_id, candidate_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      unique_index(:rebuild_operations, [:workspace_id, :idempotency_key],
        prefix: @prefix,
        name: :rebuild_operations_idempotency_uidx
      )
    )

    create(
      index(:rebuild_operations, [:workspace_id, :state, :updated_at, :operation_id],
        prefix: @prefix,
        name: :rebuild_operations_recovery_idx,
        where: "state NOT IN ('succeeded', 'failed', 'cancelled')"
      )
    )

    create(
      constraint(:rebuild_operations, :rebuild_operations_values_valid,
        prefix: @prefix,
        check:
          "plan_hash ~ '^[0-9a-f]{64}$' AND plan_version > 0 AND trigger = 'manual' " <>
            "AND action_count BETWEEN 1 AND 10000 AND window_count BETWEEN 1 AND 100000 " <>
            "AND state IN ('planned', 'queued', 'building', 'validating', 'activating', " <>
            "'activation_unknown', 'reconciling', 'cancelling', 'succeeded', 'failed', " <>
            "'cancelled') AND phase IN ('planned', 'locking', 'building', 'validating', " <>
            "'activating', 'reconciling', 'repairing', 'cleanup', 'terminal') " <>
            "AND cleanup_state IN ('not_started', 'pending', 'running', 'complete', 'failed') " <>
            "AND octet_length(reason) BETWEEN 1 AND 4096 " <>
            "AND (coverage_start IS NULL) = (coverage_end IS NULL) " <>
            "AND (coverage_start IS NULL OR coverage_start < coverage_end) AND version > 0"
      )
    )

    identifier_constraint(:rebuild_operations, [
      :workspace_id,
      :operation_id,
      :root_target_id,
      :manifest_version_id,
      :actor_id,
      :session_id,
      :idempotency_key,
      :activation_token
    ])

    payload_constraint(:rebuild_operations,
      result_marker: 65_536,
      unknown_outcome: 65_536,
      validation_result: 262_144,
      terminal_error: 65_536
    )
  end

  defp add_generation_operation_reference do
    execute("""
    ALTER TABLE #{@prefix}.asset_target_generations
    ADD CONSTRAINT asset_target_generations_rebuild_operation_fk
    FOREIGN KEY (workspace_id, creating_rebuild_operation_id)
    REFERENCES #{@prefix}.rebuild_operations(workspace_id, operation_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)
  end

  defp create_rebuild_plan_actions do
    create table(:rebuild_plan_actions, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:operation_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:ordinal, :integer, null: false)
      add(:action, :text, null: false)
      add(:reason, :map, null: false)
      add(:upstream_impact, :map, null: false)
      add(:mapping_proof, :map)
      add(:pinned_input_generation_ids, :map, null: false)
      add(:candidate_generation_id, :uuid)
      add(:status, :text, null: false)
      add(:child_operation_id, :text)
      add(:child_run_id, :text)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.rebuild_plan_actions
    ADD CONSTRAINT rebuild_plan_actions_operation_fk
    FOREIGN KEY (workspace_id, operation_id)
    REFERENCES #{@prefix}.rebuild_operations(workspace_id, operation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_plan_actions
    ADD CONSTRAINT rebuild_plan_actions_candidate_generation_fk
    FOREIGN KEY (workspace_id, target_id, candidate_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_plan_actions
    ADD CONSTRAINT rebuild_plan_actions_child_run_fk
    FOREIGN KEY (workspace_id, child_run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_plan_actions
    ADD CONSTRAINT rebuild_plan_actions_child_operation_fk
    FOREIGN KEY (workspace_id, child_operation_id)
    REFERENCES #{@prefix}.rebuild_operations(workspace_id, operation_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      unique_index(:rebuild_plan_actions, [:workspace_id, :operation_id, :ordinal],
        prefix: @prefix,
        name: :rebuild_plan_actions_ordinal_uidx
      )
    )

    create(
      constraint(:rebuild_plan_actions, :rebuild_plan_actions_values_valid,
        prefix: @prefix,
        check:
          "ordinal BETWEEN 0 AND 9999 AND action IN ('no_action', 'backfill', 'rebuild', " <>
            "'operator_decision') AND status IN ('planned', 'running', 'succeeded', " <>
            "'failed', 'cancelled', 'outcome_unknown') AND jsonb_typeof(reason) = 'object' " <>
            "AND jsonb_typeof(upstream_impact) = 'object' " <>
            "AND jsonb_typeof(pinned_input_generation_ids) = 'array' AND version > 0"
      )
    )

    identifier_constraint(:rebuild_plan_actions, [
      :workspace_id,
      :operation_id,
      :target_id,
      :child_operation_id,
      :child_run_id
    ])

    payload_constraint(:rebuild_plan_actions,
      reason: 65_536,
      upstream_impact: 65_536,
      mapping_proof: 65_536,
      pinned_input_generation_ids: 262_144
    )
  end

  defp create_rebuild_windows do
    create table(:rebuild_windows, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:operation_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:item_id, :text, null: false, primary_key: true)
      add(:ordinal, :integer, null: false)
      add(:work_kind, :text, null: false)
      add(:window_key, :text, null: false)
      add(:window_start, :timestamptz)
      add(:window_end, :timestamptz)
      add(:status, :text, null: false)
      add(:claim_owner, :text)
      add(:fencing_token, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:last_command_id, :text)
      add(:claim_expires_at, :timestamptz)
      add(:child_run_id, :text)
      add(:materialization_id, :text)
      add(:attempt_count, :integer, null: false, default: 0)
      add(:row_count, :bigint)
      add(:last_error, :map)
      add(:candidate_generation_id, :uuid)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.rebuild_windows
    ADD CONSTRAINT rebuild_windows_action_fk
    FOREIGN KEY (workspace_id, operation_id, target_id)
    REFERENCES #{@prefix}.rebuild_plan_actions(workspace_id, operation_id, target_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_windows
    ADD CONSTRAINT rebuild_windows_candidate_generation_fk
    FOREIGN KEY (workspace_id, target_id, candidate_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_windows
    ADD CONSTRAINT rebuild_windows_child_run_fk
    FOREIGN KEY (workspace_id, child_run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.rebuild_windows
    ADD CONSTRAINT rebuild_windows_materialization_fk
    FOREIGN KEY (workspace_id, materialization_id)
    REFERENCES #{@prefix}.materializations(workspace_id, materialization_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      unique_index(:rebuild_windows, [:workspace_id, :operation_id, :target_id, :ordinal],
        prefix: @prefix,
        name: :rebuild_windows_ordinal_uidx
      )
    )

    create(
      unique_index(:rebuild_windows, [:workspace_id, :operation_id, :target_id, :window_key],
        prefix: @prefix,
        name: :rebuild_windows_key_uidx
      )
    )

    create(
      index(
        :rebuild_windows,
        [:workspace_id, :operation_id, :status, :ordinal, :item_id],
        prefix: @prefix,
        name: :rebuild_windows_claim_idx,
        where: "status IN ('ready', 'claimed', 'running', 'outcome_unknown')"
      )
    )

    create(
      constraint(:rebuild_windows, :rebuild_windows_values_valid,
        prefix: @prefix,
        check:
          "ordinal BETWEEN 0 AND 99999 AND work_kind IN ('window', 'full_load') " <>
            "AND status IN ('planned', 'ready', 'claimed', 'running', 'succeeded', " <>
            "'failed', 'cancelled', 'outcome_unknown') AND fencing_token >= 0 " <>
            "AND attempt_count >= 0 AND (row_count IS NULL OR row_count >= 0) AND version > 0 " <>
            "AND ((work_kind = 'window' AND window_start IS NOT NULL AND window_end IS NOT NULL " <>
            "AND window_start < window_end) OR (work_kind = 'full_load' AND window_key = 'full_load' " <>
            "AND window_start IS NULL AND window_end IS NULL))"
      )
    )

    identifier_constraint(:rebuild_windows, [
      :workspace_id,
      :operation_id,
      :target_id,
      :item_id,
      :window_key,
      :claim_owner,
      :claim_command_id,
      :last_command_id,
      :child_run_id,
      :materialization_id
    ])

    payload_constraint(:rebuild_windows, last_error: 65_536)
  end

  defp create_target_operation_locks do
    create table(:target_operation_locks, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:operation_id, :text, null: false)
      add(:operation_type, :text, null: false)
      add(:fencing_token, :bigint, null: false)
      add(:lease_owner, :text, null: false)
      add(:lease_expires_at, :timestamptz, null: false)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.target_operation_locks
    ADD CONSTRAINT target_operation_locks_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:target_operation_locks, [:workspace_id, :lease_expires_at, :target_id],
        prefix: @prefix,
        name: :target_operation_locks_expiry_idx
      )
    )

    create(
      constraint(:target_operation_locks, :target_operation_locks_values_valid,
        prefix: @prefix,
        check:
          "operation_type IN ('materialization', 'rebuild') AND fencing_token > 0 AND version > 0"
      )
    )

    identifier_constraint(:target_operation_locks, [
      :workspace_id,
      :target_id,
      :operation_id,
      :lease_owner
    ])
  end

  defp add_materialization_generations do
    alter table(:materialization_claims, prefix: @prefix) do
      add(:target_generation_id, :uuid)
      add(:evidence_generation_id, :text)
    end

    alter table(:materializations, prefix: @prefix) do
      add(:target_generation_id, :uuid)
      add(:evidence_generation_id, :text)
    end

    execute("""
    UPDATE #{@prefix}.materialization_claims claim
    SET evidence_generation_id = 'legacy_' || md5(claim.workspace_id || ':' || deployment.manifest_version_id || ':' || claim.target_id)
    FROM #{@prefix}.workspace_deployments deployment
    WHERE deployment.workspace_id = claim.workspace_id
      AND deployment.deployment_id = claim.deployment_id
    """)

    execute("""
    UPDATE #{@prefix}.materializations materialization
    SET evidence_generation_id = 'legacy_' || md5(materialization.workspace_id || ':' || deployment.manifest_version_id || ':' || materialization.target_id)
    FROM #{@prefix}.workspace_deployments deployment
    WHERE deployment.workspace_id = materialization.workspace_id
      AND deployment.deployment_id = materialization.deployment_id
    """)

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    ALTER COLUMN evidence_generation_id SET NOT NULL
    """)

    execute("""
    ALTER TABLE #{@prefix}.materializations
    ALTER COLUMN evidence_generation_id SET NOT NULL
    """)

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    ADD CONSTRAINT materialization_claims_target_generation_fk
    FOREIGN KEY (workspace_id, target_id, target_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.materializations
    ADD CONSTRAINT materializations_target_generation_fk
    FOREIGN KEY (workspace_id, target_id, target_generation_id)
    REFERENCES #{@prefix}.asset_target_generations(workspace_id, target_id, target_generation_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:materializations, [:workspace_id, :target_id, :target_generation_id, :inserted_at],
        prefix: @prefix,
        name: :materializations_generation_idx,
        where: "target_generation_id IS NOT NULL"
      )
    )

    create(
      constraint(:materialization_claims, :materialization_claims_generation_valid,
        prefix: @prefix,
        check:
          "octet_length(evidence_generation_id) BETWEEN 1 AND 255 AND " <>
            "(target_generation_id IS NULL OR evidence_generation_id = target_generation_id::text)"
      )
    )

    create(
      constraint(:materializations, :materializations_generation_valid,
        prefix: @prefix,
        check:
          "octet_length(evidence_generation_id) BETWEEN 1 AND 255 AND " <>
            "(target_generation_id IS NULL OR evidence_generation_id = target_generation_id::text)"
      )
    )
  end

  defp replace_evidence_projection_identity do
    alter table(:asset_window_states, prefix: @prefix) do
      add(:evidence_generation_id, :text)
    end

    alter table(:asset_freshness_states, prefix: @prefix) do
      add(:evidence_generation_id, :text)
      add(:manifest_version_id, :text)
    end

    execute("""
    UPDATE #{@prefix}.asset_window_states
    SET evidence_generation_id = 'legacy_' || md5(workspace_id || ':' || manifest_version_id || ':' || target_id)
    """)

    execute("""
    UPDATE #{@prefix}.asset_freshness_states freshness
    SET evidence_generation_id = 'legacy_' || md5(freshness.workspace_id || ':' || deployment.manifest_version_id || ':' || freshness.target_id),
        manifest_version_id = deployment.manifest_version_id
    FROM #{@prefix}.workspace_deployments deployment
    WHERE deployment.workspace_id = freshness.workspace_id
      AND deployment.deployment_id = freshness.deployment_id
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_window_states
    ALTER COLUMN evidence_generation_id SET NOT NULL
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_freshness_states
    ALTER COLUMN evidence_generation_id SET NOT NULL,
    ALTER COLUMN manifest_version_id SET NOT NULL
    """)

    execute("ALTER TABLE #{@prefix}.asset_window_states DROP CONSTRAINT asset_window_states_pkey")

    execute(
      "ALTER TABLE #{@prefix}.asset_freshness_states DROP CONSTRAINT asset_freshness_states_pkey"
    )

    execute("""
    ALTER TABLE #{@prefix}.asset_window_states
    ADD CONSTRAINT asset_window_states_pkey
    PRIMARY KEY (workspace_id, evidence_generation_id, target_id, window_key)
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_freshness_states
    ADD CONSTRAINT asset_freshness_states_pkey
    PRIMARY KEY (workspace_id, evidence_generation_id, target_id, freshness_key)
    """)

    drop(index(:asset_window_states, [], prefix: @prefix, name: :asset_window_states_history_idx))

    create(
      index(
        :asset_window_states,
        [:workspace_id, :evidence_generation_id, :target_id, {:desc, :window_start}, :window_key],
        prefix: @prefix,
        name: :asset_window_states_history_idx
      )
    )

    create(
      constraint(:asset_window_states, :asset_window_states_evidence_generation_valid,
        prefix: @prefix,
        check: "octet_length(evidence_generation_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:asset_freshness_states, :asset_freshness_states_evidence_generation_valid,
        prefix: @prefix,
        check:
          "octet_length(evidence_generation_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(manifest_version_id) BETWEEN 1 AND 255"
      )
    )
  end

  defp restore_projection_identity do
    drop(
      constraint(:asset_freshness_states, :asset_freshness_states_evidence_generation_valid,
        prefix: @prefix
      )
    )

    drop(
      constraint(:asset_window_states, :asset_window_states_evidence_generation_valid,
        prefix: @prefix
      )
    )

    drop(index(:asset_window_states, [], prefix: @prefix, name: :asset_window_states_history_idx))
    execute("ALTER TABLE #{@prefix}.asset_window_states DROP CONSTRAINT asset_window_states_pkey")

    execute(
      "ALTER TABLE #{@prefix}.asset_freshness_states DROP CONSTRAINT asset_freshness_states_pkey"
    )

    execute("""
    ALTER TABLE #{@prefix}.asset_window_states
    ADD CONSTRAINT asset_window_states_pkey
    PRIMARY KEY (workspace_id, manifest_version_id, target_id, window_key)
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_freshness_states
    ADD CONSTRAINT asset_freshness_states_pkey
    PRIMARY KEY (workspace_id, deployment_id, target_id, freshness_key)
    """)

    create(
      index(
        :asset_window_states,
        [:workspace_id, :manifest_version_id, :target_id, {:desc, :window_start}, :window_key],
        prefix: @prefix,
        name: :asset_window_states_history_idx
      )
    )

    alter table(:asset_freshness_states, prefix: @prefix) do
      remove(:manifest_version_id)
      remove(:evidence_generation_id)
    end

    alter table(:asset_window_states, prefix: @prefix) do
      remove(:evidence_generation_id)
    end
  end

  defp remove_materialization_generations do
    drop(constraint(:materializations, :materializations_generation_valid, prefix: @prefix))

    drop(
      constraint(:materialization_claims, :materialization_claims_generation_valid,
        prefix: @prefix
      )
    )

    drop(index(:materializations, [], prefix: @prefix, name: :materializations_generation_idx))

    execute("""
    ALTER TABLE #{@prefix}.materializations
    DROP CONSTRAINT materializations_target_generation_fk
    """)

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    DROP CONSTRAINT materialization_claims_target_generation_fk
    """)

    alter table(:materializations, prefix: @prefix) do
      remove(:evidence_generation_id)
      remove(:target_generation_id)
    end

    alter table(:materialization_claims, prefix: @prefix) do
      remove(:evidence_generation_id)
      remove(:target_generation_id)
    end
  end

  defp identifier_constraint(table, columns) do
    check =
      Enum.map_join(columns, " AND ", fn column ->
        "(#{column} IS NULL OR octet_length(#{column}) BETWEEN 1 AND 255)"
      end)

    create(
      constraint(table, String.to_atom("#{table}_identifier_lengths_v2"),
        prefix: @prefix,
        check: check
      )
    )
  end

  defp payload_constraint(table, columns) do
    check =
      Enum.map_join(columns, " AND ", fn {column, max_bytes} ->
        "(#{column} IS NULL OR octet_length(#{column}::text) <= #{max_bytes})"
      end)

    create(
      constraint(table, String.to_atom("#{table}_payload_bounds_v2"),
        prefix: @prefix,
        check: check
      )
    )
  end
end
