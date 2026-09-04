defmodule FavnStoragePostgres.Migrations.AddCrashSafeRunnerTasksV2 do
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM favn_control.runner_tasks LIMIT 1)
        OR EXISTS (SELECT 1 FROM favn_control.materialization_claims LIMIT 1)
        OR EXISTS (SELECT 1 FROM favn_control.target_operation_locks LIMIT 1) THEN
        RAISE EXCEPTION 'Runner task persistence changed. Stop development writers and explicitly bootstrap a fresh control-plane database; old task data cannot be upgraded.';
      END IF;
    END $$
    """)

    alter table(:runner_tasks, prefix: @prefix) do
      add(:manifest_version_id, :text, null: false)
      add(:manifest_content_hash, :text, null: false)
      add(:orchestration_context_hash, :binary, null: false)
      add(:write_claim_key, :text)
      add(:write_claim_fence, :bigint)
      add(:write_target_id, :text)
      add(:write_operation_id, :text)
      add(:write_lock_fence, :bigint)
      add(:persistence_failure, :text)
    end

    alter table(:materialization_claims, prefix: @prefix) do
      add(:purpose, :text, null: false, default: "materialization")
    end

    drop(
      constraint(:materialization_claims, :materialization_claims_values_valid, prefix: @prefix)
    )

    create(
      constraint(:materialization_claims, :materialization_claims_values_valid,
        prefix: @prefix,
        check:
          "fencing_token > 0 AND version > 0 AND status IN ('claimed', 'succeeded', 'failed', 'expired', 'released') AND " <>
            "purpose IN ('materialization', 'ownership_only') AND (status != 'released' OR purpose = 'ownership_only')"
      )
    )

    for name <- [:materialization_claims, :target_operation_locks] do
      alter table(name, prefix: @prefix) do
        add(:effect_state, :text, null: false, default: "not_started")
        add(:effect_task_id, :text)
        add(:effect_assignment_generation, :bigint)
        add(:effect_started_at, :utc_datetime_usec)
        add(:effect_resolution, :map)
      end

      create(
        constraint(name, "#{name}_effect_state_valid",
          prefix: @prefix,
          check:
            "effect_state IN ('not_started', 'in_flight', 'outcome_unknown', 'resolved') AND " <>
              "(effect_state NOT IN ('in_flight', 'outcome_unknown', 'resolved') OR " <>
              "(effect_task_id IS NOT NULL AND effect_assignment_generation > 0 AND effect_started_at IS NOT NULL))"
        )
      )

      create(
        index(name, [:workspace_id, :effect_task_id],
          prefix: @prefix,
          where: "effect_task_id IS NOT NULL"
        )
      )
    end

    drop(
      index(:materialization_claims, [:status, :updated_at, :workspace_id, :claim_key],
        prefix: @prefix,
        name: :materialization_claims_retention_idx
      )
    )

    create(
      index(:materialization_claims, [:updated_at, :workspace_id, :claim_key],
        prefix: @prefix,
        name: :materialization_claims_retention_idx,
        where:
          "status IN ('succeeded', 'failed', 'expired', 'released') AND effect_state NOT IN ('in_flight', 'outcome_unknown')"
      )
    )

    create(
      index(:materialization_claims, [:workspace_id, :target_id, :operation_id],
        prefix: @prefix,
        name: :materialization_claims_unresolved_target_idx,
        where: "effect_state IN ('in_flight', 'outcome_unknown')"
      )
    )

    execute("""
    ALTER TABLE favn_control.runner_tasks
      ADD CONSTRAINT runner_tasks_manifest_pin_fk FOREIGN KEY (manifest_version_id)
        REFERENCES favn_control.manifest_versions (manifest_version_id),
      ADD CONSTRAINT runner_tasks_persistence_failure_valid CHECK
        (persistence_failure IS NULL OR persistence_failure IN ('payload', 'context', 'result', 'manifest_pin'))
    """)

    drop(constraint(:runner_tasks, :runner_tasks_payload_valid, prefix: @prefix))

    create(
      constraint(:runner_tasks, :runner_tasks_payload_valid,
        prefix: @prefix,
        check:
          "payload_version = 13 AND octet_length(payload_hash) = 32 AND " <>
            "octet_length(orchestration_context_hash) = 32 AND " <>
            "pg_column_size(payload) <= CASE WHEN task_kind = 'asset_attempt' THEN 33562624 ELSE 4202496 END AND " <>
            "pg_column_size(orchestration_context) <= 16785408 AND " <>
            "(result IS NULL OR pg_column_size(result) <= 4202496) AND " <>
            "(error IS NULL OR pg_column_size(error) <= 262144) AND " <>
            "(runtime_input_error IS NULL OR pg_column_size(runtime_input_error) <= 262144)"
      )
    )

    execute("""
    ALTER TABLE favn_control.runner_task_outcomes
      DROP CONSTRAINT runner_task_outcomes_values_valid,
      ADD CONSTRAINT runner_task_outcomes_values_valid CHECK (
        assignment_generation >= 0 AND (result_version IS NULL OR result_version >= 0)
        AND octet_length(result_hash) = 32
        AND (result IS NULL OR pg_column_size(result) <= 4202496)
        AND (error IS NULL OR pg_column_size(error) <= 262144))
    """)

    drop(constraint(:runner_task_commands, :runner_task_commands_values_valid, prefix: @prefix))

    create(
      constraint(:runner_task_commands, :runner_task_commands_values_valid,
        prefix: @prefix,
        check:
          "octet_length(scope_id) BETWEEN 1 AND 255 AND octet_length(command_id) BETWEEN 1 AND 255 AND " <>
            "operation IN ('enqueue','claim','transition','runtime_inputs','append_log_batch','complete','request_cancellation','acknowledge_cancellation','release','retry','recover_expired','reconcile_demand','resolve_write') AND " <>
            "octet_length(request_hash) = 32 AND pg_column_size(result) <= 262144"
      )
    )
  end

  def down,
    do:
      raise(
        "Crash recovery persistence requires a separate compatible development database for rollback"
      )
end
