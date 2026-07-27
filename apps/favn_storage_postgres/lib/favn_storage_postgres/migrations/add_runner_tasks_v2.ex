defmodule FavnStoragePostgres.Migrations.AddRunnerTasksV2 do
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:runner_tasks, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:task_id, :text, null: false, primary_key: true)
      add(:domain_identity, :text, null: false)
      add(:task_kind, :text, null: false)
      add(:run_id, :text)
      add(:operation_id, :text)
      add(:asset_step_id, :text)
      add(:runner_pool, :text, null: false)
      add(:required_runner_release_id, :text, null: false)
      add(:required_capability, :text)
      add(:retry_class, :text, null: false)
      add(:status, :text, null: false)
      add(:enqueued_at, :utc_datetime_usec, null: false)
      add(:deadline_at, :utc_datetime_usec)
      add(:payload_version, :integer, null: false)
      add(:payload, :map, null: false)
      add(:payload_hash, :binary, null: false)
      add(:orchestration_context, :map, null: false)
      add(:assigned_runner_instance_id, :text)
      add(:assigned_runner_session_generation, :bigint)
      add(:assignment_generation, :bigint, null: false, default: 0)
      add(:assignment_expires_at, :utc_datetime_usec)
      add(:cancellation_requested_at, :utc_datetime_usec)
      add(:cancellation_acknowledged_at, :utc_datetime_usec)
      add(:runtime_input_resolution_id, :text)
      add(:runtime_input_resolution_status, :text)
      add(:runtime_input_payload_fingerprint, :binary)
      add(:runtime_input_error, :map)
      add(:runtime_inputs_resolved_at, :utc_datetime_usec)
      add(:last_command_id, :text, null: false)
      add(:result_version, :integer)
      add(:result, :map)
      add(:error, :map)
      add(:terminal_at, :utc_datetime_usec)
      add(:inserted_at, :utc_datetime_usec, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    create(
      unique_index(:runner_tasks, [:workspace_id, :domain_identity],
        name: :runner_tasks_domain_identity_uidx,
        prefix: @prefix
      )
    )

    create(
      index(
        :runner_tasks,
        [
          :runner_pool,
          :required_runner_release_id,
          :status,
          :enqueued_at,
          :task_id
        ],
        name: :runner_tasks_claim_idx,
        prefix: @prefix
      )
    )

    create(
      index(:runner_tasks, [:workspace_id, :run_id, :status, :enqueued_at, :task_id],
        name: :runner_tasks_run_idx,
        prefix: @prefix
      )
    )

    create(
      index(:runner_tasks, [:workspace_id, :assignment_expires_at, :task_id],
        name: :runner_tasks_expired_idx,
        prefix: @prefix,
        where: "status IN ('assigned','preparing','running','cancelling')"
      )
    )

    create(
      index(:runner_tasks, [:workspace_id, :terminal_at, :task_id],
        name: :runner_tasks_terminal_retention_idx,
        prefix: @prefix,
        where: "terminal_at IS NOT NULL"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_status_valid,
        prefix: @prefix,
        check:
          "status IN ('queued','assigned','preparing','running','cancelling','succeeded','failed','cancelled','unknown')"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_kind_valid,
        prefix: @prefix,
        check:
          "task_kind IN ('asset_attempt','relation_inspection','generation_capabilities','generation_marker_read','generation_marker_initialize','generation_activate','generation_reconcile','generation_discard')"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_retry_class_valid,
        prefix: @prefix,
        check:
          "retry_class IN ('safe_to_retry','reconcile_before_retry','unknown_do_not_retry','terminal')"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_identity_valid,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND task_id ~ '^rt_[A-Za-z0-9_-]{1,252}$' AND octet_length(task_id) <= 255 AND octet_length(domain_identity) BETWEEN 1 AND 255 AND runner_pool ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$' AND required_runner_release_id ~ '^rr_[0-9a-f]{64}$' AND octet_length(last_command_id) BETWEEN 1 AND 255 AND (run_id IS NULL OR octet_length(run_id) BETWEEN 1 AND 255) AND (operation_id IS NULL OR octet_length(operation_id) BETWEEN 1 AND 255) AND (asset_step_id IS NULL OR octet_length(asset_step_id) BETWEEN 1 AND 255) AND (required_capability IS NULL OR octet_length(required_capability) BETWEEN 1 AND 255) AND (assigned_runner_instance_id IS NULL OR octet_length(assigned_runner_instance_id) BETWEEN 1 AND 255)"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_assignment_valid,
        prefix: @prefix,
        check:
          "(status = 'queued' AND assigned_runner_instance_id IS NULL AND assigned_runner_session_generation IS NULL AND assignment_expires_at IS NULL) OR (status IN ('assigned','preparing','running','cancelling') AND assigned_runner_instance_id IS NOT NULL AND assigned_runner_session_generation IS NOT NULL AND assignment_generation > 0 AND assignment_expires_at IS NOT NULL) OR (status IN ('succeeded','failed','cancelled','unknown') AND assignment_expires_at IS NULL)"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_state_shape_valid,
        prefix: @prefix,
        check:
          "(status IN ('queued','assigned','preparing','running','cancelling') AND terminal_at IS NULL AND result_version IS NULL AND result IS NULL AND error IS NULL) OR (status = 'succeeded' AND terminal_at IS NOT NULL AND result_version IS NOT NULL AND result IS NOT NULL AND error IS NULL) OR (status IN ('failed','unknown') AND terminal_at IS NOT NULL AND result_version IS NOT NULL AND error IS NOT NULL) OR (status = 'cancelled' AND terminal_at IS NOT NULL)"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_payload_valid,
        prefix: @prefix,
        check:
          "payload_version = 13 AND octet_length(payload_hash) = 32 AND pg_column_size(payload) <= 2097152 AND pg_column_size(orchestration_context) <= 2097152 AND (result IS NULL OR pg_column_size(result) <= 2097152) AND (error IS NULL OR pg_column_size(error) <= 262144) AND (runtime_input_error IS NULL OR pg_column_size(runtime_input_error) <= 262144)"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_time_valid,
        prefix: @prefix,
        check:
          "(deadline_at IS NULL OR deadline_at >= enqueued_at) AND (terminal_at IS NULL OR terminal_at >= enqueued_at) AND (cancellation_requested_at IS NULL OR cancellation_requested_at >= enqueued_at) AND (cancellation_acknowledged_at IS NULL OR (cancellation_requested_at IS NOT NULL AND cancellation_acknowledged_at >= cancellation_requested_at))"
      )
    )

    create(
      constraint(:runner_tasks, :runner_tasks_runtime_inputs_valid,
        prefix: @prefix,
        check:
          "(runtime_input_resolution_status IS NULL AND runtime_input_resolution_id IS NULL AND runtime_input_payload_fingerprint IS NULL AND runtime_input_error IS NULL AND runtime_inputs_resolved_at IS NULL) OR (runtime_input_resolution_status = 'resolved' AND runtime_input_resolution_id IS NOT NULL AND octet_length(runtime_input_resolution_id) BETWEEN 1 AND 255 AND octet_length(runtime_input_payload_fingerprint) = 32 AND runtime_input_error IS NULL AND runtime_inputs_resolved_at IS NOT NULL) OR (runtime_input_resolution_status = 'failed' AND runtime_input_resolution_id IS NOT NULL AND octet_length(runtime_input_resolution_id) BETWEEN 1 AND 255 AND runtime_input_payload_fingerprint IS NULL AND runtime_input_error IS NOT NULL AND runtime_inputs_resolved_at IS NOT NULL)"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.runner_tasks
    ADD CONSTRAINT runner_tasks_workspace_fkey
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    create table(:runner_task_log_batches, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:task_id, :text, null: false, primary_key: true)
      add(:batch_id, :text, null: false, primary_key: true)
      add(:assignment_generation, :bigint, null: false)
      add(:sequence, :bigint, null: false)
      add(:entries, {:array, :map}, null: false)
      add(:payload_hash, :binary, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.runner_task_log_batches
    ADD CONSTRAINT runner_task_log_batches_task_fkey
    FOREIGN KEY (workspace_id, task_id)
    REFERENCES #{@prefix}.runner_tasks(workspace_id, task_id)
    ON DELETE CASCADE
    """)

    create(
      constraint(:runner_task_log_batches, :runner_task_log_batches_identity_valid,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND task_id ~ '^rt_[A-Za-z0-9_-]{1,252}$' AND octet_length(batch_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:runner_task_log_batches, :runner_task_log_batches_payload_valid,
        prefix: @prefix,
        check:
          "assignment_generation > 0 AND sequence >= 0 AND octet_length(payload_hash) = 32 AND pg_column_size(entries) <= 262144"
      )
    )

    create(
      unique_index(
        :runner_task_log_batches,
        [:workspace_id, :task_id, :assignment_generation, :sequence],
        name: :runner_task_log_batches_sequence_uidx,
        prefix: @prefix
      )
    )

    create table(:runner_capacity_demands, primary_key: false, prefix: @prefix) do
      add(:runner_pool, :text, null: false, primary_key: true)
      add(:required_runner_release_id, :text, null: false, primary_key: true)
      add(:outstanding_count, :bigint, null: false, default: 0)
      add(:queued_count, :bigint, null: false, default: 0)
      add(:active_count, :bigint, null: false, default: 0)
      add(:oldest_queued_at, :utc_datetime_usec)
      add(:version, :bigint, null: false, default: 1)
      add(:healthy, :boolean, null: false, default: true)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    create(
      constraint(:runner_capacity_demands, :runner_capacity_demands_counts_valid,
        prefix: @prefix,
        check:
          "outstanding_count >= 0 AND queued_count >= 0 AND active_count >= 0 AND outstanding_count = queued_count + active_count"
      )
    )

    create(
      constraint(:runner_capacity_demands, :runner_capacity_demands_identity_valid,
        prefix: @prefix,
        check:
          "runner_pool ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$' AND required_runner_release_id ~ '^rr_[0-9a-f]{64}$'"
      )
    )

    create table(:runner_task_commands, primary_key: false, prefix: @prefix) do
      add(:scope_id, :text, null: false, primary_key: true)
      add(:command_id, :text, null: false, primary_key: true)
      add(:operation, :text, null: false)
      add(:request_hash, :binary, null: false)
      add(:result, :map, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
    end

    create(
      constraint(:runner_task_commands, :runner_task_commands_values_valid,
        prefix: @prefix,
        check:
          "octet_length(scope_id) BETWEEN 1 AND 255 AND octet_length(command_id) BETWEEN 1 AND 255 AND operation IN ('enqueue','claim','transition','runtime_inputs','append_log_batch','complete','request_cancellation','acknowledge_cancellation','release','retry','recover_expired','reconcile_demand') AND octet_length(request_hash) = 32 AND pg_column_size(result) <= 262144"
      )
    )
  end

  def down do
    drop(table(:runner_task_commands, prefix: @prefix))
    drop(table(:runner_capacity_demands, prefix: @prefix))
    drop(table(:runner_task_log_batches, prefix: @prefix))
    drop(table(:runner_tasks, prefix: @prefix))
  end
end
