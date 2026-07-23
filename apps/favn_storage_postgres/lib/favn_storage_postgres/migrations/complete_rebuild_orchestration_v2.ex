defmodule FavnStoragePostgres.Migrations.CompleteRebuildOrchestrationV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:materialization_claims, prefix: @prefix) do
      add(:operation_id, :text)
    end

    create(
      constraint(:materialization_claims, :materialization_claims_operation_id_valid,
        prefix: @prefix,
        check: "operation_id IS NULL OR octet_length(operation_id) BETWEEN 1 AND 255"
      )
    )

    alter table(:rebuild_operations, prefix: @prefix) do
      add(:plan_payload, :map)
      add(:last_command_id, :text)
      add(:dispatcher_owner, :text)
      add(:dispatcher_fencing_token, :bigint, null: false, default: 0)
      add(:dispatcher_expires_at, :timestamptz)
      add(:cancel_requested, :boolean, null: false, default: false)
    end

    alter table(:rebuild_plan_actions, prefix: @prefix) do
      add(:activation_intent, :map)
      add(:validation_result, :map)
      add(:terminal_error, :map)
      add(:cleanup_state, :text, null: false, default: "not_started")
      add(:activated_at, :timestamptz)
      add(:last_command_id, :text)
    end

    alter table(:rebuild_windows, prefix: @prefix) do
      add(:runtime_input_expectation, :map)
    end

    replace_rebuild_window_values(true)

    replace_rebuild_operation_authority(
      1_000_000,
      "state NOT IN ('succeeded', 'failed', 'cancelled') OR " <>
        "cleanup_state IN ('pending', 'failed')"
    )

    create(
      constraint(:rebuild_operations, :rebuild_operations_dispatch_valid,
        prefix: @prefix,
        check:
          "dispatcher_fencing_token >= 0 AND " <>
            "((dispatcher_owner IS NULL AND dispatcher_expires_at IS NULL) OR " <>
            "(dispatcher_owner IS NOT NULL AND dispatcher_expires_at IS NOT NULL)) AND " <>
            "(plan_payload IS NULL OR jsonb_typeof(plan_payload) = 'object') AND " <>
            "(plan_payload IS NULL OR octet_length(plan_payload::text) <= 1048576)"
      )
    )

    create(
      constraint(:rebuild_plan_actions, :rebuild_plan_actions_saga_valid,
        prefix: @prefix,
        check:
          "cleanup_state IN ('not_started', 'pending', 'running', 'complete', 'failed') " <>
            "AND (activation_intent IS NULL OR jsonb_typeof(activation_intent) = 'object') " <>
            "AND (validation_result IS NULL OR jsonb_typeof(validation_result) = 'object') " <>
            "AND (terminal_error IS NULL OR jsonb_typeof(terminal_error) = 'object') " <>
            "AND (last_command_id IS NULL OR octet_length(last_command_id) BETWEEN 1 AND 255) " <>
            "AND (activation_intent IS NULL OR octet_length(activation_intent::text) <= 65536) " <>
            "AND (validation_result IS NULL OR octet_length(validation_result::text) <= 262144) " <>
            "AND (terminal_error IS NULL OR octet_length(terminal_error::text) <= 65536)"
      )
    )

    create(
      constraint(:rebuild_windows, :rebuild_windows_runtime_input_valid,
        prefix: @prefix,
        check:
          "runtime_input_expectation IS NULL OR " <>
            "(jsonb_typeof(runtime_input_expectation) = 'object' AND " <>
            "octet_length(runtime_input_expectation::text) <= 8192)"
      )
    )
  end

  def down do
    drop(
      constraint(:materialization_claims, :materialization_claims_operation_id_valid,
        prefix: @prefix
      )
    )

    drop(constraint(:rebuild_plan_actions, :rebuild_plan_actions_saga_valid, prefix: @prefix))
    drop(constraint(:rebuild_windows, :rebuild_windows_runtime_input_valid, prefix: @prefix))
    drop(constraint(:rebuild_operations, :rebuild_operations_dispatch_valid, prefix: @prefix))

    replace_rebuild_window_values(false)

    replace_rebuild_operation_authority(
      100_000,
      "state NOT IN ('succeeded', 'failed', 'cancelled')"
    )

    alter table(:rebuild_plan_actions, prefix: @prefix) do
      remove(:activated_at)
      remove(:last_command_id)
      remove(:cleanup_state)
      remove(:terminal_error)
      remove(:validation_result)
      remove(:activation_intent)
    end

    alter table(:rebuild_windows, prefix: @prefix) do
      remove(:runtime_input_expectation)
    end

    alter table(:rebuild_operations, prefix: @prefix) do
      remove(:cancel_requested)
      remove(:dispatcher_expires_at)
      remove(:dispatcher_fencing_token)
      remove(:dispatcher_owner)
      remove(:last_command_id)
      remove(:plan_payload)
    end

    alter table(:materialization_claims, prefix: @prefix) do
      remove(:operation_id)
    end
  end

  defp replace_rebuild_operation_authority(max_window_count, recovery_predicate) do
    drop(constraint(:rebuild_operations, :rebuild_operations_values_valid, prefix: @prefix))

    drop(
      index(:rebuild_operations, [:workspace_id, :state, :updated_at, :operation_id],
        prefix: @prefix,
        name: :rebuild_operations_recovery_idx
      )
    )

    create(
      index(:rebuild_operations, [:workspace_id, :state, :updated_at, :operation_id],
        prefix: @prefix,
        name: :rebuild_operations_recovery_idx,
        where: recovery_predicate
      )
    )

    create(
      constraint(:rebuild_operations, :rebuild_operations_values_valid,
        prefix: @prefix,
        check:
          "plan_hash ~ '^[0-9a-f]{64}$' AND plan_version > 0 AND trigger = 'manual' " <>
            "AND action_count BETWEEN 1 AND 10000 AND window_count BETWEEN 1 AND #{max_window_count} " <>
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
  end

  defp replace_rebuild_window_values(empty_generation?) do
    drop(constraint(:rebuild_windows, :rebuild_windows_values_valid, prefix: @prefix))

    work_kinds =
      if empty_generation?,
        do: "'window', 'full_load', 'empty_generation'",
        else: "'window', 'full_load'"

    empty_clause =
      if empty_generation? do
        " OR (work_kind = 'empty_generation' AND window_start IS NOT NULL AND " <>
          "window_end IS NOT NULL AND window_start < window_end)"
      else
        ""
      end

    create(
      constraint(:rebuild_windows, :rebuild_windows_values_valid,
        prefix: @prefix,
        check:
          "ordinal BETWEEN 0 AND 99999 AND work_kind IN (#{work_kinds}) " <>
            "AND status IN ('planned', 'ready', 'claimed', 'running', 'succeeded', 'failed', " <>
            "'cancelled', 'outcome_unknown') AND attempt_count >= 0 AND fencing_token >= 0 " <>
            "AND (row_count IS NULL OR row_count >= 0) AND version > 0 " <>
            "AND ((work_kind = 'window' AND window_start IS NOT NULL AND " <>
            "window_end IS NOT NULL AND window_start < window_end) OR " <>
            "(work_kind = 'full_load' AND window_key = 'full_load' AND window_start IS NULL " <>
            "AND window_end IS NULL)#{empty_clause})"
      )
    )
  end
end
