defmodule FavnStoragePostgres.Migrations.AddRebuildRunnerBindingsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    execute(
      "ALTER TABLE #{@prefix}.rebuild_operations " <>
        "ALTER COLUMN candidate_generation_id DROP NOT NULL"
    )

    replace_rebuild_operation_authority(true)

    alter table(:rebuild_plan_actions, prefix: @prefix) do
      add(:runner_pool, :text)
      add(:required_runner_release_id, :text)
    end

    create(
      constraint(:rebuild_plan_actions, :rebuild_plan_actions_runner_binding_valid,
        prefix: @prefix,
        check:
          "(runner_pool IS NULL AND required_runner_release_id IS NULL) OR " <>
            "(runner_pool ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$' AND " <>
            "required_runner_release_id ~ '^rr_[0-9a-f]{64}$')"
      )
    )

    create(
      index(:runner_tasks, [:workspace_id, :operation_id, :status, :task_id],
        prefix: @prefix,
        name: :runner_tasks_operation_cancellation_idx,
        where:
          "operation_id IS NOT NULL AND status IN " <>
            "('queued','assigned','preparing','running','cancelling')"
      )
    )
  end

  def down do
    drop(
      index(:runner_tasks, [:workspace_id, :operation_id, :status, :task_id],
        prefix: @prefix,
        name: :runner_tasks_operation_cancellation_idx
      )
    )

    drop(
      constraint(:rebuild_plan_actions, :rebuild_plan_actions_runner_binding_valid,
        prefix: @prefix
      )
    )

    alter table(:rebuild_plan_actions, prefix: @prefix) do
      remove(:runner_pool)
      remove(:required_runner_release_id)
    end

    replace_rebuild_operation_authority(false)

    execute(
      "ALTER TABLE #{@prefix}.rebuild_operations " <>
        "ALTER COLUMN candidate_generation_id SET NOT NULL"
    )
  end

  defp replace_rebuild_operation_authority(planning?) do
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
        where:
          "state NOT IN ('succeeded', 'failed', 'cancelled') OR " <>
            "cleanup_state IN ('pending', 'failed')"
      )
    )

    {counts, states, phases, candidate} =
      if planning? do
        {
          "((state IN ('planning', 'failed', 'cancelled') AND action_count = 0 AND " <>
            "window_count = 0 AND candidate_generation_id IS NULL) OR " <>
            "(state <> 'planning' AND action_count BETWEEN 1 AND 10000 AND " <>
            "window_count BETWEEN 1 AND 1000000 AND candidate_generation_id IS NOT NULL))",
          "'planning', 'planned', 'queued', 'building', 'validating', 'activating', " <>
            "'activation_unknown', 'reconciling', 'cancelling', 'succeeded', 'failed', " <>
            "'cancelled'",
          "'planning', 'planned', 'locking', 'building', 'validating', 'activating', " <>
            "'reconciling', 'repairing', 'cleanup', 'terminal'",
          "((action_count = 0 AND window_count = 0 AND candidate_generation_id IS NULL) OR " <>
            "(action_count > 0 AND window_count > 0 AND candidate_generation_id IS NOT NULL))"
        }
      else
        {
          "action_count BETWEEN 1 AND 10000 AND window_count BETWEEN 1 AND 1000000",
          "'planned', 'queued', 'building', 'validating', 'activating', " <>
            "'activation_unknown', 'reconciling', 'cancelling', 'succeeded', 'failed', " <>
            "'cancelled'",
          "'planned', 'locking', 'building', 'validating', 'activating', " <>
            "'reconciling', 'repairing', 'cleanup', 'terminal'",
          "candidate_generation_id IS NOT NULL"
        }
      end

    create(
      constraint(:rebuild_operations, :rebuild_operations_values_valid,
        prefix: @prefix,
        check:
          "plan_hash ~ '^[0-9a-f]{64}$' AND plan_version > 0 AND trigger = 'manual' " <>
            "AND #{counts} AND state IN (#{states}) AND phase IN (#{phases}) " <>
            "AND #{candidate} " <>
            "AND cleanup_state IN ('not_started', 'pending', 'running', 'complete', 'failed') " <>
            "AND octet_length(reason) BETWEEN 1 AND 4096 " <>
            "AND (coverage_start IS NULL) = (coverage_end IS NULL) " <>
            "AND (coverage_start IS NULL OR coverage_start < coverage_end) AND version > 0"
      )
    )
  end
end
