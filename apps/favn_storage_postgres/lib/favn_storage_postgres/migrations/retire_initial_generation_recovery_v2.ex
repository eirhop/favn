defmodule FavnStoragePostgres.Migrations.RetireInitialGenerationRecoveryV2 do
  use Ecto.Migration
  @prefix "favn_control"

  def up do
    execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM favn_control.runner_tasks WHERE task_kind='generation_marker_initialize')
        OR EXISTS (SELECT 1 FROM favn_control.target_operation_locks WHERE operation_type='target_recovery')
        OR EXISTS (SELECT 1 FROM favn_control.target_recovery_operations)
      THEN RAISE EXCEPTION 'Atomic generation publication requires a fresh environment; historical registration and target recovery are unsupported';
      END IF;
    END $$
    """)

    drop(table(:target_recovery_operations, prefix: @prefix))
    drop(constraint(:runner_tasks, :runner_tasks_kind_valid, prefix: @prefix))

    create(
      constraint(:runner_tasks, :runner_tasks_kind_valid,
        prefix: @prefix,
        check:
          "task_kind IN ('asset_attempt','runtime_input_resolution','relation_inspection','generation_capabilities','generation_marker_read','generation_activate','generation_reconcile','generation_discard')"
      )
    )

    drop(
      constraint(:target_operation_locks, :target_operation_locks_values_valid, prefix: @prefix)
    )

    create(
      constraint(:target_operation_locks, :target_operation_locks_values_valid,
        prefix: @prefix,
        check:
          "operation_type IN ('materialization','rebuild') AND fencing_token > 0 AND version > 0"
      )
    )
  end

  def down, do: raise("Atomic generation publication is a reset-only contract change")
end
