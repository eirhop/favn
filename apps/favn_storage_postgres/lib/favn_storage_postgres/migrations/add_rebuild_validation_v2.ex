defmodule FavnStoragePostgres.Migrations.AddRebuildValidationV2 do
  use Ecto.Migration
  @prefix "favn_control"

  def up do
    alter table(:rebuild_operations, prefix: @prefix) do
      add(:validation_request, :map)
    end

    create(
      constraint(:rebuild_operations, :rebuild_validation_bounded,
        prefix: @prefix,
        check:
          "validation_request IS NULL OR (jsonb_typeof(validation_request) = 'object' AND octet_length(validation_request::text) <= 8192)"
      )
    )

    execute(
      "CREATE INDEX rebuild_validation_expiry ON favn_control.rebuild_operations (workspace_id, dispatcher_expires_at) WHERE validation_request->>'status' = 'active'"
    )

    execute("""
    UPDATE favn_control.rebuild_operations
    SET state = 'failed', phase = 'terminal', dispatcher_owner = NULL,
        dispatcher_expires_at = NULL, completed_at = clock_timestamp(),
        updated_at = clock_timestamp(), version = version + 1,
        terminal_error = '{"reason_code":"rebuild_planning_failed","message":"Create a new rebuild plan after upgrading."}'::jsonb
    WHERE state = 'planning' AND validation_request IS NULL
    """)

    drop(constraint(:runner_tasks, :runner_tasks_kind_valid, prefix: @prefix))

    create(
      constraint(:runner_tasks, :runner_tasks_kind_valid,
        prefix: @prefix,
        check:
          "task_kind IN ('asset_attempt','runtime_input_resolution','relation_inspection','generation_capabilities','generation_marker_read','generation_marker_initialize','generation_activate','generation_reconcile','generation_discard')"
      )
    )
  end

  def down, do: raise("Drain rebuild validations and retain their evidence before rollback")
end
