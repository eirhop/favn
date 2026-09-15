defmodule FavnStoragePostgres.Migrations.ReferenceRunnerTaskPackagesV2 do
  @moduledoc false
  use Ecto.Migration

  def up do
    execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM favn_control.workspaces)
         OR EXISTS (SELECT 1 FROM favn_control.runner_task_commands)
         OR EXISTS (SELECT 1 FROM favn_control.runner_capacity_demands)
         OR EXISTS (SELECT 1 FROM favn_control.runner_sessions) THEN
        RAISE EXCEPTION 'Task package references require an explicit environment reset before migration';
      END IF;
    END $$
    """)

    drop(constraint(:runner_tasks, :runner_tasks_payload_valid, prefix: "favn_control"))

    create(
      constraint(:runner_tasks, :runner_tasks_payload_valid,
        prefix: "favn_control",
        check:
          "payload_version = 2 AND octet_length(payload_hash) = 32 AND " <>
            "octet_length(orchestration_context_hash) = 32 AND " <>
            "pg_column_size(payload) <= CASE WHEN task_kind = 'asset_attempt' THEN 33562624 ELSE 4202496 END AND " <>
            "pg_column_size(orchestration_context) <= 16785408 AND " <>
            "(result IS NULL OR pg_column_size(result) <= 4202496) AND " <>
            "(error IS NULL OR pg_column_size(error) <= 262144) AND " <>
            "(runtime_input_error IS NULL OR pg_column_size(runtime_input_error) <= 262144)"
      )
    )
  end

  def down,
    do: raise("Task package references require a separate compatible environment for rollback")
end
