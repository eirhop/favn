defmodule FavnStoragePostgres.Migrations.IncreaseRunnerTaskOrchestrationContextBoundV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @old_bound 2 * 1_024 * 1_024
  @new_context_bound 8 * 1_024 * 1_024

  def up do
    replace_constraint(@new_context_bound)
  end

  def down do
    replace_constraint(@old_bound)
  end

  defp replace_constraint(context_bound) do
    drop(constraint(:runner_tasks, :runner_tasks_payload_valid, prefix: @prefix))

    create(
      constraint(:runner_tasks, :runner_tasks_payload_valid,
        prefix: @prefix,
        check:
          "payload_version = 13 AND octet_length(payload_hash) = 32 AND " <>
            "pg_column_size(payload) <= 2097152 AND " <>
            "pg_column_size(orchestration_context) <= #{context_bound} AND " <>
            "(result IS NULL OR pg_column_size(result) <= 2097152) AND " <>
            "(error IS NULL OR pg_column_size(error) <= 262144) AND " <>
            "(runtime_input_error IS NULL OR pg_column_size(runtime_input_error) <= 262144)"
      )
    )
  end
end
