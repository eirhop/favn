defmodule FavnStoragePostgres.Migrations.IncreaseRunnerTaskPayloadBoundV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up, do: replace_constraint(12 * 1_024 * 1_024)
  def down, do: replace_constraint(2 * 1_024 * 1_024)

  defp replace_constraint(payload_bound) do
    drop(constraint(:runner_tasks, :runner_tasks_payload_valid, prefix: @prefix))

    create(
      constraint(:runner_tasks, :runner_tasks_payload_valid,
        prefix: @prefix,
        check:
          "payload_version = 13 AND octet_length(payload_hash) = 32 AND " <>
            "pg_column_size(payload) <= #{payload_bound} AND " <>
            "pg_column_size(orchestration_context) <= 8388608 AND " <>
            "(result IS NULL OR pg_column_size(result) <= 2097152) AND " <>
            "(error IS NULL OR pg_column_size(error) <= 262144) AND " <>
            "(runtime_input_error IS NULL OR pg_column_size(runtime_input_error) <= 262144)"
      )
    )
  end
end
