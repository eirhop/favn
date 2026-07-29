defmodule FavnStoragePostgres.Migrations.AddRunExecutionCheckpointsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @max_payload_bytes 64 * 1_024 * 1_024

  def up do
    create table(:run_execution_checkpoints, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false, primary_key: true)
      add(:owner_id, :text, null: false)
      add(:fencing_token, :bigint, null: false)
      add(:checkpoint_version, :smallint, null: false)
      add(:checkpoint_revision, :bigint, null: false)
      add(:checkpoint_sequence, :bigint, null: false)
      add(:stage, :integer, null: false)
      add(:attempt, :integer, null: false)
      add(:payload, :binary, null: false)
      add(:payload_hash, :binary, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      constraint(:run_execution_checkpoints, :run_execution_checkpoints_values_valid,
        prefix: @prefix,
        check:
          "fencing_token > 0 AND checkpoint_version = 1 AND checkpoint_revision > 0 AND " <>
            "checkpoint_sequence > 0 AND " <>
            "stage >= 0 AND attempt > 0 AND octet_length(payload) BETWEEN 1 AND " <>
            Integer.to_string(@max_payload_bytes) <>
            " AND octet_length(payload_hash) = 32"
      )
    )

    create(
      constraint(
        :run_execution_checkpoints,
        :run_execution_checkpoints_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(run_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(owner_id) BETWEEN 1 AND 255"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.run_execution_checkpoints
    ADD CONSTRAINT run_execution_checkpoints_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE CASCADE
    """)
  end

  def down do
    drop(table(:run_execution_checkpoints, prefix: @prefix))
  end
end
