defmodule FavnStoragePostgres.Migrations.AddCommitSafeLogReplayV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:log_entries, prefix: @prefix) do
      add(:asset_step_id, :text)
      add(:runner_execution_id, :text)
      add(:node_key_hash, :binary)
      add(:asset_ref_hash, :binary)
      add(:stream, :text)
    end

    create(
      unique_index(:log_batches, [:workspace_id, :outbox_event_id],
        prefix: @prefix,
        name: :log_batches_outbox_uidx
      )
    )

    create(
      index(
        :log_entries,
        [:workspace_id, :run_id, :asset_step_id, {:desc, :occurred_at}, {:desc, :log_id}],
        prefix: @prefix,
        name: :log_entries_asset_idx,
        where: "run_id IS NOT NULL AND asset_step_id IS NOT NULL"
      )
    )

    create(
      index(
        :log_entries,
        [:workspace_id, :runner_execution_id, {:desc, :occurred_at}, {:desc, :log_id}],
        prefix: @prefix,
        name: :log_entries_runner_idx,
        where: "runner_execution_id IS NOT NULL"
      )
    )

    create(
      index(:log_entries, [:workspace_id, :node_key_hash, :log_id],
        prefix: @prefix,
        name: :log_entries_node_key_idx,
        where: "node_key_hash IS NOT NULL"
      )
    )

    create(
      index(:log_entries, [:workspace_id, :asset_ref_hash, :log_id],
        prefix: @prefix,
        name: :log_entries_asset_ref_idx,
        where: "asset_ref_hash IS NOT NULL"
      )
    )

    create(
      constraint(:log_entries, :log_entries_filter_values_valid,
        prefix: @prefix,
        check:
          "(asset_step_id IS NULL OR octet_length(asset_step_id) BETWEEN 1 AND 255) " <>
            "AND (runner_execution_id IS NULL OR octet_length(runner_execution_id) BETWEEN 1 AND 255) " <>
            "AND (stream IS NULL OR stream IN ('stdout', 'stderr', 'system'))"
      )
    )
  end

  def down do
    drop(constraint(:log_entries, :log_entries_filter_values_valid, prefix: @prefix))
    drop(index(:log_entries, [], prefix: @prefix, name: :log_entries_asset_ref_idx))
    drop(index(:log_entries, [], prefix: @prefix, name: :log_entries_node_key_idx))
    drop(index(:log_entries, [], prefix: @prefix, name: :log_entries_runner_idx))
    drop(index(:log_entries, [], prefix: @prefix, name: :log_entries_asset_idx))

    drop(
      index(:log_batches, [:workspace_id, :outbox_event_id],
        prefix: @prefix,
        name: :log_batches_outbox_uidx
      )
    )

    alter table(:log_entries, prefix: @prefix) do
      remove(:stream)
      remove(:asset_ref_hash)
      remove(:node_key_hash)
      remove(:runner_execution_id)
      remove(:asset_step_id)
    end
  end
end
