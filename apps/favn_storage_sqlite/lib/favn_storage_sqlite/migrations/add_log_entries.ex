defmodule FavnStorageSqlite.Migrations.AddLogEntries do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_log_entries, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:global_sequence, :integer, null: false)
      add(:run_id, :string)
      add(:asset_step_id, :string)
      add(:node_key_hash, :string)
      add(:node_key_blob, :text)
      add(:asset_ref_key, :string)
      add(:asset_ref_blob, :text)
      add(:runner_execution_id, :string)
      add(:attempt, :integer)
      add(:producer_id, :string)
      add(:producer_sequence, :integer)
      add(:occurred_at, :text, null: false)
      add(:level, :string)
      add(:source, :string)
      add(:stream, :string)
      add(:message, :text, null: false)
      add(:metadata_blob, :text, null: false)
      add(:log_blob, :text, null: false)
      add(:truncated, :boolean, null: false, default: false)
      add(:inserted_at, :text, null: false)
    end

    create_if_not_exists(unique_index(:favn_log_entries, [:global_sequence]))
    create_if_not_exists(unique_index(:favn_log_entries, [:producer_id, :producer_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:run_id, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:run_id, :asset_step_id, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:asset_ref_key, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:level, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:source, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:occurred_at]))
  end
end
