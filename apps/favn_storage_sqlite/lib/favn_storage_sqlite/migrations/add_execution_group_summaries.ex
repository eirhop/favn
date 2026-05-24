defmodule FavnStorageSqlite.Migrations.AddExecutionGroupSummaries do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_execution_group_summaries, primary_key: false) do
      add(:group_id, :string, primary_key: true)
      add(:root_run_id, :string, null: false)
      add(:root_status, :string, null: false)
      add(:status, :string, null: false)
      add(:trigger_type, :string, null: false)
      add(:target_refs_text, :text, null: false)
      add(:has_window, :boolean, null: false, default: false)
      add(:failed, :boolean, null: false, default: false)
      add(:running, :boolean, null: false, default: false)
      add(:activity_seq, :integer, null: false)
      add(:summary_blob, :binary, null: false)
      add(:updated_at, :text, null: false)
    end

    create_if_not_exists(index(:favn_execution_group_summaries, [:activity_seq, :group_id]))
    create_if_not_exists(index(:favn_execution_group_summaries, [:trigger_type, :activity_seq]))
    create_if_not_exists(index(:favn_execution_group_summaries, [:root_status, :activity_seq]))
    create_if_not_exists(index(:favn_execution_group_summaries, [:failed, :activity_seq]))
    create_if_not_exists(index(:favn_execution_group_summaries, [:running, :activity_seq]))
    create_if_not_exists(index(:favn_execution_group_summaries, [:has_window, :activity_seq]))

    create_if_not_exists(index(:favn_log_entries, [:runner_execution_id, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:stream, :global_sequence]))
    create_if_not_exists(index(:favn_log_entries, [:node_key_hash, :global_sequence]))
  end
end
