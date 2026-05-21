defmodule FavnStorageSqlite.Migrations.AddRunGroupQueryColumns do
  @moduledoc false

  use Ecto.Migration

  def change do
    alter table(:favn_runs) do
      add(:root_execution_group_id, :string)
      add(:parent_run_id, :string)
      add(:root_run_id, :string)
      add(:submit_kind, :string)
      add(:asset_ref_text, :text)
      add(:target_refs_text, :text)
      add(:window_key, :text)
    end

    create(index(:favn_runs, [:root_execution_group_id, :updated_seq]))
    create(index(:favn_runs, [:root_execution_group_id, :run_id]))
  end
end
