defmodule FavnStorageSqlite.Migrations.AddRunPipelineQueryColumn do
  @moduledoc false

  use Ecto.Migration

  def change do
    alter table(:favn_runs) do
      add(:pipeline_submit_ref_text, :text)
    end

    create(index(:favn_runs, [:manifest_version_id, :pipeline_submit_ref_text, :updated_seq]))
  end
end
