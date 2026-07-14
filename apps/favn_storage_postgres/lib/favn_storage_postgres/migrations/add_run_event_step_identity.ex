defmodule FavnStoragePostgres.Migrations.AddRunEventStepIdentity do
  @moduledoc false

  use Ecto.Migration

  def up do
    alter table(:favn_run_events) do
      add(:asset_step_id, :string)
    end

    execute("""
    UPDATE favn_run_events
    SET asset_step_id = convert_from(event_blob, 'UTF8')::jsonb #>> '{data,asset_step_id}'
    WHERE asset_step_id IS NULL
    """)

    create_if_not_exists(index(:favn_run_events, [:run_id, :asset_step_id, :global_sequence]))
  end

  def down do
    drop_if_exists(index(:favn_run_events, [:run_id, :asset_step_id, :global_sequence]))

    alter table(:favn_run_events) do
      remove(:asset_step_id)
    end
  end
end
