defmodule FavnStorageSqlite.Migrations.AddRunEventType do
  @moduledoc false

  use Ecto.Migration

  def up do
    alter table(:favn_run_events) do
      add(:event_type, :string)
    end

    execute("""
    UPDATE favn_run_events
    SET event_type = json_extract(CAST(event_blob AS TEXT), '$.event_type')
    WHERE event_type IS NULL
    """)

    create_if_not_exists(index(:favn_run_events, [:run_id, :event_type, :global_sequence]))
  end

  def down do
    drop_if_exists(index(:favn_run_events, [:run_id, :event_type, :global_sequence]))

    alter table(:favn_run_events) do
      remove(:event_type)
    end
  end
end
