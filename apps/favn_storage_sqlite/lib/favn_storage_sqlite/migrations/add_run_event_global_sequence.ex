defmodule FavnStorageSqlite.Migrations.AddRunEventGlobalSequence do
  @moduledoc false

  use Ecto.Migration

  def up do
    alter table(:favn_run_events) do
      add(:global_sequence, :integer)
    end

    execute("""
    UPDATE favn_run_events
    SET global_sequence = (
      SELECT COUNT(*)
      FROM favn_run_events AS ordered
      WHERE ordered.occurred_at < favn_run_events.occurred_at
         OR (ordered.occurred_at = favn_run_events.occurred_at AND ordered.run_id < favn_run_events.run_id)
         OR (ordered.occurred_at = favn_run_events.occurred_at AND ordered.run_id = favn_run_events.run_id AND ordered.sequence <= favn_run_events.sequence)
    )
    """)

    create_if_not_exists(unique_index(:favn_run_events, [:global_sequence]))

    execute("""
    INSERT INTO favn_counters (name, value)
    VALUES ('run_event_global_sequence', (SELECT COALESCE(MAX(global_sequence), 0) FROM favn_run_events))
    ON CONFLICT(name) DO UPDATE SET value = excluded.value
    WHERE favn_counters.value < excluded.value
    """)
  end

  def down do
    drop_if_exists(index(:favn_run_events, [:global_sequence]))
  end
end
