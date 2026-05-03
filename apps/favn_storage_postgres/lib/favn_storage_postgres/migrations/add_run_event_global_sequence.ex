defmodule FavnStoragePostgres.Migrations.AddRunEventGlobalSequence do
  @moduledoc false

  use Ecto.Migration

  def up do
    execute("CREATE SEQUENCE IF NOT EXISTS favn_run_event_global_seq START 1")

    alter table(:favn_run_events) do
      add(:global_sequence, :bigint)
    end

    execute("""
    WITH ordered AS (
      SELECT run_id,
             sequence,
             row_number() OVER (ORDER BY occurred_at ASC NULLS FIRST, run_id ASC, sequence ASC) AS global_sequence
      FROM favn_run_events
    )
    UPDATE favn_run_events AS events
    SET global_sequence = ordered.global_sequence
    FROM ordered
    WHERE events.run_id = ordered.run_id
      AND events.sequence = ordered.sequence
    """)

    create_if_not_exists(unique_index(:favn_run_events, [:global_sequence]))

    execute("""
    SELECT setval(
      'favn_run_event_global_seq',
      GREATEST((SELECT COALESCE(MAX(global_sequence), 1) FROM favn_run_events), 1),
      (SELECT COALESCE(MAX(global_sequence), 0) FROM favn_run_events) > 0
    )
    """)
  end

  def down do
    drop_if_exists(index(:favn_run_events, [:global_sequence]))
    execute("DROP SEQUENCE IF EXISTS favn_run_event_global_seq")
  end
end
