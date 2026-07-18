defmodule FavnStoragePostgres.Migrations.AddScheduleOperatorReadsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:schedule_cursors, prefix: @prefix) do
      add(:schedule_fingerprint, :text, null: false, default: "legacy")
      add(:definition, :map, null: false, default: %{})
    end

    create(
      index(
        :schedule_occurrences,
        [
          :workspace_id,
          :deployment_id,
          :pipeline_target_id,
          :schedule_id,
          :due_at,
          :occurrence_id
        ],
        prefix: @prefix,
        name: :schedule_occurrences_history_idx
      )
    )

    create(
      constraint(:schedule_cursors, :schedule_cursors_definition_bounded,
        prefix: @prefix,
        check:
          "octet_length(schedule_fingerprint) BETWEEN 1 AND 255 AND pg_column_size(definition) <= 65536"
      )
    )
  end

  def down do
    drop(constraint(:schedule_cursors, :schedule_cursors_definition_bounded, prefix: @prefix))

    drop(
      index(:schedule_occurrences, [],
        prefix: @prefix,
        name: :schedule_occurrences_history_idx
      )
    )

    alter table(:schedule_cursors, prefix: @prefix) do
      remove(:definition)
      remove(:schedule_fingerprint)
    end
  end
end
