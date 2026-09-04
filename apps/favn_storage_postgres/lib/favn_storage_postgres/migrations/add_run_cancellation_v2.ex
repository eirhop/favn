defmodule FavnStoragePostgres.Migrations.AddRunCancellationV2 do
  @moduledoc false
  use Ecto.Migration

  alias Ecto.Adapters.SQL

  @prefix "favn_control"

  def up do
    execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM favn_control.runs LIMIT 1)
         OR EXISTS (SELECT 1 FROM favn_control.run_submissions LIMIT 1) THEN
        RAISE EXCEPTION 'run cancellation schema requires an empty coordinated control-plane/data-plane baseline; never reset control-plane state alone';
      END IF;
    END $$
    """)

    for name <- [:runs, :run_submissions] do
      alter table(name, prefix: @prefix) do
        add(:cancellation_owner_run_id, :text, null: false)
      end

      create(
        index(name, [:workspace_id, :cancellation_owner_run_id, :run_id],
          prefix: @prefix,
          name: :"#{name}_cancellation_owner_idx"
        )
      )

      create(
        constraint(name, :"#{name}_cancellation_owner_valid",
          prefix: @prefix,
          check: "octet_length(cancellation_owner_run_id) BETWEEN 1 AND 255"
        )
      )
    end

    alter table(:runs, prefix: @prefix) do
      add(:cancellation_requested_at, :utc_datetime_usec)
      add(:cancellation_status, :text)
    end

    create(
      index(:runs, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :runs_cancelling_idx,
        where: "cancellation_status = 'cancelling'"
      )
    )

    create(
      index(:runs, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :runs_requested_cancellation_idx,
        where: "cancellation_requested_at IS NOT NULL AND status IN ('pending','running')"
      )
    )

    create(
      index(:run_submissions, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :run_submissions_requested_cancellation_idx,
        where:
          "cancellation_requested_at IS NOT NULL AND status IN ('queued','preparing','admitting')"
      )
    )

    create(
      constraint(:runs, :runs_cancellation_status_valid,
        prefix: @prefix,
        check: "cancellation_status IN ('cancelling', 'cancelled', 'needs_attention')"
      )
    )

    alter table(:backfills, prefix: @prefix) do
      add(:cancellation_requested_at, :utc_datetime_usec)
      add(:cancellation_reason, :map)
    end

    create(
      index(:backfills, [:workspace_id, :root_run_id],
        prefix: @prefix,
        name: :backfills_cancelling_idx,
        where: "status = 'cancelling'"
      )
    )

    create(
      index(
        :resource_recovery_candidates,
        [:workspace_id, :source_run_id, :status, :candidate_id],
        prefix: @prefix,
        name: :resource_recovery_candidates_source_idx
      )
    )

    create(
      index(:resource_recovery_candidates, [:workspace_id, :recovery_run_id],
        prefix: @prefix,
        name: :resource_recovery_candidates_run_idx,
        where: "recovery_run_id IS NOT NULL"
      )
    )

    create(
      index(:materializations, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :materializations_run_generation_idx,
        where: "target_generation_id IS NOT NULL"
      )
    )

    flush()

    extend_constraint(
      "backfills",
      "backfills_values_valid",
      "'cancelled'::text",
      "'cancelled'::text, 'cancelling'::text, 'needs_attention'::text"
    )

    extend_constraint(
      "resource_recovery_candidates",
      "resource_recovery_candidates_values_valid",
      "'submitted'::text",
      "'submitted'::text, 'cancelled'::text"
    )
  end

  defp extend_constraint(table, name, old, new) do
    %{rows: [[definition]]} =
      SQL.query!(
        repo(),
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = $1::text::regclass AND conname = $2",
        ["favn_control." <> table, name]
      )

    if not String.contains?(definition, old), do: raise("unexpected #{name} definition")
    execute("ALTER TABLE favn_control.#{table} DROP CONSTRAINT #{name}")

    execute(
      "ALTER TABLE favn_control.#{table} ADD CONSTRAINT #{name} #{String.replace(definition, old, new)}"
    )

    flush()
  end
end
