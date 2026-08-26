defmodule FavnStoragePostgres.Migrations.AddBackfillWindowRunLookupV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  # The run header read resolves a run's own window on every refresh, and the
  # window-run rebind checks for dangling references, both by (workspace_id,
  # run_id). No existing index leads with that pair, so both scanned a table
  # that grows with every backfill ever run. The partial predicate keeps the
  # index to windows that have actually started a run.
  def up do
    create(
      index(:backfill_windows, [:workspace_id, :run_id],
        prefix: @prefix,
        name: :backfill_windows_run_idx,
        where: "run_id IS NOT NULL"
      )
    )
  end

  def down do
    drop(index(:backfill_windows, [], prefix: @prefix, name: :backfill_windows_run_idx))
  end
end
