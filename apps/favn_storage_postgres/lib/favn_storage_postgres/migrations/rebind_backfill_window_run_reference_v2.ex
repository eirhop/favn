defmodule FavnStoragePostgres.Migrations.RebindBackfillWindowRunReferenceV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    reject_windows_without_submissions!()

    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
      DROP CONSTRAINT backfill_windows_run_fk,
      ADD CONSTRAINT backfill_windows_run_submission_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES #{@prefix}.run_submissions(workspace_id, run_id)
        ON DELETE RESTRICT
    """)
  end

  def down do
    reject_windows_without_runs!()

    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
      DROP CONSTRAINT backfill_windows_run_submission_fk,
      ADD CONSTRAINT backfill_windows_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES #{@prefix}.runs(workspace_id, run_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    """)
  end

  defp reject_windows_without_submissions! do
    execute("""
    DO $$
    DECLARE
      dangling_count bigint;
    BEGIN
      SELECT count(*)
      INTO dangling_count
      FROM #{@prefix}.backfill_windows window_run
      LEFT JOIN #{@prefix}.run_submissions submission
        ON submission.workspace_id = window_run.workspace_id
       AND submission.run_id = window_run.run_id
      WHERE window_run.run_id IS NOT NULL
        AND submission.run_id IS NULL;

      IF dangling_count > 0 THEN
        RAISE EXCEPTION USING
          ERRCODE = '23503',
          MESSAGE = format(
            'backfill window migration requires durable submissions for %s linked windows',
            dangling_count
          );
      END IF;
    END
    $$
    """)
  end

  defp reject_windows_without_runs! do
    execute("""
    DO $$
    DECLARE
      dangling_count bigint;
    BEGIN
      SELECT count(*)
      INTO dangling_count
      FROM #{@prefix}.backfill_windows window_run
      LEFT JOIN #{@prefix}.runs run
        ON run.workspace_id = window_run.workspace_id
       AND run.run_id = window_run.run_id
      WHERE window_run.run_id IS NOT NULL
        AND run.run_id IS NULL;

      IF dangling_count > 0 THEN
        RAISE EXCEPTION USING
          ERRCODE = '23503',
          MESSAGE = format(
            'backfill window downgrade requires durable runs for %s linked windows',
            dangling_count
          );
      END IF;
    END
    $$
    """)
  end
end
