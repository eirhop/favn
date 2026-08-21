defmodule FavnStoragePostgres.Migrations.RebindScheduleOccurrenceRunReferenceV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    reject_occurrences_without_submissions!()

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
      DROP CONSTRAINT schedule_occurrences_run_fk,
      ADD CONSTRAINT schedule_occurrences_run_submission_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES #{@prefix}.run_submissions(workspace_id, run_id)
        ON DELETE RESTRICT
    """)
  end

  def down do
    reject_occurrences_without_runs!()

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
      DROP CONSTRAINT schedule_occurrences_run_submission_fk,
      ADD CONSTRAINT schedule_occurrences_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES #{@prefix}.runs(workspace_id, run_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    """)
  end

  defp reject_occurrences_without_submissions! do
    execute("""
    DO $$
    DECLARE
      dangling_count bigint;
    BEGIN
      SELECT count(*)
      INTO dangling_count
      FROM #{@prefix}.schedule_occurrences occurrence
      LEFT JOIN #{@prefix}.run_submissions submission
        ON submission.workspace_id = occurrence.workspace_id
       AND submission.run_id = occurrence.run_id
      WHERE occurrence.run_id IS NOT NULL
        AND submission.run_id IS NULL;

      IF dangling_count > 0 THEN
        RAISE EXCEPTION USING
          ERRCODE = '23503',
          MESSAGE = format(
            'schedule occurrence migration requires durable submissions for %s linked occurrences',
            dangling_count
          );
      END IF;
    END
    $$
    """)
  end

  defp reject_occurrences_without_runs! do
    execute("""
    DO $$
    DECLARE
      dangling_count bigint;
    BEGIN
      SELECT count(*)
      INTO dangling_count
      FROM #{@prefix}.schedule_occurrences occurrence
      LEFT JOIN #{@prefix}.runs run
        ON run.workspace_id = occurrence.workspace_id
       AND run.run_id = occurrence.run_id
      WHERE occurrence.run_id IS NOT NULL
        AND run.run_id IS NULL;

      IF dangling_count > 0 THEN
        RAISE EXCEPTION USING
          ERRCODE = '23503',
          MESSAGE = format(
            'schedule occurrence downgrade requires durable runs for %s linked occurrences',
            dangling_count
          );
      END IF;
    END
    $$
    """)
  end
end
