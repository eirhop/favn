defmodule FavnStoragePostgres.Maintenance.LogRetention do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Maintenance.Replay
  alias FavnStoragePostgres.Maintenance.Retention
  alias FavnStoragePostgres.Repo

  def delete!(policy, requested_cutoff, _cursor, workspace_id) do
    cutoff = min_cutoff(requested_cutoff)

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS MATERIALIZED (
          SELECT entry.log_id, entry.workspace_id, entry.position, event.publication_id
          FROM favn_control.log_entries entry
          JOIN favn_control.log_batches batch USING (workspace_id, batch_id)
          JOIN favn_control.outbox_events event ON event.outbox_event_id = batch.outbox_event_id
            AND event.workspace_id = batch.workspace_id
          WHERE entry.inserted_at < $1 AND batch.inserted_at < $1 AND event.published_at < $1
            AND NOT (entry.workspace_id = ANY($2::text[]))
        AND ($4::text IS NULL OR entry.workspace_id=$4)
            AND NOT EXISTS (SELECT 1 FROM favn_control.runs run
              WHERE run.workspace_id = entry.workspace_id AND run.run_id = entry.run_id
                AND run.status NOT IN ('ok','partial','error','cancelled','timed_out'))
          ORDER BY event.publication_id, entry.position LIMIT $3 FOR UPDATE OF entry SKIP LOCKED
        )
        DELETE FROM favn_control.log_entries entry USING candidates
        WHERE entry.log_id = candidates.log_id
        RETURNING candidates.workspace_id, candidates.publication_id, candidates.position
        """,
        [cutoff, policy.excluded_workspace_ids, policy.row_limit, workspace_id]
      )

    rows
    |> Enum.group_by(&hd/1)
    |> Enum.each(fn {workspace, deleted} ->
      [_, publication, offset] = Enum.max_by(deleted, fn [_, id, position] -> {id, position} end)
      Replay.advance!(workspace, "logs", publication, offset)
    end)

    remaining = policy.row_limit - length(rows)

    count =
      if remaining >= 2,
        do: empty_batches!(policy, cutoff, div(remaining, 2), workspace_id),
        else: 0

    %{deleted_count: length(rows) + count, cursor: nil}
  end

  def preview!(policy, cutoff) do
    %{rows: [[count]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*) FROM (
          SELECT 1 FROM favn_control.log_entries entry
          JOIN favn_control.log_batches batch USING (workspace_id,batch_id)
          JOIN favn_control.outbox_events event ON event.workspace_id = batch.workspace_id
            AND event.outbox_event_id = batch.outbox_event_id
          WHERE entry.inserted_at < $1 AND batch.inserted_at < $1 AND event.published_at < $1
            AND NOT (entry.workspace_id = ANY($2::text[]))
            AND NOT EXISTS (SELECT 1 FROM favn_control.runs run
              WHERE run.workspace_id = entry.workspace_id AND run.run_id = entry.run_id
                AND run.status NOT IN ('ok','partial','error','cancelled','timed_out'))
          LIMIT $3
        ) candidates
        """,
        [min_cutoff(cutoff), policy.excluded_workspace_ids, policy.scan_limit + 1]
      )

    %{
      family: :logs,
      eligible_count: min(count, policy.scan_limit),
      complete?: count <= policy.scan_limit,
      cutoff: cutoff
    }
  end

  defp empty_batches!(policy, cutoff, limit, workspace_id) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT batch.workspace_id,batch.batch_id,batch.outbox_event_id
        FROM favn_control.log_batches batch
        JOIN favn_control.outbox_events event USING (workspace_id,outbox_event_id)
        WHERE batch.inserted_at < $1 AND event.published_at < $1
          AND NOT (batch.workspace_id = ANY($2::text[]))
        AND ($4::text IS NULL OR batch.workspace_id=$4)
          AND NOT EXISTS (SELECT 1 FROM favn_control.log_entries entry
            WHERE entry.workspace_id = batch.workspace_id AND entry.batch_id = batch.batch_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.projection_cursors cursor
            WHERE cursor.last_publication_id < event.publication_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.projection_failures failure
            WHERE failure.publication_id = event.publication_id)
        ORDER BY event.publication_id LIMIT $3 FOR UPDATE OF batch SKIP LOCKED
        """,
        [cutoff, policy.excluded_workspace_ids, limit, workspace_id]
      )

    Enum.each(rows, fn [workspace, batch, event] ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.log_batches WHERE workspace_id=$1 AND batch_id=$2",
        [workspace, batch]
      )

      SQL.query!(
        Repo,
        "DELETE FROM favn_control.outbox_events WHERE workspace_id=$1 AND outbox_event_id=$2",
        [workspace, event]
      )
    end)

    length(rows) * 2
  end

  defp min_cutoff(requested) do
    replay_cutoff = DateTime.add(Retention.now!(), -605_100, :second)
    if DateTime.compare(requested, replay_cutoff) == :lt, do: requested, else: replay_cutoff
  end
end
