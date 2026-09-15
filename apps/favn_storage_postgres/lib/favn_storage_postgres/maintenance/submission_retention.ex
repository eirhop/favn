defmodule FavnStoragePostgres.Maintenance.SubmissionRetention do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunIdentity

  @candidate """
  item.terminal_at<$1 AND NOT (item.workspace_id=ANY($2::text[]))
  AND (item.status='cancelled' OR (item.status='failed' AND item.failure_kind IN ('safe','permanent')))
  AND item.failure_kind IS DISTINCT FROM 'unknown'
  """
  @unreferenced """
  item.retry_of_submission_id IS NULL AND item.superseded_by_submission_id IS NULL
  AND item.retry_root_id=item.submission_id
  AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions r WHERE r.workspace_id=item.workspace_id AND r.submission_id<>item.submission_id
    AND (r.retry_of_submission_id=item.submission_id OR r.retry_root_id=item.submission_id OR r.superseded_by_submission_id=item.submission_id))
  AND NOT EXISTS (SELECT 1 FROM favn_control.run_submission_commands c WHERE c.workspace_id=item.workspace_id
    AND (c.submission_id=item.submission_id OR (c.result->'submission_ids') ? item.submission_id))
  AND NOT EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id IN (item.run_id,item.cancellation_owner_run_id))
  AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.execution_leases r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.admission_waiters r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.materialization_claims r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.log_entries r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.schedule_occurrences r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.resource_recovery_candidates r WHERE r.workspace_id=item.workspace_id AND (r.source_run_id=item.run_id OR r.recovery_run_id=item.run_id))
  AND NOT EXISTS (SELECT 1 FROM favn_control.auth_operator_commands r WHERE r.workspace_id=item.workspace_id
    AND (r.resource_id IN (item.submission_id,item.run_id,item.cancellation_owner_run_id) OR r.result_resource_id IN (item.submission_id,item.run_id,item.cancellation_owner_run_id)))
  AND NOT EXISTS (SELECT 1 FROM favn_control.idempotency_records r WHERE r.workspace_id=item.workspace_id AND r.resource_id IN (item.submission_id,item.run_id,item.cancellation_owner_run_id))
  """

  def delete!(policy, cutoff, cursor) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT item.workspace_id,item.submission_id,item.run_id,item.cancellation_owner_run_id
        FROM favn_control.run_submissions item WHERE #{@candidate}
          AND (item.workspace_id,item.submission_id)>($3,$4)
        ORDER BY item.workspace_id,item.submission_id LIMIT 1
        """,
        [
          cutoff,
          policy.excluded_workspace_ids,
          (cursor || %{})["workspace_id"] || "",
          (cursor || %{})["id"] || ""
        ]
      )

    case rows do
      [] ->
        %{deleted_count: 0, cursor: nil}

      [[workspace, id, run, owner]] ->
        # Match cancellation/new-run authority before locking the submission row.
        locked? =
          [owner, run]
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> Enum.all?(&RunIdentity.try_lock!(workspace, &1))

        count =
          if locked? do
            %{rows: locked} =
              SQL.query!(
                Repo,
                "SELECT submission_id FROM favn_control.run_submissions WHERE workspace_id=$1 AND submission_id=$2 FOR UPDATE SKIP LOCKED",
                [workspace, id]
              )

            if locked == [] do
              0
            else
              # Recheck references after the row lock in a new READ COMMITTED snapshot.
              SQL.query!(
                Repo,
                "DELETE FROM favn_control.run_submissions item WHERE item.workspace_id=$3 AND item.submission_id=$4 AND #{@candidate} AND #{@unreferenced}",
                [cutoff, policy.excluded_workspace_ids, workspace, id]
              ).num_rows
            end
          else
            0
          end

        %{deleted_count: count, cursor: %{"workspace_id" => workspace, "id" => id}}
    end
  end

  def preview!(policy, cutoff) do
    %{rows: [[count]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*) FROM (SELECT 1 FROM favn_control.run_submissions item
          WHERE #{@candidate} AND #{@unreferenced} LIMIT $3) bounded
        """,
        [cutoff, policy.excluded_workspace_ids, policy.scan_limit + 1]
      )

    %{
      table: "standalone_run_submissions",
      unit: :owners,
      eligible_count: min(count, policy.scan_limit),
      complete?: count <= policy.scan_limit
    }
  end
end
