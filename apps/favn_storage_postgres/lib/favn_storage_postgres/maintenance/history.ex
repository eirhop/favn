defmodule FavnStoragePostgres.Maintenance.History do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Maintenance.Replay
  alias FavnStoragePostgres.Repo

  @children ~w(backfill_events backfill_windows backfill_plan_batches backfill_overviews backfills runner_task_log_batches runner_task_outcomes runner_task_runtime_input_errors runner_tasks resource_circuit_outcomes runtime_input_pins run_execution_checkpoints run_ownerships run_targets run_plans asset_attempt_overviews execution_group_overviews run_submissions)

  @eligibility_sql """
  WITH members AS MATERIALIZED (
    SELECT * FROM favn_control.runs WHERE workspace_id=$1 AND root_execution_group_id=$2
  )
  SELECT NOT EXISTS (SELECT 1 FROM members WHERE terminal_at IS NULL OR terminal_at >= $3)
    AND NOT EXISTS (SELECT 1 FROM members WHERE snapshot #>> '{metadata,failure_cleanup,state}' IN ('pending','attention'))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.materializations x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.materialization_claims x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.runner_tasks t USING(workspace_id,run_id)
      JOIN favn_control.materialization_claims x ON x.workspace_id=t.workspace_id AND x.effect_task_id=t.task_id)
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.log_entries x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.run_ownerships x USING(workspace_id,run_id) WHERE x.owner_id IS NOT NULL AND (x.expires_at >= $3 OR x.updated_at >= $3))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.execution_leases x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.admission_waiters x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.backfills x ON x.workspace_id=m.workspace_id AND x.root_run_id=m.run_id
      WHERE x.status NOT IN ('completed','failed','cancelled') OR x.updated_at >= $3
        OR EXISTS (SELECT 1 FROM favn_control.auth_operator_commands c WHERE c.workspace_id=x.workspace_id AND (c.resource_id=x.backfill_id OR c.result_resource_id=x.backfill_id))
        OR EXISTS (SELECT 1 FROM favn_control.idempotency_records c WHERE c.workspace_id=x.workspace_id AND c.resource_id=x.backfill_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.backfills b ON b.workspace_id=m.workspace_id AND b.root_run_id=m.run_id
      JOIN favn_control.backfill_windows w ON w.workspace_id=b.workspace_id AND w.backfill_id=b.backfill_id
      WHERE w.status NOT IN ('succeeded','failed','cancelled') OR (w.run_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM members child WHERE child.run_id=w.run_id)))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.backfills b ON b.workspace_id=m.workspace_id AND b.root_run_id=m.run_id
      JOIN favn_control.outbox_events e ON e.workspace_id=b.workspace_id AND e.event_kind LIKE 'backfill.%'
        AND (e.payload->>'backfill_id'=b.backfill_id OR (e.aggregate_kind='backfill' AND e.aggregate_id=b.backfill_id))
      WHERE e.publication_id IS NULL OR e.published_at >= $3 OR EXISTS (SELECT 1 FROM favn_control.projection_cursors c WHERE c.last_publication_id<e.publication_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.backfill_windows x USING(workspace_id,run_id)
      JOIN favn_control.backfills b USING(workspace_id,backfill_id) WHERE NOT EXISTS (SELECT 1 FROM members owner WHERE owner.run_id=b.root_run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.rebuild_plan_actions x ON x.workspace_id=m.workspace_id AND x.child_run_id=m.run_id)
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.rebuild_windows x ON x.workspace_id=m.workspace_id AND x.child_run_id=m.run_id)
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.target_statuses x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.asset_window_states x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.runner_tasks t USING(workspace_id,run_id)
      JOIN favn_control.target_operation_locks x ON x.workspace_id=t.workspace_id AND x.effect_task_id=t.task_id)
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.schedule_occurrences x USING(workspace_id,run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.resource_recovery_candidates x ON x.workspace_id=m.workspace_id AND (x.source_run_id=m.run_id OR x.recovery_run_id=m.run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.runs x ON x.workspace_id=m.workspace_id
      AND x.root_execution_group_id<>$2 AND (x.parent_run_id=m.run_id OR x.rerun_of_run_id=m.run_id OR x.cancellation_owner_run_id=m.run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.run_submissions x ON x.workspace_id=m.workspace_id AND (x.run_id=m.run_id OR x.cancellation_owner_run_id=m.run_id)
  WHERE x.terminal_at IS NULL OR x.terminal_at >= $3 OR x.status IN ('queued','preparing','admitting')
    OR x.retry_of_submission_id IS NOT NULL OR x.superseded_by_submission_id IS NOT NULL
    OR EXISTS (SELECT 1 FROM favn_control.run_submission_commands c WHERE c.workspace_id=x.workspace_id AND (c.submission_id=x.submission_id OR (c.result->'submission_ids') ? x.submission_id))
    OR EXISTS (SELECT 1 FROM favn_control.run_submissions retry WHERE retry.workspace_id=x.workspace_id
      AND (retry.retry_of_submission_id=x.submission_id OR retry.retry_root_id=x.submission_id)
      AND NOT EXISTS (SELECT 1 FROM members r WHERE r.run_id=retry.run_id OR r.run_id=retry.cancellation_owner_run_id)))
  AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.idempotency_records i ON i.workspace_id=m.workspace_id AND i.resource_id=m.run_id)
  AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.auth_operator_commands i ON i.workspace_id=m.workspace_id AND (i.resource_id=m.run_id OR i.result_resource_id=m.run_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.runner_tasks x USING(workspace_id,run_id)
      WHERE x.terminal_at IS NULL OR x.terminal_at >= $3 OR x.operation_id IS NOT NULL OR x.status NOT IN ('succeeded','failed','cancelled'))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.runner_tasks t USING(workspace_id,run_id)
      JOIN favn_control.runner_task_command_tasks x USING(workspace_id,task_id))
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.resource_circuit_outcomes x USING(workspace_id,run_id) WHERE x.inserted_at >= $3)
    AND NOT EXISTS (SELECT 1 FROM members m JOIN favn_control.run_events e USING(workspace_id,run_id)
      JOIN favn_control.outbox_events o USING(workspace_id,outbox_event_id)
      WHERE o.publication_id IS NULL OR o.published_at >= $3 OR
        EXISTS (SELECT 1 FROM favn_control.projection_cursors c WHERE c.last_publication_id < o.publication_id))
  """

  def guard!(workspace, run_id, repo \\ Repo) do
    %{rows: roots} =
      SQL.query!(
        repo,
        "SELECT root_execution_group_id FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2",
        [workspace, run_id]
      )

    Enum.each(roots, fn [root] ->
      %{rows: [[locked]]} =
        SQL.query!(
          repo,
          "SELECT pg_try_advisory_xact_lock_shared(hashtextextended(jsonb_build_array('execution_history',$1::text,$2::text)::text,0))",
          [workspace, root]
        )

      unless locked,
        do:
          repo.rollback(
            Error.new(:conflict, "execution history owner is busy",
              retryable?: true,
              details: %{reason_code: "execution_history_owner_busy"}
            )
          )
    end)

    check!(workspace, run_id, repo)
  end

  @spec try_guard!(String.t(), String.t()) :: boolean()
  def try_guard!(workspace, run_id) do
    %{rows: roots} =
      SQL.query!(
        Repo,
        "SELECT root_execution_group_id FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2",
        [workspace, run_id]
      )

    locked? =
      Enum.all?(roots, fn [root] ->
        %{rows: [[locked]]} =
          SQL.query!(
            Repo,
            "SELECT pg_try_advisory_xact_lock_shared(hashtextextended(jsonb_build_array('execution_history',$1::text,$2::text)::text,0))",
            [workspace, root]
          )

        locked
      end)

    if locked?, do: check!(workspace, run_id)
    locked?
  end

  def check!(workspace, run_id, repo \\ Repo) do
    %{rows: rows} =
      SQL.query!(
        repo,
        """
        SELECT root.retiring FROM favn_control.runs member
        JOIN favn_control.runs root ON root.workspace_id=member.workspace_id
          AND root.run_id=member.root_execution_group_id
        WHERE member.workspace_id=$1 AND member.run_id=$2
        """,
        [workspace, run_id]
      )

    if rows == [[true]], do: repo.rollback(Error.new(:expired, "execution history is retiring"))
    :ok
  end

  def readable!(workspace, run_id) do
    %{rows: rows} =
      SQL.query!(Repo, "SELECT 1 FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2", [
        workspace,
        run_id
      ])

    if rows == [], do: Repo.rollback(Error.new(:not_found, "run not found"))
    check!(workspace, run_id)
  end

  def delete!(policy, cutoff, cursor) do
    case select_owner!(policy, cutoff, cursor) do
      nil ->
        %{deleted_count: 0, cursor: nil}

      [workspace, root] ->
        if workspace in policy.excluded_workspace_ids do
          %{deleted_count: 0, cursor: cursor}
        else
          lock!(workspace, root)

          %{rows: [[retiring]]} =
            SQL.query!(
              Repo,
              "SELECT retiring FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2 FOR UPDATE",
              [workspace, root]
            )

          if retiring or eligible?(workspace, root, cutoff) do
            SQL.query!(
              Repo,
              "UPDATE favn_control.runs SET retiring=true WHERE workspace_id=$1 AND run_id=$2",
              [workspace, root]
            )

            phase = (cursor || %{})["phase"] || 0
            {count, finished?} = phase!(workspace, root, phase, policy.row_limit)

            next =
              if finished?,
                do: %{"after_workspace" => workspace, "after_root" => root},
                else: %{
                  "workspace_id" => workspace,
                  "root" => root,
                  "phase" => rem(phase + 1, length(children()) + 2)
                }

            %{deleted_count: count, cursor: next}
          else
            %{deleted_count: 0, cursor: %{"after_workspace" => workspace, "after_root" => root}}
          end
        end
    end
  end

  def preview!(policy, cutoff) do
    eligibility =
      @eligibility_sql
      |> String.replace("$1", "candidate.workspace_id")
      |> String.replace("$2", "candidate.run_id")
      |> String.replace("$3", "$1")

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS MATERIALIZED (
          SELECT item.workspace_id,item.run_id,item.retiring,item.terminal_at AS settled_at FROM favn_control.runs item
          WHERE item.run_id=item.root_execution_group_id AND (item.retiring OR item.terminal_at<$1) AND NOT (item.workspace_id=ANY($2::text[]))
          ORDER BY item.workspace_id,item.run_id LIMIT $3
        )
        SELECT candidate.workspace_id,candidate.run_id,candidate.settled_at,
          candidate.retiring OR (#{eligibility}) FROM candidates candidate
        ORDER BY candidate.workspace_id,candidate.run_id
        """,
        [cutoff, policy.excluded_workspace_ids, policy.scan_limit + 1]
      )

    selected = Enum.take(rows, policy.scan_limit)
    eligible = Enum.filter(selected, &List.last/1)

    oldest =
      eligible
      |> Enum.map(&Enum.at(&1, 2))
      |> Enum.reject(&is_nil/1)
      |> Enum.min(DateTime, fn -> nil end)

    %{
      table: "runs",
      unit: :owners,
      count: length(eligible),
      eligible_count: length(eligible),
      protected_count: length(selected) - length(eligible),
      protection_reason: :referenced_or_unsettled,
      oldest_eligible_settlement: oldest,
      complete?: length(rows) <= policy.scan_limit,
      cutoff: cutoff
    }
  end

  defp select_owner!(_policy, _cutoff, %{"root" => root, "workspace_id" => workspace}) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        "SELECT workspace_id,run_id FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2",
        [workspace, root]
      )

    List.first(rows)
  end

  defp select_owner!(policy, cutoff, cursor),
    do: candidates(policy, cutoff, cursor, 1).rows |> List.first()

  defp candidates(policy, cutoff, cursor, limit) do
    SQL.query!(
      Repo,
      """
      SELECT workspace_id,run_id FROM favn_control.runs
      WHERE run_id=root_execution_group_id AND (retiring OR terminal_at < $1)
        AND NOT (workspace_id=ANY($2::text[]))
        AND (workspace_id,run_id) > ($3,$4)
      ORDER BY workspace_id,run_id LIMIT $5
      """,
      [
        cutoff,
        policy.excluded_workspace_ids,
        (cursor || %{})["after_workspace"] || "",
        (cursor || %{})["after_root"] || "",
        limit
      ]
    )
  end

  defp eligible?(workspace, root, cutoff) do
    %{rows: [[eligible]]} =
      SQL.query!(
        Repo,
        @eligibility_sql,
        [workspace, root, cutoff]
      )

    eligible
  end

  defp children, do: @children

  defp phase!(workspace, root, 0, limit) do
    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (
          SELECT e.outbox_event_id FROM favn_control.outbox_events e
          WHERE e.workspace_id=$1 AND e.event_kind LIKE 'backfill.%'
            AND EXISTS (SELECT 1 FROM favn_control.backfills b JOIN favn_control.runs r ON r.workspace_id=b.workspace_id AND r.run_id=b.root_run_id
              WHERE b.workspace_id=e.workspace_id AND r.root_execution_group_id=$2
                AND (e.payload->>'backfill_id'=b.backfill_id OR (e.aggregate_kind='backfill' AND e.aggregate_id=b.backfill_id)))
          ORDER BY e.publication_id LIMIT $3 FOR UPDATE OF e)
        DELETE FROM favn_control.outbox_events e USING candidates WHERE e.outbox_event_id=candidates.outbox_event_id
        """,
        [workspace, root, limit]
      )

    {count, false}
  end

  defp phase!(workspace, root, phase, limit) when phase < length(@children) do
    table = Enum.at(children(), phase)

    membership =
      case table do
        "run_submissions" ->
          "EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND (r.run_id=item.run_id OR r.run_id=item.cancellation_owner_run_id) AND r.root_execution_group_id=$2)"

        "backfills" ->
          "EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.root_run_id AND r.root_execution_group_id=$2)"

        t when t in ~w(backfill_windows backfill_plan_batches backfill_overviews) ->
          "EXISTS (SELECT 1 FROM favn_control.backfills b JOIN favn_control.runs r ON r.workspace_id=b.workspace_id AND r.run_id=b.root_run_id WHERE b.workspace_id=item.workspace_id AND b.backfill_id=item.backfill_id AND r.root_execution_group_id=$2)"

        t
        when t in ~w(runner_task_log_batches runner_task_outcomes runner_task_runtime_input_errors) ->
          "EXISTS (SELECT 1 FROM favn_control.runner_tasks t JOIN favn_control.runs r USING(workspace_id,run_id) WHERE t.workspace_id=item.workspace_id AND t.task_id=item.task_id AND r.root_execution_group_id=$2)"

        "execution_group_overviews" ->
          "item.root_run_id=$2"

        _ ->
          "EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id AND r.root_execution_group_id=$2)"
      end

    childless =
      if table == "runner_tasks",
        do:
          "AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_log_batches c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id) AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_outcomes c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id) AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_runtime_input_errors c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id)",
        else: ""

    childless =
      case table do
        "backfill_plan_batches" ->
          "AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_windows w WHERE w.workspace_id=item.workspace_id AND w.backfill_id=item.backfill_id AND w.batch_index=item.batch_index)"

        "backfills" ->
          "AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_windows w WHERE w.workspace_id=item.workspace_id AND w.backfill_id=item.backfill_id) AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_plan_batches w WHERE w.workspace_id=item.workspace_id AND w.backfill_id=item.backfill_id) AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_overviews w WHERE w.workspace_id=item.workspace_id AND w.backfill_id=item.backfill_id) AND NOT EXISTS (SELECT 1 FROM favn_control.outbox_events e WHERE e.workspace_id=item.workspace_id AND e.event_kind LIKE 'backfill.%' AND (e.payload->>'backfill_id'=item.backfill_id OR (e.aggregate_kind='backfill' AND e.aggregate_id=item.backfill_id)))"

        _ ->
          childless
      end

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (SELECT item.ctid FROM favn_control.#{table} item
          WHERE item.workspace_id=$1 AND #{membership} #{childless}
          LIMIT $3 FOR UPDATE OF item SKIP LOCKED)
        DELETE FROM favn_control.#{table} item USING candidates WHERE item.ctid=candidates.ctid
        """,
        [workspace, root, limit]
      )

    {count, false}
  end

  defp phase!(workspace, root, phase, limit) when phase == length(@children) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT e.event_id,o.outbox_event_id,o.publication_id FROM favn_control.run_events e
        JOIN favn_control.runs r USING(workspace_id,run_id)
        JOIN favn_control.outbox_events o USING(workspace_id,outbox_event_id)
        WHERE e.workspace_id=$1 AND r.root_execution_group_id=$2
          AND e.event_id<>r.submitted_event_id AND e.event_id<>r.latest_event_id
        ORDER BY o.publication_id LIMIT $3 FOR UPDATE OF e
        """,
        [workspace, root, div(limit, 2)]
      )

    Enum.each(rows, fn [event, outbox, publication] ->
      SQL.query!(Repo, "DELETE FROM favn_control.run_events WHERE event_id=$1", [event])

      SQL.query!(Repo, "DELETE FROM favn_control.outbox_events WHERE outbox_event_id=$1", [outbox])

      Replay.advance!(workspace, "events", publication, 0)
    end)

    {length(rows) * 2, false}
  end

  defp phase!(_workspace, _root, phase, limit) when phase == length(@children) + 1 and limit < 5,
    do: {0, false}

  defp phase!(workspace, root, phase, _limit) when phase == length(@children) + 1 do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT r.run_id FROM favn_control.runs r
        WHERE r.workspace_id=$1 AND r.root_execution_group_id=$2
          AND NOT EXISTS (SELECT 1 FROM favn_control.runs child WHERE child.workspace_id=r.workspace_id
            AND child.run_id<>r.run_id AND (child.parent_run_id=r.run_id OR child.rerun_of_run_id=r.run_id OR child.cancellation_owner_run_id=r.run_id OR child.root_execution_group_id=r.run_id))
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_events e WHERE e.workspace_id=r.workspace_id AND e.run_id=r.run_id
            AND e.event_id<>r.submitted_event_id AND e.event_id<>r.latest_event_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_targets t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_plans t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.runtime_input_pins t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_ownerships t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_execution_checkpoints t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.materialization_claims t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions t WHERE t.workspace_id=r.workspace_id AND (t.run_id=r.run_id OR t.cancellation_owner_run_id=r.run_id))
          AND NOT EXISTS (SELECT 1 FROM favn_control.asset_attempt_overviews t WHERE t.workspace_id=r.workspace_id AND t.run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.execution_group_overviews t WHERE t.workspace_id=r.workspace_id AND t.root_run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.backfills b WHERE b.workspace_id=r.workspace_id AND b.root_run_id=r.run_id)
          AND NOT EXISTS (SELECT 1 FROM favn_control.backfill_windows b WHERE b.workspace_id=r.workspace_id AND b.run_id=r.run_id)
        ORDER BY r.run_id LIMIT 1
        """,
        [workspace, root]
      )

    case rows do
      [] ->
        {0, false}

      [[run]] ->
        %{rows: events} =
          SQL.query!(
            Repo,
            "SELECT e.event_id,o.outbox_event_id,o.publication_id FROM favn_control.run_events e JOIN favn_control.outbox_events o USING(workspace_id,outbox_event_id) WHERE e.workspace_id=$1 AND e.run_id=$2",
            [workspace, run]
          )

        SQL.query!(Repo, "DELETE FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2", [
          workspace,
          run
        ])

        Enum.each(events, fn [event, outbox, publication] ->
          SQL.query!(Repo, "DELETE FROM favn_control.run_events WHERE event_id=$1", [event])

          SQL.query!(Repo, "DELETE FROM favn_control.outbox_events WHERE outbox_event_id=$1", [
            outbox
          ])

          Replay.advance!(workspace, "events", publication, 0)
        end)

        {1 + 2 * length(events), run == root}
    end
  end

  defp lock!(workspace, root) do
    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array('execution_history',$1::text,$2::text)::text,0))",
      [workspace, root]
    )
  end
end
