defmodule FavnStoragePostgres.Maintenance.OperationRetention do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Repo

  @eligibility_sql """
  SELECT NOT EXISTS (SELECT 1 FROM favn_control.asset_target_generations WHERE workspace_id=$1 AND creating_rebuild_operation_id=$2)
    AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_plan_actions WHERE workspace_id=$1 AND child_operation_id=$2)
    AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks t WHERE t.workspace_id=$1 AND t.operation_id=$2 AND
      (t.run_id IS NOT NULL OR t.terminal_at IS NULL OR t.terminal_at >= $3 OR t.status NOT IN ('succeeded','failed','cancelled')
       OR EXISTS (SELECT 1 FROM favn_control.runner_task_command_tasks c WHERE c.workspace_id=t.workspace_id AND c.task_id=t.task_id)
       OR EXISTS (SELECT 1 FROM favn_control.materialization_claims c WHERE c.workspace_id=t.workspace_id AND c.effect_task_id=t.task_id)
       OR EXISTS (SELECT 1 FROM favn_control.target_operation_locks c WHERE c.workspace_id=t.workspace_id AND c.effect_task_id=t.task_id)))
    AND NOT EXISTS (SELECT 1 FROM favn_control.materialization_claims WHERE workspace_id=$1 AND operation_id=$2)
    AND NOT EXISTS (SELECT 1 FROM favn_control.target_operation_locks WHERE workspace_id=$1 AND operation_id=$2)
    AND NOT EXISTS (SELECT 1 FROM favn_control.auth_operator_commands WHERE workspace_id=$1 AND (resource_id=$2 OR result_resource_id=$2))
    AND NOT EXISTS (SELECT 1 FROM favn_control.idempotency_records WHERE workspace_id=$1 AND resource_id=$2)
    AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_plan_actions WHERE workspace_id=$1 AND operation_id=$2 AND
      (status NOT IN ('planned','succeeded','failed','cancelled','skipped') OR child_run_id IS NOT NULL OR child_operation_id IS NOT NULL OR candidate_generation_id IS NOT NULL OR cleanup_state NOT IN ('complete','not_started')))
    AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_windows WHERE workspace_id=$1 AND operation_id=$2 AND
      (status NOT IN ('planned','ready','succeeded','failed','cancelled','skipped') OR child_run_id IS NOT NULL OR materialization_id IS NOT NULL OR candidate_generation_id IS NOT NULL))
    AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_operations WHERE workspace_id=$1 AND operation_id=$2 AND
      (candidate_generation_id IS NOT NULL OR active_generation_id IS NOT NULL OR evaluated_at >= $3))
  """

  def check!(workspace, id) do
    case SQL.query!(
           Repo,
           "SELECT retiring FROM favn_control.rebuild_operations WHERE workspace_id=$1 AND operation_id=$2",
           [workspace, id]
         ).rows do
      rows ->
        if Enum.any?(rows, &(&1 == [true])),
          do: Repo.rollback(Error.new(:expired, "operation history is retiring")),
          else: :ok
    end
  end

  def guard!(workspace, id), do: guard_operation!(workspace, id, true)
  def guard_if_present!(workspace, id), do: guard_operation!(workspace, id, false)

  defp guard_operation!(workspace, id, required?) do
    %{rows: rebuilds} =
      SQL.query!(
        Repo,
        "SELECT retiring FROM favn_control.rebuild_operations WHERE workspace_id=$1 AND operation_id=$2 FOR SHARE NOWAIT",
        [workspace, id]
      )

    %{rows: recoveries} =
      SQL.query!(
        Repo,
        "SELECT false FROM favn_control.target_recovery_operations WHERE workspace_id=$1 AND operation_id=$2 FOR SHARE NOWAIT",
        [workspace, id]
      )

    case rebuilds ++ recoveries do
      [] ->
        if required?,
          do: Repo.rollback(Error.new(:not_found, "operation history not found")),
          else: :ok

      rows ->
        if Enum.any?(rows, &(&1 == [true])),
          do: Repo.rollback(Error.new(:expired, "operation history is retiring")),
          else: :ok
    end
  end

  def delete!(policy, cutoff, cursor) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT o.workspace_id,o.operation_id,o.retiring FROM favn_control.rebuild_operations o
        WHERE o.state IN ('succeeded','failed','cancelled') AND (o.retiring OR COALESCE(o.completed_at,o.cancelled_at)<$1)
          AND o.cleanup_state IN ('complete','not_started') AND o.unknown_outcome IS NULL
          AND NOT (o.workspace_id=ANY($2::text[]))
          AND (o.workspace_id,o.operation_id)>($3,$4)
          AND ($5::text IS NULL OR (o.workspace_id=$5 AND o.operation_id=$6))
        ORDER BY o.workspace_id,o.operation_id LIMIT 1 FOR UPDATE SKIP LOCKED
        """,
        [
          cutoff,
          policy.excluded_workspace_ids,
          (cursor || %{})["after_workspace"] || "",
          (cursor || %{})["after_id"] || "",
          (cursor || %{})["workspace_id"],
          (cursor || %{})["id"]
        ]
      )

    case rows do
      [] ->
        %{deleted_count: 0, cursor: nil}

      [[workspace, id, retiring]] ->
        if retiring or eligible?(workspace, id, cutoff) do
          SQL.query!(
            Repo,
            "UPDATE favn_control.rebuild_operations SET retiring=true WHERE workspace_id=$1 AND operation_id=$2",
            [workspace, id]
          )

          phase = (cursor || %{})["phase"] || 0

          {count, done?} = phase!(workspace, id, phase, policy.row_limit)

          %{
            deleted_count: count,
            cursor:
              if(done?,
                do: %{"after_workspace" => workspace, "after_id" => id},
                else: %{
                  "workspace_id" => workspace,
                  "id" => id,
                  "phase" => rem(phase + 1, 7)
                }
              )
          }
        else
          %{deleted_count: 0, cursor: %{"after_workspace" => workspace, "after_id" => id}}
        end
    end
  end

  def preview!(policy, cutoff) do
    eligibility =
      @eligibility_sql
      |> String.replace("$1", "candidate.workspace_id")
      |> String.replace("$2", "candidate.operation_id")
      |> String.replace("$3", "$1")

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS MATERIALIZED (
          SELECT item.workspace_id,item.operation_id,item.retiring,COALESCE(item.completed_at,item.cancelled_at) AS settled_at FROM favn_control.rebuild_operations item
          WHERE (item.retiring OR (item.state IN ('succeeded','failed','cancelled') AND item.cleanup_state IN ('complete','not_started') AND item.unknown_outcome IS NULL AND COALESCE(item.completed_at,item.cancelled_at)<$1)) AND NOT (item.workspace_id=ANY($2::text[]))
          ORDER BY item.workspace_id,item.operation_id LIMIT $3
        )
        SELECT candidate.workspace_id,candidate.operation_id,candidate.settled_at,
          candidate.retiring OR (#{eligibility}) FROM candidates candidate
        ORDER BY candidate.workspace_id,candidate.operation_id
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
      table: "rebuild_operations",
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

  defp eligible?(workspace, id, cutoff) do
    %{rows: [[eligible]]} =
      SQL.query!(
        Repo,
        @eligibility_sql,
        [workspace, id, cutoff]
      )

    eligible
  end

  defp phase!(workspace, id, phase, limit) when phase in 0..5 do
    table =
      Enum.at(
        ~w(runner_task_log_batches runner_task_outcomes runner_task_runtime_input_errors runner_tasks rebuild_windows rebuild_plan_actions),
        phase
      )

    owner =
      if phase < 3,
        do:
          "EXISTS (SELECT 1 FROM favn_control.runner_tasks t WHERE t.workspace_id=item.workspace_id AND t.task_id=item.task_id AND t.operation_id=$2)",
        else: "item.operation_id=$2"

    safe =
      case phase do
        3 ->
          "AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_log_batches c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id) AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_outcomes c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id) AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_runtime_input_errors c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id)"

        5 ->
          "AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_windows w WHERE w.workspace_id=item.workspace_id AND w.operation_id=item.operation_id AND w.target_id=item.target_id)"

        _ ->
          ""
      end

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (SELECT item.ctid FROM favn_control.#{table} item WHERE item.workspace_id=$1 AND #{owner} #{safe} LIMIT $3 FOR UPDATE)
        DELETE FROM favn_control.#{table} item USING candidates WHERE item.ctid=candidates.ctid
        """,
        [workspace, id, limit]
      )

    {count, false}
  end

  defp phase!(workspace, id, 6, _limit) do
    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        DELETE FROM favn_control.rebuild_operations WHERE workspace_id=$1 AND operation_id=$2
          AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks WHERE workspace_id=$1 AND operation_id=$2)
          AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_plan_actions WHERE workspace_id=$1 AND operation_id=$2)
          AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_windows WHERE workspace_id=$1 AND operation_id=$2)
        """,
        [workspace, id]
      )

    {count, count == 1}
  end
end
