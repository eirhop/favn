defmodule FavnStoragePostgres.Maintenance.TaskRetention do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  @eligible """
  item.run_id IS NULL AND item.operation_id IS NULL
  AND item.status IN ('succeeded','failed','cancelled') AND (item.retiring OR item.terminal_at<$1)
  AND NOT (item.workspace_id=ANY($2::text[]))
  AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_command_tasks c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.materialization_claims c WHERE c.workspace_id=item.workspace_id AND c.effect_task_id=item.task_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.target_operation_locks c WHERE c.workspace_id=item.workspace_id AND c.effect_task_id=item.task_id)
  AND NOT EXISTS (SELECT 1 FROM favn_control.auth_operator_commands c WHERE c.workspace_id=item.workspace_id AND (c.resource_id=item.task_id OR c.result_resource_id=item.task_id))
  AND NOT EXISTS (SELECT 1 FROM favn_control.idempotency_records c WHERE c.workspace_id=item.workspace_id AND c.resource_id=item.task_id)
  """
  @children ~w(runner_task_log_batches runner_task_outcomes runner_task_runtime_input_errors)

  def delete!(policy, cutoff, cursor) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidate AS MATERIALIZED (
          SELECT item.* FROM favn_control.runner_tasks item
          WHERE item.run_id IS NULL AND item.operation_id IS NULL AND item.status IN ('succeeded','failed','cancelled')
            AND (item.retiring OR item.terminal_at<$1) AND NOT (item.workspace_id=ANY($2::text[]))
            AND ($3::text IS NULL OR (item.workspace_id=$3 AND item.task_id=$4))
            AND (item.workspace_id,item.task_id)>($5,$6)
          ORDER BY item.workspace_id,item.task_id LIMIT 1 FOR UPDATE OF item SKIP LOCKED
        )
        SELECT item.workspace_id,item.task_id,(#{@eligible}) FROM candidate item
        """,
        [
          cutoff,
          policy.excluded_workspace_ids,
          (cursor || %{})["workspace_id"],
          (cursor || %{})["id"],
          (cursor || %{})["after_workspace"] || "",
          (cursor || %{})["after_id"] || ""
        ]
      )

    case rows do
      [] ->
        %{deleted_count: 0, cursor: nil}

      [[workspace, id, false]] ->
        %{deleted_count: 0, cursor: %{"after_workspace" => workspace, "after_id" => id}}

      [[workspace, id, true]] ->
        SQL.query!(
          Repo,
          "UPDATE favn_control.runner_tasks SET retiring=true WHERE workspace_id=$1 AND task_id=$2",
          [workspace, id]
        )

        phase = (cursor || %{})["phase"] || 0
        table = Enum.at(@children, phase, "runner_tasks")

        childless =
          if phase == 3,
            do:
              Enum.map_join(@children, " AND ", fn child ->
                "NOT EXISTS (SELECT 1 FROM favn_control.#{child} c WHERE c.workspace_id=item.workspace_id AND c.task_id=item.task_id)"
              end),
            else: "true"

        %{num_rows: count} =
          SQL.query!(
            Repo,
            """
            WITH candidates AS (SELECT item.ctid FROM favn_control.#{table} item
              WHERE item.workspace_id=$1 AND item.task_id=$2 AND #{childless} LIMIT $3 FOR UPDATE)
            DELETE FROM favn_control.#{table} item USING candidates WHERE item.ctid=candidates.ctid
            """,
            [workspace, id, policy.row_limit]
          )

        %{
          deleted_count: count,
          cursor:
            if(phase == 3 and count == 1,
              do: %{"after_workspace" => workspace, "after_id" => id},
              else: %{"workspace_id" => workspace, "id" => id, "phase" => rem(phase + 1, 4)}
            )
        }
    end
  end

  def preview!(policy, cutoff) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS MATERIALIZED (
          SELECT item.* FROM favn_control.runner_tasks item
          WHERE item.run_id IS NULL AND item.operation_id IS NULL AND item.status IN ('succeeded','failed','cancelled')
            AND (item.retiring OR item.terminal_at<$1) AND NOT (item.workspace_id=ANY($2::text[]))
          ORDER BY item.workspace_id,item.task_id LIMIT $3
        )
        SELECT (#{@eligible}) FROM candidates item
        """,
        [cutoff, policy.excluded_workspace_ids, policy.scan_limit + 1]
      )

    selected = Enum.take(rows, policy.scan_limit)
    count = Enum.count(selected, &(&1 == [true]))

    %{
      table: "standalone_runner_tasks",
      unit: :owners,
      eligible_count: count,
      protected_count: length(selected) - count,
      complete?: length(rows) <= policy.scan_limit
    }
  end
end
