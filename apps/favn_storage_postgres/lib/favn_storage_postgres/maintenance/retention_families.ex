defmodule FavnStoragePostgres.Maintenance.RetentionFamilies do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  def delete!(family, policy, cutoff, cursor, workspace_id \\ nil)

  def delete!(:execution_history, policy, cutoff, cursor, nil) do
    cursor = cursor || %{}
    phase = cursor["phase"] || 0

    {key, result} =
      case phase do
        0 ->
          {"groups",
           FavnStoragePostgres.Maintenance.History.delete!(policy, cutoff, cursor["groups"])}

        1 ->
          {"tasks",
           FavnStoragePostgres.Maintenance.TaskRetention.delete!(policy, cutoff, cursor["tasks"])}

        2 ->
          {"submissions",
           FavnStoragePostgres.Maintenance.SubmissionRetention.delete!(
             policy,
             cutoff,
             cursor["submissions"]
           )}
      end

    %{
      result
      | cursor: cursor |> Map.put(key, result.cursor) |> Map.put("phase", rem(phase + 1, 3))
    }
  end

  def delete!(:logs, policy, cutoff, cursor, workspace_id),
    do: FavnStoragePostgres.Maintenance.LogRetention.delete!(policy, cutoff, cursor, workspace_id)

  def delete!(:registry, policy, cutoff, %{"phase" => phase} = cursor, nil)
      when phase in [1, 2] do
    kind = if phase == 1, do: :deployment, else: :manifest

    result =
      FavnStoragePostgres.Maintenance.RegistryRetention.delete!(
        kind,
        policy,
        cutoff,
        cursor[Atom.to_string(kind)]
      )

    next =
      cursor
      |> Map.put("phase", rem(phase + 1, 3))
      |> Map.put(Atom.to_string(kind), result.cursor)

    %{result | cursor: next}
  end

  def delete!(:operations, policy, cutoff, %{"phase" => 5} = cursor, nil) do
    result =
      FavnStoragePostgres.Maintenance.OperationRetention.delete!(
        policy,
        cutoff,
        cursor["rebuild"]
      )

    %{result | cursor: cursor |> Map.put("phase", 0) |> Map.put("rebuild", result.cursor)}
  end

  def delete!(family, policy, cutoff, cursor, workspace_id) do
    queries = queries(family)
    phase = rem((cursor || %{})["phase"] || 0, length(queries))
    {table, predicate, order} = Enum.at(queries, phase)

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS MATERIALIZED (
          SELECT item.ctid FROM favn_control.#{table} item
          WHERE #{predicate} AND #{scope(table)}
          ORDER BY #{order} LIMIT $3 FOR UPDATE OF item SKIP LOCKED
        )
        DELETE FROM favn_control.#{table} item USING candidates WHERE item.ctid = candidates.ctid
        """,
        [cutoff, policy.excluded_workspace_ids, policy.row_limit, workspace_id]
      )

    %{
      deleted_count: count,
      cursor:
        Map.put(
          cursor || %{},
          "phase",
          rem(
            phase + 1,
            length(queries) +
              if(is_nil(workspace_id),
                do:
                  case family do
                    :operations -> 1
                    :registry -> 2
                    _ -> 0
                  end,
                else: 0
              )
          )
        )
    }
  end

  def preview!(:execution_history, policy, cutoff) do
    groups = FavnStoragePostgres.Maintenance.History.preview!(policy, cutoff)
    tasks = FavnStoragePostgres.Maintenance.TaskRetention.preview!(policy, cutoff)
    submissions = FavnStoragePostgres.Maintenance.SubmissionRetention.preview!(policy, cutoff)

    %{
      family: :execution_history,
      cutoff: cutoff,
      owners: [groups, tasks, submissions],
      eligible_count: groups.eligible_count + tasks.eligible_count + submissions.eligible_count,
      complete?: groups.complete? and tasks.complete? and submissions.complete?
    }
  end

  def preview!(:logs, policy, cutoff),
    do: FavnStoragePostgres.Maintenance.LogRetention.preview!(policy, cutoff)

  def preview!(family, policy, cutoff) do
    counts =
      Enum.map(queries(family), fn {table, predicate, _order} ->
        %{rows: [[count]]} =
          SQL.query!(
            Repo,
            """
            SELECT count(*) FROM (
              SELECT 1 FROM favn_control.#{table} item
              WHERE #{predicate} LIMIT $3
            ) bounded
            """,
            [cutoff, policy.excluded_workspace_ids, policy.scan_limit + 1]
          )

        %{
          table: table,
          unit: :rows,
          count: min(count, policy.scan_limit),
          complete?: count <= policy.scan_limit
        }
      end)

    counts =
      if family == :registry,
        do:
          counts ++
            Enum.map(
              [:deployment, :manifest],
              &FavnStoragePostgres.Maintenance.RegistryRetention.preview!(&1, policy, cutoff)
            ),
        else: counts

    counts =
      if family == :operations,
        do:
          counts ++
            [
              FavnStoragePostgres.Maintenance.OperationRetention.preview!(policy, cutoff)
            ],
        else: counts

    %{
      family: family,
      cutoff: cutoff,
      tables: counts,
      eligible_count:
        if(family in [:registry, :operations],
          do: nil,
          else: Enum.sum(Enum.map(counts, & &1.count))
        ),
      complete?: Enum.all?(counts, & &1.complete?)
    }
  end

  defp receipt_eligible do
    """
    receipt.inserted_at < $1 AND receipt.issued_at < $1
    AND NOT EXISTS (SELECT 1 FROM unnest($2::text[]) held(workspace_id)
      WHERE receipt.scope_id = 'workspace:' || held.workspace_id
         OR receipt.scope_id = 'platform:runner_tasks')
    """
  end

  defp queries(:receipts) do
    [
      {"runner_task_command_tasks",
       """
       EXISTS (SELECT 1 FROM favn_control.runner_task_commands receipt
         WHERE receipt.scope_id = item.scope_id AND receipt.command_id = item.command_id
           AND #{receipt_eligible()})
       """, "item.scope_id, item.command_id, item.ordinal"},
      {"runner_task_commands",
       """
       EXISTS (SELECT 1 FROM favn_control.runner_task_commands receipt
         WHERE receipt.scope_id = item.scope_id AND receipt.command_id = item.command_id
           AND #{receipt_eligible()})
       AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_command_tasks snapshot
         WHERE snapshot.scope_id = item.scope_id AND snapshot.command_id = item.command_id)
       """, "item.inserted_at, item.scope_id, item.command_id"},
      {"run_submission_commands",
       "item.inserted_at < $1 AND NOT (item.workspace_id = ANY($2::text[]))",
       "item.inserted_at, item.workspace_id, item.command_id"},
      {"runner_task_outcomes",
       """
       item.inserted_at < $1 AND NOT (item.workspace_id = ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.runner_tasks task
         WHERE task.workspace_id = item.workspace_id AND task.task_id = item.task_id
           AND task.assignment_generation > item.assignment_generation)
       AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_command_tasks snapshot
         WHERE snapshot.workspace_id = item.workspace_id AND snapshot.task_id = item.task_id
           AND snapshot.outcome_assignment_generation = item.assignment_generation)
       """, "item.inserted_at, item.workspace_id, item.task_id"},
      {"runner_task_runtime_input_errors",
       """
       item.inserted_at < $1 AND NOT (item.workspace_id = ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.runner_tasks task
         WHERE task.workspace_id = item.workspace_id AND task.task_id = item.task_id
           AND task.runtime_input_resolution_id IS DISTINCT FROM item.resolution_id)
       AND NOT EXISTS (SELECT 1 FROM favn_control.runner_task_command_tasks snapshot
         WHERE snapshot.workspace_id = item.workspace_id AND snapshot.task_id = item.task_id
           AND snapshot.runtime_input_resolution_id = item.resolution_id)
       """, "item.inserted_at, item.workspace_id, item.task_id"}
    ]
  end

  defp queries(:sessions) do
    [
      {"auth_operator_commands",
       """
       item.expires_at < $1 AND item.terminal_at < $1
       AND item.status IN ('accepted','partial','rejected')
       AND NOT (item.workspace_id = ANY($2::text[]))
       """, "item.expires_at, item.intent_id"},
      {"auth_sessions",
       """
       item.expires_at < $1 AND item.updated_at < $1
       AND NOT (item.workspace_id = ANY($2::text[]))
       AND NOT EXISTS (SELECT 1 FROM favn_control.auth_operator_commands command
         WHERE command.session_id = item.session_id)
       """, "item.expires_at, item.session_id"},
      {"runner_sessions",
       """
       item.ended_at < $1 AND cardinality($2::text[]) = 0
       AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks task
         WHERE task.assigned_runner_instance_id = item.runner_instance_id
           AND task.assigned_runner_session_generation = item.session_generation)
       """, "item.ended_at, item.session_id"}
    ]
  end

  defp queries(:idempotency) do
    [
      {"idempotency_records",
       """
       item.expires_at < $1 AND item.status = 'committed'
       AND NOT (item.workspace_id = ANY($2::text[]))
       AND NOT EXISTS (SELECT 1 FROM favn_control.auth_operator_commands command
         WHERE command.workspace_id = item.workspace_id AND command.operation = item.operation
           AND command.principal_kind = item.principal_kind AND command.principal_id = item.principal_id
           AND convert_to(command.key_hash,'UTF8') = item.key_hash)
       """, "item.expires_at, item.workspace_id"}
    ]
  end

  defp queries(:maintenance) do
    [
      {"maintenance_jobs",
       """
       item.job_kind <> 'retention' AND item.status = 'completed' AND item.updated_at < $1
       AND (NOT (item.workspace_id = ANY($2::text[])) OR
            (item.workspace_id IS NULL AND cardinality($2::text[]) = 0))
       """, "item.updated_at, item.job_id"}
    ]
  end

  defp queries(:registry) do
    [
      {"execution_packages",
       """
       item.inserted_at < $1 AND cardinality($2::text[]) = 0
       AND NOT EXISTS (SELECT 1 FROM favn_control.manifest_execution_packages link
         WHERE link.package_hash = item.content_hash)
       AND NOT EXISTS (SELECT 1 FROM favn_control.runtime_input_pins pin WHERE pin.execution_package_hash=item.content_hash)
       """, "item.inserted_at, item.content_hash"}
    ]
  end

  defp queries(:operations) do
    [
      {"projection_failures",
       """
       item.updated_at < $1 AND NOT (item.workspace_id=ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.projection_cursors c
         WHERE c.projector_name=item.projector_name AND c.shard_id=item.shard_id
           AND c.last_publication_id>=item.publication_id)
       """, "item.updated_at,item.failure_id"},
      {"execution_lease_scopes",
       """
       NOT (item.workspace_id=ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.execution_leases lease
         WHERE lease.workspace_id=item.workspace_id AND lease.lease_id=item.lease_id
           AND lease.status<>'active' AND lease.released_at < $1)
       """, "item.workspace_id,item.lease_id,item.scope_id"},
      {"execution_leases",
       """
       item.status<>'active' AND item.released_at < $1 AND NOT (item.workspace_id=ANY($2::text[]))
       AND NOT EXISTS (SELECT 1 FROM favn_control.execution_lease_scopes scope
         WHERE scope.workspace_id=item.workspace_id AND scope.lease_id=item.lease_id)
       """, "item.released_at,item.workspace_id,item.lease_id"},
      {"schedule_occurrences",
       """
       item.status IN ('completed','failed','suppressed') AND item.updated_at<$1
       AND item.due_at<$1 AND NOT (item.workspace_id=ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.schedule_cursors c WHERE c.workspace_id=item.workspace_id AND c.deployment_id=item.deployment_id AND c.schedule_id=item.schedule_id AND c.next_due_at>item.due_at AND c.last_command_id IS DISTINCT FROM item.evaluation_command_id)
       AND NOT EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id AND (r.terminal_at IS NULL OR r.terminal_at >= $1))
       AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.run_id AND r.source='scheduler' AND (r.terminal_at IS NULL OR r.terminal_at >= $1))
       """, "item.updated_at,item.workspace_id,item.occurrence_id"},
      {"resource_recovery_candidates",
       """
       item.status IN ('submitted','cancelled') AND item.updated_at<$1
       AND NOT (item.workspace_id=ANY($2::text[]))
       AND EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.source_run_id AND r.terminal_at<$1)
       AND (item.recovery_run_id IS NULL OR EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.workspace_id=item.workspace_id AND r.run_id=item.recovery_run_id AND r.terminal_at<$1))
       AND NOT EXISTS (SELECT 1 FROM favn_control.materializations m WHERE m.workspace_id=item.workspace_id AND m.run_id IN (item.source_run_id,item.recovery_run_id))
       """, "item.updated_at,item.workspace_id,item.candidate_id"}
    ]
  end

  defp scope(table) when table in ["execution_packages", "runner_sessions"],
    do: "$4::text IS NULL"

  defp scope("runner_task_commands"), do: "($4::text IS NULL OR item.scope_id='workspace:' || $4)"
  defp scope(_), do: "($4::text IS NULL OR item.workspace_id=$4)"
end
