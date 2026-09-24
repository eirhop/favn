defmodule FavnStoragePostgres.Maintenance.RegistryRetention do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  def delete!(kind, policy, cutoff, cursor) when kind in [:manifest, :deployment] do
    {table, key, child} = tables(kind)
    workspace = if kind == :deployment, do: "item.workspace_id", else: "''::text"

    scope =
      if kind == :deployment,
        do: "NOT (item.workspace_id=ANY($2::text[]))",
        else: "cardinality($2::text[])=0"

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT #{workspace},item.#{key},item.retiring FROM favn_control.#{table} item
        WHERE (item.retiring OR item.inserted_at < $1) AND #{scope}
          AND (#{workspace},item.#{key}) > ($3,$4)
          AND ($5::text IS NULL OR (#{workspace}=$5 AND item.#{key}=$6))
        ORDER BY #{workspace},item.#{key} LIMIT 1 FOR UPDATE OF item SKIP LOCKED
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
        identity =
          if kind == :deployment,
            do: "item.workspace_id=$1 AND item.#{key}=$2",
            else: "$1::text='' AND item.#{key}=$2"

        %{rows: [[eligible]]} =
          SQL.query!(
            Repo,
            "SELECT #{predicate(kind)} FROM favn_control.#{table} item WHERE #{identity}",
            [workspace, id]
          )

        if retiring or eligible do
          SQL.query!(
            Repo,
            "UPDATE favn_control.#{table} item SET retiring=true WHERE #{identity}",
            [workspace, id]
          )

          # The owner is unreadable before any child is removed; reference triggers reject new links.
          %{num_rows: deleted} =
            SQL.query!(
              Repo,
              """
              WITH candidates AS (SELECT item.ctid FROM favn_control.#{child} item WHERE #{identity}
                LIMIT $3 FOR UPDATE SKIP LOCKED)
              DELETE FROM favn_control.#{child} item USING candidates WHERE item.ctid=candidates.ctid
              """,
              [workspace, id, policy.row_limit]
            )

          count =
            if deleted == 0 do
              SQL.query!(
                Repo,
                "DELETE FROM favn_control.#{table} item WHERE #{identity} AND NOT EXISTS (SELECT 1 FROM favn_control.#{child} item WHERE #{identity})",
                [workspace, id]
              ).num_rows
            else
              0
            end

          %{
            deleted_count: deleted + count,
            cursor:
              if(count == 1,
                do: %{"after_workspace" => workspace, "after_id" => id},
                else: %{"workspace_id" => workspace, "id" => id}
              )
          }
        else
          %{deleted_count: 0, cursor: %{"after_workspace" => workspace, "after_id" => id}}
        end
    end
  end

  def preview!(kind, policy, cutoff) do
    {table, _, _} = tables(kind)

    scope =
      if kind == :deployment,
        do: "NOT (item.workspace_id=ANY($2::text[]))",
        else: "cardinality($2::text[])=0"

    %{rows: [[count]]} =
      SQL.query!(
        Repo,
        "SELECT count(*) FROM (SELECT 1 FROM favn_control.#{table} item WHERE (item.retiring OR item.inserted_at<$1) AND #{scope} AND (item.retiring OR (#{predicate(kind)})) LIMIT $3) bounded",
        [cutoff, policy.excluded_workspace_ids, policy.scan_limit + 1]
      )

    %{table: table, count: min(count, policy.scan_limit), complete?: count <= policy.scan_limit}
  end

  defp tables(:manifest),
    do: {"manifest_versions", "manifest_version_id", "manifest_execution_packages"}

  defp tables(:deployment),
    do: {"workspace_deployments", "deployment_id", "workspace_deployment_targets"}

  defp predicate(:manifest),
    do: """
    NOT EXISTS (SELECT 1 FROM favn_control.asset_evidence_bindings r WHERE r.initial_manifest_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.asset_freshness_states r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.asset_target_bindings r WHERE r.desired_manifest_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.asset_target_generations r WHERE r.creating_manifest_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.asset_window_states r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.backfills r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.coverage_baselines r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.manifest_deployment_operations r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.rebuild_operations r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.run_plans r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.run_targets r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.manifest_version_id=item.manifest_version_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.workspace_deployments r WHERE r.manifest_version_id=item.manifest_version_id)
    """

  defp predicate(:deployment),
    do: """
    NOT EXISTS (SELECT 1 FROM favn_control.asset_freshness_states r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.backfills r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.coverage_baselines r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.manifest_deployment_operations r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.materialization_claims r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.materializations r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.run_submissions r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.run_targets r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.runs r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.schedule_cursors r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.schedule_occurrences r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.target_statuses r WHERE r.deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    AND NOT EXISTS (SELECT 1 FROM favn_control.workspace_runtime_state r WHERE r.active_deployment_id=item.deployment_id AND r.workspace_id=item.workspace_id)
    """
end
