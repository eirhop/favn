defmodule FavnStoragePostgres.RuntimeCatalogGuard do
  @moduledoc false
  import Ecto.Query
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.WriteOwnership
  alias FavnStoragePostgres.Schemas.{ManifestVersion, WorkspaceDeployment, WorkspaceRuntimeState}

  @required_contract 17
  @adapter "Elixir.Favn.SQL.Adapter.DuckDB.ADBC"

  # Called before runtime-state or owner/task row locks. Include removed targets
  # so legacy work cannot start while a new workspace contract is activated.
  def lock_deployment!(workspace, targets) do
    ids = target_ids(workspace, targets)
    Enum.each(ids, &WriteOwnership.lock_target!(workspace, &1))
    ids
  end

  def validate_deployment!(workspace, manifest_id, targets, locked_ids) do
    if target_ids(workspace, targets) != locked_ids,
      do: reject!(:runtime_catalog_stale_deployment)

    incoming = Repo.get!(ManifestVersion, manifest_id).runner_contract_version

    if (active_contract(workspace) || 0) >= @required_contract and incoming < @required_contract,
      do: reject!(:runtime_catalog_contract_downgrade)

    if incoming >= @required_contract do
      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          SELECT DISTINCT t.write_target_id FROM favn_control.runner_tasks t
          JOIN favn_control.manifest_versions m USING (manifest_version_id)
          WHERE t.workspace_id = $1 AND t.write_target_id = ANY($2::text[])
            AND m.runner_contract_version < $3
            AND (EXISTS (SELECT 1 FROM favn_control.materialization_claims c WHERE c.workspace_id=t.workspace_id AND c.effect_task_id=t.task_id AND c.effect_state IN ('in_flight','outcome_unknown'))
              OR EXISTS (SELECT 1 FROM favn_control.target_operation_locks l WHERE l.workspace_id=t.workspace_id AND l.effect_task_id=t.task_id AND l.effect_state IN ('in_flight','outcome_unknown')))
          """,
          [workspace, locked_ids, @required_contract]
        )

      Enum.each(rows, fn [target] -> WriteOwnership.guard_target!(workspace, target) end)
      reject_unsupported_reuse!(workspace, manifest_id, targets)
    end

    :ok
  end

  def start!(task) do
    if task.task_kind in [
         "asset_attempt",
         "generation_activate",
         "generation_marker_initialize",
         "generation_discard"
       ] and
         (active_contract(task.workspace_id) || 0) >= @required_contract do
      version = Repo.get!(ManifestVersion, task.manifest_version_id)

      if version.runner_contract_version < @required_contract,
        do: reject!(:runtime_catalog_tracking_required)
    end

    :ok
  end

  defp target_ids(workspace, targets) do
    requested = for t <- targets, t["target_kind"] == "asset", do: t["target_id"]

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT target_id FROM favn_control.asset_target_bindings WHERE workspace_id = $1
        UNION
        SELECT t.target_id FROM favn_control.workspace_deployment_targets t
        JOIN favn_control.workspace_runtime_state r
          ON r.workspace_id = t.workspace_id AND r.active_deployment_id = t.deployment_id
        WHERE t.workspace_id = $1 AND t.target_kind = 'asset'
        """,
        [workspace]
      )

    (requested ++ Enum.map(rows, &hd/1)) |> Enum.uniq() |> Enum.sort()
  end

  defp active_contract(workspace) do
    Repo.one(
      from(r in WorkspaceRuntimeState,
        join: d in WorkspaceDeployment,
        on: d.workspace_id == r.workspace_id and d.deployment_id == r.active_deployment_id,
        join: m in ManifestVersion,
        on: m.manifest_version_id == d.manifest_version_id,
        where: r.workspace_id == ^workspace,
        select: m.runner_contract_version
      )
    )
  end

  defp reject_unsupported_reuse!(workspace, manifest_id, targets) do
    selected = for t <- targets, t["target_kind"] == "asset", do: t["target_id"]

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT b.target_id, previous.descriptor, proposed.descriptor
        FROM favn_control.asset_target_bindings b
        JOIN favn_control.manifest_versions old ON old.manifest_version_id = b.desired_manifest_id
        JOIN favn_control.manifest_versions new ON new.manifest_version_id = $2
        LEFT JOIN LATERAL (
          SELECT a->'target_descriptor' AS descriptor
          FROM jsonb_array_elements(old.manifest->'assets') a
          WHERE a->'target_descriptor'->>'target_id' = b.target_id
        ) previous ON TRUE
        LEFT JOIN LATERAL (
          SELECT a->'target_descriptor' AS descriptor
          FROM jsonb_array_elements(new.manifest->'assets') a
          WHERE a->'target_descriptor'->>'target_id' = b.target_id
        ) proposed ON TRUE
        WHERE b.workspace_id = $1 AND b.target_id = ANY($3::text[])
        """,
        [workspace, manifest_id, selected]
      )

    Enum.each(rows, fn
      [_id, %{"adapter" => @adapter, "runner_contract_version" => v} = old, new]
      when v >= @required_contract ->
        unless is_map(new) and new["adapter"] == @adapter,
          do: reject!(:runtime_catalog_unsupported_target_reuse)

        if old["relation"] != new["relation"] or
             old["connection_identity"] != new["connection_identity"],
           do: reject!(:runtime_catalog_destination_change_unsupported)

      _ ->
        :ok
    end)
  end

  defp reject!(reason),
    do:
      Repo.rollback(
        Error.new(:conflict, "Runtime catalog contract rejected",
          details: %{reason_code: Atom.to_string(reason)}
        )
      )
end
