defmodule FavnStoragePostgres.Migrations.OptimizeLogicalTargetHistoryV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      index(
        :runs,
        [:workspace_id, {:desc, :submitted_event_id}, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_root_submission_history_idx,
        where: "run_id = root_execution_group_id"
      )
    )

    create(
      index(
        :run_targets,
        [
          :workspace_id,
          :target_kind,
          :target_id,
          {:desc, :submitted_event_id},
          {:desc, :run_id}
        ],
        prefix: @prefix,
        name: :run_targets_logical_history_idx,
        include: [:deployment_id, :manifest_version_id, :is_primary]
      )
    )

    create(
      index(
        :target_statuses,
        [:workspace_id, :target_kind, :target_id, {:desc, :source_publication_id}],
        prefix: @prefix,
        name: :target_statuses_logical_latest_idx,
        include: [:deployment_id, :status, :run_id, :event_id, :updated_at]
      )
    )
  end

  def down do
    drop(
      index(:target_statuses, [],
        prefix: @prefix,
        name: :target_statuses_logical_latest_idx
      )
    )

    drop(
      index(:run_targets, [],
        prefix: @prefix,
        name: :run_targets_logical_history_idx
      )
    )

    drop(
      index(:runs, [],
        prefix: @prefix,
        name: :runs_root_submission_history_idx
      )
    )
  end
end
