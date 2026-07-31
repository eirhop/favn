defmodule FavnStoragePostgres.Migrations.AddRunnerTaskOperatorReadsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      index(:runner_tasks, [:workspace_id, {:desc, :inserted_at}, {:desc, :task_id}],
        name: :runner_tasks_workspace_recent_idx,
        prefix: @prefix
      )
    )

    create(
      index(
        :runner_tasks,
        [:workspace_id, {:desc, :inserted_at}, {:desc, :task_id}],
        name: :runner_tasks_workspace_status_recent_idx,
        prefix: @prefix,
        where: "status IN ('failed', 'unknown')"
      )
    )
  end

  def down do
    drop(
      index(:runner_tasks, [],
        name: :runner_tasks_workspace_status_recent_idx,
        prefix: @prefix
      )
    )

    drop(
      index(:runner_tasks, [],
        name: :runner_tasks_workspace_recent_idx,
        prefix: @prefix
      )
    )
  end
end
