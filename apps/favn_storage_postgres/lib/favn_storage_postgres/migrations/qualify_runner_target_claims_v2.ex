defmodule FavnStoragePostgres.Migrations.QualifyRunnerTargetClaimsV2 do
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      index(
        :runner_tasks,
        [
          :workspace_id,
          :write_target_id,
          :runner_pool,
          :required_runner_release_id,
          :status,
          :enqueued_at,
          :task_id
        ],
        name: :runner_tasks_target_reservation_idx,
        where:
          "write_target_id IS NOT NULL AND status IN ('queued','assigned','preparing','running','cancelling')",
        prefix: @prefix
      )
    )
  end

  def down do
    drop(
      index(:runner_tasks, [],
        name: :runner_tasks_target_reservation_idx,
        prefix: @prefix
      )
    )
  end
end
