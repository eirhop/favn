defmodule FavnStoragePostgres.Migrations.OptimizeRunStatusPagingV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      index(:runs, [:workspace_id, :status, {:desc, :latest_event_id}, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_workspace_status_recent_idx
      )
    )

    create(
      index(:runs, [:status, {:desc, :latest_event_id}, :workspace_id, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_platform_status_recent_idx
      )
    )
  end

  def down do
    drop(
      index(:runs, [],
        prefix: @prefix,
        name: :runs_platform_status_recent_idx
      )
    )

    drop(
      index(:runs, [],
        prefix: @prefix,
        name: :runs_workspace_status_recent_idx
      )
    )
  end
end
