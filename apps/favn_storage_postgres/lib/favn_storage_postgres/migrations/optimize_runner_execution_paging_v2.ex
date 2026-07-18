defmodule FavnStoragePostgres.Migrations.OptimizeRunnerExecutionPagingV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    drop(index(:runner_executions, [], prefix: @prefix, name: :runner_executions_run_idx))

    drop(
      index(:runner_executions, [],
        prefix: @prefix,
        name: :runner_executions_owner_active_idx
      )
    )

    create(
      index(:runner_executions, [:workspace_id, :run_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_run_page_idx
      )
    )

    create(
      index(:runner_executions, [:workspace_id, :run_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_run_active_page_idx,
        where: "terminal_at IS NULL"
      )
    )

    create(
      index(:runner_executions, [:workspace_id, :owner_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_owner_active_idx,
        where: "terminal_at IS NULL"
      )
    )

    create(
      index(:runner_executions, [:workspace_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_workspace_active_idx,
        where: "terminal_at IS NULL"
      )
    )
  end

  def down do
    drop(
      index(:runner_executions, [],
        prefix: @prefix,
        name: :runner_executions_workspace_active_idx
      )
    )

    drop(
      index(:runner_executions, [],
        prefix: @prefix,
        name: :runner_executions_owner_active_idx
      )
    )

    drop(
      index(:runner_executions, [],
        prefix: @prefix,
        name: :runner_executions_run_active_page_idx
      )
    )

    drop(index(:runner_executions, [], prefix: @prefix, name: :runner_executions_run_page_idx))

    create(
      index(:runner_executions, [:workspace_id, :run_id, {:desc, :inserted_at}],
        prefix: @prefix,
        name: :runner_executions_run_idx
      )
    )

    create(
      index(:runner_executions, [:owner_id, :status, :workspace_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_owner_active_idx,
        where: "terminal_at IS NULL"
      )
    )
  end
end
