defmodule FavnStoragePostgres.Migrations.AddRunnerSessionsV2 do
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:runner_sessions, primary_key: false, prefix: @prefix) do
      add(:session_id, :text, null: false, primary_key: true)
      add(:runner_instance_id, :text, null: false)
      add(:runner_boot_id, :text, null: false)
      add(:session_generation, :bigint, null: false)
      add(:control_plane_boot_id, :text, null: false)
      add(:runner_pool, :text, null: false)
      add(:required_runner_release_id, :text, null: false)
      add(:beam_node, :text, null: false)
      add(:protocol_version, :integer, null: false)
      add(:lifecycle_mode, :text, null: false)
      add(:registered_at, :utc_datetime_usec, null: false)
      add(:ended_at, :utc_datetime_usec)
      add(:end_reason, :text)
      add(:busy_at_exit, :boolean, null: false, default: false)
      add(:interrupted_task_workspace_id, :text)
      add(:interrupted_task_id, :text)
      add(:inserted_at, :utc_datetime_usec, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    create(
      index(:runner_sessions, [:runner_instance_id],
        name: :runner_sessions_open_instance_idx,
        where: "ended_at IS NULL",
        prefix: @prefix
      )
    )

    create(
      index(:runner_sessions, [:control_plane_boot_id],
        name: :runner_sessions_open_boot_idx,
        where: "ended_at IS NULL",
        prefix: @prefix
      )
    )

    create(
      index(:runner_sessions, [{:desc, :registered_at}, :session_id],
        name: :runner_sessions_recent_idx,
        prefix: @prefix
      )
    )

    create(
      index(:runner_sessions, [:ended_at],
        name: :runner_sessions_prune_idx,
        where: "ended_at IS NOT NULL",
        prefix: @prefix
      )
    )

    create(
      constraint(:runner_sessions, :runner_sessions_end_shape_valid,
        prefix: @prefix,
        check:
          "(ended_at IS NULL AND end_reason IS NULL) OR (ended_at IS NOT NULL AND end_reason IN ('shut_down','crashed','presumed_dead') AND ended_at >= registered_at)"
      )
    )

    create(
      constraint(:runner_sessions, :runner_sessions_identity_valid,
        prefix: @prefix,
        check:
          "session_id ~ '^rs_[0-9a-f]{32}$' AND octet_length(runner_instance_id) BETWEEN 1 AND 255 AND octet_length(runner_boot_id) BETWEEN 1 AND 255 AND session_generation > 0 AND octet_length(control_plane_boot_id) BETWEEN 1 AND 255 AND runner_pool ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$' AND required_runner_release_id ~ '^rr_[0-9a-f]{64}$' AND octet_length(beam_node) BETWEEN 1 AND 255 AND protocol_version > 0 AND octet_length(lifecycle_mode) BETWEEN 1 AND 63 AND (interrupted_task_workspace_id IS NULL OR octet_length(interrupted_task_workspace_id) BETWEEN 1 AND 255) AND (interrupted_task_id IS NULL OR octet_length(interrupted_task_id) BETWEEN 1 AND 255)"
      )
    )

    alter table(:runner_tasks, prefix: @prefix) do
      add(:assigned_at, :utc_datetime_usec)
    end

    create(
      index(:runner_tasks, [:assigned_runner_instance_id, :assigned_runner_session_generation],
        name: :runner_tasks_session_attribution_idx,
        where: "assigned_runner_instance_id IS NOT NULL",
        prefix: @prefix
      )
    )

    create(
      index(:runner_tasks, [:terminal_at],
        name: :runner_tasks_busy_window_idx,
        where: "assigned_at IS NOT NULL AND terminal_at IS NOT NULL",
        prefix: @prefix
      )
    )
  end

  def down do
    drop(
      index(:runner_tasks, [:terminal_at], name: :runner_tasks_busy_window_idx, prefix: @prefix)
    )

    drop(
      index(:runner_tasks, [:assigned_runner_instance_id, :assigned_runner_session_generation],
        name: :runner_tasks_session_attribution_idx,
        prefix: @prefix
      )
    )

    alter table(:runner_tasks, prefix: @prefix) do
      remove(:assigned_at)
    end

    drop(table(:runner_sessions, prefix: @prefix))
  end
end
