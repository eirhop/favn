defmodule FavnStoragePostgres.Migrations.AddExecutionGroupStartOrderingV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:execution_group_overviews, prefix: @prefix) do
      add(:trigger_type, :text)
      add(:started_at, :timestamptz)
      add(:finished_at, :timestamptz)
    end

    # `finished_at` is when the group stopped, not when its root run did: a
    # backfill's root run is terminal the instant it is created, so it is the last
    # of the group's runs to end that finished the group. A group with work still
    # outstanding has not finished at all.
    execute("""
    UPDATE #{@prefix}.execution_group_overviews AS overview
    SET trigger_type = root.trigger_type,
        started_at = root.inserted_at,
        finished_at = CASE
          WHEN overview.pending_count > 0 OR overview.running_count > 0 THEN NULL
          ELSE (
            SELECT max(member.terminal_at)
            FROM #{@prefix}.runs AS member
            WHERE member.workspace_id = overview.workspace_id
              AND member.root_execution_group_id = overview.root_run_id
          )
        END
    FROM #{@prefix}.runs AS root
    WHERE root.workspace_id = overview.workspace_id
      AND root.run_id = overview.root_run_id
    """)

    # The runs list orders by when a group started, so the sort key has to live on
    # the group. Ordering it on the root run instead meant a join and a sort of the
    # whole workspace for every page. NULLS LAST descending is the exact reverse of
    # NULLS FIRST ascending, so one index serves both directions by being scanned
    # backwards, and the window filters are a range on the same prefix.
    create(
      index(
        :execution_group_overviews,
        [:workspace_id, {:desc_nulls_last, :started_at}, {:desc, :root_run_id}],
        prefix: @prefix,
        name: :execution_group_overviews_started_idx
      )
    )

    create(
      index(
        :execution_group_overviews,
        [{:desc_nulls_last, :started_at}, {:desc, :workspace_id}, {:desc, :root_run_id}],
        prefix: @prefix,
        name: :execution_group_overviews_platform_started_idx
      )
    )
  end

  def down do
    drop(
      index(:execution_group_overviews, [],
        prefix: @prefix,
        name: :execution_group_overviews_platform_started_idx
      )
    )

    drop(
      index(:execution_group_overviews, [],
        prefix: @prefix,
        name: :execution_group_overviews_started_idx
      )
    )

    alter table(:execution_group_overviews, prefix: @prefix) do
      remove(:finished_at)
      remove(:started_at)
      remove(:trigger_type)
    end
  end
end
