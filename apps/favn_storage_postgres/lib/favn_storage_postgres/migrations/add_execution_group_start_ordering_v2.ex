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

    execute("""
    UPDATE #{@prefix}.execution_group_overviews AS overview
    SET trigger_type = root.trigger_type,
        started_at = root.inserted_at,
        finished_at = root.terminal_at
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
