defmodule FavnStoragePostgres.Migrations.AddRebuildOperatorReadsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def change do
    create(
      index(:rebuild_operations, [:workspace_id, "inserted_at DESC", "operation_id DESC"],
        prefix: @prefix,
        name: :rebuild_operations_page_idx
      )
    )

    create(
      index(
        :rebuild_operations,
        [:workspace_id, :state, "inserted_at DESC", "operation_id DESC"],
        prefix: @prefix,
        name: :rebuild_operations_state_page_idx
      )
    )

    create(
      index(
        :rebuild_windows,
        [:workspace_id, :operation_id, :ordinal, :target_id, :item_id],
        prefix: @prefix,
        name: :rebuild_windows_operation_page_idx
      )
    )
  end
end
