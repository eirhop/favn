defmodule FavnOrchestrator.Rebuild.ItemPagerTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuild.ItemPager

  defmodule Store do
    def page_items(query) do
      send(Process.get(:item_pager_test_pid), {:page_rebuild_items, query})

      case query.after do
        nil ->
          {:ok,
           %CursorPage{
             items: Enum.map(0..499, &%{ordinal: &1}),
             limit: 500,
             has_more?: true,
             next_cursor: %{ordinal: 499, target_id: "target", item_id: "item-499"}
           }}

        %{ordinal: 499} ->
          {:ok,
           %CursorPage{
             items: [%{ordinal: 500}],
             limit: 500,
             has_more?: false,
             next_cursor: nil
           }}
      end
    end
  end

  test "reads every bounded page when candidate evidence exceeds 500 items" do
    Process.put(:item_pager_test_pid, self())
    {:ok, context} = WorkspaceContext.new("workspace", "dispatcher", [:customer_operator])

    assert {:ok, 501} =
             ItemPager.count(
               Store,
               context,
               "operation",
               [target_id: "target", status: :succeeded],
               &(&1.ordinal <= 500)
             )

    assert_receive {:page_rebuild_items,
                    %{after: nil, limit: 500, target_id: "target", status: :succeeded}}

    assert_receive {:page_rebuild_items,
                    %{
                      after: %{ordinal: 499},
                      limit: 500,
                      target_id: "target",
                      status: :succeeded
                    }}
  end
end
