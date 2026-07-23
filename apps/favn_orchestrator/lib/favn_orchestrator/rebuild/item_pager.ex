defmodule FavnOrchestrator.Rebuild.ItemPager do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems

  @page_size 500

  @spec all(module(), struct(), String.t(), keyword()) :: {:ok, [term()]} | {:error, term()}
  def all(store, context, operation_id, opts \\ [])
      when is_atom(store) and is_binary(operation_id) and is_list(opts) do
    page(store, context, operation_id, opts, nil, [])
  end

  @spec count(module(), struct(), String.t(), keyword(), (term() -> boolean())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def count(store, context, operation_id, opts, predicate)
      when is_atom(store) and is_binary(operation_id) and is_list(opts) and
             is_function(predicate, 1) do
    count_page(store, context, operation_id, opts, predicate, nil, 0)
  end

  defp page(store, context, operation_id, opts, cursor, pages) do
    page_query = page_query(context, operation_id, opts, cursor)

    case store.page_items(page_query) do
      {:ok, %{items: items, has_more?: true, next_cursor: next_cursor}}
      when not is_nil(next_cursor) ->
        page(store, context, operation_id, opts, next_cursor, [items | pages])

      {:ok, %{items: items, has_more?: false}} ->
        {:ok, pages |> Enum.reverse() |> List.flatten() |> Kernel.++(items)}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_rebuild_item_page}
    end
  end

  defp count_page(store, context, operation_id, opts, predicate, cursor, count) do
    case store.page_items(page_query(context, operation_id, opts, cursor)) do
      {:ok, %{items: items} = page} ->
        if Enum.all?(items, predicate) do
          next_count = count + length(items)

          if page.has_more? and not is_nil(page.next_cursor) do
            count_page(
              store,
              context,
              operation_id,
              opts,
              predicate,
              page.next_cursor,
              next_count
            )
          else
            {:ok, next_count}
          end
        else
          {:error, :rebuild_item_evidence_invalid}
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_rebuild_item_page}
    end
  end

  defp page_query(context, operation_id, opts, cursor) do
    %PageRebuildItems{
      workspace_context: context,
      operation_id: operation_id,
      target_id: Keyword.get(opts, :target_id),
      status: Keyword.get(opts, :status),
      after: cursor,
      limit: @page_size
    }
  end
end
