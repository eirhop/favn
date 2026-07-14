defmodule FavnOrchestrator.Operator.Catalogue.PageReaderTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Operator.Catalogue.PageReader
  alias FavnOrchestrator.Page

  test "collects pages in order" do
    fetch_page = fn
      0 -> {:ok, page([1, 2], 0, true, 2)}
      2 -> {:ok, page([3], 2, false, nil)}
    end

    assert {:ok, [1, 2, 3]} = PageReader.all(fetch_page)
  end

  test "rejects a non-advancing cursor" do
    assert {:error, :invalid_pagination_cursor} =
             PageReader.all(fn offset -> {:ok, page([], offset, true, offset)} end)
  end

  defp page(items, offset, has_more?, next_offset) do
    %Page{
      items: items,
      limit: 2,
      offset: offset,
      has_more?: has_more?,
      next_offset: next_offset
    }
  end
end
