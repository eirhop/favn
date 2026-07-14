defmodule FavnOrchestrator.Operator.Catalogue.PageReader do
  @moduledoc """
  Exhausts an offset-paginated orchestrator read without hiding page failures.

  A non-advancing `next_offset` is rejected so a malformed adapter page cannot
  trap an operator request in an infinite recursion.
  """

  alias FavnOrchestrator.Page

  @doc "Returns all items produced by an offset-based page function."
  @spec all((non_neg_integer() -> {:ok, Page.t(item)} | {:error, term()})) ::
          {:ok, [item]} | {:error, term()}
        when item: term()
  def all(fetch_page) when is_function(fetch_page, 1), do: fetch_all(fetch_page, 0, [])

  defp fetch_all(fetch_page, offset, acc) do
    case fetch_page.(offset) do
      {:ok, %Page{has_more?: true, next_offset: next_offset, items: items}}
      when is_integer(next_offset) and next_offset > offset ->
        fetch_all(fetch_page, next_offset, [items | acc])

      {:ok, %Page{has_more?: true}} ->
        {:error, :invalid_pagination_cursor}

      {:ok, %Page{items: items}} ->
        {:ok, Enum.concat(Enum.reverse([items | acc]))}

      {:error, _reason} = error ->
        error
    end
  end
end
