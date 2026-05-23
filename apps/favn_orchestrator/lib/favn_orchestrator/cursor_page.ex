defmodule FavnOrchestrator.CursorPage do
  @moduledoc """
  Bounded cursor page returned by internal storage scan APIs.

  `FavnOrchestrator.Page` remains the public offset-pagination contract for
  operator/UI reads. `CursorPage` is for orchestrator-owned full walks and repair
  paths that need stable keyset traversal over mutable persisted truth.
  """

  alias FavnOrchestrator.Page

  @enforce_keys [:items, :limit, :after_cursor, :has_more?, :next_cursor]
  defstruct [:items, :limit, :after_cursor, :has_more?, :next_cursor]

  @type cursor :: map() | nil

  @type t(item) :: %__MODULE__{
          items: [item],
          limit: pos_integer(),
          after_cursor: cursor(),
          has_more?: boolean(),
          next_cursor: cursor()
        }

  @type opts :: [limit: pos_integer(), after: cursor()]

  @doc "Normalizes internal cursor scan options."
  @spec normalize_opts(keyword()) :: {:ok, opts()} | {:error, :invalid_cursor_pagination}
  def normalize_opts(opts) when is_list(opts) do
    with :ok <- reject_unknown_opts(opts),
         {:ok, limit} <- normalize_limit(Keyword.get(opts, :limit, Page.default_limit())),
         {:ok, after_cursor} <- normalize_after(Keyword.get(opts, :after)) do
      {:ok, [limit: limit, after: after_cursor]}
    else
      {:error, :invalid_cursor_pagination} -> {:error, :invalid_cursor_pagination}
    end
  end

  @doc "Builds a cursor page from rows fetched with `limit + 1`."
  @spec from_fetched([item], opts(), (item -> cursor())) :: t(item) when item: term()
  def from_fetched(items, opts, cursor_fun)
      when is_list(items) and is_list(opts) and is_function(cursor_fun, 1) do
    limit = Keyword.fetch!(opts, :limit)
    page_items = Enum.take(items, limit)
    has_more? = length(items) > limit

    %__MODULE__{
      items: page_items,
      limit: limit,
      after_cursor: Keyword.get(opts, :after),
      has_more?: has_more?,
      next_cursor: if(has_more? and page_items != [], do: cursor_fun.(List.last(page_items)))
    }
  end

  defp reject_unknown_opts(opts) do
    if Enum.all?(opts, fn {key, _value} -> key in [:limit, :after] end) do
      :ok
    else
      {:error, :invalid_cursor_pagination}
    end
  end

  defp normalize_limit(value) when is_integer(value) and value >= 1 do
    if value <= Page.max_limit(), do: {:ok, value}, else: {:error, :invalid_cursor_pagination}
  end

  defp normalize_limit(_value), do: {:error, :invalid_cursor_pagination}

  defp normalize_after(nil), do: {:ok, nil}
  defp normalize_after(value) when is_map(value), do: {:ok, value}
  defp normalize_after(_value), do: {:error, :invalid_cursor_pagination}
end
