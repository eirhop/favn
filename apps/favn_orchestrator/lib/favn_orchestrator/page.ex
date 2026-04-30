defmodule FavnOrchestrator.Page do
  @moduledoc """
  Bounded page returned by operational read-model list APIs.
  """

  @default_limit 100
  @max_limit 500

  @enforce_keys [:items, :limit, :offset, :has_more?, :next_offset]
  defstruct [:items, :limit, :offset, :has_more?, :next_offset]

  @type t(item) :: %__MODULE__{
          items: [item],
          limit: pos_integer(),
          offset: non_neg_integer(),
          has_more?: boolean(),
          next_offset: non_neg_integer() | nil
        }

  @type opts :: [limit: pos_integer(), offset: non_neg_integer()]

  @spec default_limit() :: pos_integer()
  def default_limit, do: @default_limit

  @spec max_limit() :: pos_integer()
  def max_limit, do: @max_limit

  @spec normalize_opts(keyword()) :: {:ok, opts()} | {:error, :invalid_pagination}
  def normalize_opts(opts) when is_list(opts) do
    with {:ok, limit} <- normalize_limit(Keyword.get(opts, :limit, @default_limit)),
         {:ok, offset} <- normalize_offset(Keyword.get(opts, :offset, 0)) do
      {:ok, [limit: limit, offset: offset]}
    else
      {:error, :invalid_pagination} -> {:error, :invalid_pagination}
    end
  end

  @spec from_fetched([item], opts()) :: t(item) when item: term()
  def from_fetched(items, opts) when is_list(items) and is_list(opts) do
    limit = Keyword.fetch!(opts, :limit)
    offset = Keyword.fetch!(opts, :offset)
    page_items = Enum.take(items, limit)
    has_more? = length(items) > limit

    %__MODULE__{
      items: page_items,
      limit: limit,
      offset: offset,
      has_more?: has_more?,
      next_offset: if(has_more?, do: offset + length(page_items), else: nil)
    }
  end

  defp normalize_limit(value) when is_integer(value) and value >= 1 and value <= @max_limit,
    do: {:ok, value}

  defp normalize_limit(_value), do: {:error, :invalid_pagination}

  defp normalize_offset(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp normalize_offset(_value), do: {:error, :invalid_pagination}
end
