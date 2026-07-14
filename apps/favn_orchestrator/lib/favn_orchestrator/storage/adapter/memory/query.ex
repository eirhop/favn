defmodule FavnOrchestrator.Storage.Adapter.Memory.Query do
  @moduledoc """
  Shared map-backed query primitives for in-memory storage collections.
  """

  alias FavnOrchestrator.Page

  @doc false
  @spec fetch(map(), term()) :: {:ok, term()} | {:error, :not_found}
  def fetch(values, key) do
    case Map.fetch(values, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end

  @doc false
  @spec filter(Enumerable.t(), keyword()) :: list()
  def filter(values, filters) do
    read_filters = Keyword.drop(filters, [:limit, :offset])

    Enum.filter(values, fn value ->
      Enum.all?(read_filters, fn {key, expected} -> same_value?(Map.get(value, key), expected) end)
    end)
  end

  @doc false
  @spec page(Enumerable.t(), keyword(), (term() -> term())) ::
          {:ok, Page.t()} | {:error, term()}
  def page(values, opts, sort_key) when is_function(sort_key, 1) do
    with {:ok, page_opts} <- Page.normalize_opts(opts) do
      rows =
        values
        |> filter(opts)
        |> Enum.sort_by(sort_key)
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))
        |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)

      {:ok, Page.from_fetched(rows, page_opts)}
    end
  end

  @doc false
  @spec validate_filters(keyword(), [atom()]) :: :ok | {:error, {:unsupported_filter, term()}}
  def validate_filters(filters, allowed_keys) do
    filters
    |> Keyword.drop([:limit, :offset])
    |> Enum.find_value(:ok, fn {key, _value} ->
      if key in allowed_keys, do: false, else: {:error, {:unsupported_filter, key}}
    end)
  end

  @doc false
  @spec drop_after(list(), term() | nil, (term() -> term())) :: list()
  def drop_after(rows, nil, _sort_key), do: rows

  def drop_after(rows, after_key, sort_key),
    do: Enum.drop_while(rows, &(sort_key.(&1) <= after_key))

  defp same_value?(actual, expected) when is_atom(actual) and is_binary(expected),
    do: Atom.to_string(actual) == expected

  defp same_value?(actual, expected) when is_binary(actual) and is_atom(expected),
    do: actual == Atom.to_string(expected)

  defp same_value?(actual, expected), do: actual == expected
end
