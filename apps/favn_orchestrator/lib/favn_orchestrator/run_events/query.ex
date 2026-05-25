defmodule FavnOrchestrator.RunEvents.Query do
  @moduledoc """
  Bounded run-event query parsing for public orchestrator boundaries.
  """

  @default_limit 100
  @max_limit 500

  @type opts :: [after_sequence: non_neg_integer(), limit: pos_integer()]

  @doc """
  Parses HTTP query parameters into bounded run-event read options.
  """
  @spec from_params(map()) :: {:ok, opts()} | {:error, :invalid_opts}
  def from_params(params) when is_map(params) do
    with {:ok, after_sequence} <- optional_non_neg_int(Map.get(params, "after_sequence")),
         {:ok, limit} <- optional_limit(Map.get(params, "limit")) do
      opts = [limit: limit]

      opts =
        if is_nil(after_sequence),
          do: opts,
          else: Keyword.put(opts, :after_sequence, after_sequence)

      {:ok, opts}
    end
  end

  def from_params(_params), do: {:error, :invalid_opts}

  @doc """
  Normalizes facade options and applies the public default limit.
  """
  @spec normalize_opts(keyword()) :: {:ok, opts()} | {:error, :invalid_opts}
  def normalize_opts(opts) when is_list(opts) do
    with {:ok, after_sequence} <- optional_non_neg_int(Keyword.get(opts, :after_sequence)),
         {:ok, limit} <- optional_limit(Keyword.get(opts, :limit)) do
      normalized = [limit: limit]

      normalized =
        if is_nil(after_sequence),
          do: normalized,
          else: Keyword.put(normalized, :after_sequence, after_sequence)

      {:ok, normalized}
    end
  end

  def normalize_opts(_opts), do: {:error, :invalid_opts}

  defp optional_non_neg_int(nil), do: {:ok, nil}
  defp optional_non_neg_int(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp optional_non_neg_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _other -> {:error, :invalid_opts}
    end
  end

  defp optional_non_neg_int(_value), do: {:error, :invalid_opts}

  defp optional_limit(nil), do: {:ok, @default_limit}
  defp optional_limit(value), do: positive_int(value, 1, @max_limit)

  defp positive_int(value, min, max) when is_integer(value) and value >= min and value <= max,
    do: {:ok, value}

  defp positive_int(value, min, max) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= min and int <= max -> {:ok, int}
      _other -> {:error, :invalid_opts}
    end
  end

  defp positive_int(_value, _min, _max), do: {:error, :invalid_opts}
end
