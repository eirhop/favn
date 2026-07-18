defmodule FavnOrchestrator.API.Filters do
  @moduledoc """
  Validates bounded query options accepted by the private HTTP API.
  """

  @status_filters %{
    "pending" => :pending,
    "running" => :running,
    "ok" => :ok,
    "partial" => :partial,
    "error" => :error,
    "cancelled" => :cancelled,
    "timed_out" => :timed_out
  }

  @default_limit 100
  @max_limit 500
  @max_sample_limit 20

  @doc "Builds bounded filters for the run read model."
  @spec runs(map()) :: {:ok, keyword()} | {:error, :invalid_filter}
  def runs(params) when is_map(params) do
    with {:ok, limit} <- integer(Map.get(params, "limit", @default_limit), 1, @max_limit),
         {:ok, opts} <- put_status([limit: limit], Map.get(params, "status")) do
      {:ok, opts}
    else
      {:error, :invalid_pagination} -> {:error, :invalid_filter}
      {:error, _reason} = error -> error
    end
  end

  @doc "Validates and caps relation-inspection sample size."
  @spec inspection_sample_limit(map()) ::
          {:ok, non_neg_integer()} | {:error, :invalid_sample_limit}
  def inspection_sample_limit(params) when is_map(params) do
    case Map.get(params, "sample_limit") || Map.get(params, "limit") || @max_sample_limit do
      value when is_integer(value) and value >= 0 -> {:ok, min(value, @max_sample_limit)}
      value when is_binary(value) -> parse_sample_limit(value)
      _invalid -> {:error, :invalid_sample_limit}
    end
  end

  defp parse_sample_limit(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> {:ok, min(integer, @max_sample_limit)}
      _invalid -> {:error, :invalid_sample_limit}
    end
  end

  defp integer(value, min, max) when is_integer(value), do: validate_integer(value, min, max)

  defp integer(value, min, max) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> validate_integer(integer, min, max)
      _invalid -> {:error, :invalid_pagination}
    end
  end

  defp integer(_value, _min, _max), do: {:error, :invalid_pagination}

  defp validate_integer(value, min, max) when value >= min and value <= max,
    do: {:ok, value}

  defp validate_integer(_value, _min, _max), do: {:error, :invalid_pagination}

  defp put_status(opts, value) when value in [nil, ""], do: {:ok, opts}

  defp put_status(opts, value) when is_binary(value) do
    case Map.fetch(@status_filters, value) do
      {:ok, status} -> {:ok, Keyword.put(opts, :status, status)}
      :error -> {:error, :invalid_filter}
    end
  end

  defp put_status(_opts, _value), do: {:error, :invalid_filter}
end
