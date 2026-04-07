defmodule Favn.Window.Validate do
  @moduledoc false

  @type kind :: :hour | :day | :month

  @spec kind(kind()) :: :ok | {:error, term()}
  def kind(kind) when kind in [:hour, :day, :month], do: :ok
  def kind(kind), do: {:error, {:invalid_kind, kind}}

  @spec timezone(String.t()) :: :ok | {:error, term()}
  def timezone(timezone) when is_binary(timezone) and byte_size(timezone) > 0 do
    case DateTime.now(timezone) do
      {:ok, _dt} -> :ok
      {:error, _reason} -> {:error, {:invalid_timezone, timezone}}
    end
  end

  def timezone(timezone), do: {:error, {:invalid_timezone, timezone}}

  @spec strict_keyword_opts(keyword(), [atom()]) :: :ok | {:error, term()}
  def strict_keyword_opts(opts, allowed_keys) when is_list(opts) and is_list(allowed_keys) do
    with :ok <- validate_keyword_list(opts),
         :ok <- validate_duplicate_keys(opts),
         :ok <- validate_allowed_keys(opts, allowed_keys) do
      :ok
    end
  end

  def strict_keyword_opts(opts, _allowed_keys), do: {:error, {:invalid_opts, opts}}

  defp validate_keyword_list(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, {:invalid_opts, opts}}
  end

  defp validate_duplicate_keys(opts) do
    case opts
         |> Keyword.keys()
         |> Enum.frequencies()
         |> Enum.find(fn {_k, count} -> count > 1 end) do
      nil -> :ok
      {key, _count} -> {:error, {:duplicate_opt, key}}
    end
  end

  defp validate_allowed_keys(opts, allowed_keys) do
    case Enum.find(opts, fn {key, _value} -> key not in allowed_keys end) do
      nil -> :ok
      {key, _value} -> {:error, {:unknown_opt, key}}
    end
  end
end
