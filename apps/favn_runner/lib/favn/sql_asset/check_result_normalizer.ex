defmodule Favn.SQLAsset.CheckResultNormalizer do
  @moduledoc false

  alias Favn.SQL.Check
  alias Favn.SQL.Result
  alias Favn.SQLAsset.Error

  @max_metric_columns 32
  @max_text_bytes 4_096
  @max_metrics_bytes 65_536

  @spec normalize(Result.t(), Check.t(), Favn.asset_ref()) ::
          {:ok, boolean(), map()} | {:error, Error.t()}
  def normalize(%Result{} = result, %Check{} = check, asset_ref) do
    with :ok <- validate_columns(result.columns, check, asset_ref),
         {:ok, row} <- one_row(result.rows, check, asset_ref),
         {:ok, passed} <- passed_value(row, check, asset_ref),
         {:ok, metrics} <- metrics(row, result.columns, check, asset_ref) do
      {:ok, passed, metrics}
    end
  end

  defp validate_columns(columns, check, asset_ref) when is_list(columns) do
    names = Enum.map(columns, &to_string/1)
    duplicates = names -- Enum.uniq(names)

    cond do
      duplicates != [] ->
        invalid(check, asset_ref, :duplicate_columns, %{columns: names})

      Enum.count(names, &(&1 == "passed")) != 1 ->
        invalid(check, asset_ref, :invalid_passed_column, %{columns: names})

      length(names) - 1 > @max_metric_columns ->
        invalid(check, asset_ref, :metric_column_limit_exceeded, %{
          actual: length(names) - 1,
          limit: @max_metric_columns
        })

      true ->
        :ok
    end
  end

  defp validate_columns(_columns, check, asset_ref),
    do: invalid(check, asset_ref, :invalid_columns, %{})

  defp one_row([row], _check, _asset_ref) when is_map(row), do: {:ok, row}

  defp one_row(rows, check, asset_ref) when is_list(rows),
    do: invalid(check, asset_ref, :invalid_row_count, %{actual: length(rows), expected: 1})

  defp one_row(_rows, check, asset_ref),
    do: invalid(check, asset_ref, :invalid_rows, %{})

  defp passed_value(row, check, asset_ref) do
    case fetch_column(row, "passed") do
      {:ok, value} when is_boolean(value) -> {:ok, value}
      {:ok, nil} -> invalid(check, asset_ref, :null_passed, %{})
      {:ok, value} -> invalid(check, asset_ref, :non_boolean_passed, %{value: inspect(value)})
      :error -> invalid(check, asset_ref, :missing_passed, %{})
    end
  end

  defp metrics(row, columns, check, asset_ref) do
    columns
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == "passed"))
    |> Enum.reduce_while({:ok, %{}}, fn column, {:ok, acc} ->
      value =
        case fetch_column(row, column) do
          {:ok, value} -> value
          :error -> nil
        end

      case validate_metric(value) do
        :ok -> {:cont, {:ok, Map.put(acc, column, value)}}
        {:error, reason} -> {:halt, invalid(check, asset_ref, reason, %{column: column})}
      end
    end)
    |> case do
      {:ok, metrics} -> validate_metrics_budget(metrics, check, asset_ref)
      {:error, %Error{}} = error -> error
    end
  end

  defp validate_metric(value)
       when is_nil(value) or is_boolean(value) or is_integer(value) or is_float(value),
       do: :ok

  defp validate_metric(%Decimal{}), do: :ok
  defp validate_metric(%Date{}), do: :ok
  defp validate_metric(%Time{}), do: :ok
  defp validate_metric(%NaiveDateTime{}), do: :ok
  defp validate_metric(%DateTime{}), do: :ok

  defp validate_metric(value) when is_binary(value) do
    cond do
      not String.valid?(value) -> {:error, :unsupported_metric_type}
      byte_size(value) > @max_text_bytes -> {:error, :text_metric_limit_exceeded}
      true -> :ok
    end
  end

  defp validate_metric(_value), do: {:error, :unsupported_metric_type}

  defp validate_metrics_budget(metrics, check, asset_ref) do
    with {:ok, encoded} <- Jason.encode(json_metrics(metrics)) do
      bytes = byte_size(encoded)

      if bytes <= @max_metrics_bytes do
        {:ok, metrics}
      else
        invalid(check, asset_ref, :metrics_byte_limit_exceeded, %{
          actual: bytes,
          limit: @max_metrics_bytes
        })
      end
    else
      {:error, _reason} -> invalid(check, asset_ref, :unsupported_metric_type, %{})
    end
  end

  defp json_metrics(metrics),
    do: Map.new(metrics, fn {key, value} -> {key, json_value(value)} end)

  defp json_value(%Decimal{} = value), do: Decimal.to_string(value)
  defp json_value(%Date{} = value), do: Date.to_iso8601(value)
  defp json_value(%Time{} = value), do: Time.to_iso8601(value)
  defp json_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_value(value), do: value

  defp fetch_column(row, name) do
    case Map.fetch(row, name) do
      {:ok, value} -> {:ok, value}
      :error -> fetch_atom_column(row, name)
    end
  end

  defp fetch_atom_column(row, name) do
    Enum.find_value(row, :error, fn
      {key, value} when is_atom(key) -> if Atom.to_string(key) == name, do: {:ok, value}
      _entry -> false
    end)
  end

  defp invalid(check, asset_ref, reason, details) do
    {:error,
     %Error{
       type: :invalid_check_result,
       phase: check.at,
       asset_ref: asset_ref,
       message: "SQL check #{inspect(check.name)} returned an invalid result",
       details: Map.merge(details, %{check: check.name, reason: reason})
     }}
  end
end
