defmodule Favn.Contracts.RunnerTask.OpenData do
  @moduledoc false

  @default_max_bytes 1_048_576
  @default_max_depth 16
  @default_max_nodes 10_000

  @type reason ::
          :duplicate_key
          | :invalid_map_key
          | :too_deep
          | :too_large
          | :too_many_values
          | :unsupported_value

  @spec normalize(term(), keyword()) :: {:ok, term()} | {:error, reason()}
  def normalize(value, opts \\ []) when is_list(opts) do
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)
    max_depth = Keyword.get(opts, :max_depth, @default_max_depth)
    max_nodes = Keyword.get(opts, :max_nodes, @default_max_nodes)
    tuples = Keyword.get(opts, :tuples, :reject)

    with true <- positive_integer?(max_bytes),
         true <- positive_integer?(max_depth),
         true <- positive_integer?(max_nodes),
         true <- tuples in [:reject, :lists],
         {:ok, normalized, _remaining} <-
           normalize_value(value, max_depth, max_nodes, tuples),
         true <- :erlang.external_size(normalized, [:deterministic]) <= max_bytes do
      {:ok, normalized}
    else
      false -> {:error, :too_large}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _error -> {:error, :unsupported_value}
  catch
    _kind, _reason -> {:error, :unsupported_value}
  end

  defp normalize_value(_value, depth, _nodes, _tuples) when depth < 0,
    do: {:error, :too_deep}

  defp normalize_value(_value, _depth, nodes, _tuples) when nodes <= 0,
    do: {:error, :too_many_values}

  defp normalize_value(value, _depth, nodes, _tuples)
       when is_number(value) or is_boolean(value) or is_nil(value) or is_binary(value),
       do: {:ok, value, nodes - 1}

  defp normalize_value(value, _depth, nodes, _tuples) when is_atom(value),
    do: {:ok, Atom.to_string(value), nodes - 1}

  defp normalize_value(%module{} = value, _depth, nodes, _tuples)
       when module in [Date, Time, NaiveDateTime, DateTime, Decimal],
       do: {:ok, value, nodes - 1}

  defp normalize_value(%_module{}, _depth, _nodes, _tuples),
    do: {:error, :unsupported_value}

  defp normalize_value(value, depth, nodes, tuples) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}, nodes - 1}, fn {key, child}, {:ok, acc, left} ->
      with {:ok, normalized_key} <- normalize_key(key),
           false <- Map.has_key?(acc, normalized_key),
           {:ok, normalized_child, remaining} <-
             normalize_value(child, depth - 1, left, tuples) do
        {:cont, {:ok, Map.put(acc, normalized_key, normalized_child), remaining}}
      else
        true -> {:halt, {:error, :duplicate_key}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_value(value, depth, nodes, tuples) when is_list(value) do
    Enum.reduce_while(value, {:ok, [], nodes - 1}, fn child, {:ok, acc, left} ->
      case normalize_value(child, depth - 1, left, tuples) do
        {:ok, normalized, remaining} -> {:cont, {:ok, [normalized | acc], remaining}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_values()
  end

  defp normalize_value(value, depth, nodes, :lists) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> normalize_value(depth, nodes, :lists)
  end

  defp normalize_value(value, _depth, _nodes, _tuples)
       when is_tuple(value) or is_pid(value) or is_port(value) or is_reference(value) or
              is_function(value),
       do: {:error, :unsupported_value}

  defp normalize_value(_value, _depth, _nodes, _tuples),
    do: {:error, :unsupported_value}

  defp normalize_key(key) when is_binary(key), do: {:ok, key}
  defp normalize_key(key) when is_atom(key), do: {:ok, Atom.to_string(key)}
  defp normalize_key(_key), do: {:error, :invalid_map_key}

  defp reverse_values({:ok, values, remaining}), do: {:ok, Enum.reverse(values), remaining}
  defp reverse_values({:error, reason}), do: {:error, reason}

  defp positive_integer?(value), do: is_integer(value) and value > 0
end
