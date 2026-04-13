defmodule Favn.Storage.Postgres.TermJSON do
  @moduledoc false

  @spec encode(term()) :: map()
  def encode(value), do: do_encode(value)

  @spec decode(term()) :: {:ok, term()} | {:error, term()}
  def decode(value), do: do_decode(value)

  defp do_encode(nil), do: %{"$t" => "nil"}
  defp do_encode(true), do: %{"$t" => "bool", "v" => true}
  defp do_encode(false), do: %{"$t" => "bool", "v" => false}
  defp do_encode(value) when is_integer(value), do: %{"$t" => "int", "v" => value}
  defp do_encode(value) when is_float(value), do: %{"$t" => "float", "v" => value}
  defp do_encode(value) when is_binary(value), do: %{"$t" => "string", "v" => value}
  defp do_encode(value) when is_atom(value), do: %{"$t" => "atom", "v" => Atom.to_string(value)}

  defp do_encode(%DateTime{} = value) do
    %{"$t" => "datetime", "v" => DateTime.to_iso8601(value)}
  end

  defp do_encode(value) when is_struct(value) do
    value
    |> Map.from_struct()
    |> Enum.map(fn {key, item} -> [do_encode(key), do_encode(item)] end)
    |> Enum.sort_by(&inspect/1)
    |> then(fn pairs ->
      %{
        "$t" => "struct",
        "module" => value.__struct__ |> Atom.to_string(),
        "v" => pairs
      }
    end)
  end

  defp do_encode(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> [do_encode(key), do_encode(item)] end)
    |> Enum.sort_by(&inspect/1)
    |> then(fn pairs -> %{"$t" => "map", "v" => pairs} end)
  end

  defp do_encode(value) when is_list(value) do
    %{"$t" => "list", "v" => Enum.map(value, &do_encode/1)}
  end

  defp do_encode(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&do_encode/1)
    |> then(fn parts -> %{"$t" => "tuple", "v" => parts} end)
  end

  defp do_encode(value), do: %{"$t" => "inspect", "v" => inspect(value)}

  defp do_decode(%{"$t" => "nil"}), do: {:ok, nil}
  defp do_decode(%{"$t" => "bool", "v" => value}) when is_boolean(value), do: {:ok, value}
  defp do_decode(%{"$t" => "int", "v" => value}) when is_integer(value), do: {:ok, value}
  defp do_decode(%{"$t" => "float", "v" => value}) when is_float(value), do: {:ok, value}
  defp do_decode(%{"$t" => "string", "v" => value}) when is_binary(value), do: {:ok, value}

  defp do_decode(%{"$t" => "atom", "v" => value}) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp do_decode(%{"$t" => "datetime", "v" => value}) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_decode(%{"$t" => "list", "v" => value}) when is_list(value) do
    decode_list(value, [])
  end

  defp do_decode(%{"$t" => "tuple", "v" => value}) when is_list(value) do
    with {:ok, decoded} <- decode_list(value, []) do
      {:ok, List.to_tuple(decoded)}
    end
  end

  defp do_decode(%{"$t" => "map", "v" => value}) when is_list(value) do
    decode_pairs(value, %{})
  end

  defp do_decode(%{"$t" => "struct", "module" => module_name, "v" => value})
       when is_binary(module_name) and is_list(value) do
    with {:ok, module} <- decode_module(module_name),
         {:ok, decoded} <- decode_pairs(value, %{}) do
      {:ok, struct(module, decoded)}
    end
  end

  defp do_decode(%{"$t" => "inspect", "v" => value}), do: {:ok, value}
  defp do_decode(other), do: {:error, {:invalid_encoded_term, other}}

  defp decode_list([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_list([head | tail], acc) do
    with {:ok, value} <- do_decode(head) do
      decode_list(tail, [value | acc])
    end
  end

  defp decode_pairs([], acc), do: {:ok, acc}

  defp decode_pairs([[encoded_key, encoded_value] | tail], acc) do
    with {:ok, key} <- do_decode(encoded_key),
         {:ok, value} <- do_decode(encoded_value) do
      decode_pairs(tail, Map.put(acc, key, value))
    end
  end

  defp decode_pairs([other | _tail], _acc), do: {:error, {:invalid_encoded_pair, other}}

  defp decode_module(module_name) do
    module = String.to_existing_atom(module_name)

    if Code.ensure_loaded?(module) do
      {:ok, module}
    else
      {:error, {:unknown_struct_module, module_name}}
    end
  rescue
    ArgumentError -> {:error, {:unknown_struct_module, module_name}}
  end
end
