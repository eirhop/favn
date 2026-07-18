defmodule FavnOrchestrator.Idempotency do
  @moduledoc """
  Canonical hashes for store-owned atomic command idempotency.
  """

  @spec key_hash(String.t()) :: String.t()
  def key_hash(key) when is_binary(key) do
    key
    |> sha256()
    |> Base.encode16(case: :lower)
  end

  @spec request_fingerprint(term()) :: String.t()
  def request_fingerprint(input) do
    input
    |> canonicalize()
    |> Jason.encode!()
    |> sha256()
    |> Base.encode16(case: :lower)
  end

  defp sha256(value) when is_binary(value), do: :crypto.hash(:sha256, value)

  defp canonicalize(nil), do: %{"__type__" => "null"}

  defp canonicalize(value) when is_boolean(value),
    do: %{"__type__" => "boolean", "value" => value}

  defp canonicalize(value) when is_binary(value), do: %{"__type__" => "string", "value" => value}

  defp canonicalize(value) when is_integer(value),
    do: %{"__type__" => "integer", "value" => value}

  defp canonicalize(value) when is_float(value), do: %{"__type__" => "float", "value" => value}

  defp canonicalize(%DateTime{} = value) do
    %{"__type__" => "datetime", "value" => DateTime.to_iso8601(value)}
  end

  defp canonicalize(value) when is_map(value) do
    entries =
      value
      |> Enum.map(fn {key, val} -> [to_string(key), canonicalize(val)] end)
      |> Enum.sort_by(fn [key, _val] -> key end)

    %{"__type__" => "map", "entries" => entries}
  end

  defp canonicalize(value) when is_list(value) do
    %{"__type__" => "list", "items" => Enum.map(value, &canonicalize/1)}
  end

  defp canonicalize(value) when is_tuple(value), do: value |> Tuple.to_list() |> canonicalize()

  defp canonicalize(value) when is_atom(value),
    do: %{"__type__" => "atom", "value" => Atom.to_string(value)}
end
