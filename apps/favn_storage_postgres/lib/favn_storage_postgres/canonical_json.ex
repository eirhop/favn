defmodule FavnStoragePostgres.CanonicalJSON do
  @moduledoc false

  @spec encode(term()) :: {:ok, binary()} | {:error, term()}
  def encode(value) do
    value
    |> ordered()
    |> Jason.encode()
  rescue
    error -> {:error, error}
  end

  @spec hash(term()) :: {:ok, binary()} | {:error, term()}
  def hash(value) do
    with {:ok, encoded} <- encode(value) do
      {:ok, :crypto.hash(:sha256, encoded)}
    end
  end

  defp ordered(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp ordered(%_{} = struct), do: struct |> Map.from_struct() |> ordered()

  defp ordered(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), ordered(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Jason.OrderedObject.new()
  end

  defp ordered(values) when is_list(values), do: Enum.map(values, &ordered/1)
  defp ordered(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  defp ordered(value), do: value
end
