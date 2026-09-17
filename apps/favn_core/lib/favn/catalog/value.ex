defmodule Favn.Catalog.Value do
  @moduledoc false

  # A public JSON projection, never a decoder for executable values.
  def encode(nil), do: nil
  def encode(value) when is_boolean(value), do: value
  def encode(value) when is_atom(value), do: to_string(value)
  def encode(value) when is_tuple(value), do: value |> Tuple.to_list() |> encode()
  def encode(value) when is_list(value), do: Enum.map(value, &encode/1)
  def encode(%Date{} = value), do: Date.to_iso8601(value)
  def encode(%DateTime{} = value), do: DateTime.to_iso8601(value)
  def encode(%_{} = value), do: value |> Map.from_struct() |> encode()

  def encode(value) when is_map(value),
    do: Map.new(value, fn {k, v} -> {to_string(k), encode(v)} end)

  def encode(value) when is_binary(value) or is_number(value), do: value
end
