defmodule Favn.Manifest.Serializer do
  @moduledoc """
  Canonical manifest serialization and deserialization.

  The serializer produces stable JSON bytes suitable for content hashing and
  persistence boundaries.
  """

  alias Favn.Manifest.Build

  @compile_time_only_keys MapSet.new([
                            "diagnostics",
                            "generated_at",
                            "compiler_version",
                            "build_metadata"
                          ])

  @type error :: {:encode_failed, term()} | {:decode_failed, term()}

  @spec encode_manifest(map() | struct() | Build.t()) :: {:ok, binary()} | {:error, error()}
  def encode_manifest(manifest) do
    {:ok, encode_manifest!(manifest)}
  rescue
    error -> {:error, {:encode_failed, error}}
  end

  @spec encode_manifest!(map() | struct() | Build.t()) :: binary()
  def encode_manifest!(manifest) do
    manifest
    |> manifest_payload()
    |> normalize_value()
    |> encode_json()
  end

  @spec decode_manifest(binary()) :: {:ok, map()} | {:error, error()}
  def decode_manifest(binary) when is_binary(binary) do
    case Jason.decode(binary) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, _other} -> {:error, {:decode_failed, :invalid_manifest_root}}
      {:error, reason} -> {:error, {:decode_failed, reason}}
    end
  end

  @spec manifest_payload(map() | struct() | Build.t()) :: map() | struct() | term()
  def manifest_payload(%Build{manifest: manifest}), do: manifest

  def manifest_payload(manifest) when is_map(manifest) or is_struct(manifest),
    do: manifest

  def manifest_payload(other), do: other

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalize_value(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> normalize_value()
  end

  defp normalize_value({module, name}) when is_atom(module) and is_atom(name) do
    %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}
  end

  defp normalize_value(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.map(&normalize_value/1)
  end

  defp normalize_value(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      normalized_key = normalize_key(key)

      if MapSet.member?(@compile_time_only_keys, normalized_key) do
        acc
      else
        Map.put(acc, normalized_key, normalize_value(value))
      end
    end)
  end

  defp normalize_value(list) when is_list(list), do: Enum.map(list, &normalize_value/1)
  defp normalize_value(value) when is_binary(value), do: value
  defp normalize_value(value) when is_number(value), do: value
  defp normalize_value(true), do: true
  defp normalize_value(false), do: false
  defp normalize_value(nil), do: nil
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: inspect(value)

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key), do: inspect(key)

  defp encode_json(map) when is_map(map) do
    body =
      map
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.map_join(",", fn {key, value} ->
        Jason.encode!(key) <> ":" <> encode_json(value)
      end)

    "{" <> body <> "}"
  end

  defp encode_json(list) when is_list(list) do
    body = Enum.map_join(list, ",", &encode_json/1)
    "[" <> body <> "]"
  end

  defp encode_json(value), do: Jason.encode!(value)
end
