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

  @doc """
  Encodes an arbitrary JSON-compatible value with Favn's canonical ordering.

  Unlike `encode_manifest/1`, this function does not remove manifest
  compile-time metadata. It is intended for other immutable Favn contracts
  that need stable bytes for hashing or persistence.
  """
  @spec encode_canonical(term()) :: {:ok, binary()} | {:error, error()}
  def encode_canonical(value) do
    {:ok, encode_canonical!(value)}
  rescue
    error -> {:error, {:encode_failed, error}}
  end

  @doc """
  Encodes an arbitrary value with canonical ordering, raising on invalid JSON
  values.
  """
  @spec encode_canonical!(term()) :: binary()
  def encode_canonical!(value) do
    value
    |> normalize_canonical_value()
    |> encode_json()
  end

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
    |> normalize_value(@compile_time_only_keys)
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

  defp normalize_value(%DateTime{} = datetime, _dropped_keys), do: DateTime.to_iso8601(datetime)

  defp normalize_value(%MapSet{} = set, dropped_keys) do
    set
    |> MapSet.to_list()
    |> Enum.sort_by(&inspect/1)
    |> normalize_value(dropped_keys)
  end

  defp normalize_value(%_{} = struct, dropped_keys) do
    struct
    |> Map.from_struct()
    |> normalize_value(dropped_keys)
  end

  defp normalize_value(tuple, dropped_keys) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.map(&normalize_value(&1, dropped_keys))
  end

  defp normalize_value(map, dropped_keys) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      normalized_key = normalize_key(key)

      if MapSet.member?(dropped_keys, normalized_key) do
        acc
      else
        Map.put(acc, normalized_key, normalize_value(value, dropped_keys))
      end
    end)
  end

  defp normalize_value(list, dropped_keys) when is_list(list),
    do: Enum.map(list, &normalize_value(&1, dropped_keys))

  defp normalize_value(value, _dropped_keys) when is_binary(value), do: value
  defp normalize_value(value, _dropped_keys) when is_number(value), do: value
  defp normalize_value(true, _dropped_keys), do: true
  defp normalize_value(false, _dropped_keys), do: false
  defp normalize_value(nil, _dropped_keys), do: nil
  defp normalize_value(value, _dropped_keys) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value, _dropped_keys), do: inspect(value)

  defp normalize_canonical_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalize_canonical_value(map) when is_map(map) and not is_struct(map) do
    Enum.reduce(map, %{}, fn {key, value}, acc ->
      normalized_key = normalize_canonical_key(key)

      if Map.has_key?(acc, normalized_key) do
        raise ArgumentError, "canonical JSON received duplicate normalized key #{normalized_key}"
      end

      Map.put(acc, normalized_key, normalize_canonical_value(value))
    end)
  end

  defp normalize_canonical_value(list) when is_list(list),
    do: Enum.map(list, &normalize_canonical_value/1)

  defp normalize_canonical_value(value) when is_binary(value), do: value
  defp normalize_canonical_value(value) when is_number(value), do: value
  defp normalize_canonical_value(true), do: true
  defp normalize_canonical_value(false), do: false
  defp normalize_canonical_value(nil), do: nil
  defp normalize_canonical_value(value) when is_atom(value), do: Atom.to_string(value)

  defp normalize_canonical_value(_value) do
    raise ArgumentError, "canonical JSON accepts only explicit JSON-compatible values"
  end

  defp normalize_canonical_key(key) when is_binary(key), do: key
  defp normalize_canonical_key(key) when is_atom(key), do: Atom.to_string(key)

  defp normalize_canonical_key(_key) do
    raise ArgumentError, "canonical JSON object keys must be strings or atoms"
  end

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
