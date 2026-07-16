defmodule Favn.Settings do
  @moduledoc """
  Validation and normalization for static asset and pipeline settings.

  Settings are non-secret values compiled into the manifest. Top-level keys are
  atoms so runtime code can use `ctx.asset.settings.source`; nested map keys are
  normalized to strings so arbitrary external payload shapes remain JSON-safe.

  A settings map may contain at most 128 top-level entries and 64 KiB of
  canonical JSON. Top-level keys use identifier-shaped atoms no longer than 128
  bytes so manifests and recovered run snapshots can rehydrate them safely.
  """

  @max_entries 128
  @max_key_bytes 128
  @max_encoded_bytes 64 * 1024
  @key_pattern ~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/

  @type scalar :: nil | boolean() | number() | String.t()
  @type value :: scalar() | [value()] | %{optional(String.t()) => value()}
  @type t :: %{optional(atom()) => value()}

  @doc """
  Normalizes and validates one settings declaration.

  The top level must be a keyword list or map with atom keys. Values must be
  JSON-like. Non-boolean atom values become strings, and all nested map keys
  become strings.
  """
  @spec normalize!(keyword() | map()) :: t()
  def normalize!(settings) when is_list(settings) do
    unless Keyword.keyword?(settings) do
      raise ArgumentError, "settings must be a keyword list or map with atom keys"
    end

    settings
    |> Map.new()
    |> normalize!()
  end

  def normalize!(settings) when is_map(settings) and not is_struct(settings) do
    validate_entry_count!(settings)

    normalized =
      Map.new(settings, fn
        {key, value} when is_atom(key) ->
          validate_key!(key)
          {key, normalize_value!(value, [key])}

        {key, _value} ->
          raise ArgumentError, "settings keys must be atoms, got: #{inspect(key)}"
      end)

    validate_encoded_size!(normalized)
    normalized
  end

  def normalize!(value) do
    raise ArgumentError,
          "settings must be a keyword list or map with atom keys, got: #{inspect(value)}"
  end

  @doc """
  Shallow-merges normalized settings declarations from left to right.

  A later value, including `nil`, replaces the earlier value at the same key.
  """
  @spec merge_all!([keyword() | map()]) :: t()
  def merge_all!(declarations) when is_list(declarations) do
    declarations
    |> Enum.reduce(%{}, fn declaration, acc -> Map.merge(acc, normalize!(declaration)) end)
    |> validate_entry_count!()
    |> validate_encoded_size!()
  end

  def merge_all!(value) do
    raise ArgumentError, "settings declarations must be a list, got: #{inspect(value)}"
  end

  @doc false
  @spec max_entries() :: pos_integer()
  def max_entries, do: @max_entries

  @doc false
  @spec max_key_bytes() :: pos_integer()
  def max_key_bytes, do: @max_key_bytes

  @doc false
  @spec valid_key_string?(term()) :: boolean()
  def valid_key_string?(value) when is_binary(value) do
    byte_size(value) in 1..@max_key_bytes and Regex.match?(@key_pattern, value)
  end

  def valid_key_string?(_value), do: false

  @doc false
  @spec max_encoded_bytes() :: pos_integer()
  def max_encoded_bytes, do: @max_encoded_bytes

  defp normalize_value!(nil, _path), do: nil
  defp normalize_value!(value, _path) when is_boolean(value), do: value
  defp normalize_value!(value, _path) when is_number(value), do: value
  defp normalize_value!(value, _path) when is_binary(value), do: value
  defp normalize_value!(value, _path) when is_atom(value), do: Atom.to_string(value)

  defp normalize_value!(value, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.map(fn {item, index} -> normalize_value!(item, [index | path]) end)
  end

  defp normalize_value!(value, path) when is_map(value) and not is_struct(value) do
    Enum.reduce(value, %{}, fn
      {key, item}, acc when is_atom(key) or is_binary(key) ->
        normalized_key = to_string(key)

        if Map.has_key?(acc, normalized_key) do
          raise ArgumentError,
                "settings nested map contains duplicate normalized key #{inspect(normalized_key)} at #{format_path(path)}"
        end

        Map.put(acc, normalized_key, normalize_value!(item, [key | path]))

      {key, _item}, _acc ->
        raise ArgumentError,
              "settings nested map keys must be atoms or strings at #{format_path(path)}, got: #{inspect(key)}"
    end)
  end

  defp normalize_value!(value, path) do
    raise ArgumentError,
          "settings values must be JSON-like at #{format_path(path)}, got: #{inspect(value)}"
  end

  defp validate_entry_count!(settings) when map_size(settings) <= @max_entries, do: settings

  defp validate_entry_count!(settings) do
    raise ArgumentError,
          "settings may contain at most #{@max_entries} top-level entries, got: #{map_size(settings)}"
  end

  defp validate_key!(key) do
    key_string = Atom.to_string(key)

    unless valid_key_string?(key_string) do
      raise ArgumentError,
            "settings key #{inspect(key)} must be an identifier of at most #{@max_key_bytes} bytes"
    end
  end

  defp validate_encoded_size!(settings) do
    encoded = Jason.encode!(settings)

    if byte_size(encoded) > @max_encoded_bytes do
      raise ArgumentError,
            "settings canonical JSON may be at most #{@max_encoded_bytes} bytes, got: #{byte_size(encoded)}"
    end

    settings
  end

  defp format_path(path) do
    path
    |> Enum.reverse()
    |> Enum.map_join(".", &to_string/1)
  end
end
