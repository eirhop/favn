defmodule FavnStoragePostgres.RuntimeInputKeys do
  @moduledoc false

  @key_bytes 32

  @spec current() :: {:ok, {pos_integer(), binary()}} | {:error, term()}
  def current do
    version = configured_current_version()

    with true <- is_integer(version) and version > 0,
         {:ok, key} <- fetch(version) do
      {:ok, {version, key}}
    else
      false -> {:error, :invalid_runtime_input_pin_key_version}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch(pos_integer()) :: {:ok, binary()} | {:error, term()}
  def fetch(version) when is_integer(version) and version > 0 do
    case Map.fetch(configured_keys(), version) do
      {:ok, key} -> decode_key(key)
      :error -> {:error, :runtime_input_pin_key_unavailable}
    end
  end

  def fetch(_version), do: {:error, :invalid_runtime_input_pin_key_version}

  @spec diagnostics() :: map()
  def diagnostics do
    %{
      configured?: configured_keys() != %{},
      current_version: configured_current_version(),
      retained_versions: configured_keys() |> Map.keys() |> Enum.sort()
    }
  end

  defp configured_current_version do
    Application.get_env(:favn_storage_postgres, :runtime_input_pin_current_key_version) ||
      env_integer("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION") || 1
  end

  defp configured_keys do
    configured = Application.get_env(:favn_storage_postgres, :runtime_input_pin_keys, %{})

    configured
    |> Enum.into(%{}, fn {version, key} -> {normalize_version(version), key} end)
    |> maybe_put_environment_key()
  rescue
    _error -> %{}
  end

  defp maybe_put_environment_key(keys) do
    case System.get_env("FAVN_RUNTIME_INPUT_PIN_KEY") do
      key when is_binary(key) and key != "" ->
        Map.put_new(keys, configured_current_version(), key)

      _missing ->
        keys
    end
  end

  defp normalize_version(version) when is_integer(version), do: version

  defp normalize_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {parsed, ""} -> parsed
      _invalid -> version
    end
  end

  defp normalize_version(version), do: version

  defp decode_key(key) when is_binary(key) and byte_size(key) == @key_bytes, do: {:ok, key}

  defp decode_key(encoded) when is_binary(encoded) do
    with {:ok, key} <- Base.decode64(encoded),
         @key_bytes <- byte_size(key) do
      {:ok, key}
    else
      _invalid -> {:error, :invalid_runtime_input_pin_key}
    end
  end

  defp decode_key(_key), do: {:error, :invalid_runtime_input_pin_key}

  defp env_integer(name) do
    case System.get_env(name) do
      value when is_binary(value) ->
        case Integer.parse(value) do
          {parsed, ""} -> parsed
          _invalid -> nil
        end

      _missing ->
        nil
    end
  end
end
