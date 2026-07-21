defmodule Favn.RuntimeInput.KeyringConfig do
  @moduledoc """
  Parses the bounded runtime-input encryption keyring from process environment.

  The returned map contains secret key material and must remain process-local.
  Use `diagnostics/1` when reporting configuration state.
  """

  @max_keys 32
  @max_encoded_bytes 8 * 1_024
  @key_bytes 32

  @type t :: %{
          required(:keys) => %{pos_integer() => binary()},
          required(:current_version) => pos_integer()
        }

  @doc "Parses the keyring from the current process environment."
  @spec from_env() :: {:ok, t()} | {:error, term()}
  def from_env, do: from_env(System.get_env())

  @doc "Parses and validates the keyring from an explicit environment map."
  @spec from_env(map()) :: {:ok, t()} | {:error, term()}
  def from_env(env) when is_map(env) do
    with {:ok, encoded_keys} <- required_secret(env, "FAVN_RUNTIME_INPUT_PIN_KEYS"),
         {:ok, keys} <- decode_keys(encoded_keys),
         {:ok, current_version} <- current_version(env),
         true <- Map.has_key?(keys, current_version) do
      {:ok, %{keys: keys, current_version: current_version}}
    else
      false ->
        {:error,
         {:invalid_env, "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", :redacted,
          "version present in FAVN_RUNTIME_INPUT_PIN_KEYS"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Returns keyring metadata without exposing key material."
  @spec diagnostics(t()) :: %{
          current_version: pos_integer(),
          retained_versions: [pos_integer()]
        }
  def diagnostics(%{keys: keys, current_version: current_version}) do
    %{
      current_version: current_version,
      retained_versions: keys |> Map.keys() |> Enum.sort()
    }
  end

  defp decode_keys(encoded_keys) do
    if byte_size(encoded_keys) <= @max_encoded_bytes do
      case Jason.decode(encoded_keys, objects: :ordered_objects) do
        {:ok, %Jason.OrderedObject{values: keys}}
        when length(keys) > 0 and length(keys) <= @max_keys ->
          decode_key_entries(keys)

        _invalid ->
          invalid_keys(:invalid_keyring)
      end
    else
      invalid_keys(:invalid_keyring)
    end
  end

  defp decode_key_entries(keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn {encoded_version, encoded_key}, {:ok, decoded} ->
      with {:ok, version} <- decode_version(encoded_version),
           {:ok, key} <- decode_key(encoded_key),
           false <- Map.has_key?(decoded, version) do
        {:cont, {:ok, Map.put(decoded, version, key)}}
      else
        true -> {:halt, invalid_keys(:duplicate_version)}
        {:error, reason} -> {:halt, invalid_keys(reason)}
      end
    end)
  end

  defp current_version(env) do
    case env |> Map.get("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "1") |> parse_integer() do
      {:ok, version} when version > 0 ->
        {:ok, version}

      _invalid ->
        {:error,
         {:invalid_env, "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION",
          Map.get(env, "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "1"), ">= 1"}}
    end
  end

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} -> {:ok, integer}
      _invalid -> :error
    end
  end

  defp parse_integer(_value), do: :error

  defp decode_version(encoded_version) when is_binary(encoded_version) do
    case Integer.parse(encoded_version) do
      {version, ""} when version > 0 ->
        if Integer.to_string(version) == encoded_version,
          do: {:ok, version},
          else: {:error, :invalid_version}

      _invalid ->
        {:error, :invalid_version}
    end
  end

  defp decode_version(_encoded_version), do: {:error, :invalid_version}

  defp decode_key(key) when is_binary(key) and byte_size(key) == @key_bytes,
    do: {:ok, key}

  defp decode_key(encoded) when is_binary(encoded) do
    case Base.decode64(encoded) do
      {:ok, key} when byte_size(key) == @key_bytes -> {:ok, key}
      _invalid -> {:error, :invalid_key}
    end
  end

  defp decode_key(_encoded), do: {:error, :invalid_key}

  defp required_secret(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: {:error, {:missing_env, name}}, else: {:ok, value}

      _other ->
        {:error, {:missing_env, name}}
    end
  end

  defp invalid_keys(reason),
    do: {:error, {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEYS", reason}}
end
