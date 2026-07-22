defmodule Favn.Dev.ComposeEnv do
  @moduledoc """
  Encodes and decodes the literal subset of Docker Compose environment files.

  Values are always single quoted so Compose does not interpolate `$NAME` or
  `${NAME}` expressions. The format preserves quotes, backslashes, `#`, and
  embedded newlines without putting values on the Docker command line.
  """

  @environment_key ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/

  @doc "Encodes a string-keyed environment map in deterministic key order."
  @spec encode(%{required(String.t()) => String.t()}) ::
          {:ok, String.t()} | {:error, :invalid_environment}
  def encode(environment) when is_map(environment) do
    if Enum.all?(environment, &valid_entry?/1) do
      encoded =
        environment
        |> Enum.sort_by(fn {key, _value} -> key end)
        |> Enum.map_join(fn {key, value} -> "#{key}=#{encode_value(value)}\n" end)

      {:ok, encoded}
    else
      {:error, :invalid_environment}
    end
  end

  @doc "Encodes one value as a literal Compose environment-file value."
  @spec encode_value(String.t()) :: String.t()
  def encode_value(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "\\'") <> "'"
  end

  @doc "Decodes environment content produced by `encode/1`."
  @spec decode(String.t()) :: {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  def decode(contents) when is_binary(contents), do: decode_entries(contents, %{})

  @doc "Reads and decodes one generated Compose environment file."
  @spec read(Path.t()) :: {:ok, map()} | {:error, term()}
  def read(path) when is_binary(path) do
    with {:ok, contents} <- File.read(path), do: decode(contents)
  end

  defp decode_entries("", environment), do: {:ok, environment}
  defp decode_entries("\n" <> rest, environment), do: decode_entries(rest, environment)

  defp decode_entries(contents, environment) do
    with {:ok, key, rest} <- take_key(contents),
         {:ok, value, rest} <- take_value(rest, ""),
         false <- Map.has_key?(environment, key) do
      decode_entries(rest, Map.put(environment, key, value))
    else
      true -> {:error, :duplicate_environment_key}
      {:error, _reason} = error -> error
    end
  end

  defp take_key(contents) do
    case :binary.match(contents, "='") do
      {index, 2} when index > 0 ->
        key = binary_part(contents, 0, index)
        rest_size = byte_size(contents) - index - 2
        rest = binary_part(contents, index + 2, rest_size)

        if Regex.match?(@environment_key, key),
          do: {:ok, key, rest},
          else: {:error, :invalid_environment_key}

      _missing ->
        {:error, :invalid_environment_entry}
    end
  end

  defp take_value("\\'" <> rest, value), do: take_value(rest, value <> "'")

  defp take_value("'\n" <> rest, value), do: {:ok, value, rest}
  defp take_value("'", value), do: {:ok, value, ""}
  defp take_value("", _value), do: {:error, :unterminated_environment_value}

  defp take_value(<<codepoint::utf8, rest::binary>>, value),
    do: take_value(rest, value <> <<codepoint::utf8>>)

  defp valid_entry?({key, value}) when is_binary(key) and is_binary(value),
    do: Regex.match?(@environment_key, key) and not String.contains?(value, <<0>>)

  defp valid_entry?(_entry), do: false
end
