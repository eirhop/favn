defmodule Favn.Dev.EnvFile do
  @moduledoc false

  alias Favn.Dev.Paths

  @key_pattern ~r/^[A-Za-z_][A-Za-z0-9_]*$/

  @type load_result :: %{
          path: Path.t(),
          values: %{optional(String.t()) => String.t()},
          loaded: %{optional(String.t()) => String.t()}
        }

  @spec load(keyword()) :: {:ok, load_result()} | {:error, term()}
  def load(opts \\ []) when is_list(opts) do
    path = env_file_path(opts)

    cond do
      File.exists?(path) ->
        with {:ok, parsed} <- parse_file(path) do
          loaded = put_missing_env(parsed)
          {:ok, %{path: path, values: parsed, loaded: loaded}}
        end

      explicit_env_file?() ->
        {:error, {:env_file_not_found, path}}

      true ->
        {:ok, %{path: path, values: %{}, loaded: %{}}}
    end
  end

  @spec load!(keyword()) :: load_result()
  def load!(opts \\ []) when is_list(opts) do
    case load(opts) do
      {:ok, result} -> result
      {:error, reason} -> raise ArgumentError, message: error_message(reason)
    end
  end

  @spec env_file_path(keyword()) :: Path.t()
  def env_file_path(opts \\ []) when is_list(opts) do
    root_dir = Paths.root_dir(opts)

    case System.get_env("FAVN_ENV_FILE") do
      nil -> Path.join(root_dir, ".env")
      "" -> Path.join(root_dir, ".env")
      path -> expand_env_path(path, root_dir)
    end
  end

  @spec parse_file(Path.t()) :: {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  def parse_file(path) when is_binary(path) do
    case File.read(path) do
      {:ok, contents} -> parse(contents, path)
      {:error, reason} -> {:error, {:env_file_read_failed, path, reason}}
    end
  end

  @spec parse(String.t(), Path.t()) ::
          {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  def parse(contents, source \\ ".env") when is_binary(contents) and is_binary(source) do
    contents
    |> String.split(["\r\n", "\n"], trim: false)
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, %{}}, fn {line, line_number}, {:ok, acc} ->
      case parse_line(line) do
        :skip ->
          {:cont, {:ok, acc}}

        {:ok, key, value} ->
          {:cont, {:ok, Map.put(acc, key, value)}}

        :error ->
          {:halt, {:error, {:invalid_env_line, source, line_number}}}
      end
    end)
  end

  @spec loaded_env(keyword()) :: %{optional(String.t()) => String.t()}
  def loaded_env(opts \\ []) when is_list(opts) do
    case Keyword.get(opts, :env_file_loaded) do
      loaded when is_map(loaded) -> loaded
      _other -> %{}
    end
  end

  defp put_missing_env(values) do
    Enum.reduce(values, %{}, fn {key, value}, loaded ->
      if System.get_env(key) == nil do
        System.put_env(key, value)
        Map.put(loaded, key, value)
      else
        loaded
      end
    end)
  end

  defp parse_line(line) do
    line = String.trim(line)

    cond do
      line == "" ->
        :skip

      String.starts_with?(line, "#") ->
        :skip

      true ->
        line
        |> strip_export()
        |> split_assignment()
    end
  end

  defp strip_export("export " <> rest), do: String.trim_leading(rest)
  defp strip_export(line), do: line

  defp split_assignment(line) do
    case String.split(line, "=", parts: 2) do
      [key, value] -> parse_assignment(String.trim(key), String.trim_leading(value))
      _other -> :error
    end
  end

  defp parse_assignment(key, value) do
    with true <- Regex.match?(@key_pattern, key),
         {:ok, parsed_value} <- parse_value(value) do
      {:ok, key, parsed_value}
    else
      _other -> :error
    end
  end

  defp parse_value(<<"\"", rest::binary>>) do
    with {:ok, value, trailing} <- take_quoted(rest, "\"", ""),
         true <- trailing_comment_or_empty?(trailing) do
      {:ok, value}
    else
      _other -> :error
    end
  end

  defp parse_value(<<"'", rest::binary>>) do
    with {:ok, value, trailing} <- take_single_quoted(rest, ""),
         true <- trailing_comment_or_empty?(trailing) do
      {:ok, value}
    else
      _other -> :error
    end
  end

  defp parse_value(value), do: {:ok, value |> strip_inline_comment() |> String.trim_trailing()}

  defp take_quoted(<<"\"", trailing::binary>>, "\"", acc), do: {:ok, acc, trailing}
  defp take_quoted(<<>>, _quote, _acc), do: :error
  defp take_quoted(<<"\\n", rest::binary>>, quote, acc), do: take_quoted(rest, quote, acc <> "\n")
  defp take_quoted(<<"\\r", rest::binary>>, quote, acc), do: take_quoted(rest, quote, acc <> "\r")
  defp take_quoted(<<"\\t", rest::binary>>, quote, acc), do: take_quoted(rest, quote, acc <> "\t")

  defp take_quoted(<<"\\\"", rest::binary>>, quote, acc),
    do: take_quoted(rest, quote, acc <> "\"")

  defp take_quoted(<<"\\\\", rest::binary>>, quote, acc),
    do: take_quoted(rest, quote, acc <> "\\")

  defp take_quoted(<<char::utf8, rest::binary>>, quote, acc),
    do: take_quoted(rest, quote, acc <> <<char::utf8>>)

  defp take_single_quoted(<<"'", trailing::binary>>, acc), do: {:ok, acc, trailing}
  defp take_single_quoted(<<>>, _acc), do: :error

  defp take_single_quoted(<<char::utf8, rest::binary>>, acc),
    do: take_single_quoted(rest, acc <> <<char::utf8>>)

  defp trailing_comment_or_empty?(trailing) do
    trailing = String.trim_leading(trailing)
    trailing == "" or String.starts_with?(trailing, "#")
  end

  defp strip_inline_comment(value) do
    case :binary.match(value, " #") do
      {index, _length} -> binary_part(value, 0, index)
      :nomatch -> value
    end
  end

  defp expand_env_path(path, root_dir) do
    if Path.type(path) == :absolute do
      Path.expand(path)
    else
      Path.expand(path, root_dir)
    end
  end

  defp explicit_env_file? do
    case System.get_env("FAVN_ENV_FILE") do
      value when is_binary(value) -> String.trim(value) != ""
      _other -> false
    end
  end

  defp error_message({:invalid_env_line, source, line_number}),
    do: "invalid env file line #{line_number} in #{source}"

  defp error_message({:env_file_not_found, path}),
    do: "env file does not exist: #{path}"

  defp error_message({:env_file_read_failed, path, reason}),
    do: "could not read env file #{path}: #{inspect(reason)}"
end
