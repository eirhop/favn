defmodule Favn.SQL.SessionScript.Template do
  @moduledoc """
  Value-only `@param` rendering for trusted SQL session scripts.

  The scanner recognizes parameters only in SQL code, not inside quoted
  strings, quoted identifiers, line comments, nested block comments, or
  dollar-quoted strings. Parameters are rendered as typed SQL literals and can
  never inject identifiers, keywords, clauses, or additional statements.
  """

  @max_param_bytes 1_048_576
  @max_rendered_bytes 4_194_304
  @redacted "[REDACTED]"

  @type segment :: {:text, String.t()} | {:param, String.t()}

  @type rendered :: %{
          statement: String.t(),
          safe_statement: String.t(),
          params: [String.t()],
          secret_values: [String.t()]
        }

  @doc """
  Renders one SQL script with exact configured parameters.

  Missing parameters and configured-but-unused parameters are rejected to keep
  resource configuration deterministic and typo-safe.
  """
  @spec render(String.t(), map(), MapSet.t(String.t())) ::
          {:ok, rendered()} | {:error, term()}
  def render(sql, params, secret_params \\ MapSet.new())

  def render(sql, params, %MapSet{} = secret_params) when is_binary(sql) and is_map(params) do
    with {:ok, segments} <- scan(sql),
         names <- segment_parameter_names(segments),
         normalized_params <- normalize_params(params),
         :ok <- validate_exact_params(names, normalized_params),
         {:ok, statement, safe_statement} <-
           render_segments(segments, normalized_params, secret_params),
         :ok <- validate_rendered_size(statement) do
      {:ok,
       %{
         statement: statement,
         safe_statement: safe_statement,
         params: names,
         secret_values: secret_values(normalized_params, secret_params)
       }}
    end
  end

  def render(_sql, _params, _secret_params), do: {:error, :invalid_script_template_input}

  @doc false
  @spec parameter_names(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def parameter_names(sql) when is_binary(sql) do
    with {:ok, segments} <- scan(sql), do: {:ok, segment_parameter_names(segments)}
  end

  defp scan(sql), do: scan(sql, :code, [], [])

  defp scan(<<>>, _state, text, segments),
    do: {:ok, segments |> flush_text(text) |> Enum.reverse()}

  defp scan(<<"--", rest::binary>>, :code, text, segments),
    do: scan(rest, :line_comment, ["--" | text], segments)

  defp scan(<<"/*", rest::binary>>, :code, text, segments),
    do: scan(rest, {:block_comment, 1}, ["/*" | text], segments)

  defp scan(<<"'", rest::binary>>, :code, text, segments),
    do: scan(rest, :single_quote, ["'" | text], segments)

  defp scan(<<"\"", rest::binary>>, :code, text, segments),
    do: scan(rest, :double_quote, ["\"" | text], segments)

  defp scan(<<"@", rest::binary>>, :code, text, segments) do
    case take_parameter(rest) do
      {nil, _rest} ->
        scan(rest, :code, ["@" | text], segments)

      {name, tail} ->
        segments = flush_text(segments, text)
        scan(tail, :code, [], [{:param, name} | segments])
    end
  end

  defp scan(<<"$", _rest::binary>> = binary, :code, text, segments) do
    case dollar_delimiter(binary) do
      nil -> take_codepoint(binary, :code, text, segments)
      delimiter ->
        delimiter_size = byte_size(delimiter)
        <<_prefix::binary-size(^delimiter_size), rest::binary>> = binary
        scan(rest, {:dollar_quote, delimiter}, [delimiter | text], segments)
    end
  end

  defp scan(binary, :code, text, segments),
    do: take_codepoint(binary, :code, text, segments)

  defp scan(<<"\n", rest::binary>>, :line_comment, text, segments),
    do: scan(rest, :code, ["\n" | text], segments)

  defp scan(binary, :line_comment, text, segments),
    do: take_codepoint(binary, :line_comment, text, segments)

  defp scan(<<"''", rest::binary>>, :single_quote, text, segments),
    do: scan(rest, :single_quote, ["''" | text], segments)

  defp scan(<<"'", rest::binary>>, :single_quote, text, segments),
    do: scan(rest, :code, ["'" | text], segments)

  defp scan(binary, :single_quote, text, segments),
    do: take_codepoint(binary, :single_quote, text, segments)

  defp scan(<<"\"\"", rest::binary>>, :double_quote, text, segments),
    do: scan(rest, :double_quote, ["\"\"" | text], segments)

  defp scan(<<"\"", rest::binary>>, :double_quote, text, segments),
    do: scan(rest, :code, ["\"" | text], segments)

  defp scan(binary, :double_quote, text, segments),
    do: take_codepoint(binary, :double_quote, text, segments)

  defp scan(<<"/*", rest::binary>>, {:block_comment, depth}, text, segments),
    do: scan(rest, {:block_comment, depth + 1}, ["/*" | text], segments)

  defp scan(<<"*/", rest::binary>>, {:block_comment, 1}, text, segments),
    do: scan(rest, :code, ["*/" | text], segments)

  defp scan(<<"*/", rest::binary>>, {:block_comment, depth}, text, segments),
    do: scan(rest, {:block_comment, depth - 1}, ["*/" | text], segments)

  defp scan(binary, {:block_comment, _depth} = state, text, segments),
    do: take_codepoint(binary, state, text, segments)

  defp scan(binary, {:dollar_quote, delimiter} = state, text, segments) do
    if String.starts_with?(binary, delimiter) do
      delimiter_size = byte_size(delimiter)
      <<_prefix::binary-size(^delimiter_size), rest::binary>> = binary
      scan(rest, :code, [delimiter | text], segments)
    else
      take_codepoint(binary, state, text, segments)
    end
  end

  defp take_codepoint(<<codepoint::utf8, rest::binary>>, state, text, segments),
    do: scan(rest, state, [<<codepoint::utf8>> | text], segments)

  defp take_codepoint(<<byte, rest::binary>>, state, text, segments),
    do: scan(rest, state, [<<byte>> | text], segments)

  defp flush_text(segments, []), do: segments

  defp flush_text(segments, text),
    do: [{:text, text |> Enum.reverse() |> IO.iodata_to_binary()} | segments]

  defp take_parameter(rest) do
    case Regex.run(~r/^([a-z_][A-Za-z0-9_]*)/, rest, capture: :all_but_first) do
      [name] -> {name, binary_part(rest, byte_size(name), byte_size(rest) - byte_size(name))}
      _ -> {nil, rest}
    end
  end

  defp dollar_delimiter(binary) do
    case Regex.run(~r/^(\$\$|\$[A-Za-z_][A-Za-z0-9_]*\$)/, binary, capture: :all_but_first) do
      [delimiter] -> delimiter
      _ -> nil
    end
  end

  defp segment_parameter_names(segments) do
    segments
    |> Enum.flat_map(fn
      {:param, name} -> [name]
      {:text, _text} -> []
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_params(params) do
    Map.new(params, fn {name, value} -> {to_string(name), value} end)
  end

  defp validate_exact_params(names, params) do
    required = MapSet.new(names)
    configured = params |> Map.keys() |> MapSet.new()
    missing = required |> MapSet.difference(configured) |> MapSet.to_list() |> Enum.sort()
    unused = configured |> MapSet.difference(required) |> MapSet.to_list() |> Enum.sort()

    cond do
      missing != [] -> {:error, {:missing_script_parameters, missing}}
      unused != [] -> {:error, {:unused_script_parameters, unused}}
      true -> :ok
    end
  end

  defp render_segments(segments, params, secret_params) do
    Enum.reduce_while(segments, {:ok, [], []}, fn
      {:text, text}, {:ok, statement, safe_statement} ->
        {:cont, {:ok, [text | statement], [text | safe_statement]}}

      {:param, name}, {:ok, statement, safe_statement} ->
        value = Map.fetch!(params, name)

        case literal(value) do
          {:ok, encoded} ->
            safe = if MapSet.member?(secret_params, name), do: quote_string(@redacted), else: encoded
            {:cont, {:ok, [encoded | statement], [safe | safe_statement]}}

          {:error, reason} ->
            {:halt, {:error, {:invalid_script_parameter, name, reason}}}
        end
    end)
    |> case do
      {:ok, statement, safe_statement} ->
        {:ok, statement |> Enum.reverse() |> IO.iodata_to_binary(),
         safe_statement |> Enum.reverse() |> IO.iodata_to_binary()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp literal(nil), do: {:ok, "NULL"}
  defp literal(true), do: {:ok, "TRUE"}
  defp literal(false), do: {:ok, "FALSE"}
  defp literal(value) when is_integer(value), do: {:ok, Integer.to_string(value)}

  defp literal(value) when is_float(value) do
    if value == value do
      {:ok, Float.to_string(value)}
    else
      {:error, :non_finite_float}
    end
  end

  defp literal(%Decimal{} = value), do: {:ok, Decimal.to_string(value, :normal)}
  defp literal(%Date{} = value), do: value |> Date.to_iso8601() |> quote_checked_string()
  defp literal(%Time{} = value), do: value |> Time.to_iso8601() |> quote_checked_string()

  defp literal(%NaiveDateTime{} = value),
    do: value |> NaiveDateTime.to_iso8601() |> quote_checked_string()

  defp literal(%DateTime{} = value), do: value |> DateTime.to_iso8601() |> quote_checked_string()

  defp literal(value) when is_atom(value),
    do: value |> Atom.to_string() |> quote_checked_string()

  defp literal(value) when is_binary(value), do: quote_checked_string(value)
  defp literal(value), do: {:error, {:unsupported_value, value_type(value)}}

  defp quote_checked_string(value) do
    cond do
      byte_size(value) > @max_param_bytes -> {:error, :value_too_large}
      not String.valid?(value) -> {:error, :invalid_utf8}
      String.contains?(value, <<0>>) -> {:error, :nul_byte_not_allowed}
      true -> {:ok, quote_string(value)}
    end
  end

  defp quote_string(value), do: ["'", String.replace(value, "'", "''"), "'"]

  defp validate_rendered_size(statement) do
    if byte_size(statement) <= @max_rendered_bytes,
      do: :ok,
      else: {:error, {:rendered_script_too_large, @max_rendered_bytes}}
  end

  defp secret_values(params, secret_params) do
    secret_params
    |> Enum.flat_map(fn name ->
      case Map.get(params, name) do
        "" ->
          []

        value when is_binary(value) and value != "" ->
          case literal(value) do
            {:ok, encoded} -> [value, IO.iodata_to_binary(encoded)]
            {:error, _reason} -> [value]
          end

        value ->
          case literal(value) do
            {:ok, encoded} -> [IO.iodata_to_binary(encoded)]
            {:error, _reason} -> []
          end
      end
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp value_type(value) when is_map(value), do: :map
  defp value_type(value) when is_list(value), do: :list
  defp value_type(value) when is_tuple(value), do: :tuple
  defp value_type(value) when is_pid(value), do: :pid
  defp value_type(value) when is_reference(value), do: :reference
  defp value_type(value) when is_function(value), do: :function
  defp value_type(_value), do: :unsupported
end
