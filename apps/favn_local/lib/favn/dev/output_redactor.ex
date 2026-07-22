defmodule Favn.Dev.OutputRedactor do
  @moduledoc """
  Redacts generated local secrets and runner environment values from output.

  The redactor is applied before Docker output is returned, streamed, or
  persisted. Streaming keeps a bounded raw suffix so values split across
  command-output chunks cannot escape redaction.
  """

  alias Favn.Dev.{ComposeEnv, Paths, State}

  @redacted "[REDACTED]"
  @redacted_url "[REDACTED_URL]"
  @max_pending_bytes 1_048_576
  @generic_overlap_bytes 4_096
  @minimum_exact_value_bytes 8
  @sensitive_key ~r/(?:authorization|credential|password|secret|token|cookie|database_url|pin_key)/i
  @database_url ~r{\b(?:ecto|postgres(?:ql)?):\/\/[^\s\"'<>]+}iu
  @credential_url ~r{\b[a-z][a-z0-9+.-]*:\/\/[^\s\/@:]+:[^\s\/@]*@[^\s\"'<>]+}iu
  @sensitive_assignment ~r{(?im)(\b(?:authorization|credential|password|secret|token|cookie|database_url|pin_key)\b\s*[:=]\s*)(?:\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;]+)}u

  @type stream_writer :: (binary() -> term())

  @doc "Redacts sensitive values from one output string."
  @spec redact(String.t(), keyword()) :: String.t()
  def redact(output, opts \\ []) when is_binary(output) and is_list(opts) do
    opts
    |> known_values()
    |> redact_values(output)
    |> redact_patterns()
  end

  @doc "Recursively redacts sensitive keys and string values from a term."
  @spec redact_term(term(), keyword()) :: term()
  def redact_term(value, opts \\ []) when is_list(opts) do
    redact_term_value(value, known_values(opts))
  end

  @doc false
  @spec stream_writer(keyword(), stream_writer()) :: {stream_writer(), (-> :ok)}
  def stream_writer(opts, sink) when is_list(opts) and is_function(sink, 1) do
    values = known_values(opts)
    {:ok, buffer} = Agent.start_link(fn -> "" end)

    writer = fn chunk ->
      emitted =
        Agent.get_and_update(buffer, fn pending ->
          split_stream_buffer(pending <> IO.iodata_to_binary(chunk), values)
        end)

      if emitted != "", do: sink.(redact_with_values(emitted, values))
      :ok
    end

    flush = fn ->
      pending = Agent.get_and_update(buffer, fn pending -> {pending, ""} end)
      if pending != "", do: sink.(redact_with_values(pending, values))
      Agent.stop(buffer, :normal)
      :ok
    end

    {writer, flush}
  end

  defp known_values(opts) do
    root_dir = Paths.root_dir(opts)

    []
    |> append_map_values(read_map(fn -> State.read_secrets(opts) end))
    |> append_map_values(read_map(fn -> State.read_maintenance(opts) end))
    |> append_map_values(Keyword.get(opts, :env_file_loaded, %{}))
    |> append_map_values(read_runner_environment(root_dir))
    |> append_map_values(read_sensitive_compose_environment(root_dir))
    |> Enum.filter(&(is_binary(&1) and byte_size(&1) >= @minimum_exact_value_bytes))
    |> Enum.uniq()
    |> Enum.sort_by(&byte_size/1, :desc)
  end

  defp append_map_values(values, map) when is_map(map), do: values ++ Map.values(map)
  defp append_map_values(values, _invalid), do: values

  defp read_map(fun) do
    case fun.() do
      {:ok, map} when is_map(map) -> map
      _unavailable -> %{}
    end
  end

  defp read_runner_environment(root_dir) do
    root_dir
    |> Paths.compose_runner_env_path()
    |> ComposeEnv.read()
    |> case do
      {:ok, environment} -> environment
      _unavailable -> %{}
    end
  end

  defp read_sensitive_compose_environment(root_dir) do
    root_dir
    |> Paths.compose_env_path()
    |> ComposeEnv.read()
    |> case do
      {:ok, environment} ->
        Map.filter(environment, fn {key, _value} -> Regex.match?(@sensitive_key, key) end)

      _unavailable ->
        %{}
    end
  end

  defp redact_values(values, output) do
    Enum.reduce(values, output, fn value, redacted ->
      String.replace(redacted, value, @redacted)
    end)
  end

  defp redact_patterns(output) do
    output
    |> then(&Regex.replace(@database_url, &1, @redacted_url))
    |> then(&Regex.replace(@credential_url, &1, @redacted_url))
    |> then(&Regex.replace(@sensitive_assignment, &1, "\\1" <> @redacted))
  end

  defp redact_with_values(output, values) do
    values
    |> redact_values(output)
    |> redact_patterns()
  end

  defp redact_term_value(value, values) when is_map(value) do
    Map.new(value, fn {key, child} ->
      if sensitive_key?(key), do: {key, @redacted}, else: {key, redact_term_value(child, values)}
    end)
  end

  defp redact_term_value(value, values) when is_list(value),
    do: Enum.map(value, &redact_term_value(&1, values))

  defp redact_term_value(value, values) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&redact_term_value(&1, values)) |> List.to_tuple()
  end

  defp redact_term_value(value, values) when is_binary(value),
    do: redact_with_values(value, values)

  defp redact_term_value(value, _values), do: value

  defp sensitive_key?(key) when is_atom(key), do: key |> Atom.to_string() |> sensitive_key?()
  defp sensitive_key?(key) when is_binary(key), do: Regex.match?(@sensitive_key, key)
  defp sensitive_key?(_key), do: false

  defp split_stream_buffer(buffer, values) do
    desired_cutoff = stream_cutoff(buffer, values)
    cutoff = avoid_value_boundary(buffer, desired_cutoff, values)

    if cutoff > 0 do
      emitted = binary_part(buffer, 0, cutoff)
      pending = binary_part(buffer, cutoff, byte_size(buffer) - cutoff)
      {emitted, pending}
    else
      {"", buffer}
    end
  end

  defp stream_cutoff(buffer, values) do
    overlap = max(max_value_size(values) - 1, @generic_overlap_bytes)
    safe_cutoff = max(byte_size(buffer) - overlap, 0)

    if safe_cutoff == 0 do
      0
    else
      newline_cutoff =
        buffer
        |> :binary.matches("\n")
        |> Enum.map(fn {index, 1} -> index + 1 end)
        |> Enum.filter(&(&1 <= safe_cutoff))
        |> List.last()

      cond do
        is_integer(newline_cutoff) -> newline_cutoff
        byte_size(buffer) > @max_pending_bytes + overlap -> safe_cutoff
        true -> 0
      end
    end
  end

  defp avoid_value_boundary(_buffer, 0, _values), do: 0

  defp avoid_value_boundary(buffer, cutoff, values) do
    Enum.reduce(values, cutoff, fn value, safe_cutoff ->
      buffer
      |> :binary.matches(value)
      |> Enum.reduce(safe_cutoff, fn {start, length}, candidate ->
        if start < candidate and start + length > candidate, do: start, else: candidate
      end)
    end)
  end

  defp max_value_size([]), do: 0
  defp max_value_size(values), do: values |> Enum.map(&byte_size/1) |> Enum.max()
end
