defmodule FavnOrchestrator.Redaction do
  @moduledoc """
  Redacts untrusted operator diagnostics, logs, and hook metadata.
  """

  @sensitive_atom_keys [
    :token,
    :tokens,
    :password,
    :secret,
    :authorization,
    :cookie,
    :database,
    :database_path,
    :dsn,
    :url,
    :uri,
    :api_key,
    :access_key,
    :private_key
  ]

  @sensitive_fragments [
    "token",
    "password",
    "secret",
    "authorization",
    "cookie",
    "credential",
    "database",
    "dsn",
    "url",
    "uri",
    "api_key",
    "apikey",
    "access_key",
    "accesskey",
    "private_key",
    "privatekey"
  ]

  @operational_untrusted_keys [:reason, :message, :detail, :details, :error, :exception]

  @max_operational_depth 8
  @max_operational_entries 50
  @max_operational_string_bytes 8_192

  @sensitive_assignment ~r/(token|password|secret|authorization|cookie|credential|database|dsn|url|uri|api_key|apikey|access_key|accesskey|private_key|privatekey)\s*[:=]\s*((?:Bearer\s+)?[^\s,;]+)/i
  @bearer_token ~r/(bearer)\s+([^\s,;]+)/i
  @url_userinfo ~r/([a-z][a-z0-9+.-]*:\/\/)([^\s\/@:]+):([^\s\/@]+)@([^\s,;]+)/i

  @doc """
  Redacts sensitive fields while preserving safe scalar values.
  """
  @spec redact(term()) :: term()
  def redact(%DateTime{} = value), do: value

  def redact(%{__exception__: true, __struct__: module}),
    do: %{type: module, message: "[REDACTED]"}

  def redact(%_struct{} = value), do: redact_struct(value)

  def redact(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {key, redact(key, val)} end)
    |> Map.new()
  end

  def redact(value) when is_list(value), do: Enum.map(value, &redact/1)

  def redact(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact/1)
    |> List.to_tuple()
  end

  def redact(value) when is_atom(value), do: value
  def redact(value) when is_integer(value), do: value
  def redact(value) when is_float(value), do: value
  def redact(value) when is_binary(value), do: value
  def redact(value), do: inspect(value)

  @doc """
  Redacts operational log and metrics metadata.

  Unlike general diagnostics redaction, common error-bearing fields are treated
  as untrusted because adapter/runtime errors can embed secrets in messages.
  """
  @spec redact_operational(term()) :: term()
  def redact_operational(value), do: redact_operational(nil, value)

  @doc """
  Redacts operational metadata and bounds container depth, size, and strings.

  Use this at boundaries where untrusted diagnostics remain Elixir terms rather
  than being normalized by a storage codec.
  """
  @spec redact_operational_bounded(term()) :: term()
  def redact_operational_bounded(value) do
    value
    |> redact_operational()
    |> bound_operational(@max_operational_depth)
  rescue
    _error -> "[REDACTED]"
  end

  @doc """
  Redacts an untrusted value without preserving binary contents.
  """
  @spec redact_untrusted(term()) :: term()
  def redact_untrusted(%DateTime{} = value), do: value
  def redact_untrusted(%_struct{}), do: "[REDACTED]"
  def redact_untrusted(value) when is_atom(value), do: value
  def redact_untrusted(value) when is_integer(value), do: value
  def redact_untrusted(value) when is_float(value), do: value
  def redact_untrusted(value) when is_binary(value), do: "[REDACTED]"

  def redact_untrusted(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact_untrusted/1)
    |> List.to_tuple()
  end

  def redact_untrusted(value) when is_list(value), do: Enum.map(value, &redact_untrusted/1)

  def redact_untrusted(value) when is_map(value) do
    Map.new(value, fn {key, val} -> {key, redact_untrusted(val)} end)
  end

  def redact_untrusted(_value), do: "[REDACTED]"

  defp redact(key, _value) when key in @sensitive_atom_keys, do: "[REDACTED]"

  defp redact(key, value) when is_binary(key) do
    if sensitive_key?(key), do: "[REDACTED]", else: redact(value)
  end

  defp redact(key, value) when is_atom(key) do
    key
    |> Atom.to_string()
    |> sensitive_key?()
    |> case do
      true -> "[REDACTED]"
      false -> redact(value)
    end
  end

  defp redact(_key, value), do: redact(value)

  defp redact_operational(key, value) when key in @operational_untrusted_keys,
    do: redact_operational_untrusted(value)

  defp redact_operational(key, _value) when key in @sensitive_atom_keys, do: "[REDACTED]"

  defp redact_operational(key, value) when is_binary(key) do
    cond do
      sensitive_key?(key) -> "[REDACTED]"
      operational_untrusted_key?(key) -> redact_untrusted(value)
      true -> redact_operational(nil, value)
    end
  end

  defp redact_operational(nil, nil), do: nil

  defp redact_operational(nil, value)
       when is_atom(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_binary(value),
       do: value

  defp redact_operational(key, value) when is_atom(key) and not is_nil(key) do
    key
    |> Atom.to_string()
    |> redact_operational(value)
  end

  defp redact_operational(_key, %DateTime{} = value), do: value
  defp redact_operational(_key, %_struct{}), do: "[REDACTED]"

  defp redact_operational(_key, value) when is_map(value) do
    Map.new(value, fn {child_key, child_value} ->
      {child_key, redact_operational(child_key, child_value)}
    end)
  end

  defp redact_operational(_key, value) when is_list(value),
    do: Enum.map(value, &redact_operational(nil, &1))

  defp redact_operational(_key, value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact_operational(nil, &1))
    |> List.to_tuple()
  end

  defp redact_operational(_key, value), do: redact(value)

  defp redact_operational_untrusted(%DateTime{} = value), do: value

  defp redact_operational_untrusted(%{__exception__: true, __struct__: module} = exception) do
    %{type: module, message: sanitize_text(Exception.message(exception))}
  rescue
    _error -> %{type: module, message: "[REDACTED]"}
  end

  defp redact_operational_untrusted(%_struct{} = value) do
    value
    |> Map.from_struct()
    |> redact_operational_untrusted()
    |> Map.put(:type, value.__struct__)
  rescue
    _error -> %{type: value.__struct__, message: "[REDACTED]"}
  end

  defp redact_operational_untrusted(value) when is_atom(value), do: value
  defp redact_operational_untrusted(value) when is_integer(value), do: value
  defp redact_operational_untrusted(value) when is_float(value), do: value
  defp redact_operational_untrusted(value) when is_binary(value), do: sanitize_text(value)

  defp redact_operational_untrusted(value) when is_tuple(value) do
    case Tuple.to_list(value) do
      [key | rest] when is_atom(key) or is_binary(key) ->
        if sensitive_key?(key_to_string(key)) do
          [key | Enum.map(rest, fn _child -> "[REDACTED]" end)]
        else
          [key | Enum.map(rest, &redact_operational_untrusted/1)]
        end

      values ->
        Enum.map(values, &redact_operational_untrusted/1)
    end
    |> List.to_tuple()
  end

  defp redact_operational_untrusted(value) when is_list(value),
    do: Enum.map(value, &redact_operational_untrusted/1)

  defp redact_operational_untrusted(value) when is_map(value) do
    Map.new(value, fn {key, val} ->
      if sensitive_key?(key_to_string(key)) do
        {key, "[REDACTED]"}
      else
        {key, redact_operational_untrusted(val)}
      end
    end)
  end

  defp redact_operational_untrusted(value), do: value |> inspect() |> sanitize_text()

  defp sanitize_text(value) when is_binary(value) do
    value
    |> String.replace(@url_userinfo, "[REDACTED_URL]")
    |> String.replace(@bearer_token, "\\1 [REDACTED]")
    |> String.replace(@sensitive_assignment, "\\1=[REDACTED]")
  rescue
    _error -> "[REDACTED]"
  end

  defp bound_operational(_value, depth) when depth <= 0, do: "[TRUNCATED]"
  defp bound_operational(%DateTime{} = value, _depth), do: value

  defp bound_operational(value, depth) when is_map(value) do
    value
    |> Enum.take(@max_operational_entries)
    |> Map.new(fn {key, child} ->
      {bound_operational_key(key), bound_operational(child, depth - 1)}
    end)
  end

  defp bound_operational(value, depth) when is_list(value) do
    value
    |> Enum.take(@max_operational_entries)
    |> Enum.map(&bound_operational(&1, depth - 1))
  end

  defp bound_operational(value, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.take(@max_operational_entries)
    |> Enum.map(&bound_operational(&1, depth - 1))
    |> List.to_tuple()
  end

  defp bound_operational(value, _depth) when is_binary(value),
    do: truncate_operational(value)

  defp bound_operational(value, _depth)
       when is_atom(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: value

  defp bound_operational(value, _depth),
    do:
      value
      |> inspect(limit: 20, printable_limit: @max_operational_string_bytes)
      |> truncate_operational()

  defp bound_operational_key(key) when is_atom(key), do: key
  defp bound_operational_key(key) when is_binary(key), do: truncate_operational(key)
  defp bound_operational_key(key), do: bound_operational(key, 1)

  defp truncate_operational(value) do
    value = json_safe_binary(value)

    if byte_size(value) <= @max_operational_string_bytes do
      value
    else
      truncate_valid_binary(value)
    end
  end

  defp json_safe_binary(value) do
    if String.valid?(value) do
      value
    else
      inspect(value,
        binaries: :as_binaries,
        limit: @max_operational_entries,
        printable_limit: @max_operational_string_bytes
      )
    end
  end

  defp truncate_valid_binary(value) do
    suffix = "..."
    prefix_bytes = @max_operational_string_bytes - byte_size(suffix)
    valid_prefix(value, prefix_bytes) <> suffix
  end

  defp valid_prefix(_value, size) when size <= 0, do: ""

  defp valid_prefix(value, size) do
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix), do: prefix, else: valid_prefix(value, size - 1)
  end

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@sensitive_fragments, &String.contains?(key, &1))
  end

  defp operational_untrusted_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@operational_untrusted_keys, &(key == Atom.to_string(&1)))
  end

  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key) when is_binary(key), do: key
  defp key_to_string(key), do: inspect(key)

  defp redact_struct(value) do
    value
    |> Map.from_struct()
    |> Map.new(fn {key, val} -> {key, redact(key, val)} end)
    |> Map.put(:type, value.__struct__)
  end
end
