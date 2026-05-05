defmodule FavnOrchestrator.Storage.JsonSafe do
  @moduledoc false

  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Redaction

  @max_depth 8
  @max_entries 50
  @max_string_bytes 8_192

  @sensitive_key_fragments ~w(
    token tokens password secret authorization cookie credential credentials database dsn url uri
    api_key apikey access_key accesskey private_key privatekey
  )

  @spec data(term()) :: map() | list() | String.t() | number() | boolean() | nil
  def data(value), do: data(value, nil, @max_depth)

  @spec error(term()) :: map() | nil
  def error(nil), do: nil

  def error(%{type: :missing_runtime_config} = value), do: runtime_config_diagnostic(value)
  def error(%{"type" => "missing_runtime_config"} = value), do: runtime_config_diagnostic(value)

  def error(%{"kind" => kind, "message" => message, "reason" => reason, "type" => type}) do
    %{
      "kind" => scalar_string(kind, "error"),
      "type" => scalar_string(type, "term"),
      "message" => safe_error_message(message),
      "reason" => safe_existing_error_reason(reason),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(%{kind: kind} = value) do
    reason = Map.get(value, :reason) || Map.get(value, "reason")
    message = Map.get(value, :message) || Map.get(value, "message") || exception_message(reason)

    %{
      "kind" => scalar_string(kind, "error"),
      "type" => error_type(reason),
      "message" => safe_error_message(message || reason || value),
      "reason" => safe_error_reason(reason || value),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(%{__exception__: true} = exception) do
    %{
      "kind" => "error",
      "type" => exception.__struct__ |> Atom.to_string(),
      "message" => safe_error_message(exception_message(exception) || exception),
      "reason" => safe_error_reason(exception),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(value) do
    %{
      "kind" => "error",
      "type" => error_type(value),
      "message" => safe_error_message(exception_message(value) || value),
      "reason" => safe_error_reason(value),
      "redacted" => true,
      "truncated" => false
    }
  end

  @spec ref(Favn.Ref.t() | term()) :: map() | nil
  def ref({module, name}) when is_atom(module) and is_atom(name) do
    %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}
  end

  def ref(_value), do: nil

  defp data(_value, _key, depth) when depth <= 0, do: "[TRUNCATED]"
  defp data(%DateTime{} = value, _key, _depth), do: DateTime.to_iso8601(value)
  defp data(%AssetResult{} = value, _key, depth), do: asset_result(value, depth)

  defp data(%{__exception__: true} = value, _key, _depth), do: error(value)

  defp data(%_{} = value, key, depth) do
    value
    |> Map.from_struct()
    |> data(key, depth - 1)
  end

  defp data(value, _key, depth) when is_map(value) do
    value
    |> Enum.take(@max_entries)
    |> Map.new(fn {child_key, child_value} ->
      key_string = key_to_string(child_key)

      normalized_value =
        if sensitive_key?(key_string) do
          redact_sensitive_value(child_value)
        else
          data(child_value, key_string, depth - 1)
        end

      {key_string, normalized_value}
    end)
  end

  defp data(value, _key, depth) when is_list(value) do
    value
    |> Enum.take(@max_entries)
    |> Enum.map(&data(&1, nil, depth - 1))
  end

  defp data({module, name}, _key, _depth) when is_atom(module) and is_atom(name),
    do: ref({module, name})

  defp data(value, _key, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.take(@max_entries)
    |> Enum.map(&data(&1, nil, depth - 1))
  end

  defp data(value, _key, _depth) when is_binary(value), do: truncate(value)
  defp data(value, _key, _depth) when is_integer(value) or is_float(value), do: value
  defp data(value, _key, _depth) when is_boolean(value), do: value
  defp data(nil, _key, _depth), do: nil
  defp data(value, _key, _depth) when is_atom(value), do: Atom.to_string(value)
  defp data(value, _key, _depth), do: inspect_value(value)

  defp asset_result(%AssetResult{} = result, depth) do
    %{
      "ref" => ref(result.ref),
      "stage" => result.stage,
      "status" => atom_string(result.status),
      "started_at" => data(result.started_at, nil, depth - 1),
      "finished_at" => data(result.finished_at, nil, depth - 1),
      "duration_ms" => result.duration_ms,
      "meta" => data(result.meta, "meta", depth - 1),
      "error" => error(result.error),
      "attempt_count" => result.attempt_count,
      "max_attempts" => result.max_attempts,
      "attempts" => Enum.map(result.attempts || [], &attempt(&1, depth - 1)),
      "next_retry_at" => data(result.next_retry_at, nil, depth - 1)
    }
  end

  defp attempt(%{} = attempt, depth) do
    attempt
    |> data(nil, depth)
    |> Map.update("error", nil, &error/1)
  end

  defp attempt(value, depth), do: data(value, nil, depth)

  defp runtime_config_diagnostic(value) when is_map(value) do
    value
    |> Map.drop([:stacktrace, "stacktrace"])
    |> Map.new(fn {key, child_value} ->
      {key_to_string(key), runtime_config_diagnostic_value(key, child_value)}
    end)
  end

  defp runtime_config_diagnostic_value(key, value) do
    key_string = key_to_string(key)

    cond do
      key_string in ["message", "type", "phase", "provider", "env", "scope", "field"] ->
        data(value, key_string, @max_depth - 1)

      key_string in [
        "key",
        "connection",
        "module",
        "asset_ref",
        "asset_type",
        "connections",
        "sql_asset_refs",
        "connection_asset_refs"
      ] ->
        data(value, key_string, @max_depth - 1)

      key_string in ["secret?"] ->
        data(value, key_string, @max_depth - 1)

      is_map(value) ->
        runtime_config_diagnostic(value)

      is_list(value) ->
        Enum.map(value, &runtime_config_diagnostic_nested/1)

      is_tuple(value) ->
        value
        |> Tuple.to_list()
        |> Enum.map(&runtime_config_diagnostic_nested/1)

      true ->
        data(value, key_string, @max_depth - 1)
    end
  end

  defp runtime_config_diagnostic_nested(value) when is_map(value),
    do: runtime_config_diagnostic(value)

  defp runtime_config_diagnostic_nested(value) when is_list(value),
    do: Enum.map(value, &runtime_config_diagnostic_nested/1)

  defp runtime_config_diagnostic_nested(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&runtime_config_diagnostic_nested/1)
  end

  defp runtime_config_diagnostic_nested(value), do: data(value, nil, @max_depth - 1)

  defp safe_error_message(value) do
    case Redaction.redact_operational(%{message: value}) do
      %{message: redacted} -> scalar_string(redacted, "Runner error")
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp safe_error_reason(value) do
    case Redaction.redact_operational(%{reason: value}) do
      %{reason: redacted} -> inspect_value(redacted)
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp safe_existing_error_reason(value) when is_binary(value), do: safe_error_message(value)
  defp safe_existing_error_reason(value), do: safe_error_reason(value)

  defp exception_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp exception_message(_value), do: nil

  defp error_type(%{__exception__: true, __struct__: module}), do: Atom.to_string(module)
  defp error_type(%{__struct__: module}), do: Atom.to_string(module)
  defp error_type(value) when is_atom(value), do: Atom.to_string(value)
  defp error_type(value) when is_map(value), do: "map"
  defp error_type(value) when is_tuple(value), do: "tuple"
  defp error_type(value) when is_list(value), do: "list"
  defp error_type(value) when is_binary(value), do: "string"
  defp error_type(value) when is_number(value), do: "number"
  defp error_type(value) when is_boolean(value), do: "boolean"
  defp error_type(nil), do: "nil"
  defp error_type(_value), do: "term"

  defp scalar_string(value, _default) when is_binary(value), do: truncate(value)
  defp scalar_string(value, _default) when is_atom(value), do: Atom.to_string(value)
  defp scalar_string(nil, default), do: default
  defp scalar_string(value, _default), do: inspect_value(value)

  defp atom_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_string(value) when is_binary(value), do: value
  defp atom_string(nil), do: nil
  defp atom_string(value), do: inspect_value(value)

  defp key_to_string(key) when is_binary(key), do: key
  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key), do: inspect_value(key)

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@sensitive_key_fragments, &String.contains?(key, &1))
  end

  defp redact_sensitive_value(value) when is_boolean(value), do: value
  defp redact_sensitive_value(nil), do: nil
  defp redact_sensitive_value(_value), do: "[REDACTED]"

  defp truncate(value) when is_binary(value) do
    if byte_size(value) > @max_string_bytes do
      suffix = "..."
      content_bytes = @max_string_bytes - byte_size(suffix)
      valid_prefix(value, content_bytes) <> suffix
    else
      value
    end
  end

  defp valid_prefix(_value, size) when size <= 0, do: ""

  defp valid_prefix(value, size) do
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix) do
      prefix
    else
      valid_prefix(value, size - 1)
    end
  end

  defp inspect_value(value) do
    value
    |> inspect(limit: 20, printable_limit: @max_string_bytes)
    |> truncate()
  rescue
    _error -> "#Inspect.Error<>"
  end
end
