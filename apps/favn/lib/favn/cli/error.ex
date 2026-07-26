defmodule Favn.CLI.Error do
  @moduledoc """
  Formats bounded, redacted operator errors for public Mix tasks.

  The formatter exposes transport status, a stable code, a short safe message,
  an actionable next step, and only a small allowlist of scalar details. It
  never dumps arbitrary response payloads or exception terms.
  """

  @detail_keys ~w(
    target_id run_id operation_id plan_id schedule_id workspace_id status field kind expected actual
  )
  @detail_atoms Enum.map(@detail_keys, &String.to_atom/1)
  @sensitive_assignment ~r/(token|password|secret|authorization|cookie|credential|database|dsn|url|uri|api_key|apikey|access_key|accesskey|private_key|privatekey)\s*[:=]\s*((?:Bearer\s+)?[^\s,;]+)/i
  @bearer_token ~r/(bearer)\s+([^\s,;]+)/i
  @url_userinfo ~r/([a-z][a-z0-9+.-]*:\/\/)([^\s\/@:]+):([^\s\/@]+)@([^\s,;]+)/i
  @sql_statement ~r/(?is)\b(?:select\b.+\bfrom\b|insert\s+into\b|update\s+[\w".]+\s+set\b|delete\s+from\b|merge\s+into\b|create\s+table\b|alter\s+table\b|drop\s+table\b|with\b.+\bselect\b).*/
  @max_message_bytes 240
  @max_detail_bytes 96
  @max_details 4

  @doc "Formats one CLI failure without exposing arbitrary payload contents."
  @spec format(term(), keyword()) :: String.t()
  def format(reason, opts \\ []) when is_list(opts) do
    context = Keyword.get(opts, :context, "command")
    next_step = Keyword.get(opts, :next)

    reason
    |> normalize()
    |> render(context, next_step)
  end

  @doc false
  @spec safe_message(term()) :: String.t()
  def safe_message(message), do: safe_text(message, "the request failed", @max_message_bytes)

  defp normalize(%{operation: operation, reason: reason}) do
    reason
    |> normalize()
    |> Map.put_new(:operation, operation_label(operation))
  end

  defp normalize({:http_error, status, payload}) when is_integer(status) do
    error = error_payload(payload)

    %{
      status: status,
      code: field(error, :error_code) || field(error, :code) || http_code(status),
      message: field(error, :message) || http_message(status),
      details: safe_details(field(error, :details))
    }
  end

  defp normalize({:connect_failed, _reason}),
    do: %{code: "connection_failed", message: "could not reach the Favn service"}

  defp normalize({:timeout, _phase}),
    do: %{code: "request_timeout", message: "the Favn request timed out"}

  defp normalize(reason) when reason in [:not_running, :stack_not_running],
    do: %{code: "stack_not_running", message: "Favn is not running"}

  defp normalize(reason) do
    %{code: reason_code(reason), message: "the request could not be completed"}
  end

  defp render(error, context, next_step) do
    operation = Map.get(error, :operation)
    label = operation || context
    status = if error[:status], do: " HTTP #{error.status}", else: ""
    code = safe_text(error[:code], "unknown_error", 80)
    message = safe_text(error[:message], "the request failed", @max_message_bytes)
    details = render_details(error[:details])
    next_step = render_next(next_step)

    "#{label} failed:#{status} [#{code}] #{message}#{details}#{next_step}"
  end

  defp error_payload(payload) when is_map(payload) do
    case field(payload, :error) do
      error when is_map(error) -> error
      _other -> payload
    end
  end

  defp error_payload(_payload), do: %{}

  defp safe_details(details) when is_map(details) do
    Enum.zip(@detail_keys, @detail_atoms)
    |> Enum.reduce([], fn {key, atom_key}, acc ->
      value = Map.get(details, key, Map.get(details, atom_key))

      if scalar?(value) and length(acc) < @max_details,
        do: [{key, safe_text(value, "", @max_detail_bytes)} | acc],
        else: acc
    end)
    |> Enum.reverse()
  end

  defp safe_details(_details), do: []

  defp render_details(details) when details in [nil, []], do: ""

  defp render_details(details) do
    rendered = Enum.map_join(details, ", ", fn {key, value} -> "#{key}=#{value}" end)
    " (#{rendered})"
  end

  defp render_next(nil), do: ""
  defp render_next(""), do: ""
  defp render_next(next_step), do: ". Next: " <> safe_text(next_step, "", @max_message_bytes)

  defp safe_text(value, fallback, max_bytes) when is_atom(value),
    do: value |> Atom.to_string() |> safe_text(fallback, max_bytes)

  defp safe_text(value, fallback, max_bytes) when is_integer(value),
    do: value |> Integer.to_string() |> safe_text(fallback, max_bytes)

  defp safe_text(value, fallback, max_bytes) when is_binary(value) do
    value =
      value
      |> String.replace(@url_userinfo, "[REDACTED_URL]")
      |> String.replace(@bearer_token, "\\1 [REDACTED]")
      |> String.replace(@sensitive_assignment, "\\1=[REDACTED]")
      |> String.replace(@sql_statement, "[REDACTED SQL]")
      |> String.replace(~r/[\r\n\t]+/, " ")
      |> String.trim()

    cond do
      value == "" -> fallback
      byte_size(value) <= max_bytes -> value
      true -> truncate_valid_binary(value, max_bytes)
    end
  rescue
    _error -> fallback
  end

  defp safe_text(_value, fallback, _max_bytes), do: fallback

  defp scalar?(value),
    do: is_binary(value) or (is_atom(value) and not is_nil(value)) or is_integer(value)

  defp field(map, key) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp truncate_valid_binary(value, max_bytes) do
    suffix = "..."
    prefix_bytes = max(max_bytes - byte_size(suffix), 0)
    valid_prefix(value, prefix_bytes) <> suffix
  end

  defp valid_prefix(_value, size) when size <= 0, do: ""

  defp valid_prefix(value, size) do
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix), do: prefix, else: valid_prefix(value, size - 1)
  end

  defp reason_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_code({code, _details}) when is_atom(code), do: Atom.to_string(code)
  defp reason_code({code, _, _}) when is_atom(code), do: Atom.to_string(code)
  defp reason_code(_reason), do: "request_failed"

  defp http_code(status), do: "http_#{status}"

  defp http_message(400), do: "the request was invalid"
  defp http_message(401), do: "authentication failed"
  defp http_message(403), do: "the operation is not allowed"
  defp http_message(404), do: "the requested resource was not found"
  defp http_message(409), do: "the request conflicts with current state"
  defp http_message(422), do: "the request failed validation"
  defp http_message(status) when status >= 500, do: "the Favn service failed"
  defp http_message(_status), do: "the Favn service rejected the request"

  defp operation_label(operation) when is_atom(operation),
    do: operation |> Atom.to_string() |> operation_label()

  defp operation_label("plan_rebuild"), do: "rebuild plan"
  defp operation_label("start_rebuild"), do: "rebuild start"
  defp operation_label("get_rebuild"), do: "rebuild status"
  defp operation_label("cancel_rebuild"), do: "rebuild cancel"
  defp operation_label("retry_rebuild"), do: "rebuild retry"
  defp operation_label("reconcile_rebuild"), do: "rebuild reconcile"
  defp operation_label("submit_run"), do: "run submission"
  defp operation_label("cancel_run"), do: "run cancellation"

  defp operation_label(operation) when is_binary(operation),
    do: String.replace(operation, "_", " ")

  defp operation_label(_operation), do: "command"
end
