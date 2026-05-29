defmodule FavnOrchestrator.Audit.Redactor do
  @moduledoc """
  Audit-specific redaction for operator command payloads and request metadata.
  """

  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.JsonSafe

  @max_payload_bytes 16_384
  @sensitive_fragments ~w(
    token password secret credential authorization cookie dsn url uri api_key apikey
    access_key accesskey private_key privatekey service_token connection database
  )

  @doc """
  Redacts and JSON-normalizes an untrusted operator command payload.
  """
  @spec redact_payload(term()) :: map()
  def redact_payload(value) do
    value
    |> redact_sensitive_keys()
    |> Redaction.redact()
    |> JsonSafe.data()
    |> bound_payload()
  end

  @doc """
  Redacts and JSON-normalizes allow-listed request context.
  """
  @spec redact_request_context(term()) :: map()
  def redact_request_context(value) when is_map(value) do
    value
    |> Map.take([:remote_ip, :user_agent, :request_id, "remote_ip", "user_agent", "request_id"])
    |> redact_payload()
  end

  def redact_request_context(_value), do: %{}

  @doc """
  Produces a small, redacted failure class safe for audit storage.
  """
  @spec failure_class(term()) :: String.t()
  def failure_class(reason) when is_atom(reason), do: Atom.to_string(reason)

  def failure_class({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)

  def failure_class(%{__exception__: true, __struct__: module}) when is_atom(module),
    do: Atom.to_string(module)

  def failure_class(reason) do
    reason
    |> Redaction.redact_operational()
    |> inspect(limit: 5, printable_limit: 80)
  end

  defp redact_sensitive_keys(%DateTime{} = value), do: value

  defp redact_sensitive_keys(%_{} = value),
    do: value |> Map.from_struct() |> redact_sensitive_keys()

  defp redact_sensitive_keys(value) when is_map(value) do
    Map.new(value, fn {key, child_value} ->
      key_string = key_to_string(key)

      if sensitive_key?(key_string) do
        {key, "[REDACTED]"}
      else
        {key, redact_sensitive_keys(child_value)}
      end
    end)
  end

  defp redact_sensitive_keys(value) when is_list(value),
    do: Enum.map(value, &redact_sensitive_keys/1)

  defp redact_sensitive_keys(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact_sensitive_keys/1)
  end

  defp redact_sensitive_keys(value), do: value

  defp sensitive_key?(key) do
    normalized = String.downcase(key)
    Enum.any?(@sensitive_fragments, &String.contains?(normalized, &1))
  end

  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key) when is_binary(key), do: key
  defp key_to_string(key), do: inspect(key)

  defp bound_payload(value) when is_map(value) do
    if encoded_size(value) <= @max_payload_bytes do
      value
    else
      %{"payload_truncated" => true}
    end
  end

  defp bound_payload(value), do: %{"value" => value}

  defp encoded_size(value), do: value |> Jason.encode!() |> byte_size()
end
