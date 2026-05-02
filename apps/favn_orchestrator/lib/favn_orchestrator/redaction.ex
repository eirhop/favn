defmodule FavnOrchestrator.Redaction do
  @moduledoc """
  Redacts untrusted operator diagnostics, logs, and hook metadata.
  """

  @sensitive_atom_keys [:token, :tokens, :password, :secret, :authorization, :cookie]
  @sensitive_fragments ["token", "password", "secret", "authorization", "cookie", "credential"]

  @doc """
  Redacts sensitive fields while preserving safe scalar values.
  """
  @spec redact(term()) :: term()
  def redact(%DateTime{} = value), do: value

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
  def redact(value) when is_boolean(value), do: value
  def redact(value) when is_binary(value), do: value
  def redact(nil), do: nil
  def redact(value), do: inspect(value)

  @doc """
  Redacts an untrusted value without preserving binary contents.
  """
  @spec redact_untrusted(term()) :: term()
  def redact_untrusted(%DateTime{} = value), do: value
  def redact_untrusted(value) when is_atom(value), do: value
  def redact_untrusted(value) when is_integer(value), do: value
  def redact_untrusted(value) when is_float(value), do: value
  def redact_untrusted(value) when is_boolean(value), do: value
  def redact_untrusted(nil), do: nil
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

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@sensitive_fragments, &String.contains?(key, &1))
  end
end
