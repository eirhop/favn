defmodule FavnOrchestrator.API.SSE do
  @moduledoc """
  Line-safe Server-Sent Events field encoding for orchestrator streams.

  JSON payloads remain Jason-encoded by callers. This module is only for SSE
  control fields, where CR/LF characters can forge additional fields or events.
  """

  @safe_field ~r/\A[a-zA-Z0-9_.:-]{1,128}\z/

  @type field_name :: :event | :id

  @doc """
  Encodes one SSE control field value or rejects it when it is not line-safe.
  """
  @spec field(field_name(), atom() | String.t()) :: {:ok, String.t()} | {:error, term()}
  def field(name, value) when name in [:event, :id] do
    value = stringify(value)

    if is_binary(value) and String.match?(value, @safe_field) do
      {:ok, value}
    else
      {:error, {:invalid_sse_field, name, value}}
    end
  end

  def field(name, value), do: {:error, {:invalid_sse_field, name, value}}

  defp stringify(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp stringify(_value), do: nil
end
