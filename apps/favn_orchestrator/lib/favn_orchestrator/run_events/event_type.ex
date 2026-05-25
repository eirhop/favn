defmodule FavnOrchestrator.RunEvents.EventType do
  @moduledoc """
  Line-safe run-event type contract used by persistence and SSE framing.

  Event types are stored and streamed as SSE `event:` names, so they must stay on
  one line and use a compact, predictable character set.
  """

  @safe_pattern ~r/\A[a-zA-Z0-9_.:-]{1,128}\z/

  @doc """
  Returns true when a value can be safely used as a run-event type or SSE field.
  """
  @spec line_safe?(atom() | String.t()) :: boolean()
  def line_safe?(value) when is_atom(value) and not is_nil(value) do
    value |> Atom.to_string() |> line_safe?()
  end

  def line_safe?(value) when is_binary(value), do: String.match?(value, @safe_pattern)
  def line_safe?(_value), do: false
end
