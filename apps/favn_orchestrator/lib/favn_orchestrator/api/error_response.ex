defmodule FavnOrchestrator.API.ErrorResponse do
  @moduledoc """
  Stable private API error response contracts.

  Detailed internal reasons belong in server logs. Values returned from this
  module are safe to expose to API clients and browser callers.
  """

  @type response :: {pos_integer(), String.t(), String.t(), map()}

  @doc """
  Maps boundary failures to stable API error tuples.
  """
  @spec response(term()) :: response()
  def response(:idempotency_completion_failed) do
    {500, "internal_error", "Command outcome is unknown", %{outcome: "unknown"}}
  end

  def response(:request_failed), do: {400, "bad_request", "Request failed", %{}}
end
