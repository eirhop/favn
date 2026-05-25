defmodule FavnOrchestrator.SchedulerError do
  @moduledoc """
  Compact scheduler failure visible to operator read models.

  This intentionally excludes stack traces and raw internal payloads.
  """

  @type phase :: :evaluate | :compute_due | :submit_run | :persist_state

  @type t :: %__MODULE__{
          occurred_at: DateTime.t(),
          phase: phase(),
          code: atom() | String.t(),
          message: String.t()
        }

  @enforce_keys [:occurred_at, :phase, :code, :message]
  defstruct [:occurred_at, :phase, :code, :message]

  @doc """
  Builds a compact scheduler error from an internal reason.
  """
  @spec new(phase(), term(), DateTime.t()) :: t()
  def new(phase, reason, %DateTime{} = occurred_at)
      when phase in [:evaluate, :compute_due, :submit_run, :persist_state] do
    %__MODULE__{
      occurred_at: occurred_at,
      phase: phase,
      code: error_code(reason),
      message: error_message(reason)
    }
  end

  defp error_code(reason) when is_atom(reason), do: reason
  defp error_code({reason, _details}) when is_atom(reason), do: reason
  defp error_code(_reason), do: :scheduler_error

  defp error_message(reason) when is_atom(reason), do: humanize(reason)

  defp error_message({reason, details}) when is_atom(reason),
    do: humanize(reason) <> ": " <> inspect(details)

  defp error_message(reason), do: inspect(reason)

  defp humanize(value) do
    value
    |> Atom.to_string()
    |> String.replace("_", " ")
  end
end
