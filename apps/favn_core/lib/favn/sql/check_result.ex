defmodule Favn.SQL.CheckResult do
  @moduledoc """
  Typed runtime outcome for one declarative SQL check.

  Metric keys remain strings because SQL result column names are runtime data
  and must not create atoms.
  """

  @outcomes [
    :passed,
    :warned,
    :failed,
    :materialization_skipped,
    :condition_skipped,
    :not_run,
    :errored
  ]

  @type outcome ::
          :passed
          | :warned
          | :failed
          | :materialization_skipped
          | :condition_skipped
          | :not_run
          | :errored

  @type metric_value ::
          nil
          | boolean()
          | number()
          | Decimal.t()
          | String.t()
          | Date.t()
          | Time.t()
          | NaiveDateTime.t()
          | DateTime.t()

  @enforce_keys [:name, :phase, :outcome]
  defstruct [:name, :phase, :outcome, :message, :duration_ms, :reason, metrics: %{}]

  @type t :: %__MODULE__{
          name: atom(),
          phase: Favn.SQL.Check.phase(),
          outcome: outcome(),
          message: String.t() | nil,
          metrics: %{optional(String.t()) => metric_value()},
          duration_ms: non_neg_integer() | nil,
          reason: atom() | String.t() | nil
        }

  @doc "Builds a runtime result and rejects unknown outcomes."
  @spec new(map() | keyword()) :: t()
  def new(fields) when is_map(fields) or is_list(fields) do
    result = struct!(__MODULE__, fields)

    unless is_atom(result.name) and not is_nil(result.name),
      do: raise(ArgumentError, "SQL check result name must be a non-nil atom")

    unless result.phase in [:before_materialize, :after_materialize],
      do: raise(ArgumentError, "invalid SQL check result phase #{inspect(result.phase)}")

    unless result.outcome in @outcomes,
      do: raise(ArgumentError, "invalid SQL check result outcome #{inspect(result.outcome)}")

    unless is_map(result.metrics),
      do: raise(ArgumentError, "SQL check result metrics must be a map")

    unless Enum.all?(result.metrics, fn {key, _value} -> is_binary(key) end),
      do: raise(ArgumentError, "SQL check result metric names must be strings")

    unless is_nil(result.duration_ms) or
             (is_integer(result.duration_ms) and result.duration_ms >= 0),
           do: raise(ArgumentError, "SQL check result duration must be a non-negative integer")

    unless is_nil(result.message) or is_binary(result.message),
      do: raise(ArgumentError, "SQL check result message must be a string")

    unless is_nil(result.reason) or is_atom(result.reason) or is_binary(result.reason),
      do: raise(ArgumentError, "SQL check result reason must be an atom or string")

    result
  end
end
