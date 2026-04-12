defmodule Favn.SQL.IncrementalWindow do
  @moduledoc false

  alias Favn.Window.{Runtime, Spec}

  @enforce_keys [:kind, :start_at, :end_at, :timezone, :lookback, :requested_window]
  defstruct [:kind, :start_at, :end_at, :timezone, :lookback, :requested_window, widened?: false]

  @type t :: %__MODULE__{
          kind: Runtime.kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t(),
          lookback: non_neg_integer(),
          requested_window: Runtime.t(),
          widened?: boolean()
        }

  @spec resolve(Runtime.t(), Spec.t()) :: {:ok, t()} | {:error, term()}
  def resolve(%Runtime{} = runtime, %Spec{} = spec) do
    with :ok <- validate_runtime(runtime, spec),
         {:ok, start_at} <- shift_kind(runtime.start_at, spec.kind, -spec.lookback) do
      {:ok,
       %__MODULE__{
         kind: spec.kind,
         start_at: start_at,
         end_at: runtime.end_at,
         timezone: spec.timezone,
         lookback: spec.lookback,
         requested_window: runtime,
         widened?: spec.lookback > 0
       }}
    end
  end

  @spec from_runtime(Runtime.t(), Spec.t()) :: t()
  def from_runtime(%Runtime{} = runtime, %Spec{} = spec) do
    %__MODULE__{
      kind: spec.kind,
      start_at: runtime.start_at,
      end_at: runtime.end_at,
      timezone: spec.timezone,
      lookback: spec.lookback,
      requested_window: runtime,
      widened?: spec.lookback > 0
    }
  end

  @spec to_runtime(t()) :: {:ok, Runtime.t()} | {:error, term()}
  def to_runtime(%__MODULE__{} = window) do
    Runtime.new(
      window.kind,
      window.start_at,
      window.end_at,
      window.requested_window.anchor_key,
      timezone: window.timezone
    )
  end

  defp validate_runtime(%Runtime{kind: kind, timezone: timezone}, %Spec{} = spec)
       when kind == spec.kind and timezone == spec.timezone,
       do: :ok

  defp validate_runtime(%Runtime{} = runtime, %Spec{} = spec),
    do: {:error, {:runtime_window_mismatch, runtime, spec}}

  defp shift_kind(datetime, :hour, count),
    do: {:ok, DateTime.add(datetime, count * 3600, :second)}

  defp shift_kind(datetime, :day, count), do: {:ok, DateTime.add(datetime, count, :day)}

  defp shift_kind(%DateTime{} = datetime, :month, count) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    year = div(total, 12)
    month = rem(total, 12) + 1

    with {:ok, new_date} <- Date.new(year, month, 1),
         {:ok, naive} <- NaiveDateTime.new(new_date, ~T[00:00:00.000000]) do
      {:ok, DateTime.from_naive!(naive, datetime.time_zone)}
    end
  end
end
