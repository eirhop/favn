defmodule Favn.TimePeriod do
  @moduledoc """
  Shared period and timezone logic for window-oriented time calculations.

  Periods use local calendar boundaries in the requested timezone. Daily,
  monthly, and yearly periods therefore follow the local calendar rather than a
  fixed number of seconds. Hourly periods shift by 3600 seconds, matching the
  existing window behavior.

  This module is the common implementation behind window anchors, backfill range
  expansion, and calendar freshness policies. For author-facing freshness input,
  read `Favn.Freshness.Policy`; for runtime freshness keys, read
  `Favn.Freshness.Key`.
  """

  alias Favn.Window.Validate

  @typedoc "Canonical period kind."
  @type kind :: :hour | :day | :month | :year

  @typedoc "Accepted period kind or alias."
  @type kind_alias :: kind() | :hourly | :daily | :monthly | :yearly

  @typedoc "Concrete period bounds in a timezone."
  @type t :: %__MODULE__{
          kind: kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t()
        }

  defstruct [:kind, :start_at, :end_at, :timezone]

  @doc """
  Normalizes period kind aliases to canonical kinds.
  """
  @spec normalize_kind(term()) :: {:ok, kind()} | {:error, term()}
  def normalize_kind(kind) when kind in [:hour, :hourly], do: {:ok, :hour}
  def normalize_kind(kind) when kind in [:day, :daily], do: {:ok, :day}
  def normalize_kind(kind) when kind in [:month, :monthly], do: {:ok, :month}
  def normalize_kind(kind) when kind in [:year, :yearly], do: {:ok, :year}
  def normalize_kind(kind), do: {:error, {:invalid_period_kind, kind}}

  @doc """
  Floors a datetime to the start of its containing period in `timezone`.
  """
  @spec floor(DateTime.t(), kind_alias(), String.t()) :: {:ok, DateTime.t()} | {:error, term()}
  def floor(%DateTime{} = datetime, kind, timezone) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- Validate.timezone(timezone),
         {:ok, local} <- shift_zone(datetime, timezone) do
      floor_local(local, kind)
    end
  end

  def floor(datetime, _kind, _timezone), do: {:error, {:invalid_datetime, datetime}}

  @doc """
  Floors a datetime, raising `ArgumentError` when the input is invalid.
  """
  @spec floor!(DateTime.t(), kind_alias(), String.t()) :: DateTime.t()
  def floor!(%DateTime{} = datetime, kind, timezone) do
    case floor(datetime, kind, timezone) do
      {:ok, floored} -> floored
      {:error, reason} -> raise ArgumentError, "invalid time period floor: #{inspect(reason)}"
    end
  end

  @doc """
  Shifts a period boundary by `count` periods.

  Daily, monthly, and yearly shifts use local calendar boundaries in the
  datetime's timezone. Hourly shifts use 3600-second increments.
  """
  @spec shift(DateTime.t(), kind_alias(), integer()) :: {:ok, DateTime.t()} | {:error, term()}
  def shift(%DateTime{} = datetime, kind, count) when is_integer(count) do
    with {:ok, kind} <- normalize_kind(kind) do
      shift_kind(datetime, kind, count)
    end
  end

  def shift(%DateTime{} = _datetime, _kind, count), do: {:error, {:invalid_period_count, count}}
  def shift(datetime, _kind, _count), do: {:error, {:invalid_datetime, datetime}}

  @doc """
  Shifts a period boundary, raising `ArgumentError` when the input is invalid.
  """
  @spec shift!(DateTime.t(), kind_alias(), integer()) :: DateTime.t()
  def shift!(%DateTime{} = datetime, kind, count) do
    case shift(datetime, kind, count) do
      {:ok, shifted} -> shifted
      {:error, reason} -> raise ArgumentError, "invalid time period shift: #{inspect(reason)}"
    end
  end

  @doc """
  Builds period bounds from a string value.

  Accepted value forms are `YYYY-MM-DDTHH` for hours, `YYYY-MM-DD` for days,
  `YYYY-MM` for months, and `YYYY` for years.
  """
  @spec bounds(kind_alias(), String.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def bounds(kind, value, timezone) when is_binary(value) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- Validate.timezone(timezone),
         {:ok, start_at, end_at} <- bounds_for_value(kind, value, timezone) do
      {:ok, %__MODULE__{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}}
    end
  end

  def bounds(kind, value, _timezone), do: {:error, {:invalid_window_value, kind, value}}

  @doc """
  Returns the current period containing `now` in `timezone`.
  """
  @spec current(kind_alias(), DateTime.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def current(kind, %DateTime{} = now, timezone) do
    with {:ok, kind} <- normalize_kind(kind),
         {:ok, start_at} <- floor(now, kind, timezone),
         {:ok, end_at} <- shift(start_at, kind, 1) do
      {:ok, %__MODULE__{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}}
    end
  end

  def current(_kind, now, _timezone), do: {:error, {:invalid_datetime, now}}

  @doc """
  Returns the previous complete period ending at or before `due_at`.
  """
  @spec previous_complete(kind_alias(), DateTime.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def previous_complete(kind, %DateTime{} = due_at, timezone) do
    with {:ok, kind} <- normalize_kind(kind),
         {:ok, end_at} <- floor(due_at, kind, timezone),
         {:ok, start_at} <- shift(end_at, kind, -1) do
      {:ok, %__MODULE__{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}}
    end
  end

  def previous_complete(_kind, due_at, _timezone), do: {:error, {:invalid_datetime, due_at}}

  @doc """
  Expands `[start_at, end_at)` into contiguous period bounds.

  The start and end datetimes are floored in `timezone`, preserving existing
  window range behavior where a partial trailing period is not included.
  """
  @spec expand_range(kind_alias(), DateTime.t(), DateTime.t(), String.t()) ::
          {:ok, [t()]} | {:error, term()}
  def expand_range(kind, %DateTime{} = start_at, %DateTime{} = end_at, timezone) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- Validate.timezone(timezone),
         :ok <- validate_order(start_at, end_at),
         {:ok, current} <- floor(start_at, kind, timezone),
         {:ok, boundary} <- floor(end_at, kind, timezone) do
      current
      |> Stream.unfold(fn cursor -> unfold_period(cursor, boundary, kind, timezone) end)
      |> Enum.to_list()
      |> then(&{:ok, &1})
    end
  end

  def expand_range(_kind, start_at, end_at, _timezone),
    do: {:error, {:invalid_period_range, start_at, end_at}}

  defp bounds_for_value(:hour, value, timezone) do
    case String.split(value, "T", parts: 2) do
      [date_raw, hour_raw] ->
        with {:ok, date} <- Date.from_iso8601(date_raw),
             {hour, ""} <- Integer.parse(hour_raw),
             true <- hour in 0..23,
             {:ok, start_at} <- datetime(date.year, date.month, date.day, hour, timezone),
             {:ok, end_at} <- shift(start_at, :hour, 1) do
          {:ok, start_at, end_at}
        else
          _error -> {:error, {:invalid_window_value, :hour, value}}
        end

      _other ->
        {:error, {:invalid_window_value, :hour, value}}
    end
  end

  defp bounds_for_value(:day, value, timezone) do
    with {:ok, date} <- Date.from_iso8601(value),
         {:ok, start_at} <- local_midnight(date, timezone),
         {:ok, end_at} <- local_midnight(Date.add(date, 1), timezone) do
      {:ok, start_at, end_at}
    else
      _error -> {:error, {:invalid_window_value, :day, value}}
    end
  end

  defp bounds_for_value(:month, value, timezone) do
    case String.split(value, "-", parts: 2) do
      [year_raw, month_raw] ->
        with {year, ""} <- Integer.parse(year_raw),
             {month, ""} <- Integer.parse(month_raw),
             {:ok, start_at} <- local_midnight(year, month, 1, timezone),
             {:ok, end_at} <- shift(start_at, :month, 1) do
          {:ok, start_at, end_at}
        else
          _error -> {:error, {:invalid_window_value, :month, value}}
        end

      _other ->
        {:error, {:invalid_window_value, :month, value}}
    end
  end

  defp bounds_for_value(:year, value, timezone) do
    with {year, ""} <- Integer.parse(value),
         {:ok, start_at} <- local_midnight(year, 1, 1, timezone),
         {:ok, end_at} <- local_midnight(year + 1, 1, 1, timezone) do
      {:ok, start_at, end_at}
    else
      _error -> {:error, {:invalid_window_value, :year, value}}
    end
  end

  defp floor_local(%DateTime{} = local, :hour) do
    {:ok, %{local | minute: 0, second: 0, microsecond: {0, 0}}}
  end

  defp floor_local(%DateTime{} = local, :day),
    do: local_midnight(local.year, local.month, local.day, local.time_zone)

  defp floor_local(%DateTime{} = local, :month),
    do: local_midnight(local.year, local.month, 1, local.time_zone)

  defp floor_local(%DateTime{} = local, :year),
    do: local_midnight(local.year, 1, 1, local.time_zone)

  defp shift_kind(%DateTime{} = datetime, :hour, count),
    do: {:ok, DateTime.add(datetime, count * 3600, :second)}

  defp shift_kind(%DateTime{} = datetime, :day, count) do
    datetime
    |> DateTime.to_date()
    |> Date.add(count)
    |> local_midnight(datetime.time_zone)
  end

  defp shift_kind(%DateTime{} = datetime, :month, count) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    local_midnight(div(total, 12), rem(total, 12) + 1, 1, datetime.time_zone)
  end

  defp shift_kind(%DateTime{} = datetime, :year, count) do
    date = DateTime.to_date(datetime)
    local_midnight(date.year + count, 1, 1, datetime.time_zone)
  end

  defp unfold_period(%DateTime{} = cursor, %DateTime{} = boundary, kind, timezone) do
    if DateTime.compare(cursor, boundary) == :lt do
      {:ok, next} = shift(cursor, kind, 1)
      {%__MODULE__{kind: kind, start_at: cursor, end_at: next, timezone: timezone}, next}
    else
      nil
    end
  end

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _other -> {:error, :invalid_period_bounds}
    end
  end

  defp datetime(year, month, day, hour, timezone) do
    with {:ok, date} <- Date.new(year, month, day),
         {:ok, time} <- Time.new(hour, 0, 0),
         {:ok, naive} <- NaiveDateTime.new(date, time) do
      {:ok, DateTime.from_naive!(naive, timezone, Favn.Timezone.database!())}
    end
  rescue
    ArgumentError -> {:error, {:invalid_timezone, timezone}}
  end

  defp local_midnight(%Date{} = date, timezone),
    do: local_midnight(date.year, date.month, date.day, timezone)

  defp local_midnight(year, month, day, timezone), do: datetime(year, month, day, 0, timezone)

  defp shift_zone(%DateTime{} = datetime, timezone) do
    case DateTime.shift_zone(datetime, timezone, Favn.Timezone.database!()) do
      {:ok, local} -> {:ok, local}
      {:error, _reason} -> {:error, {:invalid_timezone, timezone}}
    end
  end
end
