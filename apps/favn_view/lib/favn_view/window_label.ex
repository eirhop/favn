defmodule FavnView.WindowLabel do
  @moduledoc """
  Names one coverage window by the calendar period it covers.

  A window is a half-open interval, so the honest full form is a range of two
  timestamps. That form is unreadable wherever windows are compared: a day
  window renders as fifty-eight characters that differ from the next window's in
  two of them, and the operator has to read both ends to find the one digit that
  changed.

  So a window that covers exactly one whole calendar period is named by that
  period — `Jul 17, 2026` rather than `Jul 17, 2026 00:00:00 UTC – Jul 18, 2026
  00:00:00 UTC`. Anything else keeps the range, because a partial or irregular
  window has no shorter true name.

  Whole-period detection happens in the display timezone, which is the only
  place a day is a day: a `day` window in `Europe/Oslo` is 23 or 25 hours across
  a daylight-saving boundary, and its UTC bounds are not midnight at all.
  `full/3` remains available for the tooltip, so the exact bounds are one hover
  away wherever the compact form is shown.
  """

  alias Favn.Timezone
  alias FavnView.Time

  @type configuration :: String.t() | map()
  @type unit :: :hour | :day | :month | :year

  @doc """
  Returns the shortest true name for the window, or `nil` when there is none.

  ## Examples

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.compact(~U[2026-07-17 00:00:00Z], ~U[2026-07-18 00:00:00Z], "Etc/UTC")
      "Jul 17, 2026"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.compact(~U[2026-07-17 14:00:00Z], ~U[2026-07-17 15:00:00Z], "Etc/UTC")
      "Jul 17, 14:00"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.compact(~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z], "Etc/UTC")
      "Jul 2026"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.compact(~U[2026-07-17 09:30:00Z], ~U[2026-07-17 11:15:00Z], "Etc/UTC")
      "Jul 17, 2026 09:30 – 11:15"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.compact(nil, nil, "Etc/UTC")
      nil
  """
  @spec compact(DateTime.t() | nil, DateTime.t() | nil, configuration()) :: String.t() | nil
  def compact(nil, nil, _configuration), do: nil

  def compact(%DateTime{} = start_at, %DateTime{} = end_at, configuration) do
    start_local = Time.shift(start_at, configuration)
    end_local = Time.shift(end_at, configuration)

    period(start_local, end_local) || range(start_local, end_local)
  end

  def compact(start_at, end_at, configuration), do: full(start_at, end_at, configuration)

  @doc """
  Names a stretch of contiguous whole periods by its first and last one.

  A combined run and a flat rail both cover many windows at once, and their
  coverage has the same two ends as a single window: a start bound and an
  exclusive end bound. `compact/3` can only name that as a period or a range,
  and twenty-four months is neither — it renders as
  `Jan 1, 2023 00:00 – Jan 1, 2025 00:00`, which states the boundary between two
  years twice and never says how much lies between them.

  Given the unit those windows are counted in, this names the first and last
  period the coverage actually contains, so the same stretch reads
  `Jan 2023 – Dec 2024`. The exclusive end bound is never named: `Jan 2025` is
  the bound, not a month the coverage includes.

  Coverage that is not whole periods of the unit, or a unit that is not known,
  falls back to `compact/3` rather than rounding to a period it does not cover.

  ## Examples

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.span(~U[2023-01-01 00:00:00Z], ~U[2025-01-01 00:00:00Z], :month, "Etc/UTC")
      "Jan 2023 – Dec 2024"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.span(~U[2026-03-01 00:00:00Z], ~U[2026-03-09 00:00:00Z], :day, "Etc/UTC")
      "Mar 1, 2026 – Mar 8, 2026"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.span(~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z], :month, "Etc/UTC")
      "Jul 2026"

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.span(~U[2026-07-17 09:30:00Z], ~U[2026-07-18 11:15:00Z], :day, "Etc/UTC")
      "Jul 17 09:30 – Jul 18 11:15, 2026"
  """
  @spec span(DateTime.t() | nil, DateTime.t() | nil, unit() | nil, configuration()) ::
          String.t() | nil
  def span(start_at, end_at, unit, configuration)

  def span(%DateTime{} = start_at, %DateTime{} = end_at, unit, configuration)
      when unit in [:hour, :day, :month, :year] do
    start_local = Time.shift(start_at, configuration)
    end_local = Time.shift(end_at, configuration)

    with true <- boundary?(start_local, unit),
         true <- boundary?(end_local, unit),
         true <- DateTime.compare(end_local, start_local) == :gt do
      name_span(start_local, end_local, unit)
    else
      _partial -> compact(start_at, end_at, configuration)
    end
  end

  def span(start_at, end_at, _unit, configuration), do: compact(start_at, end_at, configuration)

  @doc """
  Returns both bounds in full, for the tooltip behind a compact label.

  ## Examples

      iex> alias FavnView.WindowLabel
      iex> WindowLabel.full(~U[2026-07-17 00:00:00Z], ~U[2026-07-18 00:00:00Z], "Etc/UTC")
      "Jul 17, 2026 00:00:00 UTC – Jul 18, 2026 00:00:00 UTC"
  """
  @spec full(DateTime.t() | nil, DateTime.t() | nil, configuration()) :: String.t() | nil
  def full(nil, nil, _configuration), do: nil

  def full(start_at, end_at, configuration) do
    [start_at, end_at]
    |> Enum.map(&stamp(&1, configuration))
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      parts -> Enum.join(parts, " – ")
    end
  end

  defp stamp(%DateTime{} = value, configuration),
    do: Time.format(value, "%b %-d, %Y %H:%M:%S %Z", configuration)

  defp stamp(_value, _configuration), do: nil

  # A whole period is recognised from the local calendar fields rather than from
  # the elapsed seconds, so a daylight-saving day of 23 or 25 hours is still one
  # day and an hour repeated by a backward transition is still one hour.
  defp period(start_local, end_local) do
    cond do
      year?(start_local, end_local) -> Calendar.strftime(start_local, "%Y")
      month?(start_local, end_local) -> Calendar.strftime(start_local, "%b %Y")
      day?(start_local, end_local) -> Calendar.strftime(start_local, "%b %-d, %Y")
      hour?(start_local, end_local) -> Calendar.strftime(start_local, "%b %-d, %H:%M")
      true -> nil
    end
  end

  # The last period a stretch covers is found from inside the coverage rather
  # than by subtracting a calendar unit from the exclusive end: one second back
  # is always within the final period, whatever that period's length turned out
  # to be, so a 23-hour daylight-saving day and a 28-day February need no case
  # of their own.
  defp name_span(start_local, end_local, unit) do
    last_local = DateTime.add(end_local, -1, :second, Timezone.database!())

    case {period_name(start_local, unit), period_name(last_local, unit)} do
      {same, same} -> same
      {_first, _last} -> "#{ends_name(start_local, unit)} – #{ends_name(last_local, unit)}"
    end
  end

  # Only the hour form leaves its year implicit, because one hour is always read
  # beside the day it belongs to. The two ends of a stretch are not: an hourly
  # run can cover a year boundary, and `Dec 31, 23:00 – Jan 1, 05:00` says which
  # hours without saying which December.
  defp ends_name(local, :hour), do: Calendar.strftime(local, "%b %-d, %Y %H:00")
  defp ends_name(local, unit), do: period_name(local, unit)

  defp period_name(local, :year), do: Calendar.strftime(local, "%Y")
  defp period_name(local, :month), do: Calendar.strftime(local, "%b %Y")
  defp period_name(local, :day), do: Calendar.strftime(local, "%b %-d, %Y")
  defp period_name(local, :hour), do: Calendar.strftime(local, "%b %-d, %H:00")

  defp boundary?(local, :year), do: midnight?(local) and local.month == 1 and local.day == 1
  defp boundary?(local, :month), do: midnight?(local) and local.day == 1
  defp boundary?(local, :day), do: midnight?(local)
  defp boundary?(local, :hour), do: on_the_hour?(local)

  defp year?(start_local, end_local) do
    midnight?(start_local) and midnight?(end_local) and
      start_local.month == 1 and start_local.day == 1 and
      end_local.month == 1 and end_local.day == 1 and
      end_local.year == start_local.year + 1
  end

  defp month?(start_local, end_local) do
    midnight?(start_local) and midnight?(end_local) and
      start_local.day == 1 and end_local.day == 1 and
      next_month?(start_local, end_local)
  end

  defp day?(start_local, end_local) do
    midnight?(start_local) and midnight?(end_local) and
      Date.diff(DateTime.to_date(end_local), DateTime.to_date(start_local)) == 1
  end

  defp hour?(start_local, end_local) do
    on_the_hour?(start_local) and on_the_hour?(end_local) and
      DateTime.diff(end_local, start_local, :second) == 3_600
  end

  defp next_month?(%{year: year, month: 12}, %{year: end_year, month: 1}),
    do: end_year == year + 1

  defp next_month?(%{year: year, month: month}, %{year: end_year, month: end_month}),
    do: end_year == year and end_month == month + 1

  defp midnight?(value), do: on_the_hour?(value) and value.hour == 0

  defp on_the_hour?(value), do: value.minute == 0 and value.second == 0

  # A partial window still drops everything the two ends share: within one day
  # the date is said once, and within one year the year is.
  defp range(start_local, end_local) do
    cond do
      DateTime.to_date(start_local) == DateTime.to_date(end_local) ->
        Calendar.strftime(start_local, "%b %-d, %Y %H:%M") <>
          " – " <> Calendar.strftime(end_local, "%H:%M")

      start_local.year == end_local.year ->
        Calendar.strftime(start_local, "%b %-d %H:%M") <>
          " – " <> Calendar.strftime(end_local, "%b %-d %H:%M, %Y")

      true ->
        Calendar.strftime(start_local, "%b %-d, %Y %H:%M") <>
          " – " <> Calendar.strftime(end_local, "%b %-d, %Y %H:%M")
    end
  end
end
