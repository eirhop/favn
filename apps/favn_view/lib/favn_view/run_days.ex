defmodule FavnView.RunDays do
  @moduledoc """
  Groups runs into calendar days, including the days that have none.

  "Did this pipeline run every day this month?" is a question about absence, and
  a list can only answer it if the days with nothing in them are on screen. So
  when the filter bounds a range, every day in that range gets a row, whether or
  not a run landed on it — the gap is the answer.

  Consecutive empty days collapse into one `:gap` entry. Twenty-eight rows each
  saying "no runs" is the right answer told badly; one row saying "28 days with no
  runs" is the same fact in a form the operator can read at a glance.

  When the range is unbounded there is no set of days to enumerate, so only the
  days that have runs appear. A single day is not worth a header, so that case
  returns the runs flat and the caller renders one table.

  Days use the configured Favn default timezone, matching `FavnView.RunsFilters`.

  ## Examples

      iex> run = %{started_at_raw: ~U[2026-07-30 09:00:00Z], raw_status: :ok}
      iex> FavnView.RunDays.layout([run], {nil, nil}, ~U[2026-07-30 10:00:00Z])
      {:flat, [%{started_at_raw: ~U[2026-07-30 09:00:00Z], raw_status: :ok}]}
  """

  @max_days 62

  @type day ::
          %{
            kind: :day,
            date: Date.t() | nil,
            id: String.t(),
            label: String.t(),
            runs: [map()],
            total: non_neg_integer(),
            failed: non_neg_integer(),
            active: non_neg_integer()
          }
          | %{kind: :gap, id: String.t(), label: String.t(), days: pos_integer()}

  @doc """
  Decides whether the list needs day headers, and builds them if it does.

  `window` is a `FavnView.RunsFilters.window/2` result. An unbounded upper end is
  treated as `now`, so "last 30 days" enumerates thirty days rather than none.

  Options:

    * `:order` — `:started_desc` (default) or `:started_asc`
    * `:complete?` — whether `runs` is the whole filtered set. When it is not,
      days are only enumerated between the oldest and newest run that was loaded,
      because a day past the end of a truncated page is unknown rather than empty.
  """
  @spec layout([map()], {DateTime.t() | nil, DateTime.t() | nil}, DateTime.t(), keyword()) ::
          {:flat, [map()]} | {:days, [day()]}
  def layout(runs, window, now, opts \\ []) when is_list(runs) do
    by_date = Enum.group_by(runs, &run_date/1)
    order = Keyword.get(opts, :order, :started_desc)
    window = if Keyword.get(opts, :complete?, true), do: window, else: clamp(window, by_date)

    case dates(window, by_date, now) do
      dates when length(dates) < 2 -> {:flat, runs}
      dates -> {:days, days(dates, by_date, now, order)}
    end
  end

  # A truncated page cannot speak for days it never reached, so the range shrinks
  # to what was loaded and the days inside it are still filled in.
  defp clamp(window, by_date) do
    case by_date |> Map.keys() |> Enum.reject(&is_nil/1) |> Enum.min_max(fn -> nil end) do
      nil ->
        {nil, nil}

      {first, last} ->
        {bound(window, first), FavnView.Time.beginning_of_day(Date.add(last, 1))}
    end
  end

  defp bound({nil, _before_at}, first),
    do: FavnView.Time.beginning_of_day(first)

  defp bound({after_at, _before_at}, first) do
    floor = FavnView.Time.beginning_of_day(first)
    if DateTime.compare(after_at, floor) == :gt, do: after_at, else: floor
  end

  defp days(dates, by_date, now, order) do
    today = FavnView.Time.to_date(now)

    dates
    |> sort(order)
    |> Enum.map(&day(&1, Map.get(by_date, &1, []), today))
    |> collapse_gaps()
  end

  defp day(date, day_runs, today) do
    %{
      kind: :day,
      date: date,
      id: day_id(date),
      label: label(date, today),
      runs: day_runs,
      total: length(day_runs),
      failed: count(day_runs, [:error, :partial, :failed, :timed_out]),
      active: count(day_runs, [:running, :pending, :queued])
    }
  end

  # Every run of empty days becomes one row. The dates come from the days
  # themselves rather than from arithmetic, so a collapsed gap names exactly the
  # days it swallowed.
  defp collapse_gaps(days) do
    days
    |> Enum.chunk_by(&(&1.total == 0))
    |> Enum.flat_map(fn
      [%{total: 0} | _rest] = empty -> [gap(empty)]
      populated -> populated
    end)
  end

  defp gap([%{date: only}]), do: %{kind: :gap, id: day_id(only), label: gap_label(only), days: 1}

  defp gap(empty) do
    first = List.first(empty)
    last = List.last(empty)

    %{
      kind: :gap,
      id: "gap-#{day_id(first.date)}-#{day_id(last.date)}",
      label: "#{short_label(first.date)} to #{short_label(last.date)}",
      days: length(empty)
    }
  end

  defp gap_label(nil), do: "Not started"
  defp gap_label(date), do: short_label(date)

  defp short_label(nil), do: "unknown"
  defp short_label(date), do: Calendar.strftime(date, "%-d %b")

  # A bounded range enumerates its own days so the empty ones are visible. An
  # unbounded one can only report the days that actually hold runs.
  defp dates({nil, _before_at}, by_date, _now), do: Map.keys(by_date)

  defp dates({after_at, before_at}, by_date, now) do
    first = FavnView.Time.to_date(after_at)
    last = FavnView.Time.to_date(last_instant(before_at, now))

    cond do
      Date.compare(last, first) == :lt -> Map.keys(by_date)
      Date.diff(last, first) + 1 > @max_days -> Map.keys(by_date)
      true -> Enum.uniq(Enum.to_list(Date.range(first, last)) ++ Map.keys(by_date))
    end
  end

  # `started_before` is exclusive, so the last day it covers ends one second earlier.
  defp last_instant(nil, now), do: now
  defp last_instant(before_at, _now), do: DateTime.add(before_at, -1, :second)

  # A run with no start instant has no day at all, so its group goes after every
  # dated one whichever way the dated ones are ordered.
  defp sort(dates, order) do
    {undated, dated} = Enum.split_with(dates, &is_nil/1)
    Enum.sort(dated, sorter(order)) ++ Enum.take(undated, 1)
  end

  defp sorter(:started_asc), do: &(Date.compare(&1, &2) != :gt)
  defp sorter(_order), do: &(Date.compare(&1, &2) != :lt)

  defp run_date(run) do
    case Map.get(run, :started_at_raw) do
      %DateTime{} = started_at -> FavnView.Time.to_date(started_at)
      _absent -> nil
    end
  end

  defp count(runs, statuses),
    do: Enum.count(runs, &(Map.get(&1, :raw_status) in statuses))

  defp day_id(nil), do: "day-unknown"
  defp day_id(date), do: "day-" <> Date.to_iso8601(date)

  defp label(nil, _today), do: "Not started"

  defp label(date, today) do
    case Date.diff(today, date) do
      0 -> "Today"
      1 -> "Yesterday"
      _other when date.year == today.year -> Calendar.strftime(date, "%a %-d %b")
      _other -> Calendar.strftime(date, "%a %-d %b %Y")
    end
  end
end
