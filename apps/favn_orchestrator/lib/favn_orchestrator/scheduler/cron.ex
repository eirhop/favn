defmodule FavnOrchestrator.Scheduler.Cron do
  @moduledoc false

  @max_lookback_seconds 525_600 * 60

  @spec latest_due(String.t(), String.t(), DateTime.t()) :: DateTime.t() | nil
  def latest_due(cron, timezone, %DateTime{} = now_utc) do
    case parse(cron) do
      {:ok, expr} ->
        now = now_utc |> DateTime.shift_zone!(timezone) |> floor_for_expr(expr)
        from = DateTime.add(now, -@max_lookback_seconds, :second)

        expr
        |> find_previous(from, now, true)
        |> maybe_to_utc()

      :error ->
        nil
    end
  end

  @spec occurrences_between(String.t(), String.t(), DateTime.t(), DateTime.t(), keyword()) :: [
          DateTime.t()
        ]
  def occurrences_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc,
        opts \\ []
      ) do
    case parse(cron) do
      {:ok, expr} ->
        from = DateTime.shift_zone!(last_due_utc, timezone)
        to = DateTime.shift_zone!(latest_due_utc, timezone)
        limit = occurrence_limit(opts)

        collect_occurrences(expr, from, to, limit)
        |> Enum.map(&DateTime.shift_zone!(&1, "Etc/UTC"))

      :error ->
        []
    end
  end

  @spec first_occurrence_between(String.t(), String.t(), DateTime.t(), DateTime.t()) ::
          DateTime.t() | nil
  def first_occurrence_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc
      ) do
    case parse(cron) do
      {:ok, expr} ->
        from = DateTime.shift_zone!(last_due_utc, timezone)
        to = DateTime.shift_zone!(latest_due_utc, timezone)

        expr
        |> find_next(from, to)
        |> maybe_to_utc()

      :error ->
        nil
    end
  end

  @spec last_occurrence_between(String.t(), String.t(), DateTime.t(), DateTime.t()) ::
          DateTime.t() | nil
  def last_occurrence_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc
      ) do
    case parse(cron) do
      {:ok, expr} ->
        from = DateTime.shift_zone!(last_due_utc, timezone)
        to = DateTime.shift_zone!(latest_due_utc, timezone)

        expr
        |> find_previous(from, to, false)
        |> maybe_to_utc()

      :error ->
        nil
    end
  end

  @spec matches?(String.t(), DateTime.t()) :: boolean()
  def matches?(cron, %DateTime{} = dt) when is_binary(cron) do
    case parse(cron) do
      {:ok, expr} -> time_matches?(expr, dt) and date_matches?(expr, DateTime.to_date(dt))
      :error -> false
    end
  end

  defp parse(cron) when is_binary(cron) do
    case String.split(cron, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        build_expr(["0", minute, hour, day, month, weekday], 5)

      [second, minute, hour, day, month, weekday] ->
        build_expr([second, minute, hour, day, month, weekday], 6)

      _other ->
        :error
    end
  end

  defp parse(_cron), do: :error

  defp build_expr([second, minute, hour, day, month, weekday], field_count) do
    with {:ok, seconds} <- parse_field(second, 0, 59, & &1),
         {:ok, minutes} <- parse_field(minute, 0, 59, & &1),
         {:ok, hours} <- parse_field(hour, 0, 23, & &1),
         {:ok, days} <- parse_field(day, 1, 31, & &1),
         {:ok, months} <- parse_field(month, 1, 12, & &1),
         {:ok, weekdays} <- parse_field(weekday, 0, 7, &normalize_weekday/1) do
      {:ok,
       %{
         field_count: field_count,
         seconds: seconds,
         minutes: minutes,
         hours: hours,
         days: days,
         months: months,
         weekdays: weekdays,
         day_unrestricted?: length(days) == 31,
         weekday_unrestricted?: length(weekdays) == 7
       }}
    end
  end

  defp parse_field(field, min, max, normalize) do
    field
    |> String.split(",", trim: false)
    |> Enum.reduce_while(MapSet.new(), fn raw_token, values ->
      token = String.trim(raw_token)

      case parse_token(token, min, max, normalize) do
        {:ok, token_values} -> {:cont, MapSet.union(values, MapSet.new(token_values))}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      values -> parsed_values(values)
    end
  end

  defp parsed_values(values) do
    if MapSet.size(values) == 0 do
      :error
    else
      {:ok, values |> MapSet.to_list() |> Enum.sort()}
    end
  end

  defp parse_token("", _min, _max, _normalize), do: :error

  defp parse_token(token, min, max, normalize) do
    case String.split(token, "/", parts: 2) do
      [base] ->
        with {:ok, values, _origin} <- parse_base(base, min, max) do
          {:ok, Enum.map(values, normalize)}
        end

      [base, step] ->
        with {:ok, step} <- parse_positive_int(step),
             {:ok, values, origin} <- parse_base(base, min, max) do
          stepped =
            values
            |> Enum.filter(&(rem(&1 - origin, step) == 0))
            |> Enum.map(normalize)

          {:ok, stepped}
        end

      _other ->
        :error
    end
  end

  defp parse_base("*", min, max), do: {:ok, Enum.to_list(min..max), 0}

  defp parse_base(base, min, max) do
    case String.split(base, "-", parts: 2) do
      [single] ->
        with {:ok, value} <- parse_int(single), true <- value >= min and value <= max do
          {:ok, [value], value}
        else
          _ -> :error
        end

      [left, right] ->
        with {:ok, first} <- parse_int(left),
             {:ok, last} <- parse_int(right),
             true <- first <= last,
             true <- first >= min,
             true <- last <= max do
          {:ok, Enum.to_list(first..last), first}
        else
          _ -> :error
        end

      _other ->
        :error
    end
  end

  defp parse_int(value) do
    case Integer.parse(String.trim(value)) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp parse_positive_int(value) do
    case parse_int(value) do
      {:ok, int} when int > 0 -> {:ok, int}
      _other -> :error
    end
  end

  defp normalize_weekday(7), do: 0
  defp normalize_weekday(value), do: value

  defp occurrence_limit(opts) do
    case Keyword.get(opts, :limit, :infinity) do
      :infinity -> :infinity
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> :infinity
    end
  end

  defp collect_occurrences(expr, from, to, limit) do
    case DateTime.compare(from, to) do
      :lt ->
        from
        |> dates_until(to, :asc)
        |> Stream.flat_map(&occurrences_on_date(expr, &1, from, to, false, :asc))
        |> take_limit(limit)
        |> Enum.to_list()

      _other ->
        []
    end
  end

  defp take_limit(stream, :infinity), do: stream
  defp take_limit(stream, limit), do: Stream.take(stream, limit)

  defp find_next(expr, from, to) do
    case DateTime.compare(from, to) do
      :lt ->
        from
        |> dates_until(to, :asc)
        |> Enum.reduce_while(nil, fn date, _acc ->
          case occurrences_on_date(expr, date, from, to, false, :asc) do
            [candidate | _rest] -> {:halt, candidate}
            [] -> {:cont, nil}
          end
        end)

      _other ->
        nil
    end
  end

  defp find_previous(expr, from, to, include_from?) do
    case DateTime.compare(from, to) do
      comparison when comparison in [:lt, :eq] ->
        to
        |> dates_until(from, :desc)
        |> Enum.reduce_while(nil, fn date, _acc ->
          case occurrences_on_date(expr, date, from, to, include_from?, :desc) do
            [candidate | _rest] -> {:halt, candidate}
            [] -> {:cont, nil}
          end
        end)

      _other ->
        nil
    end
  end

  defp dates_until(%DateTime{} = start_dt, %DateTime{} = end_dt, direction) do
    dates_until(DateTime.to_date(start_dt), DateTime.to_date(end_dt), direction)
  end

  defp dates_until(start_date, end_date, :asc) do
    Stream.unfold(start_date, fn date ->
      if Date.compare(date, end_date) in [:lt, :eq] do
        {date, Date.add(date, 1)}
      else
        nil
      end
    end)
  end

  defp dates_until(start_date, end_date, :desc) do
    Stream.unfold(start_date, fn date ->
      if Date.compare(date, end_date) in [:gt, :eq] do
        {date, Date.add(date, -1)}
      else
        nil
      end
    end)
  end

  defp occurrences_on_date(expr, date, from, to, include_from?, direction) do
    if date_matches?(expr, date) do
      expr
      |> times_for_date(date, from, to, direction)
      |> Enum.flat_map(&date_time_candidates(date, &1, from.time_zone, direction))
      |> Enum.filter(&candidate_in_range?(&1, from, to, include_from?))
    else
      []
    end
  end

  defp times_for_date(expr, date, from, to, direction) do
    lower =
      if Date.compare(date, DateTime.to_date(from)) == :eq,
        do: DateTime.to_time(from),
        else: ~T[00:00:00]

    upper =
      if Date.compare(date, DateTime.to_date(to)) == :eq,
        do: DateTime.to_time(to),
        else: ~T[23:59:59]

    for hour <- ordered(expr.hours, direction),
        minute <- ordered(expr.minutes, direction),
        second <- ordered(expr.seconds, direction),
        time = Time.new!(hour, minute, second),
        Time.compare(time, lower) in [:gt, :eq],
        Time.compare(time, upper) in [:lt, :eq] do
      time
    end
  end

  defp ordered(values, :asc), do: values
  defp ordered(values, :desc), do: Enum.reverse(values)

  defp date_time_candidates(date, time, timezone, direction) do
    case DateTime.new(date, time, timezone) do
      {:ok, dt} ->
        [dt]

      {:ambiguous, first, second} ->
        order_candidates([first, second], direction)

      {:gap, _before_gap, _after_gap} ->
        []
    end
  end

  defp order_candidates(candidates, :asc) do
    Enum.sort(candidates, &(DateTime.compare(&1, &2) != :gt))
  end

  defp order_candidates(candidates, :desc) do
    Enum.sort(candidates, &(DateTime.compare(&1, &2) != :lt))
  end

  defp candidate_in_range?(candidate, from, to, include_from?) do
    lower = DateTime.compare(candidate, from)
    upper = DateTime.compare(candidate, to)

    lower_allowed? = lower == :gt or (include_from? and lower == :eq)
    upper_allowed? = upper in [:lt, :eq]

    lower_allowed? and upper_allowed?
  end

  defp time_matches?(expr, %DateTime{} = dt) do
    dt.second in expr.seconds and dt.minute in expr.minutes and dt.hour in expr.hours
  end

  defp date_matches?(expr, date) do
    month_match? = date.month in expr.months
    day_match? = date.day in expr.days
    weekday_match? = cron_weekday(date) in expr.weekdays

    month_match? and
      cond do
        expr.day_unrestricted? and expr.weekday_unrestricted? -> true
        expr.day_unrestricted? -> weekday_match?
        expr.weekday_unrestricted? -> day_match?
        true -> day_match? or weekday_match?
      end
  end

  defp cron_weekday(date) do
    case Date.day_of_week(date) do
      7 -> 0
      value -> value
    end
  end

  defp floor_for_expr(%DateTime{} = dt, %{field_count: 6}), do: %{dt | microsecond: {0, 0}}

  defp floor_for_expr(%DateTime{} = dt, _expr), do: %{dt | second: 0, microsecond: {0, 0}}

  defp maybe_to_utc(nil), do: nil
  defp maybe_to_utc(dt), do: DateTime.shift_zone!(dt, "Etc/UTC")
end
