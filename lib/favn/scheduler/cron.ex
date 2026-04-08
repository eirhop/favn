defmodule Favn.Scheduler.Cron do
  @moduledoc false

  @spec latest_due(String.t(), String.t(), DateTime.t()) :: DateTime.t() | nil
  def latest_due(cron, timezone, %DateTime{} = now_utc) do
    now = floor_to_minute(DateTime.shift_zone!(now_utc, timezone))
    find_latest(cron, now, 525_600) |> maybe_to_utc()
  end

  @spec occurrences_between(String.t(), String.t(), DateTime.t(), DateTime.t()) :: [DateTime.t()]
  def occurrences_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc
      ) do
    from = DateTime.shift_zone!(last_due_utc, timezone)
    to = DateTime.shift_zone!(latest_due_utc, timezone)

    cursor = DateTime.add(floor_to_minute(from), 60, :second)

    Stream.unfold(cursor, fn current ->
      if DateTime.compare(current, to) in [:lt, :eq] do
        {current, DateTime.add(current, 60, :second)}
      else
        nil
      end
    end)
    |> Enum.filter(&matches?(cron, &1))
    |> Enum.map(&DateTime.shift_zone!(&1, "Etc/UTC"))
  end

  @spec first_occurrence_between(String.t(), String.t(), DateTime.t(), DateTime.t()) ::
          DateTime.t() | nil
  def first_occurrence_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc
      ) do
    from = DateTime.shift_zone!(last_due_utc, timezone)
    to = DateTime.shift_zone!(latest_due_utc, timezone)
    cursor = DateTime.add(floor_to_minute(from), 60, :second)
    find_forward(cron, cursor, to)
  end

  @spec last_occurrence_between(String.t(), String.t(), DateTime.t(), DateTime.t()) ::
          DateTime.t() | nil
  def last_occurrence_between(
        cron,
        timezone,
        %DateTime{} = last_due_utc,
        %DateTime{} = latest_due_utc
      ) do
    from = DateTime.shift_zone!(last_due_utc, timezone)
    to = DateTime.shift_zone!(latest_due_utc, timezone)
    cursor = floor_to_minute(to)
    find_backward_after(cron, cursor, from)
  end

  @spec matches?(String.t(), DateTime.t()) :: boolean()
  def matches?(cron, %DateTime{} = dt) when is_binary(cron) do
    case String.split(cron, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        day_match = match_field?(day, dt.day)
        weekday_match = match_weekday?(weekday, dt)

        match_field?(minute, dt.minute) and
          match_field?(hour, dt.hour) and
          match_field?(month, dt.month) and
          match_day_constraints?(day, day_match, weekday, weekday_match)

      _ ->
        false
    end
  end

  defp match_day_constraints?(day_field, day_match, weekday_field, weekday_match) do
    day_unrestricted? = day_of_month_unrestricted?(day_field)
    weekday_unrestricted? = day_of_week_unrestricted?(weekday_field)

    cond do
      day_unrestricted? and weekday_unrestricted? -> true
      day_unrestricted? -> weekday_match
      weekday_unrestricted? -> day_match
      true -> day_match or weekday_match
    end
  end

  defp day_of_month_unrestricted?(field) do
    Enum.all?(1..31, &match_field?(field, &1))
  end

  defp day_of_week_unrestricted?(field) do
    Enum.all?(0..6, fn value ->
      match_field?(field, value) or (value == 0 and match_field?(field, 7))
    end)
  end

  defp match_weekday?(field, dt) do
    weekday = Date.day_of_week(DateTime.to_date(dt))
    cron_weekday = if weekday == 7, do: 0, else: weekday
    match_field?(field, cron_weekday) or (cron_weekday == 0 and match_field?(field, 7))
  end

  defp match_field?(field, value) do
    field
    |> String.split(",", trim: true)
    |> Enum.any?(&match_token?(&1, value))
  end

  defp match_token?("*", _value), do: true

  defp match_token?(token, value) do
    case String.split(token, "/", parts: 2) do
      [base, step] ->
        with {step_int, ""} when step_int > 0 <- Integer.parse(step),
             true <- base_match?(base, value),
             true <- rem(value - step_origin(base), step_int) == 0 do
          true
        else
          _ -> false
        end

      [base] ->
        base_match?(base, value)

      _ ->
        false
    end
  end

  defp base_match?("*", _value), do: true

  defp base_match?(base, value) do
    case String.split(base, "-", parts: 2) do
      [single] ->
        parse_int(single) == value

      [left, right] ->
        l = parse_int(left)
        r = parse_int(right)
        is_integer(l) and is_integer(r) and value >= l and value <= r

      _ ->
        false
    end
  end

  defp step_origin("*"), do: 0

  defp step_origin(base) do
    case String.split(base, "-", parts: 2) do
      [single] -> parse_int(single) || 0
      [left, _right] -> parse_int(left) || 0
      _ -> 0
    end
  end

  defp parse_int(value) do
    case Integer.parse(String.trim(value)) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp floor_to_minute(%DateTime{} = dt), do: %{dt | second: 0, microsecond: {0, 0}}

  defp maybe_to_utc(nil), do: nil
  defp maybe_to_utc(dt), do: DateTime.shift_zone!(dt, "Etc/UTC")

  defp find_latest(_cron, _cursor, 0), do: nil

  defp find_latest(cron, cursor, remaining) do
    if matches?(cron, cursor) do
      cursor
    else
      find_latest(cron, DateTime.add(cursor, -60, :second), remaining - 1)
    end
  end

  defp find_forward(cron, cursor, to) do
    case DateTime.compare(cursor, to) do
      :gt ->
        nil

      _ ->
        if matches?(cron, cursor) do
          DateTime.shift_zone!(cursor, "Etc/UTC")
        else
          find_forward(cron, DateTime.add(cursor, 60, :second), to)
        end
    end
  end

  defp find_backward_after(cron, cursor, from) do
    case DateTime.compare(cursor, from) do
      :gt ->
        if matches?(cron, cursor) do
          DateTime.shift_zone!(cursor, "Etc/UTC")
        else
          find_backward_after(cron, DateTime.add(cursor, -60, :second), from)
        end

      _ ->
        nil
    end
  end
end
