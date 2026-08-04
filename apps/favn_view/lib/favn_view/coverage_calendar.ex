defmodule FavnView.CoverageCalendar do
  @moduledoc """
  Lays out one screen of the periods an asset should hold data for.

  Coverage asks one question — is any period missing data — and a list of window keys
  answers it in the wrong shape. `day:Europe/Oslo:2026-07-08` has to be read character
  by character, and twenty of them in a row still do not say whether the gap is one bad
  week or every Sunday. A grid says both at a glance.

  ## One unit per grain

  Coverage is only ever hourly, daily, monthly, or yearly, and each wants a different
  screen. The unit is the next period up, which is also the step the navigator takes:

  | Coverage | One screen | Cells | Steps by |
  | --- | --- | --- | --- |
  | hourly | one day | 24 (23 or 25 across a clock change) | day |
  | daily | one month | 28 to 31, aligned to weekdays | month |
  | monthly | one year | 12 | year |
  | yearly | every year at once | one per year | nothing |

  One component rather than four, because the cells, the tones, the selection and the
  accessible names are identical and only the shape differs. Four would drift.

  ## Nothing is counted here

  Every cell comes from a window the backend reported, covered or not. The view never
  enumerates a range itself: an hour-grained day has 23, 24, or 25 hours depending on
  the clock change, and a calendar that assumed 24 would silently misplace a whole day
  twice a year. A period nobody looked at is absent rather than drawn, because a blank
  cell must never be mistaken for a covered one.

  ## Examples

      iex> FavnView.CoverageCalendar.build(%{
      ...>   kind: :day,
      ...>   timezone: "Europe/Oslo",
      ...>   windows: [
      ...>     %{window_key: "day:Europe/Oslo:2026-07-06", start_at: ~U[2026-07-06 00:00:00Z], covered?: true},
      ...>     %{window_key: "day:Europe/Oslo:2026-07-07", start_at: ~U[2026-07-07 00:00:00Z], covered?: false}
      ...>   ]
      ...> })
      ...> |> then(&{&1.layout, &1.unit_label, &1.missing_count})
      {:grid, "July 2026", 1}
  """

  @weekdays ~w(Mon Tue Wed Thu Fri Sat Sun)

  @type cell :: %{
          key: String.t() | nil,
          id: String.t(),
          label: String.t(),
          title: String.t(),
          state: :missing | :covered,
          selected?: boolean()
        }

  @type t :: %{
          layout: :grid | :empty,
          kind: atom() | nil,
          timezone: String.t() | nil,
          unit_label: String.t() | nil,
          blanks: non_neg_integer(),
          columns: pos_integer(),
          column_labels: [String.t()],
          cells: [cell()],
          period_count: non_neg_integer(),
          missing_count: non_neg_integer(),
          selected_count: non_neg_integer()
        }

  @doc """
  Builds one screen from the windows the backend reported for it.

  Expects `:kind`, `:timezone`, `:windows` (each with `:window_key`, `:start_at` and
  `:covered?`), and `:selected` (window keys the operator picked).
  """
  @spec build(map()) :: t()
  def build(attrs) when is_map(attrs) do
    kind = Map.get(attrs, :kind)
    windows = Map.get(attrs, :windows) || []
    selected = MapSet.new(Map.get(attrs, :selected) || [])
    cells = Enum.map(windows, &cell(&1, kind, selected))

    %{
      layout: if(cells == [], do: :empty, else: :grid),
      kind: kind,
      timezone: Map.get(attrs, :timezone),
      unit_label: unit_label(kind, windows),
      blanks: blanks(kind, windows),
      columns: columns(kind),
      column_labels: column_labels(kind),
      cells: cells,
      period_count: length(windows),
      missing_count: Enum.count(windows, &(Map.get(&1, :covered?) != true)),
      selected_count: MapSet.size(selected)
    }
  end

  @doc """
  Where the navigator can go from the screen on show.

  Steps and jump options are both bounded by the range coverage actually has, so an
  operator can reach the day coverage started and no further. A step with nowhere to go
  is `nil` rather than a disabled button, and a select with one option is left out
  entirely — a control that cannot change anything should not be on screen.

  Targets are plain dates. The instant inside the period is the caller's to build,
  because only the caller knows the timezone.

  Expects `:kind`, `:at` (the first period on screen), `:first_expected_at` and
  `:last_expected_at`.
  """
  @spec navigation(map()) :: %{
          previous: String.t() | nil,
          next: String.t() | nil,
          jumps: [map()]
        }
  def navigation(attrs) when is_map(attrs) do
    kind = Map.get(attrs, :kind)
    at = Map.get(attrs, :at)
    first = Map.get(attrs, :first_expected_at)
    last = Map.get(attrs, :last_expected_at)

    if is_nil(unit_noun(kind)) or is_nil(at) or is_nil(first) or is_nil(last) do
      %{previous: nil, next: nil, jumps: []}
    else
      bounded_navigation(kind, to_date(at), to_date(first), to_date(last))
    end
  end

  @doc """
  The unit to open on when nothing has been asked for.

  The newest one, because a gap that matters is usually recent and a multi-year range
  opened at its start puts the operator years from what they came to see.

  A grain with no unit above it is the exception: every period is already on one
  screen, so the screen has to start where coverage starts. Opening a year-grained
  asset on "the unit holding the last year" drew that one year and nothing else.

      iex> FavnView.CoverageCalendar.opening_date(%{
      ...>   kind: :day,
      ...>   first_expected_at: ~U[2026-01-05 00:00:00Z],
      ...>   last_expected_at: ~U[2026-07-19 00:00:00Z]
      ...> })
      ~D[2026-07-01]

      iex> FavnView.CoverageCalendar.opening_date(%{
      ...>   kind: :year,
      ...>   first_expected_at: ~U[2022-01-01 00:00:00Z],
      ...>   last_expected_at: ~U[2025-01-01 00:00:00Z]
      ...> })
      ~D[2022-01-01]
  """
  @spec opening_date(map()) :: Date.t() | nil
  def opening_date(attrs) when is_map(attrs) do
    kind = Map.get(attrs, :kind)
    first = Map.get(attrs, :first_expected_at)
    last = Map.get(attrs, :last_expected_at)

    cond do
      is_nil(unit_noun(kind)) -> first && to_date(first)
      is_nil(last) -> first && to_date(first)
      true -> unit_start(kind, to_date(last))
    end
  end

  @doc """
  The dates bounding the unit holding `date`, the second exclusive.

  This is what the screen asks the backend for. It is a range and not a count because
  a count cannot name a calendar unit: February holds 28 days and a clock change makes
  a day hold 23 or 25 hours, so "31 days from 1 February" reaches into March. Asking
  for the unit's own bounds is the only way to get exactly the unit.

  `nil` as the upper bound means read to the end of coverage, which is what a grain
  with no unit above it wants — every year belongs on one screen.

      iex> FavnView.CoverageCalendar.unit_bounds(:day, ~D[2026-02-17])
      {~D[2026-02-01], ~D[2026-03-01]}

      iex> FavnView.CoverageCalendar.unit_bounds(:hour, ~D[2026-03-29])
      {~D[2026-03-29], ~D[2026-03-30]}

      iex> FavnView.CoverageCalendar.unit_bounds(:month, ~D[2026-07-08])
      {~D[2026-01-01], ~D[2027-01-01]}

      iex> FavnView.CoverageCalendar.unit_bounds(:year, ~D[2026-07-08])
      {~D[2026-07-08], nil}
  """
  @spec unit_bounds(atom() | nil, Date.t() | nil) :: {Date.t() | nil, Date.t() | nil}
  def unit_bounds(_kind, nil), do: {nil, nil}

  def unit_bounds(kind, %Date{} = date) do
    if is_nil(unit_noun(kind)) do
      {date, nil}
    else
      from = unit_start(kind, date)
      {from, shift_unit(kind, from, 1)}
    end
  end

  @doc """
  The unit a jump selected, clamped into the range coverage has.

  Whichever select the operator changed, the others keep their value, so changing the
  year of a February screen lands on February. A combination that falls outside the
  range is clamped rather than refused, because the selects only ever offer values
  inside it and clamping is what makes a stale form harmless.
  """
  @spec jump_target(map(), map()) :: Date.t() | nil
  def jump_target(attrs, params) when is_map(attrs) and is_map(params) do
    kind = Map.get(attrs, :kind)
    at = Map.get(attrs, :at)
    first = Map.get(attrs, :first_expected_at)
    last = Map.get(attrs, :last_expected_at)

    if is_nil(unit_noun(kind)) or is_nil(at) or is_nil(first) or is_nil(last) do
      nil
    else
      current = to_date(at)

      year = integer_param(params, "year", current.year)
      month = integer_param(params, "month", current.month)
      day = integer_param(params, "day", current.day)

      kind
      |> jump_date(year, month, day)
      |> clamp(unit_start(kind, to_date(first)), unit_start(kind, to_date(last)))
    end
  end

  defp bounded_navigation(kind, at, first, last) do
    current = unit_start(kind, at)
    floor = unit_start(kind, first)
    ceiling = unit_start(kind, last)

    %{
      previous: step_target(kind, current, -1, floor, ceiling),
      next: step_target(kind, current, 1, floor, ceiling),
      jumps: jumps(kind, current, first, last)
    }
  end

  defp step_target(kind, current, direction, floor, ceiling) do
    target = shift_unit(kind, current, direction)

    if Date.compare(target, floor) != :lt and Date.compare(target, ceiling) != :gt,
      do: Date.to_iso8601(target)
  end

  # The unit the screen belongs to, as its first date: the day for hourly coverage, the
  # first of the month for daily, the first of January for monthly.
  defp unit_start(:hour, date), do: date
  defp unit_start(:day, date), do: %{date | day: 1}
  defp unit_start(:month, date), do: %{date | month: 1, day: 1}
  defp unit_start(_kind, date), do: date

  defp shift_unit(:hour, date, count), do: Date.add(date, count)
  defp shift_unit(:day, date, count), do: shift_months(date, count)
  defp shift_unit(:month, date, count), do: %{date | year: date.year + count}
  defp shift_unit(_kind, date, _count), do: date

  defp shift_months(date, count) do
    index = date.year * 12 + date.month - 1 + count
    %{date | year: div(index, 12), month: rem(index, 12) + 1}
  end

  # Coarsest first, and only where there is a choice to make. A year select holding one
  # year is a control that does nothing.
  defp jumps(kind, current, first, last) do
    [year_jump(current, first, last), month_jump(kind, current, first, last)]
    |> Enum.concat([day_jump(kind, current, first, last)])
    |> Enum.reject(&(is_nil(&1) or length(&1.options) < 2))
  end

  defp year_jump(current, first, last) do
    %{
      name: "year",
      label: "Year",
      value: Integer.to_string(current.year),
      options: Enum.map(first.year..last.year, &{Integer.to_string(&1), Integer.to_string(&1)})
    }
  end

  defp month_jump(:month, _current, _first, _last), do: nil

  defp month_jump(_kind, current, first, last) do
    %{
      name: "month",
      label: "Month",
      value: Integer.to_string(current.month),
      options: Enum.map(months_in(current.year, first, last), &month_option/1)
    }
  end

  defp day_jump(:hour, current, first, last) do
    %{
      name: "day",
      label: "Day",
      value: Integer.to_string(current.day),
      options:
        current.year
        |> days_in(current.month, first, last)
        |> Enum.map(&{Integer.to_string(&1), Integer.to_string(&1)})
    }
  end

  defp day_jump(_kind, _current, _first, _last), do: nil

  # Only the months this year actually holds coverage for, so the first and last year
  # of a range offer a part-year rather than all twelve.
  defp months_in(year, first, last) do
    from = if year == first.year, do: first.month, else: 1
    through = if year == last.year, do: last.month, else: 12
    if through < from, do: [], else: Enum.to_list(from..through)
  end

  defp days_in(year, month, first, last) do
    from = if year == first.year and month == first.month, do: first.day, else: 1

    through =
      if year == last.year and month == last.month,
        do: last.day,
        else: Date.days_in_month(Date.new!(year, month, 1))

    if through < from, do: [], else: Enum.to_list(from..through)
  end

  defp month_option(month),
    do: {Calendar.strftime(Date.new!(2000, month, 1), "%B"), Integer.to_string(month)}

  defp jump_date(:hour, year, month, day),
    do: Date.new!(year, month, min(day, Date.days_in_month(Date.new!(year, month, 1))))

  defp jump_date(:day, year, month, _day), do: Date.new!(year, month, 1)
  defp jump_date(_kind, year, _month, _day), do: Date.new!(year, 1, 1)

  defp clamp(date, floor, ceiling) do
    cond do
      Date.compare(date, floor) == :lt -> floor
      Date.compare(date, ceiling) == :gt -> ceiling
      true -> date
    end
  end

  defp integer_param(params, name, fallback) do
    case params |> Map.get(name) |> to_integer() do
      nil -> fallback
      value -> value
    end
  end

  defp to_integer(value) when is_integer(value), do: value

  defp to_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _other -> nil
    end
  end

  defp to_integer(_value), do: nil

  defp to_date(%DateTime{} = at), do: DateTime.to_date(at)
  defp to_date(%Date{} = date), do: date

  @doc """
  The unit one screen covers, in the words its navigator uses.

      iex> FavnView.CoverageCalendar.unit_noun(:day)
      "month"

      iex> FavnView.CoverageCalendar.unit_noun(:year)
      nil
  """
  @spec unit_noun(atom() | nil) :: String.t() | nil
  def unit_noun(:hour), do: "day"
  def unit_noun(:day), do: "month"
  def unit_noun(:month), do: "year"
  def unit_noun(_kind), do: nil

  @doc """
  The plain name for one period of the given grain, singular or plural.

      iex> FavnView.CoverageCalendar.period_noun(:day, 1)
      "day"

      iex> FavnView.CoverageCalendar.period_noun(:month, 4)
      "months"
  """
  @spec period_noun(atom() | nil, integer()) :: String.t()
  def period_noun(kind, count) when count == 1, do: singular_noun(kind)
  def period_noun(kind, _count), do: singular_noun(kind) <> "s"

  defp singular_noun(:hour), do: "hour"
  defp singular_noun(:day), do: "day"
  defp singular_noun(:month), do: "month"
  defp singular_noun(:year), do: "year"
  defp singular_noun(_kind), do: "period"

  @doc """
  Names one period the way a person would write it.

      iex> FavnView.CoverageCalendar.period_label(:day, ~U[2026-07-08 00:00:00Z])
      "8 July 2026"

      iex> FavnView.CoverageCalendar.period_label(:month, ~U[2026-07-01 00:00:00Z])
      "July 2026"
  """
  @spec period_label(atom() | nil, DateTime.t() | nil) :: String.t() | nil
  def period_label(_kind, nil), do: nil
  def period_label(:hour, %DateTime{} = at), do: Calendar.strftime(at, "%H:%M on %-d %B %Y")
  def period_label(:day, %DateTime{} = at), do: Calendar.strftime(at, "%-d %B %Y")
  def period_label(:month, %DateTime{} = at), do: Calendar.strftime(at, "%B %Y")
  def period_label(:year, %DateTime{} = at), do: Calendar.strftime(at, "%Y")
  def period_label(_kind, %DateTime{} = at), do: Calendar.strftime(at, "%-d %B %Y")

  # Hours already read as a clock, days as a date; a month name or a year is the whole
  # cell. Yearly coverage names the span rather than one year, because every year on
  # screen at once has no single unit above it.
  defp unit_label(_kind, []), do: nil
  defp unit_label(:hour, [first | _rest]), do: Calendar.strftime(first.start_at, "%A %-d %B %Y")
  defp unit_label(:day, [first | _rest]), do: Calendar.strftime(first.start_at, "%B %Y")
  defp unit_label(:month, [first | _rest]), do: Calendar.strftime(first.start_at, "%Y")

  defp unit_label(:year, windows) do
    first = List.first(windows).start_at
    last = List.last(windows).start_at

    if first.year == last.year,
      do: Integer.to_string(first.year),
      else: "#{first.year} to #{last.year}"
  end

  defp unit_label(_kind, _windows), do: nil

  # A month rarely starts on a Monday, so its first cell needs padding to land in its
  # own weekday column. Every other grain fills from the left.
  defp blanks(:day, [first | _rest]), do: Date.day_of_week(DateTime.to_date(first.start_at)) - 1
  defp blanks(_kind, _windows), do: 0

  defp columns(:hour), do: 6
  defp columns(:day), do: 7
  defp columns(:month), do: 4
  defp columns(_kind), do: 5

  defp column_labels(:day), do: @weekdays
  defp column_labels(_kind), do: []

  defp cell(window, kind, selected) do
    key = Map.get(window, :window_key)
    covered? = Map.get(window, :covered?) == true

    %{
      # A covered period is not selectable, so it carries no key to select it with.
      key: if(covered?, do: nil, else: key),
      id: "coverage-#{key}",
      label: cell_label(kind, window.start_at),
      title: period_label(kind, window.start_at),
      state: if(covered?, do: :covered, else: :missing),
      selected?: not covered? and MapSet.member?(selected, key)
    }
  end

  defp cell_label(:hour, at), do: Calendar.strftime(at, "%H:%M")
  defp cell_label(:day, at), do: Integer.to_string(at.day)
  defp cell_label(:month, at), do: Calendar.strftime(at, "%b")
  defp cell_label(:year, at), do: Integer.to_string(at.year)
  defp cell_label(_kind, at), do: Calendar.strftime(at, "%-d %b")
end
