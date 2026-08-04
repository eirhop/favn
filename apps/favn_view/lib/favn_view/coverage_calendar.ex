defmodule FavnView.CoverageCalendar do
  @moduledoc """
  Lays out the periods an asset should hold data for as a calendar.

  Coverage asks one question — is any period missing data — and a list of window
  keys answers it in the wrong shape. `day:Europe/Oslo:2026-07-08` has to be read
  character by character, and twenty of them in a row still do not say whether the
  gap is one bad week or every Sunday. A grid says both at a glance.

  The grid draws exactly the periods the backend reported examining. A period past
  the end of that range is neither covered nor missing: nothing looked at it, so it
  is absent from the calendar rather than drawn as good news.

  Days, months, and years get a real grid. Hours get a grouped list of the missing
  ones instead, because stepping hour by hour across a daylight-saving boundary is
  timezone arithmetic and a screen of 366 hour cells is not a calendar anyone reads.

  ## Examples

      iex> FavnView.CoverageCalendar.build(%{
      ...>   examined: %{
      ...>     kind: :day,
      ...>     timezone: "Europe/Oslo",
      ...>     from: ~U[2026-07-06 00:00:00Z],
      ...>     through: ~U[2026-07-08 00:00:00Z],
      ...>     count: 3
      ...>   },
      ...>   gaps: [%{window_key: "day:Europe/Oslo:2026-07-07", start_at: ~U[2026-07-07 00:00:00Z]}]
      ...> })
      ...> |> then(&{&1.layout, &1.missing_count, Enum.map(&1.groups, fn group -> group.label end)})
      {:grid, 1, ["July 2026"]}
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

  @type group :: %{
          id: String.t(),
          label: String.t(),
          blanks: non_neg_integer(),
          cells: [cell()]
        }

  @type t :: %{
          layout: :grid | :list | :empty,
          kind: atom() | nil,
          timezone: String.t() | nil,
          period_noun: String.t(),
          columns: pos_integer(),
          column_labels: [String.t()],
          groups: [group()],
          from_label: String.t() | nil,
          through_label: String.t() | nil,
          examined_count: non_neg_integer(),
          missing_count: non_neg_integer(),
          selected_count: non_neg_integer()
        }

  @doc """
  Builds the calendar from one page of missing coverage windows.

  Expects `:examined` (the range the page compared against evidence), `:gaps` (the
  windows it found missing), and `:selected` (window keys the operator picked).
  """
  @spec build(map()) :: t()
  def build(attrs) when is_map(attrs) do
    examined = Map.get(attrs, :examined) || %{}
    kind = Map.get(examined, :kind)
    gaps = Map.get(attrs, :gaps) || []
    selected = MapSet.new(Map.get(attrs, :selected) || [])
    periods = periods(kind, Map.get(examined, :from), Map.get(examined, :through))

    %{
      layout: layout(kind, periods),
      kind: kind,
      timezone: Map.get(examined, :timezone),
      period_noun: period_noun(kind),
      columns: columns(kind),
      column_labels: column_labels(kind),
      groups: groups(kind, periods, gaps, selected),
      from_label: period_label(kind, Map.get(examined, :from)),
      through_label: period_label(kind, Map.get(examined, :through)),
      examined_count: Map.get(examined, :count, 0),
      missing_count: length(gaps),
      selected_count: MapSet.size(selected)
    }
  end

  @doc """
  The plain name for one period of the given cadence, singular or plural.

      iex> FavnView.CoverageCalendar.period_noun(:day, 1)
      "day"

      iex> FavnView.CoverageCalendar.period_noun(:month, 4)
      "months"
  """
  @spec period_noun(atom() | nil, integer()) :: String.t()
  def period_noun(kind, count) when count == 1, do: period_noun(kind)
  def period_noun(kind, _count), do: period_noun(kind) <> "s"

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

  @spec period_noun(atom() | nil) :: String.t()
  def period_noun(:hour), do: "hour"
  def period_noun(:day), do: "day"
  def period_noun(:month), do: "month"
  def period_noun(:year), do: "year"
  def period_noun(_kind), do: "period"

  defp layout(_kind, []), do: :empty
  defp layout(:hour, _periods), do: :list
  defp layout(_kind, _periods), do: :grid

  defp columns(:day), do: 7
  defp columns(:month), do: 6
  defp columns(_kind), do: 6

  defp column_labels(:day), do: @weekdays
  defp column_labels(_kind), do: []

  # Hours never enumerate: stepping across a daylight-saving boundary is timezone
  # arithmetic the view has no business doing, and the list only needs the gaps.
  defp periods(:hour, from, _through) when not is_nil(from), do: [:listed]
  defp periods(_kind, nil, _through), do: []
  defp periods(_kind, _from, nil), do: []

  defp periods(:day, from, through) do
    first = DateTime.to_date(from)
    last = DateTime.to_date(through)

    if Date.compare(last, first) == :lt do
      []
    else
      Enum.map(Date.range(first, last), fn date ->
        %{
          key: date,
          group_id: Calendar.strftime(date, "%Y-%m"),
          group_label: Calendar.strftime(date, "%B %Y"),
          label: Integer.to_string(date.day),
          title: Calendar.strftime(date, "%A %-d %B %Y"),
          column: Date.day_of_week(date)
        }
      end)
    end
  end

  defp periods(:month, from, through) do
    first = month_index(DateTime.to_date(from))
    last = month_index(DateTime.to_date(through))

    if last < first do
      []
    else
      Enum.map(first..last, fn index ->
        {year, month} = {div(index, 12), rem(index, 12) + 1}
        date = Date.new!(year, month, 1)

        %{
          key: {year, month},
          group_id: Integer.to_string(year),
          group_label: Integer.to_string(year),
          label: Calendar.strftime(date, "%b"),
          title: Calendar.strftime(date, "%B %Y"),
          column: month
        }
      end)
    end
  end

  defp periods(:year, from, through) do
    first = DateTime.to_date(from).year
    last = DateTime.to_date(through).year

    if last < first do
      []
    else
      Enum.map(first..last, fn year ->
        %{
          key: year,
          group_id: "years",
          group_label: "Years",
          label: Integer.to_string(year),
          title: Integer.to_string(year),
          column: nil
        }
      end)
    end
  end

  defp periods(_kind, _from, _through), do: []

  defp month_index(%Date{} = date), do: date.year * 12 + date.month - 1

  defp groups(_kind, [], _gaps, _selected), do: []

  defp groups(:hour, _periods, gaps, selected), do: hour_groups(gaps, selected)

  defp groups(kind, periods, gaps, selected) do
    missing = missing_by_period(kind, gaps)

    periods
    |> Enum.chunk_by(& &1.group_id)
    |> Enum.map(fn [first | _rest] = chunk ->
      %{
        id: first.group_id,
        label: first.group_label,
        blanks: blanks(first),
        cells: Enum.map(chunk, &cell(&1, missing, selected))
      }
    end)
  end

  # The first cell of a month is rarely a Monday, so it needs padding to land in
  # its own weekday column. Every other cadence fills from the left.
  defp blanks(%{column: column}) when is_integer(column), do: column - 1
  defp blanks(_period), do: 0

  defp cell(period, missing, selected) do
    key = Map.get(missing, period.key)

    %{
      key: key,
      id: cell_id(period),
      label: period.label,
      title: period.title,
      state: if(key, do: :missing, else: :covered),
      selected?: !is_nil(key) and MapSet.member?(selected, key)
    }
  end

  defp cell_id(%{group_id: group_id, label: label}), do: "coverage-#{group_id}-#{label}"

  defp missing_by_period(kind, gaps) do
    Enum.reduce(gaps, %{}, fn gap, acc ->
      case period_key(kind, Map.get(gap, :start_at)) do
        nil -> acc
        key -> Map.put(acc, key, Map.get(gap, :window_key))
      end
    end)
  end

  defp period_key(_kind, nil), do: nil
  defp period_key(:day, %DateTime{} = start_at), do: DateTime.to_date(start_at)
  defp period_key(:month, %DateTime{} = start_at), do: {start_at.year, start_at.month}
  defp period_key(:year, %DateTime{} = start_at), do: start_at.year
  defp period_key(_kind, _start_at), do: nil

  # An hour list groups the missing hours under the day they fall on, which is the
  # only grouping that makes a run of them readable.
  defp hour_groups(gaps, selected) do
    gaps
    |> Enum.filter(&match?(%DateTime{}, Map.get(&1, :start_at)))
    |> Enum.sort_by(& &1.start_at, DateTime)
    |> Enum.group_by(&DateTime.to_date(&1.start_at))
    |> Enum.sort_by(fn {date, _gaps} -> date end, Date)
    |> Enum.map(fn {date, day_gaps} ->
      %{
        id: Date.to_iso8601(date),
        label: Calendar.strftime(date, "%A %-d %B %Y"),
        blanks: 0,
        cells: Enum.map(day_gaps, &hour_cell(&1, selected))
      }
    end)
  end

  defp hour_cell(gap, selected) do
    label = Calendar.strftime(gap.start_at, "%H:%M")

    %{
      key: gap.window_key,
      id: "coverage-#{gap.window_key}",
      label: label,
      title: Calendar.strftime(gap.start_at, "%H:%M on %-d %B %Y"),
      state: :missing,
      selected?: MapSet.member?(selected, gap.window_key)
    }
  end
end
