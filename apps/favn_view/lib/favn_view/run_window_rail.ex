defmodule FavnView.RunWindowRail do
  @moduledoc """
  Calendar layout for the run detail window rail.

  Pure functions over the window choices the page has already loaded. Nothing
  here issues a query: the rail is a navigation aid drawn from the rows the
  window read returned.

  The rail scales by span rather than by cell count. A short list is one flat
  strip with a cell per window run. A long one becomes a coarse band bucketed
  one calendar unit up — hours group by day, days by month, months by year —
  plus a fine band holding the selected bucket's window runs.

  Cells are executions, not coverage. Logical windows describe what data a
  backfill covers; child runs describe what actually executed. A combined
  backfill runs several contiguous coverage windows in one child run, so those
  windows collapse into one cell spanning them, carrying `window_count`. One
  cell is therefore always one place to navigate to.

  A window the backfill has planned but not yet started has no run to navigate
  to and is not a cell, so `in_progress?` reports that the set is still growing
  while the backfill has not finished.

  A cell also reports its place in a comparison. `track` is the fixed position a
  compared window's bars occupy in every lane, taken from the caller's ordering
  rather than from this cell's place in the rail, so opening a different band
  does not renumber the tracks the chart is already drawing.
  """

  alias Favn.Timezone
  alias FavnView.Time

  @flat_threshold 120
  @compare_limit 4
  @terminal_backfill_statuses [:completed, :failed, :cancelled]

  @type cell :: %{
          run_id: String.t(),
          label: String.t(),
          status: atom() | nil,
          selected?: boolean(),
          compared?: boolean(),
          track: pos_integer() | nil,
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          window_count: pos_integer()
        }

  @type bucket :: %{
          id: String.t(),
          label: String.t(),
          count: pos_integer(),
          selected?: boolean()
        }

  @type t :: %__MODULE__{
          layout: :flat | :banded,
          cells: [cell()],
          buckets: [bucket()],
          open_bucket: String.t() | nil,
          truncated?: boolean(),
          in_progress?: boolean(),
          compare_run_ids: [String.t()],
          compare_full?: boolean()
        }

  @enforce_keys [
    :layout,
    :cells,
    :buckets,
    :truncated?,
    :in_progress?,
    :compare_run_ids,
    :compare_full?
  ]
  defstruct @enforce_keys ++ [:open_bucket]

  @doc """
  Builds the rail from loaded window choices.

  `selected_run_id` marks the run the page is showing; it need not be present
  in `choices`, because the read caps at 1,000 rows. `timezone` labels a choice
  whose own window key did not decode one.

  ## Options

    * `:truncated?` - the window read reported more rows than it returned
    * `:backfill_status` - the owning backfill's status, used for `in_progress?`
    * `:open_bucket` - which coarse band bucket the fine band shows, when banded
    * `:compare_run_ids` - the compared window runs, in track order

  """
  @spec build([map()], String.t() | nil, String.t() | map(), keyword()) :: t()
  def build(choices, selected_run_id, timezone, opts \\ []) when is_list(choices) do
    kind = list_kind(choices)
    compare_run_ids = Keyword.get(opts, :compare_run_ids, [])

    cells =
      choices
      |> Enum.sort_by(&{DateTime.to_unix(&1.window_start_at, :microsecond), &1.run_id})
      |> combine_by_run()
      |> Enum.map(&cell(&1, kind, selected_run_id, compare_run_ids, timezone))

    %__MODULE__{
      layout: layout(cells),
      cells: cells,
      buckets: [],
      open_bucket: Keyword.get(opts, :open_bucket),
      truncated?: Keyword.get(opts, :truncated?, false),
      in_progress?: in_progress?(Keyword.get(opts, :backfill_status)),
      compare_run_ids: compare_run_ids,
      compare_full?: length(compare_run_ids) >= @compare_limit
    }
    |> band(kind, timezone)
  end

  @doc "Returns the flat-strip cell ceiling, above which the rail bands."
  @spec flat_threshold() :: pos_integer()
  def flat_threshold, do: @flat_threshold

  @doc """
  Returns the most windows one comparison may hold.

  The bound exists because each compared window costs a separate exact-run read
  on every refresh cycle, so it belongs to the page's read behaviour rather than
  to its layout. This module states it once; the page enforces it again where
  those reads are issued.
  """
  @spec compare_limit() :: pos_integer()
  def compare_limit, do: @compare_limit

  defp layout(cells) when length(cells) > @flat_threshold, do: :banded
  defp layout(_cells), do: :flat

  defp band(%__MODULE__{layout: :flat} = rail, _kind, _timezone), do: rail

  defp band(%__MODULE__{} = rail, kind, timezone) do
    case bucket_unit(kind, rail.cells, timezone) do
      nil ->
        # Years have no coarser unit to group by, so a very long year list stays
        # flat rather than collapsing into a single meaningless band.
        %{rail | layout: :flat}

      unit ->
        buckets = buckets(rail.cells, unit, timezone)
        selected = open_bucket(buckets, rail.open_bucket)

        %{
          rail
          | buckets: Enum.map(buckets, &bucket(&1, selected)),
            cells: Enum.filter(rail.cells, &(bucket_id(&1, unit, timezone) == selected))
        }
    end
  end

  defp buckets(cells, unit, timezone) do
    cells
    |> Enum.group_by(&bucket_id(&1, unit, timezone))
    |> Enum.map(fn {id, grouped} ->
      %{
        id: id,
        label: bucket_label(hd(grouped), unit, timezone),
        count: length(grouped),
        selected?: Enum.any?(grouped, & &1.selected?)
      }
    end)
    |> Enum.sort_by(& &1.id)
  end

  defp bucket(bucket, selected), do: %{bucket | selected?: bucket.id == selected}

  # The operator can open any bucket, but a request for one the current rows no
  # longer contain falls back rather than emptying the fine band: the window
  # list grows under the page while the backfill runs.
  defp open_bucket(buckets, requested) do
    cond do
      Enum.any?(buckets, &(&1.id == requested)) -> requested
      bucket = Enum.find(buckets, & &1.selected?) -> bucket.id
      true -> buckets |> List.last() |> then(&(&1 && &1.id))
    end
  end

  # One calendar unit up from the window kind. A mixed or undecodable list has
  # no single kind, so it takes the smallest unit that keeps the coarse band
  # inside the flat threshold.
  defp bucket_unit(:hour, _cells, _timezone), do: :day
  defp bucket_unit(:day, _cells, _timezone), do: :month
  defp bucket_unit(:month, _cells, _timezone), do: :year
  defp bucket_unit(:year, _cells, _timezone), do: nil

  defp bucket_unit(nil, cells, timezone) do
    Enum.find([:day, :month, :year], fn unit ->
      cells
      |> Enum.map(&bucket_id(&1, unit, timezone))
      |> Enum.uniq()
      |> length()
      |> Kernel.<=(@flat_threshold)
    end) || :year
  end

  defp bucket_id(cell, unit, timezone) do
    cell.start_at
    |> local(cell, timezone)
    |> truncate_to(unit)
  end

  defp truncate_to(%DateTime{} = at, :day),
    do: at |> DateTime.to_date() |> Date.to_iso8601()

  defp truncate_to(%DateTime{year: year, month: month}, :month),
    do: "#{year}-#{pad(month)}"

  defp truncate_to(%DateTime{year: year}, :year), do: Integer.to_string(year)

  defp bucket_label(cell, :day, timezone),
    do: cell.start_at |> local(cell, timezone) |> Calendar.strftime("%b %-d")

  defp bucket_label(cell, :month, timezone),
    do: cell.start_at |> local(cell, timezone) |> Calendar.strftime("%b %Y")

  defp bucket_label(cell, :year, timezone),
    do: cell.start_at |> local(cell, timezone) |> Calendar.strftime("%Y")

  # A combined backfill executes several logical coverage windows in one child
  # run. Those are one execution, so they are one cell: rendering a cell per
  # window would offer the operator four identical destinations. The cell spans
  # the combined coverage and says how many windows it covers.
  defp combine_by_run(sorted) do
    sorted
    |> Enum.chunk_by(& &1.run_id)
    |> Enum.map(fn
      [only] ->
        Map.put(only, :window_count, 1)

      [first | _rest] = combined ->
        last = List.last(combined)

        first
        |> Map.put(:window_end_at, last.window_end_at)
        |> Map.put(:window_count, length(combined))
    end)
  end

  defp cell(choice, kind, selected_run_id, compare_run_ids, timezone) do
    track = Enum.find_index(compare_run_ids, &(&1 == choice.run_id))

    %{
      run_id: choice.run_id,
      label: cell_label(choice, kind, timezone),
      status: Map.get(choice, :status),
      selected?: choice.run_id == selected_run_id,
      compared?: not is_nil(track),
      track: track && track + 1,
      start_at: choice.window_start_at,
      end_at: choice.window_end_at,
      window_count: Map.get(choice, :window_count, 1),
      timezone: Map.get(choice, :timezone)
    }
  end

  # A cell is labelled by its calendar position, which only means something when
  # every window shares one kind. A mixed list, or a key too old to decode one,
  # falls back to the start timestamp rather than to a misleading calendar word.
  defp cell_label(choice, nil, timezone),
    do: choice.window_start_at |> local(choice, timezone) |> Calendar.strftime("%b %-d, %Y %H:%M")

  defp cell_label(choice, :hour, timezone),
    do: choice.window_start_at |> local(choice, timezone) |> Calendar.strftime("%H:%M")

  defp cell_label(choice, :day, timezone),
    do: choice.window_start_at |> local(choice, timezone) |> Calendar.strftime("%-d")

  defp cell_label(choice, :month, timezone),
    do: choice.window_start_at |> local(choice, timezone) |> Calendar.strftime("%b")

  defp cell_label(choice, :year, timezone),
    do: choice.window_start_at |> local(choice, timezone) |> Calendar.strftime("%Y")

  # Bucketing and labelling both happen in the window's own timezone, so a day
  # cell names the day the operator scheduled rather than its UTC neighbour.
  defp local(%DateTime{} = at, source, fallback) do
    case Map.get(source, :timezone) do
      zone when is_binary(zone) and zone != "" -> shift(at, zone, fallback)
      _other -> Time.shift(at, fallback)
    end
  end

  defp shift(at, zone, fallback) do
    case DateTime.shift_zone(at, zone, Timezone.database!()) do
      {:ok, shifted} -> shifted
      {:error, _reason} -> Time.shift(at, fallback)
    end
  end

  defp list_kind(choices) do
    choices
    |> Enum.map(&Map.get(&1, :kind))
    |> Enum.uniq()
    |> case do
      [kind] when not is_nil(kind) -> kind
      _mixed_or_unknown -> nil
    end
  end

  defp in_progress?(status), do: not is_nil(status) and status not in @terminal_backfill_statuses

  defp pad(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")
end
