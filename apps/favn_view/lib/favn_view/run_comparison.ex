defmodule FavnView.RunComparison do
  @moduledoc """
  Multi-track lane geometry for comparing window runs.

  Pure functions over the per-window rows the page has already read. One lane
  per asset reference, one thin track per compared window, in the track order
  the page fixed. Colour still encodes status and position encodes window:
  colouring by window instead would destroy the failed-or-succeeded reading,
  which is the reading the chart exists for.

  An asset a window never planned draws an empty track in that window's
  position, so a plan difference between windows is visible rather than hidden.
  A window whose read failed draws unavailable tracks for every lane, so a gap
  never reads as "that window skipped this asset".

  Alignment defaults to each window's own start, which is what makes two runs of
  the same pipeline comparable. Wall-clock alignment shares one real timeline
  and is offered only when the windows ran closely enough together for that
  timeline to show anything: across windows days apart every bar is a hairline.

  Geometry comes from `FavnView.RunTimeline`, so a bar means the same thing in
  both charts. Window alignment is expressed as a shift applied to each window's
  instants, which leaves one axis implementation serving both modes.

  Bands group lanes exactly as the single-run chart does, but a comparison never
  collapses one: a collapsed multi-window band would have to summarise across
  windows, and that summary is the comparison the operator opened the chart to
  make.
  """

  alias FavnView.RunTimeline

  @wall_clock_max_ratio 4.0

  @type alignment :: :window | :wall_clock

  @type presence :: :drawn | :waiting | :absent | :unavailable | :loading

  @type track :: %{
          track: pos_integer(),
          run_id: String.t(),
          label: String.t() | nil,
          attempt_id: String.t() | nil,
          state: atom() | nil,
          outcome: RunTimeline.outcome() | nil,
          bar: RunTimeline.bar() | nil,
          advance_ms: pos_integer() | nil,
          presence: presence()
        }

  @type lane :: %{
          id: term(),
          name: String.t(),
          stage: term(),
          tracks: [track()]
        }

  @type band :: %{id: String.t(), stage: term(), label: String.t(), lanes: [lane()]}

  @type head :: %{
          track: pos_integer(),
          run_id: String.t(),
          label: String.t() | nil,
          title: String.t() | nil,
          state: :loaded | :loading | :unavailable,
          selected?: boolean(),
          reason: atom() | nil
        }

  @type t :: %__MODULE__{
          axis: RunTimeline.axis() | nil,
          bands: [band()],
          tracks: [head()],
          alignment: alignment(),
          wall_clock?: boolean(),
          span_ratio: float() | nil,
          density: RunTimeline.density(),
          lane_count: non_neg_integer(),
          track_count: non_neg_integer()
        }

  @enforce_keys [
    :axis,
    :bands,
    :tracks,
    :alignment,
    :wall_clock?,
    :span_ratio,
    :density,
    :lane_count,
    :track_count
  ]
  defstruct @enforce_keys

  @doc """
  Builds the comparison from the windows the page loaded.

  A window supplies `:run_id`, `:track`, `:state`, `:label`, `:selected?` and
  its `:assets` rows, which carry the same fields the single-run chart reads.
  Windows are drawn in `:track` order whatever order they arrive in.

  ## Options

    * `:now` - the instant an unfinished attempt is measured against, so a
      comparison of the same windows is the same comparison in a test
    * `:alignment` - `:window`, the default, or `:wall_clock`, which falls back
      to `:window` when the windows are too far apart to share one axis

  """
  @spec build([map()], keyword()) :: t()
  def build(windows, opts \\ []) when is_list(windows) do
    now = Keyword.get(opts, :now) || DateTime.utc_now()
    windows = Enum.sort_by(windows, &Map.get(&1, :track, 0))

    {wall_clock?, span_ratio} = wall_clock(windows, now)
    alignment = alignment(Keyword.get(opts, :alignment, :window), wall_clock?)

    prepared = Enum.map(windows, &prepare(&1, alignment, windows, now))
    axis = prepared |> Enum.flat_map(&shifted_rows/1) |> RunTimeline.axis(axis_now(prepared, now))
    lanes = lanes(prepared, axis, now)
    track_count = length(lanes) * length(prepared)

    %__MODULE__{
      axis: axis,
      bands: bands(lanes),
      tracks: Enum.map(prepared, &head/1),
      alignment: alignment,
      wall_clock?: wall_clock?,
      span_ratio: span_ratio,
      density: RunTimeline.density(track_count),
      lane_count: length(lanes),
      track_count: track_count
    }
  end

  @doc """
  Returns how many times the widest window span the compared windows may cover
  before one shared wall-clock axis stops showing anything.
  """
  @spec wall_clock_max_ratio() :: float()
  def wall_clock_max_ratio, do: @wall_clock_max_ratio

  defp alignment(:wall_clock, true), do: :wall_clock
  defp alignment(_requested, _wall_clock?), do: :window

  # The axis measures a running bar against the shifted now of the window that
  # is furthest into its own span. Measuring against the unshifted clock would
  # stretch a window-aligned axis by the wall-clock gap between the windows,
  # which is exactly the gap this alignment exists to remove.
  defp axis_now(prepared, now) do
    prepared
    |> Enum.filter(&live?/1)
    |> Enum.map(&shift(now, &1.shift_ms))
    |> case do
      [] -> now
      nows -> Enum.max_by(nows, &unix/1)
    end
  end

  defp live?(%{state: :loaded} = window), do: window |> rows() |> Enum.any?(&running?/1)
  defp live?(_window), do: false

  # Wall-clock alignment is worth offering only when the windows overlap enough
  # in real time that one axis still resolves individual attempts. The ratio the
  # decision turns on is reported, so the control can explain itself rather than
  # simply being missing.
  defp wall_clock(windows, now) do
    windows
    |> Enum.map(&span(&1, now))
    |> Enum.reject(&is_nil/1)
    |> case do
      spans when length(spans) < 2 ->
        {false, nil}

      spans ->
        earliest = spans |> Enum.map(& &1.origin) |> Enum.min_by(&unix/1)
        latest = spans |> Enum.map(& &1.end_at) |> Enum.max_by(&unix/1)
        combined = max(DateTime.diff(latest, earliest, :millisecond), 1)
        longest = spans |> Enum.map(& &1.span_ms) |> Enum.max()
        ratio = combined / max(longest, 1)

        {ratio <= @wall_clock_max_ratio, Float.round(ratio, 1)}
    end
  end

  # A window's own span runs from its first attempt to its last finish, or to
  # now while anything in it is still running.
  defp span(window, now) do
    rows = rows(window)
    starts = rows |> Enum.map(&Map.get(&1, :started_at)) |> Enum.reject(&is_nil/1)

    case starts do
      [] ->
        nil

      starts ->
        origin = Enum.min_by(starts, &unix/1)
        end_at = span_end(rows, origin, now)

        %{
          origin: origin,
          end_at: end_at,
          span_ms: max(DateTime.diff(end_at, origin, :millisecond), 1)
        }
    end
  end

  defp span_end(rows, origin, now) do
    finishes = rows |> Enum.map(&Map.get(&1, :finished_at)) |> Enum.reject(&is_nil/1)
    candidates = if Enum.any?(rows, &running?/1), do: [now | finishes], else: finishes

    Enum.max_by([origin | candidates], &unix/1)
  end

  defp running?(row),
    do: not is_nil(Map.get(row, :started_at)) and is_nil(Map.get(row, :finished_at))

  # Window alignment is a shift: every instant in a window moves by the gap
  # between that window's own start and the earliest start in the comparison, so
  # one axis implementation serves both alignments and a bar keeps meaning the
  # elapsed time it always meant.
  defp prepare(window, alignment, windows, now) do
    rows = rows(window)

    window
    |> Map.put(:rows_by_ref, Map.new(rows, &{ref(&1), &1}))
    |> Map.put(:shift_ms, shift_ms(window, alignment, windows, now))
  end

  defp shift_ms(_window, :wall_clock, _windows, _now), do: 0

  defp shift_ms(window, :window, windows, now) do
    with %{origin: origin} <- span(window, now),
         %DateTime{} = base <- base(windows, now) do
      DateTime.diff(base, origin, :millisecond)
    else
      _no_origin -> 0
    end
  end

  # The earliest start across the comparison anchors the shifted axis, so the
  # window that started first is drawn unshifted and every other window slides
  # back onto it.
  defp base(windows, now) do
    windows
    |> Enum.map(&span(&1, now))
    |> Enum.reject(&is_nil/1)
    |> Enum.map(& &1.origin)
    |> case do
      [] -> nil
      origins -> Enum.min_by(origins, &unix/1)
    end
  end

  defp shifted_rows(%{state: :loaded} = window) do
    window
    |> rows()
    |> Enum.map(fn row ->
      %{
        started_at: shift(Map.get(row, :started_at), window.shift_ms),
        finished_at: shift(Map.get(row, :finished_at), window.shift_ms)
      }
    end)
  end

  defp shifted_rows(_window), do: []

  defp shift(nil, _shift_ms), do: nil
  defp shift(%DateTime{} = at, 0), do: at
  defp shift(%DateTime{} = at, shift_ms), do: DateTime.add(at, shift_ms, :millisecond)

  # The lane set is the union across windows, in first-seen order, so an asset
  # only one window planned still gets a lane with empty tracks beside it.
  defp lanes(windows, axis, now) do
    windows
    |> Enum.flat_map(&rows/1)
    |> Enum.reduce({%{}, []}, fn row, {seen, order} ->
      key = ref(row)

      if Map.has_key?(seen, key),
        do: {seen, order},
        else: {Map.put(seen, key, row), [key | order]}
    end)
    |> then(fn {seen, order} ->
      order
      |> Enum.reverse()
      |> Enum.map(fn key -> lane(key, Map.fetch!(seen, key), windows, axis, now) end)
    end)
  end

  defp lane(key, sample, windows, axis, now) do
    %{
      id: key,
      name: Map.get(sample, :name) || to_string(key),
      stage: Map.get(sample, :stage),
      tracks: Enum.map(windows, &track(&1, key, axis, now))
    }
  end

  defp track(%{state: :loaded} = window, key, axis, now) do
    case Map.get(window.rows_by_ref, key) do
      nil -> blank_track(window, :absent)
      row -> drawn_track(window, row, axis, now)
    end
  end

  defp track(%{state: state} = window, _key, _axis, _now), do: blank_track(window, state)

  defp drawn_track(window, row, axis, now) do
    started_at = shift(Map.get(row, :started_at), window.shift_ms)
    finished_at = shift(Map.get(row, :finished_at), window.shift_ms)
    window_now = shift(now, window.shift_ms)
    bar = RunTimeline.bar(started_at, finished_at, axis, window_now)

    %{
      track: window.track,
      run_id: window.run_id,
      label: Map.get(window, :label),
      attempt_id: Map.get(row, :id),
      state: Map.get(row, :state),
      outcome: RunTimeline.outcome(Map.get(row, :state)),
      bar: bar,
      advance_ms: advance_ms(bar, axis, window_now),
      presence: if(started_at, do: :drawn, else: :waiting)
    }
  end

  # A running bar grows across the axis it has left in exactly the real time that
  # distance represents. On a window-aligned axis each window's bar ends at its
  # own shifted now, so a window further behind has more axis left and needs
  # longer to cross it. Sharing the chart's single advance would have made a
  # lagging window's bar sweep several times faster than the work it draws.
  defp advance_ms(%{running?: true}, %{end_at: end_at}, window_now),
    do: max(DateTime.diff(end_at, window_now, :millisecond), 1)

  defp advance_ms(_bar, _axis, _window_now), do: nil

  defp blank_track(window, presence) do
    %{
      track: window.track,
      run_id: window.run_id,
      label: Map.get(window, :label),
      attempt_id: nil,
      state: nil,
      outcome: nil,
      bar: nil,
      advance_ms: nil,
      presence: presence
    }
  end

  defp bands(lanes) do
    lanes
    |> Enum.group_by(& &1.stage)
    |> Enum.sort_by(fn {stage, _lanes} -> RunTimeline.band_order(stage) end)
    |> Enum.map(fn {stage, band_lanes} ->
      stage
      |> RunTimeline.band()
      |> Map.put(:lanes, Enum.sort_by(band_lanes, & &1.name))
    end)
  end

  defp head(window) do
    %{
      track: window.track,
      run_id: window.run_id,
      label: Map.get(window, :label),
      title: Map.get(window, :title),
      state: window.state,
      selected?: Map.get(window, :selected?, false),
      reason: Map.get(window, :reason)
    }
  end

  defp rows(window), do: Map.get(window, :assets) || []

  defp ref(row), do: Map.get(row, :asset_ref) || Map.get(row, :id)

  defp unix(%DateTime{} = at), do: DateTime.to_unix(at, :microsecond)
end
