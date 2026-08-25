defmodule FavnView.RunTimeline do
  @moduledoc """
  Lane geometry for the run detail stage timeline.

  Pure functions over the asset attempt rows the run page has already loaded.
  Nothing here reads: the chart draws the rows the table draws, so charting a
  run costs exactly what listing it costs.

  Offsets and widths are percentages of the axis, so markup places a bar in CSS
  with no measurement step and no resize handler. The axis fits the run — it
  starts at the first attempt to start and ends at the last to finish, or a
  little past now while anything is still running — so there is nothing to zoom
  and nothing to scroll. Exact values belong on the attempt page.

  Stage is presentation only. Bands group and order lanes; they never change
  which assets appear, their state, or the counts above the chart. An attempt
  recorded before stage persistence has no stage and groups into an explicit
  unstaged band rather than being hidden or forced into stage zero.
  """

  @min_bar_width 1.0
  @live_headroom 0.08
  @comfortable_max 60
  @compact_max 200
  @max_tick_intervals 6

  @tick_intervals_ms [
    100,
    250,
    500,
    1_000,
    5_000,
    10_000,
    15_000,
    30_000,
    60_000,
    120_000,
    300_000,
    600_000,
    900_000,
    1_800_000,
    3_600_000,
    7_200_000,
    21_600_000,
    43_200_000,
    86_400_000
  ]

  @type density :: :comfortable | :compact | :dense
  @type outcome :: :succeeded | :failed | :running | :waiting

  @type bar :: %{offset: float(), width: float(), running?: boolean()}

  @type tick :: %{offset: float(), label: String.t()}

  @type axis :: %{
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          span_ms: pos_integer(),
          ticks: [tick()],
          now_offset: float() | nil,
          advance_ms: pos_integer() | nil
        }

  @type lane :: %{
          id: String.t(),
          run_id: String.t(),
          name: String.t(),
          asset_ref: term(),
          state: atom(),
          stage: term(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          outcome: outcome(),
          bar: bar() | nil
        }

  @type summary :: %{
          total: non_neg_integer(),
          succeeded: non_neg_integer(),
          failed: non_neg_integer(),
          running: non_neg_integer(),
          waiting: non_neg_integer(),
          bar: bar() | nil
        }

  @type band :: %{
          id: String.t(),
          stage: term(),
          label: String.t(),
          lanes: [lane()],
          collapsed?: boolean(),
          summary: summary()
        }

  @type t :: %__MODULE__{
          axis: axis() | nil,
          bands: [band()],
          density: density(),
          lane_count: non_neg_integer(),
          ghost_count: non_neg_integer()
        }

  @enforce_keys [:axis, :bands, :density, :lane_count, :ghost_count]
  defstruct @enforce_keys

  @doc """
  Builds the chart from loaded asset attempt rows.

  A row supplies `:id`, `:run_id`, `:name`, `:asset_ref`, `:state`, `:stage`,
  and raw `:started_at` and `:finished_at` values. A row that has not started
  becomes a ghost lane — a labelled track with no bar — never a zero-length bar
  at the origin.

  ## Options

    * `:now` - the instant an unfinished attempt is measured against, so a
      chart of the same rows is the same chart in a test
    * `:expanded` - band ids drawn in full while the rest stay collapsed, which
      only applies in dense mode
    * `:sort` - lane order within a band: `:start`, the default, or `:name`

  """
  @spec build([map()], keyword()) :: t()
  def build(rows, opts \\ []) when is_list(rows) do
    now = Keyword.get(opts, :now) || DateTime.utc_now()
    expanded = Keyword.get(opts, :expanded, [])
    sort = Keyword.get(opts, :sort, :start)
    axis = axis(rows, now)
    density = density(length(rows))

    bands =
      rows
      |> Enum.map(&lane(&1, axis, now))
      |> bands(sort)
      |> Enum.map(&collapse(&1, density, expanded))

    %__MODULE__{
      axis: axis,
      bands: bands,
      density: density,
      lane_count: length(rows),
      ghost_count: Enum.count(rows, &is_nil(Map.get(&1, :started_at)))
    }
  end

  @doc """
  Lane height mode for a lane count.

  Comparison counts tracks rather than lanes, so the choice is exposed instead
  of being buried in `build/2`.
  """
  @spec density(non_neg_integer()) :: density()
  def density(count) when count <= @comfortable_max, do: :comfortable
  def density(count) when count <= @compact_max, do: :compact
  def density(_count), do: :dense

  @doc """
  What an attempt state means for reading the chart.

  This is not a badge tone. It answers whether a stage is done and whether
  anything in it went wrong, which is the question a band summary exists to
  answer when its lanes are collapsed.
  """
  @spec outcome(atom()) :: outcome()
  def outcome(state) when state in [:ok, :succeeded, :skipped_fresh], do: :succeeded

  def outcome(state) when state in [:error, :failed, :timed_out, :blocked, :cancelled],
    do: :failed

  def outcome(state) when state in [:running, :retrying], do: :running
  def outcome(_state), do: :waiting

  @doc """
  Compact elapsed label for an offset into the axis.

  ## Examples

      iex> FavnView.RunTimeline.elapsed_label(0)
      "0s"

      iex> FavnView.RunTimeline.elapsed_label(450)
      "450ms"

      iex> FavnView.RunTimeline.elapsed_label(90_000)
      "1m 30s"

      iex> FavnView.RunTimeline.elapsed_label(7_200_000)
      "2h"

  """
  @spec elapsed_label(non_neg_integer()) :: String.t()
  def elapsed_label(0), do: "0s"
  def elapsed_label(ms) when ms < 1_000, do: "#{ms}ms"

  def elapsed_label(ms) when ms < 60_000 do
    seconds = ms / 1_000
    if seconds == trunc(seconds), do: "#{trunc(seconds)}s", else: "#{Float.round(seconds, 1)}s"
  end

  def elapsed_label(ms) when ms < 3_600_000 do
    minutes = div(ms, 60_000)
    seconds = div(rem(ms, 60_000), 1_000)
    if seconds == 0, do: "#{minutes}m", else: "#{minutes}m #{seconds}s"
  end

  def elapsed_label(ms) do
    hours = div(ms, 3_600_000)
    minutes = div(rem(ms, 3_600_000), 60_000)
    if minutes == 0, do: "#{hours}h", else: "#{hours}h #{minutes}m"
  end

  # No attempt has started, so there is no span to draw against and every lane
  # is a ghost. The chart says so rather than inventing an axis around now.
  defp axis(rows, now) do
    rows
    |> Enum.map(&Map.get(&1, :started_at))
    |> Enum.reject(&is_nil/1)
    |> case do
      [] ->
        nil

      starts ->
        start_at = Enum.min_by(starts, &unix/1)
        end_at = axis_end(rows, start_at, now)
        span_ms = max(DateTime.diff(end_at, start_at, :millisecond), 1)

        %{
          start_at: start_at,
          end_at: end_at,
          span_ms: span_ms,
          ticks: ticks(span_ms),
          now_offset: now_offset(rows, now, start_at, span_ms),
          advance_ms: advance_ms(rows, now, end_at)
        }
    end
  end

  # A live run's axis reaches past now by a fraction of what has already
  # elapsed. That headroom is what the running bars and the now line advance
  # into by CSS animation between reads; without it they would already sit on
  # the right edge with nowhere to go.
  defp axis_end(rows, start_at, now) do
    finishes = rows |> Enum.map(&Map.get(&1, :finished_at)) |> Enum.reject(&is_nil/1)

    candidates =
      if Enum.any?(rows, &running?/1) do
        elapsed = max(DateTime.diff(now, start_at, :millisecond), 1)
        [DateTime.add(now, round(elapsed * @live_headroom), :millisecond) | finishes]
      else
        finishes
      end

    Enum.max_by([start_at | candidates], &unix/1)
  end

  # The now line exists only while something is still running. On a finished run
  # it would be a line through the present, which says nothing about the run.
  defp now_offset(rows, now, start_at, span_ms) do
    if Enum.any?(rows, &running?/1) do
      now |> DateTime.diff(start_at, :millisecond) |> percent(span_ms) |> clamp() |> round3()
    end
  end

  # How long the headroom lasts in real time, which is exactly how long the CSS
  # animation has to advance across it before the next read redraws the axis.
  defp advance_ms(rows, now, end_at) do
    if Enum.any?(rows, &running?/1) do
      max(DateTime.diff(end_at, now, :millisecond), 1)
    end
  end

  defp running?(row),
    do: not is_nil(Map.get(row, :started_at)) and is_nil(Map.get(row, :finished_at))

  defp ticks(span_ms) do
    interval = tick_interval(span_ms)

    0
    |> Stream.iterate(&(&1 + interval))
    |> Enum.take_while(&(&1 <= span_ms))
    |> Enum.map(&%{offset: &1 |> percent(span_ms) |> round3(), label: elapsed_label(&1)})
  end

  # The smallest round interval that keeps the axis under seven gridlines, so a
  # tick reads as "30s" rather than as an arbitrary fraction of the run.
  defp tick_interval(span_ms) do
    Enum.find(@tick_intervals_ms, &(div(span_ms, &1) <= @max_tick_intervals)) ||
      max(div(span_ms, @max_tick_intervals), 1)
  end

  defp lane(row, axis, now) do
    started_at = Map.get(row, :started_at)
    finished_at = Map.get(row, :finished_at)

    %{
      id: Map.get(row, :id),
      run_id: Map.get(row, :run_id),
      name: Map.get(row, :name),
      asset_ref: Map.get(row, :asset_ref),
      state: Map.get(row, :state),
      stage: Map.get(row, :stage),
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: duration_ms(started_at, finished_at, now),
      outcome: outcome(Map.get(row, :state)),
      bar: bar(started_at, finished_at, axis, now)
    }
  end

  defp bar(nil, _finished_at, _axis, _now), do: nil
  defp bar(_started_at, _finished_at, nil, _now), do: nil

  defp bar(started_at, finished_at, axis, now) do
    from = offset_of(started_at, axis)
    to = offset_of(finished_at || now, axis)
    width = max(to - from, @min_bar_width)

    %{
      offset: from |> min(100.0 - width) |> round3(),
      width: round3(width),
      running?: is_nil(finished_at)
    }
  end

  # Timing outside the axis clamps to it. Nothing loaded with the axis can fall
  # outside it, but a row whose finish precedes its own start would otherwise
  # draw backwards, and comparison shares one axis across several windows.
  defp offset_of(at, axis) do
    at |> DateTime.diff(axis.start_at, :millisecond) |> percent(axis.span_ms) |> clamp()
  end

  defp duration_ms(nil, _finished_at, _now), do: nil

  defp duration_ms(started_at, nil, now),
    do: max(DateTime.diff(now, started_at, :millisecond), 0)

  defp duration_ms(started_at, finished_at, _now),
    do: max(DateTime.diff(finished_at, started_at, :millisecond), 0)

  defp bands(lanes, sort) do
    lanes
    |> Enum.group_by(& &1.stage)
    |> Enum.sort_by(fn {stage, _lanes} -> {stage_order(stage), stage} end)
    |> Enum.map(fn {stage, band_lanes} ->
      band(stage, Enum.sort_by(band_lanes, &lane_order(&1, sort)))
    end)
  end

  # Unstaged sorts last: it holds attempts too old to carry a stage, which
  # belong after the dependency order rather than before it.
  defp stage_order(nil), do: 1
  defp stage_order(_stage), do: 0

  # Name order is plainly alphabetical, because that is what makes a wide stage
  # scannable. Start order puts a lane with no attempt last rather than at the
  # top of a stage the run has not reached.
  defp lane_order(%{name: name}, :name), do: {0, 0, name}
  defp lane_order(%{started_at: nil, name: name}, _sort), do: {1, 0, name}
  defp lane_order(%{started_at: at, name: name}, _sort), do: {0, unix(at), name}

  defp band(stage, lanes) do
    %{
      id: band_id(stage),
      stage: stage,
      label: band_label(stage),
      lanes: lanes,
      collapsed?: false,
      summary: summary(lanes)
    }
  end

  defp band_id(nil), do: "unstaged"
  defp band_id(stage), do: "stage-#{stage}"

  defp band_label(nil), do: "Unstaged"
  defp band_label(stage), do: "Stage #{stage}"

  defp collapse(band, :dense, expanded), do: %{band | collapsed?: band.id not in expanded}
  defp collapse(band, _density, _expanded), do: band

  defp summary(lanes) do
    counts = Enum.frequencies_by(lanes, & &1.outcome)

    %{
      total: length(lanes),
      succeeded: Map.get(counts, :succeeded, 0),
      failed: Map.get(counts, :failed, 0),
      running: Map.get(counts, :running, 0),
      waiting: Map.get(counts, :waiting, 0),
      bar: span_bar(lanes)
    }
  end

  # A collapsed band draws one strip covering everything its lanes cover, so the
  # dense chart still shows when a stage ran and whether it is still running.
  defp span_bar(lanes) do
    lanes
    |> Enum.map(& &1.bar)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] ->
        nil

      bars ->
        offset = bars |> Enum.map(& &1.offset) |> Enum.min()
        finish = bars |> Enum.map(&(&1.offset + &1.width)) |> Enum.max()

        %{
          offset: round3(offset),
          width: finish |> Kernel.-(offset) |> round3(),
          running?: Enum.any?(bars, & &1.running?)
        }
    end
  end

  defp percent(value_ms, span_ms), do: value_ms / span_ms * 100.0

  defp clamp(value) when value < 0.0, do: 0.0
  defp clamp(value) when value > 100.0, do: 100.0
  defp clamp(value), do: value

  defp round3(value), do: Float.round(value, 3)

  defp unix(%DateTime{} = at), do: DateTime.to_unix(at, :microsecond)
end
