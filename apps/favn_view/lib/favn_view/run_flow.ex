defmodule FavnView.RunFlow do
  @moduledoc """
  Projects a run's asset attempts onto one shared time axis.

  This is the geometry behind `FavnView.Components.RunDetailPage.Flow`, kept
  separate so the arithmetic is testable without rendering. It takes the
  attempts a run detail already carries and returns lanes grouped by execution
  stage, each lane's bars positioned as percentages of the run's own span.

  Two rules keep the result honest:

    * A queued attempt gets no bar. Drawing a bar where work has not started
      invents a schedule the runner never promised.
    * The axis ends at `now` only while the run is active. A settled run's axis
      ends when its last attempt ended, so reopening it an hour later does not
      squash the whole run into the left edge.
  """

  @minimum_width 2.5
  @tick_count 4
  @fallback_span_ms 60_000

  @type bar :: %{
          id: String.t(),
          attempt_id: String.t() | nil,
          left: float(),
          width: float(),
          track: non_neg_integer(),
          tone: atom(),
          running?: boolean(),
          label: String.t(),
          title: String.t()
        }

  @type lane :: %{
          id: String.t(),
          key: String.t(),
          name: String.t(),
          tone: atom(),
          raw_status: atom() | nil,
          detail: String.t(),
          empty_label: String.t(),
          bars: [bar()],
          tracks: pos_integer(),
          error: %{summary: String.t(), attempt_id: String.t() | nil} | nil
        }

  @type t :: %{
          axis: %{
            ticks: [%{label: String.t(), offset: float(), align: atom()}],
            now_offset: float() | nil,
            start_ms: integer(),
            end_ms: integer()
          },
          stages: [%{id: String.t(), label: String.t(), hint: String.t(), lanes: [lane()]}]
        }

  @doc """
  Builds the flow view model.

  `attempts` are run-detail attempt maps. `opts` accepts `:active?` and
  `:now_ms`, both of which the caller owns so the projection stays pure.

      iex> attempts = [
      ...>   %{
      ...>     id: "a1", asset_key: "crm.orders", short_asset_name: "Orders",
      ...>     stage: 1, raw_status: :ok, status: "Succeeded", status_tone: :success,
      ...>     started_at_raw: ~U[2026-07-23 10:00:00Z], finished_at_raw: ~U[2026-07-23 10:00:30Z],
      ...>     duration: "30s", window_label: "Jul 23", error_summary: nil
      ...>   }
      ...> ]
      iex> flow = FavnView.RunFlow.build(attempts, active?: false)
      iex> [stage] = flow.stages
      iex> stage.label
      "Stage 1"
      iex> [lane] = stage.lanes
      iex> {lane.name, lane.tone, length(lane.bars)}
      {"Orders", :success, 1}
      iex> flow.axis.now_offset
      nil
  """
  @spec build([map()], keyword()) :: t()
  def build(attempts, opts \\ []) do
    active? = Keyword.get(opts, :active?, false)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

    attempts = Enum.reject(attempts, &is_nil/1)
    {start_ms, end_ms} = bounds(attempts, active?, now_ms)
    span = max(end_ms - start_ms, 1)

    %{
      axis: %{
        ticks: ticks(start_ms, span),
        now_offset: if(active?, do: offset(now_ms - start_ms, span)),
        start_ms: start_ms,
        end_ms: end_ms
      },
      stages: stages(attempts, start_ms, span, active?, now_ms)
    }
  end

  defp stages([], _start_ms, _span, _active?, _now_ms), do: []

  defp stages(attempts, start_ms, span, active?, now_ms) do
    attempts
    |> Enum.group_by(&stage_key/1)
    |> Enum.sort_by(fn {stage, _attempts} -> stage end)
    |> Enum.map(fn {stage, stage_attempts} ->
      lanes = lanes(stage_attempts, start_ms, span, active?, now_ms)

      %{
        id: stage_id(stage),
        label: stage_label(stage),
        hint: stage_hint(stage, lanes),
        lanes: lanes
      }
    end)
  end

  defp lanes(attempts, start_ms, span, active?, now_ms) do
    attempts
    |> Enum.group_by(&lane_key/1)
    |> Enum.map(fn {key, lane_attempts} ->
      lane(key, lane_attempts, start_ms, span, active?, now_ms)
    end)
    |> Enum.sort_by(& &1.name)
  end

  defp lane(key, attempts, start_ms, span, active?, now_ms) do
    first = List.first(attempts)
    bars = attempts |> Enum.flat_map(&bar(&1, start_ms, span, active?, now_ms)) |> pack()
    failed = Enum.find(attempts, &(tone(&1) == :error))
    tone = lane_tone(attempts)

    %{
      id: key,
      key: key,
      name: Map.get(first, :short_asset_name) || Map.get(first, :asset_name) || key,
      tone: tone,
      status: lane_status(attempts, tone),
      raw_status: Map.get(first, :raw_status),
      detail: detail(attempts),
      empty_label: empty_label(attempts, active?),
      bars: bars,
      tracks: max(Enum.count(Enum.uniq(Enum.map(bars, & &1.track))), 1),
      error: error(failed)
    }
  end

  defp bar(attempt, start_ms, span, active?, now_ms) do
    case datetime_ms(Map.get(attempt, :started_at_raw)) do
      nil ->
        []

      bar_start ->
        running? = tone(attempt) == :info and is_nil(Map.get(attempt, :finished_at_raw))

        bar_end =
          datetime_ms(Map.get(attempt, :finished_at_raw)) ||
            running_end(active?, now_ms, bar_start)

        bar_width = width(bar_end - bar_start, span)

        [
          %{
            id: Map.get(attempt, :id) || "#{start_ms}-#{bar_start}",
            attempt_id: Map.get(attempt, :id),
            left: left(bar_start - start_ms, span, bar_width),
            width: bar_width,
            track: 0,
            tone: tone(attempt),
            running?: running?,
            label: bar_label(attempt, bar_width),
            title: title(attempt),
            start: bar_start,
            finish: bar_end
          }
        ]
    end
  end

  # Concurrent windows for one asset would otherwise draw on top of each other.
  # Greedy interval packing puts each overlapping bar on its own track so the
  # lane shows two windows running at once instead of one bar with a shadow.
  defp pack(bars) do
    bars
    |> Enum.sort_by(& &1.start)
    |> Enum.reduce({[], []}, fn bar, {packed, track_ends} ->
      case Enum.find_index(track_ends, fn track_end -> bar.start >= track_end end) do
        nil ->
          {[%{bar | track: length(track_ends)} | packed], track_ends ++ [bar.finish]}

        index ->
          {[%{bar | track: index} | packed], List.replace_at(track_ends, index, bar.finish)}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
    |> Enum.map(&Map.drop(&1, [:start, :finish]))
  end

  defp bounds(attempts, active?, now_ms) do
    times =
      attempts
      |> Enum.flat_map(fn attempt ->
        [Map.get(attempt, :started_at_raw), Map.get(attempt, :finished_at_raw)]
      end)
      |> Enum.map(&datetime_ms/1)
      |> Enum.reject(&is_nil/1)

    case times do
      [] ->
        {now_ms - @fallback_span_ms, now_ms}

      times ->
        start_ms = Enum.min(times)
        end_ms = if active?, do: max(Enum.max(times), now_ms), else: Enum.max(times)
        pad(start_ms, end_ms)
    end
  end

  # A run whose attempts all finished inside a second would otherwise have a
  # one-millisecond axis, where every tick carries the same label.
  defp pad(start_ms, end_ms) when end_ms - start_ms < 2_000, do: {start_ms, start_ms + 2_000}
  defp pad(start_ms, end_ms), do: {start_ms, end_ms}

  defp ticks(start_ms, span) do
    for index <- 0..@tick_count do
      tick_ms = start_ms + div(span * index, @tick_count)

      %{
        label: clock_label(tick_ms),
        offset: offset(tick_ms - start_ms, span),
        align: tick_align(index)
      }
    end
  end

  defp tick_align(0), do: :start
  defp tick_align(@tick_count), do: :end
  defp tick_align(_index), do: :center

  defp running_end(true, now_ms, bar_start), do: max(now_ms, bar_start)
  defp running_end(false, _now_ms, bar_start), do: bar_start

  defp offset(value, span), do: value |> Kernel.*(100) |> Kernel./(span) |> clamp()

  # The last attempt of a run starts at ~100% of the run's own span, so a bar
  # placed there with a minimum width would hang off the right edge. Pulling it
  # back keeps the whole mark inside the track without moving anything that fits.
  defp left(value, span, bar_width), do: min(offset(value, span), 100.0 - bar_width)

  defp width(value, span) do
    value |> Kernel.*(100) |> Kernel./(span) |> max(@minimum_width) |> min(100.0)
  end

  defp clamp(value), do: value |> max(0.0) |> min(100.0)

  # A bar narrower than this cannot hold even "9 ms" without clipping, and a
  # clipped label is worse than none. Against a real pipeline every bar was at the
  # minimum width — 50 ms of work inside a 9 s run — so this is the common case,
  # not the edge. The duration lives in the lane instead, where it always fits.
  @labelled_width 9.0

  defp bar_label(attempt, width) when width >= @labelled_width,
    do: Map.get(attempt, :duration) || ""

  defp bar_label(_attempt, _width), do: nil

  # The lane's second line carries the window and the duration, because the bar
  # can carry neither at these widths.
  defp detail(attempts) do
    case Enum.reject([window_detail(attempts), duration_detail(attempts)], &is_nil/1) do
      [] -> Map.get(List.first(attempts), :status) || "Unknown"
      parts -> Enum.join(parts, " · ")
    end
  end

  defp window_detail(attempts) do
    windows = attempts |> Enum.map(&Map.get(&1, :window_label)) |> Enum.reject(&is_nil/1)

    case Enum.uniq(windows) do
      [] -> nil
      [single] -> single
      many -> "#{length(many)} windows"
    end
  end

  defp duration_detail([attempt]), do: usable(Map.get(attempt, :duration))

  defp duration_detail(attempts) do
    case attempts |> Enum.map(&Map.get(&1, :duration_ms)) |> Enum.reject(&is_nil/1) do
      [] -> nil
      values -> "#{total_duration_label(Enum.sum(values))} total"
    end
  end

  defp total_duration_label(ms) when ms < 1_000, do: "#{ms} ms"
  defp total_duration_label(ms) when ms < 60_000, do: "#{Float.round(ms / 1_000, 1)} s"
  defp total_duration_label(ms), do: "#{div(ms, 60_000)}m #{div(rem(ms, 60_000), 1_000)}s"

  defp usable(value) when is_binary(value) and value != "" and value != "-", do: value
  defp usable(_value), do: nil

  defp empty_label(attempts, true) do
    if Enum.any?(attempts, &(tone(&1) == :error)), do: "Did not start", else: "Waiting to start"
  end

  defp empty_label(attempts, false) do
    case Map.get(List.first(attempts), :status) do
      nil -> "Not run"
      status -> status
    end
  end

  defp error(nil), do: nil

  defp error(attempt) do
    %{
      summary: Map.get(attempt, :error_summary) || "Failed without a reported reason",
      attempt_id: Map.get(attempt, :id)
    }
  end

  # A lane escalates to its worst outcome: an asset that failed one window and
  # succeeded another needs to read as a problem.
  defp lane_tone(attempts) do
    tones = Enum.map(attempts, &tone/1)

    Enum.find([:error, :warning, :info, :success], :neutral, &(&1 in tones))
  end

  # The dot's accessible name has to name the state it is colouring, and for a
  # lane that means the attempt the lane's tone came from.
  defp lane_status(attempts, tone) do
    attempt = Enum.find(attempts, List.first(attempts), &(tone(&1) == tone))

    Map.get(attempt, :status) || "Unknown"
  end

  defp tone(attempt), do: FavnView.UI.Tokens.tone(Map.get(attempt, :status_tone))

  defp lane_key(attempt) do
    Map.get(attempt, :asset_key) || Map.get(attempt, :asset_ref) ||
      Map.get(attempt, :short_asset_name) || "unknown"
  end

  defp stage_key(attempt) do
    case Map.get(attempt, :stage) do
      stage when is_integer(stage) -> stage
      _absent -> 0
    end
  end

  defp stage_id(0), do: "stage-unknown"
  defp stage_id(stage), do: "stage-#{stage}"

  defp stage_label(0), do: "Ungrouped"
  defp stage_label(stage), do: "Stage #{stage}"

  # Stage 0 is not a stage: it is the assets whose stage the run did not record.
  # Saying so stops "Ungrouped" reading like a category the pipeline declared.
  defp stage_hint(0, lanes), do: lane_hint(lanes) <> " · stage not reported"
  defp stage_hint(_stage, lanes), do: lane_hint(lanes)

  defp lane_hint(lanes) do
    count = length(lanes)
    noun = if count == 1, do: "asset", else: "assets"

    case Enum.count(lanes, &(&1.tone == :error)) do
      0 -> "#{count} #{noun}"
      failed -> "#{count} #{noun} · #{failed} failed"
    end
  end

  defp datetime_ms(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :millisecond)
  defp datetime_ms(_value), do: nil

  defp clock_label(ms) do
    ms |> DateTime.from_unix!(:millisecond) |> Calendar.strftime("%H:%M:%S")
  end

  defp title(attempt) do
    [
      Map.get(attempt, :short_asset_name),
      Map.get(attempt, :window_label),
      Map.get(attempt, :status),
      Map.get(attempt, :duration)
    ]
    |> Enum.reject(&(is_nil(&1) or &1 == "" or &1 == "-"))
    |> Enum.join(" · ")
  end
end
