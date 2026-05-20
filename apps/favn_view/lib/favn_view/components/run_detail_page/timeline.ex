defmodule FavnView.Components.RunDetailPage.Timeline do
  @moduledoc false
  use FavnView, :html
  import FavnView.Components.RunDetailPage.Ui

  attr :run, :map, required: true
  attr :timeline_state, :map, required: true
  attr :timeline_hook?, :boolean, default: false

  def timeline_panel(assigns) do
    assigns = assign(assigns, :timeline, timeline_view(assigns.run, assigns.timeline_state))

    ~H"""
    <section data-testid="timeline-view" class="space-y-4">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 class="text-lg font-medium tracking-tight">Timeline</h2>
          <p class="text-sm text-base-content/55">
            Asset attempts grouped by operational state on execution wall-clock time.
          </p>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <span
            class={timeline_mode_badge_class(@timeline_state.mode)}
            data-testid="timeline-mode-indicator"
            data-mode={@timeline_state.mode}
          >
            {timeline_mode_label(@timeline_state.mode)}
          </span>
          <span :if={@timeline.active?} class="badge badge-info badge-soft">
            Now {now_label(@timeline.now)}
          </span>
        </div>
      </div>

      <.timeline_controls
        windows={@run.windows}
        timeline_state={@timeline_state}
        active?={@timeline.active?}
      />
      <.timeline_minimap timeline={@timeline} timeline_state={@timeline_state} />

      <div class="overflow-hidden rounded-box border border-base-content/10 bg-base-300/10 shadow-inner shadow-primary/5">
        <div
          id="run-timeline-scroll"
          class="overflow-x-auto"
          data-testid="timeline-scroll-region"
          data-active={to_string(@timeline.active?)}
          data-live-follow={to_string(@timeline_state.live_follow?)}
          data-now-offset={@timeline.now_offset}
          data-fit-mode={to_string(@timeline_state.mode == :fit)}
          phx-hook={if(@timeline_hook?, do: "FavnTimeline")}
        >
          <div class="relative" style={"width: #{@timeline.chart_width}; min-width: 76rem;"}>
            <div class="grid grid-cols-[40rem_minmax(38rem,1fr)] border-b border-base-content/10 bg-base-300/35 text-xs text-base-content/60 backdrop-blur">
              <div class="sticky left-0 z-30 grid grid-cols-[minmax(0,1.4fr)_8rem_7rem_8rem_7rem] border-r border-base-content/10 bg-base-300/95 backdrop-blur">
                <div class="p-3">Asset</div>
                <div class="p-3">Window</div>
                <div class="p-3">Status</div>
                <div class="p-3">Started</div>
                <div class="p-3">Duration</div>
              </div>
              <div class="relative grid grid-cols-5 px-4 py-3 text-center">
                <span :for={tick <- @timeline.ticks}>{tick.label}</span>
              </div>
            </div>

            <div class="relative">
              <.timeline_grid
                ticks={@timeline.ticks}
                now_offset={@timeline.now_offset}
                show_now?={@timeline.active?}
              />
              <.timeline_section
                :for={section <- @timeline.sections}
                section={section}
                empty_label={timeline_empty_label(section.id)}
              />
            </div>
          </div>
        </div>
      </div>
    </section>
    """
  end

  attr :windows, :list, required: true
  attr :timeline_state, :map, required: true
  attr :active?, :boolean, required: true

  def timeline_controls(assigns) do
    ~H"""
    <form
      class="grid gap-2 rounded-box border border-base-content/10 bg-base-content/[0.025] p-3 xl:grid-cols-[minmax(12rem,1fr)_9rem_10rem_11rem_auto_auto_auto_auto] xl:items-center"
      data-testid="timeline-controls"
      phx-change="timeline_filter"
    >
      <label class="input input-sm favn-surface-control flex items-center gap-2 rounded-field">
        <.icon name="hero-magnifying-glass" class="size-4 text-base-content/45" />
        <input
          type="search"
          name="timeline[search]"
          value={@timeline_state.search}
          placeholder="Search assets..."
          class="grow"
        />
      </label>
      <select
        name="timeline[status]"
        class="select select-sm favn-surface-control rounded-field"
        aria-label="Status filter"
      >
        <option value="all" selected={@timeline_state.status == "all"}>All statuses</option>
        <option value="running" selected={@timeline_state.status == "running"}>Running</option>
        <option value="succeeded" selected={@timeline_state.status == "succeeded"}>Succeeded</option>
        <option value="failed" selected={@timeline_state.status == "failed"}>Failed</option>
        <option value="queued" selected={@timeline_state.status == "queued"}>Queued</option>
      </select>
      <select
        name="timeline[window]"
        class="select select-sm favn-surface-control rounded-field"
        aria-label="Window filter"
      >
        <option value="all" selected={@timeline_state.window == "all"}>All windows</option>
        <option
          :for={window <- @windows}
          value={window.label}
          selected={@timeline_state.window == window.label}
        >
          {window.label}
        </option>
      </select>
      <select
        name="timeline[group_by]"
        class="select select-sm favn-surface-control rounded-field"
        aria-label="Group by"
        disabled
      >
        <option>Group by Status</option>
      </select>
      <label class="flex items-center gap-2 text-xs text-base-content/65">
        <input
          type="checkbox"
          name="timeline[failed_only]"
          checked={@timeline_state.failed_only?}
          class="toggle toggle-error toggle-xs"
        /> Show failed only
      </label>
      <label class="flex items-center gap-2 text-xs text-base-content/65">
        <input
          type="checkbox"
          name="timeline[running_only]"
          checked={@timeline_state.running_only?}
          class="toggle toggle-info toggle-xs"
        /> Show running only
      </label>
      <div class="join justify-self-start xl:justify-self-end" aria-label="Zoom control">
        <button
          :for={zoom <- timeline_zoom_levels()}
          type="button"
          phx-click="timeline_zoom"
          phx-value-zoom={zoom.id}
          class={zoom_button_class(@timeline_state.zoom == zoom.id and @timeline_state.mode != :fit)}
          data-testid={"timeline-zoom-#{zoom.id}"}
        >
          {zoom.label}
        </button>
      </div>
      <div class="join justify-self-start xl:justify-self-end">
        <button
          type="button"
          phx-click="timeline_fit"
          class={fit_button_class(@timeline_state.mode == :fit)}
          data-testid="timeline-fit-run"
        >
          Fit run
        </button>
        <button
          :if={@active? and !@timeline_state.live_follow?}
          type="button"
          phx-click="timeline_jump_now"
          class="btn btn-sm join-item btn-info"
          data-testid="timeline-jump-now"
        >
          Jump to now
        </button>
      </div>
    </form>
    """
  end

  attr :timeline, :map, required: true
  attr :timeline_state, :map, required: true

  def timeline_minimap(assigns) do
    ~H"""
    <div
      id="run-timeline-minimap"
      class="relative h-12 overflow-hidden rounded-box border border-base-content/10 bg-base-300/20 px-3 py-2 shadow-inner shadow-primary/5"
      data-testid="timeline-minimap"
      data-target="run-timeline-scroll"
    >
      <div class="relative h-full rounded-full bg-base-content/[0.06]">
        <span
          :for={segment <- @timeline.minimap_segments}
          class={minimap_segment_class(segment.tone)}
          style={"left: #{segment.left}%; width: #{segment.width}%;"}
        />
        <span
          :if={@timeline.active?}
          class="absolute inset-y-0 w-px bg-info/80 shadow-[0_0_12px_rgba(14,165,233,0.65)]"
          style={"left: #{@timeline.now_offset}%;"}
        />
        <span
          class="absolute inset-y-0 rounded-full border border-info/70 bg-info/10"
          style={"left: #{@timeline.viewport.left}%; width: #{@timeline.viewport.width}%;"}
          data-testid="timeline-minimap-viewport"
        />
      </div>
      <p class="sr-only">Compressed overview of the full run timeline.</p>
    </div>
    """
  end

  attr :ticks, :list, required: true
  attr :now_offset, :integer, required: true
  attr :show_now?, :boolean, required: true

  def timeline_grid(assigns) do
    ~H"""
    <div class="pointer-events-none absolute inset-y-0 left-[40rem] right-0 z-0 px-4">
      <span
        :for={tick <- @ticks}
        class="absolute inset-y-0 border-l border-base-content/10"
        style={"left: #{tick.offset}%;"}
      />
      <span
        :if={@show_now?}
        class="absolute inset-y-0 border-l border-info/70 shadow-[0_0_18px_rgba(14,165,233,0.5)]"
        style={"left: #{@now_offset}%;"}
        data-testid="timeline-now-marker"
      />
    </div>
    """
  end

  attr :section, :map, required: true
  attr :empty_label, :string, required: true

  def timeline_section(assigns) do
    ~H"""
    <section data-testid="timeline-section" data-section={@section.id} class="relative z-10">
      <div class="grid grid-cols-[40rem_minmax(38rem,1fr)] border-b border-base-content/10 bg-base-300/45">
        <div class="sticky left-0 z-20 flex items-center gap-2 border-r border-base-content/10 bg-base-300/95 px-3 py-2 font-medium backdrop-blur">
          <.icon
            name={timeline_section_icon(@section.id)}
            class={timeline_section_icon_class(@section.id)}
          />
          <span>{@section.label}</span>
          <span class="badge badge-sm badge-ghost">{length(@section.rows)}</span>
        </div>
        <div class="px-4 py-2 text-xs text-base-content/45">{@section.hint}</div>
      </div>

      <div
        :if={@section.rows == []}
        class="grid grid-cols-[40rem_minmax(38rem,1fr)] border-b border-base-content/10 text-sm text-base-content/45"
      >
        <div class="sticky left-0 z-20 border-r border-base-content/10 bg-base-300/80 px-3 py-3 backdrop-blur">
          {@empty_label}
        </div>
        <div class="px-4 py-3">No rows in this state.</div>
      </div>

      <.timeline_attempt_row :for={row <- @section.rows} row={row} />
    </section>
    """
  end

  attr :row, :map, required: true

  def timeline_attempt_row(assigns) do
    ~H"""
    <button
      type="button"
      phx-click={if(@row.attempt_id, do: "select_attempt")}
      phx-value-attempt-id={@row.attempt_id}
      disabled={is_nil(@row.attempt_id)}
      class="group grid w-full grid-cols-[40rem_minmax(38rem,1fr)] border-b border-base-content/10 text-left transition hover:bg-base-content/[0.035] disabled:cursor-default disabled:hover:bg-transparent"
      data-testid="timeline-row"
      data-section={@row.section}
      data-attempt-id={@row.attempt_id}
      data-window-label={@row.window_label}
      data-start-sort={@row.start_sort || "none"}
      data-time-mode={@row.time_mode}
    >
      <div class="sticky left-0 z-20 grid grid-cols-[minmax(0,1.4fr)_8rem_7rem_8rem_7rem] items-center border-r border-base-content/10 bg-base-300/80 text-sm backdrop-blur group-hover:bg-base-300/95">
        <div class="min-w-0 px-3 py-2.5">
          <p class="truncate font-medium text-base-content">{@row.asset_name}</p>
          <p class="truncate font-mono text-[0.68rem] text-base-content/35">{@row.asset_key}</p>
          <p :if={@row.error_summary} class="sr-only">{@row.error_summary}</p>
        </div>
        <div class="truncate px-3 py-2.5 text-xs text-base-content/65">{@row.window_label}</div>
        <div class="px-3 py-2.5">
          <span class={status_badge_class(@row.status_tone)}>{@row.status}</span>
        </div>
        <div class="truncate px-3 py-2.5 text-xs text-base-content/65">{@row.started_at}</div>
        <div class="truncate px-3 py-2.5 text-xs text-base-content/65">{@row.duration}</div>
      </div>
      <div class="relative min-h-12 px-4 py-3">
        <%= if @row.has_real_bar? do %>
          <span
            class={timeline_bar_class(@row.status_tone)}
            style={"left: #{@row.left}%; width: #{@row.width}%;"}
            data-testid="timeline-bar"
          >
            <span class={timeline_bar_end_class(@row.status_tone)} />
          </span>
        <% else %>
          <span
            class="absolute left-4 right-4 top-1/2 border-t border-dashed border-base-content/20"
            data-testid="timeline-queued-placeholder"
          />
          <span class="relative z-10 rounded-full bg-base-300/80 px-2 py-0.5 text-xs text-base-content/45">
            No scheduled start
          </span>
        <% end %>
      </div>
    </button>
    """
  end

  defp timeline_view(run, timeline_state) do
    all_rows =
      run
      |> timeline_attempts()
      |> Enum.map(&timeline_row/1)

    rows = Enum.filter(all_rows, &timeline_row_matches?(&1, timeline_state))

    running =
      rows |> Enum.filter(&(&1.section == "running")) |> Enum.sort_by(&(&1.start_sort || 0))

    ran = rows |> Enum.filter(&(&1.section == "ran")) |> Enum.sort_by(&(&1.start_sort || 0))
    queued = Enum.filter(rows, &(&1.section == "queued"))

    active? = run[:active?] || Enum.any?(all_rows, &(&1.section == "running"))
    now = timeline_now(all_rows)
    {start_ms, end_ms} = timeline_bounds(all_rows, now)
    span = max(end_ms - start_ms, 1)
    chart_width = timeline_chart_width(span, timeline_state)

    sections = [
      %{
        id: "running",
        label: "Running",
        hint: "Bars extend from start time to now.",
        rows: Enum.map(running, &position_timeline_row(&1, start_ms, span, now))
      },
      %{
        id: "ran",
        label: "Ran",
        hint: "Terminal attempts use their real start and finish times.",
        rows: Enum.map(ran, &position_timeline_row(&1, start_ms, span, now))
      },
      %{
        id: "queued",
        label: "Queued",
        hint: "Queued work is shown without fabricated scheduled bars.",
        rows: Enum.map(queued, &position_timeline_row(&1, start_ms, span, now))
      }
    ]

    %{
      active?: active?,
      sections: sections,
      ticks: timeline_ticks(start_ms, span),
      minimap_segments: minimap_segments(rows, start_ms, span, now),
      viewport: minimap_viewport(timeline_state),
      now: now,
      now_offset: percent(now - start_ms, span),
      chart_width: chart_width
    }
  end

  defp timeline_row_matches?(row, state) do
    search = state.search |> to_string() |> String.trim() |> String.downcase()

    search_match? =
      search == "" or
        row.asset_name |> to_string() |> String.downcase() |> String.contains?(search) or
        row.asset_key |> to_string() |> String.downcase() |> String.contains?(search)

    status_match? =
      case state.status do
        "all" -> true
        "running" -> row.section == "running"
        "succeeded" -> row.status_tone == :success
        "failed" -> row.status_tone == :error
        "queued" -> row.section == "queued"
        _status -> true
      end

    window_match? = state.window in ["all", row.window_label]
    failed_match? = !state.failed_only? or row.status_tone == :error
    running_match? = !state.running_only? or row.section == "running"

    search_match? and status_match? and window_match? and failed_match? and running_match?
  end

  defp timeline_attempts(%{matrix: %{rows: matrix_rows}}) do
    matrix_rows
    |> Enum.flat_map(& &1.cells)
    |> Enum.with_index()
    |> Enum.map(fn {attempt, index} -> Map.put_new(attempt, :timeline_order, index) end)
  end

  defp timeline_attempts(%{attempts: attempts}),
    do: Enum.with_index(attempts, &Map.put_new(&1, :timeline_order, &2))

  defp timeline_attempts(_run), do: []

  defp timeline_row(attempt) do
    start_ms = datetime_ms(attempt[:started_at_raw])
    finish_ms = datetime_ms(attempt[:finished_at_raw])
    section = timeline_section_for(attempt[:raw_status])

    %{
      attempt_id: attempt[:id],
      asset_key: attempt[:asset_key] || attempt[:asset_ref] || attempt[:short_asset_name],
      asset_name: attempt[:short_asset_name] || attempt[:asset_name] || attempt[:asset_key],
      window_label: attempt[:window_label] || "No window",
      status: attempt[:status] || status_label(attempt[:raw_status]),
      raw_status: attempt[:raw_status],
      status_tone: attempt[:status_tone],
      started_at: attempt[:started_at] || "-",
      duration: attempt[:duration] || "-",
      error_summary: attempt[:error_summary],
      section: section,
      start_ms: start_ms,
      finish_ms: finish_ms,
      start_sort: start_ms || attempt[:timeline_order],
      order: attempt[:timeline_order] || 0,
      time_mode: timeline_time_mode(section, start_ms, finish_ms),
      has_real_bar?: not is_nil(start_ms) and section != "queued"
    }
  end

  defp timeline_section_for(status) when status in [:running, :retrying], do: "running"

  defp timeline_section_for(status)
       when status in [
              :ok,
              :partial,
              :error,
              :failed,
              :timed_out,
              :cancelled,
              :blocked,
              :skipped_fresh,
              :skipped
            ],
       do: "ran"

  defp timeline_section_for(_status), do: "queued"

  defp timeline_time_mode("running", start_ms, _finish_ms) when is_integer(start_ms),
    do: "started-to-now"

  defp timeline_time_mode("ran", start_ms, finish_ms)
       when is_integer(start_ms) and is_integer(finish_ms), do: "started-to-finished"

  defp timeline_time_mode("queued", nil, _finish_ms), do: "no-start"
  defp timeline_time_mode(_section, _start_ms, _finish_ms), do: "unknown"

  defp position_timeline_row(%{has_real_bar?: false} = row, _start_ms, _span, _now),
    do: Map.merge(row, %{left: 0, width: 0})

  defp position_timeline_row(
         %{section: "running", start_ms: start_ms} = row,
         axis_start,
         span,
         now
       ) do
    left = percent(start_ms - axis_start, span)
    Map.merge(row, %{left: left, width: max(percent(now - start_ms, span), 2)})
  end

  defp position_timeline_row(
         %{start_ms: start_ms, finish_ms: finish_ms} = row,
         axis_start,
         span,
         _now
       ) do
    finish_ms = finish_ms || start_ms
    left = percent(start_ms - axis_start, span)
    Map.merge(row, %{left: left, width: max(percent(finish_ms - start_ms, span), 2)})
  end

  defp timeline_now(rows) do
    if Enum.any?(rows, &(&1.section == "running")) do
      System.system_time(:millisecond)
    else
      timeline_latest_ms(rows) || System.system_time(:millisecond)
    end
  end

  defp timeline_latest_ms(rows) do
    rows
    |> Enum.flat_map(fn row -> [row.start_ms, row.finish_ms] end)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      values -> Enum.max(values)
    end
  end

  defp timeline_bounds(rows, now) do
    real_values =
      rows
      |> Enum.reject(&(&1.section == "queued"))
      |> Enum.flat_map(fn row -> [row.start_ms, row.finish_ms] end)
      |> Enum.reject(&is_nil/1)

    start_ms = Enum.min(real_values ++ [now - 30 * 60 * 1_000])
    end_ms = Enum.max(real_values ++ [now])
    padding = max(div(end_ms - start_ms, 12), 60_000)

    {start_ms - padding, end_ms + padding}
  end

  defp timeline_chart_width(_span, %{mode: :fit}), do: "100%"

  defp timeline_chart_width(span, %{zoom: "full"}) do
    minutes = span / 60_000
    pixels = minutes * 10

    "#{round(max(1216, min(pixels, 12_000)))}px"
  end

  defp timeline_chart_width(span, %{zoom: zoom}) do
    zoom_ms = zoom_ms(zoom)
    pixels = span / zoom_ms * 840

    "#{round(max(1216, min(pixels, 12_000)))}px"
  end

  defp zoom_ms("5m"), do: 5 * 60 * 1_000
  defp zoom_ms("15m"), do: 15 * 60 * 1_000
  defp zoom_ms("30m"), do: 30 * 60 * 1_000
  defp zoom_ms("1h"), do: 60 * 60 * 1_000
  defp zoom_ms("6h"), do: 6 * 60 * 60 * 1_000
  defp zoom_ms(_zoom), do: 30 * 60 * 1_000

  defp minimap_segments(rows, start_ms, span, now) do
    rows
    |> Enum.map(fn row ->
      cond do
        row.has_real_bar? ->
          finish_ms = if(row.section == "running", do: now, else: row.finish_ms || row.start_ms)

          %{
            tone: row.status_tone,
            left: percent(row.start_ms - start_ms, span),
            width: max(percent(finish_ms - row.start_ms, span), 1)
          }

        row.section == "queued" ->
          %{tone: :queued, left: 0, width: 100}

        true ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp minimap_viewport(%{mode: :fit}), do: %{left: 0, width: 100}
  defp minimap_viewport(%{mode: :live, live_follow?: true}), do: %{left: 72, width: 28}
  defp minimap_viewport(%{zoom: "5m"}), do: %{left: 70, width: 12}
  defp minimap_viewport(%{zoom: "15m"}), do: %{left: 66, width: 18}
  defp minimap_viewport(%{zoom: "30m"}), do: %{left: 60, width: 24}
  defp minimap_viewport(%{zoom: "1h"}), do: %{left: 50, width: 34}
  defp minimap_viewport(%{zoom: "6h"}), do: %{left: 20, width: 65}
  defp minimap_viewport(_state), do: %{left: 0, width: 100}

  defp timeline_ticks(start_ms, span) do
    for index <- 0..4 do
      tick_ms = start_ms + div(span * index, 4)
      %{label: time_tick_label(tick_ms), offset: percent(tick_ms - start_ms, span)}
    end
  end

  defp datetime_ms(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :millisecond)
  defp datetime_ms(_value), do: nil

  defp percent(value, span) do
    value
    |> Kernel.*(100)
    |> div(max(span, 1))
    |> max(0)
    |> min(100)
  end

  defp time_tick_label(ms) do
    ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M")
  end

  defp now_label(ms) do
    ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M")
  end

  defp timeline_empty_label("running"), do: "No attempts are running."
  defp timeline_empty_label("ran"), do: "No attempts have completed yet."
  defp timeline_empty_label("queued"), do: "No queued attempts."

  defp timeline_zoom_levels do
    [
      %{id: "5m", label: "5m"},
      %{id: "15m", label: "15m"},
      %{id: "30m", label: "30m"},
      %{id: "1h", label: "1h"},
      %{id: "6h", label: "6h"},
      %{id: "full", label: "Full run"}
    ]
  end

  defp timeline_mode_label(:live), do: "Live"
  defp timeline_mode_label(:fit), do: "Fit run"
  defp timeline_mode_label(_mode), do: "Manual zoom"

  defp timeline_mode_badge_class(:live), do: "badge badge-info badge-soft"
  defp timeline_mode_badge_class(:fit), do: "badge badge-warning badge-soft"
  defp timeline_mode_badge_class(_mode), do: "badge badge-ghost"

  defp zoom_button_class(true), do: "btn btn-sm join-item btn-info"
  defp zoom_button_class(false), do: "btn btn-sm join-item favn-surface-control"

  defp fit_button_class(true), do: "btn btn-sm join-item btn-warning"
  defp fit_button_class(false), do: "btn btn-sm join-item favn-surface-control"

  defp minimap_segment_class(:success),
    do: "absolute top-1/2 h-3 -translate-y-1/2 rounded-full bg-success/70"

  defp minimap_segment_class(:error),
    do: "absolute top-1/2 h-4 -translate-y-1/2 rounded-full bg-error/80"

  defp minimap_segment_class(:info),
    do:
      "absolute top-1/2 h-3 -translate-y-1/2 rounded-full bg-info/75 shadow-[0_0_12px_rgba(14,165,233,0.45)]"

  defp minimap_segment_class(:queued),
    do: "absolute inset-y-2 rounded-full bg-base-content/15"

  defp minimap_segment_class(_tone),
    do: "absolute top-1/2 h-3 -translate-y-1/2 rounded-full bg-base-content/30"

  defp timeline_section_icon("running"), do: "hero-arrow-path"
  defp timeline_section_icon("ran"), do: "hero-check-circle"
  defp timeline_section_icon("queued"), do: "hero-clock"

  defp timeline_section_icon_class("running"), do: "size-4 text-info"
  defp timeline_section_icon_class("ran"), do: "size-4 text-success"
  defp timeline_section_icon_class("queued"), do: "size-4 text-warning"

  defp timeline_bar_class(:success),
    do:
      "absolute top-1/2 h-4 -translate-y-1/2 rounded-full border border-success/70 bg-success/30 shadow-[0_0_18px_rgba(34,197,94,0.25)]"

  defp timeline_bar_class(:error),
    do:
      "absolute top-1/2 h-4 -translate-y-1/2 rounded-full border border-error/75 bg-error/30 shadow-[0_0_18px_rgba(239,68,68,0.25)]"

  defp timeline_bar_class(:info),
    do:
      "absolute top-1/2 h-4 -translate-y-1/2 rounded-full border border-info/80 bg-info/30 shadow-[0_0_22px_rgba(14,165,233,0.35)]"

  defp timeline_bar_class(_tone),
    do:
      "absolute top-1/2 h-4 -translate-y-1/2 rounded-full border border-base-content/25 bg-base-content/10"

  defp timeline_bar_end_class(:success),
    do:
      "absolute right-0 top-1/2 size-3 -translate-y-1/2 rounded-full border border-success bg-base-100 text-success"

  defp timeline_bar_end_class(:error),
    do:
      "absolute right-0 top-1/2 size-3 -translate-y-1/2 rounded-full border border-error bg-base-100 text-error"

  defp timeline_bar_end_class(:info),
    do:
      "absolute right-0 top-1/2 size-3 -translate-y-1/2 rounded-full bg-info shadow-[0_0_16px_rgba(14,165,233,0.85)]"

  defp timeline_bar_end_class(_tone),
    do: "absolute right-0 top-1/2 size-3 -translate-y-1/2 rounded-full bg-base-content/40"
end
