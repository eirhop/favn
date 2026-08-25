defmodule FavnView.Components.RunDetailPage.Timeline do
  @moduledoc """
  The run drawn as lanes on one shared time axis.

  Geometry comes from `FavnView.RunTimeline`, which is pure and tested apart
  from this markup. Nothing here reads or measures: every offset arrives as a
  percentage, so a lane is placed in CSS with no resize handler and the chart
  costs the page nothing beyond the rows the table already has.

  A bar is a link to the attempt it draws, so the chart is a way into the same
  detail the table leads to rather than a second, shallower view of it. A row
  that has not started has no bar and renders as a ghost track labelled with
  its state, which is how a stage blocked by an earlier failure reads.
  """

  use FavnView, :html

  alias FavnView.LogsViewModel
  alias FavnView.RunTimeline
  alias FavnView.UI.Tokens

  attr :chart, RunTimeline, required: true
  attr :on_toggle_band, :string, default: "toggle_flow_band"

  def timeline(assigns) do
    ~H"""
    <div
      class="favn-timeline flex flex-col gap-2 p-3"
      data-testid="run-timeline"
      data-density={@chart.density}
      style={@chart.axis && advance_style(@chart.axis)}
    >
      <%!-- An unfiltered run always has a band, so an empty chart means the
      filter matched nothing rather than that the run is empty. A run that has
      simply not started yet still has its lanes; they draw as ghosts with no
      axis above them. --%>
      <.empty_state
        :if={@chart.bands == []}
        icon="hero-funnel"
        title="No assets match the filter"
        description="Clear a status filter to see the rest of the run."
      />

      <div :if={@chart.axis} class="favn-timeline-axis" data-testid="run-timeline-axis">
        <span class="favn-timeline-label"></span>
        <span class="favn-timeline-track">
          <span
            :for={tick <- @chart.axis.ticks}
            class="favn-timeline-tick favn-text-subtle"
            style={"left:#{tick.offset}%"}
          >
            {tick.label}
          </span>
        </span>
      </div>

      <div
        :for={band <- @chart.bands}
        class="favn-timeline-band"
        data-testid="run-timeline-band"
        data-band={band.id}
        data-collapsed={to_string(band.collapsed?)}
      >
        <div class="favn-timeline-band-header">
          <%!-- Only a dense chart collapses, so only a dense chart offers the
          control. Expanding one stage leaves the rest collapsed. --%>
          <button
            :if={@chart.density == :dense}
            type="button"
            phx-click={@on_toggle_band}
            phx-value-band={band.id}
            aria-expanded={to_string(!band.collapsed?)}
            data-testid="run-timeline-band-toggle"
            class="favn-timeline-label truncate text-left text-xs font-medium link link-hover"
          >
            {band.label}
          </button>

          <span
            :if={@chart.density != :dense}
            class="favn-timeline-label truncate text-xs font-medium"
          >
            {band.label}
          </span>

          <span class="favn-timeline-track text-xs favn-text-subtle">
            {summary_label(band.summary)}
          </span>
        </div>

        <.band_strip :if={band.collapsed?} band={band} now_offset={now_offset(@chart)} />

        <.lane
          :for={lane <- band.lanes}
          :if={!band.collapsed?}
          lane={lane}
          density={lane_density(@chart.density)}
          now_offset={now_offset(@chart)}
        />
      </div>
    </div>
    """
  end

  attr :lane, :map, required: true
  attr :density, :atom, required: true
  attr :now_offset, :any, required: true

  defp lane(assigns) do
    ~H"""
    <div class="favn-timeline-lane" data-testid="run-timeline-lane" data-state={@lane.state}>
      <span
        :if={@density != :dense}
        class="favn-timeline-label truncate text-xs"
        title={@lane.name}
      >
        {@lane.name}
      </span>

      <div class="favn-timeline-track">
        <.now_line offset={@now_offset} />

        <.link
          :if={@lane.bar}
          navigate={~p"/runs/#{@lane.run_id}/assets/#{@lane.id}"}
          class={["favn-timeline-bar", Tokens.fill_class(tone(@lane.outcome))]}
          style={bar_style(@lane.bar)}
          data-testid="run-timeline-bar"
          data-running={to_string(@lane.bar.running?)}
          title={bar_title(@lane)}
          aria-label={bar_title(@lane)}
        ></.link>

        <span
          :if={is_nil(@lane.bar)}
          class="favn-timeline-ghost text-xs favn-text-subtle"
          data-testid="run-timeline-ghost"
        >
          {LogsViewModel.status_label(@lane.state)}
        </span>
      </div>
    </div>
    """
  end

  attr :band, :map, required: true
  attr :now_offset, :any, required: true

  defp band_strip(assigns) do
    ~H"""
    <div class="favn-timeline-lane" data-testid="run-timeline-band-strip">
      <span class="favn-timeline-label truncate text-xs favn-text-subtle">
        {@band.summary.total} assets
      </span>

      <div class="favn-timeline-track">
        <.now_line offset={@now_offset} />

        <span
          :if={@band.summary.bar}
          class={["favn-timeline-bar", Tokens.fill_class(summary_tone(@band.summary))]}
          style={bar_style(@band.summary.bar)}
          data-running={to_string(@band.summary.bar.running?)}
          title={summary_label(@band.summary)}
        ></span>
      </div>
    </div>
    """
  end

  attr :offset, :any, required: true

  defp now_line(assigns) do
    ~H"""
    <span
      :if={@offset}
      class="favn-timeline-now"
      style={"left:#{@offset}%"}
      data-testid="run-timeline-now"
      aria-hidden="true"
    ></span>
    """
  end

  defp now_offset(%RunTimeline{axis: %{now_offset: offset}}), do: offset
  defp now_offset(_chart), do: nil

  # A stage the operator expanded out of a dense chart is read one stage at a
  # time, so it is drawn compact — with its labels — while the rest stay strips.
  defp lane_density(:dense), do: :compact
  defp lane_density(density), do: density

  # The bar carries its own remaining width as a custom property so the CSS
  # animation can grow it toward the axis end without another render.
  defp bar_style(bar) do
    remaining = Float.round(100.0 - bar.offset - bar.width, 3)

    "left:#{bar.offset}%;--favn-bar-width:#{bar.width}%;--favn-bar-remaining:#{remaining}%"
  end

  defp advance_style(%{advance_ms: nil}), do: nil
  defp advance_style(%{advance_ms: ms}), do: "--favn-timeline-advance:#{ms}ms"

  defp bar_title(lane) do
    [lane.name, LogsViewModel.status_label(lane.state), duration(lane)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  defp duration(%{duration_ms: nil}), do: nil
  defp duration(%{duration_ms: ms}), do: RunTimeline.elapsed_label(ms)

  # A collapsed band is read for whether its stage finished and whether anything
  # in it went wrong, so the strip takes the worst outcome it contains.
  defp summary_tone(%{failed: failed}) when failed > 0, do: :error
  defp summary_tone(%{running: running}) when running > 0, do: :info
  defp summary_tone(%{waiting: waiting}) when waiting > 0, do: :neutral
  defp summary_tone(_summary), do: :success

  defp summary_label(summary) do
    [
      {summary.succeeded, "succeeded"},
      {summary.failed, "failed"},
      {summary.running, "running"},
      {summary.waiting, "waiting"}
    ]
    |> Enum.reject(fn {count, _word} -> count == 0 end)
    |> Enum.map_join(" · ", fn {count, word} -> "#{count} #{word}" end)
  end

  defp tone(:succeeded), do: :success
  defp tone(:failed), do: :error
  defp tone(:running), do: :info
  defp tone(:waiting), do: :neutral
end
