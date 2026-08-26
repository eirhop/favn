defmodule FavnView.Components.RunDetailPage.Comparison do
  @moduledoc """
  Several window runs drawn as one lane per asset and one track per window.

  Geometry comes from `FavnView.RunComparison`, which is pure and tested apart
  from this markup. The tracks keep the order the page fixed and the legend
  states that order once, so a position means the same window in every lane
  without repeating the window's name on every track.

  Colour still encodes status. A track that is empty says why it is empty —
  the window never planned the asset, has not reached it, or could not be read
  — because those three read identically as a gap and mean entirely different
  things.
  """

  use FavnView, :html

  alias FavnView.LogsViewModel
  alias FavnView.RunComparison
  alias FavnView.UI.Tokens

  attr :chart, RunComparison, required: true
  attr :on_set_alignment, :string, default: "set_flow_alignment"

  def comparison(assigns) do
    ~H"""
    <div
      class="favn-timeline favn-comparison flex flex-col gap-2 p-3"
      data-testid="run-comparison"
      data-density={@chart.density}
      data-alignment={@chart.alignment}
    >
      <div class="flex flex-wrap items-center justify-between gap-2">
        <%!-- The number is the whole legend: it repeats on every track in every
        lane, so a row identifies its own window without the operator carrying
        an order in their head back to this line. --%>
        <div class="flex flex-wrap gap-1" data-testid="run-comparison-legend">
          <.badge
            :for={head <- @chart.tracks}
            tone={head_tone(head)}
            variant={:outline}
            data-testid="run-comparison-track-head"
            data-track={head.track}
            data-state={head.state}
            title={head.title || head.label}
          >
            <span class="favn-track-index">{head.track}</span>
            {head.label || head.run_id}
            <span :if={head.state != :loaded} class="favn-text-subtle">{head_label(head)}</span>
          </.badge>
        </div>

        <.alignment_control chart={@chart} on_set_alignment={@on_set_alignment} />
      </div>

      <div :if={@chart.axis} class="favn-timeline-axis" data-testid="run-comparison-axis">
        <span class="favn-timeline-label"></span>
        <span class="favn-comparison-row">
          <span class="favn-comparison-index"></span>
          <span class="favn-timeline-track">
            <span
              :for={tick <- @chart.axis.ticks}
              class="favn-timeline-tick favn-text-subtle"
              style={"left:#{tick.offset}%"}
            >
              {tick.label}
            </span>
          </span>
        </span>
      </div>

      <div
        :for={band <- @chart.bands}
        class="favn-timeline-band"
        data-testid="run-comparison-band"
        data-band={band.id}
      >
        <div class="favn-timeline-band-header">
          <span class="favn-timeline-label truncate text-sm font-medium">{band.label}</span>
          <span class="favn-comparison-row">
            <span class="favn-comparison-index"></span>
            <span class="favn-timeline-track"></span>
          </span>
        </div>

        <.lane :for={lane <- band.lanes} lane={lane} density={@chart.density} />
      </div>
    </div>
    """
  end

  attr :chart, RunComparison, required: true
  attr :on_set_alignment, :string, required: true

  defp alignment_control(assigns) do
    ~H"""
    <div class="flex items-center gap-2 text-sm">
      <span class="favn-text-subtle">Align</span>

      <div class="join" role="group" data-testid="run-comparison-alignment">
        <button
          :for={{value, label} <- alignments()}
          type="button"
          phx-click={@on_set_alignment}
          phx-value-alignment={value}
          disabled={value == :wall_clock and not @chart.wall_clock?}
          aria-pressed={to_string(@chart.alignment == value)}
          data-testid="run-comparison-alignment-option"
          data-alignment={value}
          class={[
            "btn btn-sm join-item",
            @chart.alignment == value && "btn-active"
          ]}
        >
          {label}
        </button>
      </div>

      <%!-- The control explains itself rather than simply being dead: the span
      ratio is the reason wall clock would draw nothing readable. --%>
      <span
        :if={not @chart.wall_clock? and @chart.span_ratio}
        class="favn-text-subtle"
        data-testid="run-comparison-alignment-unavailable"
      >
        Windows span {@chart.span_ratio}× the longest run
      </span>
    </div>
    """
  end

  attr :lane, :map, required: true
  attr :density, :atom, required: true

  defp lane(assigns) do
    ~H"""
    <div class="favn-comparison-lane" data-testid="run-comparison-lane" data-lane={@lane.id}>
      <span :if={@density != :dense} class="favn-timeline-label truncate text-sm" title={@lane.name}>
        {@lane.name}
      </span>

      <div class="favn-comparison-tracks">
        <%!-- No now line: on a window-aligned axis each window's now sits at a
        different offset, so one shared line would mark the wrong instant on
        every track but the leader. A running bar's own leading edge is that
        window's now, and it advances at the real-time rate its track sets. --%>
        <div
          :for={track <- @lane.tracks}
          class="favn-comparison-row"
          data-testid="run-comparison-track"
          data-track={track.track}
          data-presence={track.presence}
        >
          <span class="favn-comparison-index favn-text-subtle" aria-hidden="true">
            {track.track}
          </span>

          <div class="favn-timeline-track favn-comparison-track" style={advance_style(track)}>
            <.link
              :if={track.bar}
              navigate={~p"/runs/#{track.run_id}/assets/#{track.attempt_id}"}
              class={["favn-timeline-bar", Tokens.fill_class(tone(track.outcome))]}
              style={bar_style(track.bar)}
              data-testid="run-comparison-bar"
              data-running={to_string(track.bar.running?)}
              title={bar_title(@lane, track)}
              aria-label={bar_title(@lane, track)}
            ></.link>

            <span
              :if={is_nil(track.bar)}
              class="favn-comparison-blank text-sm favn-text-subtle"
              data-testid="run-comparison-blank"
              title={blank_title(@lane, track)}
            >
              {presence_label(track.presence)}
            </span>
          </div>
        </div>
      </div>
    </div>
    """
  end

  defp alignments, do: [{:window, "Window start"}, {:wall_clock, "Wall clock"}]

  defp advance_style(%{advance_ms: nil}), do: nil
  defp advance_style(%{advance_ms: ms}), do: "--favn-timeline-advance:#{ms}ms"

  defp bar_style(bar) do
    remaining = Float.round(100.0 - bar.offset - bar.width, 3)

    "left:#{bar.offset}%;--favn-bar-width:#{bar.width}%;--favn-bar-remaining:#{remaining}%"
  end

  defp bar_title(lane, track) do
    [lane.name, track.label, LogsViewModel.status_label(track.state)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  defp blank_title(lane, track) do
    [lane.name, track.label, blank_reason(track.presence)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  # A gap is ambiguous, so each kind of gap names itself. Not planned is a plan
  # difference between the windows; not read is this page's own failure.
  defp blank_reason(:absent), do: "This window did not plan this asset"
  defp blank_reason(:waiting), do: "This window has not reached this asset"
  defp blank_reason(:unavailable), do: "This window could not be read"
  defp blank_reason(:loading), do: "Still loading this window"
  defp blank_reason(_presence), do: nil

  defp presence_label(:absent), do: "Not planned"
  defp presence_label(:waiting), do: "Waiting"
  defp presence_label(:unavailable), do: "Unavailable"
  defp presence_label(:loading), do: "Loading"
  defp presence_label(_presence), do: ""

  defp head_label(%{state: :unavailable}), do: "unavailable"
  defp head_label(%{state: :loading}), do: "loading"
  defp head_label(_head), do: nil

  defp head_tone(%{state: :unavailable}), do: :error
  defp head_tone(%{state: :loading}), do: :neutral
  defp head_tone(%{selected?: true}), do: :info
  defp head_tone(_head), do: :neutral

  defp tone(:succeeded), do: :success
  defp tone(:failed), do: :error
  defp tone(:running), do: :info
  defp tone(:waiting), do: :neutral
  defp tone(_outcome), do: :neutral
end
