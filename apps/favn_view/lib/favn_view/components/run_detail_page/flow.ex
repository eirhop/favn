defmodule FavnView.Components.RunDetailPage.Flow do
  @moduledoc """
  The run as work flowing through assets over time.

  One lane per asset, lanes grouped by execution stage, every lane sharing one
  time axis. Stage order is dependency order, so a failure in an early stage sits
  directly above the empty lanes it blocked — the question an operator asks
  straight after "what failed?".

  The axis always fits the run. There is no zoom and no horizontal scroll: a
  chart wider than the viewport asks the operator to pan a canvas to answer a
  question the detail panel answers exactly. Bars keep a minimum width so a
  hundred-millisecond attempt stays visible and clickable.
  """

  use FavnView, :html

  alias FavnView.UI.Tokens

  attr :flow, :map, required: true, doc: "see `FavnView.RunFlow.build/2`"
  attr :selected_attempt_id, :string, default: nil
  attr :class, :any, default: nil

  def flow(assigns) do
    ~H"""
    <section class={["min-w-0", @class]} data-testid="run-flow">
      <.empty_state
        :if={@flow.stages == []}
        icon="hero-square-3-stack-3d"
        title="No asset work yet"
        description="This run has been accepted. Lanes appear as the runner reports each asset."
      />

      <div :if={@flow.stages != []} class="favn-surface-panel overflow-hidden rounded-box">
        <.axis axis={@flow.axis} />

        <div :for={stage <- @flow.stages} data-testid="flow-stage" data-stage={stage.id}>
          <div class="flex items-baseline gap-2 border-b border-base-content/10 bg-base-content/[0.03] px-3 py-1.5">
            <span class="text-xs font-medium">{stage.label}</span>
            <span class="text-xs favn-text-subtle">{stage.hint}</span>
          </div>

          <.lane
            :for={lane <- stage.lanes}
            lane={lane}
            now_offset={@flow.axis.now_offset}
            selected_attempt_id={@selected_attempt_id}
          />
        </div>
      </div>
    </section>
    """
  end

  attr :axis, :map, required: true

  defp axis(assigns) do
    ~H"""
    <div class="grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] border-b border-base-content/10 text-xs favn-text-subtle">
      <div class="border-r border-base-content/10 px-3 py-1.5">Asset</div>
      <div class="relative h-7">
        <span
          :for={tick <- @axis.ticks}
          class={tick_class(tick.align)}
          style={"left: #{tick.offset}%"}
        >
          {tick.label}
        </span>
      </div>
    </div>
    """
  end

  attr :lane, :map, required: true
  attr :now_offset, :any, required: true
  attr :selected_attempt_id, :string, required: true

  defp lane(assigns) do
    ~H"""
    <div
      class="grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] border-b border-base-content/10 last:border-b-0"
      data-testid="flow-lane"
      data-asset-key={@lane.key}
      data-status={@lane.raw_status}
    >
      <div class="min-w-0 border-r border-base-content/10 px-3 py-2">
        <div class="flex min-w-0 items-center gap-1.5">
          <.status_dot tone={@lane.tone} label={@lane.status} />
          <p class="truncate text-sm font-medium" title={@lane.key}>{@lane.name}</p>
        </div>
        <p class="mt-0.5 truncate text-xs favn-text-subtle">{@lane.detail}</p>
      </div>

      <div class="relative py-2" style={"min-height: #{lane_height(@lane.tracks)}rem"}>
        <span
          :if={!is_nil(@now_offset)}
          class="pointer-events-none absolute inset-y-0 z-0 w-px bg-info/50"
          style={"left: #{@now_offset}%"}
          aria-hidden="true"
        />

        <p
          :if={@lane.bars == []}
          class="px-3 py-1 text-xs favn-text-subtle"
          data-testid="flow-lane-empty"
        >
          {@lane.empty_label}
        </p>

        <button
          :for={bar <- @lane.bars}
          type="button"
          phx-click={bar.attempt_id && "select_attempt"}
          phx-value-attempt-id={bar.attempt_id}
          disabled={is_nil(bar.attempt_id)}
          class={[
            "absolute z-10 flex h-5 min-w-1.5 items-center overflow-hidden rounded-full border",
            "px-1.5 text-[0.7rem] leading-none transition disabled:cursor-default",
            "hover:brightness-125 focus-visible:outline focus-visible:outline-2",
            bar_class(bar.tone),
            bar.running? && "favn-flow-bar-running",
            bar.attempt_id == @selected_attempt_id &&
              "ring-2 ring-primary ring-offset-1 ring-offset-base-100"
          ]}
          style={"left: #{bar.left}%; width: #{bar.width}%; top: #{bar_top(bar.track)}rem"}
          title={bar.title}
          data-testid="flow-bar"
          data-tone={bar.tone}
          data-track={bar.track}
        >
          <span :if={bar.label} class="truncate">{bar.label}</span>
        </button>
      </div>
    </div>

    <div
      :if={@lane.error}
      class="grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] border-b border-base-content/10 bg-error/[0.07]"
      data-testid="flow-lane-error"
    >
      <div class="border-r border-base-content/10" />
      <div class="min-w-0 px-3 py-2">
        <p class="text-xs text-error">{@lane.error.summary}</p>
        <button
          :if={@lane.error.attempt_id}
          type="button"
          phx-click="select_attempt"
          phx-value-attempt-id={@lane.error.attempt_id}
          class="mt-1 text-xs font-medium text-error underline-offset-2 hover:underline"
        >
          Open failed attempt
        </button>
      </div>
    </div>
    """
  end

  # Bars are 1.25rem tall on a 1.75rem pitch, so two concurrent windows for one
  # asset read as two rows rather than one bar with a shadow.
  defp bar_top(track), do: 0.5 + track * 1.75
  defp lane_height(tracks), do: 1.0 + tracks * 1.75

  defp bar_class(tone) do
    tone = Tokens.tone(tone)

    [
      Tokens.border_class(tone),
      Tokens.surface_class(tone),
      Tokens.text_class(tone)
    ]
  end

  defp tick_class(:start), do: "absolute top-1/2 left-0 -translate-y-1/2 whitespace-nowrap pl-3"

  defp tick_class(:end),
    do: "absolute top-1/2 -translate-x-full -translate-y-1/2 whitespace-nowrap pr-3"

  defp tick_class(_align),
    do: "absolute top-1/2 -translate-x-1/2 -translate-y-1/2 whitespace-nowrap"
end
