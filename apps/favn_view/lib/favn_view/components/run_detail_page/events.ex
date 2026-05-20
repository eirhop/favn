defmodule FavnView.Components.RunDetailPage.Events do
  @moduledoc false
  use FavnView, :html

  attr :run, :map, required: true

  def events_panel(assigns) do
    ~H"""
    <section data-testid="run-event-timeline">
      <div class="mb-4 flex items-center justify-between gap-3">
        <div>
          <h2 class="text-lg font-medium tracking-tight">Events/logs</h2>
          <p class="text-sm text-base-content/55">Persisted run events for diagnostics.</p>
        </div>
        <span class="badge badge-ghost">{length(@run.events)} events</span>
      </div>

      <div
        :if={@run.events == []}
        class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
      >
        No events persisted for this run yet.
      </div>

      <ol :if={@run.events != []} class="space-y-3">
        <.event_row :for={event <- @run.events} event={event} />
      </ol>
    </section>
    """
  end

  attr :event, :map, required: true

  def event_row(assigns) do
    ~H"""
    <li class="grid gap-2 rounded-box border border-base-content/10 bg-base-content/[0.03] p-3 sm:grid-cols-[5rem_11rem_12rem_1fr] sm:items-start">
      <span class="badge badge-ghost badge-sm">#{@event.sequence}</span>
      <time class="text-xs text-base-content/50">{@event.timestamp}</time>
      <span class="text-sm font-medium text-base-content">{@event.event_type}</span>
      <p class="text-sm text-base-content/65">
        <span :if={@event.asset} class="mr-2 font-mono text-xs text-base-content/45">
          {@event.asset}
        </span>
        {@event.summary}
      </p>
    </li>
    """
  end
end
