defmodule FavnView.Components.RunDetailPage.Events do
  @moduledoc """
  The persisted run event stream, for when the flow is not enough.

  This is a diagnostic view of a table, not a peer of "what happened in this run".
  It answers one question the flow cannot: what did a stuck run last report.
  """

  use FavnView, :html

  attr :run, :map, required: true

  def events_panel(assigns) do
    ~H"""
    <section data-testid="run-event-timeline">
      <.empty_state
        :if={@run.events == []}
        icon="hero-signal"
        title="No events yet"
        description="Events appear as the orchestrator and runner report progress."
      />
      <.panel :if={@run.events != []} padding={:sm}>
        <:header title="Events" subtitle="Persisted run events, oldest first." />
        <:actions>
          <.count_badge count={length(@run.events)} label="events" />
        </:actions>

        <.data_table id="run-events" rows={@run.events} row_testid="run-event-row">
          <:col :let={event} label="#" class="w-16">
            <span class="font-mono text-sm favn-text-subtle">{event.sequence}</span>
          </:col>

          <:col :let={event} label="Time" class="w-40">
            <time class="text-sm favn-text-subtle">{event.timestamp}</time>
          </:col>

          <:col :let={event} label="Event" class="w-48">{event.event_type}</:col>

          <:col :let={event} label="Asset" class="w-56">
            <.mono :if={event.asset} value={event.asset} truncate />
          </:col>

          <:col :let={event} label="Detail">{event.summary}</:col>
        </.data_table>
      </.panel>
    </section>
    """
  end
end
