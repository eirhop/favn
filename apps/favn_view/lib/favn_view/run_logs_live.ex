defmodule FavnView.RunLogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.LogPages
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    run = LogsLiveSupport.run_context(run_id)

    socket =
      LogsLiveSupport.mount_logs(socket, %{
        filter: %Favn.Log.Filter{run_id: run_id},
        scope: :run,
        nav_items: LogsLiveSupport.nav_items(:runs),
        title: run[:title],
        subtitle: run[:subtitle] || "Run #{run[:title]}",
        status: run[:status],
        status_tone: run[:status_tone] || :neutral,
        facts: [
          %{label: "Started", value: run[:started_at] || "-"},
          %{label: "Duration", value: run[:duration] || "-"}
        ],
        back_href: ~p"/runs/#{run_id}",
        back_label: "Back to run",
        empty_state: "No logs recorded for this run yet."
      })

    {:ok, socket}
  end

  @impl true
  def handle_info({:favn_log_entry, entry}, socket),
    do: {:noreply, LogsLiveSupport.add_live_log(socket, entry)}

  @impl true
  def handle_event("filter_logs", params, socket),
    do: {:noreply, LogsLiveSupport.handle_filter(socket, params)}

  def handle_event("toggle_wrap", _params, socket),
    do: {:noreply, LogsLiveSupport.toggle(socket, :wrap?)}

  def handle_event("toggle_live_tail", _params, socket),
    do: {:noreply, LogsLiveSupport.toggle(socket, :live_tail?)}

  @impl true
  def terminate(_reason, socket), do: LogsLiveSupport.unsubscribe(socket)

  @impl true
  def render(assigns) do
    ~H"""
    <LogPages.run_logs_page {assigns} />
    """
  end
end
