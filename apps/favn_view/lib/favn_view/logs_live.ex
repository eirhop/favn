defmodule FavnView.LogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.LogPages
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(_params, _session, socket) do
    operator_context = socket.assigns.current_scope.operator_context

    socket =
      LogsLiveSupport.mount_logs(socket, %{
        operator_context: operator_context,
        filter: %Favn.Log.Filter{},
        scope: :global,
        nav_items: LogsLiveSupport.nav_items(:logs),
        title: "Logs",
        subtitle: "Live system and run logs",
        empty_state: "No logs yet."
      })

    {:ok, socket}
  end

  @impl true
  def handle_info({:favn_log_entry, entry}, socket),
    do: {:noreply, LogsLiveSupport.add_live_log(socket, entry)}

  def handle_info(:poll_logs, socket), do: {:noreply, LogsLiveSupport.poll(socket)}

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
    <LogPages.global_logs_page {assigns} />
    """
  end
end
