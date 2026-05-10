defmodule FavnView.LogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AppShell
  alias FavnView.Components.LogViewer
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(_params, _session, socket) do
    socket =
      LogsLiveSupport.mount_logs(socket, %{
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
    <AppShell.app_shell title={@title} subtitle={@subtitle} nav_items={@nav_items}>
      <LogViewer.log_viewer
        logs={@logs}
        visible_logs={@visible_logs}
        filter={@filter}
        scope={@scope}
        title="Logs"
        subtitle={@subtitle}
        status={@logs_status}
        live?={@live?}
        live_tail?={@live_tail?}
        wrap?={@wrap?}
        search_query={@search_query}
        selected_level={@selected_level}
        selected_source={@selected_source}
        next_cursor={@next_cursor}
        empty_state={@empty_state}
        warning={@stream_warning}
      />
    </AppShell.app_shell>
    """
  end
end
