defmodule FavnView.RunLogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AppShell
  alias FavnView.Components.LogViewer
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    run = LogsLiveSupport.run_context(run_id)

    socket =
      LogsLiveSupport.mount_logs(socket, %{
        filter: %Favn.Log.Filter{run_id: run_id},
        scope: :run,
        nav_items: LogsLiveSupport.nav_items(:runs),
        title: "Run logs",
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
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      back_href={@back_href}
      back_label={@back_label}
      facts={@facts}
    >
      <LogViewer.log_viewer
        logs={@logs}
        visible_logs={@visible_logs}
        filter={@filter}
        scope={@scope}
        title="Logs"
        subtitle="Run-scoped backend logs"
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
