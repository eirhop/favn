defmodule FavnView.AssetRunLogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AppShell
  alias FavnView.Components.LogViewer
  alias FavnView.Components.ModeRail
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(%{"run_id" => run_id, "asset_step_id" => asset_step_id}, _session, socket) do
    context = LogsLiveSupport.asset_context(run_id, asset_step_id)

    socket =
      LogsLiveSupport.mount_logs(socket, %{
        filter: %Favn.Log.Filter{run_id: run_id, asset_step_id: asset_step_id},
        scope: :asset,
        nav_items: LogsLiveSupport.nav_items(:runs),
        title: context.title,
        subtitle: context.subtitle,
        status: context.status,
        status_tone: context.status_tone,
        facts: context.facts,
        back_href: ~p"/runs/#{run_id}",
        back_label: "Back to run",
        empty_state: "No logs recorded for this asset step yet.",
        context_note: context.note
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

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

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
        subtitle="Asset-step scoped backend logs"
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
        context_note={@context_note}
        facts={@facts}
      />

      <:mode_rail>
        <ModeRail.mode_rail active={:logs} modes={asset_log_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  defp asset_log_modes do
    [
      %{id: :logs, label: "Logs", icon: "hero-calendar-days"},
      %{id: :inputs, label: "Inputs", icon: "hero-rocket-launch", disabled: true},
      %{id: :outputs, label: "Outputs", icon: "hero-share-nodes", disabled: true},
      %{id: :error, label: "Error", icon: "hero-book-open", disabled: true},
      %{id: :context, label: "Context", icon: "hero-code-bracket", disabled: true},
      %{id: :debug, label: "Debug", icon: "hero-document-text", disabled: true}
    ]
  end
end
