defmodule FavnView.AssetRunLogsLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.LogPages
  alias FavnView.LogsLiveSupport

  @impl true
  def mount(%{"run_id" => run_id, "asset_step_id" => asset_step_id}, _session, socket) do
    context = LogsLiveSupport.asset_context(run_id, asset_step_id)

    socket =
      LogsLiveSupport.mount_logs(socket, %{
        filter:
          context.log_filter || %Favn.Log.Filter{run_id: run_id, asset_step_id: asset_step_id},
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
    <LogPages.asset_run_logs_page {assigns} />
    """
  end
end
