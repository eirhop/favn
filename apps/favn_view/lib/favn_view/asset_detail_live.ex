defmodule FavnView.AssetDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel

  @valid_modes ~w(timeline runs lineage docs code details)

  @impl true
  def mount(%{"asset_id" => asset_id}, _session, socket) do
    asset = load_asset(asset_id)

    socket =
      assign(socket,
        asset_id: asset_id,
        asset: asset,
        active_mode: :timeline,
        selected_window: default_selected_window(asset),
        submitting_window_run?: false,
        submitted_run_id: nil,
        selected_window_error: nil,
        nav_items: AssetCataloguePage.nav_items()
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  def handle_event("select_window", %{"window-id" => window_id}, socket) do
    selected_window =
      socket.assigns
      |> Map.get(:asset)
      |> asset_timeline()
      |> Enum.find(&(&1.id == window_id))

    if selected_window do
      {:noreply,
       assign(socket,
         selected_window: selected_window,
         selected_window_error: nil,
         submitted_run_id: nil
       )}
    else
      {:noreply, socket}
    end
  end

  def handle_event("select_window", _params, socket), do: {:noreply, socket}

  def handle_event("run_selected_window", _params, socket) do
    %{asset: asset, selected_window: selected_window} = socket.assigns

    cond do
      is_nil(asset) or is_nil(selected_window) ->
        {:noreply, assign(socket, :selected_window_error, "Select a runnable window first.")}

      !selected_window.run_enabled? ->
        {:noreply,
         assign(
           socket,
           :selected_window_error,
           disabled_reason_label(selected_window.run_disabled_reason)
         )}

      true ->
        socket =
          assign(socket,
            submitting_window_run?: true,
            selected_window_error: nil,
            submitted_run_id: nil
          )

        case FavnOrchestrator.submit_asset_window_run(
               asset.manifest_version_id,
               asset.target_id,
               selected_window.id
             ) do
          {:ok, run_id} ->
            {:noreply,
             socket
             |> put_flash(:info, "Run submitted")
             |> push_navigate(to: ~p"/runs/#{run_id}")}

          {:error, reason} ->
            {:noreply,
             assign(socket,
               submitting_window_run?: false,
               selected_window_error: submit_error_label(reason)
             )}
        end
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <AssetDetailPage.asset_detail_page
      :if={@asset}
      title={@asset.title}
      status={@asset.status}
      status_tone={@asset.status_tone}
      window_range={@asset.window_range}
      nav_items={@nav_items}
      timeline={@asset.timeline}
      active_mode={@active_mode}
      selected_window={@selected_window}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
    />

    <AppShell.app_shell
      :if={!@asset}
      title="Asset not found"
      subtitle={@asset_id}
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-4xl">
        <GlassPanel.glass_panel class="p-8 text-center" data-testid="asset-not-found-state">
          <h2 class="text-xl font-medium">Asset not found</h2>
          <p class="mt-2 text-base-content/60">
            No active catalogue entry matches this asset id.
          </p>
          <.link navigate={~p"/assets"} class="btn btn-primary btn-soft mt-6">
            Back to catalogue
          </.link>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>
    """
  end

  defp load_asset(asset_id) do
    target_id = AssetRoute.from_param(asset_id)

    case FavnOrchestrator.active_asset_detail(target_id) do
      {:ok, detail} -> asset_from_detail(detail)
      {:error, _reason} -> nil
    end
  end

  defp asset_from_detail(detail) do
    timeline = Enum.map(detail.timeline, &timeline_window/1)
    selected_window = timeline_selected_window(timeline, detail)

    timeline =
      Enum.map(timeline, fn window ->
        Map.put(window, :current, selected_window && window.id == selected_window.id)
      end)

    %{
      manifest_version_id: detail.manifest_version_id,
      target_id: detail.target_id,
      title: detail.name || asset_name(detail),
      status: status_label(Map.get(detail, :status)),
      status_tone: status_tone(Map.get(detail, :status)),
      window_range: window_range(timeline),
      timeline: timeline
    }
  end

  defp timeline_window(window) do
    %{
      id: window.id,
      date: window.date,
      day: Calendar.strftime(window.date, "%d") |> String.trim_leading("0"),
      month: Calendar.strftime(window.date, "%b"),
      date_label: window.range,
      range_label: window.range,
      status: timeline_status(window.status),
      latest_run_id: window.latest_run_id,
      latest_run_status: window.latest_run_status,
      latest_run_at: window.latest_run_at,
      run_enabled?: window.run_enabled?,
      run_disabled_reason: window.run_disabled_reason,
      run_label: window.run_label || "Run this window"
    }
  end

  defp timeline_selected_window(timeline, detail) do
    latest_run_date = detail.latest_run_at && DateTime.to_date(detail.latest_run_at)

    Enum.find(timeline, fn window ->
      latest_run_date && window.id == "window:day:#{Date.to_iso8601(latest_run_date)}"
    end) || List.last(timeline)
  end

  defp timeline_status(:healthy), do: :success
  defp timeline_status(:running), do: :warning
  defp timeline_status(:failed), do: :warning
  defp timeline_status(_status), do: :muted

  defp window_range([]), do: "No windows"

  defp window_range([first | _] = timeline) do
    last = List.last(timeline)
    "#{first.month} #{first.day} - #{last.month} #{last.day}, #{last.date.year}"
  end

  defp asset_name(detail) do
    detail
    |> Map.get(:asset_ref, detail[:label] || detail[:target_id])
    |> to_string()
    |> String.split(":")
    |> List.last()
  end

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(_status), do: "Unknown"

  defp status_tone(:healthy), do: :success
  defp status_tone(:running), do: :warning
  defp status_tone(:failed), do: :error
  defp status_tone(_status), do: :neutral

  defp default_selected_window(nil), do: nil

  defp default_selected_window(asset) do
    Enum.find(asset.timeline, & &1.current) || List.last(asset.timeline)
  end

  defp asset_timeline(nil), do: []
  defp asset_timeline(asset), do: Map.get(asset, :timeline, [])

  defp disabled_reason_label(:asset_has_no_window_policy), do: "This asset has no window policy."
  defp disabled_reason_label(:invalid_window), do: "This window cannot be run."
  defp disabled_reason_label(_reason), do: "This window is not runnable."

  defp submit_error_label(:invalid_asset_target), do: "Asset target is no longer available."
  defp submit_error_label({:invalid_window_id, _reason}), do: "Window id is invalid."

  defp submit_error_label({:window_request_without_policy, _kind}),
    do: "This asset has no window policy."

  defp submit_error_label(_reason), do: "Could not submit run."
end
