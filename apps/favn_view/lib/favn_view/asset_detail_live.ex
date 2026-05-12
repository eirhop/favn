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
        run_config_open?: false,
        run_config: default_run_config(),
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
         run_config_open?: false,
         run_config: default_run_config(),
         selected_window_error: nil,
         submitted_run_id: nil
       )}
    else
      {:noreply, socket}
    end
  end

  def handle_event("select_window", _params, socket), do: {:noreply, socket}

  def handle_event("open_run_config", _params, socket) do
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
        {:noreply,
         assign(socket,
           run_config_open?: true,
           run_config: default_run_config(),
           selected_window_error: nil,
           submitted_run_id: nil
         )}
    end
  end

  def handle_event("close_run_config", _params, socket) do
    {:noreply, assign(socket, :run_config_open?, false)}
  end

  def handle_event("run_selected_window", params, socket) do
    %{asset: asset, selected_window: selected_window} = socket.assigns
    run_config = run_config_from_params(params)

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
        case run_submit_opts(asset, run_config) do
          {:ok, opts} ->
            submit_selected_window(socket, asset, selected_window, run_config, opts)

          {:error, reason} ->
            {:noreply,
             assign(socket,
               run_config: run_config,
               selected_window_error: submit_error_label(reason)
             )}
        end
    end
  end

  defp submit_selected_window(socket, asset, selected_window, run_config, opts) do
    socket =
      assign(socket,
        run_config: run_config,
        submitting_window_run?: true,
        selected_window_error: nil,
        submitted_run_id: nil
      )

    case FavnOrchestrator.submit_asset_window_run(
           asset.manifest_version_id,
           asset.target_id,
           selected_window.id,
           opts
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

  @impl true
  def render(assigns) do
    ~H"""
    <AssetDetailPage.asset_detail_page
      :if={@asset}
      title={@asset.title}
      status={@asset.status}
      status_tone={@asset.status_tone}
      window_kind_label={@asset.window_kind_label}
      window_range={@asset.window_range}
      nav_items={@nav_items}
      timeline={@asset.timeline}
      active_mode={@active_mode}
      freshness={@asset.freshness}
      selected_window={@selected_window}
      run_config_open?={@run_config_open?}
      run_config={@run_config}
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
      canonical_asset_ref: detail.canonical_asset_ref,
      title: detail.name || asset_name(detail),
      status: status_label(Map.get(detail, :status)),
      status_tone: status_tone(Map.get(detail, :status)),
      freshness: Map.get(detail, :freshness, missing_freshness_detail()),
      window_kind_label: window_kind_label(Map.get(detail, :window)),
      window_range: window_range(timeline),
      timeline: timeline
    }
  end

  defp timeline_window(window) do
    %{
      id: window.id,
      label: window.label,
      value: Map.get(window, :value),
      kind: Map.get(window, :kind),
      date: window.date,
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
    Enum.find(timeline, fn window ->
      detail.latest_run_id && window.latest_run_id == detail.latest_run_id
    end) || List.last(timeline)
  end

  defp timeline_status(:healthy), do: :success
  defp timeline_status(:running), do: :warning
  defp timeline_status(:failed), do: :error
  defp timeline_status(_status), do: :muted

  defp window_range([]), do: "No windows"

  defp window_range([first | _] = timeline) do
    last = List.last(timeline)
    "#{first.label} - #{last.label}"
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

  defp window_kind_label(%{kind: kind}), do: window_kind_label(kind)
  defp window_kind_label(%{"kind" => kind}), do: window_kind_label(kind)
  defp window_kind_label(kind) when kind in [:hour, "hour"], do: "Hourly windows"
  defp window_kind_label(kind) when kind in [:day, "day"], do: "Daily windows"
  defp window_kind_label(kind) when kind in [:month, "month"], do: "Monthly windows"
  defp window_kind_label(kind) when kind in [:year, "year"], do: "Yearly windows"
  defp window_kind_label(_kind), do: "Windows"

  defp default_selected_window(nil), do: nil

  defp default_selected_window(asset) do
    Enum.find(asset.timeline, & &1.current) || List.last(asset.timeline)
  end

  defp asset_timeline(nil), do: []
  defp asset_timeline(asset), do: Map.get(asset, :timeline, [])

  defp missing_freshness_detail do
    %{
      state: :unknown,
      policy: %{kind: :none, label: "no freshness policy"},
      latest_success: nil,
      explanation: "Freshness detail is not available from the backend.",
      reasons: [
        %{
          kind: :insufficient_state,
          message: "Freshness detail is not available from the backend."
        }
      ]
    }
  end

  defp default_run_config, do: %{dependencies: "all", refresh: "auto"}

  defp run_config_from_params(%{"run_config" => params}) when is_map(params) do
    %{
      dependencies: Map.get(params, "dependencies", "all"),
      refresh: Map.get(params, "refresh", "auto")
    }
  end

  defp run_config_from_params(_params), do: default_run_config()

  defp run_submit_opts(asset, %{dependencies: dependencies, refresh: refresh}) do
    with {:ok, dependencies} <- dependency_option(dependencies),
         {:ok, refresh} <-
           refresh_option(refresh, Map.get(asset, :canonical_asset_ref), dependencies) do
      {:ok, [dependencies: dependencies, refresh: refresh]}
    end
  end

  defp dependency_option("all"), do: {:ok, :all}
  defp dependency_option("none"), do: {:ok, :none}
  defp dependency_option(value), do: {:error, {:invalid_dependencies_mode, value}}

  defp refresh_option("auto", _asset_ref, _dependencies), do: {:ok, :auto}
  defp refresh_option("missing", _asset_ref, _dependencies), do: {:ok, :missing}
  defp refresh_option("force_all", _asset_ref, _dependencies), do: {:ok, :force}

  defp refresh_option("force_selected", asset_ref, _dependencies) when is_tuple(asset_ref) do
    {:ok, {:force_assets, [asset_ref]}}
  end

  defp refresh_option("force_selected_upstream", _asset_ref, :none) do
    {:error, {:refresh_include_upstream_requires_dependencies, :all}}
  end

  defp refresh_option("force_selected_upstream", asset_ref, :all) when is_tuple(asset_ref) do
    {:ok, {:force_assets, [asset_ref], include_upstream: true}}
  end

  defp refresh_option(value, _asset_ref, _dependencies),
    do: {:error, {:invalid_refresh_policy, value}}

  defp disabled_reason_label(:asset_has_no_window_policy), do: "This asset has no window policy."
  defp disabled_reason_label(:invalid_window), do: "This window cannot be run."
  defp disabled_reason_label(_reason), do: "This window is not runnable."

  defp submit_error_label(:invalid_asset_target), do: "Asset target is no longer available."
  defp submit_error_label({:invalid_window_id, _reason}), do: "Window id is invalid."

  defp submit_error_label({:window_request_without_policy, _kind}),
    do: "This asset has no window policy."

  defp submit_error_label({:refresh_include_upstream_requires_dependencies, :all}),
    do: "Force selected + upstream requires including upstream dependencies."

  defp submit_error_label({:invalid_dependencies_mode, _value}), do: "Dependency mode is invalid."

  defp submit_error_label({:invalid_refresh_policy, _value}), do: "Refresh behavior is invalid."

  defp submit_error_label(_reason), do: "Could not submit run."
end
