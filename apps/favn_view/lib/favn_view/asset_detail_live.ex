defmodule FavnView.AssetDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Auth.Scope

  @valid_modes ~w(timeline runs lineage docs code details)

  @impl true
  def mount(%{"asset_id" => asset_id}, _session, socket) do
    asset = load_asset(asset_id)

    socket =
      assign(socket,
        asset_id: asset_id,
        asset: asset,
        active_mode: :timeline,
        active_timeline: :refresh,
        selected_window: nil,
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
    current = socket.assigns.selected_window

    selected_window =
      socket.assigns
      |> Map.get(:asset)
      |> asset_timeline(socket.assigns.active_timeline)
      |> Enum.find(&(&1.id == window_id))

    cond do
      current && current.id == window_id ->
        {:noreply,
         assign(socket,
           selected_window: nil,
           run_config_open?: false,
           run_config: default_run_config(),
           selected_window_error: nil,
           submitted_run_id: nil
         )}

      selected_window ->
        {:noreply,
         assign(socket,
           selected_window: selected_window,
           run_config_open?: false,
           run_config: default_run_config(),
           selected_window_error: nil,
           submitted_run_id: nil
         )}

      true ->
        {:noreply, socket}
    end
  end

  def handle_event("select_window", _params, socket), do: {:noreply, socket}

  def handle_event("set_timeline", %{"timeline" => timeline}, socket)
      when timeline in ["refresh", "data_coverage"] do
    {:noreply,
     assign(socket,
       active_timeline: timeline_atom(timeline),
       selected_window: nil,
       run_config_open?: false,
       run_config: default_run_config(),
       selected_window_error: nil,
       submitted_run_id: nil
     )}
  end

  def handle_event("set_timeline", _params, socket), do: {:noreply, socket}

  def handle_event("open_run_config", _params, socket) do
    %{asset: asset, selected_window: selected_window} = socket.assigns

    cond do
      !socket.assigns.can_submit_runs? ->
        {:noreply,
         assign(socket, :selected_window_error, "Operator role required to submit runs.")}

      is_nil(asset) or !asset.can_run_asset? ->
        {:noreply, assign(socket, :selected_window_error, "This asset cannot be run.")}

      selected_window && !selected_window.run_enabled? ->
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
           run_config: context_run_config(asset, socket.assigns.active_timeline, selected_window),
           selected_window_error: nil,
           submitted_run_id: nil
         )}
    end
  end

  def handle_event("close_run_config", _params, socket) do
    {:noreply, assign(socket, :run_config_open?, false)}
  end

  def handle_event("change_run_config", params, socket) do
    run_config =
      params
      |> run_config_from_params(socket.assigns.run_config)
      |> apply_asset_range_defaults(socket.assigns.selected_window)

    {:noreply, assign(socket, :run_config, run_config)}
  end

  def handle_event("run_selected_window", params, socket) do
    %{asset: asset, selected_window: selected_window} = socket.assigns

    run_config =
      params
      |> run_config_from_params(socket.assigns.run_config)
      |> apply_asset_range_defaults(selected_window)

    cond do
      !socket.assigns.can_submit_runs? ->
        {:noreply,
         assign(socket,
           run_config: run_config,
           selected_window_error: "Operator role required to submit runs."
         )}

      is_nil(asset) or !asset.can_run_asset? ->
        {:noreply, assign(socket, :selected_window_error, "This asset cannot be run.")}

      selected_window && !selected_window.run_enabled? ->
        {:noreply,
         assign(
           socket,
           :selected_window_error,
           disabled_reason_label(selected_window.run_disabled_reason)
         )}

      true ->
        case run_submit_opts(asset, run_config) do
          {:ok, opts} ->
            submit_asset_run(socket, asset, selected_window, run_config, opts)

          {:error, reason} ->
            {:noreply,
             assign(socket,
               run_config: run_config,
               selected_window_error: submit_error_label(reason)
             )}
        end
    end
  end

  defp submit_asset_run(socket, asset, selected_window, run_config, opts) do
    socket =
      assign(socket,
        run_config: run_config,
        submitting_window_run?: true,
        selected_window_error: nil,
        submitted_run_id: nil
      )

    case submit_asset_window_run(socket, asset, selected_window, run_config, opts) do
      {:ok, run_id, :single} ->
        {:noreply,
         socket
         |> put_flash(:info, "Run submitted")
         |> push_navigate(to: ~p"/runs/#{run_id}")}

      {:ok, run_id, :backfill} ->
        {:noreply,
         socket
         |> put_flash(:info, "Asset backfill submitted")
         |> push_navigate(to: ~p"/runs/#{run_id}")}

      {:error, reason} ->
        {:noreply,
         assign(socket,
           submitting_window_run?: false,
           selected_window_error: submit_error_label(reason)
         )}
    end
  end

  defp submit_asset_window_run(socket, asset, nil, %{to: to} = run_config, opts)
       when is_binary(to) and to != "" do
    request = %{
      range: range_request(run_config),
      dependencies: Keyword.get(opts, :dependencies),
      refresh: backfill_refresh_option(opts)
    }

    case FavnOrchestrator.submit_operator_asset_backfill(
           actor_context(socket),
           asset.manifest_version_id,
           asset.target_id,
           request
         ) do
      {:ok, run_id} -> {:ok, run_id, :backfill}
      {:error, reason} -> {:error, reason}
    end
  end

  defp submit_asset_window_run(socket, asset, selected_window, run_config, opts) do
    request = %{
      selection: timeline_selection(selected_window, run_config),
      config: Map.new(opts)
    }

    case FavnOrchestrator.submit_operator_asset_run(
           actor_context(socket),
           asset.manifest_version_id,
           asset.target_id,
           request
         ) do
      {:ok, run_id} -> {:ok, run_id, :single}
      {:error, reason} -> {:error, reason}
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
      refresh_timeline_label={@asset.refresh_timeline_label}
      refresh_cadence_label={@asset.refresh_cadence_label}
      data_coverage_timeline_label={@asset.data_coverage_timeline_label}
      window_range={@asset.window_range}
      refresh_window_range={@asset.refresh_window_range}
      data_coverage_window_range={@asset.data_coverage_window_range}
      active_timeline={@active_timeline}
      has_data_windows?={@asset.has_data_windows?}
      can_run_asset?={@asset.can_run_asset?}
      nav_items={@nav_items}
      refresh_timeline={@asset.refresh_timeline}
      data_coverage_timeline={@asset.data_coverage_timeline}
      active_mode={@active_mode}
      freshness={@asset.freshness}
      selected_window={@selected_window}
      run_config_open?={@run_config_open?}
      run_config={@run_config}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
      can_submit_runs?={@can_submit_runs?}
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

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope
    %{actor: scope.actor, session: scope.session}
  end

  defp asset_from_detail(detail) do
    refresh_timeline = Enum.map(detail.refresh_timeline, &timeline_window/1)

    data_coverage_timeline =
      detail.data_coverage_timeline && Enum.map(detail.data_coverage_timeline, &timeline_window/1)

    timeline = refresh_timeline

    %{
      manifest_version_id: detail.manifest_version_id,
      target_id: detail.target_id,
      canonical_asset_ref: detail.canonical_asset_ref,
      can_run_asset?: detail.can_run_asset?,
      has_data_windows?: detail.has_data_windows?,
      title: detail.name || asset_name(detail),
      status: status_label(Map.get(detail, :status)),
      status_tone: status_tone(Map.get(detail, :status)),
      freshness: Map.get(detail, :freshness, missing_freshness_detail()),
      window_kind_label: window_kind_label(Map.get(detail, :window)),
      refresh_timeline_label: Map.get(detail, :refresh_timeline_label, "Refresh periods"),
      refresh_cadence_label: Map.get(detail, :refresh_cadence_label, "Refresh cadence"),
      data_coverage_timeline_label:
        Map.get(detail, :data_coverage_timeline_label, "Data windows"),
      window_range: window_range(timeline),
      refresh_window_range: window_range(refresh_timeline),
      data_coverage_window_range: window_range(data_coverage_timeline || []),
      refresh_timeline: refresh_timeline,
      data_coverage_timeline: data_coverage_timeline,
      timeline: timeline
    }
  end

  defp timeline_window(window) do
    %{
      id: window.id,
      label: window.label,
      value: Map.get(window, :value),
      kind: Map.get(window, :kind),
      source: Map.get(window, :source),
      timezone: Map.get(window, :timezone),
      date: window.date,
      date_label: window.range,
      range_label: window.range,
      status: timeline_status(window.status),
      status_label: timeline_status_label(window.status),
      latest_run_id: window.latest_run_id,
      latest_run_status: window.latest_run_status,
      latest_run_at: window.latest_run_at,
      run_enabled?: window.run_enabled?,
      run_disabled_reason: window.run_disabled_reason,
      run_label: window.run_label || "Run asset",
      default_run_config: Map.get(window, :default_run_config, %{}),
      latest_run_config: Map.get(window, :latest_run_config)
    }
  end

  defp timeline_status(:healthy), do: :success
  defp timeline_status(:fresh), do: :success
  defp timeline_status(:covered), do: :success
  defp timeline_status(:running), do: :warning
  defp timeline_status(:failed), do: :error
  defp timeline_status(:stale), do: :warning
  defp timeline_status(:missing), do: :muted
  defp timeline_status(_status), do: :muted

  defp timeline_status_label(:fresh), do: "Fresh"
  defp timeline_status_label(:covered), do: "Covered"
  defp timeline_status_label(:missing), do: "Missing"
  defp timeline_status_label(:stale), do: "Stale"
  defp timeline_status_label(:failed), do: "Failed"
  defp timeline_status_label(:running), do: "Running"
  defp timeline_status_label(_status), do: "Unknown"

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

  defp timeline_atom("refresh"), do: :refresh
  defp timeline_atom("data_coverage"), do: :data_coverage

  defp asset_timeline(nil, _active_timeline), do: []
  defp asset_timeline(asset, :refresh), do: Map.get(asset, :refresh_timeline, [])

  defp asset_timeline(asset, :data_coverage),
    do: Map.get(asset, :data_coverage_timeline, []) || []

  defp context_run_config(asset, active_timeline, nil) do
    asset
    |> asset_timeline(active_timeline)
    |> List.last()
    |> selected_run_config()
  end

  defp context_run_config(_asset, _active_timeline, selected_window),
    do: selected_run_config(selected_window)

  defp selected_run_config(nil), do: default_run_config()

  defp selected_run_config(%{latest_run_config: config}) when is_map(config) do
    config_from_backend(config)
  end

  defp selected_run_config(%{default_run_config: config}) when is_map(config) do
    config_from_backend(config)
  end

  defp config_from_backend(config) do
    %{
      dependencies: config |> Map.get(:dependencies, :all) |> config_atom_value(),
      refresh: config |> Map.get(:refresh, :auto) |> refresh_config_value(),
      source: config |> Map.get(:source) |> source_config_value(),
      kind: config |> Map.get(:kind) |> kind_config_value(),
      value: config |> Map.get(:value, "") |> to_string(),
      to: config |> Map.get(:to, "") |> to_string(),
      timezone: config |> Map.get(:timezone, "Etc/UTC") |> to_string()
    }
  end

  defp config_atom_value(value) when is_atom(value), do: Atom.to_string(value)
  defp config_atom_value(value) when is_binary(value), do: value
  defp config_atom_value(_value), do: "all"

  defp kind_config_value(value) when value in [:hour, :day, :month, :year],
    do: Atom.to_string(value)

  defp kind_config_value(value) when value in ["hour", "day", "month", "year"], do: value
  defp kind_config_value(_value), do: ""

  defp refresh_config_value(value) when value in [:auto, "auto"], do: "auto"
  defp refresh_config_value(value) when value in [:missing, "missing"], do: "missing"

  defp refresh_config_value(value) when value in [:force, :force_all, "force", "force_all"],
    do: "force_all"

  defp refresh_config_value(value) when value in [:force_selected, "force_selected"],
    do: "force_selected"

  defp refresh_config_value(value)
       when value in [:force_selected_upstream, "force_selected_upstream"],
       do: "force_selected_upstream"

  defp refresh_config_value(_value), do: "auto"

  defp source_config_value(:refresh_timeline), do: "refresh_timeline"
  defp source_config_value(:data_coverage_timeline), do: "data_coverage_timeline"
  defp source_config_value("refresh_timeline"), do: "refresh_timeline"
  defp source_config_value("data_coverage_timeline"), do: "data_coverage_timeline"
  defp source_config_value(_source), do: nil

  defp timeline_selection(nil, %{source: source, kind: kind, value: value, timezone: timezone})
       when is_binary(source) and source != nil and is_binary(kind) and kind != "" and
              is_binary(value) and value != "" do
    %{
      source: source,
      id: selection_id(source, kind, value),
      kind: kind,
      value: value,
      timezone: timezone,
      run_id: nil
    }
  end

  defp timeline_selection(nil, _run_config), do: nil

  defp timeline_selection(window, _run_config) do
    %{
      source: window.source,
      id: window.id,
      kind: window.kind,
      value: window.value,
      timezone: window.timezone,
      run_id: window.latest_run_id
    }
  end

  defp selection_id("refresh_timeline", kind, value), do: "refresh:#{kind}:#{value}"
  defp selection_id("data_coverage_timeline", kind, value), do: "window:#{kind}:#{value}"
  defp selection_id(_source, kind, value), do: "window:#{kind}:#{value}"

  defp range_request(%{kind: kind, value: from, to: to, timezone: timezone}) do
    %{kind: kind, from: from, to: to, timezone: timezone}
  end

  defp backfill_refresh_option(opts) do
    case Keyword.get(opts, :refresh) do
      refresh when refresh in [:auto, :missing] -> nil
      refresh -> refresh
    end
  end

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

  defp default_run_config,
    do: %{
      dependencies: "all",
      refresh: "auto",
      source: nil,
      kind: "",
      value: "",
      to: "",
      timezone: "Etc/UTC"
    }

  defp run_config_from_params(%{"run_config" => params}, current_config) when is_map(params) do
    %{
      dependencies: Map.get(params, "dependencies", "all"),
      refresh: Map.get(params, "refresh", "auto"),
      source: Map.get(params, "source", Map.get(current_config, :source)),
      kind: Map.get(params, "kind", Map.get(current_config, :kind, "")),
      value: Map.get(params, "value", Map.get(current_config, :value, "")),
      to: Map.get(params, "to", Map.get(current_config, :to, "")),
      timezone: Map.get(params, "timezone", Map.get(current_config, :timezone, "Etc/UTC"))
    }
  end

  defp run_config_from_params(_params, current_config), do: current_config || default_run_config()

  defp apply_asset_range_defaults(%{to: to, refresh: refresh} = run_config, nil)
       when is_binary(to) and to != "" and refresh == "auto" do
    %{run_config | refresh: "missing"}
  end

  defp apply_asset_range_defaults(run_config, _selected_window), do: run_config

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

  defp submit_error_label(:invalid_window_range), do: "Window range is invalid."

  defp submit_error_label(:invalid_backfill_range_bounds), do: "Window range is invalid."

  defp submit_error_label({:invalid_backfill_range_request, _value}),
    do: "Window range is invalid."

  defp submit_error_label(:forbidden), do: "Operator role required to submit runs."

  defp submit_error_label(_reason), do: "Could not submit run."
end
