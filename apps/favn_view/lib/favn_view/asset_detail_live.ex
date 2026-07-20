defmodule FavnView.AssetDetailLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Auth.Scope

  @valid_modes ~w(timeline runs lineage docs code details)
  @dependency_choices ~w(all none)
  @refresh_choices ~w(auto missing force_selected force_selected_upstream force_all)
  @source_choices ~w(refresh_timeline data_coverage_timeline)
  @window_kind_choices ~w(hour day month year)
  @timezone_pattern ~r/\A[A-Za-z0-9_+\-\/]{1,64}\z/

  @impl true
  def mount(%{"asset_id" => asset_id}, _session, socket) do
    asset_state = load_asset(socket.assigns.current_scope.operator_context, asset_id)
    asset = asset_from_state(asset_state)

    socket =
      assign(socket,
        asset_id: asset_id,
        asset_state: asset_state,
        asset: asset,
        active_mode: :timeline,
        active_timeline: :refresh,
        selected_window: nil,
        run_config_open?: false,
        run_config: default_run_config(),
        run_config_valid?: true,
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

  def handle_event("select_window", _params, %{assigns: %{active_timeline: :freshness}} = socket),
    do: {:noreply, socket}

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
           run_config_valid?: true,
           selected_window_error: nil,
           submitted_run_id: nil
         )}

      selected_window ->
        {:noreply,
         assign(socket,
           selected_window: selected_window,
           run_config_open?: false,
           run_config: default_run_config(),
           run_config_valid?: true,
           selected_window_error: nil,
           submitted_run_id: nil
         )}

      true ->
        {:noreply, socket}
    end
  end

  def handle_event("select_window", _params, socket), do: {:noreply, socket}

  def handle_event("set_timeline", %{"timeline" => timeline}, socket)
      when timeline in ["refresh", "freshness", "data_coverage"] do
    {:noreply,
     assign(socket,
       active_timeline: timeline_atom(timeline),
       selected_window: nil,
       run_config_open?: false,
       run_config: default_run_config(),
       run_config_valid?: true,
       selected_window_error: nil,
       submitted_run_id: nil
     )}
  end

  def handle_event("set_timeline", _params, socket), do: {:noreply, socket}

  def handle_event(
        "open_run_config",
        _params,
        %{assigns: %{active_timeline: :freshness}} = socket
      ) do
    {:noreply, assign(socket, :selected_window_error, "Freshness periods are read-only.")}
  end

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
        run_config = context_run_config(asset, socket.assigns.active_timeline, selected_window)
        error = validate_run_config(run_config, selected_window)

        {:noreply,
         assign(socket,
           run_config_open?: true,
           run_config: run_config,
           run_config_valid?: is_nil(error),
           selected_window_error: error,
           submitted_run_id: nil
         )}
    end
  end

  def handle_event("close_run_config", _params, socket) do
    {:noreply, assign(socket, :run_config_open?, false)}
  end

  def handle_event("change_run_config", params, socket) do
    run_config = run_config_from_params(params, socket.assigns.run_config)
    error = validate_run_config(run_config, socket.assigns.selected_window)

    {:noreply,
     assign(socket,
       run_config: run_config,
       run_config_valid?: is_nil(error),
       selected_window_error: error
     )}
  end

  def handle_event(
        "run_selected_window",
        _params,
        %{assigns: %{active_timeline: :freshness}} = socket
      ) do
    {:noreply,
     assign(socket,
       run_config_open?: false,
       submitting_window_run?: false,
       selected_window_error: "Freshness periods are read-only."
     )}
  end

  def handle_event("run_selected_window", params, socket) do
    %{asset: asset, selected_window: selected_window} = socket.assigns

    run_config = run_config_from_params(params, socket.assigns.run_config)

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

      error = validate_run_config(run_config, selected_window) ->
        {:noreply,
         assign(socket,
           run_config: run_config,
           run_config_valid?: false,
           submitting_window_run?: false,
           selected_window_error: error
         )}

      true ->
        submit_asset_run(socket, asset, selected_window, run_config)
    end
  end

  defp submit_asset_run(socket, asset, selected_window, run_config) do
    socket =
      assign(socket,
        run_config: run_config,
        run_config_valid?: true,
        submitting_window_run?: true,
        selected_window_error: nil,
        submitted_run_id: nil
      )

    case submit_asset_window_run(socket, asset, selected_window, run_config) do
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
        Logger.error("asset.run submit failed reason=#{inspect(reason)}")

        {:noreply,
         assign(socket,
           submitting_window_run?: false,
           selected_window_error: submit_error_label(reason)
         )}
    end
  end

  defp submit_asset_window_run(socket, asset, nil, %{to: to} = run_config)
       when is_binary(to) and to != "" do
    request = %{
      range: range_request(run_config),
      dependency_mode: run_config.dependencies,
      refresh_mode: run_config.refresh
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

  defp submit_asset_window_run(socket, asset, selected_window, run_config) do
    request = %{
      selection: timeline_selection(selected_window, run_config),
      dependency_mode: run_config.dependencies,
      refresh_mode: run_config.refresh
    }

    case FavnOrchestrator.submit_operator_run(
           actor_context(socket),
           asset.manifest_version_id,
           %{type: :asset, id: asset.target_id},
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
      freshness_timeline_label={@asset.freshness_timeline_label}
      freshness_cadence_label={@asset.freshness_cadence_label}
      data_coverage_timeline_label={@asset.data_coverage_timeline_label}
      window_range={@asset.window_range}
      refresh_window_range={@asset.refresh_window_range}
      freshness_window_range={@asset.freshness_window_range}
      data_coverage_window_range={@asset.data_coverage_window_range}
      active_timeline={@active_timeline}
      has_freshness_timeline?={@asset.has_freshness_timeline?}
      has_data_windows?={@asset.has_data_windows?}
      can_run_asset?={@asset.can_run_asset?}
      nav_items={@nav_items}
      refresh_timeline={@asset.refresh_timeline}
      freshness_timeline={@asset.freshness_timeline}
      data_coverage_timeline={@asset.data_coverage_timeline}
      active_mode={@active_mode}
      freshness={@asset.freshness}
      assurance={@asset.assurance}
      selected_window={@selected_window}
      run_config_open?={@run_config_open?}
      run_config={@run_config}
      run_config_valid?={@run_config_valid?}
      submitting_window_run?={@submitting_window_run?}
      selected_window_error={@selected_window_error}
      submitted_run_id={@submitted_run_id}
      can_submit_runs?={@can_submit_runs?}
    />

    <AppShell.app_shell
      :if={match?({:error, _reason}, @asset_state)}
      title={asset_error_title(@asset_state)}
      subtitle={@asset_id}
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-4xl">
        <GlassPanel.glass_panel class="p-8 text-center" data-testid="asset-backend-error-state">
          <h2 class="text-xl font-medium">{asset_error_title(@asset_state)}</h2>
          <p class="mt-2 text-base-content/60">
            {asset_error_message(@asset_state)}
          </p>
          <.link navigate={~p"/assets"} class="btn btn-primary btn-soft mt-6">
            Back to catalogue
          </.link>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>

    <AppShell.app_shell
      :if={match?({:not_found, _id}, @asset_state)}
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

  defp load_asset(operator_context, asset_id) do
    target_id = AssetRoute.from_param(asset_id)

    case FavnOrchestrator.active_asset_detail(operator_context, target_id, []) do
      {:ok, detail} ->
        {:ok, asset_from_detail(detail)}

      {:error, :not_found} ->
        {:not_found, asset_id}

      {:error, :active_manifest_not_set} ->
        {:error, :active_manifest_not_set}

      {:error, reason} ->
        Logger.error(
          "asset_detail.load failed asset_id=#{inspect(asset_id)} reason=#{inspect(reason)}"
        )

        {:error, :backend_unavailable}
    end
  end

  defp asset_from_state({:ok, asset}), do: asset
  defp asset_from_state(_state), do: nil

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope
    scope.operator_context
  end

  defp asset_from_detail(detail) do
    refresh_timeline = Enum.map(detail.refresh_timeline, &timeline_window/1)

    freshness_timeline =
      detail[:freshness_timeline] && Enum.map(detail.freshness_timeline, &timeline_window/1)

    data_coverage_timeline =
      detail.data_coverage_timeline && Enum.map(detail.data_coverage_timeline, &timeline_window/1)

    timeline = refresh_timeline

    %{
      manifest_version_id: detail.manifest_version_id,
      target_id: detail.target_id,
      canonical_asset_ref: detail.canonical_asset_ref,
      can_run_asset?: detail.can_run_asset?,
      has_data_windows?: detail.has_data_windows?,
      has_freshness_timeline?: Map.get(detail, :has_freshness_timeline?, false),
      title: detail.name || asset_name(detail),
      status: status_label(Map.get(detail, :status)),
      status_tone: status_tone(Map.get(detail, :status)),
      freshness: Map.get(detail, :freshness, missing_freshness_detail()),
      assurance: Map.get(detail, :assurance),
      window_kind_label: window_kind_label(Map.get(detail, :window)),
      refresh_timeline_label: Map.get(detail, :refresh_timeline_label, "Refresh periods"),
      refresh_cadence_label: Map.get(detail, :refresh_cadence_label, "Refresh cadence"),
      freshness_timeline_label: Map.get(detail, :freshness_timeline_label, "Freshness periods"),
      freshness_cadence_label: Map.get(detail, :freshness_cadence_label, "Freshness cadence"),
      data_coverage_timeline_label:
        Map.get(detail, :data_coverage_timeline_label, "Data windows"),
      window_range: window_range(timeline),
      refresh_window_range: window_range(refresh_timeline),
      freshness_window_range: window_range(freshness_timeline || []),
      data_coverage_window_range: window_range(data_coverage_timeline || []),
      refresh_timeline: refresh_timeline,
      freshness_timeline: freshness_timeline,
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
  defp timeline_atom("freshness"), do: :freshness
  defp timeline_atom("data_coverage"), do: :data_coverage

  defp asset_timeline(nil, _active_timeline), do: []
  defp asset_timeline(asset, :refresh), do: Map.get(asset, :refresh_timeline, [])
  defp asset_timeline(asset, :freshness), do: Map.get(asset, :freshness_timeline, []) || []

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

  defp range_request(%{kind: kind, value: from, to: to, timezone: timezone}) do
    %{kind: kind, from: from, to: to, timezone: timezone}
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

  defp validate_run_config(config, selected_window) do
    cond do
      config.dependencies not in @dependency_choices ->
        "Dependency choice is invalid."

      config.refresh not in @refresh_choices ->
        "Refresh choice is invalid."

      config.dependencies == "none" and config.refresh == "force_selected_upstream" ->
        "force_selected_upstream requires dependencies=all."

      is_nil(selected_window) and window_context_requested?(config) and
          config.source not in @source_choices ->
        "Window source is invalid."

      is_nil(selected_window) and window_context_requested?(config) and
          config.kind not in @window_kind_choices ->
        "Window kind is invalid."

      is_nil(selected_window) and window_context_requested?(config) and blank?(config.value) ->
        "Window range start is required."

      is_nil(selected_window) and range_requested?(config) and blank?(config.to) ->
        "Window range end is required."

      window_context_requested?(config) and not valid_timezone?(config.timezone) ->
        "Timezone is invalid."

      true ->
        nil
    end
  end

  defp window_context_requested?(config),
    do: not blank?(Map.get(config, :value)) or not blank?(Map.get(config, :to))

  defp range_requested?(config), do: not blank?(Map.get(config, :to))

  defp blank?(value), do: not is_binary(value) or String.trim(value) == ""
  defp valid_timezone?(value) when is_binary(value), do: String.match?(value, @timezone_pattern)
  defp valid_timezone?(_value), do: false

  defp asset_error_title({:error, :active_manifest_not_set}), do: "Active manifest not set"
  defp asset_error_title({:error, _reason}), do: "Unable to load asset"

  defp asset_error_message({:error, :active_manifest_not_set}) do
    "Set an active manifest before opening asset details."
  end

  defp asset_error_message({:error, _reason}), do: "Backend unavailable. Try again later."

  defp disabled_reason_label(:asset_has_no_window_policy), do: "This asset has no window policy."
  defp disabled_reason_label(:invalid_window), do: "This window cannot be run."
  defp disabled_reason_label(_reason), do: "This window is not runnable."

  defp submit_error_label(:invalid_asset_target), do: "Asset target is no longer available."
  defp submit_error_label({:invalid_window_id, _reason}), do: "Window id is invalid."

  defp submit_error_label({:window_request_without_policy, _kind}),
    do: "This asset has no window policy."

  defp submit_error_label({:refresh_include_upstream_requires_dependencies, :all}),
    do: "Force selected + upstream requires including upstream dependencies."

  defp submit_error_label({:invalid_operator_dependency_mode, _value}),
    do: "Dependency mode is invalid."

  defp submit_error_label({:invalid_operator_refresh_mode, _value}),
    do: "Refresh behavior is invalid."

  defp submit_error_label({:invalid_operator_selection_source, _value}),
    do: "Selected timeline is invalid."

  defp submit_error_label({:invalid_operator_selection, _value}),
    do: "Selected window is invalid."

  defp submit_error_label({:invalid_operator_selection_id, _value}),
    do: "Selected window is invalid."

  defp submit_error_label({:invalid_operator_range, _value}), do: "Window range is invalid."

  defp submit_error_label(:invalid_window_range), do: "Window range is invalid."

  defp submit_error_label(:invalid_backfill_range_bounds), do: "Window range is invalid."

  defp submit_error_label({:invalid_backfill_range_request, _value}),
    do: "Window range is invalid."

  defp submit_error_label(:forbidden), do: "Operator role required to submit runs."

  defp submit_error_label(_reason), do: "Could not submit run."
end
