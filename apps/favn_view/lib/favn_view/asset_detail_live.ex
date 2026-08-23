defmodule FavnView.AssetDetailLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
  alias FavnView.AssetRoute
  alias FavnView.AssetRunConfig
  alias FavnView.CommandAttempt
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.ErrorPage
  alias FavnView.Auth.Scope
  alias FavnView.CoverageCalendar
  alias FavnView.LogsViewModel

  @dialyzer {:no_unused,
             [
               asset_from_detail: 3,
               run_context_path: 2,
               asset_name: 1,
               run_entry: 3,
               window_entry_label: 1,
               cadence_label: 1,
               cadence_word: 1,
               window_field: 2,
               duration_label: 1,
               missing_freshness_detail: 0
             ]}
  @dialyzer {:no_match,
             [
               handle_event: 3,
               submit_asset_run: 4,
               submit_asset_range_run: 4,
               load_selected_run: 2,
               maybe_load_documentation: 1,
               load_asset: 4,
               asset_from_state: 1,
               coverage_error_label: 1,
               load_coverage_window: 2
             ]}

  @impl true
  def mount(%{"asset_id" => asset_id} = params, _session, socket) do
    run_context_id = run_context_param(params)

    asset_state =
      load_asset(
        socket.assigns.current_scope.operator_context,
        asset_id,
        run_context_id,
        socket.assigns.current_scope
      )

    asset = asset_from_state(asset_state)

    socket =
      assign(socket,
        asset_id: asset_id,
        run_context_id: run_context_id,
        asset_state: asset_state,
        asset: asset,
        selected_run_id: nil,
        selected_run: nil,
        documentation: nil,
        run_config_open?: false,
        run_config_advanced_open?: false,
        run_config: AssetRunConfig.default(),
        run_config_valid?: true,
        submitting_window_run?: false,
        run_error: nil,
        coverage_plan: nil,
        coverage_windows: nil,
        coverage_action_error: nil,
        coverage_selection: MapSet.new(),
        planning_coverage?: false,
        submitting_coverage?: false,
        coverage_attempt: nil,
        run_attempt: nil,
        nav_items: AssetCataloguePage.nav_items()
      )

    {:ok, assign_coverage_calendar(socket)}
  end

  # The rail navigates rather than assigning, so `handle_params` runs on every mode
  # change with the process still mounted. Only a different asset or run context
  # costs a reload; the mode and the selected run are resolved every time.
  @impl true
  def handle_params(%{"asset_id" => asset_id} = params, _uri, socket) do
    run_context_id = run_context_param(params)

    socket =
      if socket.assigns.asset_id == asset_id and socket.assigns.run_context_id == run_context_id do
        socket
      else
        asset_state =
          load_asset(
            actor_context(socket),
            asset_id,
            run_context_id,
            socket.assigns.current_scope
          )

        assign(socket,
          asset_id: asset_id,
          run_context_id: run_context_id,
          asset_state: asset_state,
          asset: asset_from_state(asset_state),
          run_config_open?: false,
          run_config_advanced_open?: false,
          run_config: AssetRunConfig.default(),
          run_config_valid?: true,
          submitting_window_run?: false,
          run_error: nil,
          coverage_plan: nil,
          coverage_windows: nil,
          coverage_action_error: nil,
          coverage_selection: MapSet.new(),
          planning_coverage?: false,
          submitting_coverage?: false,
          coverage_attempt: nil,
          run_attempt: nil,
          documentation: nil
        )
        |> assign_coverage_calendar()
      end

    {:noreply,
     socket
     |> assign_selected_run(Map.get(params, "run_id"))
     |> maybe_load_documentation()
     |> maybe_load_coverage()}
  end

  # Selecting a period narrows the backfill to it. A plan already under review is
  # discarded, because it was built from a different set than the one now on screen.
  @impl true
  def handle_event("toggle_coverage_window", %{"key" => key}, socket) when is_binary(key) do
    selection = socket.assigns.coverage_selection

    selection =
      if MapSet.member?(selection, key),
        do: MapSet.delete(selection, key),
        else: MapSet.put(selection, key)

    {:noreply,
     socket
     |> assign(coverage_selection: selection, coverage_plan: nil, coverage_action_error: nil)
     |> assign_coverage_calendar()}
  end

  def handle_event("clear_coverage_selection", _params, socket) do
    {:noreply,
     socket
     |> assign(coverage_selection: MapSet.new(), coverage_plan: nil, coverage_action_error: nil)
     |> assign_coverage_calendar()}
  end

  def handle_event("plan_missing_coverage", _params, socket) do
    asset = socket.assigns.asset

    cond do
      !socket.assigns.can_submit_runs? ->
        {:noreply, assign(socket, :coverage_action_error, "Operator role required.")}

      asset && Map.get(asset.compatibility, :blocks_writes?, false) ->
        {:noreply,
         assign(socket, :coverage_action_error, "Target compatibility blocks this backfill.")}

      is_nil(asset) or !asset.can_run_asset? ->
        {:noreply, assign(socket, :coverage_action_error, "Select a valid run context first.")}

      true ->
        socket =
          assign(socket,
            planning_coverage?: true,
            coverage_action_error: nil,
            coverage_plan: nil
          )

        case Orchestrator.plan_missing_coverage_backfill(
               actor_context(socket),
               asset.target_id,
               coverage_plan_options(asset, socket.assigns.coverage_selection)
             ) do
          {:ok, plan} ->
            {:noreply, assign(socket, planning_coverage?: false, coverage_plan: plan)}

          {:error, reason} ->
            {:noreply,
             assign(socket,
               planning_coverage?: false,
               coverage_action_error: coverage_error_label(reason)
             )}
        end
    end
  end

  # The navigator only ever offers dates inside the range coverage has, so a step is a
  # date and the backend clamps it. Nothing here has to know where coverage ends.
  def handle_event("show_coverage_period", %{"at" => at}, socket) when is_binary(at) do
    case Date.from_iso8601(at) do
      {:ok, date} -> {:noreply, load_coverage_window(socket, date)}
      {:error, _reason} -> {:noreply, socket}
    end
  end

  def handle_event("show_coverage_period", _params, socket), do: {:noreply, socket}

  def handle_event("jump_coverage_period", params, socket) do
    case CoverageCalendar.jump_target(coverage_view(socket), params) do
      nil -> {:noreply, socket}
      date -> {:noreply, load_coverage_window(socket, date)}
    end
  end

  def handle_event(
        "submit_missing_coverage",
        _params,
        %{assigns: %{submitting_coverage?: true}} = socket
      ),
      do: {:noreply, socket}

  def handle_event("submit_missing_coverage", params, socket) do
    case socket.assigns.coverage_plan do
      nil ->
        {:noreply, assign(socket, :coverage_action_error, "Plan missing windows first.")}

      plan ->
        attempt =
          CommandAttempt.next(
            socket.assigns.coverage_attempt,
            "coverage_backfill_submit",
            {socket.assigns.asset.target_id, plan},
            params
          )

        socket =
          assign(socket,
            submitting_coverage?: true,
            coverage_action_error: nil,
            coverage_attempt: attempt
          )

        case Orchestrator.submit_missing_coverage_backfill(
               actor_context(socket),
               socket.assigns.asset.target_id,
               plan,
               idempotency_key: attempt.key
             ) do
          {:ok, run_id} ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> put_flash(:info, "Missing-window backfill submitted")
             |> push_navigate(to: ~p"/runs/#{run_id}?back_asset_id=#{socket.assigns.asset_id}")}

          {:error, reason} ->
            {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

            {:noreply,
             assign(socket,
               submitting_coverage?: false,
               coverage_action_error: coverage_error_label(reason),
               coverage_plan: nil,
               coverage_attempt: attempt
             )}
        end
    end
  end

  # The dialog opens on the period the asset is due for, which the backend reports.
  # Nothing on screen picks a period any more, so there is no selection to reconcile.
  def handle_event("open_run_config", _params, socket) do
    %{asset: asset} = socket.assigns

    cond do
      !socket.assigns.can_submit_runs? ->
        {:noreply, assign(socket, :run_error, "Operator role required to submit runs.")}

      is_nil(asset) or !asset.can_run_asset? ->
        {:noreply, assign(socket, :run_error, "This asset cannot be run.")}

      true ->
        run_config = AssetRunConfig.from_asset(asset)
        error = AssetRunConfig.validate(run_config)

        {:noreply,
         assign(socket,
           run_config_open?: true,
           run_config_advanced_open?: false,
           run_config: run_config,
           run_config_valid?: is_nil(error),
           run_error: error
         )}
    end
  end

  def handle_event("close_run_config", _params, socket) do
    {:noreply,
     assign(socket,
       run_config_open?: false,
       run_config_advanced_open?: false
     )}
  end

  def handle_event("change_run_config", params, socket) do
    run_config = AssetRunConfig.from_params(params, socket.assigns.run_config)
    error = AssetRunConfig.validate(run_config)

    {:noreply,
     assign(socket,
       run_config_advanced_open?: true,
       run_config: run_config,
       run_config_valid?: is_nil(error),
       run_error: error
     )}
  end

  def handle_event("submit_run", params, socket) do
    %{asset: asset} = socket.assigns

    run_config = AssetRunConfig.from_params(params, socket.assigns.run_config)

    cond do
      !socket.assigns.can_submit_runs? ->
        {:noreply,
         assign(socket,
           run_config: run_config,
           run_error: "Operator role required to submit runs."
         )}

      is_nil(asset) or !asset.can_run_asset? ->
        {:noreply, assign(socket, :run_error, "This asset cannot be run.")}

      error = AssetRunConfig.validate(run_config) ->
        {:noreply,
         assign(socket,
           run_config: run_config,
           run_config_valid?: false,
           submitting_window_run?: false,
           run_error: error
         )}

      true ->
        submit_asset_run(socket, asset, run_config, params)
    end
  end

  defp submit_asset_run(socket, asset, run_config, params) do
    attempt =
      CommandAttempt.next(
        socket.assigns.run_attempt,
        "asset_run_submit",
        {asset.target_id, run_config},
        params
      )

    socket =
      assign(socket,
        run_config: run_config,
        run_config_valid?: true,
        submitting_window_run?: true,
        run_error: nil,
        run_attempt: attempt
      )

    case submit_asset_window_run(socket, asset, run_config, attempt.key) do
      {:ok, run_id, :single} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> put_flash(:info, "Run request queued")
         |> push_navigate(to: ~p"/runs/#{run_id}?back_asset_id=#{socket.assigns.asset_id}")}

      {:ok, run_id, :backfill} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> put_flash(:info, "Asset backfill submitted")
         |> push_navigate(to: ~p"/runs/#{run_id}?back_asset_id=#{socket.assigns.asset_id}")}

      {:error, reason} ->
        Logger.error("asset.run submit failed reason=#{inspect(reason)}")
        {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

        {:noreply,
         assign(socket,
           submitting_window_run?: false,
           run_error: submit_error_label(reason),
           run_attempt: attempt
         )}
    end
  end

  # A "To" period turns one run into a backfill over the inclusive range. Blankness is
  # `AssetRunConfig`'s rule, so a whitespace-only "To" is no range here either — deciding
  # it here separately let a configuration pass validation and still take this branch.
  defp submit_asset_window_run(socket, asset, run_config, idempotency_key) do
    if AssetRunConfig.range_requested?(run_config) do
      submit_asset_range_run(socket, asset, run_config, idempotency_key)
    else
      submit_asset_period_run(socket, asset, run_config, idempotency_key)
    end
  end

  defp submit_asset_range_run(socket, asset, run_config, idempotency_key) do
    request = %{
      range: range_request(run_config),
      dependency_mode: run_config.dependencies,
      refresh_mode: run_config.refresh
    }

    case Orchestrator.submit_operator_asset_backfill(
           actor_context(socket),
           asset.manifest_version_id,
           asset.target_id,
           request,
           idempotency_key: idempotency_key
         ) do
      {:ok, run_id} -> {:ok, run_id, :backfill}
      {:error, reason} -> {:error, reason}
    end
  end

  defp submit_asset_period_run(socket, asset, run_config, idempotency_key) do
    request = %{
      run_context_id: asset.selected_run_context && asset.selected_run_context.id,
      selection: timeline_selection(run_config),
      dependency_mode: run_config.dependencies,
      refresh_mode: run_config.refresh
    }

    case Orchestrator.submit_operator_run(
           actor_context(socket),
           asset.manifest_version_id,
           %{type: :asset, id: asset.target_id},
           request,
           idempotency_key: idempotency_key
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
      has_data_windows?={@asset.has_data_windows?}
      can_run_asset?={@asset.can_run_asset?}
      run_contexts={@asset.run_contexts}
      selected_run_context={@asset.selected_run_context}
      run_context_status={@asset.run_context_status}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      active_mode={active_mode(@live_action)}
      asset_id={@asset_id}
      runs={@asset.runs}
      relation={@asset.relation}
      cadence_label={@asset.cadence_label}
      type={@asset.type}
      upstream={@asset.upstream}
      downstream={@asset.downstream}
      selected_run_id={@selected_run_id}
      selected_run={@selected_run}
      documentation={@documentation}
      freshness={@asset.freshness}
      coverage={@asset.coverage}
      coverage_policy={@asset.coverage_policy}
      coverage_calendar={@coverage_calendar}
      coverage_navigation={@coverage_navigation}
      compatibility={@asset.compatibility}
      rebuild_target_id={@asset.target_id}
      manifest_version_id={@asset.manifest_version_id}
      assurance={@asset.assurance}
      coverage_plan={@coverage_plan}
      coverage_action_error={@coverage_action_error}
      planning_coverage?={@planning_coverage?}
      submitting_coverage?={@submitting_coverage?}
      run_config_open?={@run_config_open?}
      run_config_advanced_open?={@run_config_advanced_open?}
      run_config={@run_config}
      run_config_valid?={@run_config_valid?}
      submitting_window_run?={@submitting_window_run?}
      run_error={@run_error}
      can_submit_runs?={@can_submit_runs?}
      flash={@flash}
    />
    <ErrorPage.error_page
      :if={match?({:error, _reason}, @asset_state)}
      title={asset_error_title(@asset_state)}
      subtitle={@asset_id}
      description={asset_error_message(@asset_state)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      back_navigate={~p"/assets"}
      back_label="Back to catalogue"
      data-testid="asset-backend-error-state"
    />
    <ErrorPage.error_page
      :if={match?({:not_found, _id}, @asset_state)}
      title="Asset not found"
      subtitle={@asset_id}
      description="No active catalogue entry matches this asset id."
      tone={:neutral}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      back_navigate={~p"/assets"}
      back_label="Back to catalogue"
      data-testid="asset-not-found-state"
    />
    """
  end

  # A run selection loads only that run. Re-resolving the whole asset detail would
  # rebuild the freshness plan, the coverage page, and three timelines to change one
  # panel.
  defp assign_selected_run(socket, nil),
    do: assign(socket, selected_run_id: nil, selected_run: nil)

  defp assign_selected_run(%{assigns: %{selected_run_id: run_id}} = socket, run_id), do: socket

  defp assign_selected_run(socket, run_id) do
    assign(socket,
      selected_run_id: run_id,
      selected_run: load_selected_run(socket, run_id)
    )
  end

  defp load_selected_run(%{assigns: %{asset: nil}}, _run_id), do: nil

  defp load_selected_run(socket, run_id) do
    target_id = AssetRoute.from_param(socket.assigns.asset_id)

    case Orchestrator.active_asset_run_detail(actor_context(socket), target_id, run_id) do
      {:ok, run} ->
        {:ok, run}

      {:error, :not_found} ->
        {:not_found, run_id}

      {:error, reason} ->
        Logger.error(
          "asset_run_detail.load failed asset_id=#{inspect(socket.assigns.asset_id)} " <>
            "run_id=#{inspect(run_id)} reason=#{inspect(reason)}"
        )

        {:error, :backend_unavailable}
    end
  end

  # `:run` is a selection inside the runs page, not a fifth destination, so the rail
  # stays lit on Runs while a run is open.
  defp active_mode(:runs), do: :runs
  defp active_mode(:run), do: :runs
  defp active_mode(:coverage), do: :coverage
  defp active_mode(:docs), do: :docs
  defp active_mode(:diagnostics), do: :diagnostics
  defp active_mode(_live_action), do: :overview

  # A SQL asset's source is a content-addressed package that has to be fetched and
  # verified, so it is read when the page that shows it opens and not before. An asset
  # the catalogue does not have renders the not-found page, so there is nothing to fetch.
  defp maybe_load_documentation(%{assigns: %{asset: nil}} = socket), do: socket

  defp maybe_load_documentation(%{assigns: %{live_action: :docs, documentation: nil}} = socket) do
    target_id = AssetRoute.from_param(socket.assigns.asset_id)

    result =
      case Orchestrator.active_asset_documentation(actor_context(socket), target_id) do
        {:ok, documentation} ->
          {:ok, documentation}

        {:error, reason} ->
          Logger.error(
            "asset_documentation.load failed asset_id=#{inspect(socket.assigns.asset_id)} " <>
              "reason=#{inspect(reason)}"
          )

          {:error, :backend_unavailable}
      end

    assign(socket, :documentation, result)
  end

  defp maybe_load_documentation(socket), do: socket

  defp load_asset(operator_context, asset_id, run_context_id, timezone) do
    target_id = AssetRoute.from_param(asset_id)
    opts = if run_context_id, do: [run_context_id: run_context_id], else: []

    case Orchestrator.active_asset_detail(operator_context, target_id, opts) do
      {:ok, detail} ->
        {:ok, asset_from_detail(detail, asset_id, timezone)}

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

  defp asset_from_detail(detail, asset_id, timezone) do
    run_contexts =
      detail
      |> Map.get(:run_contexts, [])
      |> Enum.map(&Map.put(&1, :href, run_context_path(asset_id, &1.id)))

    compatibility =
      Map.get(detail, :compatibility, %{
        status: :operator_decision,
        reason_code: "compatibility_unavailable",
        diff: %{},
        blocks_writes?: true
      })

    headline = headline_status(detail)

    %{
      manifest_version_id: detail.manifest_version_id,
      target_id: detail.target_id,
      canonical_asset_ref: detail.canonical_asset_ref,
      can_run_asset?: detail.can_run_asset? and !Map.get(compatibility, :blocks_writes?, true),
      run_contexts: run_contexts,
      selected_run_context: Map.get(detail, :selected_run_context),
      run_context_status: Map.get(detail, :run_context_status, :unavailable),
      has_data_windows?: detail.has_data_windows?,
      default_run_config: Map.get(detail, :default_run_config),
      title: detail.name || asset_name(detail),
      status: headline.label,
      status_tone: headline.tone,
      freshness: Map.get(detail, :freshness, missing_freshness_detail()),
      coverage: Map.get(detail, :coverage),
      coverage_policy: Map.get(detail, :coverage_policy),
      compatibility: compatibility,
      assurance: Map.get(detail, :assurance),
      runs: Enum.map(Map.get(detail, :runs, []), &run_entry(&1, asset_id, timezone)),
      relation: Map.get(detail, :relation),
      type: Map.get(detail, :type),
      cadence_label: cadence_label(Map.get(detail, :window)),
      description: Map.get(detail, :description),
      metadata: Map.get(detail, :metadata) || %{},
      upstream: Map.get(detail, :upstream, []),
      downstream: Map.get(detail, :downstream, [])
    }
  end

  defp run_entry(run, asset_id, timezone) do
    started_at = Map.get(run, :started_at)

    %{
      id: run.id,
      patch: ~p"/assets/#{asset_id}/runs/#{run.id}",
      status: run.status,
      status_tone: LogsViewModel.status_tone(run.status),
      status_label: LogsViewModel.status_label(run.status),
      trigger_label: LogsViewModel.trigger_label(Map.get(run, :submit_kind)),
      started_at: started_at,
      day_label: started_at && FavnView.Time.format(started_at, "%b %-d", timezone),
      time_label: started_at && FavnView.Time.format(started_at, "%H:%M", timezone),
      duration_label: duration_label(Map.get(run, :duration_ms)),
      window_label: run |> Map.get(:window) |> window_entry_label()
    }
  end

  defp window_entry_label(%{label: label}) when is_binary(label), do: label
  defp window_entry_label(_window), do: nil

  # The backend's cadence label reads "Monthly run anchors Europe/Oslo", which names
  # Favn's scheduling vocabulary rather than answering how often the asset runs.
  defp cadence_label(nil), do: "Whenever it is asked to"

  defp cadence_label(window) do
    case window_field(window, :kind) do
      nil ->
        "Whenever it is asked to"

      kind ->
        case window_field(window, :timezone) do
          nil -> cadence_word(kind)
          timezone -> "#{cadence_word(kind)} · #{timezone}"
        end
    end
  end

  defp cadence_word(kind) when kind in [:hour, "hour"], do: "Hourly"
  defp cadence_word(kind) when kind in [:day, "day"], do: "Daily"
  defp cadence_word(kind) when kind in [:month, "month"], do: "Monthly"
  defp cadence_word(kind) when kind in [:year, "year"], do: "Yearly"
  defp cadence_word(kind), do: kind |> to_string() |> String.capitalize()

  defp window_field(window, key) when is_map(window),
    do: Map.get(window, key) || Map.get(window, Atom.to_string(key))

  defp window_field(_window, _key), do: nil

  defp duration_label(nil), do: nil
  defp duration_label(ms) when ms < 1_000, do: "#{ms}ms"

  defp duration_label(ms) when ms < 60_000 do
    seconds = Float.round(ms / 1_000, 1)
    "#{:erlang.float_to_binary(seconds, decimals: 1)}s"
  end

  defp duration_label(ms) do
    total = div(ms, 1_000)
    "#{div(total, 60)}m #{rem(total, 60)}s"
  end

  defp run_context_param(%{"run_context" => value}) when is_binary(value) and value != "",
    do: value

  defp run_context_param(_params), do: nil

  defp run_context_path(asset_id, run_context_id) do
    ~p"/assets/#{asset_id}?#{[run_context: run_context_id]}"
  end

  defp asset_name(detail) do
    detail
    |> Map.get(:asset_ref, detail[:label] || detail[:target_id])
    |> to_string()
    |> String.split(":")
    |> List.last()
  end

  @doc """
  The worst thing the asset detail page knows, as a label and a tone.

  The orchestrator's asset `:status` reports the latest *run*: `:healthy` means
  the last run did not fail. Coverage and compatibility are separate facts, and
  the page shows all three — so a badge that echoed `:status` alone announced
  "Healthy" directly above "Coverage incomplete, 6 windows missing". A headline
  states the worst thing the page knows, because that is what a headline is for.

      iex> FavnView.AssetDetailLive.headline_status(%{status: :failed})
      %{label: "Last run failed", tone: :error}

      iex> FavnView.AssetDetailLive.headline_status(%{
      ...>   status: :healthy,
      ...>   coverage: %{status: :incomplete}
      ...> })
      %{label: "Coverage incomplete", tone: :warning}

  Stale data counts too. Freshness was missing from this list, so an asset whose
  data had gone out of date was announced as "Healthy" directly above the panel
  saying it was stale:

      iex> FavnView.AssetDetailLive.headline_status(%{
      ...>   status: :healthy,
      ...>   coverage: %{status: :complete},
      ...>   freshness: %{state: :stale}
      ...> })
      %{label: "Out of date", tone: :warning}

      iex> FavnView.AssetDetailLive.headline_status(%{
      ...>   status: :healthy,
      ...>   coverage: %{status: :complete}
      ...> })
      %{label: "Healthy", tone: :success}

      iex> FavnView.AssetDetailLive.headline_status(%{})
      %{label: "Unknown", tone: :neutral}
  """
  @spec headline_status(map()) :: %{label: String.t(), tone: atom()}
  def headline_status(detail) do
    cond do
      Map.get(detail, :status) == :failed ->
        %{label: "Last run failed", tone: :error}

      # Nothing can run at all, which outranks any amount of missing data.
      blocks_writes?(detail) ->
        %{label: "Runs blocked", tone: :error}

      Map.get(detail, :status) == :running ->
        %{label: "Running", tone: :warning}

      freshness_state(detail) == :stale ->
        %{label: "Out of date", tone: :warning}

      coverage_status(detail) == :incomplete ->
        %{label: "Coverage incomplete", tone: :warning}

      Map.get(detail, :status) == :healthy and coverage_status(detail) == :complete ->
        %{label: "Healthy", tone: :success}

      Map.get(detail, :status) == :healthy ->
        %{label: "Last run ok", tone: :success}

      true ->
        %{label: "Unknown", tone: :neutral}
    end
  end

  @doc """
  Coverage status for an asset detail, `:unknown` when the backend did not report one.

      iex> FavnView.AssetDetailLive.coverage_status(%{coverage: %{status: :complete}})
      :complete

      iex> FavnView.AssetDetailLive.coverage_status(%{})
      :unknown
  """
  @spec coverage_status(map()) :: atom()
  def coverage_status(detail),
    do: get_in(detail, [:coverage, Access.key(:status)]) || :unknown

  @doc """
  Whether the active manifest's compatibility verdict forbids writing this asset.

  Absent compatibility is not a block: the caller substitutes its own conservative
  default before deciding whether a run may be submitted.

      iex> FavnView.AssetDetailLive.blocks_writes?(%{compatibility: %{blocks_writes?: true}})
      true

      iex> FavnView.AssetDetailLive.blocks_writes?(%{})
      false
  """
  @spec blocks_writes?(map()) :: boolean()
  def blocks_writes?(detail),
    do: get_in(detail, [:compatibility, Access.key(:blocks_writes?)]) == true

  @doc """
  The freshness verdict, or `:unknown` when the backend has none.

      iex> FavnView.AssetDetailLive.freshness_state(%{freshness: %{state: :stale}})
      :stale

      iex> FavnView.AssetDetailLive.freshness_state(%{})
      :unknown
  """
  @spec freshness_state(map()) :: atom()
  def freshness_state(detail),
    do: get_in(detail, [:freshness, Access.key(:state)]) || :unknown

  defp coverage_error_label(:coverage_selection_stale),
    do: "Coverage changed. Refresh the plan and review it again."

  defp coverage_error_label(:coverage_cursor_stale),
    do: "Coverage changed. Return to the first gap page and try again."

  defp coverage_error_label(:coverage_complete), do: "Coverage is already complete."
  defp coverage_error_label(:coverage_page_complete), do: "This page has no missing windows."
  defp coverage_error_label({:coverage_unknown, _reason}), do: "Coverage is unavailable."
  defp coverage_error_label(_reason), do: "Could not prepare the missing-window backfill."

  # Coverage is read when its own page opens, like the documentation page, so the other
  # four sub-pages do not each pay for a window-keys query they never render. The
  # newest unit opens first, because that is where a gap that matters usually is.
  defp maybe_load_coverage(%{assigns: %{live_action: :coverage, coverage_windows: nil}} = socket),
    do: load_coverage_window(socket, nil)

  defp maybe_load_coverage(socket), do: socket

  defp load_coverage_window(%{assigns: %{asset: nil}} = socket, _date), do: socket

  defp load_coverage_window(socket, date) do
    asset = socket.assigns.asset

    case Orchestrator.active_asset_coverage_windows(
           actor_context(socket),
           asset.target_id,
           coverage_window_options(socket, date)
         ) do
      {:ok, states} ->
        # The selection is dropped with the screen it was made on. Carrying it would
        # mean a button reading "Backfill 4 selected days" beside a calendar showing
        # none of them.
        socket
        |> assign(
          coverage_windows: states,
          asset: Map.put(asset, :coverage, states.summary),
          coverage_selection: MapSet.new(),
          coverage_plan: nil,
          coverage_action_error: nil
        )
        |> assign_coverage_calendar()

      {:error, reason} ->
        Logger.error(
          "asset_coverage.load failed asset_id=#{inspect(socket.assigns.asset_id)} " <>
            "reason=#{inspect(reason)}"
        )

        assign(socket, :coverage_action_error, coverage_error_label(reason))
    end
  end

  # A load with no date opens on whichever unit the calendar says it should, and every
  # later one names a date the navigator offered. Both work from the summary's own
  # bounds, so the first open needs no exploratory query to learn the asset's grain.
  defp coverage_window_options(socket, date) do
    basis = coverage_basis(socket)
    target = date || CoverageCalendar.opening_date(basis)
    {from, until} = CoverageCalendar.unit_bounds(basis.kind, target)

    [evaluated_at: socket.assigns.asset.coverage.evaluated_at]
    |> then(&if from, do: Keyword.put(&1, :from, from), else: &1)
    |> then(&if until, do: Keyword.put(&1, :until, until), else: &1)
  end

  # The grain and the range, from whichever source already knows them. A loaded screen
  # reports both; before that the summary's own window anchors carry them, which is what
  # lets the page open on the right month rather than on the start of coverage.
  defp coverage_basis(%{assigns: %{coverage_windows: states}}) when is_map(states),
    do: Map.take(states, [:kind, :timezone, :first_expected_at, :last_expected_at])

  defp coverage_basis(%{assigns: %{asset: %{coverage: coverage}}}) when is_map(coverage) do
    first = Map.get(coverage, :first_window)
    last = Map.get(coverage, :last_expected_window) || first
    anchor = last || first

    %{
      kind: anchor && Map.get(anchor, :kind),
      timezone: anchor && Map.get(anchor, :timezone),
      first_expected_at: first && Map.get(first, :start_at),
      last_expected_at: last && Map.get(last, :start_at)
    }
  end

  defp coverage_basis(_socket),
    do: %{kind: nil, timezone: nil, first_expected_at: nil, last_expected_at: nil}

  defp assign_coverage_calendar(%{assigns: %{coverage_windows: nil}} = socket) do
    assign(socket,
      coverage_calendar: CoverageCalendar.build(%{}),
      coverage_navigation: CoverageCalendar.navigation(%{})
    )
  end

  defp assign_coverage_calendar(%{assigns: %{coverage_windows: states}} = socket) do
    calendar =
      CoverageCalendar.build(%{
        kind: states.kind,
        timezone: states.timezone,
        windows: states.windows,
        selected: socket.assigns.coverage_selection
      })

    assign(socket,
      coverage_calendar: calendar,
      coverage_navigation: CoverageCalendar.navigation(coverage_view(socket))
    )
  end

  # What the navigator reasons about: the unit on screen and the range it may move in.
  defp coverage_view(%{assigns: %{coverage_windows: nil}}),
    do: %{kind: nil, timezone: nil, at: nil, first_expected_at: nil, last_expected_at: nil}

  defp coverage_view(%{assigns: %{coverage_windows: states}}) do
    %{
      kind: states.kind,
      timezone: states.timezone,
      at: states.windows |> List.first() |> then(&(&1 && &1.start_at)),
      first_expected_at: states.first_expected_at,
      last_expected_at: states.last_expected_at
    }
  end

  # No selection means every missing period, which is what the button offers when
  # nothing is picked. A selection plans exactly those periods and nothing else.
  defp coverage_plan_options(asset, selection) do
    if MapSet.size(selection) == 0 do
      [evaluated_at: asset.coverage.evaluated_at]
    else
      [evaluated_at: asset.coverage.evaluated_at, window_keys: MapSet.to_list(selection)]
    end
  end

  # An asset with no period to run submits no selection, and the backend plans it for
  # whatever period its own policy says is due.
  defp timeline_selection(%{source: source, kind: kind, value: value, timezone: timezone})
       when is_binary(source) and is_binary(kind) and kind != "" and
              is_binary(value) and value != "" do
    %{
      source: source,
      kind: kind,
      value: value,
      timezone: timezone,
      run_id: nil
    }
  end

  defp timeline_selection(_run_config), do: nil

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

  defp asset_error_title({:error, :active_manifest_not_set}), do: "Active manifest not set"
  defp asset_error_title({:error, _reason}), do: "Unable to load asset"

  defp asset_error_message({:error, :active_manifest_not_set}) do
    "Set an active manifest before opening asset details."
  end

  defp asset_error_message({:error, _reason}), do: "Backend unavailable. Try again later."

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

  defp submit_error_label(:runtime_starting),
    do: "Control plane is still starting. Try again shortly."

  defp submit_error_label(:runtime_draining),
    do: "Control plane is draining and cannot start new runs."

  defp submit_error_label(_reason), do: "Could not submit run."
end
