defmodule FavnView.PipelineDetailLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
  alias FavnView.AssetRoute
  alias FavnView.CommandAttempt
  alias FavnView.Components.ErrorPage
  alias FavnView.Components.PipelineDetailPage
  alias FavnView.Components.PipelinesPage
  alias FavnView.LogsViewModel
  alias FavnView.PipelineRunConfig
  alias FavnView.Auth.Scope

  @dialyzer {:no_unused,
             [
               normalize_window_kind: 1,
               pipeline_from_detail: 2,
               pipeline_name: 1,
               dependencies_label: 1,
               window_label: 1,
               default_run_label: 1,
               status_label: 1,
               last_run_label: 2
             ]}
  @dialyzer {:no_match,
             [
               handle_event: 3,
               load_pipeline: 3,
               pipeline_from_state: 1
             ]}

  @impl true
  def mount(%{"pipeline_id" => pipeline_id}, _session, socket) do
    pipeline_state =
      load_pipeline(
        socket.assigns.current_scope.operator_context,
        pipeline_id,
        socket.assigns.current_scope
      )

    pipeline = pipeline_from_state(pipeline_state)
    defaults = PipelineRunConfig.default(pipeline)

    socket =
      assign(socket,
        pipeline_id: pipeline_id,
        pipeline_state: pipeline_state,
        pipeline: pipeline,
        run_error: nil,
        run_attempt: nil,
        run_dialog_open?: false,
        run_advanced_open?: false,
        run_config: defaults,
        run_config_defaults: defaults,
        run_config_valid?: true,
        nav_items: PipelinesPage.nav_items(:pipelines)
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("open_run_dialog", _params, socket) do
    {:noreply,
     assign(socket,
       run_dialog_open?: true,
       run_advanced_open?: false,
       run_config: socket.assigns.run_config_defaults,
       run_config_valid?: true,
       run_error: nil
     )}
  end

  def handle_event("close_run_dialog", _params, socket) do
    {:noreply, assign(socket, run_dialog_open?: false, run_advanced_open?: false)}
  end

  def handle_event("reset_run_config", _params, socket) do
    {:noreply,
     assign(socket,
       run_config: socket.assigns.run_config_defaults,
       run_config_valid?: true,
       run_error: nil
     )}
  end

  def handle_event("change_run_config", params, socket) do
    config = PipelineRunConfig.from_params(params, socket.assigns.run_config)
    error = PipelineRunConfig.validate(config, windowed?(socket.assigns.pipeline))

    {:noreply,
     assign(socket,
       run_advanced_open?: true,
       run_config: config,
       run_config_valid?: is_nil(error),
       run_error: error
     )}
  end

  def handle_event("submit_pipeline_run", _params, %{assigns: %{pipeline: nil}} = socket) do
    {:noreply, assign(socket, run_error: "Pipeline not found.")}
  end

  def handle_event("submit_pipeline_run", params, socket) do
    pipeline = socket.assigns.pipeline
    config = PipelineRunConfig.from_params(params, socket.assigns.run_config)
    socket = assign(socket, run_config: config)

    case PipelineRunConfig.validate(config, windowed?(pipeline)) do
      nil ->
        submit_run(socket, pipeline, config, params)

      message ->
        {:noreply,
         assign(socket, run_error: message, run_config_valid?: false, run_advanced_open?: true)}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <PipelineDetailPage.pipeline_detail_page
      :if={@pipeline}
      pipeline={@pipeline}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      run_dialog_open?={@run_dialog_open?}
      run_config={@run_config}
      run_config_defaults={@run_config_defaults}
      run_config_valid?={@run_config_valid?}
      run_advanced_open?={@run_advanced_open?}
      run_error={@run_error}
      can_submit_runs?={@can_submit_runs?}
      flash={@flash}
    />
    <ErrorPage.error_page
      :if={match?({:error, _reason}, @pipeline_state)}
      title={pipeline_error_title(@pipeline_state)}
      subtitle={@pipeline_id}
      description={pipeline_error_message(@pipeline_state)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      back_navigate={~p"/pipelines"}
      back_label="Back to pipelines"
      data-testid="pipeline-backend-error-state"
    />
    <ErrorPage.error_page
      :if={match?({:not_found, _id}, @pipeline_state)}
      title="Pipeline not found"
      subtitle={@pipeline_id}
      description="No active manifest pipeline matches this pipeline id."
      tone={:neutral}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      back_navigate={~p"/pipelines"}
      back_label="Back to pipelines"
      data-testid="pipeline-not-found-state"
    />
    """
  end

  # A range is a different command from a run, so it is also a different intent
  # and a different operation name in the browser's command registry. Both are
  # named here rather than at the call site, so the key an operator's retry
  # reuses cannot drift from the one their first attempt minted.
  defp submit_run(socket, pipeline, config, params) do
    operation =
      if PipelineRunConfig.range_requested?(config),
        do: "pipeline_backfill_submit",
        else: "pipeline_run_submit"

    attempt =
      CommandAttempt.next(socket.assigns.run_attempt, operation, {pipeline.id, config}, params)

    socket = assign(socket, :run_attempt, attempt)

    case submit_command(actor_context(socket), pipeline, config, attempt) do
      {:ok, run_id} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> put_flash(:info, submitted_flash(config))
         |> push_navigate(to: ~p"/runs/#{run_id}")}

      {:error, reason} ->
        Logger.error("pipeline.run submit failed reason=#{inspect(reason)}")
        {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

        {:noreply, assign(socket, run_error: submit_error_label(reason), run_attempt: attempt)}
    end
  end

  defp submit_command(context, pipeline, config, attempt) do
    if PipelineRunConfig.range_requested?(config) do
      submit_pipeline_backfill(
        context,
        pipeline.manifest_version_id,
        pipeline.id,
        %{
          range: PipelineRunConfig.range_request(config),
          refresh_mode: config.refresh,
          combine_windows: config.combine_windows
        },
        idempotency_key: attempt.key
      )
    else
      facade(:submit_operator_run_fun, &Orchestrator.submit_operator_run/5).(
        context,
        pipeline.manifest_version_id,
        %{type: :pipeline, id: pipeline.id},
        run_input(config),
        idempotency_key: attempt.key
      )
    end
  end

  # No window is sent when no period was asked for, because the control plane
  # resolves the latest complete period after the availability delay its selected
  # assets declare. A window computed in the browser would ignore that delay.
  defp run_input(config) do
    if PipelineRunConfig.period_requested?(config) do
      %{refresh_mode: config.refresh, window: PipelineRunConfig.window_request(config)}
    else
      %{refresh_mode: config.refresh}
    end
  end

  defp submitted_flash(config) do
    if PipelineRunConfig.range_requested?(config),
      do: "Pipeline backfill submitted",
      else: "Run request queued"
  end

  defp windowed?(%{window: window}) when is_map(window), do: true
  defp windowed?(_pipeline), do: false

  defp load_pipeline(operator_context, pipeline_id, timezone) do
    target_id = AssetRoute.from_param(pipeline_id)

    case Orchestrator.active_pipeline_detail(operator_context, target_id) do
      {:ok, detail} ->
        {:ok, pipeline_from_detail(detail, timezone)}

      {:error, :not_found} ->
        {:not_found, pipeline_id}

      {:error, :active_manifest_not_set} ->
        {:error, :active_manifest_not_set}

      {:error, reason} ->
        Logger.error(
          "pipeline_detail.load failed pipeline_id=#{inspect(pipeline_id)} reason=#{inspect(reason)}"
        )

        {:error, :backend_unavailable}
    end
  end

  defp pipeline_from_state({:ok, pipeline}), do: pipeline
  defp pipeline_from_state(_state), do: nil

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope
    scope.operator_context
  end

  defp facade(key, default), do: Application.get_env(:favn_view, key, default)

  defp pipeline_from_detail(detail, timezone) do
    selected_assets = Map.get(detail, :selected_assets, [])
    status = Map.get(detail, :status, :unknown)
    window = Map.get(detail, :window)

    %{
      id: Map.fetch!(detail, :target_id),
      manifest_version_id: Map.fetch!(detail, :manifest_version_id),
      name: Map.get(detail, :name) || pipeline_name(Map.fetch!(detail, :label)),
      label: Map.fetch!(detail, :label),
      selected_assets: Enum.map(selected_assets, &asset_ref_label/1),
      asset_count: length(selected_assets),
      dependencies: Map.get(detail, :dependencies, :unknown),
      dependencies_label: dependencies_label(Map.get(detail, :dependencies, :unknown)),
      window: window,
      window_label: window_label(window),
      default_run_label: default_run_label(window),
      max_concurrency: Map.get(detail, :max_concurrency),
      execution_pool: Map.get(detail, :execution_pool),
      status: status,
      status_label: status_label(status),
      last_run_label: last_run_label(Map.get(detail, :latest_run_at), timezone),
      runtime_label: LogsViewModel.duration_ms_label(Map.get(detail, :latest_run_duration_ms)),
      runs: Enum.map(Map.get(detail, :runs, []), &run_from_detail(&1, timezone))
    }
  end

  defp run_from_detail(run, timezone) do
    %{
      id: run.id,
      short_id: short_id(run.id),
      status: run_status(Map.get(run, :status)),
      kind_label: kind_label(Map.get(run, :submit_kind)),
      window_label: scope_label(Map.get(run, :scope) || Map.get(run, :window)),
      started_at_label: timestamp_label(Map.get(run, :started_at), timezone),
      duration_label: LogsViewModel.duration_ms_label(Map.get(run, :duration_ms))
    }
  end

  defp pipeline_error_title({:error, :active_manifest_not_set}), do: "Active manifest not set"
  defp pipeline_error_title({:error, _reason}), do: "Unable to load pipeline"

  defp pipeline_error_message({:error, :active_manifest_not_set}) do
    "Set an active manifest before opening pipeline details."
  end

  defp pipeline_error_message({:error, _reason}), do: "Backend unavailable. Try again later."

  defp normalize_window_kind(kind) when kind in [:hour, :day, :month, :year], do: kind
  defp normalize_window_kind(:hourly), do: :hour
  defp normalize_window_kind(:daily), do: :day
  defp normalize_window_kind(:monthly), do: :month
  defp normalize_window_kind(:yearly), do: :year

  defp normalize_window_kind(kind) when kind in ["hour", "day", "month", "year"],
    do: String.to_existing_atom(kind)

  defp normalize_window_kind("hourly"), do: :hour
  defp normalize_window_kind("daily"), do: :day
  defp normalize_window_kind("monthly"), do: :month
  defp normalize_window_kind("yearly"), do: :year

  defp normalize_window_kind(_kind), do: nil

  defp submit_pipeline_backfill(context, manifest_version_id, target_id, input, opts) do
    Application.get_env(
      :favn_view,
      :submit_operator_pipeline_backfill_fun,
      &Orchestrator.submit_operator_pipeline_backfill/5
    ).(context, manifest_version_id, target_id, input, opts)
  end

  defp window_field(window, key),
    do: Map.get(window, key) || Map.get(window, Atom.to_string(key))

  defp pipeline_name(label), do: label |> String.split(".") |> List.last()

  defp asset_ref_label(ref) when is_binary(ref) do
    case String.split(ref, ":", parts: 2) do
      [module, "asset"] -> module |> String.split(".") |> List.last()
      [_module, name] -> name
      [value] -> value |> String.split(".") |> List.last()
    end
  end

  defp asset_ref_label(ref), do: to_string(ref)

  defp dependencies_label(:all), do: "Include deps"
  defp dependencies_label(:none), do: "Selected only"
  defp dependencies_label(_dependencies), do: "Unknown deps"

  defp window_label(window) when is_map(window) do
    kind = window |> window_field(:kind) |> window_kind_word()
    timezone = window_field(window, :timezone)

    case timezone do
      nil -> kind
      timezone -> "#{kind} · #{timezone}"
    end
  end

  defp window_label(_window), do: "Not windowed"

  # Said in words rather than as a date. The control plane resolves the period at
  # submission, after subtracting the availability delay the selected assets
  # declare, and a date resolved here would disagree with it on exactly the
  # pipelines that wait for late-arriving data.
  defp default_run_label(window) when is_map(window) do
    "The last complete #{String.downcase(window |> window_field(:kind) |> window_kind_word())}"
  end

  defp default_run_label(_window), do: "The whole relation"

  defp window_kind_word(kind) do
    case normalize_window_kind(kind) do
      nil -> "Windowed"
      kind -> kind |> Atom.to_string() |> String.capitalize()
    end
  end

  defp scope_label(nil), do: "-"
  defp scope_label(value) when is_binary(value), do: value

  defp scope_label(%{type: :window} = value) do
    labelled_scope_value(value) || window_scope_label(value)
  end

  defp scope_label(%{type: :range} = value) do
    [
      Map.get(value, :kind),
      range_boundary_label(Map.get(value, :range_start_at)),
      range_boundary_label(Map.get(value, :range_end_at)),
      Map.get(value, :timezone)
    ]
    |> Enum.reject(&(&1 in [nil, ""]))
    |> case do
      [] -> inspect(value)
      [kind | rest] -> Enum.join([humanize(kind) | rest], " / ")
    end
  end

  defp scope_label(value) when is_map(value) do
    labelled_scope_value(value) || inspect(value)
  end

  defp scope_label(value), do: inspect(value)

  defp labelled_scope_value(value) do
    [:label, "label", :id, "id", :key, "key"]
    |> Enum.find_value(fn key ->
      case Map.get(value, key) do
        label when is_binary(label) -> label
        label when is_atom(label) -> Atom.to_string(label)
        label when is_integer(label) -> Integer.to_string(label)
        _other -> nil
      end
    end)
  end

  defp window_scope_label(value) do
    kind = Map.get(value, :kind) || Map.get(value, "kind")
    timezone = Map.get(value, :timezone) || Map.get(value, "timezone")

    case {kind, timezone} do
      {nil, nil} -> inspect(value)
      {nil, timezone} -> timezone
      {kind, nil} -> humanize(kind)
      {kind, timezone} -> "#{humanize(kind)} #{timezone}"
    end
  end

  defp range_boundary_label(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp range_boundary_label(value), do: value

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(_status), do: "Unknown"

  defp run_status(status) when status in [:ok, :skipped_fresh, "ok", "skipped_fresh"],
    do: :healthy

  defp run_status(status)
       when status in [:running, :pending, :retrying, "running", "pending", "retrying"],
       do: :running

  defp run_status(status)
       when status in [
              :error,
              :blocked,
              :cancelled,
              :timed_out,
              "error",
              "blocked",
              "cancelled",
              "timed_out"
            ],
       do: :failed

  defp run_status(_status), do: :unknown

  defp kind_label(:backfill_pipeline), do: "Backfill"
  defp kind_label("backfill_pipeline"), do: "Backfill"
  defp kind_label(nil), do: "Pipeline"
  defp kind_label(kind), do: humanize(kind)

  defp last_run_label(%DateTime{} = datetime, timezone) do
    seconds = DateTime.diff(DateTime.utc_now(), datetime, :second)

    cond do
      seconds < 60 -> "just now"
      seconds < 3_600 -> "#{div(seconds, 60)}m ago"
      seconds < 86_400 -> "#{div(seconds, 3_600)}h ago"
      true -> FavnView.Time.format(datetime, "%b %-d %H:%M", timezone)
    end
  end

  defp last_run_label(_value, _timezone), do: "No runs yet"

  defp timestamp_label(%DateTime{} = datetime, timezone),
    do: FavnView.Time.format(datetime, "%b %-d %H:%M", timezone)

  defp timestamp_label(_value, _timezone), do: "-"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp submit_error_label({:invalid_backfill_range_request, _value}),
    do: "Invalid backfill range."

  defp submit_error_label({:invalid_operator_range, _value}),
    do: "Invalid backfill range."

  defp submit_error_label({:invalid_operator_refresh_mode, _value}),
    do: "Refresh behavior is invalid."

  defp submit_error_label({:invalid_operator_window, _value}),
    do: "That is not a period this pipeline can run. Check the format in the placeholder."

  defp submit_error_label({:window_kind_mismatch, expected, _actual}),
    do: "This pipeline runs #{expected} periods."

  # The browser holds one command key per operation and resource until the
  # command settles, so an earlier submission whose outcome was never known is
  # replayed here rather than repeated. Saying so is the difference between an
  # operator retrying and an operator giving up.
  defp submit_error_label(:idempotency_conflict),
    do:
      "An earlier submission for this pipeline is still unresolved. Open its run before submitting a different one."

  defp submit_error_label({:missing_window_request, kind}),
    do: "This #{kind} pipeline requires an explicit window request."

  defp submit_error_label(:forbidden), do: "Operator role required to submit runs."
  defp submit_error_label(:not_found), do: "Pipeline not found."

  defp submit_error_label(:runtime_starting),
    do: "Control plane is still starting. Try again shortly."

  defp submit_error_label(:runtime_draining),
    do: "Control plane is draining and cannot start new runs."

  defp submit_error_label(_reason), do: "Submit failed."

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
