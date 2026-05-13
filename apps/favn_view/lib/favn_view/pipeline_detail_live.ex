defmodule FavnView.PipelineDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias Favn.Backfill.RangeRequest
  alias Favn.Window.Request, as: WindowRequest
  alias FavnView.AssetRoute
  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.PipelineDetailPage
  alias FavnView.Components.PipelinesPage
  alias FavnView.LogsViewModel

  @valid_modes ~w(runs assets more)

  @impl true
  def mount(%{"pipeline_id" => pipeline_id}, _session, socket) do
    pipeline = load_pipeline(pipeline_id)

    socket =
      assign(socket,
        pipeline_id: pipeline_id,
        pipeline: pipeline,
        active_mode: :runs,
        run_error: nil,
        backfill_error: nil,
        backfill_config: %{from: "2024-01", to: "2026-12", kind: "month"},
        nav_items: PipelinesPage.nav_items(:pipelines)
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  def handle_event("run_pipeline", _params, %{assigns: %{pipeline: nil}} = socket) do
    {:noreply, assign(socket, :run_error, "Pipeline not found.")}
  end

  def handle_event("run_pipeline", _params, socket) do
    pipeline = socket.assigns.pipeline

    case FavnOrchestrator.submit_pipeline_run_for_manifest(
           pipeline.manifest_version_id,
           pipeline.id,
           pipeline_run_opts(pipeline)
         ) do
      {:ok, run_id} ->
        {:noreply,
         socket
         |> put_flash(:info, "Pipeline run submitted")
         |> push_navigate(to: ~p"/runs/#{run_id}")}

      {:error, reason} ->
        {:noreply, assign(socket, :run_error, submit_error_label(reason))}
    end
  end

  def handle_event(
        "submit_backfill",
        %{"backfill" => params},
        %{assigns: %{pipeline: nil}} = socket
      ) do
    {:noreply,
     assign(socket,
       backfill_config: backfill_config(params),
       backfill_error: "Pipeline not found."
     )}
  end

  def handle_event("submit_backfill", %{"backfill" => params}, socket) do
    config = backfill_config(params)
    pipeline = socket.assigns.pipeline

    with {:ok, kind} <- backfill_kind(config.kind),
         {:ok, range_request} <-
           RangeRequest.explicit(
             from: config.from,
             to: config.to,
             kind: kind,
             timezone: "Etc/UTC"
           ),
         {:ok, run_id} <-
           FavnOrchestrator.submit_pipeline_backfill_for_manifest(
             pipeline.manifest_version_id,
             pipeline.id,
             range_request: range_request
           ) do
      {:noreply,
       socket
       |> put_flash(:info, "Pipeline backfill submitted")
       |> push_navigate(to: ~p"/runs/#{run_id}")}
    else
      {:error, reason} ->
        {:noreply,
         assign(socket,
           backfill_config: config,
           backfill_error: submit_error_label(reason)
         )}
    end
  end

  def handle_event("submit_backfill", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <PipelineDetailPage.pipeline_detail_page
      :if={@pipeline}
      pipeline={@pipeline}
      nav_items={@nav_items}
      active_mode={@active_mode}
      run_error={@run_error}
      backfill_error={@backfill_error}
      backfill_config={@backfill_config}
    />

    <AppShell.app_shell
      :if={!@pipeline}
      title="Pipeline not found"
      subtitle={@pipeline_id}
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-4xl">
        <GlassPanel.glass_panel class="p-8 text-center" data-testid="pipeline-not-found-state">
          <h2 class="text-xl font-medium">Pipeline not found</h2>
          <p class="mt-2 text-base-content/60">
            No active manifest pipeline matches this pipeline id.
          </p>
          <.link navigate={~p"/pipelines"} class="btn btn-primary btn-soft mt-6">
            Back to pipelines
          </.link>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>
    """
  end

  defp load_pipeline(pipeline_id) do
    target_id = AssetRoute.from_param(pipeline_id)

    case FavnOrchestrator.active_pipeline_detail(target_id) do
      {:ok, detail} -> pipeline_from_detail(detail)
      {:error, _reason} -> nil
    end
  end

  defp pipeline_from_detail(detail) do
    selected_assets = Map.get(detail, :selected_assets, [])
    status = Map.get(detail, :status, :unknown)

    %{
      id: Map.fetch!(detail, :target_id),
      manifest_version_id: Map.fetch!(detail, :manifest_version_id),
      name: Map.get(detail, :name) || pipeline_name(Map.fetch!(detail, :label)),
      label: Map.fetch!(detail, :label),
      selected_assets: Enum.map(selected_assets, &asset_ref_label/1),
      asset_count: length(selected_assets),
      dependencies: Map.get(detail, :dependencies, :unknown),
      dependencies_label: dependencies_label(Map.get(detail, :dependencies, :unknown)),
      window: Map.get(detail, :window),
      window_label: window_label(Map.get(detail, :window)),
      status: status,
      status_label: status_label(status),
      last_run_label: last_run_label(Map.get(detail, :latest_run_at)),
      runtime_label: LogsViewModel.duration_ms_label(Map.get(detail, :latest_run_duration_ms)),
      runs: Enum.map(Map.get(detail, :runs, []), &run_from_detail/1)
    }
  end

  defp run_from_detail(run) do
    %{
      id: run.id,
      short_id: short_id(run.id),
      status: run_status(Map.get(run, :status)),
      kind_label: kind_label(Map.get(run, :submit_kind)),
      window_label: window_label_value(Map.get(run, :window)),
      started_at_label: timestamp_label(Map.get(run, :started_at)),
      duration_label: LogsViewModel.duration_ms_label(Map.get(run, :duration_ms))
    }
  end

  defp backfill_config(params) do
    %{
      from: params |> Map.get("from", "") |> String.trim(),
      to: params |> Map.get("to", "") |> String.trim(),
      kind: params |> Map.get("kind", "month") |> String.trim()
    }
  end

  defp pipeline_run_opts(%{window: nil}), do: []

  defp pipeline_run_opts(%{window: window}) do
    kind = Map.get(window, :kind) || Map.get(window, "kind")
    timezone = Map.get(window, :timezone) || Map.get(window, "timezone") || "Etc/UTC"

    case current_window_request(kind, timezone) do
      {:ok, request} -> [window_request: request]
      {:error, _reason} -> []
    end
  end

  defp current_window_request(kind, timezone) do
    kind = normalize_window_kind(kind)

    with kind when kind in [:hour, :day, :month, :year] <- kind,
         {:ok, value} <- current_window_value(kind, timezone) do
      WindowRequest.parse("#{kind}:#{value}", timezone: timezone)
    else
      _other -> {:error, :invalid_window_kind}
    end
  end

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

  defp current_window_value(kind, timezone) do
    with {:ok, datetime} <- DateTime.now(timezone) do
      value =
        case kind do
          :hour -> Calendar.strftime(datetime, "%Y-%m-%dT%H")
          :day -> Calendar.strftime(datetime, "%Y-%m-%d")
          :month -> Calendar.strftime(datetime, "%Y-%m")
          :year -> Calendar.strftime(datetime, "%Y")
        end

      {:ok, value}
    end
  end

  defp backfill_kind(kind) when kind in ~w(hour day month year),
    do: {:ok, String.to_existing_atom(kind)}

  defp backfill_kind(kind), do: {:error, {:invalid_backfill_kind, kind}}

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

  defp window_label(nil), do: "No window"
  defp window_label(%{kind: kind, timezone: timezone}), do: window_label(kind, timezone)
  defp window_label(%{"kind" => kind, "timezone" => timezone}), do: window_label(kind, timezone)
  defp window_label(_window), do: "Windowed"

  defp window_label(kind, nil), do: humanize(kind)
  defp window_label(kind, timezone), do: "#{humanize(kind)} #{timezone}"

  defp window_label_value(nil), do: "-"
  defp window_label_value(value) when is_binary(value), do: value

  defp window_label_value(value) when is_map(value) do
    Map.get(value, :label) || Map.get(value, "label") || Map.get(value, :id) ||
      Map.get(value, "id") || Map.get(value, :key) || Map.get(value, "key") || inspect(value)
  end

  defp window_label_value(value), do: inspect(value)

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

  defp last_run_label(%DateTime{} = datetime) do
    seconds = DateTime.diff(DateTime.utc_now(), datetime, :second)

    cond do
      seconds < 60 -> "just now"
      seconds < 3_600 -> "#{div(seconds, 60)}m ago"
      seconds < 86_400 -> "#{div(seconds, 3_600)}h ago"
      true -> Calendar.strftime(datetime, "%b %-d %H:%M")
    end
  end

  defp last_run_label(_value), do: "No runs yet"

  defp timestamp_label(%DateTime{} = datetime), do: Calendar.strftime(datetime, "%b %-d %H:%M")
  defp timestamp_label(_value), do: "-"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp submit_error_label({:invalid_backfill_kind, kind}), do: "Invalid backfill kind: #{kind}."

  defp submit_error_label({:invalid_backfill_range_request, _value}),
    do: "Invalid backfill range."

  defp submit_error_label(:not_found), do: "Pipeline not found."
  defp submit_error_label(reason), do: "Submit failed: #{inspect(reason)}"

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
