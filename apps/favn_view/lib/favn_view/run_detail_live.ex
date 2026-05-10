defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.AssetRoute
  alias FavnView.RunStepViewModel

  @refresh_interval_ms 1_500
  @active_statuses [:pending, :running]
  @valid_modes ~w(overview events outputs context debug)

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    run = load_run(run_id)

    socket =
      assign(socket,
        run_id: run_id,
        run: run,
        active_mode: :overview,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> maybe_schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh_run, socket) do
    run = load_run(socket.assigns.run_id, socket.assigns.run[:back_asset_href])

    socket =
      socket
      |> assign(:run, run)
      |> maybe_schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <RunDetailPage.run_detail_page
      run={@run}
      run_id={@run_id}
      nav_items={@nav_items}
      active_mode={@active_mode}
    />
    """
  end

  defp load_run(run_id, back_asset_href \\ nil) do
    with {:ok, run} <- FavnOrchestrator.get_run(run_id),
         {:ok, events} <- FavnOrchestrator.list_run_events(run_id) do
      run_from_public(run, events, back_asset_href)
    else
      {:error, reason} -> %{id: run_id, found?: false, error: error_label(reason)}
    end
  end

  defp run_from_public(run, events, existing_back_asset_href) do
    started_at = Map.get(run, :started_at)
    finished_at = Map.get(run, :finished_at)
    status = Map.get(run, :status)
    target = target_label(Map.get(run, :asset_ref), Map.get(run, :target_refs, []))
    trigger = trigger_label(Map.get(run, :trigger, %{}), Map.get(run, :submit_kind))
    window = window_label(Map.get(run, :params, %{}), Map.get(run, :metadata, %{}))
    asset_results = RunStepViewModel.from_run(run)
    event_items = event_items(events)
    back_asset_href = existing_back_asset_href || back_asset_href(Map.get(run, :asset_ref))
    failure_summary = failure_summary(status, asset_results, event_items)
    current_activity = current_activity(status, asset_results, event_items)

    %{
      found?: true,
      id: run.id,
      raw_status: status,
      active?: active_status?(status),
      short_id: short_id(run.id),
      title: short_id(run.id),
      subtitle: subtitle([target, window]),
      status: status_label(status),
      status_tone: status_tone(status),
      target: target || "No target",
      trigger: trigger || "Manual",
      window: window,
      started_at: timestamp_label(started_at),
      finished_at: timestamp_label(finished_at),
      duration: duration_label(started_at, finished_at),
      manifest_version_id: run.manifest_version_id || "Unknown",
      asset_results: asset_results,
      events: event_items,
      latest_event_summary: latest_event_summary(event_items),
      current_activity: current_activity,
      failure_summary: failure_summary,
      asset_empty_message: asset_empty_message(status, failure_summary),
      outputs: outputs(asset_results),
      context: context_items(run, target, trigger, window),
      back_asset_href: back_asset_href,
      raw_run: debug_inspect(run),
      raw_events: debug_inspect(events)
    }
  end

  defp maybe_schedule_refresh(%{assigns: %{run: %{active?: true}}} = socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh_run, @refresh_interval_ms)

    socket
  end

  defp maybe_schedule_refresh(socket), do: socket

  defp active_status?(status),
    do: status in @active_statuses or status in Enum.map(@active_statuses, &to_string/1)

  defp event_items(events) when is_list(events) do
    events
    |> Enum.map(fn event ->
      %{
        sequence: Map.get(event, :sequence),
        raw_status: Map.get(event, :status),
        timestamp: timestamp_label(Map.get(event, :occurred_at)),
        event_type: event_type_label(Map.get(event, :event_type)),
        status: status_label(Map.get(event, :status)),
        status_tone: status_tone(Map.get(event, :status)),
        asset: event_asset(event),
        summary: event_summary(event)
      }
    end)
  end

  defp event_items(_events), do: []

  defp subtitle(parts) do
    parts
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
  end

  defp target_label(nil, []), do: nil
  defp target_label(nil, refs), do: refs |> Enum.map(&ref_label/1) |> Enum.join(", ")
  defp target_label(ref, _refs), do: ref_label(ref)

  defp back_asset_href(nil), do: nil

  defp back_asset_href(ref) do
    ref_string = ref_label(ref)

    with {:ok, entries} <- FavnOrchestrator.active_asset_catalogue(),
         entry when not is_nil(entry) <-
           Enum.find(entries, fn entry -> ref_label(Map.get(entry, :asset_ref)) == ref_string end),
         target_id when is_binary(target_id) <- Map.get(entry, :target_id) do
      "/assets/#{AssetRoute.to_param(target_id)}"
    else
      _other -> nil
    end
  end

  defp trigger_label(%{kind: kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(%{"kind" => kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(_trigger, nil), do: nil
  defp trigger_label(_trigger, submit_kind), do: humanize(submit_kind)

  defp window_label(params, metadata) do
    window =
      Map.get(params, :window) || Map.get(params, "window") || Map.get(metadata, :selected_window) ||
        Map.get(metadata, "selected_window") || Map.get(metadata, :window)

    cond do
      is_binary(window) ->
        window

      is_map(window) ->
        Map.get(window, :label) || Map.get(window, "label") || Map.get(window, :id) ||
          Map.get(window, "id") || Map.get(window, :key)

      true ->
        nil
    end
  end

  defp ref_label({module, name}), do: "#{inspect(module)}.#{name}"

  defp ref_label(%{"module" => module, "name" => name}), do: "#{module}.#{name}"

  defp ref_label(ref) when is_atom(ref), do: Atom.to_string(ref)
  defp ref_label(ref) when is_binary(ref), do: ref
  defp ref_label(ref), do: inspect(ref)

  defp status_label(status) when status in [:ok, "ok"], do: "Succeeded"
  defp status_label(status) when status in [:running, "running"], do: "Running"
  defp status_label(status) when status in [:retrying, "retrying"], do: "Retrying"
  defp status_label(status) when status in [:pending, "pending"], do: "Pending"
  defp status_label(status) when status in [:partial, "partial"], do: "Partial"
  defp status_label(status) when status in [:error, "error"], do: "Failed"
  defp status_label(status) when status in [:blocked, "blocked"], do: "Blocked"
  defp status_label(status) when status in [:cancelled, "cancelled"], do: "Cancelled"
  defp status_label(status) when status in [:skipped_fresh, "skipped_fresh"], do: "Skipped fresh"
  defp status_label(status) when status in [:timed_out, "timed_out"], do: "Timed out"
  defp status_label(nil), do: "Unknown"
  defp status_label(status), do: humanize(status)

  defp status_tone(status) when status in [:ok, "ok"], do: :success

  defp status_tone(status)
       when status in [:running, :pending, :retrying, "running", "pending", "retrying"], do: :info

  defp status_tone(status) when status in [:partial, "partial"], do: :warning

  defp status_tone(status)
       when status in [:error, :timed_out, :blocked, "error", "timed_out", "blocked"], do: :error

  defp status_tone(status)
       when status in [:cancelled, :skipped_fresh, "cancelled", "skipped_fresh"], do: :neutral

  defp status_tone(_status), do: :neutral

  defp timestamp_label(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")

  defp timestamp_label(_value), do: "-"

  defp duration_label(%DateTime{} = started_at, %DateTime{} = finished_at) do
    DateTime.diff(finished_at, started_at, :millisecond)
    |> duration_ms_label()
  end

  defp duration_label(%DateTime{} = started_at, nil) do
    DateTime.diff(DateTime.utc_now(), started_at, :millisecond)
    |> duration_ms_label()
  end

  defp duration_label(_started_at, _finished_at), do: "-"

  defp duration_ms_label(value) when is_integer(value) and value < 1_000, do: "#{value} ms"
  defp duration_ms_label(value) when is_integer(value), do: "#{Float.round(value / 1_000, 1)} s"
  defp duration_ms_label(_value), do: "-"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp event_type_label(type), do: humanize(type)

  defp event_summary(event) do
    data = Map.get(event, :data, %{}) || %{}

    Map.get(data, :message) ||
      Map.get(data, "message") ||
      event_asset_summary(event) ||
      status_summary(Map.get(event, :status)) ||
      "Persisted event"
  end

  defp event_asset_summary(event) do
    case Map.get(event, :asset_ref) do
      nil -> nil
      ref -> "Asset #{ref_label(ref)}"
    end
  end

  defp event_asset(event) do
    case Map.get(event, :asset_ref) do
      nil -> nil
      ref -> ref_label(ref)
    end
  end

  defp latest_event_summary([]), do: nil
  defp latest_event_summary(events), do: events |> List.last() |> Map.get(:summary)

  defp failure_summary(status, asset_results, events)
       when status in [:partial, :error, :timed_out, "partial", "error", "timed_out"] do
    failed_assets = Enum.filter(asset_results, &(&1.status_tone == :error))
    failed_asset = List.first(failed_assets)
    error_event = latest_error_event(events)

    %{
      count: length(failed_assets),
      total: length(asset_results),
      asset: failed_asset && failed_asset.asset_ref,
      error: (failed_asset && failed_asset.error) || (error_event && error_event.summary)
    }
  end

  defp failure_summary(_status, _asset_results, _events), do: nil

  defp latest_error_event(events) do
    Enum.find(Enum.reverse(events), fn event ->
      event.status_tone == :error or
        event.raw_status in [:error, :timed_out, "error", "timed_out"]
    end)
  end

  defp current_activity(status, asset_results, events)
       when status in [:pending, :running, "pending", "running"] do
    running_asset = Enum.find(asset_results, &(&1.status == "Running"))
    latest_event = List.last(events)

    cond do
      running_asset -> "Currently executing #{running_asset.asset_ref}"
      latest_event && latest_event.asset -> "Latest event: #{latest_event.asset}"
      latest_event -> "Latest event: #{latest_event.summary}"
      true -> "Waiting for first execution event..."
    end
  end

  defp current_activity(_status, _asset_results, _events), do: nil

  defp asset_empty_message(status, _failure_summary)
       when status in [:pending, :running, "pending", "running"],
       do: "Run accepted. Waiting for asset execution results..."

  defp asset_empty_message(status, _failure_summary) when status in [:ok, "ok"],
    do: "Run completed, but no asset results were persisted."

  defp asset_empty_message(status, %{error: error})
       when status in [:error, :timed_out, "error", "timed_out"] and is_binary(error),
       do: "Run failed before asset results were persisted. Latest error: #{error}"

  defp asset_empty_message(status, _failure_summary)
       when status in [:error, :timed_out, "error", "timed_out"],
       do: "Run failed before asset results were persisted."

  defp asset_empty_message(_status, _failure_summary),
    do: "No asset results persisted for this run yet."

  defp outputs(asset_results) do
    asset_results
    |> Enum.filter(& &1.output)
    |> Enum.map(fn asset ->
      %{asset: asset.asset_ref, output: inspect(asset.output, pretty: true)}
    end)
  end

  defp context_items(run, target, trigger, window) do
    [
      %{label: "Run ID", value: run.id},
      %{label: "Manifest version", value: run.manifest_version_id || "Unknown"},
      %{label: "Target", value: target || "No target"},
      %{label: "Trigger", value: trigger || "Manual"},
      %{label: "Window", value: window || "No window metadata"},
      %{label: "Submit kind", value: submit_kind_label(Map.get(run, :submit_kind))},
      %{label: "Replay mode", value: submit_kind_label(Map.get(run, :replay_mode))}
    ]
  end

  defp submit_kind_label(nil), do: "Unknown"
  defp submit_kind_label(value), do: humanize(value)

  defp debug_inspect(value), do: inspect(value, pretty: true, limit: 50, printable_limit: 2_000)

  defp status_summary(nil), do: nil
  defp status_summary(status), do: "Status #{status_label(status)}"

  defp humanize(value) when is_atom(value), do: value |> Atom.to_string() |> humanize()

  defp humanize(value) when is_binary(value) do
    value
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp humanize(value), do: inspect(value)

  defp error_label(:not_found), do: "Run not found"
  defp error_label(_reason), do: "Run could not be loaded"
end
