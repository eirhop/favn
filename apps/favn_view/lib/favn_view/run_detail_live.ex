defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.AssetRoute
  alias FavnView.LogsViewModel
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
    event_items = event_items(events)
    asset_results = execution_rows(run, event_items)
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
        raw_event_type: Map.get(event, :event_type),
        status: status_label(Map.get(event, :status)),
        status_tone: status_tone(Map.get(event, :status)),
        asset: event_asset(event),
        asset_step_id: event_data(event, :asset_step_id),
        runner_execution_id: event_data(event, :runner_execution_id),
        attempt: event_data(event, :attempt),
        stage: Map.get(event, :stage) || event_data(event, :stage),
        occurred_at: Map.get(event, :occurred_at),
        data: Map.get(event, :data, %{}) || %{},
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

  defp execution_rows(run, event_items) do
    persisted_rows = RunStepViewModel.from_run(run)

    persisted_rows
    |> merge_event_rows(event_execution_rows(run, event_items))
    |> append_waiting_rows(waiting_execution_rows(run, persisted_rows, event_items))
    |> Enum.sort_by(&execution_sort_key/1)
  end

  defp merge_event_rows(persisted_rows, event_rows) do
    event_rows_by_id = Map.new(event_rows, &{&1.id, &1})
    unique_asset_refs = unique_asset_refs(persisted_rows, event_rows)

    event_rows_by_asset =
      event_rows
      |> Enum.filter(&MapSet.member?(unique_asset_refs, &1.asset_ref))
      |> Map.new(&{&1.asset_ref, &1})

    merged_persisted_rows =
      Enum.map(persisted_rows, fn row ->
        case Map.get(event_rows_by_id, row.id) || Map.get(event_rows_by_asset, row.asset_ref) do
          nil ->
            row

          event_row ->
            Map.merge(event_row, row, fn _key, event_value, persisted_value ->
              persisted_value || event_value
            end)
        end
      end)

    persisted_ids = MapSet.new(merged_persisted_rows, & &1.id)
    persisted_refs = MapSet.new(merged_persisted_rows, & &1.asset_ref)

    new_event_rows =
      Enum.reject(event_rows, fn row ->
        MapSet.member?(persisted_ids, row.id) ||
          (MapSet.member?(unique_asset_refs, row.asset_ref) &&
             MapSet.member?(persisted_refs, row.asset_ref))
      end)

    merged_persisted_rows ++ new_event_rows
  end

  defp unique_asset_refs(persisted_rows, event_rows) do
    persisted_unique_refs = unique_refs(persisted_rows)
    event_unique_refs = unique_refs(event_rows)

    MapSet.intersection(persisted_unique_refs, event_unique_refs)
  end

  defp unique_refs(rows) do
    rows
    |> Enum.frequencies_by(& &1.asset_ref)
    |> Enum.filter(fn {_asset_ref, count} -> count == 1 end)
    |> Enum.map(fn {asset_ref, _count} -> asset_ref end)
    |> MapSet.new()
  end

  defp append_waiting_rows(rows, waiting_rows) do
    row_refs = rows |> Enum.map(& &1.asset_ref) |> MapSet.new()
    rows ++ Enum.reject(waiting_rows, &MapSet.member?(row_refs, &1.asset_ref))
  end

  defp event_execution_rows(run, event_items) do
    event_items
    |> Enum.filter(&step_event?/1)
    |> Enum.group_by(&event_row_key(run.id, &1))
    |> Enum.map(fn {_key, events} -> event_execution_row(run.id, events) end)
  end

  defp event_execution_row(run_id, events) do
    latest = List.last(events)

    first_started = started_event_for_attempt(events, latest.attempt) || List.first(events)

    asset_ref = latest.asset || "Unknown asset"
    status = event_step_status(latest)

    %{
      id: latest.asset_step_id || LogsViewModel.deterministic_step_id(run_id, asset_ref),
      asset_ref: asset_ref,
      display_name: LogsViewModel.display_name(asset_ref) || asset_ref,
      secondary: event_row_secondary(latest),
      status: status_label(status),
      raw_status: status,
      status_tone: status_tone(status),
      duration: event_row_duration(first_started, latest),
      started_at: timestamp_label(first_started && first_started.occurred_at),
      attempt: latest.attempt,
      error: event_row_error(latest),
      explanation: event_row_explanation(latest),
      output: nil,
      inspectable?: is_binary(latest.asset_step_id)
    }
  end

  defp waiting_execution_rows(%{status: status} = run, persisted_rows, event_items)
       when status in [:pending, :running, "pending", "running"] do
    known_refs =
      (Enum.map(persisted_rows, & &1.asset_ref) ++ Enum.map(event_items, & &1.asset))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    run
    |> Map.get(:target_refs, [])
    |> Enum.map(&ref_label/1)
    |> Enum.reject(&MapSet.member?(known_refs, &1))
    |> Enum.map(fn asset_ref -> waiting_execution_row(run.id, asset_ref) end)
  end

  defp waiting_execution_rows(_run, _persisted_rows, _event_items), do: []

  defp waiting_execution_row(run_id, asset_ref) do
    %{
      id: LogsViewModel.deterministic_step_id(run_id, asset_ref),
      asset_ref: asset_ref,
      display_name: LogsViewModel.display_name(asset_ref) || asset_ref,
      secondary: "Waiting for scheduler",
      status: "Waiting",
      raw_status: :pending,
      status_tone: :neutral,
      duration: "-",
      started_at: "-",
      attempt: nil,
      error: nil,
      explanation: "Asset has not started yet for this run.",
      output: nil,
      inspectable?: false
    }
  end

  defp execution_sort_key(row), do: {stage_sort(Map.get(row, :secondary)), row.asset_ref}

  defp stage_sort(nil), do: 999_999

  defp stage_sort(secondary) do
    case Regex.run(~r/Stage (\d+)/, secondary) do
      [_, stage] -> String.to_integer(stage)
      _other -> 999_999
    end
  end

  defp step_event?(%{raw_event_type: event_type}) when is_atom(event_type) do
    String.starts_with?(Atom.to_string(event_type), "step_")
  end

  defp step_event?(%{raw_event_type: event_type}) when is_binary(event_type),
    do: String.starts_with?(event_type, "step_")

  defp step_event?(_event), do: false

  defp event_row_key(run_id, event) do
    event.asset_step_id || LogsViewModel.deterministic_step_id(run_id, event.asset || "unknown")
  end

  defp event_step_status(%{raw_event_type: event_type, raw_status: status}) do
    case event_type do
      type when type in [:step_started, "step_started"] -> :running
      type when type in [:step_finished, "step_finished"] -> :ok
      type when type in [:step_failed, "step_failed"] -> :error
      type when type in [:step_timed_out, "step_timed_out"] -> :timed_out
      type when type in [:step_cancelled, "step_cancelled"] -> :cancelled
      type when type in [:step_retry_scheduled, "step_retry_scheduled"] -> :retrying
      type when type in [:step_skipped_fresh, "step_skipped_fresh"] -> :skipped_fresh
      type when type in [:step_blocked, "step_blocked"] -> :blocked
      _other -> status || :running
    end
  end

  defp event_row_secondary(event) do
    [
      event.stage && "Stage #{event.stage}",
      event.attempt && "Attempt #{event.attempt}",
      event.runner_execution_id && "Runner #{short_id(event.runner_execution_id)}"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
    |> case do
      "" -> nil
      secondary -> secondary
    end
  end

  defp event_row_duration(%{occurred_at: %DateTime{} = started_at}, %{
         occurred_at: %DateTime{} = occurred_at,
         raw_event_type: event_type
       })
       when event_type not in [:step_started, "step_started"] do
    DateTime.diff(occurred_at, started_at, :millisecond)
    |> duration_ms_label()
  end

  defp event_row_duration(%{occurred_at: %DateTime{} = started_at}, _latest) do
    DateTime.diff(DateTime.utc_now(), started_at, :millisecond)
    |> duration_ms_label()
  end

  defp event_row_duration(_started, _latest), do: "-"

  defp event_row_error(%{data: data}) do
    data
    |> event_data_value(:error)
    |> error_summary()
  end

  defp started_event_for_attempt(events, nil) do
    Enum.find(Enum.reverse(events), &(&1.raw_event_type in [:step_started, "step_started"]))
  end

  defp started_event_for_attempt(events, attempt) do
    Enum.find(Enum.reverse(events), fn event ->
      event.raw_event_type in [:step_started, "step_started"] and event.attempt == attempt
    end) || started_event_for_attempt(events, nil)
  end

  defp event_row_explanation(%{raw_event_type: type})
       when type in [:step_started, "step_started"],
       do: "Execution has started; waiting for runner result."

  defp event_row_explanation(%{raw_event_type: type})
       when type in [:step_retry_scheduled, "step_retry_scheduled"],
       do: "Retry has been scheduled for this asset."

  defp event_row_explanation(%{raw_event_type: type})
       when type in [:step_finished, "step_finished"],
       do: "Execution finished successfully."

  defp event_row_explanation(_event), do: nil

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

  defp event_data(event, key) do
    data = Map.get(event, :data, %{}) || %{}
    event_data_value(data, key)
  end

  defp event_data_value(data, key), do: Map.get(data, key) || Map.get(data, Atom.to_string(key))

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

  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(%{reason: reason}), do: error_summary(reason)
  defp error_summary(%{"reason" => reason}), do: error_summary(reason)
  defp error_summary(reason) when is_binary(reason), do: reason
  defp error_summary(reason) when is_atom(reason), do: humanize(reason)
  defp error_summary(reason), do: inspect(reason, limit: 5, printable_limit: 200)

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
