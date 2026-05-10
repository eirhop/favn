defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage

  @valid_modes ~w(overview events assets output debug)

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    socket =
      assign(socket,
        run_id: run_id,
        run: load_run(run_id),
        active_mode: :overview,
        nav_items: AssetCataloguePage.nav_items()
      )

    {:ok, socket}
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

  defp load_run(run_id) do
    with {:ok, run} <- FavnOrchestrator.get_run(run_id),
         {:ok, events} <- FavnOrchestrator.list_run_events(run_id) do
      run_from_public(run, events)
    else
      {:error, reason} -> %{id: run_id, found?: false, error: error_label(reason)}
    end
  end

  defp run_from_public(run, events) do
    started_at = Map.get(run, :started_at)
    finished_at = Map.get(run, :finished_at)
    status = Map.get(run, :status)
    target = target_label(Map.get(run, :asset_ref), Map.get(run, :target_refs, []))
    trigger = trigger_label(Map.get(run, :trigger, %{}), Map.get(run, :submit_kind))
    window = window_label(Map.get(run, :params, %{}), Map.get(run, :metadata, %{}))
    asset_results = asset_results(Map.get(run, :asset_results, %{}))
    event_items = event_items(events)

    %{
      found?: true,
      id: run.id,
      short_id: short_id(run.id),
      title: "Run #{short_id(run.id)}",
      subtitle: subtitle([target, trigger, window]),
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
      asset_result_groups: asset_result_groups(asset_results),
      events: event_items,
      latest_events: Enum.take(Enum.reverse(event_items), 8)
    }
  end

  defp asset_results(results) when is_map(results) do
    results
    |> Map.values()
    |> Enum.map(fn result ->
      %{
        ref: ref_label(Map.get(result, :ref)),
        stage: Map.get(result, :stage),
        status: status_label(Map.get(result, :status)),
        status_tone: status_tone(Map.get(result, :status)),
        started_at: timestamp_label(Map.get(result, :started_at)),
        duration: duration_ms_label(Map.get(result, :duration_ms)),
        error: error_summary(Map.get(result, :error))
      }
    end)
    |> Enum.sort_by(&{&1.stage || 0, &1.ref})
  end

  defp asset_results(_results), do: []

  defp asset_result_groups(results) do
    results
    |> Enum.group_by(& &1.stage)
    |> Enum.sort_by(fn {stage, _items} -> stage || 0 end)
    |> Enum.map(fn {stage, items} -> %{stage: stage, items: items} end)
  end

  defp event_items(events) when is_list(events) do
    events
    |> Enum.map(fn event ->
      %{
        sequence: Map.get(event, :sequence),
        timestamp: timestamp_label(Map.get(event, :occurred_at)),
        event_type: event_type_label(Map.get(event, :event_type)),
        status: status_label(Map.get(event, :status)),
        status_tone: status_tone(Map.get(event, :status)),
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

  defp trigger_label(%{kind: kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(%{"kind" => kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(_trigger, nil), do: nil
  defp trigger_label(_trigger, submit_kind), do: humanize(submit_kind)

  defp window_label(params, metadata) do
    window = Map.get(params, :window) || Map.get(params, "window") || Map.get(metadata, :window)

    cond do
      is_binary(window) ->
        window

      is_map(window) ->
        Map.get(window, :label) || Map.get(window, "label") || Map.get(window, :key)

      true ->
        nil
    end
  end

  defp ref_label({module, name}), do: "#{inspect(module)}.#{name}"

  defp ref_label(%{"module" => module, "name" => name}), do: "#{module}.#{name}"

  defp ref_label(ref) when is_atom(ref), do: Atom.to_string(ref)
  defp ref_label(ref) when is_binary(ref), do: ref
  defp ref_label(ref), do: inspect(ref)

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:running), do: "Running"
  defp status_label(:pending), do: "Pending"
  defp status_label(:partial), do: "Partial"
  defp status_label(:error), do: "Failed"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:timed_out), do: "Timed out"
  defp status_label(nil), do: "Unknown"
  defp status_label(status), do: humanize(status)

  defp status_tone(status) when status in [:ok, "ok"], do: :success
  defp status_tone(status) when status in [:running, :pending, "running", "pending"], do: :info
  defp status_tone(status) when status in [:partial, "partial"], do: :warning
  defp status_tone(status) when status in [:error, :timed_out, "error", "timed_out"], do: :error
  defp status_tone(status) when status in [:cancelled, "cancelled"], do: :neutral
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

  defp status_summary(nil), do: nil
  defp status_summary(status), do: "Status #{status_label(status)}"

  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(error), do: inspect(error)

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
