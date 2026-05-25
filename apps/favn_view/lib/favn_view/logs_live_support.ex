defmodule FavnView.LogsLiveSupport do
  @moduledoc false

  import Phoenix.Component, only: [assign: 2, assign: 3]
  import Phoenix.LiveView, only: [connected?: 1]

  require Logger

  alias Favn.Log.Filter
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.LogsViewModel

  @initial_limit 200
  @fetch_limit 500

  def mount_logs(socket, attrs) do
    filter = Filter.normalize(Map.fetch!(attrs, :filter))
    scope = Map.fetch!(attrs, :scope)
    load_result = load_initial_logs(filter)

    socket =
      socket
      |> assign(Map.merge(default_assigns(), attrs))
      |> assign(:filter, filter)
      |> assign(:logs_status, load_result.status)
      |> assign(:logs, load_result.logs)
      |> assign(:next_cursor, LogsViewModel.latest_cursor(load_result.logs, scope, filter))
      |> assign_visible_logs()

    if connected?(socket) and load_result.status != :error do
      subscribe_and_replay(socket)
    else
      socket
    end
  end

  def handle_filter(socket, params) do
    filters = Map.get(params, "filters", %{})

    socket
    |> assign(:search_query, Map.get(filters, "search", ""))
    |> assign(:selected_level, normalize_choice(Map.get(filters, "level")))
    |> assign(:selected_source, normalize_choice(Map.get(filters, "source")))
    |> assign_visible_logs()
  end

  def toggle(socket, key) do
    assign(socket, key, !Map.fetch!(socket.assigns, key))
  end

  def add_live_log(socket, entry) do
    logs =
      socket.assigns.logs
      |> LogsViewModel.merge_entries([entry])
      |> LogsViewModel.trim_latest(@initial_limit)

    socket
    |> assign(:logs, logs)
    |> assign(
      :next_cursor,
      LogsViewModel.latest_cursor(logs, socket.assigns.scope, socket.assigns.filter)
    )
    |> assign_visible_logs()
  end

  def unsubscribe(%{assigns: %{log_subscription: subscription}}) when not is_nil(subscription) do
    _ = FavnOrchestrator.unsubscribe_logs(subscription)
    :ok
  end

  def unsubscribe(_socket), do: :ok

  def run_context(run_id) do
    case FavnOrchestrator.get_run_detail(run_id) do
      {:ok, %{summary: summary} = detail} ->
        run_context_from_public(summary, Map.get(detail, :steps, []))

      {:error, reason} ->
        Logger.error(
          "logs.run_context failed run_id=#{inspect(run_id)} reason=#{inspect(reason)}"
        )

        %{
          found?: false,
          id: run_id,
          title: LogsViewModel.short_id(run_id),
          error: error_label(reason)
        }
    end
  end

  def asset_context(run_id, asset_step_id) do
    case FavnOrchestrator.get_asset_step_log_context(run_id, asset_step_id) do
      {:ok, context} -> asset_context_from_public(context)
      {:error, _reason} -> missing_asset_context(run_id, asset_step_id)
    end
  end

  def nav_items(active \\ :logs), do: AssetCataloguePage.nav_items(active)

  defp default_assigns do
    %{
      nav_items: nav_items(),
      title: "Logs",
      subtitle: nil,
      scope: :global,
      status: nil,
      status_tone: :neutral,
      output_status: nil,
      output_metadata: nil,
      facts: [],
      back_href: nil,
      back_label: nil,
      empty_state: "No logs yet.",
      context_note: nil,
      search_query: "",
      selected_level: "all",
      selected_source: "all",
      wrap?: true,
      live_tail?: true,
      live?: false,
      stream_warning: nil,
      log_subscription: nil
    }
  end

  defp load_initial_logs(filter) do
    case FavnOrchestrator.list_logs(filter, limit: @fetch_limit, order: :desc) do
      {:ok, %{items: items}} ->
        %{status: :ready, logs: LogsViewModel.trim_latest(items, @initial_limit)}

      {:error, _reason} ->
        %{status: :error, logs: []}
    end
  end

  defp subscribe_and_replay(socket) do
    case subscribe_logs(socket.assigns.filter) do
      {:ok, subscription} ->
        socket
        |> assign(:log_subscription, subscription)
        |> assign(:live?, true)
        |> replay_gap()

      {:error, _reason} ->
        assign(
          socket,
          :stream_warning,
          "Loaded existing logs, but live streaming is unavailable."
        )
    end
  end

  defp replay_gap(%{assigns: %{next_cursor: nil}} = socket), do: socket

  defp replay_gap(socket) do
    # Initial load happens before subscription; replay closes that small handoff gap.
    case FavnOrchestrator.replay_logs(socket.assigns.next_cursor, socket.assigns.filter,
           limit: @initial_limit
         ) do
      {:ok, []} ->
        socket

      {:ok, entries} ->
        logs =
          socket.assigns.logs
          |> LogsViewModel.merge_entries(entries)
          |> LogsViewModel.trim_latest(@initial_limit)

        socket
        |> assign(:logs, logs)
        |> assign(
          :next_cursor,
          LogsViewModel.latest_cursor(logs, socket.assigns.scope, socket.assigns.filter)
        )
        |> assign_visible_logs()

      {:error, _reason} ->
        socket
    end
  end

  defp subscribe_logs(filter) do
    Application.get_env(:favn_view, :log_subscribe_fun, &FavnOrchestrator.subscribe_logs/1).(
      filter
    )
  end

  defp assign_visible_logs(socket) do
    visible_logs =
      socket.assigns.logs
      |> LogsViewModel.entries()
      |> LogsViewModel.filter_entries(
        socket.assigns.search_query,
        socket.assigns.selected_level,
        socket.assigns.selected_source
      )

    assign(socket, :visible_logs, visible_logs)
  end

  defp normalize_choice(value) when value in [nil, "", "all"], do: "all"
  defp normalize_choice(value), do: to_string(value)

  defp run_context_from_public(summary, steps) do
    status = Map.get(summary, :status)

    %{
      found?: true,
      id: summary.id,
      title: target_label(summary) || LogsViewModel.short_id(summary.id),
      subtitle: LogsViewModel.short_id(summary.id),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      started_at: LogsViewModel.timestamp_label(summary.started_at),
      duration: LogsViewModel.duration_ms_label(summary.duration_ms),
      asset_results: Enum.map(steps, &step_from_public/1)
    }
  end

  defp asset_context_from_public(context) do
    step = context[:step]

    %{
      run: context[:run],
      result: step,
      title: context[:title],
      subtitle: context[:subtitle],
      status: step && LogsViewModel.status_label(step.status),
      output_status: step && step.status,
      output_metadata: step && Map.get(step, :output_metadata),
      status_tone: (step && LogsViewModel.status_tone(step.status)) || :neutral,
      facts: Enum.map(context[:facts] || [], &fact_from_public/1),
      log_filter: context[:log_filter],
      note: context[:note]
    }
  end

  defp missing_asset_context(run_id, asset_step_id) do
    %{
      run: run_context(run_id),
      result: nil,
      title: "Asset logs",
      subtitle: "Run #{LogsViewModel.short_id(run_id)} · Asset step #{asset_step_id}",
      status: nil,
      output_status: nil,
      output_metadata: nil,
      status_tone: :neutral,
      facts: [],
      log_filter: %Filter{run_id: run_id, asset_step_id: asset_step_id},
      note: "Asset step context not found, showing matching logs."
    }
  end

  defp step_from_public(step) do
    %{
      id: step.id,
      display_name: LogsViewModel.display_name(step.asset_ref) || step.asset_ref,
      status: LogsViewModel.status_label(step.status),
      status_tone: LogsViewModel.status_tone(step.status),
      started_at: LogsViewModel.timestamp_label(step.started_at),
      duration: LogsViewModel.duration_ms_label(step.duration_ms),
      attempt: step.attempt
    }
  end

  defp fact_from_public(%{label: "Started", value: value}),
    do: %{label: "Started", value: LogsViewModel.timestamp_label(value)}

  defp fact_from_public(%{label: "Duration", value: value}),
    do: %{label: "Duration", value: LogsViewModel.duration_ms_label(value)}

  defp fact_from_public(%{label: label, value: nil}), do: %{label: label, value: "-"}
  defp fact_from_public(fact), do: fact

  defp target_label(%{target_refs: refs}) when is_list(refs) and refs != [] do
    refs |> Enum.map(&LogsViewModel.ref_label/1) |> Enum.join(", ")
  end

  defp target_label(%{asset_ref: target}), do: LogsViewModel.ref_label(target)

  defp error_label(:not_found), do: "Run not found"
  defp error_label(_reason), do: "Unable to load run"
end
