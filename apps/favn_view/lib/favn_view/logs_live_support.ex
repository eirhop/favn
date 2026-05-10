defmodule FavnView.LogsLiveSupport do
  @moduledoc false

  import Phoenix.Component, only: [assign: 2, assign: 3]
  import Phoenix.LiveView, only: [connected?: 1]

  alias Favn.Log.Filter
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.LogsViewModel
  alias FavnView.RunStepViewModel

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
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} ->
        run_context_from_public(run)

      {:error, reason} ->
        %{
          found?: false,
          id: run_id,
          title: LogsViewModel.short_id(run_id),
          error: error_label(reason)
        }
    end
  end

  def asset_context(run_id, asset_step_id) do
    run = run_context(run_id)

    result =
      if run[:found?], do: Enum.find(run.asset_results, &(&1.id == asset_step_id)), else: nil

    %{
      run: run,
      result: result,
      title: (result && result.display_name) || "Asset logs",
      subtitle: "Run #{LogsViewModel.short_id(run_id)} · Asset step #{asset_step_id}",
      status: result && result.status,
      status_tone: (result && result.status_tone) || :neutral,
      facts: asset_facts(result),
      note:
        if(run[:found?] && is_nil(result),
          do: "Asset step context not found, showing matching logs."
        )
    }
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

  defp run_context_from_public(run) do
    started_at = Map.get(run, :started_at)
    finished_at = Map.get(run, :finished_at)
    status = Map.get(run, :status)
    target = target_label(Map.get(run, :asset_ref), Map.get(run, :target_refs, []))
    asset_results = RunStepViewModel.from_run(run)

    %{
      found?: true,
      id: run.id,
      title: target || LogsViewModel.short_id(run.id),
      subtitle: LogsViewModel.short_id(run.id),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      started_at: LogsViewModel.timestamp_label(started_at),
      duration: LogsViewModel.duration_label(started_at, finished_at),
      asset_results: asset_results
    }
  end

  defp asset_facts(nil), do: []

  defp asset_facts(result) do
    [
      %{label: "Started", value: result.started_at},
      %{label: "Duration", value: result.duration},
      %{label: "Attempt", value: result.attempt || "-"}
    ]
  end

  defp target_label(nil, []), do: nil
  defp target_label(nil, [target | _rest]), do: LogsViewModel.ref_label(target)
  defp target_label(target, _targets), do: LogsViewModel.ref_label(target)

  defp error_label(:not_found), do: "Run not found"
  defp error_label(reason), do: "Unable to load run: #{inspect(reason)}"
end
