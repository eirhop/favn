defmodule FavnView.LogsLiveSupport do
  @moduledoc false

  import Phoenix.Component, only: [assign: 2, assign: 3]
  import Phoenix.LiveView, only: [connected?: 1]

  require Logger

  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnView.Orchestrator
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.LogsViewModel

  @initial_limit 200
  @fetch_limit 500
  @poll_interval_ms 2_000
  @dialyzer {:no_unused,
             [target_label: 1, run_context_from_public: 3, asset_context_from_public: 2]}
  @dialyzer {:no_match,
             [
               run_context: 3,
               asset_context: 4,
               load_initial_logs: 2,
               replay_gap: 1,
               error_label: 1
             ]}

  def mount_logs(socket, attrs) do
    filter = Filter.normalize(Map.fetch!(attrs, :filter))
    operator_context = Map.fetch!(attrs, :operator_context)
    socket = socket |> assign(Map.merge(default_assigns(), attrs)) |> assign(:filter, filter)
    socket = if connected?(socket), do: subscribe(socket), else: socket
    socket = load_snapshot(socket, operator_context, filter)
    if connected?(socket), do: socket |> replay_gap() |> schedule_poll(), else: socket
  end

  defp load_snapshot(socket, operator_context, filter) do
    result = load_initial_logs(operator_context, filter)

    socket
    |> assign(:logs_status, result.status)
    |> assign(:logs, result.logs)
    |> assign(:next_cursor, result.cursor)
    |> assign_visible_logs()
  end

  def wakeup(socket), do: replay_gap(socket)

  def poll(socket), do: socket |> replay_gap() |> schedule_poll()

  def handle_filter(socket, params) do
    filters = Map.get(params, "filters", %{})

    level = normalize_choice(Map.get(filters, "level"))
    source = normalize_choice(Map.get(filters, "source"))

    filter = %{
      socket.assigns.filter
      | levels: Enum.filter(Entry.levels(), &(to_string(&1) == level)),
        sources: Enum.filter(Entry.sources(), &(to_string(&1) == source))
    }

    changed? = filter != socket.assigns.filter

    socket =
      socket
      |> assign(:search_query, Map.get(filters, "search", ""))
      |> assign(:selected_level, level)
      |> assign(:selected_source, source)
      |> assign(:filter, filter)

    if changed?,
      do: load_snapshot(socket, socket.assigns.operator_context, filter),
      else: assign_visible_logs(socket)
  end

  def toggle(socket, key) do
    assign(socket, key, !Map.fetch!(socket.assigns, key))
  end

  def unsubscribe(%{assigns: %{log_subscription: subscription}}) when not is_nil(subscription) do
    _ = Orchestrator.unsubscribe_logs(subscription)
    :ok
  end

  def unsubscribe(_socket), do: :ok

  def run_context(operator_context, run_id, timezone) do
    case Orchestrator.get_run_detail(operator_context, run_id) do
      {:ok, %{summary: summary} = detail} ->
        run_context_from_public(summary, Map.get(detail, :steps, []), timezone)

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

  def asset_context(operator_context, run_id, asset_step_id, timezone) do
    case Orchestrator.get_asset_step_log_context(operator_context, run_id, asset_step_id) do
      {:ok, context} ->
        asset_context_from_public(context, timezone)

      {:error, _reason} ->
        missing_asset_context(operator_context, run_id, asset_step_id, timezone)
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

  defp load_initial_logs(operator_context, filter) do
    list = Application.get_env(:favn_view, :list_logs_fun, &Orchestrator.list_logs/3)

    case list.(operator_context, filter, limit: @fetch_limit) do
      {:ok, %{items: items, replay_cursor: cursor}} ->
        %{status: :ready, logs: LogsViewModel.trim_latest(items, @initial_limit), cursor: cursor}

      {:error, _reason} ->
        %{status: :error, logs: [], cursor: nil}
    end
  end

  defp subscribe(socket) do
    case subscribe_logs(socket.assigns.operator_context, socket.assigns.filter) do
      {:ok, subscription} ->
        socket
        |> assign(:log_subscription, subscription)
        |> assign(:live?, true)

      {:error, _reason} ->
        assign(
          socket,
          :stream_warning,
          "Loaded existing logs, but live streaming is unavailable."
        )
    end
  end

  defp replay_gap(%{assigns: %{next_cursor: nil}} = socket),
    do: load_snapshot(socket, socket.assigns.operator_context, socket.assigns.filter)

  defp replay_gap(socket) do
    replay = Application.get_env(:favn_view, :replay_logs_fun, &Orchestrator.replay_logs/4)

    case replay.(
           socket.assigns.operator_context,
           socket.assigns.next_cursor,
           socket.assigns.filter,
           limit: @initial_limit
         ) do
      {:ok, %{items: entries, replay_cursor: cursor, has_more?: more?}} ->
        if more?, do: send(self(), :favn_logs_available)

        logs =
          socket.assigns.logs
          |> LogsViewModel.merge_entries(entries)
          |> LogsViewModel.trim_latest(@initial_limit)

        socket
        |> assign(:stream_warning, nil)
        |> assign(:logs, logs)
        |> assign(
          :next_cursor,
          cursor
        )
        |> assign_visible_logs()

      {:error, _reason} ->
        assign(socket, :stream_warning, "Unable to refresh logs.")
    end
  end

  defp subscribe_logs(operator_context, filter) do
    fun =
      Application.get_env(
        :favn_view,
        :log_subscribe_fun,
        &Orchestrator.subscribe_logs/2
      )

    if is_function(fun, 2), do: fun.(operator_context, filter), else: fun.(filter)
  end

  defp assign_visible_logs(socket) do
    visible_logs =
      socket.assigns.logs
      |> LogsViewModel.entries(socket.assigns.current_scope)
      |> LogsViewModel.filter_entries(
        socket.assigns.search_query,
        socket.assigns.selected_level,
        socket.assigns.selected_source
      )

    assign(socket, :visible_logs, visible_logs)
  end

  defp normalize_choice(value) when value in [nil, "", "all"], do: "all"
  defp normalize_choice(value), do: to_string(value)

  defp run_context_from_public(summary, steps, timezone) do
    status = Map.get(summary, :status)

    %{
      found?: true,
      id: summary.id,
      title: target_label(summary) || LogsViewModel.short_id(summary.id),
      subtitle: LogsViewModel.short_id(summary.id),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      started_at: LogsViewModel.timestamp_label(summary.started_at, timezone),
      duration: LogsViewModel.duration_ms_label(summary.duration_ms),
      asset_results: Enum.map(steps, &step_from_public(&1, timezone))
    }
  end

  defp asset_context_from_public(context, timezone) do
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
      facts: Enum.map(context[:facts] || [], &fact_from_public(&1, timezone)),
      log_filter: context[:log_filter],
      note: context[:note]
    }
  end

  defp missing_asset_context(operator_context, run_id, asset_step_id, timezone) do
    %{
      run: run_context(operator_context, run_id, timezone),
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

  defp step_from_public(step, timezone) do
    %{
      id: step.id,
      display_name: LogsViewModel.display_name(step.asset_ref) || step.asset_ref,
      status: LogsViewModel.status_label(step.status),
      status_tone: LogsViewModel.status_tone(step.status),
      started_at: LogsViewModel.timestamp_label(step.started_at, timezone),
      duration: LogsViewModel.duration_ms_label(step.duration_ms),
      attempt: step.attempt
    }
  end

  defp fact_from_public(%{label: "Started", value: value}, timezone),
    do: %{label: "Started", value: LogsViewModel.timestamp_label(value, timezone)}

  defp fact_from_public(%{label: "Duration", value: value}, _timezone),
    do: %{label: "Duration", value: LogsViewModel.duration_ms_label(value)}

  defp fact_from_public(%{label: label, value: nil}, _timezone), do: %{label: label, value: "-"}
  defp fact_from_public(fact, _timezone), do: fact

  defp target_label(%{target_refs: refs}) when is_list(refs) and refs != [] do
    refs |> Enum.map(&LogsViewModel.ref_label/1) |> Enum.join(", ")
  end

  defp target_label(%{asset_ref: target}), do: LogsViewModel.ref_label(target)

  defp error_label(:not_found), do: "Run not found"
  defp error_label(_reason), do: "Unable to load run"

  defp schedule_poll(socket) do
    Process.send_after(self(), :poll_logs, @poll_interval_ms)
    socket
  end
end
