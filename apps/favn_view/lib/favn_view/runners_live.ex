defmodule FavnView.RunnersLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
  alias FavnView.Components.RunnersPage
  alias FavnView.LiveRefresh
  alias FavnView.Time

  @refresh_interval_ms 3_000
  @windows [:today, :week, :month, :all]
  @states [:all, :connected, :shut_down, :crashed, :presumed_dead, :struggling]

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        overview: nil,
        loading: true,
        error: nil,
        window: :week,
        state: :all,
        expanded: %{},
        nav_items: RunnersPage.nav_items()
      )
      |> LiveRefresh.init([:poll_ref])
      |> load_overview()

    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <RunnersPage.runners_page
      overview={@overview}
      loading={@loading}
      error={@error}
      window={@window}
      state={@state}
      expanded={@expanded}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    />
    """
  end

  @impl true
  def handle_event("reload", _params, socket), do: {:noreply, load_overview(socket)}

  def handle_event("set_window", %{"scope" => value}, socket) do
    case parse_enum(value, @windows) do
      {:ok, window} -> {:noreply, socket |> assign(window: window) |> load_overview()}
      :error -> {:noreply, socket}
    end
  end

  def handle_event("set_state", %{"scope" => value}, socket) do
    case parse_enum(value, @states) do
      {:ok, state} -> {:noreply, assign(socket, state: state)}
      :error -> {:noreply, socket}
    end
  end

  def handle_event("toggle_session_tasks", %{"key" => key} = params, socket) do
    if Map.has_key?(socket.assigns.expanded, key) do
      {:noreply, assign(socket, expanded: Map.delete(socket.assigns.expanded, key))}
    else
      {:noreply, expand_session(socket, key, params)}
    end
  end

  @impl true
  def handle_info({:poll_runners, token}, socket) do
    case LiveRefresh.take(socket, :poll_ref, token) do
      {:ok, socket} -> {:noreply, load_overview(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info(:favn_persistence_published, socket), do: {:noreply, load_overview(socket)}

  defp load_overview(socket) do
    opts = [limit: 50, overlapping_after: window_start(socket.assigns.window, socket)]

    result = get_operator_runner_overview(socket.assigns.current_scope.operator_context, opts)

    socket =
      case result do
        {:ok, overview} ->
          assign(socket, overview: overview, loading: false, error: nil)

        {:error, reason} ->
          Logger.error("runners.overview failed: #{inspect(reason)}")

          assign(socket,
            overview: nil,
            loading: false,
            error: "Runner diagnostics are unavailable"
          )
      end

    schedule_poll(socket)
  end

  defp expand_session(socket, key, params) do
    with {:ok, generation} <- parse_integer(params["generation"]),
         {:ok, registered_at} <- parse_datetime(params["registered-at"]),
         {:ok, ended_at} <- parse_optional_datetime(params["ended-at"]),
         instance when is_binary(instance) and instance != "" <- params["instance"],
         {:ok, tasks} <-
           get_operator_runner_session_tasks(
             socket.assigns.current_scope.operator_context,
             runner_instance_id: instance,
             session_generation: generation,
             registered_at: registered_at,
             ended_at: ended_at
           ) do
      assign(socket, expanded: Map.put(socket.assigns.expanded, key, tasks))
    else
      other ->
        Logger.error("runners.session_tasks failed: #{inspect(other)}")
        assign(socket, expanded: Map.put(socket.assigns.expanded, key, :unavailable))
    end
  end

  defp window_start(:all, _socket), do: nil

  defp window_start(:today, socket) do
    now = DateTime.utc_now()
    configuration = socket.assigns.current_scope
    Time.beginning_of_day(Time.to_date(now, configuration), configuration)
  end

  defp window_start(:week, _socket), do: DateTime.add(DateTime.utc_now(), -7, :day)
  defp window_start(:month, _socket), do: DateTime.add(DateTime.utc_now(), -30, :day)

  defp schedule_poll(socket) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :poll_ref, :poll_runners, @refresh_interval_ms)
    else
      socket
    end
  end

  defp parse_enum(value, allowed) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> :error
      atom -> {:ok, atom}
    end
  end

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _other -> :error
    end
  end

  defp parse_integer(_value), do: :error

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _other -> :error
    end
  end

  defp parse_datetime(_value), do: :error

  defp parse_optional_datetime(value) when value in [nil, ""], do: {:ok, nil}
  defp parse_optional_datetime(value), do: parse_datetime(value)

  defp get_operator_runner_overview(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :operator_runner_overview_fun,
      &Orchestrator.get_operator_runner_overview/2
    ).(operator_context, opts)
  end

  defp get_operator_runner_session_tasks(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :operator_runner_session_tasks_fun,
      &Orchestrator.get_operator_runner_session_tasks/2
    ).(operator_context, opts)
  end
end
