defmodule FavnView.RunnersLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Components.RunnersPage
  alias FavnView.LiveRefresh

  @refresh_interval_ms 3_000

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        overview: nil,
        loading: true,
        error: nil,
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
      nav_items={@nav_items}
      flash={@flash}
    />
    """
  end

  @impl true
  def handle_event("reload", _params, socket), do: {:noreply, load_overview(socket)}

  @impl true
  def handle_info({:poll_runners, token}, socket) do
    case LiveRefresh.take(socket, :poll_ref, token) do
      {:ok, socket} -> {:noreply, load_overview(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info(:favn_persistence_published, socket), do: {:noreply, load_overview(socket)}

  defp load_overview(socket) do
    result =
      get_operator_runner_overview(socket.assigns.current_scope.operator_context, limit: 50)

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

  defp schedule_poll(socket) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :poll_ref, :poll_runners, @refresh_interval_ms)
    else
      socket
    end
  end

  defp get_operator_runner_overview(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :operator_runner_overview_fun,
      &FavnOrchestrator.get_operator_runner_overview/2
    ).(operator_context, opts)
  end
end
