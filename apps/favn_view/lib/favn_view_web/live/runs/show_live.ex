defmodule FavnViewWeb.Runs.ShowLive do
  use FavnViewWeb, :live_view

  alias FavnView.Presenters.RunPresenter
  alias FavnView.Runs

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    if connected?(socket), do: _ = Runs.subscribe_run(run_id)

    with {:ok, run} <- Runs.get_run(run_id),
         {:ok, events} <- Runs.list_run_events(run_id) do
      {:ok,
       socket
       |> assign(:page_title, "Run #{run_id}")
       |> assign(:run, RunPresenter.summary(run))
       |> assign(:events, RunPresenter.timeline(events))
       |> assign(:run_id, run_id)}
    else
      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, "run load failed: #{inspect(reason)}")
         |> assign(:page_title, "Run")
         |> assign(:run, nil)
         |> assign(:events, [])
         |> assign(:run_id, run_id)}
    end
  end

  @impl true
  def handle_event("cancel", _params, socket) do
    reason = %{requested_by: :operator, reason: :manual_cancel_from_view}

    case Runs.cancel_run(socket.assigns.run_id, reason) do
      :ok ->
        {:noreply, put_flash(socket, :info, "cancel requested")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "cancel failed: #{inspect(reason)}")}
    end
  end

  def handle_event("rerun", _params, socket) do
    case Runs.rerun(socket.assigns.run_id) do
      {:ok, rerun_id} ->
        {:noreply,
         socket
         |> put_flash(:info, "rerun submitted: #{rerun_id}")
         |> push_navigate(to: ~p"/runs/#{rerun_id}")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "rerun failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def handle_info({:favn_run_event, event}, socket) do
    presented_event = RunPresenter.timeline_event(event)

    events =
      if Enum.any?(socket.assigns.events, &(&1.sequence == presented_event.sequence)) do
        socket.assigns.events
      else
        socket.assigns.events ++ [presented_event]
      end

    run =
      case Runs.get_run(socket.assigns.run_id) do
        {:ok, value} -> RunPresenter.summary(value)
        _ -> socket.assigns.run
      end

    {:noreply, assign(socket, run: run, events: events)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section>
      <h1>Run <%= @run_id %></h1>

      <%= if @run do %>
        <article style="border: 1px solid #ddd; padding: 1rem; margin-bottom: 1rem;">
          <p><strong>Status:</strong> <%= @run.status %></p>
          <p><strong>Manifest:</strong> <%= @run.manifest_version_id %></p>
          <p><strong>Submit kind:</strong> <%= @run.submit_kind %></p>
          <div style="display: flex; gap: 0.6rem; margin-top: 0.5rem;">
            <button :if={@run.cancel_enabled} phx-click="cancel">Cancel run</button>
            <button :if={@run.rerun_enabled} phx-click="rerun">Rerun</button>
          </div>
        </article>
      <% end %>

      <article style="border: 1px solid #ddd; padding: 1rem;">
        <h2>Timeline</h2>
        <ol>
          <%= for event <- @events do %>
            <li id={"event-#{event.sequence}"}>
              <strong>#<%= event.sequence %></strong>
              <span><%= event.label %></span>
              <span>entity=<%= event.entity %></span>
              <span :if={not is_nil(event.stage)}>stage=<%= event.stage %></span>
              <span>status=<%= event.status %></span>
            </li>
          <% end %>
        </ol>
      </article>
    </section>
    """
  end
end
