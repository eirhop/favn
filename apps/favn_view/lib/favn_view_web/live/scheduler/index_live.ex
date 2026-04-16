defmodule FavnViewWeb.Scheduler.IndexLive do
  use FavnViewWeb, :live_view

  alias FavnView.Presenters.SchedulerPresenter
  alias FavnView.Scheduler

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(:page_title, "Scheduler")
     |> assign(:entries, scheduler_entries())}
  end

  @impl true
  def handle_event("reload", _params, socket) do
    socket =
      case Scheduler.reload() do
        :ok ->
          put_flash(socket, :info, "scheduler reloaded")

        {:error, reason} ->
          put_flash(socket, :error, "scheduler reload failed: #{inspect(reason)}")
      end

    {:noreply, assign(socket, :entries, scheduler_entries())}
  end

  def handle_event("tick", _params, socket) do
    socket =
      case Scheduler.tick() do
        :ok -> put_flash(socket, :info, "scheduler tick complete")
        {:error, reason} -> put_flash(socket, :error, "scheduler tick failed: #{inspect(reason)}")
      end

    {:noreply, assign(socket, :entries, scheduler_entries())}
  end

  defp scheduler_entries do
    case Scheduler.scheduled_entries() do
      entries when is_list(entries) -> SchedulerPresenter.entries(entries)
      _ -> []
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section>
      <h1>Scheduler Inspection</h1>

      <div style="display: flex; gap: 0.6rem; margin-bottom: 1rem;">
        <button phx-click="reload">Reload</button>
        <button phx-click="tick">Tick</button>
      </div>

      <p><strong>Entries:</strong> <%= length(@entries) %></p>

      <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Pipeline</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Schedule</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Cron</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Timezone</th>
          </tr>
        </thead>
        <tbody>
          <%= for entry <- @entries do %>
            <tr>
              <td><%= entry.pipeline_module %></td>
              <td><%= entry.schedule_id %></td>
              <td><%= entry.cron %></td>
              <td><%= entry.timezone %></td>
            </tr>
          <% end %>
        </tbody>
      </table>
    </section>
    """
  end
end
