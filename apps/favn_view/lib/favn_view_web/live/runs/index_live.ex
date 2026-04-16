defmodule FavnViewWeb.Runs.IndexLive do
  use FavnViewWeb, :live_view

  alias FavnView.Presenters.RunPresenter
  alias FavnView.Runs

  @impl true
  def mount(_params, _session, socket) do
    runs =
      case Runs.list_runs(limit: 100) do
        {:ok, value} -> RunPresenter.summaries(value)
        _ -> []
      end

    if connected?(socket), do: _ = Runs.subscribe_runs()

    {:ok, assign(socket, page_title: "Runs", runs: runs)}
  end

  @impl true
  def handle_info({:favn_run_event, event}, socket) do
    runs =
      case Runs.get_run(event.run_id) do
        {:ok, run} -> upsert_run(socket.assigns.runs, RunPresenter.summary(run))
        _ -> socket.assigns.runs
      end

    {:noreply, assign(socket, :runs, runs)}
  end

  defp upsert_run(runs, run) do
    runs
    |> Enum.reject(&(&1.id == run.id))
    |> List.insert_at(0, run)
    |> Enum.take(100)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section>
      <h1>Runs</h1>
      <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Run</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Status</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Manifest</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Kind</th>
          </tr>
        </thead>
        <tbody>
          <%= for run <- @runs do %>
            <tr id={"run-#{run.id}"}>
              <td style="padding: 0.4rem 0;"><.link navigate={~p"/runs/#{run.id}"}><%= run.id %></.link></td>
              <td><%= run.status %></td>
              <td><%= run.manifest_version_id %></td>
              <td><%= run.submit_kind %></td>
            </tr>
          <% end %>
        </tbody>
      </table>
    </section>
    """
  end
end
