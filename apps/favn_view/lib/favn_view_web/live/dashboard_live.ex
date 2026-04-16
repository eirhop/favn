defmodule FavnViewWeb.DashboardLive do
  use FavnViewWeb, :live_view

  alias FavnView.Manifests
  alias FavnView.Presenters.ManifestPresenter
  alias FavnView.Presenters.RunPresenter
  alias FavnView.Presenters.SchedulerPresenter
  alias FavnView.Runs
  alias FavnView.Scheduler

  @impl true
  def mount(_params, _session, socket) do
    with {:ok, manifests} <- Manifests.list_manifests(),
         {:ok, runs} <- Runs.list_runs(limit: 20) do
      if connected?(socket), do: _ = Runs.subscribe_runs()

      {:ok,
       socket
       |> assign(:page_title, "Dashboard")
       |> assign(:active_manifest_id, active_manifest_id())
       |> assign(:runs, RunPresenter.summaries(runs))
       |> assign(:scheduler_entries, scheduler_entries())
       |> assign(:asset_options, ManifestPresenter.asset_options(manifests))
       |> assign(:pipeline_options, ManifestPresenter.pipeline_options(manifests))}
    else
      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, "dashboard load failed: #{inspect(reason)}")
         |> assign(:page_title, "Dashboard")
         |> assign(:active_manifest_id, nil)
         |> assign(:runs, [])
         |> assign(:scheduler_entries, [])
         |> assign(:asset_options, [])
         |> assign(:pipeline_options, [])}
    end
  end

  @impl true
  def handle_event("submit_asset", %{"asset_ref" => encoded_ref}, socket) do
    with {:ok, asset_ref} <- ManifestPresenter.decode_term(encoded_ref),
         {:ok, run_id} <- Runs.submit_asset_run(asset_ref) do
      {:noreply,
       socket
       |> put_flash(:info, "asset run submitted: #{run_id}")
       |> push_navigate(to: ~p"/runs/#{run_id}")}
    else
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "asset submit failed: #{inspect(reason)}")}
    end
  end

  def handle_event("submit_pipeline", %{"pipeline" => encoded_pipeline}, socket) do
    with {:ok, pipeline_module} <- ManifestPresenter.decode_term(encoded_pipeline),
         {:ok, run_id} <- Runs.submit_pipeline_run(pipeline_module) do
      {:noreply,
       socket
       |> put_flash(:info, "pipeline run submitted: #{run_id}")
       |> push_navigate(to: ~p"/runs/#{run_id}")}
    else
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "pipeline submit failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def handle_info({:favn_run_event, _event}, socket) do
    {:noreply, refresh_dashboard(socket)}
  end

  defp refresh_dashboard(socket) do
    runs =
      case Runs.list_runs(limit: 20) do
        {:ok, value} -> RunPresenter.summaries(value)
        _ -> socket.assigns.runs
      end

    assign(socket, :runs, runs)
  end

  defp active_manifest_id do
    case Manifests.active_manifest() do
      {:ok, manifest_id} -> manifest_id
      _ -> nil
    end
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
      <h1>Favn Dashboard</h1>
      <p>Active manifest: <strong><%= @active_manifest_id || "none" %></strong></p>

      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin: 1rem 0;">
        <article style="border: 1px solid #ddd; padding: 1rem;">
          <h2>Submit Asset Run</h2>
          <.form id="asset-submit-form" for={%{}} as={:submit_asset} phx-submit="submit_asset">
            <select name="asset_ref">
              <%= for {label, value} <- @asset_options do %>
                <option value={value}><%= label %></option>
              <% end %>
            </select>
            <button type="submit">Submit</button>
          </.form>
        </article>

        <article style="border: 1px solid #ddd; padding: 1rem;">
          <h2>Submit Pipeline Run</h2>
          <.form id="pipeline-submit-form" for={%{}} as={:submit_pipeline} phx-submit="submit_pipeline">
            <select name="pipeline">
              <%= for {label, value} <- @pipeline_options do %>
                <option value={value}><%= label %></option>
              <% end %>
            </select>
            <button type="submit">Submit</button>
          </.form>
        </article>
      </div>

      <article style="border: 1px solid #ddd; padding: 1rem; margin-bottom: 1rem;">
        <h2>Scheduler Entries</h2>
        <p><%= length(@scheduler_entries) %> entries loaded.</p>
      </article>

      <article style="border: 1px solid #ddd; padding: 1rem;">
        <h2>Recent Runs</h2>
        <table style="width: 100%; border-collapse: collapse;">
          <thead>
            <tr>
              <th style="text-align: left; border-bottom: 1px solid #eee;">Run</th>
              <th style="text-align: left; border-bottom: 1px solid #eee;">Status</th>
              <th style="text-align: left; border-bottom: 1px solid #eee;">Kind</th>
            </tr>
          </thead>
          <tbody>
            <%= for run <- @runs do %>
              <tr>
                <td style="padding: 0.4rem 0;">
                  <.link navigate={~p"/runs/#{run.id}"}><%= run.id %></.link>
                </td>
                <td><%= run.status %></td>
                <td><%= run.submit_kind %></td>
              </tr>
            <% end %>
          </tbody>
        </table>
      </article>
    </section>
    """
  end
end
