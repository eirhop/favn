defmodule FavnViewWeb.Manifests.IndexLive do
  use FavnViewWeb, :live_view

  alias FavnView.Manifests
  alias FavnView.Presenters.ManifestPresenter

  @impl true
  def mount(_params, _session, socket) do
    case Manifests.list_manifests() do
      {:ok, manifests} ->
        active_manifest_id = active_manifest_id()

        {:ok,
         socket
         |> assign(:page_title, "Manifests")
         |> assign(:manifests, ManifestPresenter.summaries(manifests, active_manifest_id))
         |> assign(:active_manifest_id, active_manifest_id)}

      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, "manifest load failed: #{inspect(reason)}")
         |> assign(:page_title, "Manifests")
         |> assign(:manifests, [])
         |> assign(:active_manifest_id, nil)}
    end
  end

  defp active_manifest_id do
    case Manifests.active_manifest() do
      {:ok, manifest_id} -> manifest_id
      _ -> nil
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section>
      <h1>Manifests</h1>
      <p>Active: <strong><%= @active_manifest_id || "none" %></strong></p>

      <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Manifest version</th>
            <th style="text-align: left; border-bottom: 1px solid #eee;">Content hash</th>
          </tr>
        </thead>
        <tbody>
          <%= for manifest <- @manifests do %>
            <tr id={"manifest-#{manifest.manifest_version_id}"}>
              <td style="padding: 0.4rem 0;">
                <.link navigate={~p"/manifests/#{manifest.manifest_version_id}"}>
                  <%= manifest.manifest_version_id %>
                </.link>
                <span :if={manifest.active}> (active)</span>
              </td>
              <td><%= manifest.content_hash %></td>
            </tr>
          <% end %>
        </tbody>
      </table>
    </section>
    """
  end
end
