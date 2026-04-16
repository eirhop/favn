defmodule FavnViewWeb.Manifests.ShowLive do
  use FavnViewWeb, :live_view

  alias FavnView.Manifests
  alias FavnView.Presenters.ManifestPresenter

  @impl true
  def mount(%{"manifest_version_id" => manifest_version_id}, _session, socket) do
    case Manifests.get_manifest(manifest_version_id) do
      {:ok, version} ->
        detail = ManifestPresenter.detail(version)

        {:ok,
         socket
         |> assign(:page_title, "Manifest #{manifest_version_id}")
         |> assign(:manifest, detail)}

      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, "manifest load failed: #{inspect(reason)}")
         |> assign(:page_title, "Manifest")
         |> assign(:manifest, nil)}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section>
      <h1>Manifest Detail</h1>

      <%= if @manifest do %>
        <p><strong>Version:</strong> <%= @manifest.manifest_version_id %></p>
        <p><strong>Content hash:</strong> <%= @manifest.content_hash %></p>
        <p><strong>Assets:</strong> <%= @manifest.asset_count %></p>
        <p><strong>Pipelines:</strong> <%= @manifest.pipeline_count %></p>
        <p><strong>Schedules:</strong> <%= @manifest.schedule_count %></p>
      <% end %>
    </section>
    """
  end
end
