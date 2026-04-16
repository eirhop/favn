defmodule FavnViewWeb.Layouts do
  use FavnViewWeb, :html

  import FavnViewWeb.CoreComponents

  def root(assigns) do
    ~H"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Favn View</title>
      </head>
      <body>
        <%= @inner_content %>
      </body>
    </html>
    """
  end

  def app(assigns) do
    ~H"""
    <main style="max-width: 1100px; margin: 0 auto; padding: 1rem; font-family: sans-serif;">
      <nav style="display: flex; gap: 1rem; margin-bottom: 1rem;">
        <.link navigate={~p"/"}>Dashboard</.link>
        <.link navigate={~p"/runs"}>Runs</.link>
        <.link navigate={~p"/manifests"}>Manifests</.link>
        <.link navigate={~p"/scheduler"}>Scheduler</.link>
      </nav>

      <.flash_group flash={@flash} />
      <%= @inner_content %>
    </main>
    """
  end
end
