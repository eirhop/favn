defmodule FavnViewWeb.CoreComponents do
  @moduledoc false

  use Phoenix.Component

  attr(:flash, :map, required: true)

  def flash_group(assigns) do
    ~H"""
    <div>
      <%= for kind <- [:info, :error] do %>
        <%= if msg = Phoenix.Flash.get(@flash, kind) do %>
          <p style="padding: 0.5rem; border: 1px solid #ddd; margin-bottom: 0.5rem;">
            <strong><%= kind %>:</strong> <%= msg %>
          </p>
        <% end %>
      <% end %>
    </div>
    """
  end
end
