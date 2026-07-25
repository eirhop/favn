defmodule FavnView.Components.RunDetailPage.NotFound do
  @moduledoc false
  use FavnView, :html
  alias FavnView.Components.GlassPanel

  def not_found_panel(assigns) do
    ~H"""
    <div class="mx-auto w-full max-w-3xl">
      <GlassPanel.glass_panel class="p-8 text-center" data-testid="run-not-found-state">
        <span :if={@run[:initializing?]} class="loading loading-ring loading-lg text-primary"></span>
        <h2 class="text-xl font-medium">
          {if(@run[:initializing?], do: "Loading run", else: @run.error || "Run not found")}
        </h2>
        <p class="mt-2 text-sm text-base-content/60">
          <%= if @run[:initializing?] do %>
            The run was committed and its operator view is becoming available.
          <% else %>
            <%= if @run[:not_found?] do %>
              No persisted run snapshot matches <span class="font-mono">{@run.id}</span>.
            <% else %>
              The persisted run snapshot could not be loaded. Try again later.
            <% end %>
          <% end %>
        </p>
        <.link
          :if={!@run[:initializing?]}
          navigate={~p"/assets"}
          class="btn btn-primary btn-soft mt-6"
        >
          Back to assets
        </.link>
      </GlassPanel.glass_panel>
    </div>
    """
  end
end
