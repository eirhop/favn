defmodule FavnView.Components.RunDetailPage.NotFound do
  @moduledoc false
  use FavnView, :html
  alias FavnView.Components.GlassPanel

  def not_found_panel(assigns) do
    ~H"""
    <div class="mx-auto w-full max-w-3xl">
      <GlassPanel.glass_panel class="p-8 text-center" data-testid="run-not-found-state">
        <h2 class="text-xl font-medium">{@run.error || "Run not found"}</h2>
        <p class="mt-2 text-sm text-base-content/60">
          No persisted run snapshot matches <span class="font-mono">{@run.id}</span>.
        </p>
        <.link navigate={~p"/assets"} class="btn btn-primary btn-soft mt-6">Back to assets</.link>
      </GlassPanel.glass_panel>
    </div>
    """
  end
end
