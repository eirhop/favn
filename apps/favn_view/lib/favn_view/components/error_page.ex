defmodule FavnView.Components.ErrorPage do
  @moduledoc """
  A whole-screen failure page, for when the thing a route points at cannot be
  shown at all.

  Use this instead of hand-rolling a shell plus a panel in a LiveView. A route
  that resolves to nothing, or to a backend failure, still needs navigation and
  a way back, so it is a page and not an inline state.

  For a failure *inside* a page that otherwise rendered, use
  `FavnView.UI.State.error_state/1`.

  ## Examples

      <.error_page
        title="Asset not found"
        subtitle={@asset_id}
        description="No active catalogue entry matches this asset id."
        nav_items={@nav_items}
        back_navigate={~p"/assets"}
        back_label="Back to catalogue"
      />
  """

  use FavnView, :html

  alias FavnView.Components.AppShell

  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :description, :string, required: true
  attr :nav_items, :list, default: []
  attr :flash, :map, default: %{}
  attr :tone, :atom, default: :error, values: [:error, :warning, :neutral]
  attr :back_navigate, :string, default: nil
  attr :back_label, :string, default: nil
  attr :rest, :global

  def error_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      nav_items={@nav_items}
      flash={@flash}
    >
      <div class="mx-auto w-full max-w-4xl" {@rest}>
        <.empty_state
          :if={@tone == :neutral}
          title={@title}
          description={@description}
          icon="hero-magnifying-glass"
        >
          <:action>
            <.button :if={@back_navigate} variant={:secondary} navigate={@back_navigate}>
              {@back_label}
            </.button>
          </:action>
        </.empty_state>

        <.error_state
          :if={@tone != :neutral}
          tone={@tone}
          title={@title}
          description={@description}
        >
          <:action>
            <.button :if={@back_navigate} variant={:secondary} navigate={@back_navigate}>
              {@back_label}
            </.button>
          </:action>
        </.error_state>
      </div>
    </AppShell.app_shell>
    """
  end
end
