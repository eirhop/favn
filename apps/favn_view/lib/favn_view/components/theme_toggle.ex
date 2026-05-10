defmodule FavnView.Components.ThemeToggle do
  @moduledoc """
  Theme switcher for Favn DaisyUI themes.
  """

  use FavnView, :html

  attr :class, :any, default: nil

  def theme_toggle(assigns) do
    ~H"""
    <div class={["join favn-icon-rail rounded-box p-1", @class]} role="group" aria-label="Theme">
      <button
        type="button"
        class="btn btn-ghost btn-square btn-sm join-item favn-icon-button rounded-field [[data-theme=favn-dark]_&]:btn-active"
        aria-label="Use dark theme"
        phx-click={JS.dispatch("favn:set-theme")}
        data-favn-theme="favn-dark"
      >
        <.icon name="hero-moon" class="size-4" />
      </button>
      <button
        type="button"
        class="btn btn-ghost btn-square btn-sm join-item favn-icon-button rounded-field [[data-theme=favn-light]_&]:btn-active"
        aria-label="Use light theme"
        phx-click={JS.dispatch("favn:set-theme")}
        data-favn-theme="favn-light"
      >
        <.icon name="hero-sun" class="size-4" />
      </button>
    </div>
    """
  end
end
