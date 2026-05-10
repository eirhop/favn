defmodule FavnView.Components.IconNav do
  @moduledoc """
  Icon-only primary navigation for the Favn HUD shell.
  """

  use FavnView, :html

  attr :items, :list, required: true
  attr :class, :any, default: nil

  def icon_nav(assigns) do
    ~H"""
    <aside class={[
      "favn-icon-rail fixed inset-y-0 left-0 z-20 hidden w-24 flex-col items-center justify-between border-y-0 border-l-0 px-4 py-8 md:flex",
      @class
    ]}>
      <a href={~p"/"} class="btn btn-ghost gap-2 px-2 text-xl font-semibold" aria-label="Favn home">
        <.icon name="hero-sparkles" class="size-6" />
        <span class="sr-only">Favn</span>
      </a>

      <nav aria-label="Primary navigation">
        <ul class="menu gap-4 p-0">
          <li :for={item <- @items}>
            <a
              href={item.href}
              class={[
                "tooltip tooltip-right btn btn-ghost btn-square favn-icon-button rounded-box border border-base-content/10 text-base-content/70",
                item[:active] && "bg-primary/15 text-primary favn-status-glow"
              ]}
              data-tip={item.label}
              aria-label={item.label}
              aria-current={item[:active] && "page"}
            >
              <.icon name={item.icon} class="size-5" />
            </a>
          </li>
        </ul>
      </nav>

      <div class="indicator">
        <span class="indicator-item status status-success favn-status-glow"></span>
        <button
          type="button"
          class="avatar avatar-placeholder btn btn-ghost btn-circle favn-icon-button border border-base-content/10"
          aria-label="Current user placeholder"
        >
          <div class="w-12 rounded-full bg-base-200 text-sm text-base-content/80">
            <span>AM</span>
          </div>
        </button>
      </div>
    </aside>
    """
  end

  attr :items, :list, required: true
  attr :class, :any, default: nil
  attr :open, :boolean, default: false

  def mobile_icon_nav(assigns) do
    ~H"""
    <details class={["dropdown md:hidden", @class]} open={@open}>
      <summary
        class="btn btn-ghost btn-square favn-icon-button favn-icon-rail rounded-box [list-style:none] [&::-webkit-details-marker]:hidden"
        aria-label="Open primary navigation"
      >
        <.icon name="hero-bars-3" class="size-5" />
        <span class="sr-only">Open primary navigation</span>
      </summary>

      <nav
        class="dropdown-content favn-icon-rail z-30 mt-3 w-64 rounded-box p-3 shadow-xl"
        aria-label="Mobile primary navigation"
      >
        <ul class="menu gap-1 p-0">
          <li :for={item <- @items}>
            <a
              href={item.href}
              class={[
                "favn-icon-button rounded-field border border-transparent text-base-content/75",
                item[:active] && "border-primary/40 bg-primary/15 text-primary"
              ]}
              aria-current={item[:active] && "page"}
            >
              <.icon name={item.icon} class="size-5" />
              <span>{item.label}</span>
            </a>
          </li>
        </ul>
      </nav>
    </details>
    """
  end
end
