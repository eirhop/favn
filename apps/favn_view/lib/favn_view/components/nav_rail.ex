defmodule FavnView.Components.NavRail do
  @moduledoc """
  Primary navigation: an icon rail on desktop, a dropdown menu on mobile.

  Both render the same items from `FavnView.Components.Navigation`, so a
  destination can never exist on one viewport and be missing on the other.

  The rail is icon-only. Each item therefore carries a tooltip *and* an
  accessible name; the mobile menu shows the label as text because there is room
  for it.

  ## Examples

      <.nav_rail items={Navigation.items(:runs)} />
      <.nav_menu items={Navigation.items(:runs)} />
  """

  use FavnView, :html

  attr :items, :list, required: true, doc: "see `FavnView.Components.Navigation.items/1`"
  attr :class, :any, default: nil

  def nav_rail(assigns) do
    ~H"""
    <aside class={[
      "favn-surface-rail absolute inset-y-0 left-0 z-20 hidden w-24 flex-col items-center justify-between",
      "border-y-0 border-l-0 px-4 py-8 md:flex",
      @class
    ]}>
      <.link navigate={~p"/"} class="btn btn-ghost px-2" aria-label="Favn home">
        <.icon name="hero-sparkles" size={:md} />
      </.link>

      <nav aria-label="Primary navigation">
        <ul class="menu gap-4 p-0">
          <li :for={item <- @items}>
            <.nav_rail_item item={item} />
          </li>
        </ul>
      </nav>

      <div class="h-10" aria-hidden="true"></div>
    </aside>
    """
  end

  attr :items, :list, required: true
  attr :class, :any, default: nil
  attr :open, :boolean, default: false, doc: "render expanded; used by Storybook"

  def nav_menu(assigns) do
    ~H"""
    <details class={["dropdown md:hidden", @class]} open={@open}>
      <summary
        class="btn btn-ghost btn-square favn-icon-button favn-surface-rail rounded-box [list-style:none] [&::-webkit-details-marker]:hidden"
        aria-label="Open primary navigation"
      >
        <.icon name="hero-bars-3" size={:md} />
        <span class="sr-only">Open primary navigation</span>
      </summary>

      <nav
        class="dropdown-content favn-surface-rail z-30 mt-3 w-64 rounded-box p-3 shadow-xl"
        aria-label="Mobile primary navigation"
      >
        <ul class="menu gap-1 p-0">
          <li :for={item <- @items}>
            <.link
              navigate={item.href}
              class={[
                "favn-icon-button rounded-field border border-transparent favn-text-muted",
                item.active && "border-primary/40 bg-primary/15 text-primary"
              ]}
              aria-current={item.active && "page"}
            >
              <.icon name={item.icon} size={:md} />
              <span>{item.label}</span>
            </.link>
          </li>
        </ul>
      </nav>
    </details>
    """
  end

  @doc """
  One icon-only destination in the rail.

  There is no border on purpose. An icon rail reads as a column of glyphs, and a
  hairline box around each one measured 1.2:1 — visible to the audit and to
  nobody else. The active item is marked by background, colour, and the glow.
  """
  attr :item, :map, required: true

  def nav_rail_item(assigns) do
    ~H"""
    <.link
      navigate={@item.href}
      class={[
        "tooltip tooltip-right btn btn-ghost btn-square favn-icon-button rounded-box",
        "favn-text-muted",
        @item.active && "bg-primary/15 text-primary favn-status-glow"
      ]}
      data-tip={@item.label}
      aria-label={@item.label}
      aria-current={@item.active && "page"}
    >
      <.icon name={@item.icon} size={:md} />
    </.link>
    """
  end
end
