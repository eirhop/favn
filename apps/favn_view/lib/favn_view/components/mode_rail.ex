defmodule FavnView.Components.ModeRail do
  @moduledoc """
  Optional right-side icon rail for page modes.
  """

  use FavnView, :html

  attr :class, :any, default: nil

  slot :item do
    attr :label, :string, required: true
    attr :icon, :string, required: true
    attr :active, :boolean
  end

  def mode_rail(assigns) do
    ~H"""
    <nav
      :if={@item != []}
      class={[
        "favn-icon-rail fixed right-5 top-1/2 z-20 hidden -translate-y-1/2 flex-col items-center gap-3 rounded-box p-3 lg:flex",
        @class
      ]}
      aria-label="View modes"
    >
      <div :for={item <- @item} class="tooltip tooltip-left" data-tip={item.label}>
        <button
          type="button"
          class={[
            "btn btn-ghost btn-square favn-icon-button rounded-box border border-base-content/10 text-base-content/70",
            item[:active] && "bg-primary/15 text-primary favn-status-glow"
          ]}
          aria-label={item.label}
          aria-pressed={item[:active] || false}
        >
          <.icon name={item.icon} class="size-5" />
          <span class="sr-only">{render_slot(item)}</span>
        </button>
      </div>
    </nav>
    """
  end
end
