defmodule FavnView.Components.GlassPanel do
  @moduledoc """
  Floating glass container for sparse Favn HUD screens.
  """

  use FavnView, :html

  attr :id, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def glass_panel(assigns) do
    ~H"""
    <section id={@id} class={["glass favn-glass-panel rounded-box", @class]} {@rest}>
      {render_slot(@inner_block)}
    </section>
    """
  end
end
