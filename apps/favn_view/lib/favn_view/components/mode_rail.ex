defmodule FavnView.Components.ModeRail do
  @moduledoc """
  Page-local mode controls that render as a right rail on desktop and a bottom
  dock on mobile.

  A mode carrying `:patch` navigates instead of pushing an event, so the mode
  becomes a real URL that survives a refresh and answers the back button. Modes
  without it stay event-driven through `on_select`.
  """

  use FavnView, :html

  attr :active, :atom, default: nil
  attr :modes, :list, default: []
  attr :on_select, :string, default: nil
  attr :class, :any, default: nil

  slot :item do
    attr :id, :atom
    attr :label, :string, required: true
    attr :icon, :string, required: true
    attr :active, :boolean
    attr :disabled, :boolean
    attr :badge, :string
    attr :patch, :string
  end

  def mode_rail(assigns) do
    assigns = assign(assigns, :mode_items, mode_items(assigns))

    ~H"""
    <nav
      :if={@mode_items != []}
      class={[
        "card glass favn-surface-list absolute right-5 top-1/2 z-20 hidden -translate-y-1/2 flex-col! items-center gap-2 rounded-box p-2 lg:flex",
        @class
      ]}
      aria-label="View modes"
      data-testid="view-mode-rail"
    >
      <div :for={mode <- @mode_items} class="tooltip tooltip-left" data-tip={mode.label}>
        <.mode_button mode={mode} active={mode_active?(mode, @active)} on_select={@on_select} />
      </div>
    </nav>

    <nav
      :if={@mode_items != []}
      class="favn-mobile-mode-dock card glass favn-surface-list absolute inset-x-6 bottom-3 z-30 mx-auto flex flex-row! max-w-md items-center justify-around gap-1 rounded-box p-1.5 pb-[max(0.375rem,env(safe-area-inset-bottom))] lg:hidden"
      aria-label="View modes"
      data-testid="view-mode-dock"
    >
      <.mode_button
        :for={mode <- @mode_items}
        mode={mode}
        active={mode_active?(mode, @active)}
        on_select={@on_select}
        mobile
      />
    </nav>
    """
  end

  attr :mode, :map, required: true
  attr :active, :boolean, required: true
  attr :on_select, :string, default: nil
  attr :mobile, :boolean, default: false

  def mode_button(assigns) do
    assigns =
      assign(assigns, :item_class, [
        "favn-mode-item focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
        assigns.mobile && "favn-mode-dock-item",
        !assigns.mobile && "favn-mode-rail-item",
        assigns.active && "favn-mode-item-active",
        assigns.mode[:disabled] && "btn-disabled opacity-45"
      ])

    ~H"""
    <.link
      :if={@mode[:patch]}
      patch={@mode.patch}
      class={@item_class}
      aria-label={@mode.label}
      aria-current={(@active && "page") || nil}
    >
      <.mode_glyph mode={@mode} />
    </.link>
    <button
      :if={is_nil(@mode[:patch])}
      type="button"
      class={@item_class}
      aria-label={@mode.label}
      aria-pressed={to_string(@active)}
      disabled={@mode[:disabled] || false}
      phx-click={@on_select}
      phx-value-mode={@mode.id}
    >
      <.mode_glyph mode={@mode} />
    </button>
    """
  end

  attr :mode, :map, required: true

  defp mode_glyph(assigns) do
    ~H"""
    <.icon name={@mode.icon} class="size-5" />
    <span :if={@mode[:badge]} class="badge badge-primary badge-xs absolute right-1 top-1">
      {@mode.badge}
    </span>
    <span class="sr-only">{@mode.label}</span>
    """
  end

  defp mode_items(%{modes: modes}) when is_list(modes) and modes != [] do
    Enum.map(modes, fn mode ->
      mode
      |> Map.put_new(:id, String.downcase(mode[:label]))
      |> Map.put_new(:disabled, false)
    end)
  end

  defp mode_items(%{item: items}) do
    Enum.map(items, fn item ->
      %{
        id: item[:id] || String.downcase(item.label),
        label: item.label,
        icon: item.icon,
        active: item[:active] || false,
        disabled: item[:disabled] || false,
        badge: item[:badge],
        patch: item[:patch]
      }
    end)
  end

  defp mode_active?(mode, nil), do: mode[:active] || false
  defp mode_active?(mode, active), do: to_string(mode.id) == to_string(active)
end
