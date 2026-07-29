defmodule FavnView.UI.Button do
  @moduledoc """
  Buttons and icon buttons for the Favn operator UI.

  ## Choosing a variant

  | Variant | Use it for | Rule |
  | --- | --- | --- |
  | `:primary` | the single action that advances the current state | at most one per view state |
  | `:solid` | the confirming control in a dialog | at most one per dialog |
  | `:secondary` | supporting actions next to a primary one | any number |
  | `:ghost` | tertiary actions, toolbar controls, dismissals | any number |
  | `:danger` | destructive or irreversible actions | always confirm server-side |
  | `:link` | inline navigation inside prose or table cells | no surface, no padding |

  `:primary` and `:solid` are the only variants that use `--favn-action`, the
  violet that appears nowhere else in the interface — violet because it is the
  hue the status vocabulary leaves free, so the action can never be mistaken
  for a state. Two buttons competing for it in one view state defeats the
  point: the operator should find the next step by colour, without reading.
  `:primary` is outlined and lives on pages; `:solid` is filled and reserved
  for the moment a dialog asks for a decision.

  Every mutating button must be authorised on the server. Rendering a button
  conditionally is presentation, never authorisation.

  ## Anatomy

  A button is a label, optionally preceded by an icon. An action with no label
  is an `icon_button/1`, which requires an accessible `label` and renders a
  tooltip so the meaning stays discoverable.

  Passing `href`, `navigate`, or `patch` renders a link styled as a button;
  otherwise a real `<button>` is rendered.

  ## Examples

      <.button phx-click="run">Run pipeline</.button>
      <.button variant={:secondary} navigate={~p"/runs"}>All runs</.button>
      <.button variant={:danger} phx-click="cancel" loading={@cancelling?}>Cancel run</.button>
      <.icon_button icon="hero-chevron-right" label="Open run" navigate={~p"/runs/1"} />
  """

  use Phoenix.Component

  import FavnView.UI.Icon

  alias FavnView.UI.Tokens

  @variants %{
    primary: "favn-btn-action",
    solid: "favn-btn-action-solid",
    secondary: "favn-btn-supporting",
    ghost: "btn-ghost",
    danger: "favn-btn-destructive",
    link: "btn-link"
  }

  @sizes %{xs: "btn-xs", sm: "btn-sm", md: nil, lg: "btn-lg"}

  attr :variant, :atom,
    default: :primary,
    values: [:primary, :solid, :secondary, :ghost, :danger, :link]

  attr :size, :atom, default: :sm, values: [:xs, :sm, :md, :lg]
  attr :icon, :string, default: nil, doc: "leading heroicon name"
  attr :trailing_icon, :string, default: nil
  attr :loading, :boolean, default: false, doc: "swaps the leading icon for a spinner"
  attr :block, :boolean, default: false, doc: "stretch to the container width"
  attr :class, :any, default: nil

  attr :rest, :global,
    include: ~w(href navigate patch replace method download disabled form name type value)

  slot :inner_block, required: true

  def button(assigns) do
    assigns =
      assigns
      |> assign(:button_class, button_class(assigns))
      |> assign(:icon_size, icon_size(assigns.size))

    if link?(assigns.rest) do
      ~H"""
      <.link class={@button_class} {@rest}>
        <.icon :if={@icon} name={@icon} size={@icon_size} />
        {render_slot(@inner_block)}
        <.icon :if={@trailing_icon} name={@trailing_icon} size={@icon_size} />
      </.link>
      """
    else
      ~H"""
      <button type={@rest[:type] || "button"} class={@button_class} {@rest}>
        <span :if={@loading} class="loading loading-spinner loading-xs" aria-hidden="true"></span>
        <.icon :if={@icon && !@loading} name={@icon} size={@icon_size} />
        {render_slot(@inner_block)}
        <.icon :if={@trailing_icon} name={@trailing_icon} size={@icon_size} />
      </button>
      """
    end
  end

  attr :icon, :string, required: true
  attr :label, :string, required: true, doc: "accessible name, also used as the tooltip"
  attr :variant, :atom, default: :ghost, values: [:ghost, :surface, :danger]
  attr :size, :atom, default: :sm, values: [:xs, :sm, :md, :lg]
  attr :shape, :atom, default: :square, values: [:square, :circle]
  attr :tone, :atom, default: nil, doc: "tints the icon; see `FavnView.UI.Tokens`"
  attr :tooltip, :atom, default: nil, values: [nil, :top, :bottom, :left, :right]
  attr :class, :any, default: nil

  attr :rest, :global,
    include: ~w(href navigate patch replace method download disabled form name type value)

  def icon_button(assigns) do
    assigns =
      assigns
      |> assign(:button_class, icon_button_class(assigns))
      |> assign(:icon_size, icon_size(assigns.size))

    if link?(assigns.rest) do
      ~H"""
      <.link class={@button_class} aria-label={@label} data-tip={@tooltip && @label} {@rest}>
        <.icon name={@icon} size={@icon_size} />
        <span class="sr-only">{@label}</span>
      </.link>
      """
    else
      ~H"""
      <button
        type={@rest[:type] || "button"}
        class={@button_class}
        aria-label={@label}
        data-tip={@tooltip && @label}
        {@rest}
      >
        <.icon name={@icon} size={@icon_size} />
        <span class="sr-only">{@label}</span>
      </button>
      """
    end
  end

  @doc """
  Groups related actions so they read as one control cluster.

  ## Examples

      <.button_group>
        <.button phx-click="run">Run</.button>
        <.button variant={:ghost} phx-click="reset">Reset</.button>
      </.button_group>
  """
  attr :class, :any, default: nil
  attr :align, :atom, default: :start, values: [:start, :end, :between]
  slot :inner_block, required: true

  def button_group(assigns) do
    ~H"""
    <div class={["flex flex-wrap items-center gap-2", align_class(@align), @class]}>
      {render_slot(@inner_block)}
    </div>
    """
  end

  defp align_class(:start), do: "justify-start"
  defp align_class(:end), do: "justify-end"
  defp align_class(:between), do: "justify-between"

  defp link?(rest), do: Enum.any?([:href, :navigate, :patch], &Map.has_key?(rest, &1))

  defp button_class(assigns) do
    [
      "btn",
      Map.fetch!(@variants, assigns.variant),
      Map.fetch!(@sizes, assigns.size),
      assigns.block && "btn-block",
      assigns.loading && "pointer-events-none",
      assigns.class
    ]
  end

  defp icon_button_class(assigns) do
    [
      "btn favn-icon-button",
      icon_button_variant(assigns.variant),
      Map.fetch!(@sizes, assigns.size),
      assigns.shape == :square && "btn-square",
      assigns.shape == :circle && "btn-circle",
      assigns.tone && Tokens.text_class(assigns.tone),
      assigns.tooltip && "tooltip tooltip-#{assigns.tooltip}",
      assigns.class
    ]
  end

  defp icon_button_variant(:ghost), do: "btn-ghost"
  defp icon_button_variant(:danger), do: "btn-ghost text-error"
  defp icon_button_variant(:surface), do: "btn-ghost favn-surface-control rounded-box"

  defp icon_size(:xs), do: :xs
  defp icon_size(:sm), do: :sm
  defp icon_size(_size), do: :md
end
