defmodule FavnView.UI.Badge do
  @moduledoc """
  Badges express state in one word, using the tone vocabulary from
  `FavnView.UI.Tokens`.

  ## Choosing a badge

  | Component | Use it for |
  | --- | --- |
  | `status_badge/1` | lifecycle state of the thing the row or panel is about |
  | `badge/1` | any other single-word qualifier: kind, mode, compatibility |
  | `count_badge/1` | a number attached to a label or tab |

  Never place two `status_badge/1` next to each other. If a row needs several
  independent states, the primary lifecycle state is a `status_badge/1` and the
  rest are `badge/1` with `variant={:outline}` so scanning stays possible.

  ## Examples

      <.status_badge tone={:success} label="Healthy" />
      <.badge tone={:warning} variant={:outline}>Coverage incomplete</.badge>
      <.count_badge count={12} label="failures" />
  """

  use Phoenix.Component

  import FavnView.UI.Icon

  alias FavnView.UI.Tokens

  @sizes %{xs: "badge-xs", sm: "badge-sm", md: nil}

  attr :tone, :atom, default: :neutral, doc: "see `FavnView.UI.Tokens`"
  attr :variant, :atom, default: :soft, values: [:soft, :outline, :solid]
  attr :size, :atom, default: :sm, values: [:xs, :sm, :md]
  attr :icon, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def badge(assigns) do
    assigns =
      assigns
      |> assign(:tone, Tokens.tone(assigns.tone))
      |> assign(:size_class, Map.fetch!(@sizes, assigns.size))

    ~H"""
    <span
      class={[
        "badge gap-1.5",
        @size_class,
        variant_class(@variant),
        Tokens.badge_class(@tone),
        @class
      ]}
      {@rest}
    >
      <.icon :if={@icon} name={@icon} size={:xs} />
      {render_slot(@inner_block)}
    </span>
    """
  end

  attr :tone, :atom, required: true, doc: "see `FavnView.UI.Tokens`"
  attr :label, :string, required: true
  attr :size, :atom, default: :sm, values: [:xs, :sm, :md]
  attr :glow, :boolean, default: false, doc: "adds the HUD glow; use for the shell's live status"
  attr :class, :any, default: nil
  attr :rest, :global

  def status_badge(assigns) do
    assigns =
      assigns
      |> assign(:tone, Tokens.tone(assigns.tone))
      |> assign(:size_class, Map.fetch!(@sizes, assigns.size))

    ~H"""
    <span
      class={[
        "badge badge-soft gap-2",
        @size_class,
        Tokens.badge_class(@tone),
        @glow && "favn-status-glow",
        @class
      ]}
      {@rest}
    >
      <span class={["status", status_size_class(@size), Tokens.dot_class(@tone)]}></span>
      {@label}
    </span>
    """
  end

  attr :count, :integer, required: true
  attr :label, :string, default: nil, doc: "screen-reader context for the number"
  attr :tone, :atom, default: :neutral
  attr :class, :any, default: nil

  def count_badge(assigns) do
    assigns = assign(assigns, :tone, Tokens.tone(assigns.tone))

    ~H"""
    <span class={["badge badge-xs badge-soft", Tokens.badge_class(@tone), @class]}>
      {@count}
      <span :if={@label} class="sr-only">{@label}</span>
    </span>
    """
  end

  @doc """
  A bare status dot, for places too dense for a full badge.

  Always pair it with adjacent text; colour alone is not an accessible signal.
  """
  attr :tone, :atom, required: true
  attr :label, :string, required: true, doc: "screen-reader label"
  attr :glow, :boolean, default: false
  attr :class, :any, default: nil

  def status_dot(assigns) do
    assigns = assign(assigns, :tone, Tokens.tone(assigns.tone))

    ~H"""
    <span class={["inline-flex items-center", @class]}>
      <span
        class={["status", Tokens.dot_class(@tone), @glow && "favn-status-glow"]}
        role="img"
        aria-label={@label}
      ></span>
    </span>
    """
  end

  defp variant_class(:soft), do: "badge-soft"
  defp variant_class(:outline), do: "badge-outline"
  defp variant_class(:solid), do: nil

  defp status_size_class(:xs), do: "status-xs"
  defp status_size_class(_size), do: nil
end
