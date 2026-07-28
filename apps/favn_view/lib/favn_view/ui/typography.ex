defmodule FavnView.UI.Typography do
  @moduledoc """
  Text components and the Favn type scale.

  Favn uses one scale. Headings are light and low-contrast by design; emphasis
  comes from tone and surface, not from weight.

  | Component | Step | Use it for |
  | --- | --- | --- |
  | `page_title/1` | `:page_title` | the one heading that names the screen |
  | `section_title/1` | `:section_title` | a panel or group heading |
  | `eyebrow/1` | `:eyebrow` | the small label above a value or group |
  | `meta/1` | `:meta` | timestamps, counts, and ids in a row's second line |

  There is exactly one `page_title/1` per screen and the shell renders it. A page
  component should not add another.

  `class/1` exposes the same steps as plain strings, for the few places a
  component cannot be used.

  ## Examples

      <.section_title>Coverage</.section_title>
      <.eyebrow>Target compatibility</.eyebrow>
      <.meta title={@run.started_at}>Started {@run.started_label}</.meta>
  """

  use Phoenix.Component

  @scale %{
    page_title: "text-xl font-light tracking-tight sm:text-2xl lg:text-3xl",
    compact_title: "text-base font-light tracking-tight md:text-lg lg:text-xl",
    section_title: "text-base font-medium tracking-tight",
    eyebrow: "text-xs uppercase tracking-[0.18em] text-base-content/45",
    body: "text-sm text-base-content/80",
    meta: "text-xs text-base-content/55"
  }

  @doc """
  Returns the class string for one step of the type scale.

  ## Examples

      iex> FavnView.UI.Typography.class(:section_title)
      "text-base font-medium tracking-tight"
  """
  @spec class(atom()) :: String.t()
  def class(step), do: Map.fetch!(@scale, step)

  @doc """
  Returns every step of the type scale.
  """
  @spec steps() :: [atom()]
  def steps, do: Map.keys(@scale)

  attr :compact, :boolean, default: false, doc: "the shell's dense header variant"
  attr :class, :any, default: nil
  attr :rest, :global
  slot :inner_block, required: true

  def page_title(assigns) do
    step = if assigns.compact, do: :compact_title, else: :page_title
    assigns = assign(assigns, :scale_class, class(step))

    ~H"""
    <h1 class={["truncate text-base-content", @scale_class, @class]} {@rest}>
      {render_slot(@inner_block)}
    </h1>
    """
  end

  attr :class, :any, default: nil
  attr :rest, :global
  slot :inner_block, required: true

  def section_title(assigns) do
    assigns = assign(assigns, :scale_class, class(:section_title))

    ~H"""
    <h2 class={["truncate text-base-content", @scale_class, @class]} {@rest}>
      {render_slot(@inner_block)}
    </h2>
    """
  end

  attr :class, :any, default: nil
  attr :rest, :global
  slot :inner_block, required: true

  def eyebrow(assigns) do
    assigns = assign(assigns, :scale_class, class(:eyebrow))

    ~H"""
    <p class={[@scale_class, @class]} {@rest}>{render_slot(@inner_block)}</p>
    """
  end

  attr :class, :any, default: nil
  attr :title, :string, default: nil, doc: "hover text, for values that may truncate"
  attr :rest, :global
  slot :inner_block, required: true

  def meta(assigns) do
    assigns = assign(assigns, :scale_class, class(:meta))

    ~H"""
    <p class={["truncate", @scale_class, @class]} title={@title} {@rest}>
      {render_slot(@inner_block)}
    </p>
    """
  end
end
