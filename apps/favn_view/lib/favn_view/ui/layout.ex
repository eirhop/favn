defmodule FavnView.UI.Layout do
  @moduledoc """
  Layout primitives: the only place spacing decisions are made.

  Favn has one spacing scale. Reaching for `space-y-7` or `gap-11` in a page
  means the scale is being bypassed, and the screen will drift out of rhythm
  with every other screen.

  | Step | Gap | Use |
  | --- | --- | --- |
  | `:none` | 0 | children that must touch, such as table rows |
  | `:xs` | 0.375rem | inside a row: badges, icon and label |
  | `:sm` | 0.625rem | between list rows on mobile |
  | `:md` | 0.875rem | between page sections on mobile |
  | `:lg` | 1.25rem | between page sections on desktop |
  | `:xl` | 2rem | between major regions on desktop |

  `stack/1` and `inline/1` accept a responsive gap: pass a single step for a
  fixed gap, or `{mobile, desktop}` to widen from `lg` up. List screens are
  `{:sm, :lg}`; page sections are `{:md, :lg}`.

  ## Examples

      <.stack gap={{:md, :lg}}>
        <.asset_filters filters={@filters} />
        <.asset_table assets={@assets} />
      </.stack>

      <.inline gap={:xs}>
        <.status_badge tone={:success} label="Healthy" />
        <.badge tone={:warning} variant={:outline}>Coverage incomplete</.badge>
      </.inline>

      <.columns count={3}>
        <.metric label="Runs" value={128} />
        <.metric label="Failures" value={3} tone={:error} />
        <.metric label="Coverage" value="93%" tone={:warning} />
      </.columns>
  """

  use Phoenix.Component

  @steps [:none, :xs, :sm, :md, :lg, :xl]

  @stack_gaps %{
    none: "space-y-0",
    xs: "space-y-1.5",
    sm: "space-y-2.5",
    md: "space-y-3.5",
    lg: "space-y-5",
    xl: "space-y-8"
  }

  @stack_gaps_lg %{
    none: "lg:space-y-0",
    xs: "lg:space-y-1.5",
    sm: "lg:space-y-2.5",
    md: "lg:space-y-3.5",
    lg: "lg:space-y-5",
    xl: "lg:space-y-8"
  }

  @inline_gaps %{
    none: "gap-0",
    xs: "gap-1.5",
    sm: "gap-2.5",
    md: "gap-3.5",
    lg: "gap-5",
    xl: "gap-8"
  }

  @inline_gaps_lg %{
    none: "lg:gap-0",
    xs: "lg:gap-1.5",
    sm: "lg:gap-2.5",
    md: "lg:gap-3.5",
    lg: "lg:gap-5",
    xl: "lg:gap-8"
  }

  @doc """
  Returns the spacing steps, smallest first.
  """
  @spec steps() :: [atom()]
  def steps, do: @steps

  @doc """
  Stacks children vertically on the spacing scale.
  """
  attr :gap, :any, default: :md, doc: "a step, or `{mobile_step, desktop_step}`"
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def stack(assigns) do
    assigns = assign(assigns, :gap_class, gap_class(assigns.gap, @stack_gaps, @stack_gaps_lg))

    ~H"""
    <div class={[@gap_class, @class]} {@rest}>{render_slot(@inner_block)}</div>
    """
  end

  @doc """
  Lays children out in a row that wraps, on the spacing scale.
  """
  attr :gap, :any, default: :xs, doc: "a step, or `{mobile_step, desktop_step}`"
  attr :align, :atom, default: :center, values: [:start, :center, :end, :baseline]
  attr :justify, :atom, default: :start, values: [:start, :center, :end, :between]
  attr :wrap, :boolean, default: true
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def inline(assigns) do
    assigns = assign(assigns, :gap_class, gap_class(assigns.gap, @inline_gaps, @inline_gaps_lg))

    ~H"""
    <div
      class={[
        "flex",
        @wrap && "flex-wrap",
        align_class(@align),
        justify_class(@justify),
        @gap_class,
        @class
      ]}
      {@rest}
    >
      {render_slot(@inner_block)}
    </div>
    """
  end

  @doc """
  A responsive grid that collapses to one column on mobile.

  Use it for equal-weight children such as metrics or fact groups. Anything with
  a dominant child is a flex layout, not a grid.
  """
  attr :count, :integer, default: 3, values: [2, 3, 4, 5]
  attr :gap, :any, default: :md
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def columns(assigns) do
    assigns =
      assigns
      |> assign(:gap_class, gap_class(assigns.gap, @inline_gaps, @inline_gaps_lg))
      |> assign(:columns_class, columns_class(assigns.count))

    ~H"""
    <div class={["grid", @columns_class, @gap_class, @class]} {@rest}>
      {render_slot(@inner_block)}
    </div>
    """
  end

  defp gap_class({mobile, desktop}, base, large),
    do: [Map.fetch!(base, mobile), Map.fetch!(large, desktop)]

  defp gap_class(step, base, _large), do: Map.fetch!(base, step)

  defp columns_class(2), do: "sm:grid-cols-2"
  defp columns_class(3), do: "sm:grid-cols-2 lg:grid-cols-3"
  defp columns_class(4), do: "sm:grid-cols-2 lg:grid-cols-4"
  defp columns_class(5), do: "sm:grid-cols-2 xl:grid-cols-5"

  defp align_class(:start), do: "items-start"
  defp align_class(:center), do: "items-center"
  defp align_class(:end), do: "items-end"
  defp align_class(:baseline), do: "items-baseline"

  defp justify_class(:start), do: "justify-start"
  defp justify_class(:center), do: "justify-center"
  defp justify_class(:end), do: "justify-end"
  defp justify_class(:between), do: "justify-between"
end
