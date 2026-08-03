defmodule FavnView.UI.Surface do
  @moduledoc """
  The four Favn surfaces, as components.

  Favn has exactly four glass levels, defined in `assets/css/app.css`. Picking a
  surface is picking what the thing *is*, not how strong it should look:

  | Component | CSS class | Use it for |
  | --- | --- | --- |
  | `panel/1` | `favn-surface-panel` | page sections and large content containers |
  | `list_card/1` | `favn-surface-list` | repeated rows where scan density matters |
  | `control_surface/1` | `favn-surface-control` | search fields, selects, compact controls |
  | `rail/1` | `favn-surface-rail` | nav rails, page mode rails, mobile docks |

  Do not hand-roll `border`/`background`/`shadow` utility stacks for a container.
  If a surface is missing a variant you need, add the variant here.

  ## Examples

      <.panel>
        <:header title="Coverage" subtitle="Last 30 days" />
        <.fact_list facts={@facts} />
      </.panel>

      <.list_card navigate={~p"/runs/\#{@run.id}"}>
        <div class="min-w-0">{@run.id}</div>
      </.list_card>
  """

  use Phoenix.Component

  import FavnView.UI.Icon

  @paddings %{none: nil, sm: "p-4", md: "p-5 sm:p-6", lg: "p-8"}

  attr :id, :string, default: nil
  attr :padding, :atom, default: :md, values: [:none, :sm, :md, :lg]
  attr :class, :any, default: nil
  attr :rest, :global

  slot :header, doc: "optional panel header" do
    attr :title, :string, required: true
    attr :subtitle, :string
    attr :icon, :string
  end

  slot :actions, doc: "actions aligned to the end of the header row"
  slot :footer
  slot :inner_block, required: true

  def panel(assigns) do
    assigns = assign(assigns, :padding_class, Map.fetch!(@paddings, assigns.padding))

    ~H"""
    <section id={@id} class={["favn-surface-panel rounded-box", @padding_class, @class]} {@rest}>
      <div
        :if={@header != [] || @actions != []}
        class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between"
      >
        <div :for={header <- @header} class="flex min-w-0 items-start gap-3">
          <span
            :if={header[:icon]}
            class="flex size-9 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary"
          >
            <.icon name={header[:icon]} size={:md} />
          </span>

          <div class="min-w-0">
            <h2 class="truncate text-base font-medium tracking-tight text-base-content">
              {header.title}
            </h2>

            <p :if={header[:subtitle]} class="mt-0.5 truncate text-sm favn-text-muted">
              {header[:subtitle]}
            </p>
          </div>
        </div>

        <div :if={@actions != []} class="flex shrink-0 items-center gap-2">
          {render_slot(@actions)}
        </div>
      </div>
      {render_slot(@inner_block)}
      <div :if={@footer != []} class="mt-4 border-t border-base-content/10 pt-4">
        {render_slot(@footer)}
      </div>
    </section>
    """
  end

  @doc """
  A single row in a repeated list.

  Pass `navigate`, `patch`, or `href` to make the whole row a link, or
  `phx-click` to make it a button. A row with neither is inert content.
  """
  attr :id, :string, default: nil
  attr :selected, :boolean, default: false
  attr :class, :any, default: nil
  attr :rest, :global, include: ~w(href navigate patch replace disabled)

  slot :inner_block, required: true

  def list_card(assigns) do
    assigns =
      assigns
      |> assign(:card_class, list_card_class(assigns))
      |> assign(:current, assigns.selected && "true")

    if link?(assigns.rest) do
      ~H"""
      <.link id={@id} class={@card_class} aria-current={@current} {@rest}>
        {render_slot(@inner_block)}
      </.link>
      """
    else
      ~H"""
      <div id={@id} class={@card_class} aria-current={@current} {@rest}>
        {render_slot(@inner_block)}
      </div>
      """
    end
  end

  @doc """
  Wraps a compact control such as a search field or select.

  Prefer `FavnView.UI.Field` components, which already use this surface. Reach
  for `control_surface/1` only for a control the field module does not cover.
  """
  attr :size, :atom, default: :md, values: [:md, :lg]
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def control_surface(assigns) do
    ~H"""
    <div
      class={[
        "favn-surface-control rounded-field",
        @size == :lg && "favn-surface-control-lg",
        @class
      ]}
      {@rest}
    >
      {render_slot(@inner_block)}
    </div>
    """
  end

  @doc """
  Chrome surface for nav rails, mode rails, and mobile docks.

  A rail is one continuous rounded card. Do not style its items as disconnected
  buttons or sharp segmented tabs.
  """
  attr :label, :string, required: true, doc: "accessible name for the navigation region"
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def rail(assigns) do
    ~H"""
    <nav class={["favn-surface-rail rounded-box", @class]} aria-label={@label} {@rest}>
      {render_slot(@inner_block)}
    </nav>
    """
  end

  @doc """
  A divider that carries a label, used to break long panels into groups.
  """
  attr :label, :string, required: true
  attr :class, :any, default: nil

  def surface_divider(assigns) do
    ~H"""
    <div class={["flex items-center gap-3 py-3", @class]}>
      <span class="text-sm uppercase tracking-[0.18em] favn-text-subtle">{@label}</span>
      <span class="h-px flex-1 bg-base-content/10"></span>
    </div>
    """
  end

  defp link?(rest), do: Enum.any?([:href, :navigate, :patch], &Map.has_key?(rest, &1))

  # The selected border is opaque, not a tint. At 50% alpha over a translucent
  # card it measured 2.47:1 against the 3:1 a state cue has to reach, which in
  # practice meant an operator could not tell which row was selected.
  defp list_card_class(assigns) do
    [
      "favn-surface-list favn-density-list-card block rounded-box transition",
      link?(assigns.rest) &&
        "hover:border-primary/40 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary",
      assigns.selected && "favn-list-card-selected",
      assigns.class
    ]
  end
end
