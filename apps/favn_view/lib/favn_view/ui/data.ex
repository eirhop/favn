defmodule FavnView.UI.Data do
  @moduledoc """
  Components for presenting read-only operational data.

  Favn is a monitoring surface, so most screens are lists of facts. These
  components exist so that a fact list, a table, and a metric look the same on
  every page.

  | Component | Use it for |
  | --- | --- |
  | `fact_list/1` | a handful of labelled values about one thing |
  | `data_table/1` | many rows of the same shape, on desktop |
  | `metric/1` | one number that the operator watches |
  | `mono/1` | ids, fingerprints, and anything copied verbatim |

  A table is a desktop affordance. On mobile the same rows should render as
  `FavnView.UI.Surface.list_card/1`; do not shrink a table to fit a phone.

  ## Examples

      <.fact_list facts={[%{label: "Trigger", value: "Schedule"}]} />

      <.data_table id="runs" rows={@runs} row_navigate={&~p"/runs/\#{&1.id}"}>
        <:col :let={run} label="Run">{run.id}</:col>
        <:col :let={run} label="Status"><.status_badge tone={run.tone} label={run.status} /></:col>
      </.data_table>
  """

  use Phoenix.Component

  import FavnView.UI.Button
  import FavnView.UI.Icon

  alias FavnView.UI.Tokens

  attr :facts, :list,
    required: true,
    doc:
      "maps with `:label` and `:value`, and optional `:tone`, `:title`, and `:mono` " <>
        "for a value that is code — a cron, an id, a hash — where a proportional " <>
        "face runs characters together"

  attr :columns, :integer, default: 3, doc: "columns at the `sm` breakpoint and up"
  attr :class, :any, default: nil
  attr :rest, :global

  def fact_list(assigns) do
    assigns = assign(assigns, :grid_class, grid_class(assigns.columns))

    ~H"""
    <dl class={["grid gap-3 text-xs sm:gap-4", @grid_class, @class]} {@rest}>
      <div :for={fact <- @facts} class="min-w-0">
        <dt class="favn-text-subtle">{fact.label}</dt>
        <dd
          class={[
            "mt-0.5 truncate font-medium",
            fact[:mono] && "font-mono",
            (fact[:tone] && Tokens.text_class(fact[:tone])) || "text-base-content"
          ]}
          title={fact[:title] || to_string(fact.value)}
        >
          {fact.value}
        </dd>
      </div>
    </dl>
    """
  end

  attr :id, :string, required: true
  attr :rows, :list, required: true
  attr :row_id, :any, default: nil, doc: "function returning the DOM id for a row"
  attr :row_navigate, :any, default: nil, doc: "function returning a navigate path for a row"
  attr :row_testid, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  slot :col, required: true do
    attr :label, :string, required: true
    attr :align, :atom, doc: "`:start` (default) or `:end`"
    attr :class, :any
  end

  slot :action, doc: "trailing per-row controls, rendered in a final unlabelled column"

  def data_table(assigns) do
    ~H"""
    <div class={["overflow-x-auto", @class]}>
      <table class="table table-sm text-sm" id={@id}>
        <thead>
          <tr class="border-base-content/10 text-xs favn-text-muted">
            <th :for={col <- @col} class={["font-medium", align_class(col[:align]), col[:class]]}>
              {col.label}
            </th>
            <th :if={@action != [] || @row_navigate} class="sr-only">Actions</th>
          </tr>
        </thead>
        <tbody>
          <tr
            :for={row <- @rows}
            id={@row_id && @row_id.(row)}
            class="group border-base-content/10 transition hover:bg-primary/10 focus-within:bg-primary/10"
            data-testid={@row_testid}
          >
            <td :for={col <- @col} class={["align-middle", align_class(col[:align]), col[:class]]}>
              {render_slot(col, row)}
            </td>
            <td :if={@action != [] || @row_navigate} class="text-right align-middle">
              <div class="flex items-center justify-end gap-1">
                {render_slot(@action, row)}
                <.icon_button
                  :if={@row_navigate}
                  navigate={@row_navigate.(row)}
                  icon="hero-chevron-right"
                  label="Open"
                  shape={:circle}
                />
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  One watched number, with an optional trend or qualifier underneath.
  """
  attr :label, :string, required: true
  attr :value, :any, required: true
  attr :hint, :string, default: nil
  attr :tone, :atom, default: :neutral
  attr :icon, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  def metric(assigns) do
    assigns = assign(assigns, :tone, Tokens.tone(assigns.tone))

    ~H"""
    <div class={["min-w-0", @class]} {@rest}>
      <div class="flex items-center gap-2 text-xs favn-text-subtle">
        <.icon :if={@icon} name={@icon} size={:xs} />
        {@label}
      </div>
      <div class={["mt-1 truncate text-2xl font-light tracking-tight", Tokens.text_class(@tone)]}>
        {@value}
      </div>
      <p :if={@hint} class="mt-0.5 truncate text-xs favn-text-subtle">{@hint}</p>
    </div>
    """
  end

  @doc """
  Monospaced value for ids, fingerprints, and hashes.

  Long values wrap on any character rather than overflowing their container.
  """
  attr :value, :string, required: true
  attr :truncate, :boolean, default: false
  attr :class, :any, default: nil

  def mono(assigns) do
    ~H"""
    <span
      class={["font-mono text-xs", (@truncate && "block truncate") || "break-all", @class]}
      title={@value}
    >
      {@value}
    </span>
    """
  end

  @doc """
  Label and value stacked, for detail panels with many one-line facts.
  """
  attr :label, :string, required: true
  attr :class, :any, default: nil
  slot :inner_block, required: true

  def field_row(assigns) do
    ~H"""
    <div class={["flex flex-col gap-0.5 py-1.5 sm:flex-row sm:items-baseline sm:gap-4", @class]}>
      <span class="shrink-0 text-xs favn-text-subtle sm:w-44">{@label}</span>
      <span class="min-w-0 text-sm favn-text-muted">{render_slot(@inner_block)}</span>
    </div>
    """
  end

  defp align_class(:end), do: "text-right"
  defp align_class(_align), do: nil

  defp grid_class(1), do: "sm:grid-cols-1"
  defp grid_class(2), do: "sm:grid-cols-2"
  defp grid_class(4), do: "sm:grid-cols-2 lg:grid-cols-4"
  defp grid_class(_columns), do: "sm:grid-cols-3"
end
