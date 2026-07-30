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

  attr :row_class, :any,
    default: nil,
    doc: "function returning extra classes for a row, for a selected or deep-linked row"

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
            class={[
              "group border-base-content/10 transition hover:bg-primary/10 focus-within:bg-primary/10",
              @row_class && @row_class.(row)
            ]}
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
  A population split by outcome, as one bar plus its own legend.

  Use this instead of one `metric/1` per outcome. A row of counters makes the
  reader add up the parts to see the whole and shows a card per zero; a meter
  shows the whole first, the proportions without arithmetic, and nothing at all
  for an outcome that did not occur.

  Segments render in the order given, so pass them in escalation order and the
  bar reads left to right the way the operator scans.

      <.outcome_meter
        segments={[
          %{tone: :success, count: 5, label: "succeeded"},
          %{tone: :error, count: 1, label: "failed"},
          %{tone: :info, count: 6, label: "running"}
        ]}
        summary="12 assets"
      />
  """
  attr :segments, :list,
    required: true,
    doc: "maps with `:tone`, `:count`, and `:label`; zero counts are dropped"

  attr :summary, :string, default: nil, doc: "what the whole bar counts, e.g. \"12 assets\""
  attr :size, :atom, default: :md, doc: "`:sm` or `:md`"
  attr :legend?, :boolean, default: true
  attr :class, :any, default: nil
  attr :rest, :global

  def outcome_meter(assigns) do
    present = Enum.filter(assigns.segments, &(segment_count(&1) > 0))
    total = Enum.sum(Enum.map(present, &segment_count(&1)))

    assigns =
      assigns
      |> assign(:present, Enum.map(present, &Map.put(&1, :percent, percent(&1, total))))
      |> assign(:total, total)

    ~H"""
    <div class={["min-w-0", @class]} {@rest}>
      <div
        class={[
          "flex w-full overflow-hidden rounded-full bg-base-content/10",
          (@size == :sm && "h-1.5") || "h-2"
        ]}
        role="img"
        aria-label={meter_label(@present, @summary)}
        data-testid="outcome-meter"
      >
        <span
          :for={segment <- @present}
          class={[Tokens.fill_class(Tokens.tone(segment.tone))]}
          style={"width: #{segment.percent}%"}
          data-tone={segment.tone}
        />
      </div>
      <p
        :if={@legend? and @present != []}
        class="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs favn-text-muted"
      >
        <span :if={@summary} class="font-medium text-base-content">{@summary}</span>
        <span :for={segment <- @present} class="inline-flex items-center gap-1.5">
          <span class={[
            "size-1.5 shrink-0 rounded-full",
            Tokens.fill_class(Tokens.tone(segment.tone))
          ]} />
          {segment.count} {segment.label}
        </span>
      </p>
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

  defp segment_count(segment) do
    case Map.get(segment, :count) do
      count when is_integer(count) and count > 0 -> count
      _absent_or_zero -> 0
    end
  end

  # Widths are floats so a single failure among two hundred assets is still a
  # visible sliver rather than rounding to nothing.
  defp percent(segment, total) when total > 0 do
    max(segment_count(segment) * 100 / total, 0.6)
  end

  defp percent(_segment, _total), do: 0

  defp meter_label([], summary), do: summary || "Nothing to report"

  defp meter_label(segments, summary) do
    parts = Enum.map_join(segments, ", ", &"#{&1.count} #{&1.label}")
    if summary, do: "#{summary}: #{parts}", else: parts
  end

  defp grid_class(1), do: "sm:grid-cols-1"
  defp grid_class(2), do: "sm:grid-cols-2"
  defp grid_class(4), do: "sm:grid-cols-2 lg:grid-cols-4"
  defp grid_class(_columns), do: "sm:grid-cols-3"
end
