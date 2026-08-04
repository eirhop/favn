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
  | `stacked_cell/1` | a table cell whose value needs a qualifier under it |
  | `metric/1` | one number that the operator watches |
  | `mono/1` | ids, fingerprints, and anything copied verbatim |

  A table is a desktop affordance. On mobile the same rows should render as
  `FavnView.UI.Surface.list_card/1`; do not shrink a table to fit a phone.

  ## The list-screen standard

  `/runs` is the reference: a compact table whose header stays put while the rows
  scroll, two-line cells so a row answers a question without a second column, and
  one trailing chevron per row. `data_table/1` carries that standard, so a list
  screen adopts it by passing `fill?` and letting the panel bound the height —
  not by restyling a table of its own.

  `fill?` and the sticky header work together. Sticky positioning needs a
  scrolling ancestor with a bounded height, so `fill?` makes the table's own
  wrapper that ancestor: put it in a `flex min-h-0 flex-1` panel and the header
  pins to the top of the rows rather than to the page.

  Two more rules hold on every list screen, and both are the element's rather
  than a caller's because the screens that skipped them each looked reasonable in
  isolation: `desktop_only?` on the table, so its rows and the cards below `lg`
  are never both showing, and the filters as a narrow-screen disclosure in
  `table_toolbar/1`, so the scope rail and the rows keep the width a phone has.
  `test/favn_view/components/list_screen_standard_test.exs` asserts both for all
  of them at once.

  ## Examples

      <.fact_list facts={[%{label: "Trigger", value: "Schedule"}]} />

      <.data_table id="runs" rows={@runs} row_navigate={&~p"/runs/\#{&1.id}"} fill?>
        <:col :let={run} label="Run" class="w-64">
          <.stacked_cell primary={run.short_id} secondary={run.trigger} mono={:primary} />
        </:col>
        <:col :let={run} label="Status"><.status_badge tone={run.tone} label={run.status} /></:col>
      </.data_table>
  """

  use Phoenix.Component

  import FavnView.UI.Badge
  import FavnView.UI.Button
  import FavnView.UI.Field, only: [search_field: 1]
  import FavnView.UI.Icon
  import FavnView.UI.State, only: [empty_state: 1]
  import FavnView.UI.Surface

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
    <dl class={["grid gap-3 text-sm sm:gap-4", @grid_class, @class]} {@rest}>
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

  @doc """
  The standard list-screen table.

  Carries no size modifier, because DaisyUI's bare `.table` is already the Favn
  body size — 0.875rem, with `table-md` a synonym for it. Each modifier sets a
  font size *and* its matching cell padding as one pair with no variable between
  them, so the size class is the whole decision: `table-sm` drops rows to
  0.75rem, and its `:not(thead, tfoot) tr` selector outranks a `text-sm`
  utility in the same layer, so adding one back does nothing.

  A table is not the right shape for a phone, so a list screen pairs this with
  `card_list/1` and sets `desktop_only?`. That flag lives here rather than in a
  `class` a caller passes, because a screen that forgot it rendered every row
  twice on a phone — once as a row and once as a card — and nothing about the
  markup said which of the two was supposed to be showing.
  """
  attr :id, :string, required: true
  attr :rows, :list, required: true
  attr :row_id, :any, default: nil, doc: "function returning the DOM id for a row"
  attr :row_navigate, :any, default: nil, doc: "function returning a navigate path for a row"
  attr :row_testid, :string, default: nil

  attr :row_class, :any,
    default: nil,
    doc: "function returning extra classes for a row, for a selected or deep-linked row"

  attr :fill?, :boolean,
    default: false,
    doc:
      "make the wrapper the scroll region, for a table inside a `flex min-h-0 flex-1` panel; " <>
        "required for the sticky header to pin to the rows instead of the page"

  attr :sticky_header?, :boolean,
    default: true,
    doc: "set false only where the header would pin against a second one"

  attr :desktop_only?, :boolean,
    default: false,
    doc:
      "hide below `lg`, where a `card_list/1` renders the same rows instead; every list " <>
        "screen sets it, and a table without a card alternative does not"

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
    <div class={[
      (@fill? && "min-h-0 flex-1 overflow-auto") || "overflow-x-auto",
      @desktop_only? && "hidden lg:block",
      @class
    ]}>
      <table class="table w-full" id={@id}>
        <thead class={@sticky_header? && "sticky top-0 z-10 bg-base-100/85 backdrop-blur"}>
          <tr class="border-base-content/10 favn-text-muted">
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
  The panel a list screen's table lives in.

  A sticky header needs a scrolling ancestor of bounded height, and getting that
  wrong shows up as a header pinned to the wrong edge or a page with two
  scrollbars. The recipe is four utilities deep and every list screen needs the
  same one, so it lives here rather than in each page: this is the only place the
  bounding is written down.

  The panel also owns the footer, so how many rows a list is showing is reported
  in the same corner every time — trailing edge, below the rows it counts, where
  a reader looks after reading them rather than among the controls that produced
  them.

  Pair it with `data_table/1` and `fill?`. Below `lg` the table is not the right
  shape for the viewport, so a screen renders `FavnView.UI.Surface.list_card/1`
  rows instead; pass `desktop_only?` when those cards live outside this panel.

      <.table_panel desktop_only?>
        <.data_table id="pipelines" rows={@pipelines} fill?>
          <:col :let={pipeline} label="Pipeline">{pipeline.name}</:col>
        </.data_table>
      </.table_panel>
  """
  attr :desktop_only?, :boolean,
    default: false,
    doc: "hide below `lg`, for a screen whose cards render outside this panel"

  attr :count, :integer,
    default: nil,
    doc: "how many rows are listed; rendered in the footer's trailing corner"

  attr :count_label, :string, default: "rows", doc: "what the count counts, pluralised"

  attr :class, :any, default: nil
  attr :rest, :global
  slot :toolbar, doc: "filters or scope controls that scroll with the panel rather than the rows"

  slot :footer,
    doc: "paging or other controls for the whole list, at the footer's leading edge"

  slot :inner_block, required: true

  def table_panel(assigns) do
    ~H"""
    <.panel
      padding={:none}
      class={[
        (@desktop_only? && "hidden lg:flex") || "flex",
        "flex-col lg:min-h-0 lg:flex-1 lg:overflow-hidden",
        @class
      ]}
      {@rest}
    >
      <div :if={@toolbar != []} class="shrink-0">{render_slot(@toolbar)}</div>
      {render_slot(@inner_block)}

      <div
        :if={@count || @footer != []}
        class="flex shrink-0 flex-wrap items-center justify-between gap-3 border-t border-base-content/10 p-3"
      >
        <div class="flex min-w-0 flex-wrap items-center gap-2">{render_slot(@footer)}</div>

        <p :if={@count} class="shrink-0 text-sm favn-text-subtle" data-testid="table-count">
          {@count} {@count_label}
        </p>
      </div>
    </.panel>
    """
  end

  @doc """
  The search and narrowing controls above a list screen's rows.

  Placement is part of the standard, not a page's choice. The controls sit inside
  `table_panel/1` above the rows, separated by one rule, so the operator finds
  them in the same place on every list — and so they scroll with the panel while
  the table header pins to the rows beneath them.

  What goes where: `scopes` is a rail of buttons for the one axis that carries
  counts, at the start; `filters` is the search and the selects, at the end;
  `meta` is a result count. A screen uses the shared `search_field/1` and
  `select_field/1` inside `filters` rather than dressing its own inputs, which is
  what made `/schedules` look like a different application from `/runs`.

  Passing `on_change` renders the form, so filtering works with and without
  JavaScript without each page wiring its own.

  ## Below `lg` the filters are always a disclosure

  Not a per-screen choice. A phone has room for the scope rail or the filters,
  not both, so the filters collapse behind one control and the scopes — whose
  counts are the answer rather than a way to look for it — stay visible. A screen
  therefore assigns `filters_open?` and handles `on_toggle`, which defaults to
  `"toggle_filters"`. The collapsed control carries a dot while anything is set,
  so a filter is never in force with no sign of it.

      <.table_toolbar
        on_change="filter_schedules"
        filters_id="schedule-filters"
        filters_open?={@filters_open?}
        search_name="filters[search]"
        search_label="Search schedules"
        search_value={@filters["search"]}
        on_clear="clear_filters"
        adjusted?={@narrowed?}
      >
        <:scopes><.scope_rail label="Schedule state" choices={@scope_choices} /></:scopes>
        <:filters>
          <.select_field name="filters[window]" label="Window" options={@windows} value={@window} />
        </:filters>
      </.table_toolbar>

  ## The order is fixed

  Reading left to right: the scope rail, then the narrowing controls, then the
  search at the far edge. Search is last because it is the control an operator
  returns to most and the only one whose position should never move; the selects
  a screen happens to need sit between them. How many rows resulted is not a
  control and does not belong here — `table_panel/1` reports it in the footer.

  That order is this component's, not a caller's — the search is an attr rather
  than a slot precisely so a screen cannot place it somewhere else, and the
  selects share one width so a row of them lines up across screens.
  """
  attr :on_change, :string, default: nil, doc: "LiveView event; renders the form around `filters`"
  attr :filters_id, :string, default: nil

  attr :on_toggle, :string,
    default: "toggle_filters",
    doc: "event that opens and closes the filters below `lg`; every list screen handles it"

  attr :filters_open?, :boolean,
    default: false,
    doc: "whether the narrow-screen disclosure is open; `lg` and up ignores it"

  attr :search_name, :string, default: nil, doc: "form field name; renders the search when set"
  attr :search_label, :string, default: "Search", doc: "screen-reader name and placeholder"
  attr :search_placeholder, :string, default: nil
  attr :search_value, :any, default: nil

  attr :on_clear, :string,
    default: nil,
    doc: "event that resets every filter; the control appears only once something is set"

  attr :adjusted?, :boolean,
    default: false,
    doc: "whether anything narrows the list; shows the clear control and marks the collapsed one"

  attr :class, :any, default: nil
  attr :rest, :global

  slot :scopes, doc: "a rail of buttons for the axis that carries counts"
  slot :filters, doc: "the selects this screen narrows by; use `select_field/1`"

  def table_toolbar(assigns) do
    ~H"""
    <div class={["border-b border-base-content/10 p-3 sm:p-4", @class]} {@rest}>
      <div class="flex flex-wrap items-center gap-2 sm:gap-3 lg:flex-nowrap">
        <div :if={@scopes != []} class="w-full sm:w-auto">{render_slot(@scopes)}</div>

        <.button
          :if={@search_name || @filters != []}
          variant={:ghost}
          icon="hero-adjustments-horizontal"
          class="ml-auto shrink-0 lg:hidden"
          phx-click={@on_toggle}
          aria-expanded={to_string(@filters_open?)}
          aria-controls={@filters_id}
          data-testid="toggle-table-filters"
        >
          Filters
          <span :if={@adjusted?} class="size-1.5 rounded-full bg-primary" aria-hidden="true" />
        </.button>

        <form
          :if={@search_name || @filters != []}
          id={@filters_id}
          phx-change={@on_change}
          phx-submit={@on_change}
          class={[
            "items-stretch gap-2 sm:flex-row sm:flex-wrap sm:items-center lg:ml-auto",
            filters_class(assigns)
          ]}
          data-testid="table-filters"
        >
          <.button
            :if={@on_clear && @adjusted?}
            type="button"
            variant={:ghost}
            icon="hero-x-mark"
            class="h-9 min-h-9 shrink-0"
            phx-click={@on_clear}
            data-testid="clear-table-filters"
          >
            Clear
          </.button>

          <div
            :if={@filters != []}
            class={
              [
                "contents sm:flex sm:flex-wrap sm:items-center sm:gap-2",
                # One width for every select, so a row of them lines up across
                # screens. Wrapping is left to flexbox rather than forced off at
                # `lg`: it only breaks a line that is genuinely full, and three
                # selects beside a scope rail and a search field overflowed a
                # 1024px viewport while `nowrap` held them on one line.
                "sm:[&>label]:w-44"
              ]
            }
          >
            {render_slot(@filters)}
          </div>

          <.search_field
            :if={@search_name}
            name={@search_name}
            label={@search_label}
            placeholder={@search_placeholder}
            value={@search_value}
            class="min-w-0 sm:w-64"
          />
        </form>
      </div>
    </div>
    """
  end

  @doc """
  The one axis of a list screen that carries counts, as a rail of buttons.

  A select hides its options and tells you nothing until you open it. For the
  question a list screen exists to answer — what is failing, what is disabled,
  what has gaps — the count beside each choice *is* the answer, so it belongs in
  a rail where every count is visible at once and one click narrows to it.

  Every choice is a map with `:id`, `:label`, `:icon`, `:tone`, `:active?`, and
  a `:count` (`nil` when it could not be read). Counts must come from the whole
  collection rather than the filtered page, or a button reports the list it is
  already showing instead of the list clicking it would produce.

  A rail either patches the URL, with a `:patch` path per choice, or pushes
  `on_select` with the choice id as `scope`.

      <.scope_rail label="Run status" choices={@choices} />
      <.scope_rail label="Asset state" choices={@choices} on_select="set_scope" />
  """
  attr :label, :string, required: true, doc: "accessible name for the rail"
  attr :choices, :list, required: true
  attr :on_select, :string, default: nil, doc: "event name; omit for a patching rail"
  attr :class, :any, default: nil
  attr :rest, :global

  def scope_rail(assigns) do
    ~H"""
    <nav
      class={[
        "favn-surface-rail grid w-full grid-cols-2 gap-0.5 rounded-box p-1",
        "sm:flex sm:w-auto sm:flex-wrap sm:items-center",
        @class
      ]}
      aria-label={@label}
      {@rest}
    >
      <.scope_button :for={choice <- @choices} choice={choice} on_select={@on_select} />
    </nav>
    """
  end

  @doc """
  One value of a `scope_rail/1`, carrying how many rows it would list.

  The attr is a whole choice rather than a status, because `status` means a tone
  everywhere else in this library.
  """
  attr :choice, :map, required: true
  attr :on_select, :string, default: nil

  def scope_button(assigns) do
    assigns = assign(assigns, :classes, scope_button_classes(assigns.choice))

    ~H"""
    <.link
      :if={!@on_select}
      patch={@choice.patch}
      class={@classes}
      title={@choice[:hint]}
      aria-current={@choice.active? && "page"}
      data-testid={"scope-#{@choice.id}"}
    >
      <.scope_button_content choice={@choice} />
    </.link>

    <button
      :if={@on_select}
      type="button"
      phx-click={@on_select}
      phx-value-scope={@choice.id}
      class={@classes}
      title={@choice[:hint]}
      aria-pressed={to_string(@choice.active?)}
      data-testid={"scope-#{@choice.id}"}
    >
      <.scope_button_content choice={@choice} />
    </button>
    """
  end

  attr :choice, :map, required: true

  defp scope_button_content(assigns) do
    ~H"""
    <.icon name={@choice.icon} size={:md} class={Tokens.text_class(Tokens.tone(@choice.tone))} />
    <span class="whitespace-nowrap">{@choice.label}</span>
    <.count_badge
      :if={is_integer(@choice.count)}
      count={@choice.count}
      label={@choice[:count_label] || "rows"}
      tone={count_tone(@choice)}
    />
    """
  end

  @doc """
  A table cell that carries a value and the qualifier that makes it legible.

  A run id means little without its trigger, a schedule name little without its
  id, an asset name little without its namespace. Each list screen used to solve
  that with either a second column — which widens the table past the viewport —
  or its own two-line markup, which drifted. This is that markup, once.

  Both lines truncate rather than wrap, so a long value cannot change the row
  height, and the full value stays reachable as a tooltip.

      <.stacked_cell primary={@schedule.label} secondary={@schedule.id} mono={:secondary} />
  """
  attr :primary, :string, required: true
  attr :secondary, :string, default: nil
  attr :title, :string, default: nil, doc: "tooltip for the primary line; defaults to its text"
  attr :secondary_title, :string, default: nil
  attr :navigate, :any, default: nil, doc: "makes the primary line a link"

  attr :mono, :atom,
    default: :none,
    values: [:none, :primary, :secondary, :both],
    doc: "which lines are code — an id, a cron, a hash — rather than prose"

  attr :tone, :atom, default: nil, doc: "tone for the primary line; defaults to full contrast"
  attr :class, :any, default: nil
  attr :rest, :global

  def stacked_cell(assigns) do
    ~H"""
    <div class={["min-w-0", @class]} {@rest}>
      <.link
        :if={@navigate}
        navigate={@navigate}
        class={[
          "block truncate font-medium hover:text-primary",
          mono?(@mono, :primary) && "font-mono text-sm",
          (@tone && Tokens.text_class(Tokens.tone(@tone))) || "text-base-content"
        ]}
        title={@title || @primary}
      >
        {@primary}
      </.link>

      <p
        :if={!@navigate}
        class={[
          "truncate font-medium",
          mono?(@mono, :primary) && "font-mono text-sm",
          (@tone && Tokens.text_class(Tokens.tone(@tone))) || "text-base-content"
        ]}
        title={@title || @primary}
      >
        {@primary}
      </p>

      <p
        :if={@secondary}
        class={[
          "truncate text-sm favn-text-subtle",
          mono?(@mono, :secondary) && "font-mono"
        ]}
        title={@secondary_title || @secondary}
      >
        {@secondary}
      </p>
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
      <div class="flex items-center gap-2 text-sm favn-text-subtle">
        <.icon :if={@icon} name={@icon} size={:xs} /> {@label}
      </div>

      <div class={["mt-1 truncate text-2xl font-light tracking-tight", Tokens.text_class(@tone)]}>
        {@value}
      </div>

      <p :if={@hint} class="mt-0.5 truncate text-sm favn-text-subtle">{@hint}</p>
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
        class="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm favn-text-muted"
      >
        <span :if={@summary} class="font-medium text-base-content">{@summary}</span>
        <span :for={segment <- @present} class="inline-flex items-center gap-1.5">
          <span class={[
            "size-1.5 shrink-0 rounded-full",
            Tokens.fill_class(Tokens.tone(segment.tone))
          ]} /> {segment.count} {segment.label}
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
      class={["font-mono text-sm", (@truncate && "block truncate") || "break-all", @class]}
      title={@value}
    >
      {@value}
    </span>
    """
  end

  @doc """
  One node, what flows into it, and what flows out, wired together.

  The connectors are real: each input is joined to the centre and the centre to each
  output, drawn as elbows in an SVG that spans the gap between the columns. Two lists
  either side of a label are not a graph — the reader has to infer the edges — and
  inferring them is the whole job this component exists to do.

  The SVG uses a `0 0 100 100` box with `preserveAspectRatio="none"`, so it stretches
  to whatever the gap is. Every segment is axis-aligned, which stretches without
  distorting, and `vector-effect` keeps the stroke one weight in both directions.
  Each list is spaced with `justify-around`, so node `i` of `n` sits at
  `(i + 0.5) / n` of the height and the paths can be computed without measuring
  anything in the browser.

  Below `md` the columns stack and the connectors are replaced by a single arrow,
  because a wiring diagram three nodes wide does not fit a phone.

      <.lineage_graph
        centre={%{label: "customer_orders_daily", icon: "hero-table-cells"}}
        inputs={[%{label: "stg_orders", navigate: ~p"/assets/stg_orders"}]}
        outputs={[]}
        outputs_empty="Nothing reads this yet."
      />
  """
  attr :centre, :map, required: true, doc: "maps with `:label` and optional `:icon`"

  attr :inputs, :list,
    default: [],
    doc: "maps with `:label`, and optional `:icon`, `:navigate`, `:note`, `:title`"

  attr :outputs, :list, default: [], doc: "same shape as `inputs`"
  attr :inputs_label, :string, default: "Reads from"
  attr :outputs_label, :string, default: "Feeds"
  attr :inputs_empty, :string, default: "Nothing."
  attr :outputs_empty, :string, default: "Nothing."
  attr :class, :any, default: nil
  attr :rest, :global

  def lineage_graph(assigns) do
    ~H"""
    <div class={["flex flex-col gap-3 md:flex-row md:items-stretch md:gap-0", @class]} {@rest}>
      <.lineage_column label={@inputs_label} nodes={@inputs} empty={@inputs_empty} />

      <.lineage_wires count={length(@inputs)} direction={:in} />
      <.lineage_stack_arrow />

      <div class="flex shrink-0 flex-col md:px-1">
        <.lineage_heading />

        <div class="flex flex-1 items-center justify-center">
          <div
            class="flex min-w-0 items-center gap-2 rounded-box border border-primary/40 bg-primary/10 px-3 py-2"
            data-testid="lineage-centre"
          >
            <.icon
              :if={@centre[:icon]}
              name={@centre.icon}
              size={:sm}
              class="shrink-0 text-primary"
            />
            <span class="truncate font-medium text-primary" title={@centre.label}>
              {@centre.label}
            </span>
          </div>
        </div>
      </div>

      <.lineage_stack_arrow />
      <.lineage_wires count={length(@outputs)} direction={:out} />

      <.lineage_column label={@outputs_label} nodes={@outputs} empty={@outputs_empty} />
    </div>
    """
  end

  attr :label, :string, required: true
  attr :nodes, :list, required: true
  attr :empty, :string, required: true

  defp lineage_column(assigns) do
    ~H"""
    <div class="flex min-w-0 flex-1 flex-col">
      <.lineage_heading label={@label} />

      <p :if={@nodes == []} class="text-sm favn-text-muted">{@empty}</p>

      <!-- `auto-rows-fr` is `minmax(0, 1fr)`, so slot `i` of `n` centres exactly at
      `(i + 0.5) / n` however tall a node turns out to be — which is what the drawn
      wires assume. Neither `justify-around` nor `flex-1` slots manage that: a node
      that wraps to two lines takes more than its share and pulls its neighbours off
      the line drawn to meet them, because a percentage flex basis resolves against a
      height this container does not definitely have. No gap for the same reason;
      the separation is the slack inside each slot. -->
      <ul :if={@nodes != []} class="grid min-h-fit flex-1 auto-rows-fr">
        <li :for={node <- @nodes} class="flex min-w-0 items-center py-1">
          <.lineage_node node={node} />
        </li>
      </ul>
    </div>
    """
  end

  # Every column carries this row, the wires and the centre included, so all four
  # measure their vertical space from the same origin. Without it the SVG spanned
  # heading-plus-list while the nodes were spread over the list alone, and every wire
  # met its node half a heading too high. The invisible copy is a zero-width space
  # rather than a fixed height, so it cannot drift from the real one.
  attr :label, :string, default: nil

  defp lineage_heading(assigns) do
    ~H"""
    <p :if={@label} class="mb-2 text-sm favn-text-subtle">{@label}</p>
    <p :if={is_nil(@label)} class="mb-2 hidden text-sm md:block" aria-hidden="true">&#8203;</p>
    """
  end

  attr :node, :map, required: true

  defp lineage_node(assigns) do
    ~H"""
    <.link
      :if={@node[:navigate]}
      navigate={@node.navigate}
      class="favn-surface-control flex min-w-0 flex-1 items-center gap-2 rounded-box px-3 py-2 transition-colors hover:border-primary/40"
      title={@node[:title] || @node.label}
    >
      <.icon :if={@node[:icon]} name={@node.icon} size={:sm} class="shrink-0 favn-text-subtle" />
      <span class="truncate font-medium">{@node.label}</span>
    </.link>

    <div
      :if={is_nil(@node[:navigate])}
      class="favn-surface-control min-w-0 rounded-box border-dashed px-3 py-2"
      title={@node[:title] || @node.label}
    >
      <div class="flex min-w-0 items-center gap-2">
        <.icon :if={@node[:icon]} name={@node.icon} size={:sm} class="shrink-0 favn-text-subtle" />
        <span class="truncate favn-text-muted">{@node.label}</span>
      </div>

      <p :if={@node[:note]} class="mt-0.5 truncate text-sm favn-text-subtle">{@node.note}</p>
    </div>
    """
  end

  attr :count, :integer, required: true
  attr :direction, :atom, required: true

  defp lineage_wires(assigns) do
    assigns = assign(assigns, :paths, lineage_paths(assigns.count, assigns.direction))

    ~H"""
    <div class="hidden w-10 shrink-0 flex-col md:flex">
      <.lineage_heading />

      <svg
        :if={@paths != []}
        class="w-full flex-1 favn-text-subtle"
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        <path
          :for={path <- @paths}
          d={path}
          fill="none"
          stroke="currentColor"
          stroke-width="1"
          vector-effect="non-scaling-stroke"
        />
      </svg>
    </div>
    """
  end

  defp lineage_stack_arrow(assigns) do
    ~H"""
    <div class="flex justify-center md:hidden">
      <.icon name="hero-arrow-down" size={:sm} class="favn-text-subtle" />
    </div>
    """
  end

  # A single node is joined by one straight line; several fan into a shared bus
  # halfway across the gap, which is what makes the edges legible when there are five.
  defp lineage_paths(0, _direction), do: []

  defp lineage_paths(count, direction) do
    Enum.map(0..(count - 1), fn index ->
      y = Float.round((index + 0.5) / count * 100, 2)

      case direction do
        :in -> "M 0,#{y} H 50 V 50 H 100"
        :out -> "M 0,50 H 50 V #{y} H 100"
      end
    end)
  end

  @doc """
  A run history as a vertical spine, newest at the top.

  Use this instead of a table when the reader is picking one entry to inspect
  rather than comparing a page of them. A table answers "which of these is worst";
  a spine answers "what happened, and when did the rhythm break" — the gaps between
  entries are the point, so the dates carry the column and each entry stays one
  glance wide.

  Entries are grouped by day, and the day appears once as a heading rather than on
  every row, so a day with six runs reads as one day.

      <.run_timeline
        runs={@runs}
        selected_id={@selected_run_id}
        empty_label="This asset has not run yet."
      />
  """
  attr :runs, :list,
    required: true,
    doc:
      "newest first; maps with `:id`, `:patch`, `:status_tone`, `:status_label`, " <>
        "`:day_label`, `:time_label`, and optional `:window_label`, `:duration_label`, " <>
        "and `:trigger_label`"

  attr :selected_id, :string, default: nil
  attr :label, :string, default: "Runs", doc: "names the region for assistive technology"
  attr :empty_label, :string, default: "No runs yet."
  attr :class, :any, default: nil
  attr :rest, :global

  def run_timeline(assigns) do
    assigns = assign(assigns, :days, group_runs_by_day(assigns.runs))

    ~H"""
    <div
      class={["min-h-0 overflow-y-auto", @class]}
      role="navigation"
      aria-label={@label}
      {@rest}
    >
      <.empty_state :if={@runs == []} title={@empty_label} icon="hero-clock" />

      <ol :if={@runs != []} class="relative space-y-4">
        <li :for={day <- @days}>
          <p class="sticky top-0 z-10 bg-base-100/85 py-1 text-xs font-medium uppercase tracking-wide backdrop-blur favn-text-subtle">
            {day.label}
          </p>

          <ol class="mt-1 space-y-1">
            <li :for={run <- day.runs} class="relative pl-6">
              <span
                class="absolute left-[7px] top-0 h-full w-px bg-base-content/10"
                aria-hidden="true"
              ></span>
              <span
                class={[
                  "absolute left-0 top-3 size-[15px] rounded-full ring-3 ring-base-100",
                  Tokens.surface_class(run.status_tone)
                ]}
                aria-hidden="true"
              >
                <span class={[
                  "absolute inset-[3px] rounded-full",
                  Tokens.dot_class(run.status_tone),
                  "status"
                ]}></span>
              </span>

              <.link
                patch={run.patch}
                aria-current={(run.id == @selected_id && "true") || nil}
                class={[
                  "block rounded-field px-2.5 py-2 transition-colors",
                  (run.id == @selected_id && "favn-mode-item-active") ||
                    "hover:bg-base-content/5"
                ]}
              >
                <span class="flex items-baseline justify-between gap-2">
                  <span class="font-mono text-sm text-base-content">{run.time_label}</span>
                  <span class={["truncate text-sm", Tokens.text_class(run.status_tone)]}>
                    {run.status_label}
                  </span>
                </span>

                <span
                  :if={run[:window_label] || run[:duration_label] || run[:trigger_label]}
                  class="mt-0.5 flex items-baseline gap-2 truncate text-sm favn-text-subtle"
                >
                  <span :if={run[:window_label]} class="truncate">{run.window_label}</span>
                  <span :if={run[:duration_label]}>{run.duration_label}</span>
                  <span :if={run[:trigger_label]} class="truncate">{run.trigger_label}</span>
                </span>
              </.link>
            </li>
          </ol>
        </li>
      </ol>
    </div>
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
      <span class="shrink-0 text-sm favn-text-subtle sm:w-44">{@label}</span>
      <span class="min-w-0 text-sm favn-text-muted">{render_slot(@inner_block)}</span>
    </div>
    """
  end

  # `Enum.chunk_by/2` rather than `group_by`, so the caller's newest-first order
  # survives; grouping would reorder the days by however the map hashed them.
  defp group_runs_by_day(runs) do
    runs
    |> Enum.chunk_by(& &1[:day_label])
    |> Enum.map(&%{label: List.first(&1)[:day_label] || "Unknown date", runs: &1})
  end

  defp align_class(:end), do: "text-right"
  defp align_class(_align), do: nil

  # The rail's own colour is tuned for icon-only buttons; these carry words,
  # which are held to the higher contrast ask, so an inactive one borrows the
  # muted text tier rather than the rail's decorative tint.
  defp scope_button_classes(choice) do
    [
      "favn-mode-item h-9 justify-start gap-1.5 rounded-field px-2.5 text-sm font-medium sm:justify-center",
      "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
      (choice.active? && "favn-mode-item-active") || "favn-text-muted"
    ]
  end

  # A scope button with nothing in it should not shout, and one with failures
  # should. The count is the same number either way.
  defp count_tone(%{count: 0}), do: :neutral
  defp count_tone(%{tone: tone}), do: tone

  # Below `lg` the filters are a disclosure, on every list screen rather than on
  # the ones that opted in: a phone has room for the scope rail or the filters,
  # not both, and a screen that laid them out inline pushed its rows off-screen.
  # `lg` and up reopens them for good.
  defp filters_class(%{filters_open?: true}), do: "flex w-full flex-col lg:w-auto"
  defp filters_class(_assigns), do: "hidden w-full flex-col lg:flex lg:w-auto"

  defp mono?(:both, _line), do: true
  defp mono?(line, line), do: true
  defp mono?(_mono, _line), do: false

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
