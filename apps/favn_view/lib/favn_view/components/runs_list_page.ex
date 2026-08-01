defmodule FavnView.Components.RunsListPage do
  @moduledoc """
  The runs list, built around the four questions an operator opens it to ask.

  What is running, what failed today, what ran today, and did this pipeline run
  every day — those are the questions, so they are the four buttons across the
  top, each carrying its own count from the store rather than from this page. The
  count is the answer to three of them before anything is clicked.

  The fourth is a question about absence, which is why the table grows day
  headers whenever the range covers more than one day: a day with no runs still
  gets a row, and the gap is the answer.

  The status is that row of buttons and nothing else — a select beside them would
  be a second way to set the same field, and the counts make the buttons the
  better one. What is left is a search and a time range, plus a sort direction
  that lives in the column it acts on.

  ## Narrow screens

  The table is the reason the page exists, so on a narrow screen it gets the room:
  the search and the range collapse behind one control, and the status buttons
  stay, because their counts are the answer rather than a way to look for it. The
  rows become cards and the page itself scrolls, rather than a short region
  inside it.

  ## Why the table is local

  `FavnView.UI.Data.data_table/1` renders one flat list of rows with one header.
  This table has a sortable header and a header row per day, with columns that
  stay aligned across all of them, which is one table with several bodies rather
  than a list of tables. That structure is this page's, so it lives here.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation
  alias FavnView.RunsFilters

  attr :listing, :any,
    required: true,
    doc: "`{:flat, runs}` when the range is one day, `{:days, days}` when it is more"

  attr :filters, RunsFilters, required: true
  attr :counts, :map, default: nil, doc: "store-wide counts, or `nil` if they could not be read"
  attr :more?, :boolean, default: false, doc: "whether a page follows this one"
  attr :filters_open?, :boolean, default: false, doc: "narrow screens only; wide ones always show"
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []

  def runs_list_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Runs"
      subtitle={RunsFilters.range_label(@filters)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    >
      <div
        class="mx-auto flex w-full max-w-[110rem] flex-col lg:min-h-0 lg:flex-1"
        data-testid="runs-list-page"
      >
        <.error_state
          :if={@error}
          title="Could not load runs"
          description={@error}
          data-testid="runs-error-state"
        />
        <.table_panel
          :if={!@error}
          count={row_count(@listing)}
          count_label="runs"
          data-testid="runs-panel"
        >
          <:toolbar>
            <.runs_toolbar filters={@filters} counts={@counts} filters_open?={@filters_open?} />
          </:toolbar>

          <div class="lg:min-h-0 lg:flex-1 lg:overflow-y-auto">
            <.runs_empty_state :if={empty?(@listing)} filters={@filters} />
            <.runs_table :if={!empty?(@listing)} listing={@listing} order={@filters.order} />
            <.runs_cards :if={!empty?(@listing)} listing={@listing} />
          </div>

          <:footer>
            <.runs_pager filters={@filters} more?={@more?} />
          </:footer>
        </.table_panel>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :filters, RunsFilters, required: true
  attr :counts, :map, default: nil
  attr :filters_open?, :boolean, default: false

  def runs_toolbar(assigns) do
    choices =
      assigns.filters
      |> RunsFilters.status_filters(assigns.counts)
      |> Enum.map(&Map.merge(&1, %{patch: runs_path(&1.params), count_label: "runs"}))

    assigns = assign(assigns, :choices, choices)

    ~H"""
    <.table_toolbar
      on_change="filter_runs"
      filters_id="runs-filters"
      on_toggle="toggle_filters"
      filters_open?={@filters_open?}
      on_clear="clear_filters"
      adjusted?={RunsFilters.narrowed?(@filters) or RunsFilters.adjusted?(@filters)}
      search_name="filters[q]"
      search_label="Search runs"
      search_placeholder="Run id, pipeline, or asset"
      search_value={@filters.search}
    >
      <:scopes>
        <.scope_rail label="Run status" choices={@choices} data-testid="run-statuses" />
      </:scopes>

      <:filters>
        <.select_field
          name="filters[range]"
          label="Time range"
          options={RunsFilters.range_options()}
          value={to_string(@filters.range)}
          icon="hero-calendar-days"
        />
        <div
          :if={RunsFilters.custom?(@filters)}
          class="flex w-full items-center gap-2 sm:w-auto"
          data-testid="runs-custom-range"
        >
          <.date_input
            name="filters[from]"
            label="From date"
            value={@filters.from}
            class="min-w-0 flex-1 sm:w-36 sm:flex-none"
          /> <span class="shrink-0 text-xs favn-text-subtle">to</span>
          <.date_input
            name="filters[to]"
            label="To date"
            value={@filters.to}
            class="min-w-0 flex-1 sm:w-36 sm:flex-none"
          />
        </div>
      </:filters>
    </.table_toolbar>
    """
  end

  attr :name, :string, required: true
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :class, :any, default: nil

  def date_input(assigns) do
    ~H"""
    <label class={["input input-sm favn-surface-control h-9 min-h-9 px-3", @class]}>
      <span class="sr-only">{@label}</span>
      <input
        type="date"
        name={@name}
        value={@value && Date.to_iso8601(@value)}
        aria-label={@label}
        class="grow"
      />
    </label>
    """
  end

  attr :listing, :any, required: true
  attr :order, :atom, required: true

  def runs_table(assigns) do
    ~H"""
    <div class="hidden lg:block">
      <table class="w-full table table-sm" data-testid="runs-table">
        <thead class="sticky top-0 z-10 bg-base-100/85 backdrop-blur">
          <tr class="border-base-content/10 text-xs favn-text-muted">
            <th class="w-64 font-medium">Run</th>

            <th class="font-medium">Target</th>

            <th class="w-36 font-medium">
              <.sort_button order={@order} />
            </th>

            <th class="w-28 font-medium">Duration</th>

            <th class="w-10"><span class="sr-only">Open</span></th>
          </tr>
        </thead>

        <tbody :if={match?({:flat, _}, @listing)}>
          <.run_row :for={run <- flat_runs(@listing)} run={run} />
        </tbody>

        <tbody :for={day <- day_groups(@listing)} id={day.id} data-testid="runs-day-group">
          <tr :if={day.kind == :gap} class="border-base-content/10" data-testid="runs-day-gap">
            <td colspan="5" class="py-1.5 text-xs favn-text-subtle">
              <.gap_heading day={day} />
            </td>
          </tr>

          <tr :if={day.kind == :day} class="border-none bg-base-content/[0.06]">
            <th colspan="5" class="py-1.5">
              <.day_heading day={day} />
            </th>
          </tr>
          <.run_row :for={run <- day_runs(day)} run={run} />
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  The step to the next page of older runs, and the way back to the newest.

  The list is a window rather than a pile: one page is one read of the index the
  rows are already ordered by, so the two hundredth page costs what the second
  does. Nothing is rendered when the first page is the only page.
  """
  attr :filters, RunsFilters, required: true
  attr :more?, :boolean, required: true

  def runs_pager(assigns) do
    ~H"""
    <div
      :if={@more? or RunsFilters.paged?(@filters)}
      class="flex items-center gap-2"
      data-testid="runs-pager"
    >
      <.button
        :if={RunsFilters.paged?(@filters)}
        phx-click="first_page"
        variant={:ghost}
        icon="hero-arrow-up"
        data-testid="runs-newest-page"
      >
        Newest
      </.button>

      <.button
        :if={@more?}
        phx-click="next_page"
        variant={:ghost}
        trailing_icon="hero-arrow-right"
        data-testid="runs-next-page"
      >
        Older
      </.button>
    </div>
    """
  end

  @doc "A stretch of days on which nothing ran, as one line."
  attr :day, :map, required: true

  def gap_heading(assigns) do
    ~H"""
    <span class="inline-flex items-center gap-2">
      <.icon name="hero-minus-small" size={:sm} /> {gap_text(@day)}
    </span>
    """
  end

  attr :order, :atom, required: true

  def sort_button(assigns) do
    ~H"""
    <button
      type="button"
      phx-click="toggle_started_order"
      class="inline-flex items-center gap-1 font-medium transition hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
      aria-label={"Started, #{sort_label(@order)}. Click to reverse."}
      data-testid="sort-started"
    >
      Started
      <.icon
        name={if(@order == :started_asc, do: "hero-bars-arrow-up", else: "hero-bars-arrow-down")}
        size={:sm}
      />
    </button>
    """
  end

  attr :day, :map, required: true

  def day_heading(assigns) do
    ~H"""
    <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
      <span class="text-xs font-semibold uppercase tracking-[0.14em] text-base-content">
        {@day.label}
      </span>

      <span :if={@day.total > 0} class="text-xs font-normal favn-text-muted">
        {@day.total} {if(@day.total == 1, do: "run", else: "runs")}
      </span>

      <span :if={@day.failed > 0} class="text-xs font-normal text-error">
        {@day.failed} failed
      </span>

      <span :if={@day.active > 0} class="text-xs font-normal text-info">
        {@day.active} in flight
      </span>
    </div>
    """
  end

  attr :run, :map, required: true

  def run_row(assigns) do
    ~H"""
    <tr
      id={"run-row-#{@run.id}"}
      class="group border-base-content/10 text-sm transition hover:bg-primary/10 focus-within:bg-primary/10"
      data-testid="run-row"
      data-run-id={@run.id}
    >
      <td class="align-middle"><.run_identity run={@run} /></td>

      <td class="max-w-64 align-middle"><.run_target run={@run} /></td>

      <td class="whitespace-nowrap align-middle text-xs" title={@run.started_at_title}>
        <p class="favn-text-muted">{@run.started_at}</p>

        <p :if={@run.started_on} class="text-[0.68rem] favn-text-subtle">{@run.started_on}</p>
      </td>

      <td class="whitespace-nowrap align-middle text-xs favn-text-muted">{@run.duration}</td>

      <td class="text-right align-middle">
        <.icon_button
          navigate={~p"/runs/#{@run.id}"}
          icon="hero-chevron-right"
          label={"Open run #{@run.short_id}"}
          shape={:circle}
        />
      </td>
    </tr>
    """
  end

  attr :run, :map, required: true

  def run_identity(assigns) do
    ~H"""
    <div class="flex min-w-0 items-center gap-2">
      <.status_dot tone={@run.status} label={@run.status_label} />
      <div class="min-w-0 flex-1">
        <.link
          navigate={~p"/runs/#{@run.id}"}
          class="block truncate font-mono text-xs font-medium text-base-content hover:text-primary"
          title={@run.id}
          data-testid="run-link"
        >
          {@run.short_id}
        </.link>

        <p class="truncate text-[0.68rem] favn-text-subtle">
          {@run.status_label} · {@run.trigger}
        </p>
      </div>
    </div>
    """
  end

  @doc """
  What the run was for, and how much of it happened.

  The second line counts asset steps, because that is the work: a pipeline run of
  fourteen assets did fourteen things, and a backfill did one per window. The runs
  inside a group are a submission detail and were never worth a column.
  """
  attr :run, :map, required: true

  def run_target(assigns) do
    ~H"""
    <div class="min-w-0">
      <p class="truncate font-medium text-base-content" title={@run.target_title}>{@run.target}</p>

      <p class="flex min-w-0 items-center gap-1.5 text-[0.68rem]">
        <span :if={@run.assets} class="truncate favn-text-subtle">{@run.assets}</span>
        <span :if={!@run.assets && @run.target_detail} class="truncate favn-text-subtle">
          {@run.target_detail} planned
        </span>

        <span :if={@run.assets_failed > 0} class="shrink-0 text-error">
          {@run.assets_failed} failed
        </span>
      </p>
    </div>
    """
  end

  attr :listing, :any, required: true

  def runs_cards(assigns) do
    ~H"""
    <div class="space-y-2.5 p-3 lg:hidden" data-testid="runs-card-list">
      <.run_card :for={run <- flat_runs(@listing)} run={run} />
      <div :for={day <- day_groups(@listing)} class="space-y-2.5" id={"card-#{day.id}"}>
        <p :if={day.kind == :gap} class="pl-1 text-xs favn-text-subtle">
          <.gap_heading day={day} />
        </p>
        <.day_heading :if={day.kind == :day} day={day} />
        <.run_card :for={run <- day_runs(day)} run={run} />
      </div>
    </div>
    """
  end

  attr :run, :map, required: true

  def run_card(assigns) do
    ~H"""
    <.list_card navigate={~p"/runs/#{@run.id}"} class="space-y-1.5" data-testid="run-card">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <.status_badge tone={@run.status} label={@run.status_label} />
          <p class="mt-1.5 truncate text-sm font-medium text-base-content" title={@run.target_title}>
            {@run.target}
          </p>

          <p class="truncate font-mono text-[0.68rem] favn-text-subtle">{@run.short_id}</p>
        </div>

        <div class="shrink-0 text-right text-xs favn-text-muted">
          <p :if={@run.started_on} class="favn-text-subtle">{@run.started_on}</p>

          <p>{@run.started_at}</p>

          <p class="favn-text-subtle">{@run.duration}</p>
        </div>
      </div>

      <p class="flex items-center gap-1.5 text-[0.68rem]">
        <span :if={@run.assets} class="truncate favn-text-subtle">{@run.assets}</span>
        <span :if={@run.assets_failed > 0} class="shrink-0 text-error">
          {@run.assets_failed} failed
        </span>
      </p>
    </.list_card>
    """
  end

  attr :filters, RunsFilters, required: true

  def runs_empty_state(assigns) do
    ~H"""
    <.empty_state
      :if={RunsFilters.narrowed?(@filters)}
      title="No runs match"
      description={"Nothing in #{RunsFilters.range_label(@filters)} matches this search and status."}
      icon="hero-funnel"
      data-testid="runs-filtered-empty-state"
    />
    <.empty_state
      :if={!RunsFilters.narrowed?(@filters)}
      title={"No runs in #{RunsFilters.range_label(@filters)}"}
      description="Widen the time range, or submit an asset, pipeline, or backfill."
      icon="hero-rocket-launch"
      data-testid="runs-empty-state"
    />
    """
  end

  @doc "Navigation items for the runs section."
  @spec nav_items(atom()) :: list()
  def nav_items(active \\ :runs), do: Navigation.items(active)

  @doc """
  The path for one set of `FavnView.RunsFilters` params.

  The default view carries no params, and `/runs?` is not a URL anyone should be
  handed, so an empty list is the bare path.
  """
  @spec runs_path([{String.t(), String.t()}]) :: String.t()
  def runs_path([]), do: ~p"/runs"
  def runs_path(params), do: ~p"/runs?#{params}"

  defp empty?({:flat, runs}), do: runs == []
  defp empty?({:days, days}), do: Enum.all?(days, &(&1.kind == :gap))

  # The rows on this page, whichever layout it is in. Day headers and gaps are
  # not rows, so they are not counted.
  defp row_count({:flat, runs}), do: length(runs)

  defp row_count({:days, days}),
    do: days |> Enum.filter(&(&1.kind == :day)) |> Enum.map(&length(&1.runs)) |> Enum.sum()

  defp day_runs(%{kind: :day, runs: runs}), do: runs
  defp day_runs(_gap), do: []

  defp gap_text(%{label: label, days: 1}), do: "#{label} · no runs"
  defp gap_text(%{label: label, days: days}), do: "#{label} · no runs · #{days} days"

  defp flat_runs({:flat, runs}), do: runs
  defp flat_runs(_layout), do: []

  defp day_groups({:days, days}), do: days
  defp day_groups(_layout), do: []

  defp sort_label(:started_asc), do: "oldest first"
  defp sort_label(_order), do: "newest first"
end
