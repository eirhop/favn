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

  Everything else the operator can ask is three controls — a search, a status, and
  a time range — plus a sort direction, which lives in the column it acts on. One
  row of chrome, not nine. The four buttons write to the same three controls, so a
  question is a shortcut rather than a mode the operator can get stuck in.

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
  attr :truncated?, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true

  def runs_list_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Runs"
      subtitle={RunsFilters.range_label(@filters)}
      nav_items={@nav_items}
      content_scroll?={false}
    >
      <div
        class="mx-auto flex min-h-0 w-full max-w-[110rem] flex-1 flex-col pb-24 lg:pb-0"
        data-testid="runs-list-page"
      >
        <.error_state
          :if={@error}
          title="Could not load runs"
          description={@error}
          data-testid="runs-error-state"
        />

        <.panel
          :if={!@error}
          padding={:none}
          class="flex min-h-0 flex-1 flex-col overflow-hidden"
          data-testid="runs-panel"
        >
          <.runs_toolbar filters={@filters} counts={@counts} />

          <div class="min-h-0 flex-1 overflow-y-auto">
            <.runs_empty_state :if={empty?(@listing)} filters={@filters} />

            <.runs_table :if={!empty?(@listing)} listing={@listing} order={@filters.order} />

            <.runs_cards :if={!empty?(@listing)} listing={@listing} />

            <div :if={@truncated?} class="p-4 text-center">
              <.button
                phx-click="load_more"
                variant={:ghost}
                icon="hero-arrow-down"
                data-testid="load-more-runs"
              >
                Load {RunsFilters.growth(@filters) || 0} more
              </.button>
            </div>
          </div>
        </.panel>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :filters, RunsFilters, required: true
  attr :counts, :map, default: nil

  def runs_toolbar(assigns) do
    assigns = assign(assigns, :presets, RunsFilters.presets(assigns.filters, assigns.counts))

    ~H"""
    <div class="border-b border-base-content/10 p-3 sm:p-4">
      <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <nav
          class="favn-surface-rail flex flex-wrap items-center gap-0.5 rounded-box p-1"
          aria-label="Run scopes"
          data-testid="run-scopes"
        >
          <.scope_button :for={preset <- @presets} preset={preset} />
        </nav>

        <form
          phx-change="filter_runs"
          phx-submit="filter_runs"
          class="flex flex-wrap items-center gap-2"
          data-testid="runs-filters"
        >
          <.search_field
            name="filters[q]"
            value={@filters.search}
            label="Search runs"
            placeholder="Run id, pipeline, or asset"
            class="w-full sm:w-52"
          />
          <.select_field
            name="filters[status]"
            label="Status"
            options={RunsFilters.status_options()}
            value={to_string(@filters.status)}
            icon="hero-signal"
            class="w-full sm:w-40"
          />
          <.select_field
            name="filters[range]"
            label="Time range"
            options={RunsFilters.range_options()}
            value={to_string(@filters.range)}
            icon="hero-calendar-days"
            class="w-full sm:w-40"
          />
          <div :if={RunsFilters.custom?(@filters)} class="flex items-center gap-2">
            <.date_input name="filters[from]" label="From date" value={@filters.from} />
            <span class="text-xs favn-text-subtle">to</span>
            <.date_input name="filters[to]" label="To date" value={@filters.to} />
          </div>
        </form>
      </div>
    </div>
    """
  end

  attr :preset, :map, required: true

  def scope_button(assigns) do
    ~H"""
    <.link
      patch={runs_path(@preset.params)}
      class={
        [
          "favn-mode-item h-9 gap-1.5 rounded-field px-2.5 text-sm font-medium",
          "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
          # The rail's own colour is tuned for icon-only buttons; these carry words,
          # which are held to the higher contrast ask, so an inactive one borrows the
          # muted text tier rather than the rail's decorative tint.
          (@preset.active? && "favn-mode-item-active") || "favn-text-muted"
        ]
      }
      aria-current={@preset.active? && "page"}
      data-testid={"run-scope-#{@preset.id}"}
    >
      <.icon name={@preset.icon} size={:md} class={Tokens.text_class(Tokens.tone(@preset.tone))} />
      <span class="whitespace-nowrap">{@preset.label}</span>
      <.count_badge
        :if={is_integer(@preset.count)}
        count={@preset.count}
        label="runs"
        tone={count_tone(@preset)}
      />
    </.link>
    """
  end

  attr :name, :string, required: true
  attr :label, :string, required: true
  attr :value, :any, default: nil

  def date_input(assigns) do
    ~H"""
    <label class="input input-sm favn-surface-control w-36 px-3">
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
    <table class="hidden w-full table table-sm lg:table" data-testid="runs-table">
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
    """
  end

  @doc "A stretch of days on which nothing ran, as one line."
  attr :day, :map, required: true

  def gap_heading(assigns) do
    ~H"""
    <span class="inline-flex items-center gap-2">
      <.icon name="hero-minus-small" size={:sm} />
      {gap_text(@day)}
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

      <td class="whitespace-nowrap align-middle text-xs favn-text-muted" title={@run.started_at_title}>
        {@run.started_at}
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
        <.run_progress :if={@run.progress.total > 1} run={@run} />
      </div>
    </div>
    """
  end

  attr :run, :map, required: true

  def run_target(assigns) do
    ~H"""
    <div class="min-w-0">
      <p class="truncate font-medium text-base-content" title={@run.target_title}>{@run.target}</p>
      <p :if={@run.target_detail} class="truncate text-[0.68rem] favn-text-subtle">
        {@run.target_detail}
      </p>
    </div>
    """
  end

  @doc """
  How far a group of runs has got — which, for a backfill, is its windows.

  Only rendered when there is more than one run to divide. A single-run group
  would read `1 / 1`, which is chrome pretending to be information, and a whole
  column of it was the old page's widest dead space.
  """
  attr :run, :map, required: true

  def run_progress(assigns) do
    ~H"""
    <div class="mt-1.5 max-w-40">
      <.outcome_meter
        segments={@run.progress.segments}
        summary={@run.progress.summary}
        size={:sm}
        legend?={false}
      />
      <p class="mt-1 truncate text-[0.68rem] favn-text-muted">{@run.progress.summary}</p>
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
    <.list_card navigate={~p"/runs/#{@run.id}"} class="space-y-2.5" data-testid="run-card">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <.status_badge tone={@run.status} label={@run.status_label} />
          <p class="mt-1.5 truncate text-sm font-medium text-base-content" title={@run.target_title}>
            {@run.target}
          </p>
          <p class="truncate font-mono text-[0.68rem] favn-text-subtle">{@run.short_id}</p>
        </div>
        <div class="shrink-0 text-right text-xs favn-text-muted">
          <p>{@run.started_at}</p>
          <p class="favn-text-subtle">{@run.duration}</p>
        </div>
      </div>

      <.outcome_meter
        :if={@run.progress.total > 1}
        segments={@run.progress.segments}
        summary={@run.progress.summary}
        size={:sm}
      />
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

  # A scope button with nothing in it should not shout, and one with failures
  # should. The count is the same number either way.
  defp count_tone(%{count: 0}), do: :neutral
  defp count_tone(%{tone: tone}), do: tone
end
