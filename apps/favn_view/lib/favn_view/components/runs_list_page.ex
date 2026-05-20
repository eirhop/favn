defmodule FavnView.Components.RunsListPage do
  @moduledoc """
  Execution-group oriented runs overview components.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :groups, :list, required: true
  attr :all_groups, :list, default: []
  attr :group_details, :map, default: %{}
  attr :expanded_group_ids, MapSet, default: MapSet.new()
  attr :filters, :map, required: true
  attr :filter_options, :map, required: true
  attr :summary, :map, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true

  def runs_list_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Runs"
      subtitle="Backfills and runs"
      nav_items={@nav_items}
      show_header?={false}
      content_scroll?={false}
    >
      <div
        class="mx-auto flex min-h-0 w-full max-w-[120rem] flex-1 flex-col pb-24 lg:pb-0"
        data-testid="runs-list-page"
      >
        <.loading_state :if={@loading} />
        <.error_state :if={!@loading && @error} />

        <div :if={!@loading && !@error} class="flex min-h-0 flex-1 flex-col gap-2.5 lg:gap-3">
          <.summary_band summary={@summary} />

          <GlassPanel.glass_panel
            class="flex min-h-0 flex-1 flex-col overflow-hidden p-3 sm:p-4"
            data-testid="execution-groups-panel"
          >
            <.filters_bar
              filters={@filters}
              filter_options={@filter_options}
              result_count={length(@groups)}
            />

            <.empty_state :if={@all_groups == []} />
            <.filtered_empty_state :if={@all_groups != [] && @groups == []} />

            <.execution_groups_table
              :if={@groups != []}
              groups={@groups}
              group_details={@group_details}
              expanded_group_ids={@expanded_group_ids}
            />

            <.execution_group_cards
              :if={@groups != []}
              groups={@groups}
              group_details={@group_details}
              expanded_group_ids={@expanded_group_ids}
            />
          </GlassPanel.glass_panel>
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={runs_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :summary, :map, required: true

  def summary_band(assigns) do
    ~H"""
    <section
      class="favn-surface-panel rounded-box border border-base-content/10 bg-base-100/35 px-4 py-3 shadow-[0_16px_60px_rgba(0,0,0,0.22)] sm:px-5 sm:py-4"
      data-testid="runs-summary-band"
    >
      <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <.summary_metric label="Runs" value={@summary.total_groups} caption="Total" />
        <.summary_metric label="Windows" value={@summary.total_windows} caption="Across backfills" />
        <div class="space-y-1.5 border-base-content/10 sm:border-l sm:pl-5">
          <p class="text-[0.68rem] font-semibold uppercase tracking-[0.2em] text-base-content/50">
            Asset attempts
          </p>
          <p class="text-xl font-semibold leading-none text-base-content">
            {@summary.completed_asset_attempts}
            <span class="text-base-content/45">/ {@summary.total_asset_attempts}</span>
          </p>
          <progress
            class="progress progress-info h-1.5 w-full bg-base-content/10"
            value={@summary.completed_asset_attempts}
            max={max(@summary.total_asset_attempts, 1)}
          >
          </progress>
        </div>
        <div class="space-y-2 border-base-content/10 sm:border-l sm:pl-5 xl:col-span-2">
          <p class="text-[0.68rem] font-semibold uppercase tracking-[0.2em] text-base-content/50">
            Health summary
          </p>
          <div class="grid grid-cols-2 gap-2 text-sm sm:grid-cols-4">
            <.health_count tone="success" label="Succeeded" value={@summary.health.succeeded} />
            <.health_count tone="error" label="Failed" value={@summary.health.failed} />
            <.health_count tone="info" label="Running" value={@summary.health.running} />
            <.health_count tone="warning" label="Queued" value={@summary.health.queued} />
          </div>
        </div>
      </div>
    </section>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, required: true
  attr :caption, :string, required: true

  def summary_metric(assigns) do
    ~H"""
    <div class="space-y-1 border-base-content/10 sm:border-l sm:pl-5 first:border-l-0 first:pl-0">
      <p class="text-[0.68rem] font-semibold uppercase tracking-[0.2em] text-base-content/50">
        {@label}
      </p>
      <p class="text-xl font-semibold leading-none text-base-content">{@value}</p>
      <p class="text-xs text-base-content/55">{@caption}</p>
    </div>
    """
  end

  attr :tone, :string, required: true
  attr :label, :string, required: true
  attr :value, :integer, required: true

  def health_count(assigns) do
    ~H"""
    <div class="min-w-0">
      <p class={["font-semibold", health_text_class(@tone)]}>
        <span class={["status status-xs align-middle", health_status_class(@tone)]}></span>
        <span class="ml-1">{@value}</span>
      </p>
      <p class="truncate text-xs text-base-content/55">{@label}</p>
    </div>
    """
  end

  attr :filters, :map, required: true
  attr :filter_options, :map, required: true
  attr :result_count, :integer, required: true

  def filters_bar(assigns) do
    ~H"""
    <.form
      for={%{}}
      as={:filters}
      phx-change="filter_groups"
      class="pb-3"
      data-testid="execution-group-filters"
    >
      <div class="flex flex-wrap items-center gap-2">
        <label class="favn-surface-control input input-sm h-9 min-h-9 min-w-0 flex-1 items-center gap-2 rounded-field sm:min-w-72 lg:max-w-80">
          <.icon name="hero-magnifying-glass" class="size-4 text-base-content/45" />
          <input
            type="search"
            name="filters[search]"
            value={@filters["search"]}
            placeholder="Search runs..."
            class="grow"
            data-testid="execution-group-search"
          />
        </label>

        <input id="runs-filter-toggle" type="checkbox" class="peer sr-only" />
        <label
          for="runs-filter-toggle"
          class="btn btn-sm favn-surface-control rounded-field xl:hidden"
        >
          <.icon name="hero-funnel" class="size-4" /> Filters
        </label>

        <span
          class="ml-auto text-xs text-base-content/45 xl:hidden"
          data-testid="execution-group-result-count"
        >
          {@result_count} results
        </span>

        <div class="hidden w-full flex-wrap items-center gap-2 pt-2 peer-checked:flex xl:flex xl:w-auto xl:flex-1 xl:pt-0">
          <.select_control
            label="Status"
            name="status"
            value={@filters["status"]}
            options={status_options()}
          />
          <.select_control
            label="Trigger"
            name="trigger"
            value={@filters["trigger"]}
            options={trigger_options(@filter_options.triggers)}
          />
          <.select_control
            label="Target"
            name="target"
            value={@filters["target"]}
            options={target_options(@filter_options.targets)}
          />
          <.select_control
            label="Window"
            name="window"
            value={@filters["window"]}
            options={window_options()}
          />
          <.select_control label="Sort" name="sort" value={@filters["sort"]} options={sort_options()} />

          <div class="ml-auto flex flex-wrap items-center justify-end gap-x-3 gap-y-2">
            <.toggle_control
              label="Only failed"
              name="only_failed"
              checked={@filters["only_failed"] == "true"}
            />
            <.toggle_control
              label="Only running"
              name="only_running"
              checked={@filters["only_running"] == "true"}
            />
            <.toggle_control
              label="Only incomplete"
              name="only_incomplete"
              checked={@filters["only_incomplete"] == "true"}
            />
            <button
              type="button"
              phx-click="clear_filters"
              class="text-xs font-semibold text-base-content/70 transition hover:text-primary"
              data-testid="clear-run-filters"
            >
              Clear
            </button>
            <span
              class="hidden text-xs text-base-content/45 xl:inline"
              data-testid="execution-group-result-count"
            >
              {@result_count} results
            </span>
          </div>
        </div>
      </div>
    </.form>
    """
  end

  attr :label, :string, required: true
  attr :name, :string, required: true
  attr :value, :string, required: true
  attr :options, :list, required: true

  def select_control(assigns) do
    ~H"""
    <label class="favn-surface-control flex h-9 min-h-9 min-w-32 items-center overflow-hidden rounded-field border border-base-content/10 bg-base-100/20 text-sm shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
      <span class="border-r border-base-content/10 px-3 text-xs text-base-content/45">{@label}</span>
      <select
        name={"filters[#{@name}]"}
        value={@value}
        class="select select-ghost select-sm h-8 min-h-8 flex-1 bg-transparent px-2 focus:outline-none"
      >
        <option :for={{label, value} <- @options} value={value} selected={@value == value}>
          {label}
        </option>
      </select>
    </label>
    """
  end

  attr :label, :string, required: true
  attr :name, :string, required: true
  attr :checked, :boolean, default: false

  def toggle_control(assigns) do
    ~H"""
    <label class="flex items-center gap-2 text-xs text-base-content/75">
      <input type="hidden" name={"filters[#{@name}]"} value="false" />
      <input
        type="checkbox"
        name={"filters[#{@name}]"}
        value="true"
        checked={@checked}
        class="toggle toggle-info toggle-xs"
      />
      <span class="whitespace-nowrap leading-none">{@label}</span>
    </label>
    """
  end

  attr :groups, :list, required: true
  attr :group_details, :map, required: true
  attr :expanded_group_ids, MapSet, required: true

  def execution_groups_table(assigns) do
    ~H"""
    <div class="hidden min-h-0 flex-1 overflow-auto border-t border-base-content/10 lg:block">
      <table class="table table-sm" data-testid="execution-groups-table">
        <thead>
          <tr class="border-base-content/10 text-xs text-base-content/55">
            <th class="w-64 font-medium">Backfill / run</th>
            <th class="font-medium">Trigger</th>
            <th class="font-medium">Target</th>
            <th class="font-medium">Window range</th>
            <th class="font-medium">Progress</th>
            <th class="font-medium">Health</th>
            <th class="font-medium">Current activity</th>
            <th class="font-medium">Started</th>
            <th class="font-medium">Duration</th>
          </tr>
        </thead>
        <tbody>
          <%= for group <- @groups do %>
            <.execution_group_row
              group={group}
              expanded={MapSet.member?(@expanded_group_ids, group.id)}
            />
            <.child_runs_row
              :if={MapSet.member?(@expanded_group_ids, group.id)}
              group={group}
              detail={Map.get(@group_details, group.id)}
            />
          <% end %>
        </tbody>
      </table>
      <div class="sticky bottom-0 border-t border-base-content/10 bg-base-100/80 px-2 py-3 text-xs text-base-content/55 backdrop-blur">
        Showing 1-{length(@groups)} of {length(@groups)} runs
      </div>
    </div>
    """
  end

  attr :group, :map, required: true
  attr :expanded, :boolean, required: true

  def execution_group_row(assigns) do
    ~H"""
    <tr
      class="group border-base-content/10 bg-base-100/5 text-sm transition hover:bg-primary/10 focus-within:bg-primary/10"
      data-testid="execution-group-row"
      data-group-id={@group.id}
    >
      <td>
        <div class="flex items-start gap-2">
          <button
            type="button"
            class="btn btn-ghost btn-xs mt-0.5 h-6 min-h-6 w-6 px-0"
            phx-click="toggle_group"
            phx-value-id={@group.id}
            aria-expanded={@expanded}
            data-testid="toggle-execution-group"
          >
            <.icon
              name={if(@expanded, do: "hero-chevron-down", else: "hero-chevron-right")}
              class="size-4"
            />
          </button>
          <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-1.5">
              <.status_badge status={@group.status} />
              <.trigger_badge trigger={@group.trigger} />
            </div>
            <.link
              navigate={~p"/runs/#{@group.id}"}
              class="mt-1 block max-w-44 truncate font-mono text-xs text-base-content/65 hover:text-primary"
              title={@group.id}
              data-testid="execution-group-link"
            >
              {@group.short_id}
            </.link>
          </div>
        </div>
      </td>
      <td class="text-xs text-base-content/75">{@group.trigger}</td>
      <td class="min-w-44 max-w-56"><.target_cell group={@group} /></td>
      <td class="min-w-36 text-xs text-base-content/75">
        <p>{@group.window}</p>
        <p class="text-xs text-base-content/45">{@group.window_count_label}</p>
      </td>
      <td class="min-w-40"><.progress_cell progress={@group.progress} /></td>
      <td class="min-w-36"><.health_counts health={@group.health} /></td>
      <td class="min-w-40"><.activity_cell activity={@group.current_activity} /></td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@group.started_at}</td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@group.duration}</td>
    </tr>
    """
  end

  attr :group, :map, required: true
  attr :detail, :any, default: nil

  def child_runs_row(assigns) do
    ~H"""
    <tr class="border-base-content/10 bg-base-100/20" data-testid="execution-group-children-row">
      <td colspan="9" class="p-0">
        <div class="mx-3 mb-3 rounded-box border border-base-content/10 bg-base-100/25 p-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
          <p :if={is_nil(@detail)} class="p-4 text-sm text-base-content/55">Loading window runs...</p>
          <p :if={match?(%{error: _}, @detail)} class="p-4 text-sm text-error">
            Could not load window runs.
          </p>

          <div :if={match?(%{child_runs: []}, @detail)} class="p-4 text-sm text-base-content/55">
            No window runs for this backfill.
          </div>

          <table
            :if={match?(%{child_runs: [_ | _]}, @detail)}
            class="table table-sm"
            data-testid="child-runs-table"
          >
            <thead>
              <tr class="border-base-content/10 text-base-content/55">
                <th>Window / run</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Started</th>
                <th>Duration</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr
                :for={child <- @detail.child_runs}
                class="border-base-content/10"
                data-testid="child-run-row"
                data-run-id={child.id}
              >
                <td>
                  <p class="font-medium text-base-content">{child.window}</p>
                  <p class="font-mono text-xs text-base-content/55">{child.short_id}</p>
                </td>
                <td><.status_badge status={child.status} /></td>
                <td><.progress_label progress={child.progress} /></td>
                <td class="whitespace-nowrap text-base-content/65">{child.started_at}</td>
                <td class="whitespace-nowrap text-base-content/65">{child.duration}</td>
                <td class="text-right">
                  <.link
                    navigate={~p"/runs/#{@group.id}?view=windows&child_run_id=#{child.id}"}
                    class="btn btn-ghost btn-xs text-primary"
                  >
                    Open window run
                  </.link>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </td>
    </tr>
    """
  end

  attr :groups, :list, required: true
  attr :group_details, :map, required: true
  attr :expanded_group_ids, MapSet, required: true

  def execution_group_cards(assigns) do
    ~H"""
    <div class="space-y-2.5 p-3 lg:hidden" data-testid="execution-group-card-list">
      <.execution_group_card
        :for={group <- @groups}
        group={group}
        expanded={MapSet.member?(@expanded_group_ids, group.id)}
        detail={Map.get(@group_details, group.id)}
      />
    </div>
    """
  end

  attr :group, :map, required: true
  attr :expanded, :boolean, required: true
  attr :detail, :any, default: nil

  def execution_group_card(assigns) do
    ~H"""
    <article
      class="card favn-surface-list rounded-box p-3"
      data-testid="execution-group-card"
      data-group-id={@group.id}
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 space-y-2">
          <div class="flex flex-wrap items-center gap-1.5">
            <.status_badge status={@group.status} />
            <.trigger_badge trigger={@group.trigger} />
          </div>
          <.link
            navigate={~p"/runs/#{@group.id}"}
            class="block truncate font-mono text-sm font-medium text-base-content hover:text-primary"
          >
            {@group.short_id}
          </.link>
          <p class="line-clamp-2 text-sm text-base-content/80" title={@group.target_title}>
            {@group.target}
          </p>
        </div>
        <button
          type="button"
          class="btn btn-ghost btn-sm"
          phx-click="toggle_group"
          phx-value-id={@group.id}
          aria-expanded={@expanded}
        >
          <.icon
            name={if(@expanded, do: "hero-chevron-down", else: "hero-chevron-right")}
            class="size-4"
          />
        </button>
      </div>
      <div class="mt-3 grid grid-cols-2 gap-3 text-xs text-base-content/65">
        <div><span class="block text-base-content/40">Window</span>{@group.window}</div>
        <div><span class="block text-base-content/40">Started</span>{@group.started_at}</div>
        <div class="col-span-2"><.progress_cell progress={@group.progress} /></div>
        <div class="col-span-2"><.health_counts health={@group.health} /></div>
      </div>
      <div :if={@expanded} class="mt-3 border-t border-base-content/10 pt-3">
        <p :if={is_nil(@detail)} class="text-xs text-base-content/55">Loading window runs...</p>
        <div :if={match?(%{child_runs: [_ | _]}, @detail)} class="space-y-2">
          <.link
            :for={child <- @detail.child_runs}
            navigate={~p"/runs/#{@group.id}?view=windows&child_run_id=#{child.id}"}
            class="block rounded-field border border-base-content/10 p-2 text-xs hover:border-primary/30"
            data-testid="child-run-card"
          >
            <span class="font-medium">{child.window}</span>
            <span class="ml-2 text-base-content/55">{child.short_id}</span>
          </.link>
        </div>
      </div>
    </article>
    """
  end

  attr :group, :map, required: true

  def target_cell(%{group: %{targets: [_single]}} = assigns) do
    ~H"""
    <p class="max-w-56 truncate text-sm font-medium text-base-content" title={@group.target_title}>
      {@group.target}
    </p>
    """
  end

  def target_cell(assigns) do
    ~H"""
    <details class="dropdown dropdown-hover dropdown-bottom">
      <summary class="list-none marker:content-none">
        <span class="inline-flex max-w-full cursor-default items-center gap-2 align-middle">
          <span
            class="max-w-48 truncate text-sm font-medium text-base-content"
            title={@group.target_title}
          >
            {@group.target}
          </span>
          <span class="badge badge-xs badge-soft badge-info shrink-0">
            +{length(@group.targets) - 1}
          </span>
        </span>
      </summary>
      <div class="dropdown-content z-20 mt-2 w-80 rounded-box border border-base-content/10 bg-base-100 p-3 shadow-xl">
        <p class="mb-2 text-xs font-medium uppercase tracking-[0.2em] text-base-content/45">
          Targets
        </p>
        <ul class="space-y-1 text-xs text-base-content/75">
          <li :for={target <- @group.targets} class="truncate" title={target}>{target}</li>
        </ul>
      </div>
    </details>
    """
  end

  attr :status, :atom, required: true

  def status_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm badge-soft gap-2", status_badge_class(@status)]}>
      <span class={["status", status_dot_class(@status)]}></span>
      {status_label(@status)}
    </span>
    """
  end

  attr :trigger, :string, required: true

  def trigger_badge(assigns) do
    ~H"""
    <span class="badge badge-sm badge-outline border-primary/40 text-primary">{@trigger}</span>
    """
  end

  attr :progress, :map, required: true

  def progress_cell(assigns) do
    ~H"""
    <div class="space-y-1" title={@progress.title}>
      <p class="text-sm text-base-content/80">{@progress.window_label}</p>
      <p class="text-xs text-base-content/55">{@progress.attempt_label}</p>
      <progress
        class={["progress h-1.5 w-full bg-base-content/10", progress_class(@progress.tone)]}
        value={@progress.percent}
        max="100"
      >
      </progress>
    </div>
    """
  end

  attr :progress, :map, required: true

  def progress_label(assigns) do
    ~H"""
    <span class="whitespace-nowrap text-sm text-base-content/70" title={@progress.title}>
      {@progress.label}
    </span>
    """
  end

  attr :health, :map, required: true

  def health_counts(assigns) do
    ~H"""
    <div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
      <.health_count tone="success" label="ok" value={@health.succeeded} />
      <.health_count tone="error" label="fail" value={@health.failed} />
      <.health_count tone="info" label="run" value={@health.running} />
      <.health_count tone="warning" label="queue" value={@health.queued} />
    </div>
    """
  end

  attr :activity, :any, default: nil

  def activity_cell(%{activity: nil} = assigns) do
    ~H"""
    <span class="text-sm text-base-content/40">-</span>
    """
  end

  def activity_cell(assigns) do
    ~H"""
    <div class="text-sm">
      <p class="max-w-40 truncate font-medium text-base-content" title={@activity.asset}>
        {@activity.asset}
      </p>
      <p class="text-xs text-base-content/55">{@activity.window}</p>
    </div>
    """
  end

  def loading_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto flex min-h-64 max-w-2xl items-center justify-center p-10">
      <div class="text-center">
        <span class="loading loading-ring loading-lg text-primary"></span>
        <p class="mt-4 text-base-content/60">Loading runs</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def empty_state(assigns) do
    ~H"""
    <div class="p-10 text-center" data-testid="runs-empty-state">
      <h2 class="text-xl font-medium">No runs yet</h2>
      <p class="mt-2 text-base-content/60">
        Submit an asset, pipeline, or backfill to see run activity here.
      </p>
    </div>
    """
  end

  def filtered_empty_state(assigns) do
    ~H"""
    <div class="p-10 text-center" data-testid="runs-filtered-empty-state">
      <h2 class="text-xl font-medium">No runs found</h2>
      <p class="mt-2 text-base-content/60">Adjust search or filters to widen the overview.</p>
    </div>
    """
  end

  def error_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto max-w-2xl p-10 text-center" data-testid="runs-error-state">
      <h2 class="text-xl font-medium">Could not load runs</h2>
      <p class="mt-2 text-base-content/60">Retry</p>
    </GlassPanel.glass_panel>
    """
  end

  def runs_modes do
    [
      %{id: :list, label: "Runs", icon: "hero-list-bullet"},
      %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  def sample_groups do
    [
      %{
        id: "run_backfill_8f2c9d1",
        short_id: "run_back...2c9d1",
        target: "Favn.Examples.Sales.revenue_metrics",
        targets: ["Favn.Examples.Sales.revenue_metrics"],
        status: :running,
        raw_status: :running,
        trigger: "Backfill",
        trigger_type: :backfill,
        window: "Jan 2026 -> May 2026",
        window_count_label: "5 windows",
        progress: %{
          window_label: "4 / 5 windows",
          attempt_label: "86 / 100 attempts",
          percent: 86,
          title: "4 / 5 windows; 86 / 100 attempts",
          tone: :info
        },
        health: %{succeeded: 82, failed: 3, running: 1, queued: 14},
        current_activity: %{asset: "daily_sales", window: "May 2026 window"},
        started_at: "May 19 16:26",
        duration: "1h 32m",
        child_run_ids: ~w(run_child_91a2b7e0 run_child_d4f6a3c1 run_child_a1b3c5d9),
        total_windows: 5,
        completed_windows: 4,
        failed_windows: 0,
        total_asset_attempts: 100,
        completed_asset_attempts: 86,
        failed_asset_attempts: 3,
        running_asset_attempts: 1,
        queued_asset_attempts: 14
      },
      %{
        id: "run_backfill_7b3a4c2d",
        short_id: "run_back...a4c2d",
        target: "Favn.Examples.Sales.inventory",
        targets: ["Favn.Examples.Sales.inventory"],
        status: :failed,
        raw_status: :error,
        trigger: "Backfill",
        trigger_type: :backfill,
        window: "Mar 2025 -> Dec 2025",
        window_count_label: "10 windows",
        progress: %{
          window_label: "4 / 10 windows",
          attempt_label: "172 / 250 attempts",
          percent: 69,
          title: "4 / 10 windows; 172 / 250 attempts",
          tone: :error
        },
        health: %{succeeded: 122, failed: 12, running: 0, queued: 116},
        current_activity: nil,
        started_at: "May 12 14:03",
        duration: "2d 6h",
        child_run_ids: ~w(run_child_failure),
        total_windows: 10,
        completed_windows: 4,
        failed_windows: 1,
        total_asset_attempts: 250,
        completed_asset_attempts: 172,
        failed_asset_attempts: 12,
        running_asset_attempts: 0,
        queued_asset_attempts: 116
      }
    ]
  end

  def sample_detail do
    %{
      child_runs: [
        %{
          id: "run_child_91a2b7e0",
          short_id: "run_child_91a2b7e0",
          status: :succeeded,
          raw_status: :ok,
          target: "daily_sales",
          window: "Jan 2026",
          progress: %{label: "20 / 20", title: "20 attempts"},
          started_at: "May 19 15:10",
          duration: "8m 21s"
        },
        %{
          id: "run_child_d4f6a3c1",
          short_id: "run_child_d4f6a3c1",
          status: :succeeded,
          raw_status: :ok,
          target: "daily_sales",
          window: "Feb 2026",
          progress: %{label: "20 / 20", title: "20 attempts"},
          started_at: "May 19 15:21",
          duration: "8m 03s"
        },
        %{
          id: "run_child_a1b3c5d9",
          short_id: "run_child_a1b3c5d9",
          status: :running,
          raw_status: :running,
          target: "daily_sales",
          window: "May 2026",
          progress: %{label: "9 / 20", title: "9 attempts"},
          started_at: "May 19 16:26",
          duration: "26m 14s"
        }
      ]
    }
  end

  def sample_summary(groups \\ sample_groups()) do
    %{
      total_groups: length(groups),
      total_windows: Enum.sum(Enum.map(groups, & &1.total_windows)),
      completed_windows: Enum.sum(Enum.map(groups, & &1.completed_windows)),
      total_asset_attempts: Enum.sum(Enum.map(groups, & &1.total_asset_attempts)),
      completed_asset_attempts: Enum.sum(Enum.map(groups, & &1.completed_asset_attempts)),
      failed_asset_attempts: Enum.sum(Enum.map(groups, & &1.failed_asset_attempts)),
      running_asset_attempts: Enum.sum(Enum.map(groups, & &1.running_asset_attempts)),
      queued_asset_attempts: Enum.sum(Enum.map(groups, & &1.queued_asset_attempts)),
      health: %{succeeded: 0, failed: 1, running: 1, queued: 0},
      last_updated: "May 19 16:55"
    }
  end

  def sample_filters do
    %{
      "search" => "",
      "status" => "all",
      "trigger" => "all",
      "target" => "all",
      "window" => "all",
      "only_failed" => "false",
      "only_running" => "false",
      "only_incomplete" => "false",
      "sort" => "started_desc"
    }
  end

  def sample_filter_options(groups \\ sample_groups()) do
    %{
      targets: groups |> Enum.flat_map(& &1.targets) |> Enum.uniq(),
      triggers: [:backfill, :manual, :schedule, :retry]
    }
  end

  def nav_items(active \\ :runs), do: AssetCataloguePage.nav_items(active)

  defp status_options do
    [
      {"All", "all"},
      {"Succeeded", "succeeded"},
      {"Failed", "failed"},
      {"Running", "running"},
      {"Queued", "queued"},
      {"Incomplete", "incomplete"},
      {"Partial", "partial"}
    ]
  end

  defp trigger_options(triggers) do
    [{"All", "all"} | Enum.map(triggers, &{label(&1), to_string(&1)})]
  end

  defp target_options(targets) do
    [{"All", "all"} | Enum.map(targets, &{&1, &1})]
  end

  defp sort_options do
    [
      {"Started desc", "started_desc"},
      {"Status priority", "status_priority"},
      {"Failed first", "failed_first"},
      {"Running first", "running_first"}
    ]
  end

  defp window_options do
    [{"All", "all"}, {"Has window", "has_window"}, {"No window", "no_window"}]
  end

  defp progress_class(:error), do: "progress-error"
  defp progress_class(:warning), do: "progress-warning"
  defp progress_class(:info), do: "progress-info"
  defp progress_class(_tone), do: "progress-success"

  defp status_badge_class(:succeeded), do: "badge-success"
  defp status_badge_class(:ok), do: "badge-success"
  defp status_badge_class(:running), do: "badge-info"
  defp status_badge_class(:queued), do: "badge-warning"
  defp status_badge_class(:pending), do: "badge-warning"
  defp status_badge_class(:incomplete), do: "badge-warning"
  defp status_badge_class(:partial), do: "badge-warning"
  defp status_badge_class(:failed), do: "badge-error"
  defp status_badge_class(:error), do: "badge-error"
  defp status_badge_class(:blocked), do: "badge-error"
  defp status_badge_class(:timed_out), do: "badge-error"
  defp status_badge_class(:cancelled), do: "badge-neutral"
  defp status_badge_class(:skipped_fresh), do: "badge-neutral"
  defp status_badge_class(_status), do: "badge-neutral"

  defp status_dot_class(:succeeded), do: "status-success"
  defp status_dot_class(:ok), do: "status-success"
  defp status_dot_class(:running), do: "status-info"
  defp status_dot_class(:queued), do: "status-warning"
  defp status_dot_class(:pending), do: "status-warning"
  defp status_dot_class(:incomplete), do: "status-warning"
  defp status_dot_class(:partial), do: "status-warning"
  defp status_dot_class(:failed), do: "status-error"
  defp status_dot_class(:error), do: "status-error"
  defp status_dot_class(:blocked), do: "status-error"
  defp status_dot_class(:timed_out), do: "status-error"
  defp status_dot_class(_status), do: "status-neutral"

  defp status_label(:succeeded), do: "Succeeded"
  defp status_label(:ok), do: "Succeeded"
  defp status_label(:running), do: "Running"
  defp status_label(:queued), do: "Queued"
  defp status_label(:pending), do: "Queued"
  defp status_label(:incomplete), do: "Incomplete"
  defp status_label(:partial), do: "Partial"
  defp status_label(:failed), do: "Failed"
  defp status_label(:error), do: "Failed"
  defp status_label(:blocked), do: "Blocked"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:skipped_fresh), do: "Skipped"
  defp status_label(:timed_out), do: "Timed out"
  defp status_label(_status), do: "Unknown"

  defp health_text_class("success"), do: "text-success"
  defp health_text_class("error"), do: "text-error"
  defp health_text_class("info"), do: "text-info"
  defp health_text_class("warning"), do: "text-warning"
  defp health_text_class(_tone), do: "text-base-content/70"

  defp health_status_class("success"), do: "status-success"
  defp health_status_class("error"), do: "status-error"
  defp health_status_class("info"), do: "status-info"
  defp health_status_class("warning"), do: "status-warning"
  defp health_status_class(_tone), do: "status-neutral"

  defp label(nil), do: "Unknown"

  defp label(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
