defmodule FavnView.Components.SchedulesPage do
  @moduledoc """
  Schedules list page components for operator schedule inspection.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation
  alias FavnView.Components.ScheduleUi

  attr :schedules, :list, required: true
  attr :all_schedules, :list, default: []
  attr :filters, :map, required: true
  attr :filter_options, :map, required: true

  attr :scope_choices, :list,
    required: true,
    doc: "see `FavnView.ScheduleFilters.scope_choices/2`"

  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true

  def schedules_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Schedules"
      subtitle="Manage and monitor pipeline schedules."
      nav_items={@nav_items}
      content_scroll?={false}
    >
      <div
        id="schedules-page"
        class="mx-auto flex min-h-0 w-full max-w-[120rem] flex-1 flex-col pb-24 lg:pb-0"
        data-testid="schedules-page"
      >
        <.loading_state :if={@loading} label="Loading schedules" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load schedules"
          description={@error}
          data-testid="schedules-error-state"
        />
        <div :if={!@loading && !@error} class="flex min-h-0 flex-1 flex-col gap-2.5 lg:gap-3">
          <.helper_text />
          <.table_panel
            count={length(@schedules)}
            count_label="schedules"
            data-testid="schedules-panel"
          >
            <:toolbar>
              <.filters_bar
                filters={@filters}
                filter_options={@filter_options}
                scope_choices={@scope_choices}
              />
            </:toolbar>

            <.empty_state
              :if={@all_schedules == []}
              title="No schedules found"
              description="Deploy a manifest with scheduled pipelines to see them here."
              icon="hero-calendar-days"
              data-testid="schedules-empty-state"
            />
            <.empty_state
              :if={@all_schedules != [] && @schedules == []}
              title="No schedules match these filters"
              description="Clear filters or try a broader search."
              icon="hero-funnel"
              data-testid="schedules-filtered-empty-state"
            /> <.schedules_table :if={@schedules != []} schedules={@schedules} />
            <.schedule_cards :if={@schedules != []} schedules={@schedules} />
          </.table_panel>
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  def helper_text(assigns) do
    ~H"""
    <p class="text-xs favn-text-muted">
      New schedules are disabled by default until activated.
    </p>
    """
  end

  @doc """
  Search and narrowing for the schedule list.

  Activation and runtime are the scope rail and nothing else. They had a select
  each as well, which is a second way to set the same field, and the counts make
  the rail the better one. What is left narrows a different axis: which pipeline,
  which window, and a search.
  """
  attr :filters, :map, required: true
  attr :filter_options, :map, required: true
  attr :scope_choices, :list, required: true

  def filters_bar(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_schedules"
      filters_id="schedule-filters"
      on_clear="clear_filters"
      adjusted?={narrowed?(@filters)}
      search_name="filters[search]"
      search_label="Search schedules"
      search_value={@filters["search"]}
    >
      <:scopes>
        <.scope_rail
          label="Schedule state"
          choices={@scope_choices}
          on_select="set_scope"
          data-testid="schedule-scopes"
        />
      </:scopes>

      <:filters>
        <.select_field
          name="filters[pipeline]"
          label="Pipeline"
          icon="hero-queue-list"
          options={pipeline_options(@filter_options.pipelines)}
          value={@filters["pipeline"]}
        />
        <.select_field
          name="filters[window]"
          label="Window"
          icon="hero-calendar-days"
          options={window_options(@filter_options.windows)}
          value={@filters["window"]}
        />
      </:filters>
    </.table_toolbar>
    """
  end

  @doc """
  Desktop table of schedules, on the shared list-screen standard.

  Thirteen columns fitted no viewport, so the ones that qualify each other are
  paired into the standard's two-line cells: the id under the name, the timezone
  under the cron, the policies under the window, and each timestamp under the
  state it belongs to. Nothing was dropped — the same facts sit in seven columns
  that fit, in the order an operator reads them.
  """
  attr :schedules, :list, required: true

  def schedules_table(assigns) do
    ~H"""
    <.data_table
      id="schedules-table"
      rows={@schedules}
      row_testid="schedule-row"
      row_navigate={&~p"/schedules/#{&1.route_id}"}
      fill?
      class="hidden lg:block"
      data-testid="schedules-table"
    >
      <:col :let={schedule} label="Schedule" class="w-64">
        <.stacked_cell
          primary={schedule.schedule_label}
          secondary={schedule.id}
          mono={:secondary}
          navigate={~p"/schedules/#{schedule.route_id}"}
        />
      </:col>

      <:col :let={schedule} label="Pipeline" class="max-w-52">
        <.stacked_cell primary={schedule.pipeline_label} tone={:muted} />
      </:col>

      <:col :let={schedule} label="Cadence" class="w-40">
        <.stacked_cell
          primary={schedule.cron}
          secondary={schedule.timezone}
          mono={:primary}
          tone={:muted}
        />
      </:col>

      <:col :let={schedule} label="Window" class="w-36">
        <.stacked_cell
          primary={schedule.window_label}
          secondary={policies_label(schedule)}
          tone={:muted}
        />
      </:col>

      <:col :let={schedule} label="Activation" class="w-40">
        <div class="min-w-0 space-y-1">
          <ScheduleUi.activation_badge
            state={schedule.activation_state}
            label={schedule.activation_label}
          />
          <p class="truncate text-[0.68rem] favn-text-subtle">
            Next {schedule.next_due_label}
          </p>
        </div>
      </:col>

      <:col :let={schedule} label="Runtime" class="w-44">
        <div class="min-w-0 space-y-1">
          <div class="flex flex-wrap items-center gap-1.5">
            <ScheduleUi.runtime_badge state={schedule.runtime_state} label={schedule.runtime_label} />
            <ScheduleUi.scheduler_error_badge error={schedule.last_scheduler_error} />
          </div>

          <p class="truncate text-[0.68rem] favn-text-subtle">
            <.link
              :if={schedule.in_flight_run_id}
              navigate={~p"/runs/#{schedule.in_flight_run_id}"}
              class="font-mono text-primary hover:underline"
            >
              {schedule.current_run_label}
            </.link>
            <span :if={!schedule.in_flight_run_id}>{schedule.last_submitted_label}</span>
          </p>
        </div>
      </:col>

      <:col :let={schedule} label="Updated" class="w-28">
        <.stacked_cell primary={schedule.updated_label} tone={:muted} />
      </:col>

      <:action :let={schedule}>
        <.copy_button
          value={schedule.id}
          title={"Copy #{schedule.id}"}
          size={:xs}
          data-testid="copy-schedule-id"
        />
      </:action>
    </.data_table>
    """
  end

  attr :schedules, :list, required: true

  def schedule_cards(assigns) do
    ~H"""
    <div class="space-y-2.5 p-3 lg:hidden" data-testid="schedule-card-list">
      <.schedule_card :for={schedule <- @schedules} schedule={schedule} />
    </div>
    """
  end

  attr :schedule, :map, required: true

  def schedule_card(assigns) do
    ~H"""
    <article
      class="card glass favn-surface-list favn-density-list-card rounded-box"
      data-testid="schedule-card"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 flex-1 space-y-2">
          <div class="flex items-start gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.icon name="hero-calendar-days" class="size-4" />
            </span>

            <div class="min-w-0">
              <.link
                navigate={~p"/schedules/#{@schedule.route_id}"}
                class="block truncate text-base font-medium leading-tight text-base-content hover:text-primary"
              >
                {@schedule.schedule_label}
              </.link>

              <p class="mt-0.5 truncate text-xs favn-text-muted">{@schedule.pipeline_label}</p>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-2 text-xs favn-text-muted">
            <ScheduleUi.activation_badge
              state={@schedule.activation_state}
              label={@schedule.activation_label}
            />
            <ScheduleUi.runtime_badge state={@schedule.runtime_state} label={@schedule.runtime_label} />
            <ScheduleUi.scheduler_error_badge error={@schedule.last_scheduler_error} />
            <span>{@schedule.cron}</span> <span>{@schedule.window_label}</span>
          </div>

          <p class="truncate font-mono text-xs favn-text-subtle" title={@schedule.id}>
            {@schedule.id}
          </p>
        </div>
        <.copy_button value={@schedule.id} title={"Copy #{@schedule.id}"} size={:xs} />
      </div>
    </article>
    """
  end

  @doc """
  The overlap and missed-run policies as one line.

  They were two badges in a column of their own. As words under the window they
  read as what they are — how this cadence behaves when it collides with itself
  or falls behind — and cost no width.
  """
  @spec policies_label(map()) :: String.t()
  def policies_label(schedule) do
    Enum.map_join([schedule.overlap, schedule.missed], " · ", &policy_label/1)
  end

  @doc """
  Whether anything narrows the list beyond its default view.

  This is what decides the clear control is worth showing: with nothing set there
  is nothing to clear, and a permanent one is a button that usually does nothing.

  ## Examples

      iex> FavnView.Components.SchedulesPage.narrowed?(%{"search" => "", "window" => "all"})
      false

      iex> FavnView.Components.SchedulesPage.narrowed?(%{"search" => "daily"})
      true
  """
  @spec narrowed?(map()) :: boolean()
  def narrowed?(filters) do
    Enum.any?(filters, fn
      {"search", value} -> String.trim(to_string(value)) != ""
      {_key, value} -> value not in ["all", nil]
    end)
  end

  def nav_items(active \\ :schedules), do: Navigation.items(active)

  def activation_options do
    [
      {"Activation", "all"},
      {"Pending activation", "pending_activation"},
      {"Enabled", "enabled"},
      {"Disabled", "disabled"},
      {"Needs review", "needs_review"},
      {"Retired", "retired"}
    ]
  end

  def runtime_options do
    [
      {"Runtime", "all"},
      {"Inactive", "inactive"},
      {"Idle", "idle"},
      {"Running", "running"},
      {"Queued", "queued"}
    ]
  end

  def pipeline_options(options), do: [{"Pipeline", "all"} | options]
  def window_options(options), do: [{"Window", "all"} | options]

  defp policy_label(nil), do: "-"

  defp policy_label(value),
    do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()
end
