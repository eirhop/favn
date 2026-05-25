defmodule FavnView.Components.SchedulesPage do
  @moduledoc """
  Schedules list page components for operator schedule inspection.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail
  alias FavnView.Components.ScheduleUi

  attr :schedules, :list, required: true
  attr :all_schedules, :list, default: []
  attr :filters, :map, required: true
  attr :filter_options, :map, required: true
  attr :summary, :map, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true

  def schedules_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Schedules"
      subtitle="Manage and monitor pipeline schedules."
      nav_items={@nav_items}
      show_header?={false}
      content_scroll?={false}
    >
      <div
        id="schedules-page"
        class="mx-auto flex min-h-0 w-full max-w-[120rem] flex-1 flex-col pb-24 lg:pb-0"
        data-testid="schedules-page"
      >
        <.loading_state :if={@loading} />
        <.error_state :if={!@loading && @error} />

        <div :if={!@loading && !@error} class="flex min-h-0 flex-1 flex-col gap-2.5 lg:gap-3">
          <.helper_text />
          <.summary_band summary={@summary} />

          <GlassPanel.glass_panel
            class="flex min-h-0 flex-1 flex-col overflow-hidden p-3 sm:p-4"
            data-testid="schedules-panel"
          >
            <.filters_bar
              filters={@filters}
              filter_options={@filter_options}
              result_count={length(@schedules)}
            />

            <.empty_state :if={@all_schedules == []} />
            <.filtered_empty_state :if={@all_schedules != [] && @schedules == []} />
            <.schedules_table :if={@schedules != []} schedules={@schedules} />
            <.schedule_cards :if={@schedules != []} schedules={@schedules} />
          </GlassPanel.glass_panel>
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={schedules_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  def helper_text(assigns) do
    ~H"""
    <p class="text-xs text-base-content/55">
      New schedules are disabled by default until activated.
    </p>
    """
  end

  attr :summary, :map, required: true

  def summary_band(assigns) do
    ~H"""
    <section
      class="favn-surface-panel rounded-box border border-base-content/10 bg-base-100/35 px-4 py-3 shadow-[0_16px_60px_rgba(0,0,0,0.22)] sm:px-5 sm:py-4"
      data-testid="schedules-summary-band"
    >
      <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
        <.summary_metric label="Total schedules" value={@summary.total} caption="Active manifest" />
        <.summary_metric label="Enabled" value={@summary.enabled} caption="Operator enabled" />
        <.summary_metric
          label="Pending activation"
          value={@summary.pending_activation}
          caption="Awaiting review"
        />
        <.summary_metric label="Disabled" value={@summary.disabled} caption="Operator disabled" />
        <.summary_metric label="Running" value={@summary.running} caption="In flight" />
        <.summary_metric label="Queued" value={@summary.queued} caption="Waiting" />
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

  attr :filters, :map, required: true
  attr :filter_options, :map, required: true
  attr :result_count, :integer, required: true

  def filters_bar(assigns) do
    ~H"""
    <.form
      for={%{}}
      as={:filters}
      phx-change="filter_schedules"
      class="pb-3"
      data-testid="schedule-filters"
    >
      <div class="flex flex-wrap items-center gap-2">
        <label class="favn-surface-control input input-sm h-9 min-h-9 min-w-0 flex-1 items-center gap-2 rounded-field sm:min-w-72 lg:max-w-96">
          <.icon name="hero-magnifying-glass" class="size-4 text-base-content/45" />
          <input
            type="search"
            name="filters[search]"
            value={@filters["search"]}
            placeholder="Search schedules..."
            class="grow"
            phx-debounce="250"
            data-testid="schedule-search"
          />
        </label>

        <.select_control
          label="Activation"
          name="activation_state"
          value={@filters["activation_state"]}
          options={activation_options()}
        />
        <.select_control
          label="Runtime"
          name="runtime_state"
          value={@filters["runtime_state"]}
          options={runtime_options()}
        />
        <.select_control
          label="Pipeline"
          name="pipeline"
          value={@filters["pipeline"]}
          options={pipeline_options(@filter_options.pipelines)}
        />
        <.select_control
          label="Window"
          name="window"
          value={@filters["window"]}
          options={window_options(@filter_options.windows)}
        />

        <button
          type="button"
          phx-click="clear_filters"
          class="btn btn-sm favn-surface-control rounded-field"
          data-testid="clear-schedule-filters"
        >
          Clear
        </button>
        <span class="ml-auto text-xs text-base-content/45" data-testid="schedule-result-count">
          {@result_count} results
        </span>
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
    <label class="favn-surface-control flex h-9 min-h-9 min-w-36 items-center overflow-hidden rounded-field border border-base-content/10 bg-base-100/20 text-sm shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
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

  attr :schedules, :list, required: true

  def schedules_table(assigns) do
    ~H"""
    <div class="hidden min-h-0 flex-1 overflow-auto border-t border-base-content/10 xl:block">
      <table class="table table-sm" data-testid="schedules-table">
        <thead>
          <tr class="border-base-content/10 text-xs text-base-content/55">
            <th class="w-64 font-medium">Schedule</th>
            <th class="font-medium">Pipeline</th>
            <th class="font-medium">Cadence</th>
            <th class="font-medium">Window</th>
            <th class="font-medium">Policies</th>
            <th class="font-medium">Activation</th>
            <th class="font-medium">Runtime</th>
            <th class="font-medium">Issue</th>
            <th class="font-medium">Next due</th>
            <th class="font-medium">Last submitted</th>
            <th class="font-medium">Current run</th>
            <th class="font-medium">Updated</th>
            <th class="font-medium">Actions</th>
          </tr>
        </thead>
        <tbody>
          <.schedule_row :for={schedule <- @schedules} schedule={schedule} />
        </tbody>
      </table>
    </div>
    """
  end

  attr :schedule, :map, required: true

  def schedule_row(assigns) do
    ~H"""
    <tr
      class="group border-base-content/10 bg-base-100/5 text-sm transition hover:bg-primary/10"
      data-testid="schedule-row"
    >
      <td>
        <div class="min-w-0">
          <.link
            navigate={~p"/schedules/#{@schedule.route_id}"}
            class="block truncate font-medium text-base-content hover:text-primary"
          >
            {@schedule.schedule_label}
          </.link>
          <p class="truncate font-mono text-xs text-base-content/50" title={@schedule.id}>
            {@schedule.id}
          </p>
        </div>
      </td>
      <td class="max-w-52 truncate text-xs text-base-content/75" title={@schedule.pipeline_label}>
        {@schedule.pipeline_label}
      </td>
      <td class="whitespace-nowrap text-xs text-base-content/70">
        <p class="font-mono">{@schedule.cron}</p>
        <p class="text-base-content/45">{@schedule.timezone}</p>
      </td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@schedule.window_label}</td>
      <td><.policy_chips schedule={@schedule} /></td>
      <td>
        <ScheduleUi.activation_badge
          state={@schedule.activation_state}
          label={@schedule.activation_label}
        />
      </td>
      <td>
        <ScheduleUi.runtime_badge state={@schedule.runtime_state} label={@schedule.runtime_label} />
      </td>
      <td class="whitespace-nowrap text-xs text-base-content/70">
        <ScheduleUi.scheduler_error_badge error={@schedule.last_scheduler_error} />
      </td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@schedule.next_due_label}</td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@schedule.last_submitted_label}</td>
      <td class="whitespace-nowrap text-xs text-base-content/70">
        <.link
          :if={@schedule.in_flight_run_id}
          navigate={~p"/runs/#{@schedule.in_flight_run_id}"}
          class="font-mono text-primary hover:underline"
        >
          {@schedule.current_run_label}
        </.link>
        <span :if={!@schedule.in_flight_run_id}>-</span>
      </td>
      <td class="whitespace-nowrap text-xs text-base-content/70">{@schedule.updated_label}</td>
      <td>
        <button
          type="button"
          class="btn btn-ghost btn-xs favn-icon-button"
          data-copy-text={@schedule.id}
          aria-label={"Copy #{@schedule.id}"}
          data-testid="copy-schedule-id"
        >
          <.icon name="hero-clipboard-document" class="size-4" />
        </button>
      </td>
    </tr>
    """
  end

  attr :schedules, :list, required: true

  def schedule_cards(assigns) do
    ~H"""
    <div class="space-y-2.5 xl:hidden" data-testid="schedule-card-list">
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
              <p class="mt-0.5 truncate text-xs text-base-content/60">{@schedule.pipeline_label}</p>
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-2 text-xs text-base-content/65">
            <ScheduleUi.activation_badge
              state={@schedule.activation_state}
              label={@schedule.activation_label}
            />
            <ScheduleUi.runtime_badge state={@schedule.runtime_state} label={@schedule.runtime_label} />
            <ScheduleUi.scheduler_error_badge error={@schedule.last_scheduler_error} />
            <span>{@schedule.cron}</span>
            <span>{@schedule.window_label}</span>
          </div>
          <p class="truncate font-mono text-xs text-base-content/45" title={@schedule.id}>
            {@schedule.id}
          </p>
        </div>
        <button
          type="button"
          class="btn btn-ghost btn-xs favn-icon-button"
          data-copy-text={@schedule.id}
          aria-label={"Copy #{@schedule.id}"}
        >
          <.icon name="hero-clipboard-document" class="size-4" />
        </button>
      </div>
    </article>
    """
  end

  attr :schedule, :map, required: true

  def policy_chips(assigns) do
    ~H"""
    <div class="flex flex-wrap gap-1.5">
      <span class="badge badge-xs badge-soft badge-neutral">{policy_label(@schedule.overlap)}</span>
      <span class="badge badge-xs badge-soft badge-neutral">{policy_label(@schedule.missed)}</span>
    </div>
    """
  end

  def loading_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto flex min-h-64 max-w-2xl items-center justify-center p-10">
      <div class="text-center">
        <span class="loading loading-ring loading-lg text-primary"></span>
        <p class="mt-4 text-base-content/60">Loading schedules</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def empty_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto max-w-2xl p-10 text-center"
      data-testid="schedules-empty-state"
    >
      <h2 class="text-xl font-medium">No schedules found</h2>
      <p class="mt-2 text-base-content/60">
        Deploy a manifest with scheduled pipelines to see them here.
      </p>
    </GlassPanel.glass_panel>
    """
  end

  def filtered_empty_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto max-w-2xl p-10 text-center"
      data-testid="schedules-filtered-empty-state"
    >
      <h2 class="text-xl font-medium">No schedules match these filters</h2>
      <p class="mt-2 text-base-content/60">Clear filters or try a broader search.</p>
    </GlassPanel.glass_panel>
    """
  end

  def error_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto max-w-2xl p-10 text-center"
      data-testid="schedules-error-state"
    >
      <h2 class="text-xl font-medium">Could not load schedules</h2>
      <p class="mt-2 text-base-content/60">Retry</p>
    </GlassPanel.glass_panel>
    """
  end

  def schedules_modes do
    [
      %{id: :list, label: "List", icon: "hero-list-bullet"},
      %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  def nav_items(active \\ :schedules), do: AssetCataloguePage.nav_items(active)

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

  def sample_filters do
    %{
      "search" => "",
      "activation_state" => "all",
      "runtime_state" => "all",
      "pipeline" => "all",
      "window" => "all"
    }
  end

  def sample_filter_options do
    %{
      pipelines: [{"MyApp.Pipelines.Daily", "MyApp.Pipelines.Daily"}],
      windows: [{"Day", "day"}, {"No window", "none"}]
    }
  end

  def sample_summary(entries \\ sample_schedules()) do
    %{
      total: length(entries),
      enabled: Enum.count(entries, &(&1.activation_state == :enabled)),
      pending_activation: Enum.count(entries, &(&1.activation_state == :pending_activation)),
      disabled: Enum.count(entries, &(&1.activation_state == :disabled)),
      running: Enum.count(entries, &(&1.runtime_state == :running)),
      queued: Enum.count(entries, &(&1.runtime_state == :queued))
    }
  end

  def sample_schedules do
    [
      %{
        id: "schedule:MyApp.Pipelines.Daily:daily",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLkRhaWx5OmRhaWx5",
        schedule_label: "daily",
        pipeline_label: "MyApp.Pipelines.Daily",
        cron: "0 6 * * *",
        timezone: "Europe/Oslo",
        window_label: "Day Europe/Oslo",
        overlap: :forbid,
        missed: :skip,
        activation_state: :pending_activation,
        activation_label: "Pending activation",
        runtime_state: :inactive,
        runtime_label: "Inactive",
        next_due_label: "May 25 06:00",
        last_submitted_label: "-",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: nil,
        updated_label: "May 24 12:00"
      },
      %{
        id: "schedule:MyApp.Pipelines.Marketing:refresh",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLk1hcmtldGluZzpyZWZyZXNo",
        schedule_label: "refresh",
        pipeline_label: "MyApp.Pipelines.Marketing",
        cron: "*/15 * * * *",
        timezone: "Etc/UTC",
        window_label: "No window",
        overlap: :allow,
        missed: :one,
        activation_state: :enabled,
        activation_label: "Enabled",
        runtime_state: :running,
        runtime_label: "Running",
        next_due_label: "May 24 12:15",
        last_submitted_label: "May 24 12:00",
        in_flight_run_id: "run_8f3a2c",
        current_run_label: "run_8f3a2c",
        last_scheduler_error: nil,
        updated_label: "May 24 12:01"
      },
      %{
        id: "schedule:MyApp.Pipelines.Hourly:hourly",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLkhvdXJseTpob3VybHk",
        schedule_label: "hourly",
        pipeline_label: "MyApp.Pipelines.Hourly",
        cron: "0 * * * *",
        timezone: "Etc/UTC",
        window_label: "Hour Etc/UTC",
        overlap: :queue_one,
        missed: :one,
        activation_state: :needs_review,
        activation_label: "Needs review",
        runtime_state: :inactive,
        runtime_label: "Inactive",
        next_due_label: "May 24 13:00",
        last_submitted_label: "May 24 11:00",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: %{phase_label: "Submit run", message: "Window policy invalid"},
        updated_label: "May 24 12:03"
      },
      %{
        id: "schedule:MyApp.Pipelines.Monthly:monthly",
        route_id: "s-c2NoZWR1bGU6TXlBcHAuUGlwZWxpbmVzLk1vbnRobHk6bW9udGhseQ",
        schedule_label: "monthly",
        pipeline_label: "MyApp.Pipelines.Monthly",
        cron: "0 5 1 * *",
        timezone: "Europe/Oslo",
        window_label: "Month Europe/Oslo",
        overlap: :queue_one,
        missed: :one,
        activation_state: :disabled,
        activation_label: "Disabled",
        runtime_state: :queued,
        runtime_label: "Queued",
        next_due_label: "Jun 1 05:00",
        last_submitted_label: "May 1 05:00",
        in_flight_run_id: nil,
        current_run_label: nil,
        last_scheduler_error: nil,
        updated_label: "May 24 12:04"
      }
    ]
  end

  defp policy_label(nil), do: "-"

  defp policy_label(value),
    do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()
end
