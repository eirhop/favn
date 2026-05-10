defmodule FavnView.Components.RunsListPage do
  @moduledoc """
  Runs list page components for scanning recent Favn runs.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :runs, :list, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true

  def runs_list_page(assigns) do
    ~H"""
    <AppShell.app_shell title="Runs" subtitle="Recent orchestration activity" nav_items={@nav_items}>
      <div class="mx-auto w-full max-w-6xl pb-24 lg:pb-0" data-testid="runs-list-page">
        <.loading_state :if={@loading} />
        <.error_state :if={!@loading && @error} />

        <div :if={!@loading && !@error} class="space-y-3.5 lg:space-y-5">
          <.empty_state :if={@runs == []} />

          <GlassPanel.glass_panel :if={@runs != []} class="hidden overflow-visible lg:block">
            <.runs_table runs={@runs} />
          </GlassPanel.glass_panel>

          <.run_card_list :if={@runs != []} runs={@runs} />
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={runs_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :runs, :list, required: true

  def runs_table(assigns) do
    ~H"""
    <div class="overflow-x-auto overflow-y-visible p-5 sm:p-6">
      <table class="table table-lg" data-testid="runs-table">
        <thead>
          <tr class="border-base-content/10 text-base-content/65">
            <th class="w-44 font-medium">Run</th>
            <th class="font-medium">Target</th>
            <th class="font-medium">Status</th>
            <th class="font-medium">Trigger</th>
            <th class="font-medium">Window</th>
            <th class="font-medium">Progress</th>
            <th class="font-medium">Started</th>
            <th class="font-medium">Duration</th>
          </tr>
        </thead>
        <tbody>
          <.run_table_row :for={run <- @runs} run={run} />
        </tbody>
      </table>
    </div>
    """
  end

  attr :run, :map, required: true

  def run_table_row(assigns) do
    ~H"""
    <tr
      class="group border-base-content/10 transition hover:bg-primary/10 focus-within:bg-primary/10"
      data-testid="run-row"
      data-run-id={@run.id}
    >
      <td>
        <.run_id_link run={@run} />
      </td>
      <td class="min-w-64 max-w-96">
        <.target_cell run={@run} />
      </td>
      <td><.status_badge status={@run.raw_status} /></td>
      <td class="text-base-content/70">{@run.trigger}</td>
      <td class="max-w-40 truncate text-base-content/70" title={@run.window}>{@run.window}</td>
      <td><.progress_label progress={@run.progress} /></td>
      <td class="whitespace-nowrap text-base-content/70">{@run.started_at}</td>
      <td class="whitespace-nowrap text-base-content/70">{@run.duration}</td>
    </tr>
    """
  end

  attr :runs, :list, required: true

  def run_card_list(assigns) do
    ~H"""
    <div class="space-y-2.5 lg:hidden" data-testid="run-card-list">
      <.run_card :for={run <- @runs} run={run} />
    </div>
    """
  end

  attr :run, :map, required: true

  def run_card(assigns) do
    ~H"""
    <div
      class="card glass favn-surface-list favn-density-list-card block rounded-box"
      data-testid="run-card"
      data-run-id={@run.id}
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 flex-1 space-y-2">
          <div class="flex items-start gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.icon name="hero-rocket-launch" class="size-4" />
            </span>
            <div class="min-w-0 flex-1">
              <.run_id_link run={@run} />
              <div class="mt-1 min-w-0">
                <.target_cell run={@run} />
              </div>
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-3 text-xs text-base-content/65">
            <.status_badge status={@run.raw_status} />
            <span>{@run.trigger}</span>
            <span>{@run.window}</span>
            <.progress_label progress={@run.progress} />
          </div>
          <p class="text-xs text-base-content/55">{@run.started_at} · {@run.duration}</p>
        </div>
      </div>
    </div>
    """
  end

  attr :run, :map, required: true

  def run_id_link(assigns) do
    ~H"""
    <.link
      navigate={~p"/runs/#{@run.id}"}
      class="block max-w-40 font-mono text-sm font-medium text-base-content hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
      title={@run.id}
      data-testid="run-id-link"
    >
      <span class="block truncate">{@run.short_id}</span>
    </.link>
    """
  end

  attr :run, :map, required: true

  def target_cell(%{run: %{targets: [_single]}} = assigns) do
    ~H"""
    <p class="truncate text-sm font-medium text-base-content" title={@run.target}>{@run.target}</p>
    """
  end

  def target_cell(assigns) do
    ~H"""
    <details class="dropdown dropdown-hover dropdown-bottom">
      <summary class="list-none marker:content-none">
        <span class="inline-flex max-w-full cursor-default items-center gap-2 align-middle">
          <span class="truncate text-sm font-medium text-base-content" title={@run.target}>
            {@run.target}
          </span>
          <span class="badge badge-xs badge-soft badge-info shrink-0">
            +{length(@run.targets) - 1}
          </span>
        </span>
      </summary>
      <div class="dropdown-content z-20 mt-2 w-80 rounded-box border border-base-content/10 bg-base-100 p-3 shadow-xl">
        <p class="mb-2 text-xs font-medium uppercase tracking-[0.2em] text-base-content/45">
          Targets
        </p>
        <ul class="space-y-1 text-xs text-base-content/75">
          <li :for={target <- @run.targets} class="truncate" title={target}>{target}</li>
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

  attr :progress, :map, required: true

  def progress_label(assigns) do
    ~H"""
    <span class="whitespace-nowrap text-sm text-base-content/70" title={@progress.title}>
      {@progress.label}
    </span>
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
    <GlassPanel.glass_panel class="mx-auto max-w-2xl p-10 text-center" data-testid="runs-empty-state">
      <h2 class="text-xl font-medium">No runs yet</h2>
      <p class="mt-2 text-base-content/60">Submit an asset or pipeline to see run activity here.</p>
    </GlassPanel.glass_panel>
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
      %{id: :list, label: "List", icon: "hero-list-bullet"},
      %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  def sample_runs do
    [
      %{
        id: "run_01jrun_catalogue_very_long_identifier_for_table_overflow_testing",
        short_id: "run_01jr...esting",
        target: "FavnView.Assets.customer_orders_daily",
        targets: ["FavnView.Assets.customer_orders_daily"],
        raw_status: :running,
        trigger: "Asset",
        window: "window:day:2026-06-12",
        progress: %{label: "2/4 assets", title: "2 of 4 assets have reported results"},
        started_at: "Jun 12 08:14",
        duration: "31.2 s"
      },
      %{
        id: "run_01jpipeline_multi_target_very_long_identifier",
        short_id: "run_01jp...ifier",
        target: "FavnView.Assets.raw_orders",
        targets: [
          "FavnView.Assets.raw_orders",
          "FavnView.Assets.stg_orders",
          "FavnView.Assets.customer_orders_daily"
        ],
        raw_status: :ok,
        trigger: "Pipeline",
        window: "Latest complete day",
        progress: %{label: "3/3 assets", title: "3 of 3 assets have reported results"},
        started_at: "Jun 12 07:45",
        duration: "1.8 min"
      },
      %{
        id: "run_01jfailed_backfill_target",
        short_id: "run_01jf...arget",
        target: "FavnView.Assets.revenue_by_region",
        targets: ["FavnView.Assets.revenue_by_region"],
        raw_status: :error,
        trigger: "Backfill asset",
        window: "2026-06-01..2026-06-12",
        progress: %{label: "1/1 asset", title: "1 of 1 assets have reported results"},
        started_at: "Jun 12 06:03",
        duration: "9.4 s"
      }
    ]
  end

  def nav_items(active \\ :runs), do: AssetCataloguePage.nav_items(active)

  defp status_badge_class(:ok), do: "badge-success"
  defp status_badge_class(:running), do: "badge-info"
  defp status_badge_class(:pending), do: "badge-info"
  defp status_badge_class(:retrying), do: "badge-info"
  defp status_badge_class(:partial), do: "badge-warning"
  defp status_badge_class(:error), do: "badge-error"
  defp status_badge_class(:blocked), do: "badge-error"
  defp status_badge_class(:timed_out), do: "badge-error"
  defp status_badge_class(:cancelled), do: "badge-neutral"
  defp status_badge_class(:skipped_fresh), do: "badge-neutral"
  defp status_badge_class(_status), do: "badge-neutral"

  defp status_dot_class(:ok), do: "status-success"
  defp status_dot_class(:running), do: "status-info"
  defp status_dot_class(:pending), do: "status-info"
  defp status_dot_class(:retrying), do: "status-info"
  defp status_dot_class(:partial), do: "status-warning"
  defp status_dot_class(:error), do: "status-error"
  defp status_dot_class(:blocked), do: "status-error"
  defp status_dot_class(:timed_out), do: "status-error"
  defp status_dot_class(_status), do: "status-neutral"

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:running), do: "Running"
  defp status_label(:retrying), do: "Retrying"
  defp status_label(:pending), do: "Pending"
  defp status_label(:partial), do: "Partial"
  defp status_label(:error), do: "Failed"
  defp status_label(:blocked), do: "Blocked"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:skipped_fresh), do: "Skipped fresh"
  defp status_label(:timed_out), do: "Timed out"
  defp status_label(_status), do: "Unknown"
end
