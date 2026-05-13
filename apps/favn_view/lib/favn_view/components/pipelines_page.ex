defmodule FavnView.Components.PipelinesPage do
  @moduledoc """
  Pipelines list page components for scanning active manifest pipelines.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :pipelines, :list, required: true
  attr :filters, :map, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :status_options, :list, required: true

  def pipelines_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Pipelines"
      subtitle="Monitor active manifest pipelines"
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-6xl pb-24 lg:pb-0" data-testid="pipelines-page">
        <.loading_state :if={@loading} />
        <.error_state :if={!@loading && @error} />

        <div :if={!@loading && !@error} class="space-y-3.5 lg:space-y-5">
          <div id="pipeline-filters" data-testid="pipeline-filters">
            <.pipeline_filters filters={@filters} status_options={@status_options} />
          </div>

          <.empty_state :if={@pipelines == []} />

          <GlassPanel.glass_panel :if={@pipelines != []} class="hidden overflow-visible lg:block">
            <.pipeline_table pipelines={@pipelines} />
          </GlassPanel.glass_panel>

          <.pipeline_card_list :if={@pipelines != []} pipelines={@pipelines} />
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={pipeline_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :filters, :map, required: true
  attr :status_options, :list, required: true

  def pipeline_filters(assigns) do
    ~H"""
    <form
      phx-change="filter_pipelines"
      phx-submit="filter_pipelines"
      class="grid grid-cols-2 gap-2.5 lg:grid-cols-[1fr_12rem] lg:gap-3"
    >
      <label class="input input-sm favn-surface-control col-span-2 w-full gap-3 px-4 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary lg:col-span-1">
        <.icon name="hero-magnifying-glass" class="size-5 shrink-0 text-base-content/60" />
        <span class="sr-only">Search pipelines</span>
        <input
          id="pipeline-search"
          type="search"
          name="filters[search]"
          value={@filters.search}
          placeholder="Search pipelines or selected assets"
          autocomplete="off"
          phx-debounce="200"
        />
      </label>

      <label class="select select-sm favn-surface-control w-full gap-2 px-3 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary">
        <.icon name="hero-heart" class="size-5 shrink-0 text-base-content/65" />
        <span class="sr-only">Health filter</span>
        <select name="filters[status]" id="pipeline-status-filter" aria-label="Health filter">
          <option
            :for={{label, value} <- @status_options}
            value={value}
            selected={@filters.status == value}
          >
            {label}
          </option>
        </select>
        <.icon name="hero-chevron-down" class="size-5 shrink-0 text-base-content/65" />
      </label>
    </form>
    """
  end

  attr :pipelines, :list, required: true

  def pipeline_table(assigns) do
    ~H"""
    <div class="overflow-x-auto overflow-y-visible p-5 sm:p-6">
      <table class="table table-lg" data-testid="pipelines-table">
        <thead>
          <tr class="border-base-content/10 text-base-content/65">
            <th class="font-medium">Pipeline</th>
            <th class="font-medium">Deps</th>
            <th class="font-medium">Window</th>
            <th class="font-medium">Selected assets</th>
            <th class="font-medium">Health</th>
            <th class="font-medium">Last run</th>
            <th class="font-medium">Runtime</th>
          </tr>
        </thead>
        <tbody>
          <.pipeline_table_row :for={pipeline <- @pipelines} pipeline={pipeline} />
        </tbody>
      </table>
    </div>
    """
  end

  attr :pipeline, :map, required: true

  def pipeline_table_row(assigns) do
    ~H"""
    <tr class="group border-base-content/10 transition hover:bg-primary/10" data-testid="pipeline-row">
      <td>
        <.link
          navigate={~p"/pipelines/#{FavnView.AssetRoute.to_param(@pipeline.id)}"}
          class="flex items-center gap-3 font-medium text-base-content"
        >
          <span class="flex size-8 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
            <.icon name="hero-queue-list" class="size-4" />
          </span>
          <div class="min-w-0">
            <p class="truncate">{@pipeline.name}</p>
            <p class="truncate text-xs font-normal text-base-content/50" title={@pipeline.label}>
              {@pipeline.label}
            </p>
          </div>
        </.link>
      </td>
      <td class="whitespace-nowrap text-base-content/70">{@pipeline.dependencies_label}</td>
      <td class="whitespace-nowrap text-base-content/70">{@pipeline.window_label}</td>
      <td class="min-w-56 max-w-80">
        <.selected_assets pipeline={@pipeline} />
      </td>
      <td><.status_badge status={@pipeline.status} /></td>
      <td class="whitespace-nowrap text-base-content/70">{@pipeline.last_run_label}</td>
      <td class="whitespace-nowrap text-base-content/70">{@pipeline.runtime_label}</td>
    </tr>
    """
  end

  attr :pipelines, :list, required: true

  def pipeline_card_list(assigns) do
    ~H"""
    <div class="space-y-2.5 lg:hidden" data-testid="pipeline-card-list">
      <.pipeline_card :for={pipeline <- @pipelines} pipeline={pipeline} />
    </div>
    """
  end

  attr :pipeline, :map, required: true

  def pipeline_card(assigns) do
    ~H"""
    <.link
      navigate={~p"/pipelines/#{FavnView.AssetRoute.to_param(@pipeline.id)}"}
      class="card glass favn-surface-list favn-density-list-card block rounded-box"
      data-testid="pipeline-card"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 flex-1 space-y-2">
          <div class="flex items-start gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.icon name="hero-queue-list" class="size-4" />
            </span>
            <div class="min-w-0 flex-1">
              <h2 class="truncate text-base font-medium leading-tight text-base-content">
                {@pipeline.name}
              </h2>
              <p class="mt-0.5 truncate text-xs text-base-content/60">
                {@pipeline.dependencies_label} · {@pipeline.window_label} · {asset_count_label(
                  @pipeline
                )}
              </p>
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-3 text-xs text-base-content/65">
            <.status_badge status={@pipeline.status} />
            <span>{@pipeline.last_run_label}</span>
            <span>{@pipeline.runtime_label}</span>
          </div>
          <p
            class="truncate text-xs text-base-content/55"
            title={Enum.join(@pipeline.selected_assets, ", ")}
          >
            {selected_assets_preview(@pipeline)}
          </p>
        </div>
      </div>
    </.link>
    """
  end

  attr :pipeline, :map, required: true

  def selected_assets(%{pipeline: %{selected_assets: []}} = assigns) do
    ~H"""
    <span class="text-sm text-base-content/55">No resolved assets</span>
    """
  end

  def selected_assets(%{pipeline: %{selected_assets: [_single]}} = assigns) do
    ~H"""
    <p
      class="truncate text-sm font-medium text-base-content"
      title={List.first(@pipeline.selected_assets)}
    >
      {List.first(@pipeline.selected_assets)}
    </p>
    """
  end

  def selected_assets(assigns) do
    ~H"""
    <details class="dropdown dropdown-hover dropdown-bottom">
      <summary class="list-none marker:content-none">
        <span class="inline-flex max-w-full cursor-default items-center gap-2 align-middle">
          <span
            class="truncate text-sm font-medium text-base-content"
            title={List.first(@pipeline.selected_assets)}
          >
            {List.first(@pipeline.selected_assets)}
          </span>
          <span class="badge badge-xs badge-soft badge-info shrink-0">
            +{length(@pipeline.selected_assets) - 1}
          </span>
        </span>
      </summary>
      <div class="dropdown-content z-20 mt-2 w-80 rounded-box border border-base-content/10 bg-base-100 p-3 shadow-xl">
        <p class="mb-2 text-xs font-medium uppercase tracking-[0.2em] text-base-content/45">
          Selected assets
        </p>
        <ul class="space-y-1 text-xs text-base-content/75">
          <li :for={asset <- @pipeline.selected_assets} class="truncate" title={asset}>{asset}</li>
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

  def loading_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto flex min-h-64 max-w-2xl items-center justify-center p-10">
      <div class="text-center">
        <span class="loading loading-ring loading-lg text-primary"></span>
        <p class="mt-4 text-base-content/60">Loading pipelines</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def empty_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto max-w-2xl p-10 text-center"
      data-testid="pipelines-empty-state"
    >
      <h2 class="text-xl font-medium">No pipelines found</h2>
      <p class="mt-2 text-base-content/60">Try changing the search or health filter.</p>
    </GlassPanel.glass_panel>
    """
  end

  def error_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto max-w-2xl p-10 text-center"
      data-testid="pipelines-error-state"
    >
      <h2 class="text-xl font-medium">Could not load pipelines</h2>
      <p class="mt-2 text-base-content/60">Retry</p>
    </GlassPanel.glass_panel>
    """
  end

  def pipeline_modes do
    [
      %{id: :list, label: "List", icon: "hero-list-bullet"},
      %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  def sample_pipelines do
    [
      %{
        id: "pipeline:Elixir.FavnView.Pipelines.DailySales",
        name: "daily_sales",
        label: "FavnView.Pipelines.DailySales",
        selected_assets: ["raw_orders", "stg_orders", "customer_orders_daily", "mart_daily_sales"],
        asset_count: 4,
        dependencies: :all,
        dependencies_label: "Include deps",
        window_label: "Day Europe/Oslo",
        status: :healthy,
        status_label: "Healthy",
        last_run_label: "12m ago",
        runtime_label: "34.5 s"
      },
      %{
        id: "pipeline:Elixir.FavnView.Pipelines.Marketing",
        name: "marketing_refresh",
        label: "FavnView.Pipelines.Marketing",
        selected_assets: ["campaign_spend", "mart_customer_360"],
        asset_count: 2,
        dependencies: :none,
        dependencies_label: "Selected only",
        window_label: "No window",
        status: :running,
        status_label: "Running",
        last_run_label: "just now",
        runtime_label: "8.1 s"
      }
    ]
  end

  def status_options do
    [{"Health", "all"}, {"Healthy", "healthy"}, {"Running", "running"}, {"Failed", "failed"}]
  end

  def nav_items(active \\ :pipelines), do: AssetCataloguePage.nav_items(active)

  defp selected_assets_preview(%{selected_assets: []}), do: "No resolved assets"
  defp selected_assets_preview(%{selected_assets: assets}), do: Enum.join(assets, ", ")

  defp asset_count_label(%{asset_count: 1}), do: "1 asset"
  defp asset_count_label(%{asset_count: count}), do: "#{count} assets"

  defp status_badge_class(:healthy), do: "badge-success"
  defp status_badge_class(:running), do: "badge-info"
  defp status_badge_class(:failed), do: "badge-error"
  defp status_badge_class(:unknown), do: "badge-neutral"
  defp status_badge_class(_status), do: "badge-neutral"

  defp status_dot_class(:healthy), do: "status-success"
  defp status_dot_class(:running), do: "status-info"
  defp status_dot_class(:failed), do: "status-error"
  defp status_dot_class(_status), do: "status-neutral"

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(:unknown), do: "Unknown"
  defp status_label(_status), do: "Unknown"
end
