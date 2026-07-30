defmodule FavnView.Components.PipelinesPage do
  @moduledoc """
  Pipelines list page components for scanning active manifest pipelines.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation

  attr :pipelines, :list, required: true
  attr :filters, :map, required: true
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
      <div class="mx-auto w-full max-w-[120rem] pb-24 lg:pb-0" data-testid="pipelines-page">
        <.loading_state :if={@loading} label="Loading pipelines" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load pipelines"
          description={@error}
          data-testid="pipelines-error-state"
        />
        <div :if={!@loading && !@error} class="space-y-3.5 lg:space-y-5">
          <div id="pipeline-filters" data-testid="pipeline-filters">
            <.pipeline_filters filters={@filters} status_options={@status_options} />
          </div>

          <.empty_state
            :if={@pipelines == []}
            title="No pipelines found"
            description="Try changing the search or health filter."
            icon="hero-queue-list"
            data-testid="pipelines-empty-state"
          />
          <.panel
            :if={@pipelines != []}
            padding={:none}
            class="hidden overflow-visible p-5 sm:p-6 lg:block"
          >
            <.pipeline_table pipelines={@pipelines} />
          </.panel>
          <.pipeline_card_list :if={@pipelines != []} pipelines={@pipelines} />
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :filters, :map, required: true
  attr :status_options, :list, required: true

  def pipeline_filters(assigns) do
    ~H"""
    <.filter_bar on_change="filter_pipelines" class="lg:grid-cols-[1fr_12rem]">
      <.search_field
        id="pipeline-search"
        name="filters[search]"
        label="Search pipelines"
        placeholder="Search pipelines or selected assets"
        value={@filters.search}
      />
      <.select_field
        id="pipeline-status-filter"
        name="filters[status]"
        label="Health filter"
        icon="hero-heart"
        options={@status_options}
        value={@filters.status}
      />
    </.filter_bar>
    """
  end

  attr :pipelines, :list, required: true

  def pipeline_table(assigns) do
    ~H"""
    <div class="overflow-x-auto overflow-y-visible p-5 sm:p-6">
      <table class="table table-lg" data-testid="pipelines-table">
        <thead>
          <tr class="border-base-content/10 favn-text-muted">
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

            <p class="truncate text-xs font-normal favn-text-subtle" title={@pipeline.label}>
              {@pipeline.label}
            </p>
          </div>
        </.link>
      </td>

      <td class="whitespace-nowrap favn-text-muted">{@pipeline.dependencies_label}</td>

      <td class="whitespace-nowrap favn-text-muted">{@pipeline.window_label}</td>

      <td class="min-w-56 max-w-80">
        <.selected_assets pipeline={@pipeline} />
      </td>

      <td><.status_badge tone={@pipeline.status} label={status_label(@pipeline.status)} /></td>

      <td class="whitespace-nowrap favn-text-muted">{@pipeline.last_run_label}</td>

      <td class="whitespace-nowrap favn-text-muted">{@pipeline.runtime_label}</td>
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
    <.list_card
      navigate={~p"/pipelines/#{FavnView.AssetRoute.to_param(@pipeline.id)}"}
      data-testid="pipeline-card"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 flex-1 space-y-2">
          <div class="flex items-start gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.icon name="hero-queue-list" class="size-4" />
            </span>

            <div class="min-w-0 flex-1">
              <.section_title>{@pipeline.name}</.section_title>

              <.meta>
                {@pipeline.dependencies_label} · {@pipeline.window_label} · {asset_count_label(
                  @pipeline
                )}
              </.meta>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-3 text-xs favn-text-muted">
            <.status_badge tone={@pipeline.status} label={status_label(@pipeline.status)} />
            <span>{@pipeline.last_run_label}</span> <span>{@pipeline.runtime_label}</span>
          </div>

          <p
            class="truncate text-xs favn-text-muted"
            title={Enum.join(@pipeline.selected_assets, ", ")}
          >
            {selected_assets_preview(@pipeline)}
          </p>
        </div>
      </div>
    </.list_card>
    """
  end

  attr :pipeline, :map, required: true

  def selected_assets(%{pipeline: %{selected_assets: []}} = assigns) do
    ~H"""
    <span class="text-sm favn-text-muted">No resolved assets</span>
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

      <div class="dropdown-content z-50 mt-2 w-80 rounded-box border border-base-content/10 bg-base-100 p-3 shadow-xl">
        <p class="mb-2 text-xs font-medium uppercase tracking-[0.2em] favn-text-subtle">
          Selected assets
        </p>

        <ul class="space-y-1 text-xs favn-text-muted">
          <li :for={asset <- @pipeline.selected_assets} class="truncate" title={asset}>{asset}</li>
        </ul>
      </div>
    </details>
    """
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

  def nav_items(active \\ :pipelines), do: Navigation.items(active)

  defp selected_assets_preview(%{selected_assets: []}), do: "No resolved assets"
  defp selected_assets_preview(%{selected_assets: assets}), do: Enum.join(assets, ", ")

  defp asset_count_label(%{asset_count: 1}), do: "1 asset"
  defp asset_count_label(%{asset_count: count}), do: "#{count} assets"

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(:unknown), do: "Unknown"
  defp status_label(_status), do: "Unknown"
end
