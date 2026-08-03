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
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :status_options, :list, required: true

  def pipelines_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Pipelines"
      subtitle="Monitor active manifest pipelines"
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    >
      <div
        class="mx-auto flex w-full max-w-[120rem] flex-col pb-24 lg:min-h-0 lg:flex-1 lg:pb-0"
        data-testid="pipelines-page"
      >
        <.loading_state :if={@loading} label="Loading pipelines" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load pipelines"
          description={@error}
          data-testid="pipelines-error-state"
        />
        <div :if={!@loading && !@error} class="flex flex-col lg:min-h-0 lg:flex-1">
          <.table_panel
            count={length(@pipelines)}
            count_label="pipelines"
            data-testid="pipelines-panel"
          >
            <:toolbar>
              <.pipeline_filters filters={@filters} status_options={@status_options} />
            </:toolbar>

            <.empty_state
              :if={@pipelines == []}
              title="No pipelines found"
              description="Try changing the search or health filter."
              icon="hero-queue-list"
              data-testid="pipelines-empty-state"
            /> <.pipeline_table :if={@pipelines != []} pipelines={@pipelines} />
            <.pipeline_card_list :if={@pipelines != []} pipelines={@pipelines} />
          </.table_panel>
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :filters, :map, required: true
  attr :status_options, :list, required: true

  def pipeline_filters(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_pipelines"
      filters_id="pipeline-filters"
      on_clear="clear_filters"
      adjusted?={narrowed?(@filters)}
      search_name="filters[search]"
      search_label="Search pipelines"
      search_placeholder="Search pipelines or selected assets"
      search_value={@filters.search}
    >
      <:filters>
        <.select_field
          id="pipeline-status-filter"
          name="filters[status]"
          label="Health filter"
          icon="hero-heart"
          options={@status_options}
          value={@filters.status}
        />
      </:filters>
    </.table_toolbar>
    """
  end

  @doc """
  Desktop table of pipelines, on the shared list-screen standard.

  Deps and window used to be two columns of repeated words; they qualify the
  cadence rather than answering a question of their own, so they now sit under
  the window value in one cell and give the width back to the asset selection.
  """
  attr :pipelines, :list, required: true

  def pipeline_table(assigns) do
    ~H"""
    <.data_table
      id="pipelines-table"
      rows={@pipelines}
      row_testid="pipeline-row"
      row_navigate={&~p"/pipelines/#{FavnView.AssetRoute.to_param(&1.id)}"}
      fill?
      data-testid="pipelines-table"
    >
      <:col :let={pipeline} label="Pipeline" class="w-72">
        <div class="flex min-w-0 items-center gap-3">
          <span class="flex size-8 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
            <.icon name="hero-queue-list" class="size-4" />
          </span>

          <.stacked_cell
            primary={pipeline.name}
            secondary={pipeline.label}
            navigate={~p"/pipelines/#{FavnView.AssetRoute.to_param(pipeline.id)}"}
          />
        </div>
      </:col>

      <:col :let={pipeline} label="Window" class="w-40">
        <.stacked_cell
          primary={pipeline.window_label}
          secondary={pipeline.dependencies_label}
          tone={:muted}
        />
      </:col>

      <:col :let={pipeline} label="Selected assets" class="min-w-56 max-w-80">
        <.selected_assets pipeline={pipeline} />
      </:col>

      <:col :let={pipeline} label="Health" class="w-28">
        <.status_badge tone={pipeline.status} label={status_label(pipeline.status)} />
      </:col>

      <:col :let={pipeline} label="Last run" class="w-32">
        <.stacked_cell
          primary={pipeline.last_run_label}
          secondary={pipeline.runtime_label}
          tone={:muted}
        />
      </:col>
    </.data_table>
    """
  end

  attr :pipelines, :list, required: true

  def pipeline_card_list(assigns) do
    ~H"""
    <div class="space-y-2.5 p-3 lg:hidden" data-testid="pipeline-card-list">
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

          <div class="flex flex-wrap items-center gap-3 text-sm favn-text-muted">
            <.status_badge tone={@pipeline.status} label={status_label(@pipeline.status)} />
            <span>{@pipeline.last_run_label}</span> <span>{@pipeline.runtime_label}</span>
          </div>

          <p
            class="truncate text-sm favn-text-muted"
            title={Enum.join(@pipeline.selected_assets, ", ")}
          >
            {selected_assets_preview(@pipeline)}
          </p>
        </div>
      </div>
    </.list_card>
    """
  end

  @doc """
  What the pipeline resolves to: the first asset, and how many follow.

  A hover dropdown listing every asset used to live here. It cannot survive the
  shared table, whose header pins because the rows scroll — a scroll region clips
  any panel escaping its bounds. The runs table answers the same question with
  the first name plus a count, and the whole list stays reachable as a tooltip,
  so that is what this does. The pipeline's own page lists the assets in full.
  """
  attr :pipeline, :map, required: true

  def selected_assets(%{pipeline: %{selected_assets: []}} = assigns) do
    ~H"""
    <span class="text-sm favn-text-muted">No resolved assets</span>
    """
  end

  def selected_assets(assigns) do
    ~H"""
    <div class="flex min-w-0 items-center gap-2" title={Enum.join(@pipeline.selected_assets, ", ")}>
      <span class="truncate text-sm font-medium text-base-content">
        {List.first(@pipeline.selected_assets)}
      </span>

      <span :if={length(@pipeline.selected_assets) > 1} class="shrink-0 text-sm favn-text-subtle">
        +{length(@pipeline.selected_assets) - 1} more
      </span>
    </div>
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

  @doc """
  Whether anything narrows the list beyond its default view.

  ## Examples

      iex> FavnView.Components.PipelinesPage.narrowed?(%{search: "", status: "all"})
      false

      iex> FavnView.Components.PipelinesPage.narrowed?(%{search: "", status: "failed"})
      true
  """
  @spec narrowed?(map()) :: boolean()
  def narrowed?(filters) do
    String.trim(to_string(filters.search)) != "" or filters.status != "all"
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
