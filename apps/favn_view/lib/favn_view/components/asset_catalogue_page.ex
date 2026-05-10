defmodule FavnView.Components.AssetCataloguePage do
  @moduledoc """
  Asset catalogue page components for the first Favn operator catalogue screen.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :assets, :list, required: true
  attr :filters, :map, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :connection_options, :list, required: true
  attr :catalogue_options, :list, required: true

  def asset_catalogue_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Asset catalogue"
      subtitle="Browse and monitor all assets"
      nav_items={@nav_items}
    >
      <div class="mx-auto w-full max-w-6xl pb-24 lg:pb-0" data-testid="asset-catalogue-page">
        <.loading_state :if={@loading} />
        <.error_state :if={!@loading && @error} />

        <div :if={!@loading && !@error} class="space-y-3.5 lg:space-y-5">
          <div id="asset-filters" data-testid="asset-filters">
            <.asset_catalogue_filters
              filters={@filters}
              connection_options={@connection_options}
              catalogue_options={@catalogue_options}
            />
          </div>

          <.empty_state :if={@assets == []} />

          <GlassPanel.glass_panel :if={@assets != []} class="hidden overflow-hidden lg:block">
            <.asset_table assets={@assets} />
          </GlassPanel.glass_panel>

          <.asset_card_list :if={@assets != []} assets={@assets} />
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={catalogue_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :filters, :map, required: true
  attr :connection_options, :list, required: true
  attr :catalogue_options, :list, required: true

  def asset_catalogue_filters(assigns) do
    ~H"""
    <form
      phx-change="filter_assets"
      phx-submit="filter_assets"
      class="grid grid-cols-2 gap-2.5 lg:grid-cols-[1fr_12rem_12rem] lg:gap-3"
    >
      <label class="input input-sm favn-surface-control col-span-2 w-full gap-3 px-4 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary lg:col-span-1">
        <.icon name="hero-magnifying-glass" class="size-5 shrink-0 text-base-content/60" />
        <span class="sr-only">Search assets</span>
        <input
          id="asset-search"
          type="search"
          name="filters[search]"
          value={@filters.search}
          placeholder="Search assets"
          autocomplete="off"
          phx-debounce="200"
        />
      </label>

      <label class="select select-sm favn-surface-control w-full gap-2 px-3 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary">
        <.icon name="hero-circle-stack" class="size-5 shrink-0 text-base-content/65" />
        <span class="sr-only">Connection filter</span>
        <select
          name="filters[connection]"
          id="connection-filter"
          aria-label="Connection filter"
          class="appearance-none"
        >
          <option
            :for={{label, value} <- @connection_options}
            value={value}
            selected={@filters.connection == value}
          >
            {label}
          </option>
        </select>
        <.icon name="hero-chevron-down" class="size-5 shrink-0 text-base-content/65" />
      </label>

      <label class="select select-sm favn-surface-control w-full gap-2 px-3 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary">
        <.icon name="hero-folder" class="size-5 shrink-0 text-base-content/65" />
        <span class="sr-only">Catalogue filter</span>
        <select
          name="filters[catalogue]"
          id="catalogue-filter"
          aria-label="Catalogue filter"
          class="appearance-none"
        >
          <option
            :for={{label, value} <- @catalogue_options}
            value={value}
            selected={@filters.catalogue == value}
          >
            {label}
          </option>
        </select>
        <.icon name="hero-chevron-down" class="size-5 shrink-0 text-base-content/65" />
      </label>
    </form>
    """
  end

  attr :assets, :list, required: true

  def asset_table(assigns) do
    ~H"""
    <div class="overflow-x-auto p-5 sm:p-6">
      <table class="table table-lg" data-testid="asset-table">
        <thead>
          <tr class="border-base-content/10 text-base-content/65">
            <th class="font-medium">Asset name</th>
            <th class="font-medium">Connection</th>
            <th class="font-medium">Catalogue</th>
            <th class="font-medium">Type</th>
            <th class="font-medium">Status</th>
            <th class="font-medium">Last run</th>
            <th class="sr-only">Open</th>
          </tr>
        </thead>
        <tbody>
          <.asset_table_row :for={asset <- @assets} asset={asset} />
        </tbody>
      </table>
    </div>
    """
  end

  attr :asset, :map, required: true

  def asset_table_row(assigns) do
    ~H"""
    <tr
      class="group border-base-content/10 transition hover:bg-primary/10 focus-within:bg-primary/10"
      data-testid="asset-row"
    >
      <td>
        <.link
          navigate={~p"/assets/#{asset_route_id(@asset)}"}
          class="flex items-center gap-3 font-medium text-base-content focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
        >
          <span class="flex size-8 items-center justify-center rounded-field border border-success/25 bg-success/10 text-success">
            <.icon name={asset_type_icon(@asset.type)} class="size-4" />
          </span>
          {@asset.name}
        </.link>
      </td>
      <td>
        <span class="flex items-center gap-2 text-base-content/75">
          <.connection_icon connection={@asset.connection} />
          {connection_label(@asset.connection)}
        </span>
      </td>
      <td class="text-base-content/70">{@asset.catalogue}</td>
      <td class="text-base-content/70">{@asset.type}</td>
      <td><.status_badge status={@asset.status} /></td>
      <td class="text-base-content/70">{@asset.last_run_label}</td>
      <td class="text-right">
        <.icon_button
          navigate={~p"/assets/#{asset_route_id(@asset)}"}
          label={"Open #{@asset.name}"}
          icon="hero-chevron-right"
        />
      </td>
    </tr>
    """
  end

  attr :assets, :list, required: true

  def asset_card_list(assigns) do
    ~H"""
    <div class="space-y-2.5 lg:hidden" data-testid="asset-card-list">
      <.asset_card :for={asset <- @assets} asset={asset} />
    </div>
    """
  end

  attr :asset, :map, required: true

  def asset_card(assigns) do
    ~H"""
    <.link
      navigate={~p"/assets/#{asset_route_id(@asset)}"}
      class="card glass favn-surface-list favn-density-list-card block rounded-box transition hover:border-primary/40 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
      data-testid="asset-card"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 space-y-2">
          <div class="flex items-center gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.connection_icon connection={@asset.connection} />
            </span>
            <div class="min-w-0">
              <h2 class="truncate text-base font-medium leading-tight text-base-content">
                {@asset.name}
              </h2>
              <p class="mt-0.5 truncate text-xs text-base-content/60">
                {connection_label(@asset.connection)} · {@asset.catalogue} · {@asset.type}
              </p>
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-3 text-xs text-base-content/65">
            <.status_badge status={@asset.status} />
            <span>{@asset.last_run_label}</span>
          </div>
        </div>
        <.icon name="hero-chevron-right" class="mt-2 size-5 shrink-0 text-base-content/55" />
      </div>
    </.link>
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

  attr :connection, :string, required: true

  def connection_icon(assigns) do
    ~H"""
    <.icon
      name={connection_icon_name(@connection)}
      class={["size-5", connection_icon_class(@connection)]}
    />
    """
  end

  attr :navigate, :string, required: true
  attr :label, :string, required: true
  attr :icon, :string, required: true

  def icon_button(assigns) do
    ~H"""
    <.link
      navigate={@navigate}
      class="btn btn-ghost btn-circle favn-icon-button"
      aria-label={@label}
    >
      <.icon name={@icon} class="size-5" />
    </.link>
    """
  end

  def loading_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto flex min-h-64 max-w-2xl items-center justify-center p-10">
      <div class="text-center">
        <span class="loading loading-ring loading-lg text-primary"></span>
        <p class="mt-4 text-base-content/60">Loading assets</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def empty_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto max-w-2xl p-10 text-center" data-testid="asset-empty-state">
      <h2 class="text-xl font-medium">No assets found</h2>
      <p class="mt-2 text-base-content/60">Try changing the search or filters.</p>
    </GlassPanel.glass_panel>
    """
  end

  def error_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto max-w-2xl p-10 text-center" data-testid="asset-error-state">
      <h2 class="text-xl font-medium">Could not load assets</h2>
      <p class="mt-2 text-base-content/60">Retry</p>
    </GlassPanel.glass_panel>
    """
  end

  def catalogue_modes do
    [
      %{id: :list, label: "List", icon: "hero-list-bullet"},
      %{id: :tree, label: "Tree", icon: "hero-share", disabled: true},
      %{id: :filters, label: "Filters", icon: "hero-funnel", disabled: true},
      %{id: :more, label: "More", icon: "hero-ellipsis-vertical", disabled: true}
    ]
  end

  def nav_items(active \\ :assets) do
    [
      %{label: "Assets", icon: "hero-sparkles", href: "/assets", active: active == :assets},
      %{label: "Lineage", icon: "hero-share", href: "#"},
      %{label: "Storage", icon: "hero-circle-stack", href: "#"},
      %{label: "Runs", icon: "hero-rocket-launch", href: "/runs", active: active == :runs},
      %{label: "Logs", icon: "hero-document-text", href: "/logs", active: active == :logs},
      %{label: "Alerts", icon: "hero-bell", href: "#"},
      %{label: "Settings", icon: "hero-cog-6-tooth", href: "#"}
    ]
  end

  def sample_assets do
    [
      %{
        id: "customer_orders_daily",
        name: "customer_orders_daily",
        connection: "snowflake",
        catalogue: "sales",
        type: "table",
        status: :healthy,
        last_run_label: "6m ago"
      },
      %{
        id: "stg_orders",
        name: "stg_orders",
        connection: "snowflake",
        catalogue: "sales",
        type: "view",
        status: :healthy,
        last_run_label: "12m ago"
      },
      %{
        id: "stg_customers",
        name: "stg_customers",
        connection: "snowflake",
        catalogue: "sales",
        type: "view",
        status: :healthy,
        last_run_label: "18m ago"
      },
      %{
        id: "raw_payments",
        name: "raw_payments",
        connection: "s3",
        catalogue: "finance",
        type: "file",
        status: :running,
        last_run_label: "3m ago"
      },
      %{
        id: "mart_daily_sales",
        name: "mart_daily_sales",
        connection: "snowflake",
        catalogue: "sales",
        type: "table",
        status: :fresh,
        last_run_label: "Today 06:00"
      },
      %{
        id: "mart_customer_360",
        name: "mart_customer_360",
        connection: "duckdb",
        catalogue: "marketing",
        type: "table",
        status: :fresh,
        last_run_label: "Today 05:45"
      },
      %{
        id: "dq_orders_nulls",
        name: "dq_orders_nulls",
        connection: "snowflake",
        catalogue: "platform",
        type: "metric",
        status: :failed,
        last_run_label: "Failed 12m ago"
      },
      %{
        id: "alerts_revenue_drop",
        name: "alerts_revenue_drop",
        connection: "snowflake",
        catalogue: "finance",
        type: "metric",
        status: :missed,
        last_run_label: "Missed 1h ago"
      },
      %{
        id: "stg_payments",
        name: "stg_payments",
        connection: "postgres",
        catalogue: "finance",
        type: "table",
        status: :healthy,
        last_run_label: "24m ago"
      },
      %{
        id: "monthly_marketing_spend",
        name: "monthly_marketing_spend",
        connection: "s3",
        catalogue: "marketing",
        type: "file",
        status: :fresh,
        last_run_label: "Today 04:10"
      }
    ]
  end

  def connection_options do
    [
      {"Connection", "all"},
      {"Snowflake", "snowflake"},
      {"S3", "s3"},
      {"Postgres", "postgres"},
      {"DuckDB", "duckdb"}
    ]
  end

  def catalogue_options do
    [
      {"Catalogue", "all"},
      {"Sales", "sales"},
      {"Finance", "finance"},
      {"Platform", "platform"},
      {"Marketing", "marketing"}
    ]
  end

  defp status_badge_class(:healthy), do: "badge-success"
  defp status_badge_class(:fresh), do: "badge-info"
  defp status_badge_class(:running), do: "badge-warning"
  defp status_badge_class(:failed), do: "badge-error"
  defp status_badge_class(:missed), do: "badge-warning"
  defp status_badge_class(:unknown), do: "badge-neutral"
  defp status_badge_class(_status), do: "badge-neutral"

  defp status_dot_class(:healthy), do: "status-success"
  defp status_dot_class(:fresh), do: "status-info"
  defp status_dot_class(:running), do: "status-warning"
  defp status_dot_class(:failed), do: "status-error"
  defp status_dot_class(:missed), do: "status-warning"
  defp status_dot_class(:unknown), do: "status-neutral"
  defp status_dot_class(_status), do: "status-neutral"

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:fresh), do: "Fresh"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(:missed), do: "Missed"
  defp status_label(:unknown), do: "Unknown"
  defp status_label(_status), do: "Unknown"

  defp asset_route_id(asset), do: Map.get(asset, :route_id, asset.id)

  defp connection_label("s3"), do: "s3"
  defp connection_label(connection), do: connection

  defp connection_icon_name("snowflake"), do: "hero-sparkles"
  defp connection_icon_name("s3"), do: "hero-circle-stack"
  defp connection_icon_name("postgres"), do: "hero-circle-stack"
  defp connection_icon_name("duckdb"), do: "hero-circle-stack"
  defp connection_icon_name(_connection), do: "hero-circle-stack"

  defp connection_icon_class("snowflake"), do: "text-info"
  defp connection_icon_class("s3"), do: "text-success"
  defp connection_icon_class("postgres"), do: "text-info"
  defp connection_icon_class("duckdb"), do: "text-warning"
  defp connection_icon_class(_connection), do: "text-base-content/60"

  defp asset_type_icon("metric"), do: "hero-chart-bar"
  defp asset_type_icon("file"), do: "hero-document"
  defp asset_type_icon(_type), do: "hero-table-cells"
end
