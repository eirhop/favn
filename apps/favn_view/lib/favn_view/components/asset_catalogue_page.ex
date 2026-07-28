defmodule FavnView.Components.AssetCataloguePage do
  @moduledoc """
  The asset catalogue screen.

  This is the reference example of a Favn page component: it renders the shell,
  chooses between the content, loading, empty, and error states, and composes
  section components. It contains no surface, badge, or button styling of its
  own — those come from `FavnView.UI`.

  The catalogue renders the same rows twice by viewport: a `data_table/1` on
  desktop and one `list_card/1` per asset on mobile. That is deliberate; a table
  narrowed to a phone is unscannable.

  Health, coverage, and target compatibility are three independent
  orchestrator-owned statuses, rendered as three separate glyphs by
  `asset_state_icons/1`. The view maps each to a tone, an icon, and a label; it
  never derives one from another.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.LineagePage
  alias FavnView.Components.ModeRail
  alias FavnView.Components.Navigation

  attr :assets, :list, required: true
  attr :filters, :map, required: true
  attr :active_mode, :atom, required: true
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :connection_options, :list, required: true
  attr :catalogue_options, :list, required: true
  attr :flash, :map, default: %{}
  attr :lineage_graph, :any, default: nil
  attr :lineage_inspector, :any, default: nil
  attr :lineage_loading, :boolean, default: false
  attr :lineage_error, :any, default: nil
  attr :lineage_search, :string, default: ""
  attr :lineage_zoom, :integer, default: 62
  attr :lineage_inspector_open?, :boolean, default: true
  attr :lineage_canvas_hook?, :boolean, default: true

  def asset_catalogue_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Asset catalogue"
      subtitle="Browse and monitor all assets"
      nav_items={@nav_items}
      flash={@flash}
      content_scroll?={@active_mode != :lineage}
    >
      <div
        class={[
          "mx-auto flex w-full max-w-[120rem] flex-1 flex-col",
          @active_mode == :lineage && "min-h-0 pb-20 lg:pb-0",
          @active_mode != :lineage && "pb-24 lg:pb-0"
        ]}
        data-testid="asset-catalogue-page"
      >
        <.loading_state :if={@loading} label="Loading assets" />

        <.error_state
          :if={!@loading && @error}
          title="Could not load assets"
          description={@error}
          data-testid="asset-error-state"
        />

        <.stack :if={!@loading && !@error && @active_mode == :list} gap={{:md, :lg}}>
          <div id="asset-filters" data-testid="asset-filters">
            <.asset_filters
              filters={@filters}
              connection_options={@connection_options}
              catalogue_options={@catalogue_options}
            />
          </div>

          <.empty_state
            :if={@assets == []}
            title="No assets found"
            description="Try changing the search or filters."
            icon="hero-magnifying-glass"
            data-testid="asset-empty-state"
          />

          <.panel :if={@assets != []} class="hidden overflow-hidden lg:block">
            <.asset_table assets={@assets} />
          </.panel>

          <.asset_card_list :if={@assets != []} assets={@assets} />
        </.stack>

        <LineagePage.lineage_explorer
          :if={!@loading && !@error && @active_mode == :lineage}
          graph={@lineage_graph}
          inspector={@lineage_inspector}
          view_mode={:all}
          search={@lineage_search}
          loading={@lineage_loading}
          error={@lineage_error}
          zoom={@lineage_zoom}
          inspector_open?={@lineage_inspector_open?}
          canvas_hook?={@lineage_canvas_hook?}
        />
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={catalogue_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  @doc """
  Search and narrowing controls for the catalogue.
  """
  attr :filters, :map, required: true
  attr :connection_options, :list, required: true
  attr :catalogue_options, :list, required: true

  def asset_filters(assigns) do
    ~H"""
    <.filter_bar on_change="filter_assets">
      <.search_field
        id="asset-search"
        name="filters[search]"
        label="Search assets"
        value={@filters.search}
      />
      <.select_field
        id="connection-filter"
        name="filters[connection]"
        label="Connection filter"
        icon="hero-circle-stack"
        options={@connection_options}
        value={@filters.connection}
      />
      <.select_field
        id="catalogue-filter"
        name="filters[catalogue]"
        label="Catalogue filter"
        icon="hero-folder"
        options={@catalogue_options}
        value={@filters.catalogue}
      />
    </.filter_bar>
    """
  end

  @doc """
  Desktop table of assets.
  """
  attr :assets, :list, required: true

  def asset_table(assigns) do
    ~H"""
    <.data_table
      id="asset-table"
      rows={@assets}
      row_testid="asset-row"
      row_navigate={&~p"/assets/#{asset_route_id(&1)}"}
      data-testid="asset-table"
    >
      <:col :let={asset} label="Asset name">
        <.link
          navigate={~p"/assets/#{asset_route_id(asset)}"}
          class="flex items-center gap-3 font-medium text-base-content focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
        >
          <span class="flex size-8 items-center justify-center rounded-field border border-success/25 bg-success/10 text-success">
            <.icon name={asset_type_icon(asset.type)} />
          </span>
          {asset.name}
        </.link>
      </:col>
      <:col :let={asset} label="Connection">
        <span class="flex items-center gap-2 favn-text-muted">
          <.connection_icon connection={asset.connection} />
          {asset.connection}
        </span>
      </:col>
      <:col :let={asset} label="Catalogue" class="favn-text-muted">{asset.catalogue}</:col>
      <:col :let={asset} label="Type" class="favn-text-muted">{asset.type}</:col>
      <:col :let={asset} label="State">
        <.asset_state_icons asset={asset} />
      </:col>
      <:col :let={asset} label="Last run" class="favn-text-muted">{asset.last_run_label}</:col>
    </.data_table>
    """
  end

  @doc """
  Mobile list of assets.
  """
  attr :assets, :list, required: true

  def asset_card_list(assigns) do
    ~H"""
    <.stack gap={:sm} class="lg:hidden" data-testid="asset-card-list">
      <.asset_card :for={asset <- @assets} asset={asset} />
    </.stack>
    """
  end

  @doc """
  One asset as a compact list row.
  """
  attr :asset, :map, required: true

  def asset_card(assigns) do
    ~H"""
    <.list_card navigate={~p"/assets/#{asset_route_id(@asset)}"} data-testid="asset-card">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 space-y-2">
          <div class="flex items-center gap-3">
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-primary/30 bg-primary/10 text-primary">
              <.connection_icon connection={@asset.connection} />
            </span>
            <div class="min-w-0">
              <.section_title>{@asset.name}</.section_title>
              <.meta>{@asset.connection} · {@asset.catalogue} · {@asset.type}</.meta>
            </div>
          </div>
          <.inline gap={:sm} class="text-xs favn-text-muted">
            <.asset_state_icons asset={@asset} />
            <span>{@asset.last_run_label}</span>
          </.inline>
        </div>
        <.icon name="hero-chevron-right" size={:md} class="mt-2 shrink-0 favn-text-muted" />
      </div>
    </.list_card>
    """
  end

  @doc """
  Health, data coverage, and target compatibility as three glyphs.

  These are three independent orchestrator-owned statuses, and the view never
  derives one from another. As words they were the widest thing in the row —
  three badges taking more space than the asset name they described, which is
  what pushed the row past a phone viewport. As glyphs they read at a glance and
  keep their meaning on hover and to a screen reader.

  Each state has its own icon rather than sharing one per tone, so the three
  dimensions stay distinguishable without relying on colour.
  """
  attr :asset, :map, required: true

  def asset_state_icons(assigns) do
    ~H"""
    <.inline gap={:xs} data-testid="asset-states">
      <.status_icon {health_glyph(Map.get(@asset, :status))} />
      <.status_icon {coverage_glyph(coverage_status(@asset))} />
      <.status_icon {compatibility_glyph(compatibility_status(@asset))} />
    </.inline>
    """
  end

  @doc """
  Data-coverage completeness, independent of health.
  """
  attr :status, :atom, required: true

  def coverage_badge(assigns) do
    ~H"""
    <.badge tone={@status} variant={:outline} data-testid="asset-coverage-status">
      Coverage {coverage_status_label(@status)}
    </.badge>
    """
  end

  @doc """
  Target compatibility, independent of health and coverage.
  """
  attr :status, :atom, required: true

  def compatibility_badge(assigns) do
    ~H"""
    <.badge tone={compatibility_tone(@status)} data-testid="asset-compatibility-status">
      {compatibility_status_label(@status)}
    </.badge>
    """
  end

  @doc """
  Icon for the data system an asset lives in.
  """
  attr :connection, :string, required: true

  def connection_icon(assigns) do
    ~H"""
    <.icon
      name={connection_icon_name(@connection)}
      size={:md}
      class={connection_icon_class(@connection)}
    />
    """
  end

  @doc """
  The catalogue's view modes.
  """
  def catalogue_modes do
    [
      %{id: :list, label: "List", icon: "hero-list-bullet"},
      %{id: :lineage, label: "Lineage", icon: "hero-share"}
    ]
  end

  @doc """
  Primary navigation items with the catalogue marked active.

  Kept as a thin delegate so LiveViews and stories have one obvious call.
  """
  def nav_items(active \\ :assets), do: Navigation.items(active)

  def sample_assets do
    [
      %{
        id: "customer_orders_daily",
        name: "customer_orders_daily",
        connection: "snowflake",
        catalogue: "sales",
        type: "table",
        status: :healthy,
        coverage_status: :complete,
        compatibility_status: :ready,
        last_run_label: "6m ago"
      },
      %{
        id: "stg_orders",
        name: "stg_orders",
        connection: "snowflake",
        catalogue: "sales",
        type: "view",
        status: :healthy,
        coverage_status: :incomplete,
        compatibility_status: :rebuild_available,
        last_run_label: "12m ago"
      },
      %{
        id: "stg_customers",
        name: "stg_customers",
        connection: "snowflake",
        catalogue: "sales",
        type: "view",
        status: :healthy,
        coverage_status: :unknown,
        compatibility_status: :uninitialized,
        last_run_label: "18m ago"
      },
      %{
        id: "raw_payments",
        name: "raw_payments",
        connection: "s3",
        catalogue: "finance",
        type: "file",
        status: :running,
        compatibility_status: :ready,
        last_run_label: "3m ago"
      },
      %{
        id: "mart_daily_sales",
        name: "mart_daily_sales",
        connection: "snowflake",
        catalogue: "sales",
        type: "table",
        status: :fresh,
        compatibility_status: :rebuild_required,
        last_run_label: "Today 06:00"
      },
      %{
        id: "mart_customer_360",
        name: "mart_customer_360",
        connection: "duckdb",
        catalogue: "marketing",
        type: "table",
        status: :fresh,
        compatibility_status: :unexpected_drift,
        last_run_label: "Today 05:45"
      },
      %{
        id: "dq_orders_nulls",
        name: "dq_orders_nulls",
        connection: "snowflake",
        catalogue: "platform",
        type: "metric",
        status: :failed,
        compatibility_status: :ready,
        last_run_label: "Failed 12m ago"
      },
      %{
        id: "alerts_revenue_drop",
        name: "alerts_revenue_drop",
        connection: "snowflake",
        catalogue: "finance",
        type: "metric",
        status: :missed,
        compatibility_status: :ready,
        last_run_label: "Missed 1h ago"
      },
      %{
        id: "stg_payments",
        name: "stg_payments",
        connection: "postgres",
        catalogue: "finance",
        type: "table",
        status: :healthy,
        compatibility_status: :operator_decision,
        last_run_label: "24m ago"
      },
      %{
        id: "monthly_marketing_spend",
        name: "monthly_marketing_spend",
        connection: "s3",
        catalogue: "marketing",
        type: "file",
        status: :fresh,
        compatibility_status: :ready,
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

  defp coverage_status(asset), do: Map.get(asset, :coverage_status, :unknown)
  defp compatibility_status(asset), do: Map.get(asset, :compatibility_status, :ready)

  # One glyph per state, not per tone. A reader who cannot separate green from
  # amber still sees a tick, a clock, and a cross as different shapes, and the
  # three dimensions do not borrow each other's icons.
  defp health_glyph(status) do
    case status do
      :healthy ->
        %{tone: :success, icon: "hero-check-circle", label: "Healthy"}

      :fresh ->
        %{tone: :success, icon: "hero-check-circle", label: "Fresh"}

      :running ->
        %{tone: :info, icon: "hero-arrow-path", label: "Running now"}

      :failed ->
        %{tone: :error, icon: "hero-x-circle", label: "The last run failed"}

      :missed ->
        %{tone: :warning, icon: "hero-clock", label: "Missed its window"}

      :stale ->
        %{tone: :warning, icon: "hero-clock", label: "Later than its freshness policy allows"}

      :degraded ->
        %{tone: :warning, icon: "hero-exclamation-triangle", label: "Running with reduced health"}

      _other ->
        %{
          tone: :neutral,
          icon: "hero-question-mark-circle",
          label: "Health unknown: it has not run yet"
        }
    end
  end

  defp coverage_glyph(status) do
    case status do
      :complete ->
        %{
          tone: :success,
          icon: "hero-squares-2x2",
          label: "Coverage complete: every declared window is materialised"
        }

      :incomplete ->
        %{
          tone: :warning,
          icon: "hero-exclamation-circle",
          label: "Coverage incomplete: declared windows are missing"
        }

      _other ->
        %{tone: :neutral, icon: "hero-question-mark-circle", label: "Coverage unknown"}
    end
  end

  defp compatibility_glyph(status) do
    case status do
      :ready ->
        %{tone: :success, icon: "hero-shield-check", label: "Target compatible with the contract"}

      :rebuild_available ->
        %{tone: :info, icon: "hero-arrow-path-rounded-square", label: "A rebuild is available"}

      :rebuild_required ->
        %{
          tone: :error,
          icon: "hero-arrow-path-rounded-square",
          label: "Rebuild required before writes"
        }

      :uninitialized ->
        %{tone: :neutral, icon: "hero-minus-circle", label: "Target not initialised yet"}

      :unexpected_drift ->
        %{
          tone: :error,
          icon: "hero-exclamation-triangle",
          label: "Target drift: it no longer matches the contract"
        }

      :operator_decision ->
        %{tone: :warning, icon: "hero-hand-raised", label: "Waiting on an operator decision"}

      _other ->
        %{tone: :neutral, icon: "hero-question-mark-circle", label: "Compatibility unknown"}
    end
  end

  defp coverage_status_label(:complete), do: "complete"
  defp coverage_status_label(:incomplete), do: "incomplete"
  defp coverage_status_label(_status), do: "unknown"

  defp compatibility_tone(:ready), do: :success
  defp compatibility_tone(:rebuild_available), do: :info
  defp compatibility_tone(:uninitialized), do: :neutral
  defp compatibility_tone(_blocking), do: :error

  defp compatibility_status_label(:ready), do: "Compatible"
  defp compatibility_status_label(:rebuild_available), do: "Rebuild available"
  defp compatibility_status_label(:uninitialized), do: "Not initialized"
  defp compatibility_status_label(:rebuild_required), do: "Rebuild required"
  defp compatibility_status_label(:unexpected_drift), do: "Target drift"
  defp compatibility_status_label(:operator_decision), do: "Operator decision"
  defp compatibility_status_label(_status), do: "Compatibility unknown"

  defp asset_route_id(asset), do: Map.get(asset, :route_id, asset.id)

  defp connection_icon_name("snowflake"), do: "hero-sparkles"
  defp connection_icon_name(_connection), do: "hero-circle-stack"

  defp connection_icon_class("snowflake"), do: "text-info"
  defp connection_icon_class("s3"), do: "text-success"
  defp connection_icon_class("postgres"), do: "text-info"
  defp connection_icon_class("duckdb"), do: "text-warning"
  defp connection_icon_class(_connection), do: "favn-text-muted"

  defp asset_type_icon("metric"), do: "hero-chart-bar"
  defp asset_type_icon("file"), do: "hero-document"
  defp asset_type_icon(_type), do: "hero-table-cells"
end
