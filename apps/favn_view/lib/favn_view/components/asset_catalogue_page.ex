defmodule FavnView.Components.AssetCataloguePage do
  @moduledoc """
  The asset catalogue screen.

  This is the reference example of a Favn page component: it renders the shell,
  chooses between the content, loading, empty, and error states, and composes
  section components. It contains no surface, badge, or button styling of its
  own — those come from `FavnView.UI`.

  Assets are one table, sorted by connection, catalogue, then name, so the
  namespace still reads as the natural grouping without costing a disclosure
  per group. Namespace narrowing lives in the filters instead, and the two
  selects are dependent: choosing a connection narrows the catalogue options
  to that connection's catalogues, so an empty combination cannot be chosen.

  The rows are rendered twice by viewport: a `data_table/1` on desktop and one
  `list_card/1` per asset on mobile. That is deliberate; a table narrowed to a
  phone is unscannable.

  Health, coverage, and target compatibility are three independent
  orchestrator-owned statuses, rendered as three separate glyphs by
  `asset_state_icons/1`. The view maps each to a tone, an icon, and a label; it
  never derives one from another.
  """

  use FavnView, :html

  alias FavnView.AssetCatalogueFilters
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
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :connection_options, :list, required: true
  attr :catalogue_options, :list, required: true
  attr :schema_options, :list, required: true

  attr :scope_choices, :list,
    required: true,
    doc: "see `FavnView.AssetCatalogueFilters.scope_choices/2`"

  attr :filters_open?, :boolean, default: false, doc: "narrow screens only; wide ones always show"
  attr :flash, :map, default: %{}
  attr :lineage_graph, :any, default: nil
  attr :lineage_inspector, :any, default: nil
  attr :lineage_loading, :boolean, default: false
  attr :lineage_error, :any, default: nil
  attr :lineage_zoom, :integer, default: 62
  attr :lineage_inspector_open?, :boolean, default: true
  attr :lineage_canvas_hook?, :boolean, default: true

  def asset_catalogue_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Asset catalogue"
      subtitle="Browse and monitor all assets"
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
      content_scroll?={@active_mode != :lineage}
    >
      <div
        class={
          [
            "mx-auto flex w-full max-w-[120rem] flex-1 flex-col",
            @active_mode == :lineage && "min-h-0 pb-20 lg:pb-0",
            # `lg:min-h-0` is what lets the panel stop growing, so the table's own
            # region scrolls and its header pins. Without it the whole page grows
            # and the shell scrolls instead, taking the header with it.
            @active_mode != :lineage && "pb-24 lg:min-h-0 lg:pb-0"
          ]
        }
        data-testid="asset-catalogue-page"
      >
        <.loading_state :if={@loading} label="Loading assets" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load assets"
          description={@error}
          data-testid="asset-error-state"
        />
        <div
          :if={!@loading && !@error && @active_mode == :list}
          class="flex flex-col lg:min-h-0 lg:flex-1"
        >
          <.table_panel count={length(@assets)} count_label="assets" data-testid="assets-panel">
            <:toolbar>
              <.asset_filters
                filters={@filters}
                connection_options={@connection_options}
                catalogue_options={@catalogue_options}
                schema_options={@schema_options}
                scope_choices={@scope_choices}
                filters_open?={@filters_open?}
              />
            </:toolbar>

            <.empty_state
              :if={@assets == []}
              title="No assets found"
              description="Try changing the search or filters."
              icon="hero-magnifying-glass"
              data-testid="asset-empty-state"
            /> <.asset_table :if={@assets != []} assets={sorted_assets(@assets)} />
            <.asset_card_list :if={@assets != []} assets={sorted_assets(@assets)} />
          </.table_panel>
        </div>

        <LineagePage.lineage_explorer
          :if={!@loading && !@error && @active_mode == :lineage}
          graph={@lineage_graph}
          inspector={@lineage_inspector}
          view_mode={:all}
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
  attr :schema_options, :list, required: true
  attr :scope_choices, :list, required: true
  attr :filters_open?, :boolean, default: false

  def asset_filters(assigns) do
    ~H"""
    <.table_toolbar
      on_change="filter_assets"
      filters_id="asset-filters"
      filters_open?={@filters_open?}
      on_clear="clear_filters"
      adjusted?={AssetCatalogueFilters.narrowed?(@filters)}
      search_name="filters[search]"
      search_label="Search assets"
      search_value={@filters.search}
    >
      <:scopes>
        <.scope_rail
          label="Asset state"
          choices={@scope_choices}
          on_select="set_scope"
          data-testid="asset-scopes"
        />
      </:scopes>

      <:filters>
        <.select_field
          id="connection-filter"
          name="filters[connection]"
          label="Connection filter"
          icon="hero-circle-stack"
          options={@connection_options}
          value={@filters.connection}
        />
        <!-- A level whose options are only the "all" placeholder cannot narrow
        anything, and a select that changes nothing is worse than a missing one. -->
        <.select_field
          :if={length(@catalogue_options) > 1}
          id="catalogue-filter"
          name="filters[catalogue]"
          label="Catalogue filter"
          icon="hero-folder"
          options={@catalogue_options}
          value={@filters.catalogue}
        />
        <.select_field
          :if={length(@schema_options) > 1}
          id="schema-filter"
          name="filters[schema]"
          label="Schema filter"
          icon="hero-rectangle-stack"
          options={@schema_options}
          value={@filters.schema}
        />
      </:filters>
    </.table_toolbar>
    """
  end

  @doc """
  Sorts assets by their namespace address, then by name.

  The namespace is the sort key rather than a group header, so the hierarchy
  still reads top to bottom without costing a disclosure per group. Assets whose
  address is inferred from the module sort after every declared one: an inferred
  address addresses nothing, so interleaving the two would imply a hierarchy the
  catalogue cannot resolve.

  ## Examples

      iex> alias FavnView.Components.AssetCataloguePage
      iex> assets = [
      ...>   %{name: "d", module_path: ["lifecycle"]},
      ...>   %{name: "c", connection: "postgres", catalogue: "crm", schema: "sales"},
      ...>   %{name: "b", connection: "duckdb", catalogue: "raw", schema: "sales"},
      ...>   %{name: "a", connection: "duckdb", catalogue: "raw", schema: nil}
      ...> ]
      iex> AssetCataloguePage.sorted_assets(assets) |> Enum.map(& &1.name)
      ["a", "b", "c", "d"]
  """
  @spec sorted_assets([map()]) :: [map()]
  def sorted_assets(assets) do
    Enum.sort_by(assets, fn asset ->
      {(namespace_inferred?(asset) && 1) || 0, Enum.map(namespace_levels(asset), & &1.value),
       Map.get(asset, :name) || ""}
    end)
  end

  @doc """
  Desktop table of all assets.

  Namespace is one column — the relation address with the connection's icon —
  not one column per level: the levels are read as a single address, and columns
  of repeated words are what made the old table too wide to scan.
  """
  attr :assets, :list, required: true

  def asset_table(assigns) do
    ~H"""
    <.data_table
      id="asset-table"
      rows={@assets}
      row_testid="asset-row"
      row_navigate={&~p"/assets/#{asset_route_id(&1)}"}
      fill?
      desktop_only?
      data-testid="asset-table"
    >
      <:col :let={asset} label="Asset" class="w-80">
        <div class="flex min-w-0 items-center gap-3">
          <span class="flex size-8 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
            <.icon name={asset_type_icon(asset.type)} />
          </span>

          <.stacked_cell
            primary={asset.name}
            secondary={asset.type}
            navigate={~p"/assets/#{asset_route_id(asset)}"}
          />
        </div>
      </:col>

      <:col :let={asset} label="Namespace" class="w-64">
        <.asset_namespace asset={asset} />
      </:col>

      <:col :let={asset} label="State" class="w-28">
        <.asset_state_icons asset={asset} />
      </:col>

      <:col :let={asset} label="Last run" class="w-32 favn-text-muted">{asset.last_run_label}</:col>
    </.data_table>
    """
  end

  @doc """
  Mobile list of assets.
  """
  attr :assets, :list, required: true
  attr :class, :any, default: nil

  def asset_card_list(assigns) do
    ~H"""
    <.stack gap={:sm} class={["p-3 lg:hidden", @class]} data-testid="asset-card-list">
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
            <span class="favn-density-list-card-icon flex shrink-0 items-center justify-center rounded-field border border-secondary/30 bg-secondary/10">
              <.icon
                :if={namespace_inferred?(@asset)}
                name="hero-code-bracket-square"
                class="favn-text-subtle"
              />
              <.connection_icon
                :if={!namespace_inferred?(@asset)}
                connection={@asset.connection}
                class="text-secondary"
              />
            </span>

            <div class="min-w-0">
              <.section_title>{@asset.name}</.section_title>

              <.inline gap={:xs} class="min-w-0">
                <.asset_namespace asset={@asset} icon={false} />
                <span class="shrink-0 font-mono text-sm favn-text-subtle">· {@asset.type}</span>
              </.inline>
            </div>
          </div>

          <.inline gap={:sm} class="text-sm favn-text-muted">
            <.asset_state_icons asset={@asset} /> <span>{@asset.last_run_label}</span>
          </.inline>
        </div>
        <.icon name="hero-chevron-right" size={:md} class="mt-2 shrink-0 favn-text-muted" />
      </div>
    </.list_card>
    """
  end

  @doc """
  An asset's namespace: `connection · catalogue · schema`.

  Only levels that exist are shown. An earlier version substituted `unknown` and
  `uncatalogued` for missing ones, which read as populated levels and hid
  `schema` — the level most assets do declare — behind a placeholder for one
  they do not.

  An asset that declares no relation levels shows its module path instead, since a
  well-structured project's modules mirror its relation levels. The icon says
  which: a data-system glyph for an address that exists physically, a code glyph
  for one inferred from the module. That distinction matters — the second is not
  somewhere you can go and query.

  `namespace_defect/1` marks the one combination that cannot work: a catalogue
  without a schema. `Favn.SQLAsset` refuses to render `catalog.name`, so such an
  asset is misconfigured rather than merely sparse, and the catalogue is where an
  operator sees it first.
  """
  attr :asset, :map, required: true

  attr :icon, :boolean,
    default: true,
    doc: "set false where the surrounding row already shows the connection glyph"

  def asset_namespace(assigns) do
    assigns =
      assigns
      |> assign(:levels, namespace_levels(assigns.asset))
      |> assign(:defect, namespace_defect(assigns.asset))
      |> assign(:module?, namespace_inferred?(assigns.asset))

    ~H"""
    <span
      :if={@levels == []}
      class="inline-flex min-w-0 items-center gap-1.5 text-sm favn-text-subtle"
      title="This asset declares no relation levels and its module has no namespace segments."
      data-testid="asset-namespace"
    >
      <.icon name="hero-minus-circle" size={:sm} /> No namespace
    </span>

    <span
      :if={@levels != []}
      class="inline-flex min-w-0 items-center gap-1.5 font-mono text-sm"
      title={namespace_title(@levels, @module?)}
      data-testid="asset-namespace"
      data-namespace-source={(@module? && "module") || "relation"}
    >
      <.icon
        :if={@icon && @module?}
        name="hero-code-bracket-square"
        size={:sm}
        class="shrink-0 favn-text-subtle"
      />
      <.connection_icon
        :if={@icon && !@module?}
        connection={@asset.connection}
        size={:sm}
        class="shrink-0 text-secondary"
      />
      <span :for={{level, index} <- Enum.with_index(@levels)} class="inline-flex min-w-0 gap-1.5">
        <span :if={index > 0} class="favn-text-subtle">·</span>
        <span class={["truncate", level_class(level.level)]}>
          {level.value}
        </span>
      </span>

      <span :if={@defect} data-testid="asset-namespace-defect" class="inline-flex">
        <.status_icon tone={:error} icon="hero-exclamation-triangle" label={@defect} />
      </span>
    </span>
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

  One glyph for every connection, deliberately: connection names are chosen by
  the project author (`warehouse`, `sf_prod`), so mapping names to vendor
  icons guesses wrong everywhere except the examples. A real per-system icon
  has to come from what the user configured — the adapter behind the
  connection declaring one — which needs facade support; recorded in the
  operator UX review. Colour is the caller's: in the catalogue the whole
  connection segment wears `text-secondary`.
  """
  attr :connection, :string, default: nil, doc: "nil when the asset owns no relation"
  attr :size, :atom, default: :md, values: [:xs, :sm, :md, :lg]
  attr :class, :any, default: "favn-text-muted"

  def connection_icon(assigns) do
    ~H"""
    <.icon name="hero-circle-stack" size={@size} class={@class} />
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
        catalogue: "mart",
        schema: "sales",
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
        catalogue: "raw",
        schema: "sales",
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
        catalogue: "raw",
        schema: "sales",
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
        catalogue: "raw",
        schema: "finance",
        type: "file",
        status: :running,
        compatibility_status: :ready,
        last_run_label: "3m ago"
      },
      %{
        id: "mart_daily_sales",
        name: "mart_daily_sales",
        connection: "snowflake",
        catalogue: "mart",
        schema: "sales",
        type: "table",
        status: :fresh,
        compatibility_status: :rebuild_required,
        last_run_label: "Today 06:00"
      },
      %{
        id: "mart_customer_360",
        name: "mart_customer_360",
        connection: "duckdb",
        catalogue: "mart",
        schema: "marketing",
        type: "table",
        status: :fresh,
        compatibility_status: :unexpected_drift,
        last_run_label: "Today 05:45"
      },
      %{
        id: "dq_orders_nulls",
        name: "dq_orders_nulls",
        connection: "snowflake",
        catalogue: "core",
        schema: "platform",
        type: "metric",
        status: :failed,
        compatibility_status: :ready,
        last_run_label: "Failed 12m ago"
      },
      # Three shapes the real catalogue contains and the address has to survive:
      # an asset whose namespace comes from its module rather than a relation, one
      # with neither, and one whose catalogue has no schema and cannot resolve.
      %{
        id: "land_crm_activities",
        name: "activities",
        module_path: ["landing", "crm", "daily"],
        type: "elixir",
        status: :healthy,
        compatibility_status: :ready,
        last_run_label: "9m ago"
      },
      %{
        id: "retry_probe",
        name: "retry_probe",
        module_path: [],
        type: "elixir",
        status: :missed,
        compatibility_status: :ready,
        last_run_label: "31m ago"
      },
      %{
        id: "stg_payments",
        name: "stg_payments",
        connection: "postgres",
        catalogue: "raw",
        schema: nil,
        type: "table",
        status: :healthy,
        compatibility_status: :operator_decision,
        last_run_label: "24m ago"
      },
      %{
        id: "monthly_marketing_spend",
        name: "monthly_marketing_spend",
        connection: "s3",
        catalogue: "core",
        schema: "marketing",
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
      {"Raw", "raw"},
      {"Core", "core"},
      {"Mart", "mart"}
    ]
  end

  def schema_options do
    [
      {"Schema", "all"},
      {"Sales", "sales"},
      {"Finance", "finance"},
      {"Platform", "platform"},
      {"Marketing", "marketing"}
    ]
  end

  @doc """
  The namespace levels to show for an asset, widest first.

  The levels an asset's relation declares, and when it declares none, its module
  path — which in a well-structured project mirrors the same hierarchy. A relation
  carrying only a `name` is legal and declares no levels, so the fallback keys off
  the levels being empty rather than off the relation being absent.

  ## Examples

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_levels(
      ...>   %{connection: "warehouse", catalogue: nil, schema: "sales"}
      ...> )
      [%{level: :connection, value: "warehouse"}, %{level: :schema, value: "sales"}]

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_levels(%{module_path: ["lifecycle", "probes"]})
      [%{level: :module, value: "lifecycle"}, %{level: :module, value: "probes"}]
  """
  @spec namespace_levels(map()) :: [%{level: atom(), value: String.t()}]
  def namespace_levels(asset) do
    case relation_levels(asset) do
      [] -> asset |> Map.get(:module_path, []) |> Enum.map(&%{level: :module, value: &1})
      levels -> levels
    end
  end

  @doc """
  Whether an asset's namespace was inferred from its module rather than declared.

  ## Examples

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_inferred?(%{module_path: ["lifecycle"]})
      true

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_inferred?(
      ...>   %{connection: "warehouse", module_path: ["warehouse"]}
      ...> )
      false
  """
  @spec namespace_inferred?(map()) :: boolean()
  def namespace_inferred?(asset), do: relation_levels(asset) == []

  @doc """
  The reason an asset's relation cannot resolve, or `nil`.

  A catalogue-qualified relation with no schema is the one combination that
  cannot work: `Favn.SQLAsset`'s renderer rejects a `catalog.name` address, so
  the asset is misconfigured rather than merely sparse. Only declared levels are
  judged — a module path addresses nothing, so it cannot be wrong this way.

  ## Examples

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_defect(
      ...>   %{connection: "warehouse", catalogue: "raw", schema: nil}
      ...> )
      "A catalogue-qualified relation needs a schema; SQL rendering rejects this address."

      iex> alias FavnView.Components.AssetCataloguePage
      iex> AssetCataloguePage.namespace_defect(
      ...>   %{connection: "warehouse", catalogue: "raw", schema: "sales"}
      ...> )
      nil
  """
  @spec namespace_defect(map()) :: String.t() | nil
  def namespace_defect(asset) do
    if Map.get(asset, :catalogue) not in [nil, ""] and Map.get(asset, :schema) in [nil, ""] do
      "A catalogue-qualified relation needs a schema; SQL rendering rejects this address."
    end
  end

  defp relation_levels(asset) do
    [:connection, :catalogue, :schema]
    |> Enum.map(&%{level: &1, value: Map.get(asset, &1)})
    |> Enum.reject(&(&1.value in [nil, ""]))
  end

  defp level_class(:connection), do: "text-secondary"
  defp level_class(:catalogue), do: "text-accent"
  defp level_class(_level), do: "favn-text-muted"

  defp namespace_title(levels, true),
    do:
      "Derived from the module path; this asset declares no relation levels. " <>
        Enum.map_join(levels, ".", & &1.value)

  defp namespace_title(levels, false), do: Enum.map_join(levels, ".", & &1.value)

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

  defp asset_type_icon("metric"), do: "hero-chart-bar"
  defp asset_type_icon("file"), do: "hero-document"
  defp asset_type_icon(_type), do: "hero-table-cells"
end
