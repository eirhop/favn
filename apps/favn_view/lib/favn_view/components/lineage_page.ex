defmodule FavnView.Components.LineagePage do
  @moduledoc """
  Componentized operator asset lineage DAG page.
  """

  use FavnView, :html

  alias FavnOrchestrator.Operator.Lineage.AssetInspector
  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.EdgeInspector
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.GroupInspector
  alias FavnOrchestrator.Operator.Lineage.GroupNode
  alias FavnOrchestrator.Operator.Lineage.Limits
  alias FavnOrchestrator.Operator.Lineage.Summary
  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel

  @canvas_width 1_560
  @canvas_height 820
  @layers [:raw, :staging, :core, :marts, :dashboards]

  attr :graph, :any, default: nil
  attr :inspector, :any, default: nil
  attr :view_mode, :atom, default: :all
  attr :search, :string, default: ""
  attr :loading, :boolean, default: false
  attr :error, :any, default: nil
  attr :nav_items, :list, default: []
  attr :zoom, :integer, default: 62
  attr :canvas_hook?, :boolean, default: true
  attr :inspector_open?, :boolean, default: true

  def lineage_page(assigns) do
    assigns = assign_new(assigns, :nav_items, fn -> AssetCataloguePage.nav_items(:assets) end)

    ~H"""
    <AppShell.app_shell
      title="Asset lineage"
      subtitle={lineage_subtitle(@graph)}
      status="Live"
      status_tone={:success}
      nav_items={@nav_items}
      back_href={~p"/assets"}
      back_label="Back to assets"
      content_scroll?={false}
    >
      <.lineage_explorer
        graph={@graph}
        inspector={@inspector}
        view_mode={@view_mode}
        search={@search}
        loading={@loading}
        error={@error}
        zoom={@zoom}
        canvas_hook?={@canvas_hook?}
        inspector_open?={@inspector_open?}
      />
    </AppShell.app_shell>
    """
  end

  attr :graph, :any, default: nil
  attr :inspector, :any, default: nil
  attr :view_mode, :atom, default: :all
  attr :search, :string, default: ""
  attr :loading, :boolean, default: false
  attr :error, :any, default: nil
  attr :zoom, :integer, default: 62
  attr :canvas_hook?, :boolean, default: true
  attr :inspector_open?, :boolean, default: true

  def lineage_explorer(assigns) do
    ~H"""
    <div class="flex min-h-0 flex-1 flex-col gap-3" data-testid="lineage-page">
      <.loading_state :if={@loading} />
      <.error_state :if={!@loading && @error} error={@error} />
      <.empty_state :if={!@loading && !@error && is_nil(@graph)} />

      <div :if={!@loading && !@error && @graph} class="flex min-h-0 flex-1 flex-col gap-3">
        <.lineage_toolbar view_mode={@view_mode} search={@search} zoom={@zoom} />

        <div class={[
          "grid min-h-0 flex-1 gap-3",
          @inspector_open? && "xl:grid-cols-[minmax(0,1fr)_22rem] 2xl:grid-cols-[minmax(0,1fr)_24rem]"
        ]}>
          <.lineage_canvas graph={@graph} zoom={@zoom} canvas_hook?={@canvas_hook?} />
          <.lineage_inspector :if={@inspector_open?} inspector={@inspector} graph={@graph} />
        </div>
      </div>
    </div>
    """
  end

  attr :view_mode, :atom, required: true
  attr :search, :string, required: true
  attr :zoom, :integer, required: true

  def lineage_toolbar(assigns) do
    ~H"""
    <div
      class="flex shrink-0 flex-col gap-2 lg:flex-row lg:items-center lg:justify-between"
      data-testid="lineage-toolbar"
    >
      <div class="flex min-w-0 flex-1 gap-2">
        <label class="input input-sm favn-surface-control min-w-0 flex-1 gap-3 px-4 focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary">
          <.icon name="hero-magnifying-glass" class="size-5 shrink-0 text-base-content/60" />
          <span class="sr-only">Search lineage</span>
          <input
            type="search"
            name="search"
            value={@search}
            placeholder="Search lineage coming soon"
            autocomplete="off"
            disabled
          />
          <kbd class="kbd kbd-xs border-base-content/10 bg-base-100/20 text-base-content/50">K</kbd>
        </label>
      </div>

      <div class="flex items-center gap-2 overflow-x-auto pb-1 lg:pb-0">
        <div
          class="join favn-surface-control min-h-0 rounded-box p-1"
          role="tablist"
          aria-label="Lineage view modes"
        >
          <button
            :for={mode <- view_modes()}
            type="button"
            class={[
              "join-item btn btn-ghost btn-xs h-8 rounded-field px-3 text-xs font-normal",
              @view_mode == mode.id && "bg-primary/15 text-primary",
              mode[:disabled] && "opacity-45"
            ]}
            phx-click={if(mode[:disabled], do: false, else: "set_mode")}
            phx-value-mode={mode.id}
            disabled={mode[:disabled] || false}
            aria-pressed={@view_mode == mode.id}
          >
            {mode.label}
          </button>
        </div>

        <button
          type="button"
          class="btn btn-sm favn-surface-control gap-2 px-3"
          phx-click="fit_graph"
          data-lineage-command="fit"
          aria-label="Fit graph"
        >
          <.icon name="hero-arrows-pointing-in" class="size-4" />
          <span class="hidden xl:inline">Fit graph</span>
        </button>

        <div class="join favn-surface-control min-h-0 rounded-box p-1">
          <button
            type="button"
            class="join-item btn btn-ghost btn-xs h-8 px-3"
            phx-click="zoom_out"
            data-lineage-command="zoom-out"
            aria-label="Zoom out"
          >
            <.icon name="hero-minus" class="size-4" />
          </button>
          <span class="join-item flex h-8 min-w-14 items-center justify-center border-x border-base-content/10 px-3 text-xs text-base-content/70">
            {@zoom}%
          </span>
          <button
            type="button"
            class="join-item btn btn-ghost btn-xs h-8 px-3"
            phx-click="zoom_in"
            data-lineage-command="zoom-in"
            aria-label="Zoom in"
          >
            <.icon name="hero-plus" class="size-4" />
          </button>
        </div>
      </div>
    </div>
    """
  end

  attr :graph, :any, required: true
  attr :zoom, :integer, required: true
  attr :canvas_hook?, :boolean, default: true

  def lineage_canvas(assigns) do
    positions = layout_positions(assigns.graph.nodes)

    assigns =
      assigns
      |> assign(:positions, positions)
      |> assign(:canvas_width, @canvas_width)
      |> assign(:canvas_height, @canvas_height)

    ~H"""
    <section
      class="favn-surface-panel relative min-h-[34rem] overflow-hidden rounded-box border border-primary/20"
      data-testid="lineage-canvas-shell"
    >
      <div class="absolute inset-0 bg-base-100/20"></div>
      <div
        class="absolute inset-0 opacity-80"
        style="background-image: radial-gradient(color-mix(in oklab, var(--color-primary) 24%, transparent) 1px, transparent 1px); background-size: 18px 18px; mask-image: radial-gradient(circle at 46% 46%, black, transparent 78%);"
      >
      </div>

      <div
        class="relative h-full min-h-[34rem] overflow-hidden"
        id="lineage-canvas"
        phx-hook={if(@canvas_hook?, do: "LineageCanvas")}
        data-testid="lineage-canvas"
        data-zoom={@zoom}
      >
        <div
          class="lineage-canvas-content absolute left-0 top-0 origin-top-left transition-transform duration-150"
          style={"width: #{@canvas_width}px; height: #{@canvas_height}px; transform: scale(#{@zoom / 100});"}
          data-testid="lineage-canvas-content"
        >
          <.layer_headers graph={@graph} />

          <svg
            class="pointer-events-none absolute inset-0 size-full overflow-visible"
            viewBox={"0 0 #{@canvas_width} #{@canvas_height}"}
            aria-hidden="true"
          >
            <defs>
              <marker
                id="lineage-arrow"
                markerWidth="8"
                markerHeight="8"
                refX="6"
                refY="4"
                orient="auto"
                markerUnits="strokeWidth"
              >
                <path d="M 0 0 L 8 4 L 0 8 z" class="fill-primary/80" />
              </marker>
            </defs>
            <path
              :for={edge <- @graph.edges}
              d={edge_path(edge, @positions)}
              class={[
                "fill-none stroke-2",
                edge.selected? &&
                  "stroke-primary drop-shadow-[0_0_10px_color-mix(in_oklab,var(--color-primary)_80%,transparent)]",
                !edge.selected? && edge.status == :warning && "stroke-warning/70",
                !edge.selected? && edge.status != :warning && "stroke-success/60"
              ]}
              marker-end="url(#lineage-arrow)"
            />
          </svg>

          <button
            :for={edge <- @graph.edges}
            type="button"
            class="absolute z-20 rounded-field border border-primary/25 bg-base-100/80 px-2 py-1 text-[0.65rem] text-base-content/80 shadow-lg backdrop-blur transition hover:border-primary hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
            style={edge_label_style(edge, @positions)}
            phx-click="select_edge"
            phx-value-id={edge.id}
            data-testid="lineage-edge-label"
            aria-label={"Inspect #{edge.dependency_count} dependencies"}
          >
            {edge.dependency_count} deps
          </button>

          <.lineage_node
            :for={node <- @graph.nodes}
            node={node}
            position={Map.fetch!(@positions, node.id)}
          />
        </div>
      </div>

      <.lineage_hint_strip />
      <.lineage_minimap graph={@graph} positions={@positions} />
    </section>
    """
  end

  attr :graph, :any, required: true

  defp layer_headers(assigns) do
    assigns = assign(assigns, :layers, @layers)

    ~H"""
    <div
      :for={layer <- @layers}
      class="absolute top-8 z-10 text-xs text-base-content/75"
      style={"left: #{layer_x(layer)}px; width: 220px;"}
    >
      <div class="flex items-center gap-2">
        <.icon name={layer_icon(layer)} class="size-4 text-primary/80" />
        <span class="font-medium">{layer_label(layer)}</span>
      </div>
      <p class="mt-1 text-[0.65rem] text-base-content/45">{layer_caption(@graph, layer)}</p>
    </div>
    """
  end

  attr :node, :any, required: true
  attr :position, :map, required: true

  def lineage_node(%{node: %GroupNode{}} = assigns) do
    ~H"""
    <button
      type="button"
      class={[
        "absolute z-30 block rounded-box border p-4 text-left shadow-2xl backdrop-blur transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary",
        "border-primary/25 bg-base-200/65 hover:border-primary/60 hover:bg-base-200/80",
        @node.selected? &&
          "border-primary bg-primary/10 shadow-[0_0_0_1px_color-mix(in_oklab,var(--color-primary)_60%,transparent),0_0_32px_color-mix(in_oklab,var(--color-primary)_44%,transparent)]"
      ]}
      style={node_style(@position)}
      phx-click="select_node"
      phx-value-id={@node.id}
      phx-value-kind="group"
      data-testid="lineage-group-node"
      aria-label={"Inspect #{@node.label} lineage group"}
    >
      <div class="flex items-start justify-between gap-3">
        <div class="flex min-w-0 items-start gap-3">
          <span class="mt-0.5 flex size-8 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
            <.icon name={node_icon(@node)} class="size-5" />
          </span>
          <div class="min-w-0">
            <h2 class="truncate text-sm font-semibold text-base-content">{@node.label}</h2>
            <p class="mt-0.5 truncate text-[0.68rem] text-base-content/55">
              {asset_count_label(@node)}
            </p>
          </div>
        </div>
        <span class="rounded-field border border-base-content/10 p-1 text-base-content/55">
          <.icon name="hero-chevron-down" class="size-4" />
        </span>
      </div>

      <div class="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-[0.65rem]">
        <.status_count status={:fresh} count={@node.status_counts.fresh} />
        <.status_count status={:stale} count={@node.status_counts.stale} />
        <.status_count status={:failed} count={@node.status_counts.failed} />
        <.status_count status={:running} count={@node.status_counts.running} />
      </div>

      <div :if={@node.state != :collapsed} class="mt-3 space-y-1.5">
        <div
          :for={asset <- @node.preview_assets}
          class="flex items-center gap-2 rounded-field border border-base-content/10 bg-base-100/30 px-2.5 py-1.5 text-xs text-base-content/80"
        >
          <.icon name="hero-circle-stack" class="size-4 shrink-0 text-base-content/60" />
          <span class="min-w-0 flex-1 truncate">{asset.label}</span>
          <span class={status_dot_class(asset.freshness_status)}></span>
        </div>
        <div
          :if={@node.hidden_asset_count > 0}
          class="rounded-field border border-base-content/10 bg-base-100/20 px-2.5 py-1.5 text-xs text-base-content/55"
        >
          +{@node.hidden_asset_count} more
        </div>
      </div>
    </button>
    """
  end

  def lineage_node(%{node: %AssetNode{}} = assigns) do
    ~H"""
    <button
      type="button"
      class={[
        "absolute z-30 flex items-center gap-3 rounded-box border p-3 text-left shadow-xl backdrop-blur transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary",
        "border-primary/20 bg-base-200/70 hover:border-primary/60",
        @node.selected? &&
          "border-primary bg-primary/10 shadow-[0_0_28px_color-mix(in_oklab,var(--color-primary)_42%,transparent)]"
      ]}
      style={node_style(@position)}
      phx-click="select_node"
      phx-value-id={@node.id}
      phx-value-kind="asset"
      data-testid="lineage-asset-node"
      aria-label={"Inspect #{@node.label} lineage asset"}
    >
      <span class="flex size-8 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
        <.icon name="hero-cube-transparent" class="size-5" />
      </span>
      <div class="min-w-0 flex-1">
        <h2 class="truncate text-sm font-semibold text-base-content">{@node.label}</h2>
        <p class="truncate text-[0.68rem] text-base-content/50">
          {@node.schema || @node.layer} · {@node.kind}
        </p>
      </div>
      <span class={status_dot_class(@node.freshness_status)}></span>
    </button>
    """
  end

  attr :status, :atom, required: true
  attr :count, :integer, required: true

  def status_count(assigns) do
    ~H"""
    <span class={status_text_class(@status)}>
      <span class={status_dot_class(@status)}></span>
      {@count} {status_label(@status)}
    </span>
    """
  end

  attr :inspector, :any, default: nil
  attr :graph, Graph, required: true

  def lineage_inspector(assigns) do
    ~H"""
    <aside
      class="favn-surface-panel min-h-0 overflow-hidden rounded-box border border-primary/20"
      data-testid="lineage-inspector"
    >
      <.group_inspector :if={match?(%GroupInspector{}, @inspector)} inspector={@inspector} />
      <.asset_inspector :if={match?(%AssetInspector{}, @inspector)} inspector={@inspector} />
      <.edge_inspector :if={match?(%EdgeInspector{}, @inspector)} inspector={@inspector} />
      <.empty_inspector :if={is_nil(@inspector)} graph={@graph} />
    </aside>
    """
  end

  attr :inspector, :any, required: true

  def group_inspector(assigns) do
    ~H"""
    <div class="flex h-full min-h-0 flex-col">
      <.inspector_header
        title={@inspector.title}
        subtitle={@inspector.group.schema || "group"}
        status={dominant_status(@inspector.group.status_counts)}
        icon="hero-circle-stack"
        close_event="close_inspector"
      />
      <div class="min-h-0 flex-1 space-y-5 overflow-y-auto p-4">
        <.inspector_section title="About this group">
          <.definition label="System" value={@inspector.about.system || "-"} />
          <.definition label="Schema" value={@inspector.about.schema || "-"} />
          <.definition
            label="Type"
            value={@inspector.about.type |> to_string() |> String.replace("_", " ")}
          />
          <.definition label="Assets" value={Integer.to_string(@inspector.about.asset_count)} />
        </.inspector_section>

        <.inspector_section title="Health summary">
          <.health_bar
            label="Fresh"
            status={:fresh}
            count={@inspector.health_summary.fresh}
            total={@inspector.group.asset_count}
          />
          <.health_bar
            label="Stale"
            status={:stale}
            count={@inspector.health_summary.stale}
            total={@inspector.group.asset_count}
          />
          <.health_bar
            label="Failed"
            status={:failed}
            count={@inspector.health_summary.failed}
            total={@inspector.group.asset_count}
          />
          <.health_bar
            label="Running"
            status={:running}
            count={@inspector.health_summary.running}
            total={@inspector.group.asset_count}
          />
        </.inspector_section>

        <.inspector_section title="Top issues">
          <div :if={@inspector.top_issues == []} class="text-sm text-base-content/55">
            No active issues.
          </div>
          <div
            :for={issue <- @inspector.top_issues}
            class="flex items-center justify-between rounded-field border border-base-content/10 bg-base-100/20 px-3 py-2 text-sm"
          >
            <span class="flex items-center gap-2">
              <.icon name="hero-exclamation-triangle" class="size-4 text-warning" /> {issue.label}
            </span>
            <span class="text-base-content/55">{issue.count}</span>
          </div>
        </.inspector_section>

        <.dependency_list title="Downstream" items={@inspector.downstream} />
        <.dependency_list title="Upstream" items={@inspector.upstream} />
      </div>
      <.inspector_actions actions={@inspector.actions} />
    </div>
    """
  end

  attr :inspector, :any, required: true

  def asset_inspector(assigns) do
    ~H"""
    <div class="flex h-full min-h-0 flex-col">
      <.inspector_header
        title={@inspector.title}
        subtitle={@inspector.asset.asset_ref_text}
        status={@inspector.asset.freshness_status}
        icon="hero-cube-transparent"
        close_event="close_inspector"
      />
      <div class="min-h-0 flex-1 space-y-5 overflow-y-auto p-4">
        <.inspector_section title="Asset detail">
          <.definition label="Schema" value={@inspector.asset.schema || "-"} />
          <.definition label="Layer" value={to_string(@inspector.asset.layer)} />
          <.definition label="Kind" value={to_string(@inspector.asset.kind)} />
          <.definition label="Freshness" value={status_label(@inspector.asset.freshness_status)} />
        </.inspector_section>
        <.inspector_section title="Latest run">
          <div :if={is_nil(@inspector.latest_run)} class="text-sm text-base-content/55">
            No run evidence yet.
          </div>
          <.definition :if={@inspector.latest_run} label="Run" value={@inspector.latest_run.id} />
          <.definition
            :if={@inspector.latest_run}
            label="Status"
            value={to_string(@inspector.latest_run.status)}
          />
        </.inspector_section>
        <.dependency_list title="Downstream" items={@inspector.downstream} />
        <.dependency_list title="Upstream" items={@inspector.upstream} />
      </div>
      <.inspector_actions actions={@inspector.actions} />
    </div>
    """
  end

  attr :inspector, :any, required: true

  def edge_inspector(assigns) do
    ~H"""
    <div class="flex h-full min-h-0 flex-col">
      <.inspector_header
        title={@inspector.title}
        subtitle="dependency edge"
        status={@inspector.edge.status}
        icon="hero-share"
        close_event="close_inspector"
      />
      <div class="min-h-0 flex-1 space-y-5 overflow-y-auto p-4">
        <.inspector_section title="Dependency path">
          <.definition label="From" value={@inspector.upstream_label || @inspector.edge.from} />
          <.definition label="To" value={@inspector.downstream_label || @inspector.edge.to} />
          <.definition
            label="Dependencies"
            value={Integer.to_string(@inspector.edge.dependency_count)}
          />
        </.inspector_section>
        <.inspector_section title="Preview dependencies">
          <div
            :for={dependency <- @inspector.dependencies}
            class="rounded-field border border-base-content/10 bg-base-100/20 px-3 py-2 text-xs text-base-content/70"
          >
            {dependency.from} -> {dependency.to}
          </div>
        </.inspector_section>
      </div>
      <.inspector_actions actions={@inspector.actions} />
    </div>
    """
  end

  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :status, :atom, default: :unknown
  attr :icon, :string, required: true
  attr :close_event, :string, default: nil

  def inspector_header(assigns) do
    ~H"""
    <header class="border-b border-base-content/10 p-4">
      <div class="flex items-start justify-between gap-3">
        <div class="flex min-w-0 items-start gap-3">
          <span class="flex size-9 shrink-0 items-center justify-center rounded-field border border-primary/25 bg-primary/10 text-primary">
            <.icon name={@icon} class="size-5" />
          </span>
          <div class="min-w-0">
            <h2 class="truncate text-base font-semibold">{@title}</h2>
            <p :if={@subtitle} class="truncate text-xs text-base-content/55">{@subtitle}</p>
          </div>
        </div>
        <div class="flex items-center gap-2">
          <span class={status_pill_class(@status)}>
            <span class={status_dot_class(@status)}></span>
            {status_label(@status)}
          </span>
          <button
            :if={@close_event}
            type="button"
            class="btn btn-ghost btn-xs btn-square favn-icon-button"
            phx-click={@close_event}
            aria-label="Close lineage inspector"
            data-testid="lineage-inspector-close"
          >
            <.icon name="hero-x-mark" class="size-4" />
          </button>
        </div>
      </div>
    </header>
    """
  end

  attr :graph, :any, required: true

  def empty_inspector(assigns) do
    ~H"""
    <div class="relative flex h-full min-h-96 flex-col items-center justify-center p-8 text-center">
      <button
        type="button"
        class="btn btn-ghost btn-xs btn-square favn-icon-button absolute right-3 top-3"
        phx-click="close_inspector"
        aria-label="Close lineage inspector"
        data-testid="lineage-inspector-close"
      >
        <.icon name="hero-x-mark" class="size-4" />
      </button>
      <span class="flex size-12 items-center justify-center rounded-box border border-primary/25 bg-primary/10 text-primary">
        <.icon name="hero-share" class="size-6" />
      </span>
      <h2 class="mt-4 text-base font-semibold">Select lineage context</h2>
      <p class="mt-2 max-w-xs text-sm text-base-content/55">
        Select a group, asset, or dependency edge to inspect health, impact, and downstream relationships.
      </p>
      <p class="mt-4 text-xs text-base-content/45">
        {@graph.summary.visible_groups} groups · {@graph.summary.visible_edges} dependencies visible
      </p>
    </div>
    """
  end

  attr :title, :string, required: true
  slot :inner_block, required: true

  def inspector_section(assigns) do
    ~H"""
    <section class="space-y-2.5 border-b border-base-content/10 pb-5 last:border-b-0">
      <h3 class="text-sm font-medium text-base-content/90">{@title}</h3>
      {render_slot(@inner_block)}
    </section>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  def definition(assigns) do
    ~H"""
    <div class="grid grid-cols-[6rem_minmax(0,1fr)] gap-3 text-sm">
      <dt class="text-base-content/50">{@label}</dt>
      <dd class="truncate text-base-content/85" title={@value}>{@value}</dd>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :status, :atom, required: true
  attr :count, :integer, required: true
  attr :total, :integer, required: true

  def health_bar(assigns) do
    assigns = assign(assigns, :percent, percent(assigns.count, assigns.total))

    ~H"""
    <div class="space-y-1.5">
      <div class="flex items-center justify-between text-xs">
        <span class={status_text_class(@status)}>
          <span class={status_dot_class(@status)}></span> {@count} {@label}
        </span>
        <span class="text-base-content/45">{@percent}%</span>
      </div>
      <div class="h-1.5 overflow-hidden rounded-full bg-base-content/10">
        <div class={health_bar_class(@status)} style={"width: #{@percent}%;"}></div>
      </div>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :items, :list, required: true

  def dependency_list(assigns) do
    ~H"""
    <.inspector_section title={@title}>
      <div :if={@items == []} class="text-sm text-base-content/55">No visible dependencies.</div>
      <div
        :for={item <- @items}
        class="flex items-center justify-between rounded-field border border-base-content/10 bg-base-100/20 px-3 py-2 text-sm"
      >
        <span class="truncate">{item.label}</span>
        <span class="shrink-0 text-xs text-success">{item.dependency_count} deps</span>
      </div>
    </.inspector_section>
    """
  end

  attr :actions, :list, default: []

  def inspector_actions(assigns) do
    ~H"""
    <footer class="grid grid-cols-2 gap-2 border-t border-base-content/10 p-4">
      <button :for={action <- @actions} type="button" class="btn btn-sm btn-outline btn-primary">
        {action.label}
      </button>
    </footer>
    """
  end

  def lineage_hint_strip(assigns) do
    ~H"""
    <div
      class="absolute bottom-4 left-4 z-40 hidden rounded-box border border-base-content/10 bg-base-100/70 px-4 py-3 text-[0.68rem] text-base-content/65 shadow-xl backdrop-blur lg:flex lg:gap-5"
      data-testid="lineage-hint-strip"
    >
      <span class="flex items-center gap-2">
        <.icon name="hero-hand-raised" class="size-4" /> Pan
        <span class="text-base-content/40">Click and drag</span>
      </span>
      <span class="flex items-center gap-2">
        <.icon name="hero-magnifying-glass-plus" class="size-4" /> Zoom
        <span class="text-base-content/40">Scroll or pinch</span>
      </span>
      <span class="flex items-center gap-2">
        <.icon name="hero-cursor-arrow-rays" class="size-4" /> Select
        <span class="text-base-content/40">Click a node</span>
      </span>
      <span class="flex items-center gap-2">
        <.icon name="hero-plus-circle" class="size-4" /> Expand
        <span class="text-base-content/40">Click group chevron</span>
      </span>
    </div>
    """
  end

  attr :graph, :any, required: true
  attr :positions, :map, required: true

  def lineage_minimap(assigns) do
    ~H"""
    <div
      class="absolute bottom-4 right-4 z-40 hidden w-64 rounded-box border border-primary/25 bg-base-100/75 p-3 shadow-xl backdrop-blur xl:block"
      data-testid="lineage-minimap"
    >
      <div class="mb-2 flex justify-end gap-2 text-base-content/60">
        <.icon name="hero-magnifying-glass" class="size-4" />
        <.icon name="hero-arrows-pointing-in" class="size-4" />
        <.icon name="hero-square-3-stack-3d" class="size-4" />
      </div>
      <div class="relative h-24 overflow-hidden rounded-field border border-primary/35 bg-primary/5">
        <div
          :for={node <- @graph.nodes}
          class="absolute rounded-sm bg-primary/35"
          style={minimap_node_style(Map.fetch!(@positions, node.id))}
        >
        </div>
        <div
          class="absolute inset-3 border border-primary bg-primary/5 shadow-[0_0_18px_color-mix(in_oklab,var(--color-primary)_32%,transparent)]"
          data-testid="lineage-minimap-viewport"
        >
        </div>
      </div>
    </div>
    """
  end

  def loading_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="flex min-h-96 items-center justify-center p-10"
      data-testid="lineage-loading-state"
    >
      <div class="text-center">
        <span class="loading loading-ring loading-lg text-primary"></span>
        <p class="mt-4 text-base-content/60">Loading asset lineage</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :error, :any, required: true

  def error_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto flex min-h-96 max-w-2xl items-center justify-center p-10 text-center"
      data-testid="lineage-error-state"
    >
      <div>
        <.icon name="hero-exclamation-triangle" class="mx-auto size-10 text-warning" />
        <h2 class="mt-4 text-xl font-medium">Could not load lineage</h2>
        <p class="mt-2 text-base-content/60">{safe_error_message(@error)}</p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def empty_state(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto flex min-h-96 max-w-2xl items-center justify-center p-10 text-center"
      data-testid="lineage-empty-state"
    >
      <div>
        <.icon name="hero-share" class="mx-auto size-10 text-primary" />
        <h2 class="mt-4 text-xl font-medium">No lineage graph</h2>
        <p class="mt-2 text-base-content/60">
          Register and activate a manifest to inspect asset lineage.
        </p>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  def sample_graph do
    limits = %Limits{}

    github =
      group("group:raw:github", "GitHub raw", :raw, 0,
        asset_count: 42,
        system: "GitHub",
        schema: "raw",
        selected?: true,
        counts: %{fresh: 36, stale: 3, failed: 1, running: 2, unknown: 0},
        assets: ["raw_issues", "raw_pull_requests", "raw_commits", "raw_users"]
      )

    stripe =
      group("group:raw:stripe", "Stripe raw", :raw, 1,
        asset_count: 28,
        system: "Stripe",
        schema: "raw",
        counts: %{fresh: 24, stale: 2, failed: 0, running: 2, unknown: 0},
        assets: ["raw_payments", "raw_customers", "raw_refunds"]
      )

    stg_github =
      group("group:staging:github", "staging.github", :staging, 0,
        asset_count: 18,
        system: "github",
        schema: "staging",
        assets: ["stg_issues", "stg_prs", "stg_commits"]
      )

    stg_stripe =
      group("group:staging:stripe", "staging.stripe", :staging, 1,
        asset_count: 16,
        system: "stripe",
        schema: "staging",
        assets: ["stg_payments", "stg_customers", "stg_refunds"]
      )

    fct_orders = asset("asset:fct_orders", "fct_orders", :core, 0, :fresh, selected?: true)
    dim_customer = asset("asset:dim_customer", "dim_customer", :core, 1, :fresh)
    dim_product = asset("asset:dim_product", "dim_product", :core, 2, :fresh)
    dim_date = asset("asset:dim_date", "dim_date", :core, 3, :fresh)
    mart_sales = asset("asset:mart_sales", "mart_sales", :marts, 0, :fresh)
    mart_revenue = asset("asset:mart_revenue", "mart_revenue", :marts, 1, :stale)
    mart_customer = asset("asset:mart_customer_360", "mart_customer_360", :marts, 2, :fresh)

    sales =
      asset("asset:sales_overview", "Sales Overview", :dashboards, 0, :fresh, kind: :dashboard)

    revenue =
      asset("asset:revenue_analysis", "Revenue Analysis", :dashboards, 1, :stale,
        kind: :dashboard
      )

    customer =
      asset("asset:customer_360", "Customer 360", :dashboards, 2, :fresh, kind: :dashboard)

    executive =
      asset("asset:executive_kpi", "Executive KPI", :dashboards, 3, :fresh, kind: :dashboard)

    nodes = [
      github,
      stripe,
      stg_github,
      stg_stripe,
      dim_customer,
      fct_orders,
      dim_product,
      dim_date,
      mart_sales,
      mart_revenue,
      mart_customer,
      sales,
      revenue,
      customer,
      executive
    ]

    edges = [
      edge(github.id, stg_github.id, 18),
      edge(stripe.id, stg_stripe.id, 12),
      edge(stg_github.id, fct_orders.id, 18),
      edge(stg_stripe.id, fct_orders.id, 12),
      edge(dim_customer.id, fct_orders.id, 1),
      edge(dim_product.id, fct_orders.id, 1),
      edge(dim_date.id, fct_orders.id, 1),
      edge(fct_orders.id, mart_sales.id, 9),
      edge(fct_orders.id, mart_revenue.id, 5, :warning),
      edge(fct_orders.id, mart_customer.id, 4),
      edge(mart_sales.id, sales.id, 1),
      edge(mart_sales.id, revenue.id, 1, :warning),
      edge(mart_revenue.id, revenue.id, 1, :warning),
      edge(mart_customer.id, customer.id, 1),
      edge(mart_customer.id, executive.id, 1)
    ]

    %Graph{
      manifest_version_id: "storybook",
      scope: :global,
      selected_id: github.id,
      view_mode: :all,
      nodes: nodes,
      groups: Enum.filter(nodes, &match?(%GroupNode{}, &1)),
      edges: edges,
      summary: %Summary{
        total_assets: 1248,
        visible_assets: 74,
        total_groups: 96,
        visible_groups: 18,
        total_edges: 2930,
        visible_edges: 86,
        status_counts: %{fresh: 1090, stale: 82, failed: 9, running: 7, unknown: 60},
        truncated?: true
      },
      limits: limits,
      layout: %{direction: :left_to_right, layers: @layers},
      generated_at: ~U[2026-05-27 12:00:00Z]
    }
  end

  def sample_group_inspector do
    graph = sample_graph()
    group = hd(graph.groups)

    %GroupInspector{
      id: group.id,
      title: group.label,
      group: group,
      about: %{system: "GitHub", schema: "raw", type: :source_system, asset_count: 42},
      health_summary: group.status_counts,
      top_issues: [
        %{kind: :high_latency, label: "High latency", count: 2},
        %{kind: :schema_drift, label: "Schema drift", count: 1}
      ],
      downstream: [
        %{id: "group:staging:github", label: "staging.github", dependency_count: 18},
        %{id: "asset:fct_orders", label: "fct_orders", dependency_count: 9}
      ],
      upstream: [],
      actions: [
        %{label: "Drill into group", kind: :drill_in},
        %{label: "View all assets (42)", kind: :assets}
      ]
    }
  end

  defp group(id, label, layer, rank, opts) do
    counts =
      Keyword.get(opts, :counts, %{
        fresh: Keyword.get(opts, :asset_count, 1),
        stale: 0,
        failed: 0,
        running: 0,
        unknown: 0
      })

    asset_count = Keyword.get(opts, :asset_count, length(Keyword.get(opts, :assets, [])))
    preview_assets = Enum.map(Keyword.get(opts, :assets, []), &preview_asset(&1, id, layer))

    %GroupNode{
      id: id,
      label: label,
      system: Keyword.get(opts, :system),
      schema: Keyword.get(opts, :schema),
      layer: layer,
      type: if(layer == :raw, do: :source_system, else: :schema),
      state: :expanded_preview,
      asset_count: asset_count,
      preview_assets: preview_assets,
      preview_asset_ids: Enum.map(preview_assets, & &1.id),
      hidden_asset_count: max(asset_count - length(preview_assets), 0),
      status_counts: counts,
      top_issues: [],
      position_hint: %{layer: layer, rank: rank},
      selected?: Keyword.get(opts, :selected?, false)
    }
  end

  defp preview_asset(label, group_id, layer) do
    %AssetNode{
      id: "asset:#{label}",
      label: label,
      asset_ref: {__MODULE__, String.to_atom(label)},
      asset_ref_text: "#{__MODULE__}:#{label}",
      group_id: group_id,
      schema: to_string(layer),
      layer: layer,
      kind: :table,
      freshness_status: :fresh
    }
  end

  defp asset(id, label, layer, rank, status, opts \\ []) do
    %AssetNode{
      id: id,
      label: label,
      asset_ref: {__MODULE__, String.to_atom(id)},
      asset_ref_text: "#{__MODULE__}:#{label}",
      group_id: "group:#{layer}:#{id}",
      schema: to_string(layer),
      layer: layer,
      kind: Keyword.get(opts, :kind, :table),
      freshness_status: status,
      selected?: Keyword.get(opts, :selected?, false),
      position_hint: %{layer: layer, rank: rank}
    }
  end

  defp edge(from, to, count, status \\ :healthy) do
    %Edge{
      id: "edge:#{from}->#{to}",
      from: from,
      to: to,
      dependency_count: count,
      status: status,
      aggregated?: count > 1,
      preview_dependencies: [%{from: from, to: to}],
      hidden_dependency_count: max(count - 1, 0)
    }
  end

  defp lineage_subtitle(nil), do: "sales marts · Production"
  defp lineage_subtitle(%Graph{}), do: "sales marts · Production"

  defp view_modes do
    [
      %{id: :all, label: "All"},
      %{id: :upstream, label: "Upstream", disabled: true},
      %{id: :downstream, label: "Downstream", disabled: true},
      %{id: :impact, label: "Impact", disabled: true},
      %{id: :freshness, label: "Freshness", disabled: true}
    ]
  end

  defp layout_positions(nodes) do
    nodes
    |> Enum.with_index()
    |> Map.new(fn {node, fallback_rank} ->
      layer = node.position_hint[:layer] || node.layer
      rank = node.position_hint[:rank] || fallback_rank
      width = if match?(%GroupNode{}, node), do: 270, else: 180
      height = node_height(node)

      {node.id, %{x: layer_x(layer), y: node_y(layer, rank, node), width: width, height: height}}
    end)
  end

  defp node_height(%GroupNode{state: :collapsed}), do: 88
  defp node_height(%GroupNode{preview_assets: assets}), do: 130 + length(assets) * 34
  defp node_height(%AssetNode{}), do: 72

  defp layer_x(:raw), do: 30
  defp layer_x(:staging), do: 360
  defp layer_x(:core), do: 690
  defp layer_x(:marts), do: 980
  defp layer_x(:dashboards), do: 1270
  defp layer_x(_layer), do: 690

  defp node_y(:raw, rank, _node), do: 145 + rank * 330
  defp node_y(:staging, rank, _node), do: 200 + rank * 280
  defp node_y(:core, rank, _node), do: 210 + rank * 105
  defp node_y(:marts, rank, _node), do: 260 + rank * 125
  defp node_y(:dashboards, rank, _node), do: 220 + rank * 120
  defp node_y(_layer, rank, _node), do: 180 + rank * 120

  defp node_style(position) do
    "left: #{position.x}px; top: #{position.y}px; width: #{position.width}px; min-height: #{position.height}px;"
  end

  defp edge_path(edge, positions) do
    from = Map.fetch!(positions, edge.from)
    to = Map.fetch!(positions, edge.to)
    x1 = from.x + from.width
    y1 = from.y + from.height / 2
    x2 = to.x
    y2 = to.y + to.height / 2
    bend = max((x2 - x1) / 2, 70)

    "M #{x1} #{y1} C #{x1 + bend} #{y1}, #{x2 - bend} #{y2}, #{x2} #{y2}"
  end

  defp edge_label_style(edge, positions) do
    from = Map.fetch!(positions, edge.from)
    to = Map.fetch!(positions, edge.to)
    x = (from.x + from.width + to.x) / 2 - 22
    y = (from.y + from.height / 2 + to.y + to.height / 2) / 2 - 14
    "left: #{x}px; top: #{y}px;"
  end

  defp minimap_node_style(position) do
    "left: #{position.x / @canvas_width * 100}%; top: #{position.y / @canvas_height * 100}%; width: #{max(position.width / @canvas_width * 100, 3)}%; height: #{max(position.height / @canvas_height * 100, 4)}%;"
  end

  defp layer_label(:raw), do: "Raw systems"
  defp layer_label(:staging), do: "Staging"
  defp layer_label(:core), do: "Core"
  defp layer_label(:marts), do: "Marts"
  defp layer_label(:dashboards), do: "Dashboards"

  defp layer_caption(%Graph{} = graph, layer) do
    count = Enum.count(graph.nodes, &(&1.layer == layer))

    case layer do
      :raw -> pluralize(count, "system", "systems")
      :dashboards -> pluralize(count, "dashboard", "dashboards")
      _layer -> pluralize(count, "group", "groups")
    end
  end

  defp pluralize(1, singular, _plural), do: "1 #{singular}"
  defp pluralize(count, _singular, plural), do: "#{count} #{plural}"

  defp layer_icon(:raw), do: "hero-cpu-chip"
  defp layer_icon(:staging), do: "hero-circle-stack"
  defp layer_icon(:core), do: "hero-cube-transparent"
  defp layer_icon(:marts), do: "hero-squares-2x2"
  defp layer_icon(:dashboards), do: "hero-presentation-chart-line"

  defp node_icon(%GroupNode{layer: :raw}), do: "hero-circle-stack"
  defp node_icon(%GroupNode{layer: :staging}), do: "hero-server-stack"
  defp node_icon(%GroupNode{layer: :dashboards}), do: "hero-presentation-chart-line"
  defp node_icon(%GroupNode{}), do: "hero-cube-transparent"

  defp asset_count_label(%GroupNode{layer: :raw, asset_count: count}), do: "#{count} endpoints"
  defp asset_count_label(%GroupNode{asset_count: count}), do: "#{count} assets"

  defp percent(_count, 0), do: 0
  defp percent(count, total), do: round(count / total * 100)

  defp dominant_status(%{failed: failed}) when failed > 0, do: :failed
  defp dominant_status(%{stale: stale}) when stale > 0, do: :stale
  defp dominant_status(%{running: running}) when running > 0, do: :running
  defp dominant_status(%{unknown: unknown}) when unknown > 0, do: :unknown
  defp dominant_status(_counts), do: :fresh

  defp status_label(:healthy), do: "Fresh"
  defp status_label(:fresh), do: "Fresh"
  defp status_label(:stale), do: "Stale"
  defp status_label(:failed), do: "Failed"
  defp status_label(:running), do: "Running"
  defp status_label(:warning), do: "Stale"
  defp status_label(_status), do: "Unknown"

  defp status_dot_class(:fresh), do: "status status-xs status-success"
  defp status_dot_class(:healthy), do: "status status-xs status-success"
  defp status_dot_class(:stale), do: "status status-xs status-warning"
  defp status_dot_class(:warning), do: "status status-xs status-warning"
  defp status_dot_class(:failed), do: "status status-xs status-error"
  defp status_dot_class(:running), do: "status status-xs status-info"
  defp status_dot_class(_status), do: "status status-xs status-neutral"

  defp status_text_class(:fresh), do: "inline-flex items-center gap-1 text-success"
  defp status_text_class(:stale), do: "inline-flex items-center gap-1 text-warning"
  defp status_text_class(:failed), do: "inline-flex items-center gap-1 text-error"
  defp status_text_class(:running), do: "inline-flex items-center gap-1 text-info"
  defp status_text_class(_status), do: "inline-flex items-center gap-1 text-base-content/55"

  defp status_pill_class(status),
    do: ["badge badge-sm badge-soft gap-2", status_badge_class(status)]

  defp status_badge_class(:fresh), do: "badge-success"
  defp status_badge_class(:healthy), do: "badge-success"
  defp status_badge_class(:stale), do: "badge-warning"
  defp status_badge_class(:warning), do: "badge-warning"
  defp status_badge_class(:failed), do: "badge-error"
  defp status_badge_class(:running), do: "badge-info"
  defp status_badge_class(_status), do: "badge-neutral"

  defp health_bar_class(:fresh), do: "h-full rounded-full bg-success"
  defp health_bar_class(:stale), do: "h-full rounded-full bg-warning"
  defp health_bar_class(:failed), do: "h-full rounded-full bg-error"
  defp health_bar_class(:running), do: "h-full rounded-full bg-info"
  defp health_bar_class(_status), do: "h-full rounded-full bg-neutral"

  defp safe_error_message(%{message: message}) when is_binary(message), do: message
  defp safe_error_message(_error), do: "Backend unavailable. Try again shortly."
end
