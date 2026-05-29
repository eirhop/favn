# Scalable Asset Lineage DAG View Plan

## 1. Summary recommendation

Build the Asset Lineage DAG as an orchestrator-owned operator read model rendered by a thin `favn_view` LiveView. The default graph should be a bounded overview of collapsible lineage groups, not a full asset graph. `favn_core` should continue to own static manifest graph contracts and indexes. `favn_orchestrator` should own the public lineage facade, runtime overlays, grouped graph DTOs, search, inspector DTOs, pagination, limits, and storage access. `favn_view` should own only UI interaction state, rendering, Storybook stories, and browser hooks.

Recommended implementation shape:

- Add `FavnOrchestrator.Operator.Lineage` as the public same-BEAM facade for view use.
- Add explicit orchestrator DTO structs under `FavnOrchestrator.Operator.Lineage.*` for graph, nodes, edges, groups, summaries, limits, inspector payloads, search results, and errors.
- Add small static graph helpers in `favn_core` only if they generalize existing manifest graph/index behavior, such as manifest-version graph indexing by asset ref and deterministic static edge access.
- Use existing target-status and freshness read models for initial runtime overlays, then add a repairable lineage summary read model only when query cost or graph scale justifies persistence.
- Use a boring HTML/SVG absolute-positioned canvas first, with a focused JS hook for pan/zoom/minimap/fit. Do not add a graph library unless edge routing and interaction complexity prove the simple approach insufficient.

This should be delivered as a feature slice that first proves the grouped overview, inspector, and bounded graph contracts with deterministic mock Storybook data before wiring live data.

## 2. Product/UX behavior

The lineage page should prioritize overview, navigation, and operator diagnosis across hundreds or thousands of assets. The first paint must show the shape of the system, not every asset.

Default behavior:

- Show grouped containers by default.
- Use left-to-right flow across `Raw systems`, `Staging`, `Core`, `Marts`, and `Dashboards`.
- Default less relevant groups to `:collapsed`.
- Default selected, searched, failed, running, or high-impact groups to `:expanded_preview`.
- Use `:expanded_full` only after explicit drill-in or pagination.
- Cap the first response so the canvas remains fast and legible.

Required page layout:

- Slim header with `Back to assets`, `Asset lineage`, a `Live` badge, and context subtitle such as `sales marts · Production`.
- Compact toolbar with search, one `Filters` button, view tabs, fit graph, zoom controls, and optional fullscreen.
- Main DAG canvas filling the page with subtle grid, pan/zoom, grouped containers, and left-to-right layout.
- Right inspector panel that supports selected group, selected asset, selected edge, loading, empty, and error states.
- Minimap at the bottom-right of the canvas.
- Interaction hint strip at bottom-left for pan, zoom, select, and expand/collapse.

Supported view modes:

- `All`: bounded global grouped overview.
- `Upstream`: focus graph around selected asset/group upstream closure with limits.
- `Downstream`: focus graph around selected asset/group downstream closure with limits.
- `Impact`: downstream blast-radius view emphasizing failed/stale/running impact.
- `Freshness`: freshness-oriented styling and sorting, with stale/unknown upstream signals emphasized.

Group visual contract:

```text
GitHub raw
42 endpoints
36 fresh · 3 stale · 1 failed · 2 running

raw_issues
raw_pull_requests
raw_commits
raw_users
+38 more
```

Group states:

```elixir
:collapsed
:expanded_preview
:expanded_full
```

Node types:

```elixir
:group
:asset
:external_system
```

Edge types:

- Group-to-group dependencies.
- Group-to-asset dependencies.
- Asset-to-asset dependencies.
- Aggregated dependencies with counts such as `18 deps`.
- Clickable aggregated edge labels that load dependency details in the inspector first. Use a popover only for short previews after the inspector flow is stable.

Inspector behavior:

- Group inspector shows system, schema, type, asset count, health summary, top issues, downstream/upstream summaries, and actions.
- Asset inspector shows asset name, schema/layer, freshness, latest run, upstream, downstream, run link, catalog link, and explain lineage.
- Edge inspector shows dependency count, preview dependencies, upstream/downstream group names, affected statuses, and a paginated dependency list when expanded.
- Empty inspector shows concise guidance: select a group, asset, or edge.
- Loading inspector uses skeleton rows and preserves prior panel width to avoid layout shift.

The DAG must not reintroduce top KPI cards. Health counts belong inside group nodes, edge labels, toolbar filter state, or the inspector.

## 3. Data and contract design

The boundary between `favn_orchestrator` and `favn_view` should use explicit DTO structs. These are operator/UI read models, so they belong in `favn_orchestrator`, not `favn_core`, unless a field is a true static manifest/domain contract.

Recommended modules:

```text
FavnOrchestrator.Operator.Lineage
FavnOrchestrator.Operator.Lineage.Graph
FavnOrchestrator.Operator.Lineage.Group
FavnOrchestrator.Operator.Lineage.Node
FavnOrchestrator.Operator.Lineage.GroupNode
FavnOrchestrator.Operator.Lineage.AssetNode
FavnOrchestrator.Operator.Lineage.ExternalSystemNode
FavnOrchestrator.Operator.Lineage.Edge
FavnOrchestrator.Operator.Lineage.Summary
FavnOrchestrator.Operator.Lineage.Limits
FavnOrchestrator.Operator.Lineage.GroupInspector
FavnOrchestrator.Operator.Lineage.AssetInspector
FavnOrchestrator.Operator.Lineage.EdgeInspector
FavnOrchestrator.Operator.Lineage.SearchResult
FavnOrchestrator.Operator.Lineage.Page
```

Facade contract:

```elixir
@spec get_graph(keyword()) :: {:ok, Graph.t()} | {:error, error()}
@spec get_group(String.t(), keyword()) :: {:ok, GroupInspector.t()} | {:error, error()}
@spec get_asset(String.t(), keyword()) :: {:ok, AssetInspector.t()} | {:error, error()}
@spec get_edge(String.t(), keyword()) :: {:ok, EdgeInspector.t()} | {:error, error()}
@spec search(String.t(), keyword()) :: {:ok, Page.t(SearchResult.t())} | {:error, error()}
@spec list_group_assets(String.t(), keyword()) :: {:ok, Page.t(AssetNode.t())} | {:error, error()}
```

`get_graph/1` options:

```elixir
manifest_version_id: String.t() | :active
scope: :global | :asset | :group
selected_id: String.t() | nil
view_mode: :all | :upstream | :downstream | :impact | :freshness
filters: map()
expanded_group_ids: [String.t()]
limit: keyword()
timeout_ms: pos_integer()
```

Graph DTO:

```elixir
%LineageGraph{
  manifest_version_id: "...",
  scope: :global,
  selected_id: nil,
  nodes: [%LineageNode{}],
  edges: [%LineageEdge{}],
  groups: [%LineageGroup{}],
  summary: %LineageSummary{},
  limits: %LineageLimits{},
  layout: %{direction: :left_to_right, layers: [...]},
  generated_at: ~U[...]
}
```

Group node DTO:

```elixir
%LineageGroupNode{
  id: "group:raw:github",
  label: "GitHub raw",
  system: "GitHub",
  schema: "raw",
  layer: :raw,
  type: :source_system,
  state: :expanded_preview,
  asset_count: 42,
  preview_asset_ids: ["asset:raw_github_issues"],
  preview_assets: [%LineageAssetPreview{}],
  hidden_asset_count: 38,
  status_counts: %{fresh: 36, stale: 3, failed: 1, running: 2, unknown: 0},
  top_issues: [%{kind: :high_latency, label: "High latency", count: 2}],
  position_hint: %{layer: :raw, rank: 0}
}
```

Asset node DTO:

```elixir
%LineageAssetNode{
  id: "asset:Elixir.MyApp.RawGithub:issues",
  label: "raw_issues",
  asset_ref: {Elixir.MyApp.RawGithub, :issues},
  asset_ref_text: "Elixir.MyApp.RawGithub:issues",
  group_id: "group:raw:github",
  schema: "raw",
  layer: :raw,
  kind: :source,
  freshness_status: :fresh,
  run_status: :succeeded,
  latest_run_id: "run_...",
  selected?: false
}
```

Edge DTO:

```elixir
%LineageEdge{
  id: "edge:group:raw:github->group:staging:github",
  from: "group:raw:github",
  to: "group:staging:github",
  kind: :dependency,
  dependency_count: 18,
  status: :healthy,
  aggregated?: true,
  preview_dependencies: [%{from: "raw_issues", to: "stg_issues"}],
  hidden_dependency_count: 14
}
```

Summary DTO:

```elixir
%LineageSummary{
  total_assets: 1_248,
  visible_assets: 74,
  total_groups: 96,
  visible_groups: 18,
  total_edges: 2_930,
  visible_edges: 86,
  status_counts: %{fresh: 1_090, stale: 82, failed: 9, running: 7, unknown: 60},
  truncated?: true
}
```

Limits DTO:

```elixir
%LineageLimits{
  max_visible_groups: 60,
  max_preview_assets_per_group: 4,
  max_visible_asset_nodes: 200,
  max_visible_edges: 400,
  max_dependency_previews_per_edge: 5,
  group_asset_page_size: 50,
  search_page_size: 25,
  timeout_ms: 250
}
```

Ownership decision:

- `favn_core`: static manifest graph/index helpers only. Existing `Favn.Manifest.Graph`, `Favn.Manifest.Index`, and `Favn.Assets.GraphIndex` are reusable references. Add new static grouping metadata only if it is authoring/manifest-owned and useful outside the UI, such as explicit asset labels or relation/schema extraction.
- `favn_orchestrator`: all lineage DTO structs, group summaries, runtime overlays, inspector data, search result DTOs, payload limits, read budgets, error normalization, and public facade.
- `favn_view`: no DTO construction beyond view-specific CSS/layout normalization. The LiveView consumes orchestrator DTOs and stores browser/UI state.

Failure return shape:

```elixir
{:error, %FavnOrchestrator.Operator.Lineage.Error{
  code: :active_manifest_not_found | :manifest_not_found | :invalid_scope | :node_not_found |
        :query_timeout | :storage_unavailable | :lineage_projection_unavailable,
  message: String.t(),
  retryable?: boolean(),
  details: map()
}}
```

## 4. App ownership and boundaries

`favn_core` ownership:

- Static manifest assets, refs, relation metadata, dependency edges, graph topology, and deterministic static graph indexes.
- Potential additions: manifest graph adjacency helper by `Favn.Ref.t()`, relation/schema/layer extraction helper if it is manifest-domain behavior, and static grouping hints only when manifest-authored.
- No runtime status, persisted operator state, selected node, view mode, search cursor, inspector DTOs, or UI layout state.

`favn_orchestrator` ownership:

- `FavnOrchestrator.Operator.Lineage` public facade.
- Runtime-enriched graph read model.
- Grouping strategy and group ids.
- Status/freshness/run overlays.
- Bounded child previews and paginated expansion.
- Search across assets, groups, and schemas.
- Inspector DTOs.
- Query budgets, limits, pagination, storage access, batching, caching, telemetry, and error normalization.
- Optional repairable lineage summary/read-model projection.

`favn_view` ownership:

- `LineageLive` route and URL params.
- Visual expansion state, pan/zoom state, selected node/edge, active view mode, search input, open filter popover, and fullscreen state.
- Page/component rendering and Storybook stories.
- Browser hook for pan/zoom/minimap/fit/node selection.
- Calls only `FavnOrchestrator.Operator.Lineage` facade functions.

`favn_runner` ownership:

- No direct lineage ownership for this feature.
- Existing runner state may contribute only if already exposed through orchestrator-owned read models or future orchestrator projections.

Plugins/adapters ownership:

- No UI-specific leakage.
- Storage adapters implement orchestrator storage contracts only.
- SQL/DuckDB/plugin-specific source names may appear only through manifest metadata/relation DTOs or orchestrator-normalized labels.

Boundary rules:

- `favn_view` must not call `Favn.Manifest.Index`, `Favn.Assets.GraphIndex`, storage adapters, repos, runner modules, scheduler internals, compiler internals, or plugin internals.
- Storybook stories must use deterministic local sample data shaped like public orchestrator DTOs.
- LiveView tests should mock or seed only public orchestrator-facing behavior available to the view.

## 5. Query/read-model optimization plan

The initial graph load must return a bounded overview, never the full universe. Avoid broad scans followed by in-memory filtering in storage-backed paths. Static manifest traversal can operate in memory after loading a single manifest version because the manifest payload is already the pinned static contract, but runtime overlays must use keyed/batched reads.

Public facade budgets:

- `get_graph/1`: default `timeout_ms: 250`, max `1_000`.
- `get_group/2`: default `timeout_ms: 200`, max `750`.
- `get_asset/2`: default `timeout_ms: 200`, max `750`.
- `get_edge/2`: default `timeout_ms: 200`, max `750`.
- `search/2`: default `timeout_ms: 150`, max `500`.
- Return `{:error, %LineageError{code: :query_timeout, retryable?: true}}` when the budget is exceeded.

Initial limits:

- `max_visible_groups`: 40 default, 80 hard max.
- `max_preview_assets_per_group`: 4 default, 10 hard max.
- `max_visible_asset_nodes`: 160 default, 300 hard max.
- `max_visible_edges`: 300 default, 600 hard max.
- `max_dependency_previews_per_edge`: 5 default, 20 hard max.
- `group_asset_page_size`: 50 default, 100 hard max.
- `search_page_size`: 20 default, 50 hard max.
- Payload target: under 250 KB for initial overview; hard warning above 500 KB.

Access patterns:

- Get graph by manifest version: keyed `get_manifest(manifest_version_id)` or active manifest lookup, then build/read a manifest graph index from that single version. Do not list all manifests.
- Get selected asset lineage: locate selected asset by `target_id` or asset ref in the manifest index, traverse bounded upstream/downstream closure from static graph, group results, then batch runtime status by visible asset target ids.
- Get selected group lineage: resolve group id to a bounded group membership set using manifest-derived grouping index, include adjacent groups by static dependencies, then batch runtime status by visible/preview asset ids.
- Get child assets for group: use group id and cursor over the group membership index, sorted by status priority and stable label. Return `Page.t(AssetNode.t())` with cursor semantics for large groups.
- Get upstream/downstream for node: use static adjacency from manifest graph index; for group nodes, use aggregated group adjacency rather than expanding all assets unless the group is explicitly drilled into.
- Runtime status/freshness overlay for many asset ids: use `Storage.list_target_statuses(manifest_version_id, :asset, target_ids)` in one chunked batch. Existing SQLite/Postgres adapters chunk `IN` queries at 250 ids and return a map, which avoids N+1.
- Freshness detail for many assets: add or reuse a batched freshness query by concrete freshness keys. Existing `Storage.get_asset_freshness_states_by_keys/1` already chunks keys at 250. Prefer this over `list_asset_freshness(limit: Page.max_limit())` for lineage.
- Search assets/groups/schemas: add an orchestrator lineage search index built from the manifest and optionally cached per manifest version. For storage-backed persisted search later, add keyed indexed columns instead of scanning run history.
- Fetch inspector data: use keyed `get_group/2`, `get_asset/2`, or `get_edge/2`; each should batch only the related status and dependency ids needed for the inspector.

Required storage access patterns:

- `get_manifest_version(manifest_version_id)`.
- `get_active_manifest_version()`.
- `list_target_statuses(manifest_version_id, :asset, target_ids)` for overlay batches.
- `get_target_status(manifest_version_id, :asset, target_id)` for selected asset inspector fallback.
- `get_asset_freshness_states_by_keys(keys)` for batched freshness status where target status is insufficient.
- `list_target_runs(manifest_version_id, :asset, asset_ref, limit: n)` for selected asset inspector history only, not for graph-wide overlays.
- New: `lineage_group_summaries(manifest_version_id, group_ids)` only if group summaries become persisted.
- New: `lineage_search(manifest_version_id, query, filters, cursor)` only if manifest in-memory search becomes too costly.

Indexes already available:

- `favn_manifest_versions(manifest_version_id)` primary key and unique `content_hash`.
- `favn_target_statuses(manifest_version_id, target_kind, target_id)` unique index.
- `favn_target_statuses(manifest_version_id, target_kind)` index.
- `favn_target_statuses(manifest_version_id, target_kind, status, updated_at)` index.
- `favn_asset_freshness_states(asset_ref_module, asset_ref_name, freshness_key)` unique index.
- `favn_asset_freshness_states(manifest_version_id)` index.
- `favn_asset_freshness_states(status, updated_at)` index.
- Run target history is already served by `list_target_runs/4` and should remain inspector/detail-only.

Likely new indexes if lineage projections are persisted:

- `favn_lineage_groups(manifest_version_id, group_id)` unique.
- `favn_lineage_groups(manifest_version_id, layer, status, sort_key)` for overview loading.
- `favn_lineage_group_assets(manifest_version_id, group_id, sort_key, asset_target_id)` for group child pagination.
- `favn_lineage_group_edges(manifest_version_id, from_group_id, to_group_id)` unique.
- `favn_lineage_search(manifest_version_id, normalized_term, result_kind, result_id)` or backend-specific full-text index when needed.

Batching opportunities:

- Compute visible asset target ids once per graph request and call `list_target_statuses/3` once.
- Compute freshness keys once per visible/selected asset set and call `get_asset_freshness_states_by_keys/1` once.
- Build group status counts in one pass over the batched overlay map.
- For edge previews, collect visible dependency pairs from static adjacency and cap per edge before attaching labels.
- For inspector downstream/upstream summaries, batch statuses for all preview target ids together.

Cache/read-model opportunities:

- Cache static manifest lineage indexes in orchestrator memory keyed by `{manifest_version_id, content_hash}` with bounded ETS or persistent-term lifecycle. This avoids rebuilding grouping and adjacency on every LiveView event.
- Cache search token index per manifest version in the same static cache.
- Persist `favn_lineage_group_summaries` later if group summary aggregation over target status becomes costly or needs cross-node durability.
- Make any persisted lineage projection repairable from manifest versions, target statuses, freshness state, and run snapshots/events.

N+1 prevention:

- No graph path should call `get_target_status/3` per asset.
- No graph path should call `list_target_runs/4` per visible asset.
- No group summary path should call freshness reads per asset.
- Inspector paths may load one selected asset run history, but only after selection and with a limit.

Pagination/cursor behavior:

- Group child assets use cursor pagination with stable sort `{status_priority, label, asset_id}`.
- Edge dependency details use cursor pagination with stable sort `{from_label, to_label, dependency_id}`.
- Search uses cursor pagination with stable sort `{rank, label, id}`.
- `expanded_full` for a large group renders the first page plus `has_more?`; follow-up pages append or replace children based on UI choice.

## 6. Reuse and Refactor Opportunities

Reuse as-is:

- `FavnView.Components.AppShell` for the slim header, left navigation, Live badge, back link, and no-scroll content mode.
- `FavnView.Components.IconNav` for the left vertical app navigation.
- `FavnView.Components.GlassPanel` and CSS classes `favn-surface-panel`, `favn-surface-list`, `favn-surface-control`, `favn-surface-rail`, `favn-icon-button`, `favn-orbital-grid`, and `favn-status-glow` for Favn-native glass/HUD surfaces.
- `FavnView.CoreComponents.icon/1` and existing Heroicons setup.
- Existing Phoenix Storybook conventions under `apps/favn_view/storybook/components`.
- `Favn.Manifest.Index`, `Favn.Manifest.Graph`, and `Favn.Assets.GraphIndex` as references for deterministic static graph behavior.
- `FavnOrchestrator.TargetStatus` and `Storage.list_target_statuses/3` for batched current status overlays.
- `FavnOrchestrator.AssetFreshnessState` and `Storage.get_asset_freshness_states_by_keys/1` for batched freshness detail.
- `FavnOrchestrator.Page` / `CursorPage` patterns for bounded pagination semantics.

Extend existing component:

- Extend `AppShell.app_shell/1` with a `content_scroll?: false` usage for full-canvas pages rather than adding a second shell.
- Extend the nav items in `AssetCataloguePage.nav_items/1` so `Lineage` points to the new route and can be active.
- Extract status badge tone mapping from asset/schedule-specific components only if lineage needs the same mapping in multiple components.
- Extend existing surface CSS with a lineage-specific canvas grid class only if `favn-orbital-grid` is too page-global for the DAG canvas.

Extract reusable component:

- `FavnView.Components.OperatorStatusBadge` if asset catalogue, lineage nodes, inspectors, and future pages need one shared status contract.
- `FavnView.Components.HudToolbar` if lineage toolbar patterns repeat in schedules/assets after this feature.
- `FavnView.Components.InspectorPanel` if right-side inspectors become common across lineage, assets, runs, and future storage pages.

New component required:

- `FavnView.Components.LineagePage` top-level page component.
- `LineageToolbar`, `LineageCanvas`, `LineageGroupNode`, `LineageAssetNode`, `LineageEdge`, `LineageInspector`, `LineageMinimap`, and related lineage-specific components.
- `assets/js` hook for pan/zoom/minimap/fit and canvas event translation.

Defer:

- Generic graph layout engine abstraction.
- Persisted lineage projection tables before the in-memory manifest-index approach is measured.
- Popover-heavy edge dependency exploration before inspector selection is stable.
- External graph/canvas library until simple SVG/HTML cannot satisfy interaction/performance needs.
- Full light-theme polish for lineage until dark Favn-native visual acceptance is met, while keeping token usage compatible.

## 7. Component breakdown

LiveView structure:

```text
FavnView.LineageLive
  -> FavnView.Components.LineagePage.lineage_page/1
    -> LineageHeader through AppShell
    -> LineageToolbar
    -> LineageCanvas
      -> LineageLayerHeader
      -> LineageGroupNode
      -> LineageAssetNode
      -> LineageEdge
      -> LineageEdgeLabel
      -> LineageMinimap
      -> LineageControlsHint
    -> LineageInspector
      -> LineageGroupInspector
      -> LineageAssetInspector
      -> LineageEdgeInspector
      -> LineageEmptyInspector
      -> LineageLoadingInspector
      -> LineageErrorInspector
    -> LineageFilterPopover
    -> LineageSearchResults
```

`LineageLive` responsibilities:

- Parse URL params for selected id, view mode, filters, and search.
- Call `FavnOrchestrator.Operator.Lineage.get_graph/1` and selected inspector facades.
- Store UI-only state: selected id, selected type, expanded group ids, zoom level, pan position, active tab, search value, filter popover open state.
- Push canvas commands to the JS hook for fit/zoom when needed.
- Render exactly one top-level page component.

`LineagePage` responsibilities:

- Compose AppShell, toolbar, canvas, inspector, minimap, and states.
- Keep layout responsive.
- Avoid top cards.

`LineageToolbar` responsibilities:

- Search input with placeholder `Search assets, groups, or schemas...`.
- Keyboard shortcut hint.
- Single `Filters` button.
- View tabs: `All`, `Upstream`, `Downstream`, `Impact`, `Freshness`.
- Fit graph, zoom out, zoom percent, zoom in, and fullscreen button.

`LineageCanvas` responsibilities:

- Own visual board markup and data attributes used by the hook.
- Render SVG edges behind HTML nodes or all-SVG if measurement proves simpler.
- Render subtle grid and layer headers.
- Use stable ids and `data-testid` selectors for Playwright/LiveView tests.

Canvas implementation decision:

- Start with HTML absolute-positioned nodes and SVG edges in one overlay.
- Compute deterministic coarse positions server-side from layer/rank for Storybook and first implementation.
- Let the JS hook apply transform for pan/zoom to a single canvas content wrapper.
- Avoid a graph library for phase 1 because the required layout is grouped, layered, and bounded rather than arbitrary force-directed rendering.
- Revisit a library only if edge routing, collision avoidance, accessibility, and minimap maintenance become more expensive than integration risk.

JS hook responsibilities:

```text
LineageCanvasHook
  pan canvas by pointer drag
  wheel/pinch zoom with min/max bounds
  fit-to-graph from node bounds
  update minimap viewport
  dispatch node select events
  dispatch group expand/collapse events
  dispatch edge hover/click events
  preserve transform between LiveView patches
```

Accessibility requirements:

- Nodes are buttons or links with accessible labels.
- Edge labels are buttons when clickable.
- Keyboard selection works with tab order and Enter/Space.
- Toolbar controls have labels and `aria-pressed` for tabs.
- Inspector headings use semantic structure.

Responsive behavior:

- Desktop: left nav, full header, toolbar, canvas, right inspector.
- Tablet: inspector can collapse to a drawer.
- Mobile: canvas remains usable but defaults to fit graph with inspector as bottom sheet or route-level panel. Mobile can be a follow-up if desktop is the first acceptance target, but controls must remain reachable.

## 8. Storybook plan

Every important visual component should have Phoenix Storybook coverage with deterministic mock data matching the approved mockup direction.

Required stories:

- `lineage_page.story.exs`: full lineage page mock at desktop 16:9.
- `lineage_group_node.story.exs`: collapsed group node.
- `lineage_group_node.story.exs`: expanded preview group node.
- `lineage_group_node.story.exs`: selected group node with neon blue glow.
- `lineage_asset_node.story.exs`: fresh asset node.
- `lineage_asset_node.story.exs`: stale asset node.
- `lineage_asset_node.story.exs`: failed asset node.
- `lineage_asset_node.story.exs`: running asset node.
- `lineage_edge.story.exs`: aggregated edge with dependency count.
- `lineage_inspector.story.exs`: right inspector for group.
- `lineage_inspector.story.exs`: right inspector for asset.
- `lineage_toolbar.story.exs`: toolbar with search, filters button, tabs, fit, zoom, fullscreen.
- `lineage_minimap.story.exs`: minimap with viewport rectangle.
- `lineage_page.story.exs`: empty state.
- `lineage_page.story.exs`: loading state.
- `lineage_page.story.exs`: error state.
- `lineage_page.story.exs`: large graph overview state.

Mock data requirements:

- Include `GitHub raw` with `42 endpoints`, `36 fresh`, `3 stale`, `1 failed`, `2 running`, preview assets `raw_issues`, `raw_pull_requests`, `raw_commits`, `raw_users`, and `+38 more`.
- Include `Stripe raw`, `staging.github`, `staging.stripe`, `fct_orders`, `dim_customer`, `mart_sales`, `mart_revenue`, and dashboards.
- Include aggregated edges with `18 deps`, `12 deps`, `9 deps`, `5 deps`, and `4 deps`.
- Include group inspector matching the product example.
- Include asset inspector for a failed or stale asset.
- Include deterministic ids and `data-testid` values.

Storybook routes should be the visual contract for Playwright verification before live data is complete.

## 9. Playwright verification plan

Use Playwright against Storybook first, then the LiveView route once wired.

Desktop verification:

- Open `http://127.0.0.1:4173/storybook` and navigate to the full lineage page story.
- Set viewport to `1728x972` or `1440x900`.
- Verify the page has a left icon nav, slim header, compact toolbar, main canvas, right inspector, minimap, and bottom-left hint strip.
- Verify the DAG canvas uses most of the screen.
- Verify no top KPI cards exist.
- Verify filters are behind a single `Filters` button.
- Verify view tabs are `All`, `Upstream`, `Downstream`, `Impact`, `Freshness`.
- Verify `GitHub raw` group renders preview assets and `+38 more`.
- Verify selected group glow uses Favn neon blue treatment.
- Verify inspector supports selected group and displays health summary and actions.
- Verify aggregated edge labels render dependency counts.
- Click group expand/collapse and verify state changes.
- Click an aggregated edge and verify the inspector changes to dependency details.
- Click fit graph and zoom controls and verify transform/minimap updates.
- Take a screenshot.
- Compare against the approved mockup or project baseline where supported.

Responsive verification:

- Verify at `390x844` that navigation, toolbar, canvas controls, search, and inspector access remain reachable.
- Verify at a tablet width if inspector collapse behavior changes.

LiveView verification after wiring:

- Open `/lineage` or the final route.
- Verify active manifest fallback/loading/error states.
- Verify selecting a group calls only the orchestrator facade by checking LiveView behavior and logs when needed.
- Verify URL params can restore selected node and view mode.

Document intentional deviations:

- Any differences from the attached mockup must be listed in the PR or feature notes with product/design rationale.
- Pixel-perfect priority applies to layout, density, selected state, surface strength, typography, grid, and inspector proportions.

## 10. Focused test plan

Run focused tests only for affected apps/components.

`favn_core` static graph/index tests:

```bash
MIX_ENV=test mix do --app favn_core cmd mix test --no-compile test/manifest test/assets/graph_planner_parity_test.exs
```

`favn_orchestrator` facade and DTO tests:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile test/operator/lineage_test.exs
```

`favn_orchestrator` storage/read-model tests:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile test/storage/lineage_read_model_test.exs test/integration/storage_adapter_contract_test.exs
```

`favn_orchestrator` status/freshness aggregation tests:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile test/operator/lineage_status_aggregation_test.exs test/freshness/query_test.exs
```

`favn_orchestrator` search tests:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile test/operator/lineage_search_test.exs
```

`favn_view` component and LiveView tests:

```bash
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile test/favn_view/lineage_live_test.exs
```

Storybook/visual checks:

```bash
cd apps/favn_view && mix assets.build
```

Browser verification:

- Use Playwright MCP against `/storybook` and the final route.
- Save screenshots from the full page story and selected states.

General verification before finishing implementation:

```bash
mix format
mix compile --warnings-as-errors
```

Do not run umbrella-wide `mix test` unless explicitly requested later. Add adapter-specific SQLite/Postgres tests only when a persisted lineage projection or new storage callback is introduced.

## 11. Implementation phases

Phase 1: visual contract and mock data

- Add lineage DTO-shaped sample data in `favn_view` stories only.
- Build page and component Storybook stories against deterministic mock data.
- Implement `LineageCanvasHook` with static Storybook data for pan/zoom/minimap/fit.
- Verify with Playwright against Storybook and refine visual fidelity.

Phase 2: orchestrator DTO and static graph facade

- Add `FavnOrchestrator.Operator.Lineage` DTO structs and facade skeleton.
- Build static manifest grouping from active/passed manifest version.
- Reuse `Favn.Manifest.Index` and `Favn.Manifest.Graph` behavior rather than reconstructing dependency semantics in `favn_view`.
- Add unit tests for grouping, edge aggregation, limits, and error shapes.

Phase 3: runtime overlays and inspector facade

- Batch target status overlays with `Storage.list_target_statuses/3`.
- Batch freshness overlays with `Storage.get_asset_freshness_states_by_keys/1` where needed.
- Implement group, asset, and edge inspector DTOs.
- Add tests proving no N+1 status/freshness reads for visible asset sets.

Phase 4: LiveView route and interactions

- Add `LineageLive` and route.
- Wire toolbar, view modes, search, selected node/edge, expanded groups, and inspector loading.
- Ensure `favn_view` calls only `FavnOrchestrator.Operator.Lineage`.
- Add LiveView tests for rendering states, selection, expand/collapse, search, filters, and error state.

Phase 5: search, pagination, and large graph behavior

- Add lineage search over assets, groups, and schemas with bounded pages.
- Add group asset pagination and edge dependency pagination.
- Add large graph overview story and tests for truncation/limits.
- Measure payload size and latency with synthetic hundreds/thousands asset manifests.

Phase 6: optional persisted lineage read model

- Add storage callbacks, SQLite/Postgres migrations, adapters, and repair path only if in-memory per-manifest grouping is too slow.
- Keep projection repairable and scoped by manifest version.
- Add adapter parity tests and index coverage.

## 12. Risks / open questions

- Grouping semantics need product approval. The first heuristic can group by relation/catalog/schema/layer plus source-system metadata, but Favn may need explicit manifest grouping metadata later.
- Asset refs currently produce technical labels such as `Module:name`; the UI needs clean labels like `raw_issues`. Decide whether labels come from manifest metadata, relation table names, or normalized ref names.
- Freshness terminology differs between internal statuses (`:ok`, `:skipped_fresh`, `:blocked`) and product labels (`fresh`, `stale`, `failed`, `running`, `unknown`). The orchestrator DTO must own the mapping.
- `stale` may require freshness explanation, not just current target status. The facade must define when an asset is stale versus failed/unknown.
- Large graph layout quality may exceed simple deterministic positioning. Defer a graph library decision until the first visual prototype is measured.
- Live updates are not specified. The feature can start with manual refresh/LiveView events and later subscribe to orchestrator SSE/PubSub-derived updates if needed.
- Persisted lineage projection could become necessary for thousands of assets, but adding tables too early may create migration and repair burden.
- Mobile UX for a dense DAG is inherently constrained. The first acceptance target should be desktop 16:9, with mobile reachability rather than equal capability.
- Pixel comparison support may not exist in the project today. If no baseline tooling exists, use Playwright screenshots and documented visual review.

## 13. Explicit deferrals

- No implementation in this planning slice.
- No new graph/canvas dependency in the first implementation phase.
- No top KPI cards.
- No general SQL/data browser from lineage.
- No runner-owned lineage API.
- No storage/plugin-specific UI leakage.
- No persisted lineage tables until performance data proves in-memory manifest indexing is insufficient.
- No distributed live lineage update contract in the first slice.
- No full arbitrary graph auto-layout engine until the grouped, layered layout fails concrete cases.
- No broad umbrella tests for this feature unless explicitly requested later.
