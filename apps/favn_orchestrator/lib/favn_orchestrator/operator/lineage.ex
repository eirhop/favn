defmodule FavnOrchestrator.Operator.Lineage do
  @moduledoc """
  Public same-BEAM facade for bounded operator asset lineage views.

  The facade builds a grouped, runtime-enriched graph from one pinned manifest
  version and current target-status rows. It keeps graph payloads bounded and
  returns explicit DTO structs so browser-facing code does not stitch together
  storage, scheduler, runner, or manifest internals.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.Operator.Lineage.AssetInspector
  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.EdgeInspector
  alias FavnOrchestrator.Operator.Lineage.Error
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.GroupInspector
  alias FavnOrchestrator.Operator.Lineage.GroupNode
  alias FavnOrchestrator.Operator.Lineage.Limits
  alias FavnOrchestrator.Operator.Lineage.SearchResult
  alias FavnOrchestrator.Operator.Lineage.Summary
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TargetStatus

  @layers [:raw, :staging, :core, :marts, :dashboards]
  @scope_values [:global, :asset, :group]
  @view_modes [:all, :upstream, :downstream, :impact, :freshness]
  @status_keys [:fresh, :stale, :failed, :running, :unknown]
  @status_priority %{failed: 0, stale: 1, running: 2, unknown: 3, fresh: 4}

  @type error :: Error.t()

  @type graph_opts :: [
          manifest_version_id: String.t() | :active,
          scope: Graph.scope(),
          selected_id: String.t() | nil,
          view_mode: Graph.view_mode(),
          filters: map(),
          expanded_group_ids: [String.t()],
          limit: keyword(),
          timeout_ms: pos_integer()
        ]

  @doc """
  Returns a bounded grouped lineage graph for one manifest version.
  """
  @spec get_graph(graph_opts()) :: {:ok, Graph.t()} | {:error, error()}
  def get_graph(opts \\ []) when is_list(opts) do
    with {:ok, request} <- normalize_graph_opts(opts),
         {:ok, version} <- fetch_version(request.manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         assets <- List.wrap(version.manifest.assets),
         targets <- Enum.map(assets, &asset_target/1),
         {:ok, statuses} <- target_statuses(version.manifest_version_id, targets) do
      graph = build_graph(version, index, assets, targets, statuses, request)
      {:ok, graph}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Returns the inspector payload for one lineage group.
  """
  @spec get_group(String.t(), keyword()) :: {:ok, GroupInspector.t()} | {:error, error()}
  def get_group(group_id, opts \\ []) when is_binary(group_id) and is_list(opts) do
    with {:ok, graph} <- get_graph(Keyword.put(opts, :selected_id, group_id)),
         {:ok, group} <- fetch_group(graph, group_id) do
      {:ok, group_inspector(graph, group)}
    end
  end

  @doc """
  Returns the inspector payload for one lineage asset.
  """
  @spec get_asset(String.t(), keyword()) :: {:ok, AssetInspector.t()} | {:error, error()}
  def get_asset(asset_id, opts \\ []) when is_binary(asset_id) and is_list(opts) do
    with {:ok, graph} <- get_graph(Keyword.put(opts, :selected_id, asset_id)),
         {:ok, asset} <- fetch_asset_node(graph, asset_id) do
      {:ok, asset_inspector(graph, asset)}
    end
  end

  @doc """
  Returns the inspector payload for one lineage dependency edge.
  """
  @spec get_edge(String.t(), keyword()) :: {:ok, EdgeInspector.t()} | {:error, error()}
  def get_edge(edge_id, opts \\ []) when is_binary(edge_id) and is_list(opts) do
    with {:ok, graph} <- get_graph(Keyword.put(opts, :selected_id, edge_id)),
         {:ok, edge} <- fetch_edge(graph, edge_id) do
      {:ok, edge_inspector(graph, edge)}
    end
  end

  @doc """
  Searches visible lineage groups, schemas, and asset labels with bounded pages.
  """
  @spec search(String.t(), keyword()) :: {:ok, Page.t(SearchResult.t())} | {:error, error()}
  def search(query, opts \\ []) when is_binary(query) and is_list(opts) do
    with {:ok, graph} <- get_graph(opts),
         {:ok, page_opts} <- Page.normalize_opts(page_opts(opts, graph.limits.search_page_size)) do
      normalized = normalize_query(query)

      results =
        graph
        |> search_results()
        |> Enum.filter(&search_match?(&1, normalized))
        |> Enum.sort_by(&{result_rank(&1.kind), String.downcase(&1.label), &1.id})
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))

      {:ok, Page.from_fetched(results, page_opts)}
    else
      {:error, :invalid_pagination} -> {:error, normalize_error(:invalid_pagination)}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Lists assets belonging to a lineage group with bounded offset pagination.
  """
  @spec list_group_assets(String.t(), keyword()) ::
          {:ok, Page.t(AssetNode.t())} | {:error, error()}
  def list_group_assets(group_id, opts \\ []) when is_binary(group_id) and is_list(opts) do
    with {:ok, graph} <- get_graph(opts),
         {:ok, group} <- fetch_group(graph, group_id),
         {:ok, page_opts} <-
           Page.normalize_opts(page_opts(opts, graph.limits.group_asset_page_size)) do
      assets =
        group.preview_assets
        |> Enum.sort_by(
          &{Map.fetch!(@status_priority, &1.freshness_status), String.downcase(&1.label), &1.id}
        )
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))

      {:ok, Page.from_fetched(assets, page_opts)}
    else
      {:error, :invalid_pagination} -> {:error, normalize_error(:invalid_pagination)}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp normalize_graph_opts(opts) do
    with {:ok, scope} <-
           normalize_enum(Keyword.get(opts, :scope, :global), @scope_values, :invalid_scope),
         {:ok, view_mode} <-
           normalize_enum(Keyword.get(opts, :view_mode, :all), @view_modes, :invalid_view_mode) do
      limit_opts = Keyword.get(opts, :limit, [])
      graph_limit_opts = if Keyword.keyword?(limit_opts), do: limit_opts, else: []
      limits = normalize_limits(graph_limit_opts, Keyword.get(opts, :timeout_ms))

      {:ok,
       %{
         manifest_version_id: Keyword.get(opts, :manifest_version_id, :active),
         scope: scope,
         selected_id: Keyword.get(opts, :selected_id),
         view_mode: view_mode,
         filters: Keyword.get(opts, :filters, %{}),
         expanded_group_ids: MapSet.new(List.wrap(Keyword.get(opts, :expanded_group_ids, []))),
         limits: limits
       }}
    else
      {:error, :invalid_scope} ->
        {:error, %Error{code: :invalid_scope, message: "Invalid lineage scope."}}

      {:error, :invalid_view_mode} ->
        {:error, %Error{code: :invalid_scope, message: "Invalid lineage view mode."}}
    end
  end

  defp normalize_enum(value, allowed, error) when is_atom(value) do
    if value in allowed, do: {:ok, value}, else: {:error, error}
  end

  defp normalize_enum(value, allowed, error) when is_binary(value) do
    atom = String.to_existing_atom(value)
    normalize_enum(atom, allowed, error)
  rescue
    ArgumentError -> {:error, error}
  end

  defp normalize_enum(_value, _allowed, error), do: {:error, error}

  defp normalize_limits(limit_opts, timeout_ms) do
    base = %Limits{}

    base
    |> maybe_put_limit(:max_visible_groups, limit_opts, 40, 80)
    |> maybe_put_limit(:max_preview_assets_per_group, limit_opts, 4, 10)
    |> maybe_put_limit(:max_visible_asset_nodes, limit_opts, 160, 300)
    |> maybe_put_limit(:max_visible_edges, limit_opts, 300, 600)
    |> maybe_put_limit(:max_dependency_previews_per_edge, limit_opts, 5, 20)
    |> maybe_put_limit(:group_asset_page_size, limit_opts, 50, 100)
    |> maybe_put_limit(:search_page_size, limit_opts, 20, 50)
    |> Map.put(
      :timeout_ms,
      clamp_integer(timeout_ms || Keyword.get(limit_opts, :timeout_ms, 250), 1, 1_000, 250)
    )
  end

  defp maybe_put_limit(%Limits{} = limits, key, opts, default, max) do
    Map.put(limits, key, clamp_integer(Keyword.get(opts, key, default), 1, max, default))
  end

  defp clamp_integer(value, min, max, _default) when is_integer(value),
    do: value |> max(min) |> min(max)

  defp clamp_integer(_value, _min, _max, default), do: default

  defp fetch_version(:active) do
    with {:ok, manifest_version_id} <- FavnOrchestrator.active_manifest() do
      fetch_version(manifest_version_id)
    else
      {:error, _reason} ->
        {:error,
         %Error{
           code: :active_manifest_not_found,
           message: "No active manifest is available.",
           retryable?: true
         }}
    end
  end

  defp fetch_version(manifest_version_id) when is_binary(manifest_version_id) do
    case FavnOrchestrator.get_manifest(manifest_version_id) do
      {:ok, %Version{} = version} ->
        {:ok, version}

      {:error, :not_found} ->
        {:error, %Error{code: :manifest_not_found, message: "Manifest version was not found."}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_version(_value),
    do: {:error, %Error{code: :manifest_not_found, message: "Manifest version was not found."}}

  defp target_statuses(manifest_version_id, targets) do
    target_ids = Enum.map(targets, & &1.target_id)

    with {:ok, statuses} <- Storage.list_target_statuses(manifest_version_id, :asset, target_ids) do
      {:ok,
       Map.new(targets, fn target ->
         status =
           Map.get(statuses, target.target_id) ||
             TargetStatus.unknown(
               manifest_version_id,
               :asset,
               target.target_id,
               target.asset_ref_text
             )

         {target.target_id, status}
       end)}
    end
  end

  defp build_graph(%Version{} = version, %Index{} = index, assets, targets, statuses, request) do
    target_by_ref = Map.new(targets, &{&1.ref, &1})
    status_by_ref = Map.new(targets, &{&1.ref, Map.fetch!(statuses, &1.target_id)})
    group_contexts = build_group_contexts(assets, target_by_ref, status_by_ref, request)
    {groups, asset_nodes_by_id, group_by_ref} = build_group_nodes(group_contexts, request)
    edges = build_edges(version, index, group_by_ref, asset_nodes_by_id, request)
    visible_groups = Enum.take(groups, request.limits.max_visible_groups)
    visible_ids = MapSet.new(Enum.map(visible_groups, & &1.id))

    visible_edges =
      edges
      |> Enum.filter(&(&1.from in visible_ids and &1.to in visible_ids))
      |> Enum.take(request.limits.max_visible_edges)

    %Graph{
      manifest_version_id: version.manifest_version_id,
      scope: request.scope,
      selected_id: request.selected_id,
      view_mode: request.view_mode,
      nodes: visible_groups,
      groups: visible_groups,
      edges: visible_edges,
      summary:
        summary(length(assets), length(groups), length(edges), visible_groups, visible_edges),
      limits: request.limits,
      layout: %{direction: :left_to_right, layers: @layers},
      generated_at: DateTime.utc_now()
    }
  end

  defp build_group_contexts(assets, target_by_ref, status_by_ref, request) do
    assets
    |> Enum.group_by(&group_key/1)
    |> Enum.map(fn {key, grouped_assets} ->
      asset_nodes =
        Enum.map(
          grouped_assets,
          &asset_node(
            &1,
            Map.fetch!(target_by_ref, &1.ref),
            Map.fetch!(status_by_ref, &1.ref),
            key,
            request
          )
        )

      %{key: key, assets: grouped_assets, asset_nodes: asset_nodes}
    end)
    |> Enum.sort_by(fn %{key: key, asset_nodes: asset_nodes} ->
      {layer_rank(key.layer), String.downcase(key.label), length(asset_nodes)}
    end)
  end

  defp build_group_nodes(group_contexts, request) do
    ranked =
      group_contexts
      |> Enum.group_by(& &1.key.layer)
      |> Enum.flat_map(fn {_layer, contexts} ->
        contexts
        |> Enum.with_index()
        |> Enum.map(fn {context, rank} -> Map.put(context, :rank, rank) end)
      end)

    Enum.reduce(ranked, {[], %{}, %{}}, fn context, {groups, asset_nodes_by_id, group_by_ref} ->
      asset_nodes = sort_asset_nodes(context.asset_nodes)
      preview = Enum.take(asset_nodes, request.limits.max_preview_assets_per_group)
      state = group_state(context.key.id, request, context.key.layer)

      group = %GroupNode{
        id: context.key.id,
        label: context.key.label,
        system: context.key.system,
        schema: context.key.schema,
        layer: context.key.layer,
        type: context.key.type,
        state: state,
        asset_count: length(asset_nodes),
        preview_asset_ids: Enum.map(preview, & &1.id),
        preview_assets: preview,
        hidden_asset_count: max(length(asset_nodes) - length(preview), 0),
        status_counts: status_counts(asset_nodes),
        top_issues: top_issues(asset_nodes),
        position_hint: %{layer: context.key.layer, rank: context.rank},
        selected?: request.selected_id == context.key.id
      }

      next_asset_nodes = Map.merge(asset_nodes_by_id, Map.new(asset_nodes, &{&1.id, &1}))

      next_group_by_ref =
        Enum.reduce(context.assets, group_by_ref, &Map.put(&2, &1.ref, group.id))

      {[group | groups], next_asset_nodes, next_group_by_ref}
    end)
    |> then(fn {groups, asset_nodes_by_id, group_by_ref} ->
      {Enum.sort_by(
         groups,
         &{layer_rank(&1.layer), &1.position_hint.rank, String.downcase(&1.label)}
       ), asset_nodes_by_id, group_by_ref}
    end)
  end

  defp build_edges(
         %Version{} = version,
         %Index{} = index,
         group_by_ref,
         asset_nodes_by_id,
         request
       ) do
    raw_edges =
      case version.manifest.graph.edges do
        [] -> edges_from_assets(List.wrap(version.manifest.assets))
        edges -> edges
      end

    raw_edges
    |> Enum.reduce(%{}, fn %{from: from, to: to}, acc ->
      from_group = Map.get(group_by_ref, from)
      to_group = Map.get(group_by_ref, to)

      if is_nil(from_group) or is_nil(to_group) or from_group == to_group do
        acc
      else
        key = {from_group, to_group}
        from_asset = Map.get(asset_nodes_by_id, TargetStatus.target_id_for_asset(from))
        to_asset = Map.get(asset_nodes_by_id, TargetStatus.target_id_for_asset(to))
        dep = %{from: asset_label(index, from_asset, from), to: asset_label(index, to_asset, to)}

        Map.update(acc, key, [dep], &[dep | &1])
      end
    end)
    |> Enum.map(fn {{from, to}, deps} -> edge(from, to, deps, request) end)
    |> Enum.sort_by(&{&1.from, &1.to})
  end

  defp edges_from_assets(assets) do
    Enum.flat_map(assets, fn asset ->
      Enum.map(List.wrap(asset.depends_on), &%{from: &1, to: asset.ref})
    end)
  end

  defp edge(from, to, deps, request) do
    deps = Enum.sort_by(deps, &{&1.from, &1.to})
    preview = Enum.take(deps, request.limits.max_dependency_previews_per_edge)

    %Edge{
      id: "edge:#{from}->#{to}",
      from: from,
      to: to,
      dependency_count: length(deps),
      aggregated?: length(deps) > 1,
      preview_dependencies: preview,
      hidden_dependency_count: max(length(deps) - length(preview), 0),
      selected?: request.selected_id == "edge:#{from}->#{to}"
    }
  end

  defp asset_target(%Asset{} = asset) do
    %{
      ref: asset.ref,
      target_id: TargetStatus.target_id_for_asset(asset.ref),
      asset_ref_text: TargetStatus.ref_text(asset.ref)
    }
  end

  defp asset_node(%Asset{} = asset, target, status, group_key, request) do
    %AssetNode{
      id: target.target_id,
      label: asset_label(asset),
      asset_ref: asset.ref,
      asset_ref_text: target.asset_ref_text,
      group_id: group_key.id,
      schema: group_key.schema,
      layer: group_key.layer,
      kind: asset.type || :asset,
      freshness_status: product_status(status),
      run_status: status.latest_run_status,
      latest_run_id: status.latest_run_id,
      selected?: request.selected_id == target.target_id,
      position_hint: %{layer: group_key.layer}
    }
  end

  defp group_key(%Asset{} = asset) do
    relation = relation_ref(asset.relation)
    metadata = normalize_map(asset.metadata)
    layer = lineage_layer(asset, relation, metadata)

    system =
      first_text([
        metadata[:system],
        metadata["system"],
        relation && relation.connection,
        relation && relation.catalog
      ])

    schema =
      first_text([
        metadata[:schema],
        metadata["schema"],
        relation && relation.schema,
        layer_schema(layer)
      ])

    label = group_label(layer, system, schema, asset)
    id = "group:#{layer}:#{slug(system || schema || label)}"

    %{id: id, label: label, system: system, schema: schema, layer: layer, type: group_type(layer)}
  end

  defp group_state(group_id, request, layer) do
    cond do
      MapSet.member?(request.expanded_group_ids, group_id) -> :expanded_full
      request.selected_id == group_id -> :expanded_preview
      layer in [:raw, :staging] -> :expanded_preview
      true -> :collapsed
    end
  end

  defp relation_ref(nil), do: nil

  defp relation_ref(relation) do
    RelationRef.new!(relation)
  rescue
    ArgumentError -> nil
  end

  defp lineage_layer(%Asset{} = asset, relation, metadata) do
    explicit =
      first_text([
        metadata[:lineage_layer],
        metadata["lineage_layer"],
        metadata[:layer],
        metadata["layer"]
      ])

    text =
      Enum.join(
        [
          explicit,
          relation && relation.catalog,
          relation && relation.schema,
          relation && relation.name,
          Atom.to_string(asset.name || :asset)
        ],
        ":"
      )

    normalized = String.downcase(text)

    cond do
      String.contains?(normalized, "dashboard") or String.contains?(normalized, "report") ->
        :dashboards

      String.contains?(normalized, "mart") or String.contains?(normalized, "marts") ->
        :marts

      String.contains?(normalized, "staging") or String.contains?(normalized, "stg") ->
        :staging

      String.contains?(normalized, "raw") or asset.type == :source ->
        :raw

      true ->
        :core
    end
  end

  defp layer_schema(:raw), do: "raw"
  defp layer_schema(:staging), do: "staging"
  defp layer_schema(:marts), do: "marts"
  defp layer_schema(:dashboards), do: "dashboards"
  defp layer_schema(:core), do: "core"

  defp group_type(:raw), do: :source_system
  defp group_type(:dashboards), do: :dashboard
  defp group_type(:core), do: :domain
  defp group_type(_layer), do: :schema

  defp group_label(:raw, system, _schema, _asset) when is_binary(system),
    do: "#{titleize(system)} raw"

  defp group_label(:staging, system, _schema, _asset) when is_binary(system),
    do: "staging.#{slug(system)}"

  defp group_label(:marts, _system, schema, _asset) when is_binary(schema),
    do: "marts · #{schema}"

  defp group_label(:dashboards, _system, schema, _asset) when is_binary(schema),
    do: "#{titleize(schema)} dashboards"

  defp group_label(layer, _system, schema, _asset) when is_binary(schema),
    do: "#{layer} · #{schema}"

  defp group_label(layer, _system, _schema, asset), do: "#{layer} · #{asset_label(asset)}"

  defp asset_label(%Asset{relation: relation} = asset) do
    case relation_ref(relation) do
      %RelationRef{name: name} when is_binary(name) -> name
      _ -> asset.name |> Atom.to_string()
    end
  end

  defp asset_label(_index, %AssetNode{label: label}, _ref), do: label
  defp asset_label(_index, _node, {_module, name}), do: Atom.to_string(name)

  defp product_status(%TargetStatus{status: :running}), do: :running
  defp product_status(%TargetStatus{status: :failed}), do: :failed
  defp product_status(%TargetStatus{status: :unknown}), do: :unknown

  defp product_status(%TargetStatus{freshness_status: status})
       when status in [:stale, :expired, :blocked], do: :stale

  defp product_status(%TargetStatus{status: :healthy}), do: :fresh

  defp sort_asset_nodes(asset_nodes) do
    Enum.sort_by(
      asset_nodes,
      &{Map.fetch!(@status_priority, &1.freshness_status), String.downcase(&1.label), &1.id}
    )
  end

  defp status_counts(asset_nodes) do
    Enum.reduce(asset_nodes, empty_status_counts(), fn asset, acc ->
      Map.update!(acc, asset.freshness_status, &(&1 + 1))
    end)
  end

  defp empty_status_counts, do: Map.new(@status_keys, &{&1, 0})

  defp top_issues(asset_nodes) do
    counts = status_counts(asset_nodes)

    [
      issue(:failed, "Failed", counts.failed),
      issue(:stale, "Stale", counts.stale),
      issue(:running, "Running", counts.running)
    ]
    |> Enum.reject(&(&1.count == 0))
  end

  defp issue(kind, label, count), do: %{kind: kind, label: label, count: count}

  defp summary(total_assets, total_groups, total_edges, visible_groups, visible_edges) do
    visible_status_counts =
      Enum.reduce(
        visible_groups,
        empty_status_counts(),
        &merge_status_counts(&2, &1.status_counts)
      )

    visible_asset_count = Enum.reduce(visible_groups, 0, &(&1.asset_count + &2))

    %Summary{
      total_assets: total_assets,
      visible_assets: visible_asset_count,
      total_groups: total_groups,
      visible_groups: length(visible_groups),
      total_edges: total_edges,
      visible_edges: length(visible_edges),
      status_counts: visible_status_counts,
      truncated?: total_groups > length(visible_groups) or total_edges > length(visible_edges)
    }
  end

  defp merge_status_counts(left, right) do
    Map.new(@status_keys, &{&1, Map.get(left, &1, 0) + Map.get(right, &1, 0)})
  end

  defp fetch_group(%Graph{} = graph, group_id) do
    case Enum.find(graph.groups, &(&1.id == group_id)) do
      %GroupNode{} = group -> {:ok, group}
      nil -> {:error, %Error{code: :node_not_found, message: "Lineage group was not found."}}
    end
  end

  defp fetch_asset_node(%Graph{} = graph, asset_id) do
    graph.groups
    |> Enum.flat_map(& &1.preview_assets)
    |> Enum.find(&(&1.id == asset_id))
    |> case do
      %AssetNode{} = asset -> {:ok, asset}
      nil -> {:error, %Error{code: :node_not_found, message: "Lineage asset was not found."}}
    end
  end

  defp fetch_edge(%Graph{} = graph, edge_id) do
    case Enum.find(graph.edges, &(&1.id == edge_id)) do
      %Edge{} = edge -> {:ok, edge}
      nil -> {:error, %Error{code: :node_not_found, message: "Lineage edge was not found."}}
    end
  end

  defp group_inspector(%Graph{} = graph, %GroupNode{} = group) do
    %GroupInspector{
      id: group.id,
      title: group.label,
      group: group,
      about: %{
        system: group.system,
        schema: group.schema,
        type: group.type,
        asset_count: group.asset_count
      },
      health_summary: group.status_counts,
      top_issues: group.top_issues,
      upstream: adjacent_groups(graph, group.id, :upstream),
      downstream: adjacent_groups(graph, group.id, :downstream),
      actions: [
        %{label: "Drill into group", kind: :drill_in},
        %{label: "View all assets", kind: :assets}
      ]
    }
  end

  defp asset_inspector(%Graph{} = graph, %AssetNode{} = asset) do
    %AssetInspector{
      id: asset.id,
      title: asset.label,
      asset: asset,
      latest_run: latest_run(asset),
      upstream: adjacent_groups(graph, asset.group_id, :upstream),
      downstream: adjacent_groups(graph, asset.group_id, :downstream),
      actions: [%{label: "Open asset", kind: :asset}, %{label: "Explain lineage", kind: :explain}]
    }
  end

  defp edge_inspector(%Graph{} = graph, %Edge{} = edge) do
    %EdgeInspector{
      id: edge.id,
      title: "#{edge.dependency_count} dependencies",
      edge: edge,
      upstream_label: graph_label(graph, edge.from),
      downstream_label: graph_label(graph, edge.to),
      dependencies: edge.preview_dependencies,
      affected_statuses: %{},
      actions: [%{label: "View dependency list", kind: :dependencies}]
    }
  end

  defp latest_run(%AssetNode{latest_run_id: nil}), do: nil
  defp latest_run(%AssetNode{} = asset), do: %{id: asset.latest_run_id, status: asset.run_status}

  defp adjacent_groups(graph, group_id, direction) do
    graph.edges
    |> Enum.filter(fn edge ->
      case direction do
        :upstream -> edge.to == group_id
        :downstream -> edge.from == group_id
      end
    end)
    |> Enum.map(fn edge ->
      related_id = if direction == :upstream, do: edge.from, else: edge.to

      %{
        id: related_id,
        label: graph_label(graph, related_id),
        dependency_count: edge.dependency_count
      }
    end)
  end

  defp graph_label(%Graph{} = graph, id) do
    graph.groups
    |> Enum.find(&(&1.id == id))
    |> case do
      %GroupNode{label: label} -> label
      _ -> id
    end
  end

  defp search_results(%Graph{} = graph) do
    group_results =
      Enum.map(graph.groups, fn group ->
        %SearchResult{
          id: group.id,
          kind: :group,
          label: group.label,
          subtitle: group.schema,
          status: dominant_status(group.status_counts)
        }
      end)

    asset_results =
      graph.groups
      |> Enum.flat_map(& &1.preview_assets)
      |> Enum.map(fn asset ->
        %SearchResult{
          id: asset.id,
          kind: :asset,
          label: asset.label,
          subtitle: asset.asset_ref_text,
          status: asset.freshness_status
        }
      end)

    schema_results =
      graph.groups
      |> Enum.map(& &1.schema)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.map(&%SearchResult{id: "schema:#{slug(&1)}", kind: :schema, label: &1})

    group_results ++ asset_results ++ schema_results
  end

  defp dominant_status(%{failed: failed}) when failed > 0, do: :failed
  defp dominant_status(%{stale: stale}) when stale > 0, do: :stale
  defp dominant_status(%{running: running}) when running > 0, do: :running
  defp dominant_status(%{unknown: unknown}) when unknown > 0, do: :unknown
  defp dominant_status(_counts), do: :fresh

  defp search_match?(_result, ""), do: true

  defp search_match?(result, query),
    do:
      String.contains?(
        normalize_query(result.label <> " " <> to_string(result.subtitle || "")),
        query
      )

  defp result_rank(:group), do: 0
  defp result_rank(:asset), do: 1
  defp result_rank(:schema), do: 2

  defp page_opts(opts, default_limit) do
    [limit: Keyword.get(opts, :limit, default_limit), offset: Keyword.get(opts, :offset, 0)]
  end

  defp normalize_error(%Error{} = error), do: error

  defp normalize_error(:invalid_pagination),
    do: %Error{code: :invalid_scope, message: "Invalid lineage pagination."}

  defp normalize_error({:storage_failed, reason}),
    do: %Error{
      code: :storage_unavailable,
      message: "Lineage storage is unavailable.",
      retryable?: true,
      details: %{reason: inspect(reason)}
    }

  defp normalize_error(reason),
    do: %Error{
      code: :lineage_projection_unavailable,
      message: "Lineage projection is unavailable.",
      retryable?: true,
      details: %{reason: inspect(reason)}
    }

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp first_text(values) do
    values
    |> Enum.find_value(fn
      value when is_atom(value) and not is_nil(value) -> Atom.to_string(value)
      value when is_binary(value) and value != "" -> value
      value -> if(is_nil(value), do: nil, else: to_string(value))
    end)
  end

  defp layer_rank(layer), do: Enum.find_index(@layers, &(&1 == layer)) || length(@layers)

  defp titleize(value) when is_binary(value) do
    value
    |> String.replace(["_", "-"], " ")
    |> String.split(" ", trim: true)
    |> Enum.map_join(" ", &String.capitalize/1)
  end

  defp slug(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "_")
    |> String.trim("_")
  end

  defp normalize_query(value) do
    value
    |> String.downcase()
    |> String.trim()
  end
end
