defmodule FavnOrchestrator.Operator.Lineage.Projection do
  @moduledoc false

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.Error
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.GroupNode
  alias FavnOrchestrator.Operator.Lineage.Model
  alias FavnOrchestrator.Operator.Lineage.Request
  alias FavnOrchestrator.Operator.Lineage.Summary
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TargetStatus

  @layers [:raw, :staging, :core, :marts, :dashboards]
  @status_keys [:fresh, :stale, :failed, :running, :unknown]
  @status_priority %{failed: 0, stale: 1, running: 2, unknown: 3, fresh: 4}

  @spec read(Request.t()) :: {:ok, Model.t()} | {:error, Error.t() | term()}
  def read(%Request{} = request) do
    started_at = System.monotonic_time(:millisecond)

    with {:ok, version} <- fetch_version(request.manifest_version_id),
         :ok <- check_timeout(started_at, request.limits.timeout_ms),
         assets = List.wrap(version.manifest.assets),
         targets = Enum.map(assets, &asset_target/1),
         {:ok, statuses} <- target_statuses(version.manifest_version_id, targets),
         :ok <- check_timeout(started_at, request.limits.timeout_ms),
         model = build_model(version, assets, targets, statuses, request),
         :ok <- check_timeout(started_at, request.limits.timeout_ms) do
      {:ok, model}
    end
  end

  defp fetch_version(:active) do
    case FavnOrchestrator.active_manifest() do
      {:ok, manifest_version_id} ->
        fetch_version(manifest_version_id)

      {:error, _reason} ->
        {:error,
         %Error{
           code: :active_manifest_not_found,
           message: "No active manifest is available.",
           retryable?: true
         }}
    end
  end

  defp fetch_version(manifest_version_id) do
    case FavnOrchestrator.get_manifest(manifest_version_id) do
      {:ok, %Version{} = version} ->
        {:ok, version}

      {:error, :not_found} ->
        {:error, %Error{code: :manifest_not_found, message: "Manifest version was not found."}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_timeout(started_at, timeout_ms) do
    if System.monotonic_time(:millisecond) - started_at > timeout_ms do
      {:error,
       %Error{code: :query_timeout, message: "Lineage query timed out.", retryable?: true}}
    else
      :ok
    end
  end

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

  defp build_model(%Version{} = version, assets, targets, statuses, request) do
    target_by_ref = Map.new(targets, &{&1.ref, &1})
    status_by_ref = Map.new(targets, &{&1.ref, Map.fetch!(statuses, &1.target_id)})
    contexts = build_group_contexts(assets, target_by_ref, status_by_ref, request)

    {groups, asset_nodes_by_id, group_by_ref} =
      build_group_nodes(contexts, request)

    edges = build_edges(version, group_by_ref, asset_nodes_by_id, request)

    group_assets_by_id =
      asset_nodes_by_id
      |> Map.values()
      |> Enum.group_by(& &1.group_id, & &1)
      |> Map.new(fn {group_id, nodes} -> {group_id, sort_asset_nodes(nodes)} end)

    visible_groups = Enum.take(groups, request.limits.max_visible_groups)
    visible_ids = MapSet.new(visible_groups, & &1.id)

    visible_edges =
      edges
      |> Enum.filter(
        &(MapSet.member?(visible_ids, &1.from) and MapSet.member?(visible_ids, &1.to))
      )
      |> Enum.take(request.limits.max_visible_edges)

    graph = %Graph{
      manifest_version_id: version.manifest_version_id,
      scope: request.scope,
      selected_id: request.selected_id,
      view_mode: request.view_mode,
      nodes: visible_groups,
      groups: visible_groups,
      edges: visible_edges,
      summary: summary(length(assets), groups, edges, visible_groups, visible_edges),
      limits: request.limits,
      layout: %{direction: :left_to_right, layers: @layers},
      generated_at: DateTime.utc_now()
    }

    %Model{
      graph: graph,
      groups: groups,
      groups_by_id: Map.new(groups, &{&1.id, &1}),
      edges: edges,
      edges_by_id: Map.new(edges, &{&1.id, &1}),
      asset_nodes_by_id: asset_nodes_by_id,
      group_assets_by_id: group_assets_by_id
    }
  end

  defp build_group_contexts(assets, target_by_ref, status_by_ref, request) do
    assets
    |> Enum.group_by(&group_key/1)
    |> Enum.map(fn {key, grouped_assets} ->
      nodes =
        Enum.map(grouped_assets, fn asset ->
          asset_node(
            asset,
            Map.fetch!(target_by_ref, asset.ref),
            Map.fetch!(status_by_ref, asset.ref),
            key,
            request
          )
        end)

      %{key: key, assets: grouped_assets, asset_nodes: sort_asset_nodes(nodes)}
    end)
    |> Enum.sort_by(&{layer_rank(&1.key.layer), String.downcase(&1.key.label), &1.key.id})
    |> add_layer_ranks()
  end

  defp add_layer_ranks(contexts) do
    {ranked, _ranks} =
      Enum.map_reduce(contexts, %{}, fn context, ranks ->
        rank = Map.get(ranks, context.key.layer, 0)
        {Map.put(context, :rank, rank), Map.put(ranks, context.key.layer, rank + 1)}
      end)

    ranked
  end

  defp build_group_nodes(contexts, request) do
    initial = {[], %{}, %{}, request.limits.max_visible_asset_nodes}

    {groups, nodes_by_id, group_by_ref, _remaining_preview_slots} =
      Enum.reduce(contexts, initial, fn context, {groups, nodes_by_id, group_by_ref, slots} ->
        preview_limit = min(request.limits.max_preview_assets_per_group, slots)
        preview = Enum.take(context.asset_nodes, preview_limit)

        group = %GroupNode{
          id: context.key.id,
          label: context.key.label,
          system: context.key.system,
          schema: context.key.schema,
          layer: context.key.layer,
          type: context.key.type,
          state: group_state(context.key.id, request, context.key.layer),
          asset_count: length(context.asset_nodes),
          preview_asset_ids: Enum.map(preview, & &1.id),
          preview_assets: preview,
          hidden_asset_count: length(context.asset_nodes) - length(preview),
          status_counts: status_counts(context.asset_nodes),
          top_issues: top_issues(context.asset_nodes),
          position_hint: %{layer: context.key.layer, rank: context.rank},
          selected?: request.selected_id == context.key.id
        }

        next_nodes = Map.merge(nodes_by_id, Map.new(context.asset_nodes, &{&1.id, &1}))

        next_group_by_ref =
          Enum.reduce(context.assets, group_by_ref, &Map.put(&2, &1.ref, group.id))

        {[group | groups], next_nodes, next_group_by_ref, slots - length(preview)}
      end)

    {Enum.reverse(groups), nodes_by_id, group_by_ref}
  end

  defp build_edges(%Version{} = version, group_by_ref, asset_nodes_by_id, request) do
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
        dependency = %{
          from:
            edge_asset_label(
              Map.get(asset_nodes_by_id, TargetStatus.target_id_for_asset(from)),
              from
            ),
          to:
            edge_asset_label(Map.get(asset_nodes_by_id, TargetStatus.target_id_for_asset(to)), to)
        }

        Map.update(acc, {from_group, to_group}, [dependency], &[dependency | &1])
      end
    end)
    |> Enum.map(fn {{from, to}, dependencies} -> edge(from, to, dependencies, request) end)
    |> Enum.sort_by(&{&1.from, &1.to})
  end

  defp edges_from_assets(assets) do
    Enum.flat_map(assets, fn asset ->
      Enum.map(List.wrap(asset.depends_on), &%{from: &1, to: asset.ref})
    end)
  end

  defp edge(from, to, dependencies, request) do
    dependencies = Enum.sort_by(dependencies, &{&1.from, &1.to})
    preview = Enum.take(dependencies, request.limits.max_dependency_previews_per_edge)
    id = "edge:#{from}->#{to}"

    %Edge{
      id: id,
      from: from,
      to: to,
      dependency_count: length(dependencies),
      aggregated?: length(dependencies) > 1,
      preview_dependencies: preview,
      hidden_dependency_count: length(dependencies) - length(preview),
      selected?: request.selected_id == id
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

    %{
      id: group_id(layer, system, schema, label),
      label: label,
      system: system,
      schema: schema,
      layer: layer,
      type: group_type(layer)
    }
  end

  defp group_id(:raw, system, _schema, label),
    do: "group:raw:#{id_component(system || label)}"

  defp group_id(:staging, system, schema, label),
    do: "group:staging:#{id_component(system || label)}:#{id_component(schema || label)}"

  defp group_id(layer, _system, schema, label) when layer in [:core, :marts, :dashboards],
    do: "group:#{layer}:#{id_component(schema || label)}"

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

    normalized =
      [
        explicit,
        relation && relation.catalog,
        relation && relation.schema,
        relation && relation.name,
        asset_name(asset)
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(":")
      |> String.downcase()

    cond do
      String.contains?(normalized, "dashboard") or String.contains?(normalized, "report") ->
        :dashboards

      String.contains?(normalized, "mart") ->
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
      _ -> asset_name(asset)
    end
  end

  defp asset_name(%Asset{name: name}) when is_atom(name), do: Atom.to_string(name)
  defp asset_name(%Asset{name: name}) when is_binary(name), do: name
  defp asset_name(_asset), do: "asset"

  defp edge_asset_label(%AssetNode{label: label}, _ref), do: label
  defp edge_asset_label(_node, {_module, name}) when is_atom(name), do: Atom.to_string(name)

  defp product_status(%TargetStatus{status: :running}), do: :running
  defp product_status(%TargetStatus{status: :failed}), do: :failed
  defp product_status(%TargetStatus{status: :unknown}), do: :unknown

  defp product_status(%TargetStatus{freshness_status: status})
       when status in [:stale, :expired, :blocked],
       do: :stale

  defp product_status(%TargetStatus{status: :healthy}), do: :fresh
  defp product_status(_status), do: :unknown

  defp sort_asset_nodes(nodes) do
    Enum.sort_by(
      nodes,
      &{Map.fetch!(@status_priority, &1.freshness_status), String.downcase(&1.label), &1.id}
    )
  end

  defp status_counts(nodes) do
    Enum.reduce(nodes, empty_status_counts(), fn node, counts ->
      Map.update!(counts, node.freshness_status, &(&1 + 1))
    end)
  end

  defp empty_status_counts, do: Map.new(@status_keys, &{&1, 0})

  defp top_issues(nodes) do
    counts = status_counts(nodes)

    [{:failed, "Failed"}, {:stale, "Stale"}, {:running, "Running"}]
    |> Enum.map(fn {kind, label} ->
      %{kind: kind, label: label, count: Map.fetch!(counts, kind)}
    end)
    |> Enum.reject(&(&1.count == 0))
  end

  defp summary(total_assets, groups, edges, visible_groups, visible_edges) do
    visible_status_counts =
      Enum.reduce(
        visible_groups,
        empty_status_counts(),
        &merge_status_counts(&2, &1.status_counts)
      )

    visible_asset_nodes =
      Enum.reduce(visible_groups, 0, &(length(&1.preview_assets) + &2))

    %Summary{
      total_assets: total_assets,
      visible_assets: visible_asset_nodes,
      total_groups: length(groups),
      visible_groups: length(visible_groups),
      total_edges: length(edges),
      visible_edges: length(visible_edges),
      status_counts: visible_status_counts,
      truncated?:
        total_assets > visible_asset_nodes or length(groups) > length(visible_groups) or
          length(edges) > length(visible_edges)
    }
  end

  defp merge_status_counts(left, right) do
    Map.new(@status_keys, &{&1, Map.get(left, &1, 0) + Map.get(right, &1, 0)})
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp first_text(values) do
    Enum.find_value(values, fn
      value when is_atom(value) and not is_nil(value) -> Atom.to_string(value)
      value when is_binary(value) and value != "" -> value
      _value -> nil
    end)
  end

  defp layer_rank(layer), do: Enum.find_index(@layers, &(&1 == layer)) || length(@layers)

  defp titleize(value) do
    value
    |> String.replace(["_", "-"], " ")
    |> String.split(" ", trim: true)
    |> Enum.map_join(" ", &String.capitalize/1)
  end

  defp slug(value) do
    value
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "_")
    |> String.trim("_")
  end

  defp id_component(value), do: URI.encode_www_form(value)
end
