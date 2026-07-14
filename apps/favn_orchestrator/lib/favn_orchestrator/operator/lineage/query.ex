defmodule FavnOrchestrator.Operator.Lineage.Query do
  @moduledoc false

  alias FavnOrchestrator.Operator.Lineage.AssetInspector
  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.Edge
  alias FavnOrchestrator.Operator.Lineage.EdgeInspector
  alias FavnOrchestrator.Operator.Lineage.Error
  alias FavnOrchestrator.Operator.Lineage.GroupInspector
  alias FavnOrchestrator.Operator.Lineage.GroupNode
  alias FavnOrchestrator.Operator.Lineage.Model
  alias FavnOrchestrator.Operator.Lineage.SearchResult
  alias FavnOrchestrator.Page

  @spec group(Model.t(), String.t()) :: {:ok, GroupInspector.t()} | {:error, Error.t()}
  def group(%Model{} = model, group_id) do
    case Map.fetch(model.groups_by_id, group_id) do
      {:ok, group} -> {:ok, group_inspector(model, group)}
      :error -> {:error, not_found("Lineage group was not found.")}
    end
  end

  @spec asset(Model.t(), String.t()) :: {:ok, AssetInspector.t()} | {:error, Error.t()}
  def asset(%Model{} = model, asset_id) do
    case Map.fetch(model.asset_nodes_by_id, asset_id) do
      {:ok, asset} -> {:ok, asset_inspector(model, asset)}
      :error -> {:error, not_found("Lineage asset was not found.")}
    end
  end

  @spec edge(Model.t(), String.t()) :: {:ok, EdgeInspector.t()} | {:error, Error.t()}
  def edge(%Model{} = model, edge_id) do
    case Map.fetch(model.edges_by_id, edge_id) do
      {:ok, edge} -> {:ok, edge_inspector(model, edge)}
      :error -> {:error, not_found("Lineage edge was not found.")}
    end
  end

  @spec search(Model.t(), String.t(), keyword()) ::
          {:ok, Page.t(SearchResult.t())} | {:error, :invalid_pagination}
  def search(%Model{} = model, query, opts) do
    with {:ok, page_opts} <- normalize_page(opts, model.graph.limits.search_page_size) do
      normalized_query = normalize_query(query)

      items =
        model
        |> search_results()
        |> Enum.filter(&search_match?(&1, normalized_query))
        |> Enum.sort_by(&{result_rank(&1.kind), String.downcase(&1.label), &1.id})
        |> page_slice(page_opts)

      {:ok, Page.from_fetched(items, page_opts)}
    end
  end

  @spec group_assets(Model.t(), String.t(), keyword()) ::
          {:ok, Page.t(AssetNode.t())} | {:error, Error.t() | :invalid_pagination}
  def group_assets(%Model{} = model, group_id, opts) do
    with {:ok, _group} <- fetch_group(model, group_id),
         {:ok, page_opts} <- normalize_page(opts, model.graph.limits.group_asset_page_size) do
      items = model.group_assets_by_id |> Map.get(group_id, []) |> page_slice(page_opts)
      {:ok, Page.from_fetched(items, page_opts)}
    end
  end

  defp normalize_page(opts, default_limit) do
    Page.normalize_opts(
      limit: Keyword.get(opts, :limit, default_limit),
      offset: Keyword.get(opts, :offset, 0)
    )
  end

  defp page_slice(items, opts) do
    Enum.slice(items, Keyword.fetch!(opts, :offset), Keyword.fetch!(opts, :limit) + 1)
  end

  defp fetch_group(model, group_id) do
    case Map.fetch(model.groups_by_id, group_id) do
      {:ok, group} -> {:ok, group}
      :error -> {:error, not_found("Lineage group was not found.")}
    end
  end

  defp group_inspector(model, %GroupNode{} = group) do
    {upstream, hidden_upstream_count} = adjacent_groups(model, group.id, :upstream)
    {downstream, hidden_downstream_count} = adjacent_groups(model, group.id, :downstream)

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
      upstream: upstream,
      downstream: downstream,
      hidden_upstream_count: hidden_upstream_count,
      hidden_downstream_count: hidden_downstream_count,
      actions: [
        %{label: "Drill into group", kind: :drill_in},
        %{label: "View all assets", kind: :assets}
      ]
    }
  end

  defp asset_inspector(model, %AssetNode{} = asset) do
    {upstream, hidden_upstream_count} = adjacent_groups(model, asset.group_id, :upstream)
    {downstream, hidden_downstream_count} = adjacent_groups(model, asset.group_id, :downstream)

    %AssetInspector{
      id: asset.id,
      title: asset.label,
      asset: asset,
      latest_run: latest_run(asset),
      upstream: upstream,
      downstream: downstream,
      hidden_upstream_count: hidden_upstream_count,
      hidden_downstream_count: hidden_downstream_count,
      actions: [%{label: "Open asset", kind: :asset}, %{label: "Explain lineage", kind: :explain}]
    }
  end

  defp edge_inspector(model, %Edge{} = edge) do
    %EdgeInspector{
      id: edge.id,
      title: "#{edge.dependency_count} dependencies",
      edge: edge,
      upstream_label: graph_label(model, edge.from),
      downstream_label: graph_label(model, edge.to),
      dependencies: edge.preview_dependencies,
      affected_statuses: %{},
      actions: [%{label: "View dependency list", kind: :dependencies}]
    }
  end

  defp latest_run(%AssetNode{latest_run_id: nil}), do: nil
  defp latest_run(%AssetNode{} = asset), do: %{id: asset.latest_run_id, status: asset.run_status}

  defp adjacent_groups(model, group_id, direction) do
    limit = model.graph.limits.max_inspector_adjacent_groups

    items =
      model.edges
      |> Enum.filter(&adjacent?(&1, group_id, direction))
      |> Enum.map(fn edge ->
        related_id = related_group_id(edge, direction)

        %{
          id: related_id,
          label: graph_label(model, related_id),
          dependency_count: edge.dependency_count
        }
      end)
      |> Enum.sort_by(&{String.downcase(&1.label), &1.id})

    {Enum.take(items, limit), max(length(items) - limit, 0)}
  end

  defp adjacent?(edge, group_id, :upstream), do: edge.to == group_id
  defp adjacent?(edge, group_id, :downstream), do: edge.from == group_id
  defp related_group_id(edge, :upstream), do: edge.from
  defp related_group_id(edge, :downstream), do: edge.to

  defp graph_label(model, id) do
    case Map.get(model.groups_by_id, id) do
      %GroupNode{label: label} -> label
      nil -> id
    end
  end

  defp search_results(model) do
    group_results =
      Enum.map(model.groups, fn group ->
        %SearchResult{
          id: group.id,
          kind: :group,
          label: group.label,
          subtitle: group.schema,
          status: dominant_status(group.status_counts)
        }
      end)

    asset_results =
      Enum.map(model.asset_nodes_by_id, fn {_id, asset} ->
        %SearchResult{
          id: asset.id,
          kind: :asset,
          label: asset.label,
          subtitle: asset.asset_ref_text,
          status: asset.freshness_status
        }
      end)

    schema_results =
      model.groups
      |> Enum.map(& &1.schema)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.map(
        &%SearchResult{id: "schema:#{URI.encode_www_form(&1)}", kind: :schema, label: &1}
      )

    group_results ++ asset_results ++ schema_results
  end

  defp dominant_status(%{failed: failed}) when failed > 0, do: :failed
  defp dominant_status(%{stale: stale}) when stale > 0, do: :stale
  defp dominant_status(%{running: running}) when running > 0, do: :running
  defp dominant_status(%{unknown: unknown}) when unknown > 0, do: :unknown
  defp dominant_status(_counts), do: :fresh

  defp search_match?(_result, ""), do: true

  defp search_match?(result, query) do
    result.label
    |> Kernel.<>(" " <> to_string(result.subtitle || ""))
    |> normalize_query()
    |> String.contains?(query)
  end

  defp normalize_query(value), do: value |> String.downcase() |> String.trim()

  defp result_rank(:group), do: 0
  defp result_rank(:asset), do: 1
  defp result_rank(:schema), do: 2

  defp not_found(message), do: %Error{code: :node_not_found, message: message}
end
