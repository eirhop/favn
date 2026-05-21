defmodule Favn.Manifest.PlanningIndex do
  @moduledoc """
  Pure runtime planning index built from a pinned manifest graph.

  The planning index is the in-memory query shape used by planners after a
  `%Favn.Manifest{}` has been built, persisted, and pinned in a manifest
  version. It validates that manifest assets and the embedded
  `%Favn.Manifest.Graph{}` agree before exposing adjacency, transitive closure,
  and topological rank data.
  """

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Ref

  @typedoc """
  Manifest-derived dependency index for runtime planning.
  """
  @type t :: %__MODULE__{
          assets_by_ref: %{Ref.t() => Asset.t()},
          upstream: %{Ref.t() => MapSet.t(Ref.t())},
          downstream: %{Ref.t() => MapSet.t(Ref.t())},
          transitive_upstream: %{Ref.t() => MapSet.t(Ref.t())},
          transitive_downstream: %{Ref.t() => MapSet.t(Ref.t())},
          topo_order: [Ref.t()],
          topo_rank: %{Ref.t() => non_neg_integer()}
        }

  @typedoc """
  Planning index construction errors.
  """
  @type error ::
          :invalid_manifest
          | {:invalid_asset_ref, term()}
          | {:duplicate_asset_ref, Ref.t()}
          | {:missing_manifest_graph, :non_empty_assets}
          | {:manifest_graph_mismatch, :nodes | :edges | :topo_order}
          | {:unknown_projection_ref, Ref.t()}
          | Graph.error()

  defstruct assets_by_ref: %{},
            upstream: %{},
            downstream: %{},
            transitive_upstream: %{},
            transitive_downstream: %{},
            topo_order: [],
            topo_rank: %{}

  @doc """
  Builds a planning index from a canonical manifest.
  """
  @spec build(Manifest.t()) :: {:ok, t()} | {:error, error()}
  def build(%Manifest{assets: assets, graph: %Graph{} = graph}) do
    build(graph, assets)
  end

  def build(_other), do: {:error, :invalid_manifest}

  @doc """
  Builds a planning index from an explicit manifest graph and asset list.
  """
  @spec build(Graph.t(), [Asset.t()]) :: {:ok, t()} | {:error, error()}
  def build(%Graph{nodes: [], edges: [], topo_order: []}, assets)
      when is_list(assets) and assets != [],
      do: {:error, {:missing_manifest_graph, :non_empty_assets}}

  def build(%Graph{} = graph, assets) when is_list(assets) do
    with {:ok, assets_by_ref} <- build_assets_by_ref(assets),
         {:ok, expected_graph} <- Graph.build(assets),
         :ok <- validate_graph(graph, expected_graph),
         {upstream, downstream} <- build_adjacency(graph),
         transitive_upstream <- build_transitive_index(upstream),
         transitive_downstream <- build_transitive_index(downstream) do
      {:ok,
       %__MODULE__{
         assets_by_ref: assets_by_ref,
         upstream: upstream,
         downstream: downstream,
         transitive_upstream: transitive_upstream,
         transitive_downstream: transitive_downstream,
         topo_order: graph.topo_order,
         topo_rank: build_topo_rank(graph.topo_order)
       }}
    end
  end

  def build(%Graph{}, invalid), do: {:error, {:invalid_assets_input, invalid}}

  @doc """
  Projects an existing planning index to a selected ref set.

  Dependencies outside the selected set are removed before rebuilding the
  manifest graph/index contract for the projected plan.
  """
  @spec project(t(), MapSet.t(Ref.t())) :: {:ok, t()} | {:error, error()}
  def project(%__MODULE__{} = index, %MapSet{} = refs) do
    with :ok <- validate_projection_refs(index, refs),
         assets <- project_assets(index, refs),
         {:ok, graph} <- Graph.build(assets) do
      build(graph, assets)
    end
  end

  defp validate_projection_refs(%__MODULE__{} = index, %MapSet{} = refs) do
    case Enum.find(refs, &(not Map.has_key?(index.assets_by_ref, &1))) do
      nil -> :ok
      ref -> {:error, {:unknown_projection_ref, ref}}
    end
  end

  defp project_assets(%__MODULE__{} = index, %MapSet{} = refs) do
    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
    |> Enum.map(fn %Asset{} = asset ->
      %{asset | depends_on: Enum.filter(asset.depends_on, &MapSet.member?(refs, &1))}
    end)
  end

  defp build_assets_by_ref(assets) do
    Enum.reduce_while(assets, {:ok, %{}}, fn
      %Asset{ref: {module, name}} = asset, {:ok, acc}
      when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name) ->
        if Map.has_key?(acc, asset.ref) do
          {:halt, {:error, {:duplicate_asset_ref, asset.ref}}}
        else
          {:cont, {:ok, Map.put(acc, asset.ref, asset)}}
        end

      %Asset{ref: ref}, _acc ->
        {:halt, {:error, {:invalid_asset_ref, ref}}}

      other, _acc ->
        {:halt, {:error, {:invalid_asset_ref, other}}}
    end)
  end

  defp validate_graph(%Graph{} = graph, %Graph{} = expected_graph) do
    cond do
      graph.nodes != expected_graph.nodes ->
        {:error, {:manifest_graph_mismatch, :nodes}}

      graph.edges != expected_graph.edges ->
        {:error, {:manifest_graph_mismatch, :edges}}

      graph.topo_order != expected_graph.topo_order ->
        {:error, {:manifest_graph_mismatch, :topo_order}}

      true ->
        :ok
    end
  end

  defp build_adjacency(%Graph{} = graph) do
    empty_sets = Map.new(graph.nodes, &{&1, MapSet.new()})

    Enum.reduce(graph.edges, {empty_sets, empty_sets}, fn %{from: from, to: to},
                                                          {upstream, downstream} ->
      {
        Map.update!(upstream, to, &MapSet.put(&1, from)),
        Map.update!(downstream, from, &MapSet.put(&1, to))
      }
    end)
  end

  defp build_transitive_index(adjacency) do
    Map.new(adjacency, fn {ref, _neighbors} ->
      reachable = reachable_from_map(ref, adjacency, %{})
      {ref, reachable |> Map.keys() |> MapSet.new()}
    end)
  end

  defp reachable_from_map(ref, adjacency, visited) do
    adjacency
    |> Map.fetch!(ref)
    |> Enum.reduce(visited, fn neighbor, acc ->
      if Map.has_key?(acc, neighbor) do
        acc
      else
        reachable_from_map(neighbor, adjacency, Map.put(acc, neighbor, true))
      end
    end)
  end

  defp build_topo_rank(order) do
    order
    |> Enum.with_index()
    |> Map.new(fn {ref, index} -> {ref, index} end)
  end
end
