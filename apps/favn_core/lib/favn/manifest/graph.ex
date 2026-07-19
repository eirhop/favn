defmodule Favn.Manifest.Graph do
  @moduledoc """
  Pure deterministic dependency graph embedded in canonical manifests.
  """

  @type ref :: {module(), atom()}

  @type edge :: %{
          required(:from) => ref(),
          required(:to) => ref()
        }

  @type t :: %__MODULE__{
          nodes: [ref()],
          edges: [edge()],
          topo_order: [ref()]
        }

  @type error ::
          {:missing_dependency, ref(), ref()}
          | {:cycle, [ref()]}
          | {:invalid_assets_input, term()}

  defstruct nodes: [], edges: [], topo_order: []

  @spec build(term()) :: {:ok, t()} | {:error, error()}
  def build(assets) when is_list(assets) do
    nodes =
      assets
      |> Enum.map(&Map.get(&1, :ref))
      |> Enum.uniq()
      |> Enum.sort(&compare_refs/2)

    with :ok <- validate_dependencies(assets, MapSet.new(nodes)),
         upstream <- build_upstream(assets),
         downstream <- build_downstream(nodes, upstream),
         {:ok, topo_order} <- topological_order(nodes, upstream, downstream) do
      {:ok,
       %__MODULE__{
         nodes: nodes,
         edges: build_edges(upstream),
         topo_order: topo_order
       }}
    end
  end

  def build(invalid), do: {:error, {:invalid_assets_input, invalid}}

  defp validate_dependencies(assets, node_set) do
    assets
    |> Enum.reduce_while(:ok, fn asset, :ok ->
      ref = Map.get(asset, :ref)

      case Enum.find(normalized_depends_on(asset), &(not MapSet.member?(node_set, &1))) do
        nil -> {:cont, :ok}
        dependency -> {:halt, {:error, {:missing_dependency, ref, dependency}}}
      end
    end)
  end

  defp build_upstream(assets) do
    Map.new(assets, fn asset ->
      {Map.get(asset, :ref), normalized_depends_on(asset)}
    end)
  end

  defp build_downstream(nodes, upstream) do
    nodes
    |> Enum.reduce(Map.new(nodes, &{&1, []}), fn node, acc ->
      upstream
      |> Map.get(node, [])
      |> Enum.reduce(acc, fn dependency, by_ref ->
        Map.update!(by_ref, dependency, &[node | &1])
      end)
    end)
    |> Map.new(fn {ref, children} -> {ref, Enum.sort(children, &compare_refs/2)} end)
  end

  defp topological_order(nodes, upstream, downstream) do
    in_degree = Map.new(nodes, &{&1, length(Map.get(upstream, &1, []))})

    queue =
      in_degree
      |> Enum.filter(fn {_ref, degree} -> degree == 0 end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.map(&queue_entry/1)
      |> :gb_sets.from_list()

    {order, remaining_degrees} = consume_queue(queue, [], in_degree, downstream)

    if length(order) == length(nodes) do
      {:ok, order}
    else
      cycle_nodes =
        remaining_degrees
        |> Enum.filter(fn {_ref, degree} -> degree > 0 end)
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort(&compare_refs/2)

      {:error, {:cycle, cycle_nodes}}
    end
  end

  defp consume_queue(queue, order, degrees, downstream) do
    if :gb_sets.is_empty(queue) do
      {Enum.reverse(order), degrees}
    else
      {{_sort_key, node}, rest} = :gb_sets.take_smallest(queue)

      {updated_degrees, discovered} =
        downstream
        |> Map.get(node, [])
        |> Enum.reduce({degrees, []}, fn child, {degree_map, ready} ->
          next_value = Map.fetch!(degree_map, child) - 1
          next_map = Map.put(degree_map, child, next_value)

          if next_value == 0 do
            {next_map, [child | ready]}
          else
            {next_map, ready}
          end
        end)

      next_queue = Enum.reduce(discovered, rest, &:gb_sets.add(queue_entry(&1), &2))

      consume_queue(next_queue, [node | order], updated_degrees, downstream)
    end
  end

  defp build_edges(upstream) do
    upstream
    |> Enum.flat_map(fn {to, dependencies} ->
      Enum.map(dependencies, fn from -> %{from: from, to: to} end)
    end)
    |> Enum.sort_by(fn edge -> {ref_sort_key(edge.from), ref_sort_key(edge.to)} end)
  end

  defp normalized_depends_on(asset) do
    asset
    |> Map.get(:depends_on, [])
    |> List.wrap()
    |> Enum.uniq()
    |> Enum.sort(&compare_refs/2)
  end

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    ref_sort_key({left_module, left_name}) <= ref_sort_key({right_module, right_name})
  end

  defp queue_entry(ref), do: {ref_sort_key(ref), ref}

  defp ref_sort_key({module, name}), do: {Atom.to_string(module), Atom.to_string(name)}
end
