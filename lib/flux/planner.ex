defmodule Flux.Planner do
  @moduledoc """
  Build deterministic execution plans from the global graph index.

  The planner produces a deduplicated run graph for one or more targets and
  groups plan nodes into topological stages for parallel execution.

  Determinism guarantees:

    * target refs are normalized, deduplicated, and sorted
    * `stages` contain refs sorted by canonical ref order
    * stage number equals topological depth (`0` for source assets)
  """

  alias Flux.GraphIndex
  alias Flux.Plan
  alias Flux.Ref

  @typedoc """
  Planner options.

    * `:dependencies` - `:all` includes transitive upstream dependencies;
      `:none` includes target refs only.
  """
  @type plan_opts :: [dependencies: Flux.dependencies_mode()]

  @spec plan(Ref.t() | [Ref.t()], plan_opts()) :: {:ok, Plan.t()} | {:error, term()}
  def plan(targets, opts \\ []) when is_list(opts) do
    dependencies = Keyword.get(opts, :dependencies, :all)

    with {:ok, target_refs} <- normalize_targets(targets),
         :ok <- validate_dependencies_mode(dependencies),
         {:ok, index} <- GraphIndex.get(),
         :ok <- validate_target_refs(index, target_refs),
         {:ok, refs} <- selected_refs(index, target_refs, dependencies),
         {:ok, projected_index} <- projected_index(index, refs) do
      stage_map = build_stage_map(projected_index)

      {:ok,
       %Plan{
         target_refs: target_refs,
         dependencies: dependencies,
         nodes: build_nodes(projected_index, stage_map),
         topo_order: projected_index.topo_order,
         stages: build_stages(projected_index, stage_map)
       }}
    end
  end

  defp normalize_targets({module, name}) when is_atom(module) and is_atom(name),
    do: {:ok, [{module, name}]}

  defp normalize_targets([]), do: {:error, :empty_targets}

  defp normalize_targets(targets) when is_list(targets),
    do: normalize_target_list(targets, [])

  defp normalize_targets(_targets), do: {:error, :invalid_target_ref}

  defp normalize_target_list([], refs), do: {:ok, refs |> Enum.uniq() |> Enum.sort()}

  defp normalize_target_list([{module, name} | rest], refs)
       when is_atom(module) and is_atom(name) do
    normalize_target_list(rest, [{module, name} | refs])
  end

  defp normalize_target_list([_invalid | _rest], _refs), do: {:error, :invalid_target_ref}

  defp validate_dependencies_mode(:all), do: :ok
  defp validate_dependencies_mode(:none), do: :ok
  defp validate_dependencies_mode(other), do: {:error, {:invalid_dependencies_mode, other}}

  defp validate_target_refs(index, refs) do
    case Enum.find(refs, &(not Map.has_key?(index.assets_by_ref, &1))) do
      nil -> :ok
      _ref -> {:error, :asset_not_found}
    end
  end

  defp selected_refs(_index, target_refs, :none), do: {:ok, MapSet.new(target_refs)}

  defp selected_refs(index, target_refs, :all) do
    refs =
      Enum.reduce(target_refs, MapSet.new(), fn ref, acc ->
        upstream_refs = Map.fetch!(index.transitive_upstream, ref)

        acc
        |> MapSet.union(upstream_refs)
        |> MapSet.put(ref)
      end)

    {:ok, refs}
  end

  defp projected_index(index, refs) do
    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
    |> project_assets(refs)
    |> GraphIndex.build_index()
  end

  defp project_assets(assets, refs) do
    Enum.map(assets, fn asset ->
      %{asset | depends_on: Enum.filter(asset.depends_on, &MapSet.member?(refs, &1))}
    end)
  end

  defp build_stage_map(index) do
    Enum.reduce(index.topo_order, %{}, fn ref, acc ->
      stage =
        index.upstream
        |> Map.fetch!(ref)
        |> Enum.map(&Map.fetch!(acc, &1))
        |> case do
          [] -> 0
          stages -> Enum.max(stages) + 1
        end

      Map.put(acc, ref, stage)
    end)
  end

  defp build_nodes(index, stage_map) do
    Enum.reduce(index.topo_order, %{}, fn ref, acc ->
      node = %{
        ref: ref,
        upstream: index.upstream |> Map.fetch!(ref) |> Enum.sort(),
        downstream: index.downstream |> Map.fetch!(ref) |> Enum.sort(),
        stage: Map.fetch!(stage_map, ref),
        action: :run
      }

      Map.put(acc, ref, node)
    end)
  end

  defp build_stages(index, stage_map) do
    index.topo_order
    |> Enum.group_by(&Map.fetch!(stage_map, &1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {_rank, refs} -> Enum.sort(refs) end)
  end
end
