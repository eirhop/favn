defmodule Favn.Assets.Planner do
  @moduledoc """
  Build deterministic execution plans from the global graph index.

  The planner produces a deduplicated run graph for one or more targets and
  groups plan nodes into topological stages for parallel execution.

  Determinism guarantees:

    * target refs are normalized, deduplicated, and sorted
    * `stages` contain refs sorted by canonical ref order
    * stage number equals topological depth (`0` for source assets)
  """

  alias Favn.Assets.GraphIndex
  alias Favn.Manifest.PlanningIndex
  alias Favn.Plan
  alias Favn.Ref
  alias Favn.TimePeriod
  alias Favn.Window.{Anchor, Key, Runtime, Spec, Validate}

  @typedoc """
  Planner options.

    * `:dependencies` - `:all` includes transitive upstream dependencies;
      `:none` includes target refs only.
  """
  @type dependencies_mode :: :all | :none

  @type backfill_anchor_range :: %{
          required(:kind) => Anchor.kind(),
          required(:start_at) => DateTime.t(),
          required(:end_at) => DateTime.t(),
          optional(:timezone) => String.t()
        }

  @type plan_opts :: [
          dependencies: dependencies_mode(),
          anchor_window: Anchor.t() | nil,
          anchor_windows: [Anchor.t()],
          anchor_ranges: [backfill_anchor_range()],
          exact_windows: %{optional(Ref.t()) => [Runtime.t()]},
          planning_index: PlanningIndex.t() | nil,
          graph_index: GraphIndex.t() | nil,
          asset_modules: [module()]
        ]

  @spec plan(Ref.t() | [Ref.t()], plan_opts()) :: {:ok, Plan.t()} | {:error, term()}
  def plan(targets, opts \\ []) when is_list(opts) do
    dependencies = Keyword.get(opts, :dependencies, :all)
    anchor_window = Keyword.get(opts, :anchor_window)
    anchor_windows = Keyword.get(opts, :anchor_windows, [])
    anchor_ranges = Keyword.get(opts, :anchor_ranges, [])
    exact_windows = Keyword.get(opts, :exact_windows, %{})
    planning_index = Keyword.get(opts, :planning_index)
    graph_index = Keyword.get(opts, :graph_index)
    asset_modules = Keyword.get(opts, :asset_modules)

    with {:ok, target_refs} <- normalize_targets(targets),
         :ok <- validate_opts(opts),
         :ok <- validate_dependencies_mode(dependencies),
         {:ok, anchors} <- normalize_anchors(anchor_window, anchor_windows, anchor_ranges),
         :ok <- validate_exact_windows(exact_windows),
         {:ok, index} <- resolve_index(planning_index, graph_index, asset_modules),
         :ok <- validate_target_refs(index, target_refs),
         {:ok, refs} <- selected_refs(index, target_refs, dependencies),
         {:ok, projected_index} <- projected_index(index, refs),
         {:ok, graph} <- build_windowed_graph(projected_index, anchors, exact_windows) do
      stage_map = build_node_stage_map(graph.nodes, projected_index.topo_rank)
      ref_stage_map = build_ref_stage_map(graph.nodes, stage_map)

      {:ok,
       %Plan{
         target_refs: target_refs,
         target_node_keys: build_target_node_keys(target_refs, graph.ref_nodes),
         dependencies: dependencies,
         nodes: build_nodes(graph, stage_map, projected_index.assets_by_ref),
         topo_order: projected_index.topo_order,
         stages: build_stages(projected_index, ref_stage_map),
         node_stages: build_node_stages(graph, stage_map, projected_index.topo_rank)
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

  defp validate_opts(opts),
    do:
      Validate.strict_keyword_opts(opts, [
        :dependencies,
        :anchor_window,
        :anchor_windows,
        :anchor_ranges,
        :exact_windows,
        :planning_index,
        :graph_index,
        :asset_modules
      ])

  defp validate_dependencies_mode(:all), do: :ok
  defp validate_dependencies_mode(:none), do: :ok
  defp validate_dependencies_mode(other), do: {:error, {:invalid_dependencies_mode, other}}
  defp validate_anchor_window(nil), do: :ok
  defp validate_anchor_window(%Anchor{} = anchor), do: Anchor.validate(anchor)
  defp validate_anchor_window(other), do: {:error, {:invalid_anchor_window, other}}

  defp validate_anchor_windows(anchor_windows) when is_list(anchor_windows) do
    Enum.reduce_while(anchor_windows, :ok, fn anchor_window, _acc ->
      case validate_anchor_window(anchor_window) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_anchor_windows(other), do: {:error, {:invalid_anchor_windows, other}}

  defp validate_exact_windows(exact_windows) when is_map(exact_windows) do
    Enum.reduce_while(exact_windows, :ok, fn
      {{module, name}, windows}, _acc
      when is_atom(module) and is_atom(name) and is_list(windows) ->
        case validate_runtime_windows(windows) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      {ref, _windows}, _acc ->
        {:halt, {:error, {:invalid_exact_window_ref, ref}}}
    end)
  end

  defp validate_exact_windows(other), do: {:error, {:invalid_exact_windows, other}}

  defp validate_runtime_windows(windows) do
    Enum.reduce_while(windows, :ok, fn
      %Runtime{} = runtime, _acc ->
        case Runtime.validate(runtime) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:invalid_exact_window, reason}}}
        end

      other, _acc ->
        {:halt, {:error, {:invalid_exact_window, other}}}
    end)
  end

  defp normalize_anchors(anchor_window, anchor_windows, anchor_ranges) do
    with :ok <- validate_anchor_window(anchor_window),
         :ok <- validate_anchor_windows(anchor_windows),
         {:ok, expanded} <- expand_anchor_ranges(anchor_ranges) do
      anchors =
        [anchor_window | anchor_windows]
        |> Enum.reject(&is_nil/1)
        |> Kernel.++(expanded)
        |> Enum.uniq_by(& &1.key)
        |> Enum.sort_by(&anchor_sort_key/1)

      {:ok, anchors}
    end
  end

  defp expand_anchor_ranges(anchor_ranges) when is_list(anchor_ranges) do
    Enum.reduce_while(anchor_ranges, {:ok, []}, fn range, {:ok, acc} ->
      case expand_anchor_range(range) do
        {:ok, anchors} -> {:cont, {:ok, acc ++ anchors}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp expand_anchor_ranges(other), do: {:error, {:invalid_anchor_ranges, other}}

  defp expand_anchor_range(
         %{kind: kind, start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at} = range
       ) do
    timezone = Map.get(range, :timezone, "Etc/UTC")
    Anchor.expand_range(kind, start_at, end_at, timezone: timezone)
  end

  defp expand_anchor_range(other), do: {:error, {:invalid_anchor_range, other}}

  defp anchor_sort_key(%Anchor{key: key}), do: Key.encode(key)

  defp resolve_index(%PlanningIndex{} = index, _graph_index, _asset_modules), do: {:ok, index}

  defp resolve_index(invalid_planning_index, _graph_index, _asset_modules)
       when not is_nil(invalid_planning_index),
       do: {:error, :invalid_planning_index}

  defp resolve_index(nil, %GraphIndex{} = index, _asset_modules), do: {:ok, index}

  defp resolve_index(nil, nil, modules) when is_list(modules),
    do: GraphIndex.index_for_modules(modules)

  defp resolve_index(nil, nil, nil), do: {:error, :missing_graph_index_input}
  defp resolve_index(nil, nil, _other), do: {:error, :invalid_asset_modules}
  defp resolve_index(nil, _other, _asset_modules), do: {:error, :invalid_graph_index}

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
    project_index(index, refs)
  end

  defp project_index(%PlanningIndex{} = index, refs), do: PlanningIndex.project(index, refs)

  defp project_index(%GraphIndex{} = index, refs) do
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

  defp build_windowed_graph(index, anchors, exact_windows) do
    with {:ok, ref_nodes} <- build_ref_nodes_by_ref(index, anchors, exact_windows) do
      {:ok, %{nodes: build_windowed_nodes(index, ref_nodes), ref_nodes: ref_nodes}}
    end
  end

  defp build_windowed_nodes(index, ref_nodes) do
    Enum.flat_map(index.topo_order, fn ref ->
      Enum.map(Map.fetch!(ref_nodes, ref), fn node ->
        upstream = build_edges(node, index.upstream |> Map.fetch!(ref), ref_nodes)
        downstream = build_edges(node, index.downstream |> Map.fetch!(ref), ref_nodes)
        Map.merge(node, %{upstream: upstream, downstream: downstream})
      end)
    end)
  end

  defp build_ref_nodes_by_ref(index, anchors, exact_windows) do
    Enum.reduce_while(index.topo_order, {:ok, %{}}, fn ref, {:ok, acc} ->
      asset = Map.fetch!(index.assets_by_ref, ref)

      case build_ref_nodes(ref, asset, anchors, Map.get(exact_windows, ref)) do
        {:ok, nodes} -> {:cont, {:ok, Map.put(acc, ref, nodes)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp build_ref_nodes(ref, _asset, _anchors, exact) when is_list(exact) and exact != [] do
    nodes =
      exact
      |> Enum.uniq_by(& &1.key)
      |> Enum.sort_by(&window_sort_key/1)
      |> Enum.map(fn runtime_window ->
        %{ref: ref, node_key: {ref, runtime_window.key}, window: runtime_window}
      end)

    {:ok, nodes}
  end

  defp build_ref_nodes(ref, asset, [], _exact) do
    case asset_window_spec(asset) do
      %Spec{required: true} -> {:error, {:required_window_missing, ref}}
      _other -> {:ok, [%{ref: ref, node_key: {ref, nil}, window: nil}]}
    end
  end

  defp build_ref_nodes(ref, asset, anchors, _exact) when is_list(anchors) do
    case asset_window_spec(asset) do
      nil ->
        {:ok, [%{ref: ref, node_key: {ref, nil}, window: nil}]}

      %Spec{} = spec ->
        nodes =
          anchors
          |> Enum.flat_map(&expand_windows(&1, spec))
          |> Enum.uniq_by(& &1.key)
          |> Enum.sort_by(&window_sort_key/1)
          |> Enum.map(fn runtime_window ->
            %{ref: ref, node_key: {ref, runtime_window.key}, window: runtime_window}
          end)

        {:ok, nodes}
    end
  end

  defp asset_window_spec(%{window_spec: %Spec{} = spec}), do: spec

  defp asset_window_spec(%{window: window}) do
    case Spec.from_value(window) do
      {:ok, %Spec{} = spec} -> spec
      {:ok, nil} -> nil
      {:error, _reason} -> nil
    end
  end

  defp asset_window_spec(_asset), do: nil

  defp expand_windows(%Anchor{} = anchor_window, %Spec{} = spec) do
    window_count =
      window_units_between(spec.kind, anchor_window.start_at, anchor_window.end_at, spec.timezone)

    runtime_window_count = max(window_count + spec.lookback, 1)
    anchor_start = TimePeriod.floor!(anchor_window.start_at, spec.kind, spec.timezone)
    first_start = TimePeriod.shift!(anchor_start, spec.kind, -spec.lookback)

    for offset <- 0..(runtime_window_count - 1) do
      start_at = TimePeriod.shift!(first_start, spec.kind, offset)
      end_at = TimePeriod.shift!(start_at, spec.kind, 1)
      Runtime.new!(spec.kind, start_at, end_at, anchor_window.key, timezone: spec.timezone)
    end
    |> Enum.sort_by(&window_sort_key/1)
  end

  defp window_units_between(kind, %DateTime{} = start_at, %DateTime{} = end_at, timezone) do
    {:ok, periods} = TimePeriod.expand_range(kind, start_at, end_at, timezone)
    max(length(periods), 1)
  end

  defp window_sort_key(%Runtime{key: key}), do: {key.start_at_us, Key.encode(key)}

  defp build_edges(node, related_refs, ref_nodes) do
    related_refs
    |> Enum.flat_map(fn related_ref -> Map.fetch!(ref_nodes, related_ref) end)
    |> Enum.filter(&windows_compatible?(node.window, &1.window))
    |> Enum.map(& &1.node_key)
    |> Enum.sort_by(&node_key_sort_key/1)
  end

  defp windows_compatible?(nil, _), do: true
  defp windows_compatible?(_, nil), do: true

  defp windows_compatible?(%Runtime{} = left, %Runtime{} = right) do
    DateTime.compare(left.start_at, right.end_at) == :lt and
      DateTime.compare(right.start_at, left.end_at) == :lt
  end

  defp node_key_sort_key({ref, nil}), do: {ref, ""}
  defp node_key_sort_key({ref, key}), do: {ref, Key.encode(key)}

  defp build_node_stage_map(nodes, topo_rank) do
    sorted = Enum.sort_by(nodes, &node_key_sort_key(&1.node_key, topo_rank))

    Enum.reduce(sorted, %{}, fn node, acc ->
      stage =
        node.upstream
        |> Enum.map(&Map.fetch!(acc, &1))
        |> case do
          [] -> 0
          stages -> Enum.max(stages) + 1
        end

      Map.put(acc, node.node_key, stage)
    end)
  end

  defp build_nodes(graph, stage_map, assets_by_ref) do
    graph.nodes
    |> Enum.sort_by(&node_key_sort_key(&1.node_key))
    |> Enum.reduce(%{}, fn node, acc ->
      asset = Map.fetch!(assets_by_ref, node.ref)

      full_node =
        Map.merge(node, %{
          stage: Map.fetch!(stage_map, node.node_key),
          execution_pool: Map.get(asset, :execution_pool),
          action: node_action(asset)
        })

      Map.put(acc, full_node.node_key, full_node)
    end)
  end

  defp node_action(%{type: :source}), do: :observe
  defp node_action(_asset), do: :run

  defp build_stages(index, ref_stage_map) do
    index.topo_order
    |> Enum.group_by(&Map.fetch!(ref_stage_map, &1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {_rank, refs} -> Enum.sort(refs) end)
  end

  defp build_node_stages(graph, stage_map, topo_rank) do
    graph.nodes
    |> Enum.group_by(&Map.fetch!(stage_map, &1.node_key))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {_rank, nodes} ->
      nodes
      |> Enum.map(& &1.node_key)
      |> Enum.sort_by(&node_key_sort_key(&1, topo_rank))
    end)
  end

  defp build_target_node_keys(target_refs, ref_nodes) do
    target_refs
    |> Enum.flat_map(&Map.fetch!(ref_nodes, &1))
    |> Enum.map(& &1.node_key)
    |> Enum.sort_by(&node_key_sort_key/1)
  end

  defp build_ref_stage_map(nodes, node_stage_map) do
    nodes
    |> Enum.group_by(& &1.ref)
    |> Enum.reduce(%{}, fn {ref, ref_nodes}, acc ->
      stage = ref_nodes |> Enum.map(&Map.fetch!(node_stage_map, &1.node_key)) |> Enum.min()
      Map.put(acc, ref, stage)
    end)
  end

  defp node_key_sort_key({ref, nil}, topo_rank), do: {Map.get(topo_rank, ref, 0), ref, ""}

  defp node_key_sort_key({ref, key}, topo_rank),
    do: {Map.get(topo_rank, ref, 0), ref, Key.encode(key)}
end
