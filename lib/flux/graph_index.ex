defmodule Flux.GraphIndex do
  @moduledoc """
  Global dependency graph index for configured Flux assets.

  `Flux.GraphIndex` builds a read-only directed acyclic graph (DAG) from the
  canonical asset catalog loaded by `Flux.Registry`. The index is computed
  during application startup and cached in `:persistent_term` for fast,
  repeated read access.

  The graph is intended to support:

    * validating that every declared dependency exists
    * rejecting circular dependencies
    * upstream and downstream graph traversal
    * deterministic topological ordering for later planning layers
    * transitive dependency queries for graph inspection APIs

  Flux models dependencies as a DAG rather than a tree. Shared upstream assets
  are therefore allowed and are expected to execute at most once within a run,
  even when multiple downstream assets depend on them.
  """

  alias Flux.Asset
  alias Flux.Ref

  @index_key {__MODULE__, :index}

  @typedoc """
  Canonical cached dependency index.
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
  Graph construction errors.
  """
  @type error ::
          {:missing_dependency, Ref.t(), Ref.t()}
          | {:cycle, [Ref.t()]}
          | :invalid_opts
          | Flux.Registry.error()

  @typedoc """
  Direction used when selecting related assets or subgraphs.
  """
  @type direction :: :upstream | :downstream | :both

  @typedoc """
  Metadata filters applied to graph queries.

  When `tags` are provided, an asset matches if it includes at least one of the
  requested tags. Other filters require membership in the provided list.
  """
  @type filter_opts :: [
          direction: direction(),
          include_target: boolean(),
          transitive: boolean(),
          tags: [Asset.tag()],
          kinds: [Asset.kind()],
          modules: [module()],
          names: [atom()]
        ]

  defstruct assets_by_ref: %{},
            upstream: %{},
            downstream: %{},
            transitive_upstream: %{},
            transitive_downstream: %{},
            topo_order: [],
            topo_rank: %{}

  @doc """
  Return the cached global dependency index.
  """
  @spec get() :: {:ok, t()} | {:error, error()}
  def get do
    case :persistent_term.get(@index_key, :undefined) do
      :undefined -> load_and_fetch_index()
      %__MODULE__{} = index -> {:ok, index}
    end
  end

  @doc """
  Build and cache the dependency index from the current registry catalog.
  """
  @spec load() :: :ok | {:error, error()}
  def load do
    with {:ok, assets} <- Flux.Registry.list_assets(),
         {:ok, %__MODULE__{} = index} <- build_index(assets) do
      :persistent_term.put(@index_key, index)
      :ok
    end
  end

  @doc false
  @spec reload() :: :ok | {:error, error()}
  def reload, do: load()

  @doc """
  Return the immediate upstream dependencies for an asset.
  """
  @spec upstream_of(Ref.t()) :: {:ok, [Ref.t()]} | {:error, error() | :asset_not_found}
  def upstream_of(ref) do
    with {:ok, index} <- get(),
         {:ok, refs} <- fetch_set(index.upstream, ref) do
      {:ok, sort_refs(refs, index)}
    end
  end

  @doc """
  Return the immediate downstream dependents for an asset.
  """
  @spec downstream_of(Ref.t()) :: {:ok, [Ref.t()]} | {:error, error() | :asset_not_found}
  def downstream_of(ref) do
    with {:ok, index} <- get(),
         {:ok, refs} <- fetch_set(index.downstream, ref) do
      {:ok, sort_refs(refs, index)}
    end
  end

  @doc """
  Return the full recursive upstream closure for an asset.
  """
  @spec transitive_upstream_of(Ref.t()) :: {:ok, [Ref.t()]} | {:error, error() | :asset_not_found}
  def transitive_upstream_of(ref) do
    with {:ok, index} <- get(),
         {:ok, refs} <- fetch_set(index.transitive_upstream, ref) do
      {:ok, sort_refs(refs, index)}
    end
  end

  @doc """
  Return the full recursive downstream closure for an asset.
  """
  @spec transitive_downstream_of(Ref.t()) ::
          {:ok, [Ref.t()]} | {:error, error() | :asset_not_found}
  def transitive_downstream_of(ref) do
    with {:ok, index} <- get(),
         {:ok, refs} <- fetch_set(index.transitive_downstream, ref) do
      {:ok, sort_refs(refs, index)}
    end
  end

  @doc """
  Return related assets for a reference.

  By default this returns the full transitive upstream asset list for the
  target, which is the most useful shape for dependency inspection and later
  planning layers.
  """
  @spec related_assets(Ref.t(), filter_opts()) ::
          {:ok, [Asset.t()]} | {:error, error() | :asset_not_found}
  def related_assets(ref, opts \\ []) when is_list(opts) do
    with {:ok, index} <- get(),
         {:ok, refs} <- selected_refs(index, ref, opts) do
      refs = maybe_include_target(refs, ref, Keyword.get(opts, :include_target, false))

      {:ok,
       refs
       |> assets_for_refs(index)
       |> filter_assets(opts)
       |> sort_assets(index)}
    end
  end

  @doc """
  Return a filtered subgraph rooted at a specific asset reference.

  The returned value is another `%Flux.GraphIndex{}` limited to the selected
  refs so callers can reuse the same traversal and lookup helpers against a
  target-specific graph view.
  """
  @spec subgraph(Ref.t(), filter_opts()) :: {:ok, t()} | {:error, error() | :asset_not_found}
  def subgraph(ref, opts \\ []) when is_list(opts) do
    with {:ok, index} <- get(),
         {:ok, refs} <- selected_refs(index, ref, Keyword.put_new(opts, :transitive, true)) do
      refs = maybe_include_target(refs, ref, Keyword.get(opts, :include_target, true))

      include_target? = Keyword.get(opts, :include_target, true)

      refs
      |> assets_for_refs(index)
      |> filter_assets(opts)
      |> ensure_target_present(ref, include_target?, index)
      |> project_assets()
      |> build_index()
    end
  end

  @doc """
  Return the cached global topological order.
  """
  @spec topological_order() :: {:ok, [Ref.t()]} | {:error, error()}
  def topological_order do
    with {:ok, index} <- get() do
      {:ok, index.topo_order}
    end
  end

  @doc """
  Build a graph index from canonical asset metadata.
  """
  @spec build_index([Asset.t()]) :: {:ok, t()} | {:error, error()}
  def build_index(assets) when is_list(assets) do
    with {:ok, assets_by_ref} <- build_assets_by_ref(assets),
         {:ok, upstream, downstream} <- build_adjacency(assets, assets_by_ref),
         {:ok, topo_order} <- topological_sort(upstream, downstream),
         transitive_upstream <- build_transitive_index(upstream),
         transitive_downstream <- build_transitive_index(downstream) do
      {:ok,
       %__MODULE__{
         assets_by_ref: assets_by_ref,
         upstream: upstream,
         downstream: downstream,
         transitive_upstream: transitive_upstream,
         transitive_downstream: transitive_downstream,
         topo_order: topo_order,
         topo_rank: build_topo_rank(topo_order)
       }}
    end
  end

  defp selected_refs(index, ref, opts) do
    with :ok <- validate_opts(opts) do
      transitive? = Keyword.get(opts, :transitive, true)

      case {Keyword.get(opts, :direction, :upstream), transitive?} do
        {:upstream, false} ->
          fetch_set(index.upstream, ref)

        {:downstream, false} ->
          fetch_set(index.downstream, ref)

        {:upstream, true} ->
          fetch_set(index.transitive_upstream, ref)

        {:downstream, true} ->
          fetch_set(index.transitive_downstream, ref)

        {:both, false} ->
          with {:ok, upstream} <- fetch_set(index.upstream, ref),
               {:ok, downstream} <- fetch_set(index.downstream, ref) do
            {:ok, MapSet.union(upstream, downstream)}
          end

        {:both, true} ->
          with {:ok, upstream} <- fetch_set(index.transitive_upstream, ref),
               {:ok, downstream} <- fetch_set(index.transitive_downstream, ref) do
            {:ok, MapSet.union(upstream, downstream)}
          end
      end
    end
  end

  defp validate_opts(opts) do
    direction = Keyword.get(opts, :direction, :upstream)

    with :ok <- validate_inclusion(direction, [:upstream, :downstream, :both]),
         :ok <- validate_boolean_opt(opts, :transitive),
         :ok <- validate_boolean_opt(opts, :include_target),
         :ok <- validate_list_opt(opts, :tags),
         :ok <- validate_list_opt(opts, :kinds),
         :ok <- validate_list_opt(opts, :modules),
         :ok <- validate_list_opt(opts, :names) do
      :ok
    end
  end

  defp validate_inclusion(value, allowed) do
    if value in allowed, do: :ok, else: {:error, :invalid_opts}
  end

  defp validate_boolean_opt(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_boolean(value) -> :ok
      {:ok, _invalid} -> {:error, :invalid_opts}
    end
  end

  defp validate_list_opt(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_list(value) -> :ok
      {:ok, _invalid} -> {:error, :invalid_opts}
    end
  end

  defp maybe_include_target(refs, ref, true), do: MapSet.put(refs, ref)
  defp maybe_include_target(refs, _ref, false), do: refs

  defp assets_for_refs(refs, index) do
    refs
    |> Enum.map(&Map.fetch!(index.assets_by_ref, &1))
  end

  defp filter_assets(assets, opts) do
    tags = Keyword.get(opts, :tags)
    kinds = Keyword.get(opts, :kinds)
    modules = Keyword.get(opts, :modules)
    names = Keyword.get(opts, :names)

    Enum.filter(assets, fn asset ->
      matches_tags?(asset, tags) and
        matches_membership?(asset.kind, kinds) and
        matches_membership?(asset.module, modules) and
        matches_membership?(asset.name, names)
    end)
  end

  defp matches_tags?(_asset, nil), do: true
  defp matches_tags?(_asset, []), do: true

  defp matches_tags?(asset, tags) when is_list(tags) do
    asset.tags
    |> MapSet.new()
    |> MapSet.disjoint?(MapSet.new(tags))
    |> Kernel.not()
  end

  defp matches_membership?(_value, nil), do: true
  defp matches_membership?(_value, []), do: true
  defp matches_membership?(value, values) when is_list(values), do: value in values

  defp ensure_target_present(assets, ref, true, index) do
    if Enum.any?(assets, &(&1.ref == ref)) do
      assets
    else
      [Map.fetch!(index.assets_by_ref, ref) | assets]
    end
  end

  defp ensure_target_present(assets, _ref, false, _index), do: assets

  defp project_assets(assets) do
    refs = MapSet.new(Enum.map(assets, & &1.ref))

    Enum.map(assets, fn asset ->
      %{asset | depends_on: Enum.filter(asset.depends_on, &MapSet.member?(refs, &1))}
    end)
  end

  defp sort_assets(assets, index) do
    Enum.sort(assets, fn left, right ->
      left_rank = Map.fetch!(index.topo_rank, left.ref)
      right_rank = Map.fetch!(index.topo_rank, right.ref)

      cond do
        left_rank < right_rank -> true
        left_rank > right_rank -> false
        true -> compare_refs(left.ref, right.ref)
      end
    end)
  end

  defp load_and_fetch_index do
    with :ok <- load() do
      {:ok, :persistent_term.get(@index_key)}
    end
  end

  defp build_assets_by_ref(assets) do
    {:ok, Map.new(assets, &{&1.ref, &1})}
  end

  defp build_adjacency(assets, assets_by_ref) do
    empty_sets = Map.new(assets, &{&1.ref, MapSet.new()})

    Enum.reduce_while(assets, {:ok, empty_sets, empty_sets}, fn %Asset{} = asset,
                                                                {:ok, upstream, downstream} ->
      Enum.reduce_while(asset.depends_on, {:ok, upstream, downstream}, fn dependency,
                                                                          {:ok, current_upstream,
                                                                           current_downstream} ->
        if Map.has_key?(assets_by_ref, dependency) do
          {:cont,
           {:ok, Map.update!(current_upstream, asset.ref, &MapSet.put(&1, dependency)),
            Map.update!(current_downstream, dependency, &MapSet.put(&1, asset.ref))}}
        else
          {:halt, {:error, {:missing_dependency, asset.ref, dependency}}}
        end
      end)
      |> case do
        {:ok, _updated_upstream, _updated_downstream} = ok -> {:cont, ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp topological_sort(upstream, downstream) do
    indegree = Map.new(upstream, fn {ref, dependencies} -> {ref, MapSet.size(dependencies)} end)

    queue =
      indegree
      |> Enum.filter(fn {_ref, count} -> count == 0 end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort(&compare_refs/2)

    consume_topology(queue, indegree, downstream, [])
    |> case do
      {:ok, order} when length(order) == map_size(upstream) -> {:ok, order}
      {:ok, _partial_order} -> {:error, {:cycle, cycle_path(upstream)}}
    end
  end

  defp consume_topology([], indegree, _downstream, order) do
    remaining = Enum.any?(indegree, fn {_ref, count} -> count > 0 end)

    if remaining do
      {:ok, order}
    else
      {:ok, Enum.reverse(order)}
    end
  end

  defp consume_topology([ref | rest], indegree, downstream, order) do
    {updated_indegree, newly_ready} =
      downstream
      |> Map.fetch!(ref)
      |> Enum.reduce({indegree, []}, fn dependent, {current_indegree, ready} ->
        next_count = Map.fetch!(current_indegree, dependent) - 1
        next_indegree = Map.put(current_indegree, dependent, next_count)

        if next_count == 0 do
          {next_indegree, [dependent | ready]}
        else
          {next_indegree, ready}
        end
      end)

    next_queue = Enum.sort(rest ++ newly_ready, &compare_refs/2)
    consume_topology(next_queue, updated_indegree, downstream, [ref | order])
  end

  defp build_transitive_index(adjacency) do
    Map.new(adjacency, fn {ref, _neighbors} ->
      {ref, reachable_from(ref, adjacency, MapSet.new())}
    end)
  end

  defp reachable_from(ref, adjacency, visited) do
    adjacency
    |> Map.fetch!(ref)
    |> Enum.reduce(visited, fn neighbor, acc ->
      if MapSet.member?(acc, neighbor) do
        acc
      else
        acc = MapSet.put(acc, neighbor)
        reachable_from(neighbor, adjacency, acc)
      end
    end)
  end

  defp cycle_path(upstream) do
    refs = Map.keys(upstream) |> Enum.sort(&compare_refs/2)

    Enum.reduce_while(refs, MapSet.new(), fn ref, globally_visited ->
      if MapSet.member?(globally_visited, ref) do
        {:cont, globally_visited}
      else
        case dfs_cycle(ref, upstream, globally_visited, [], MapSet.new()) do
          {:ok, cycle} -> {:halt, cycle}
          {:error, visited} -> {:cont, visited}
        end
      end
    end)
    |> case do
      %MapSet{} -> []
      cycle -> cycle
    end
  end

  defp dfs_cycle(ref, upstream, globally_visited, path, stack) do
    next_path = [ref | path]
    next_stack = MapSet.put(stack, ref)
    next_visited = MapSet.put(globally_visited, ref)

    Enum.reduce_while(Map.fetch!(upstream, ref), {:error, next_visited}, fn dependency,
                                                                            {:error,
                                                                             current_visited} ->
      cond do
        MapSet.member?(next_stack, dependency) ->
          {:halt, {:ok, extract_cycle([dependency | next_path], dependency)}}

        MapSet.member?(current_visited, dependency) ->
          {:cont, {:error, current_visited}}

        true ->
          case dfs_cycle(dependency, upstream, current_visited, next_path, next_stack) do
            {:ok, cycle} -> {:halt, {:ok, cycle}}
            {:error, visited} -> {:cont, {:error, visited}}
          end
      end
    end)
  end

  defp extract_cycle(path, repeated_ref) do
    cycle =
      path
      |> Enum.reverse()
      |> Enum.drop_while(&(&1 != repeated_ref))

    case cycle do
      [] -> []
      [_single] -> [repeated_ref, repeated_ref]
      _many -> cycle
    end
  end

  defp build_topo_rank(order) do
    order
    |> Enum.with_index()
    |> Map.new(fn {ref, index} -> {ref, index} end)
  end

  defp fetch_set(index, ref) do
    case Map.fetch(index, ref) do
      {:ok, refs} -> {:ok, refs}
      :error -> {:error, :asset_not_found}
    end
  end

  defp sort_refs(refs, index) do
    Enum.sort(refs, fn left, right ->
      left_rank = Map.fetch!(index.topo_rank, left)
      right_rank = Map.fetch!(index.topo_rank, right)

      cond do
        left_rank < right_rank -> true
        left_rank > right_rank -> false
        true -> compare_refs(left, right)
      end
    end)
  end

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    case compare_terms(left_module, right_module) do
      :lt -> true
      :gt -> false
      :eq -> compare_terms(left_name, right_name) != :gt
    end
  end

  defp compare_terms(left, right) do
    cond do
      left < right -> :lt
      left > right -> :gt
      true -> :eq
    end
  end
end
