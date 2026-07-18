defmodule FavnOrchestrator.Persistence.DeploymentPlanner do
  @moduledoc """
  Resolves a workspace's selected manifest targets into one exact deployment catalog.

  Common and customer-specific selections are explicit inputs owned by the
  deployment use case. Pipeline members and transitive asset dependencies are
  added as executable dependency targets, but remain hidden unless the asset was
  selected explicitly. The result is deterministic and contains no duplicate
  target identity.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.TargetIdentity

  @enforce_keys [:common_assets, :common_pipelines, :workspace_assets, :workspace_pipelines]
  defstruct common_assets: [],
            common_pipelines: [],
            workspace_assets: [],
            workspace_pipelines: []

  @type pipeline_ref :: {module(), atom()}
  @type t :: %__MODULE__{
          common_assets: [Favn.Ref.t()],
          common_pipelines: [pipeline_ref()],
          workspace_assets: [Favn.Ref.t()],
          workspace_pipelines: [pipeline_ref()]
        }

  @doc "Builds the exact, dependency-closed target catalog for one deployment."
  @spec plan(Version.t(), t()) :: {:ok, [DeploymentTarget.t()]} | {:error, term()}
  def plan(%Version{} = version, %__MODULE__{} = selection) do
    with :ok <- validate_selection(selection),
         {:ok, index} <- Index.build_from_version(version),
         :ok <- validate_disjoint(selection.common_assets, selection.workspace_assets, :asset),
         :ok <-
           validate_disjoint(
             selection.common_pipelines,
             selection.workspace_pipelines,
             :pipeline
           ),
         {:ok, selected_assets} <- selected_assets(index, selection),
         {:ok, selected_pipelines, pipeline_assets} <- selected_pipelines(index, selection),
         {:ok, dependency_assets} <-
           dependency_closure(index, Map.keys(selected_assets) ++ pipeline_assets) do
      targets =
        selected_assets
        |> Map.merge(dependency_targets(dependency_assets), fn _ref, selected, _dependency ->
          selected
        end)
        |> Map.values()
        |> Kernel.++(Map.values(selected_pipelines))
        |> Enum.sort_by(&{&1.target_kind, &1.target_id})

      {:ok, targets}
    end
  end

  def plan(_version, _selection), do: {:error, :invalid_deployment_selection}

  defp selected_assets(index, selection) do
    [
      {selection.common_assets, :common},
      {selection.workspace_assets, :explicit}
    ]
    |> Enum.reduce_while({:ok, %{}}, fn {refs, source}, {:ok, targets} ->
      case add_selected_assets(index, refs, source, targets) do
        {:ok, targets} -> {:cont, {:ok, targets}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp add_selected_assets(index, refs, source, targets) do
    Enum.reduce_while(refs, {:ok, targets}, fn ref, {:ok, acc} ->
      case Index.fetch_asset(index, ref) do
        {:ok, asset} ->
          target = %DeploymentTarget{
            target_kind: :asset,
            target_id: TargetIdentity.for_asset(asset.ref),
            selection_source: source,
            customer_visible: true
          }

          {:cont, {:ok, Map.put(acc, asset.ref, target)}}

        {:error, _reason} ->
          {:halt, {:error, {:asset_not_found, ref}}}
      end
    end)
  end

  defp selected_pipelines(index, selection) do
    [
      {selection.common_pipelines, :common},
      {selection.workspace_pipelines, :explicit}
    ]
    |> Enum.reduce_while({:ok, %{}, []}, fn {refs, source}, {:ok, targets, assets} ->
      case add_selected_pipelines(index, refs, source, targets, assets) do
        {:ok, next_targets, next_assets} ->
          {:cont, {:ok, next_targets, next_assets}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, targets, assets} -> {:ok, targets, Enum.uniq(assets)}
      error -> error
    end)
  end

  defp add_selected_pipelines(index, refs, source, targets, assets) do
    Enum.reduce_while(refs, {:ok, targets, assets}, fn ref, {:ok, acc, asset_acc} ->
      with {:ok, %Pipeline{} = pipeline} <- Index.fetch_pipeline(index, ref),
           {:ok, resolution} <- PipelineResolver.resolve(index, pipeline, []) do
        target = %DeploymentTarget{
          target_kind: :pipeline,
          target_id: TargetIdentity.for_pipeline(ref),
          selection_source: source,
          customer_visible: true
        }

        {:cont, {:ok, Map.put(acc, ref, target), resolution.target_refs ++ asset_acc}}
      else
        {:error, :pipeline_not_found} ->
          {:halt, {:error, {:pipeline_not_found, ref}}}

        {:error, reason} ->
          {:halt, {:error, {:pipeline_invalid, ref, reason}}}
      end
    end)
  end

  defp dependency_closure(index, roots) do
    roots
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, MapSet.new()}, fn ref, {:ok, visited} ->
      case collect_asset_dependencies(index, ref, visited) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, visited} -> {:ok, MapSet.to_list(visited)}
      error -> error
    end)
  end

  defp collect_asset_dependencies(index, ref, visited) do
    if MapSet.member?(visited, ref) do
      {:ok, visited}
    else
      case Index.fetch_asset(index, ref) do
        {:ok, asset} ->
          Enum.reduce_while(asset.depends_on, {:ok, MapSet.put(visited, ref)}, fn dependency,
                                                                                  {:ok, acc} ->
            case collect_asset_dependencies(index, dependency, acc) do
              {:ok, next} -> {:cont, {:ok, next}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)

        {:error, _reason} ->
          {:error, {:asset_not_found, ref}}
      end
    end
  end

  defp dependency_targets(refs) do
    Map.new(refs, fn ref ->
      {ref,
       %DeploymentTarget{
         target_kind: :asset,
         target_id: TargetIdentity.for_asset(ref),
         selection_source: :dependency,
         customer_visible: false
       }}
    end)
  end

  defp validate_selection(%__MODULE__{} = selection) do
    fields = [
      selection.common_assets,
      selection.common_pipelines,
      selection.workspace_assets,
      selection.workspace_pipelines
    ]

    if Enum.all?(fields, &is_list/1) and
         valid_refs?(selection.common_assets) and
         valid_refs?(selection.workspace_assets) and
         valid_refs?(selection.common_pipelines) and
         valid_refs?(selection.workspace_pipelines) do
      :ok
    else
      {:error, :invalid_deployment_selection}
    end
  end

  defp validate_disjoint(common, workspace, kind) do
    overlap = MapSet.intersection(MapSet.new(common), MapSet.new(workspace))

    if MapSet.size(overlap) == 0,
      do: :ok,
      else: {:error, {:ambiguous_target_selection, kind, MapSet.to_list(overlap)}}
  end

  defp valid_refs?(refs) do
    Enum.all?(refs, fn
      {module, name} when is_atom(module) and is_atom(name) -> true
      _invalid -> false
    end) and length(refs) == length(Enum.uniq(refs))
  end
end
