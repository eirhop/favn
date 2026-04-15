defmodule Favn.Manifest.Index do
  @moduledoc """
  Deterministic lookup/index helpers built from a persisted manifest payload.
  """

  alias Favn.Assets.GraphIndex
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version

  @type ref :: {module(), atom()}

  @type t :: %__MODULE__{
          manifest: Manifest.t(),
          graph_index: GraphIndex.t(),
          assets_by_ref: %{required(ref()) => Asset.t()},
          pipelines_by_ref: %{required(ref()) => Pipeline.t()},
          schedules_by_ref: %{required(ref()) => Schedule.t()}
        }

  @type error ::
          :invalid_manifest
          | {:invalid_asset_ref, term()}
          | {:invalid_pipeline_ref, term()}
          | {:invalid_schedule_ref, term()}
          | {:duplicate_asset_ref, ref()}
          | {:duplicate_pipeline_ref, ref()}
          | {:duplicate_schedule_ref, ref()}
          | GraphIndex.error()

  defstruct [
    :manifest,
    :graph_index,
    assets_by_ref: %{},
    pipelines_by_ref: %{},
    schedules_by_ref: %{}
  ]

  @spec build(Manifest.t()) :: {:ok, t()} | {:error, error()}
  def build(%Manifest{} = manifest) do
    with {:ok, assets_by_ref} <- build_assets_by_ref(manifest.assets),
         {:ok, graph_index} <- GraphIndex.build_index(Map.values(assets_by_ref)),
         {:ok, pipelines_by_ref} <- build_pipelines_by_ref(manifest.pipelines),
         {:ok, schedules_by_ref} <- build_schedules_by_ref(manifest.schedules) do
      {:ok,
       %__MODULE__{
         manifest: manifest,
         graph_index: graph_index,
         assets_by_ref: assets_by_ref,
         pipelines_by_ref: pipelines_by_ref,
         schedules_by_ref: schedules_by_ref
       }}
    end
  end

  def build(_other), do: {:error, :invalid_manifest}

  @spec build_from_version(Version.t()) :: {:ok, t()} | {:error, error()}
  def build_from_version(%Version{manifest: %Manifest{} = manifest}), do: build(manifest)
  def build_from_version(%Version{}), do: {:error, :invalid_manifest}

  @spec fetch_asset(t(), ref()) :: {:ok, Asset.t()} | {:error, :asset_not_found}
  def fetch_asset(%__MODULE__{} = index, ref) when is_tuple(ref) do
    case Map.fetch(index.assets_by_ref, ref) do
      {:ok, %Asset{} = asset} -> {:ok, asset}
      :error -> {:error, :asset_not_found}
    end
  end

  @spec fetch_pipeline(t(), ref()) :: {:ok, Pipeline.t()} | {:error, :pipeline_not_found}
  def fetch_pipeline(%__MODULE__{} = index, ref) when is_tuple(ref) do
    case Map.fetch(index.pipelines_by_ref, ref) do
      {:ok, %Pipeline{} = pipeline} -> {:ok, pipeline}
      :error -> {:error, :pipeline_not_found}
    end
  end

  @spec fetch_schedule(t(), ref()) :: {:ok, Schedule.t()} | {:error, :schedule_not_found}
  def fetch_schedule(%__MODULE__{} = index, ref) when is_tuple(ref) do
    case Map.fetch(index.schedules_by_ref, ref) do
      {:ok, %Schedule{} = schedule} -> {:ok, schedule}
      :error -> {:error, :schedule_not_found}
    end
  end

  @spec list_assets(t()) :: [Asset.t()]
  def list_assets(%__MODULE__{} = index) do
    index.assets_by_ref
    |> Map.values()
    |> Enum.sort_by(& &1.ref)
  end

  @spec list_pipelines(t()) :: [Pipeline.t()]
  def list_pipelines(%__MODULE__{} = index) do
    index.pipelines_by_ref
    |> Map.values()
    |> Enum.sort_by(&{&1.module, &1.name})
  end

  @spec list_schedules(t()) :: [Schedule.t()]
  def list_schedules(%__MODULE__{} = index) do
    index.schedules_by_ref
    |> Map.values()
    |> Enum.sort_by(&{&1.module, &1.name})
  end

  defp build_assets_by_ref(assets) when is_list(assets) do
    Enum.reduce_while(assets, {:ok, %{}}, fn
      %Asset{ref: {module, name}} = asset, {:ok, acc}
      when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name) ->
        put_unique(acc, asset.ref, asset, :duplicate_asset_ref)

      %Asset{ref: ref}, _acc ->
        {:halt, {:error, {:invalid_asset_ref, ref}}}

      other, _acc ->
        {:halt, {:error, {:invalid_asset_ref, other}}}
    end)
  end

  defp build_assets_by_ref(_other), do: {:error, {:invalid_asset_ref, :invalid_assets}}

  defp build_pipelines_by_ref(pipelines) when is_list(pipelines) do
    Enum.reduce_while(pipelines, {:ok, %{}}, fn
      %Pipeline{module: module, name: name} = pipeline, {:ok, acc}
      when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name) ->
        put_unique(acc, {module, name}, pipeline, :duplicate_pipeline_ref)

      %Pipeline{} = pipeline, _acc ->
        {:halt, {:error, {:invalid_pipeline_ref, {pipeline.module, pipeline.name}}}}

      other, _acc ->
        {:halt, {:error, {:invalid_pipeline_ref, other}}}
    end)
  end

  defp build_pipelines_by_ref(_other), do: {:error, {:invalid_pipeline_ref, :invalid_pipelines}}

  defp build_schedules_by_ref(schedules) when is_list(schedules) do
    Enum.reduce_while(schedules, {:ok, %{}}, fn
      %Schedule{} = schedule, {:ok, acc} ->
        case schedule_ref(schedule) do
          {:ok, ref} ->
            put_unique(acc, ref, schedule, :duplicate_schedule_ref)

          {:error, _reason} = error ->
            {:halt, error}
        end

      other, _acc ->
        {:halt, {:error, {:invalid_schedule_ref, other}}}
    end)
  end

  defp build_schedules_by_ref(_other), do: {:error, {:invalid_schedule_ref, :invalid_schedules}}

  defp schedule_ref(%Schedule{ref: {module, name}})
       when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name),
       do: {:ok, {module, name}}

  defp schedule_ref(%Schedule{module: module, name: name})
       when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name),
       do: {:ok, {module, name}}

  defp schedule_ref(%Schedule{ref: ref}), do: {:error, {:invalid_schedule_ref, ref}}

  defp put_unique(acc, key, value, duplicate_tag) do
    if Map.has_key?(acc, key) do
      {:halt, {:error, {duplicate_tag, key}}}
    else
      {:cont, {:ok, Map.put(acc, key, value)}}
    end
  end
end
