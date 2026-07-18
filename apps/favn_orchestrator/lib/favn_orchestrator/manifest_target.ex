defmodule FavnOrchestrator.ManifestTarget do
  @moduledoc """
  Identifies and resolves manifest-owned operator targets.

  Target ids are stable control-plane identifiers derived from the manifest
  identity. This module owns their encoding so command and catalogue paths do
  not duplicate string formats or lookup behavior.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.TargetIdentity

  @type id :: String.t()

  @doc "Returns the control-plane id for an asset reference."
  @spec asset_id(Favn.Ref.t()) :: id()
  def asset_id({module, name} = ref) when is_atom(module) and is_atom(name),
    do: TargetIdentity.for_asset(ref)

  @doc "Returns the control-plane id for one named manifest pipeline."
  @spec pipeline_id({module(), atom()}) :: id()
  def pipeline_id({module, name} = ref) when is_atom(module) and is_atom(name),
    do: TargetIdentity.for_pipeline(ref)

  @doc "Returns the control-plane id for one named manifest pipeline."
  @spec pipeline_id(module(), atom()) :: id()
  def pipeline_id(module, name) when is_atom(module) and is_atom(name),
    do: pipeline_id({module, name})

  @doc "Resolves an asset target id within one manifest version."
  @spec resolve_asset(Version.t(), id()) :: {:ok, map()} | {:error, :invalid_asset_target}
  def resolve_asset(%Version{} = version, target_id) when is_binary(target_id) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.find(&(asset_id(&1.ref) == target_id))
    |> case do
      %{ref: _target_ref} = asset -> {:ok, asset}
      _other -> {:error, :invalid_asset_target}
    end
  end

  @doc "Resolves an asset reference within one manifest version."
  @spec resolve_asset_ref(Version.t(), id()) :: {:ok, Favn.Ref.t()} | {:error, term()}
  def resolve_asset_ref(%Version{} = version, target_id) when is_binary(target_id) do
    with {:ok, asset} <- resolve_asset(version, target_id), do: {:ok, asset.ref}
  end

  @doc "Resolves a pipeline module within one manifest version."
  @spec resolve_pipeline_module(Version.t(), id()) ::
          {:ok, module()} | {:error, :invalid_pipeline_target}
  def resolve_pipeline_module(%Version{} = version, target_id) when is_binary(target_id) do
    with {:ok, pipeline} <- resolve_pipeline(version, target_id), do: {:ok, pipeline.module}
  end

  @doc "Resolves one exact named pipeline within one manifest version."
  @spec resolve_pipeline(Version.t(), id()) :: {:ok, map()} | {:error, :invalid_pipeline_target}
  def resolve_pipeline(%Version{} = version, target_id) when is_binary(target_id) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(&(pipeline_id({&1.module, &1.name}) == target_id))
    |> case do
      %{module: module, name: name} = pipeline when is_atom(module) and is_atom(name) ->
        {:ok, pipeline}

      _other ->
        {:error, :invalid_pipeline_target}
    end
  end
end
