defmodule FavnOrchestrator.ManifestTarget do
  @moduledoc """
  Identifies and resolves manifest-owned operator targets.

  Target ids are stable control-plane identifiers derived from the manifest
  identity. This module owns their encoding so command and catalogue paths do
  not duplicate string formats or lookup behavior.
  """

  alias Favn.Manifest.Version

  @type id :: String.t()

  @doc "Returns the control-plane id for an asset reference."
  @spec asset_id(Favn.Ref.t()) :: id()
  def asset_id({module, name}) when is_atom(module) and is_atom(name) do
    "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  @doc "Returns the control-plane id for a pipeline module."
  @spec pipeline_id(module()) :: id()
  def pipeline_id(module) when is_atom(module), do: "pipeline:" <> Atom.to_string(module)

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
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.find(&(pipeline_id(&1.module) == target_id))
    |> case do
      %{module: target_module} -> {:ok, target_module}
      _other -> {:error, :invalid_pipeline_target}
    end
  end
end
