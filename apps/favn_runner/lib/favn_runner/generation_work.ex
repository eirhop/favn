defmodule FavnRunner.GenerationWork do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias FavnRunner.ManifestHandle
  alias FavnRunner.ManifestStore

  @spec validate(RunnerWork.t(), Asset.t(), ManifestHandle.t(), GenServer.server()) ::
          :ok | {:error, term()}
  def validate(
        %RunnerWork{} = work,
        %Asset{} = asset,
        %ManifestHandle{} = manifest,
        manifest_store
      ) do
    with :ok <- validate_target(work, asset),
         :ok <- validate_upstream_pins(work, manifest, manifest_store) do
      :ok
    end
  end

  @spec apply_overrides(Asset.t(), map(), RunnerWork.t()) :: {Asset.t(), map()}
  def apply_overrides(%Asset{} = asset, relation_by_module, %RunnerWork{} = work)
      when is_map(relation_by_module) do
    asset =
      case work.write_relation do
        %RelationRef{} = relation -> %{asset | relation: relation}
        nil -> asset
      end

    relations =
      Enum.reduce(work.upstream_generation_pins, relation_by_module, fn pin, relations ->
        {module, _name} = pin.asset_ref
        Map.put(relations, module, pin.relation)
      end)

    {asset, relations}
  end

  defp validate_target(%RunnerWork{target_operation: nil} = work, %Asset{
         target_descriptor: nil
       }),
       do: RunnerWork.validate_generation_contract(work)

  defp validate_target(%RunnerWork{} = work, %Asset{
         target_descriptor: %TargetDescriptor{} = descriptor
       }),
       do: RunnerWork.validate_target_identity(work, descriptor)

  defp validate_target(%RunnerWork{}, %Asset{}), do: {:error, :runner_target_identity_mismatch}

  defp validate_upstream_pins(%RunnerWork{upstream_generation_pins: []}, _manifest, _store),
    do: :ok

  defp validate_upstream_pins(%RunnerWork{} = work, manifest, manifest_store) do
    refs = Enum.map(work.upstream_generation_pins, & &1.asset_ref)

    with {:ok, assets} <- ManifestStore.fetch_assets(manifest, refs, server: manifest_store),
         assets_by_ref <- Map.new(assets, &{&1.ref, &1}),
         true <- map_size(assets_by_ref) == length(refs) or {:error, :upstream_asset_not_found} do
      Enum.reduce_while(work.upstream_generation_pins, :ok, fn pin, :ok ->
        asset = Map.fetch!(assets_by_ref, pin.asset_ref)

        case pin_identity(pin, asset) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp pin_identity(pin, %Asset{target_descriptor: %TargetDescriptor{} = descriptor}),
    do: Favn.Contracts.TargetGenerationPin.validate_target_identity(pin, descriptor)

  defp pin_identity(_pin, %Asset{}), do: {:error, :upstream_target_identity_mismatch}
end
