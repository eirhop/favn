defmodule FavnRunner.ManifestResolver do
  @moduledoc """
  Helpers for resolving runner work targets from pinned manifests.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version

  @type resolve_target_error ::
          :missing_asset_target | :invalid_asset_target | :multiple_asset_targets

  @spec resolve_target_ref(RunnerWork.t()) ::
          {:ok, Favn.Ref.t()} | {:error, resolve_target_error()}
  def resolve_target_ref(%RunnerWork{} = work) do
    with {:ok, refs} <- normalize_refs(work.asset_ref, work.asset_refs) do
      case refs do
        [] -> {:error, :missing_asset_target}
        [ref] -> {:ok, ref}
        _many -> {:error, :multiple_asset_targets}
      end
    end
  end

  def resolve_target_ref(_invalid), do: {:error, :invalid_asset_target}

  @spec resolve_asset(Version.t(), Favn.Ref.t()) :: {:ok, Asset.t()} | {:error, :asset_not_found}
  def resolve_asset(%Version{} = version, asset_ref) when is_tuple(asset_ref) do
    version.manifest.assets
    |> Enum.find(fn
      %Asset{ref: ^asset_ref} -> true
      _other -> false
    end)
    |> case do
      %Asset{} = asset -> {:ok, asset}
      nil -> {:error, :asset_not_found}
    end
  end

  def resolve_asset(%Version{}, _asset_ref), do: {:error, :asset_not_found}

  defp normalize_refs(asset_ref, asset_refs) when is_list(asset_refs) do
    candidate_refs =
      Enum.reduce_while(asset_refs, {:ok, []}, fn ref, {:ok, acc} ->
        if valid_ref?(ref) do
          {:cont, {:ok, [ref | acc]}}
        else
          {:halt, {:error, :invalid_asset_target}}
        end
      end)

    with {:ok, candidate_refs} <- candidate_refs,
         {:ok, refs} <- include_asset_ref(asset_ref, candidate_refs) do
      {:ok, Enum.uniq(refs)}
    end
  end

  defp normalize_refs(asset_ref, _asset_refs) do
    include_asset_ref(asset_ref, [])
  end

  defp include_asset_ref(nil, refs), do: {:ok, refs}

  defp include_asset_ref(asset_ref, refs) do
    if valid_ref?(asset_ref), do: {:ok, [asset_ref | refs]}, else: {:error, :invalid_asset_target}
  end

  defp valid_ref?({module, name}) when is_atom(module) and is_atom(name), do: true
  defp valid_ref?(_other), do: false
end
