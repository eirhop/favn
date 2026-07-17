defmodule FavnOrchestrator.ExecutionPackages do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage

  @spec attach(RunnerWork.t(), Version.t()) ::
          {:ok, RunnerWork.t()} | {:error, term()}
  def attach(%RunnerWork{} = work, %Version{} = version) do
    with {:ok, asset} <- find_asset(version, RunnerWork.asset_ref(work)),
         {:ok, package} <- fetch_for_asset(asset),
         {:ok, canonical} <- ExecutionPackage.verify_for_asset(package, asset) do
      {:ok, %{work | execution_package: canonical}}
    end
  end

  defp find_asset(%Version{manifest: %{assets: assets}}, asset_ref) do
    case Enum.find(assets, &match?(%Asset{ref: ^asset_ref}, &1)) do
      %Asset{} = asset -> {:ok, asset}
      nil -> {:error, :asset_not_found}
    end
  end

  defp fetch_for_asset(%Asset{type: :sql, execution_package_hash: hash}) when is_binary(hash),
    do: Storage.get_execution_package(hash)

  defp fetch_for_asset(%Asset{type: :sql}), do: {:error, :execution_package_required}
  defp fetch_for_asset(%Asset{}), do: {:ok, nil}
end
