defmodule FavnOrchestrator.ExecutionPackages do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @doc "Registers immutable packages before their compact manifest index."
  @spec register(PlatformContext.t(), [ExecutionPackage.t()]) :: :ok | {:error, term()}
  def register(%PlatformContext{} = context, packages) when is_list(packages) do
    Persistence.stores().registry.register_execution_packages(%RegisterExecutionPackages{
      platform_context: context,
      packages: packages
    })
  end

  @doc "Returns the requested package hashes that are not registered."
  @spec missing_hashes(PlatformContext.t(), [String.t()]) ::
          {:ok, [String.t()]} | {:error, term()}
  def missing_hashes(%PlatformContext{} = context, hashes) when is_list(hashes) do
    Persistence.stores().registry.missing_execution_package_hashes(
      %MissingExecutionPackageHashes{platform_context: context, hashes: hashes}
    )
  end

  @doc "Fetches, verifies, and attaches the package required by one runtime work item."
  @spec attach(WorkspaceContext.t(), RunnerWork.t(), Version.t()) ::
          {:ok, RunnerWork.t()} | {:error, term()}
  def attach(%WorkspaceContext{} = context, %RunnerWork{} = work, %Version{} = version) do
    with {:ok, asset} <- find_asset(version, RunnerWork.asset_ref(work)),
         {:ok, package} <- fetch_for_asset(context, asset),
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

  defp fetch_for_asset(context, %Asset{type: :sql, execution_package_hash: hash})
       when is_binary(hash) do
    Persistence.stores().registry.get_execution_package(%GetExecutionPackage{
      workspace_context: context,
      content_hash: hash
    })
  end

  defp fetch_for_asset(_context, %Asset{type: :sql}),
    do: {:error, :execution_package_required}

  defp fetch_for_asset(_context, %Asset{}), do: {:ok, nil}
end
