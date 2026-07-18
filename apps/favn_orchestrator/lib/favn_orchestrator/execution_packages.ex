defmodule FavnOrchestrator.ExecutionPackages do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Error
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
    Persistence.stores().registry.missing_execution_package_hashes(%MissingExecutionPackageHashes{
      platform_context: context,
      hashes: hashes
    })
  end

  @doc "Fetches, verifies, and attaches the package required by one runtime work item."
  @spec attach(WorkspaceContext.t(), String.t(), RunnerWork.t(), Version.t(), Index.t()) ::
          {:ok, RunnerWork.t()} | {:error, term()}
  def attach(
        %WorkspaceContext{} = context,
        deployment_id,
        %RunnerWork{} = work,
        %Version{} = version,
        %Index{} = index
      ) do
    with :ok <- validate_workspace_read(context),
         {:ok, asset} <- Index.fetch_asset(index, RunnerWork.asset_ref(work)),
         {:ok, package} <- fetch_for_asset(context, deployment_id, version, asset),
         {:ok, canonical} <- ExecutionPackage.verify_for_asset(package, asset) do
      {:ok, %{work | execution_package: canonical}}
    end
  end

  defp fetch_for_asset(
         context,
         deployment_id,
         version,
         %Asset{ref: asset_ref, type: :sql, execution_package_hash: hash}
       )
       when is_binary(deployment_id) and is_binary(hash) do
    Persistence.stores().registry.get_execution_package(%GetExecutionPackage{
      workspace_context: context,
      deployment_id: deployment_id,
      manifest_version_id: version.manifest_version_id,
      asset_ref: asset_ref,
      content_hash: hash
    })
  end

  defp fetch_for_asset(_context, deployment_id, _version, %Asset{type: :sql})
       when is_binary(deployment_id),
       do: {:error, :execution_package_required}

  defp fetch_for_asset(_context, _deployment_id, _version, %Asset{type: :sql}),
    do: {:error, :execution_package_deployment_required}

  defp fetch_for_asset(_context, _deployment_id, _version, %Asset{}), do: {:ok, nil}

  defp validate_workspace_read(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
         ),
       do: :ok,
       else: {:error, Error.new(:forbidden, "workspace read role required")}
  end
end
