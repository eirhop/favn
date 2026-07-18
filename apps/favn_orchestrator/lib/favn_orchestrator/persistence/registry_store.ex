defmodule FavnOrchestrator.Persistence.RegistryStore do
  @moduledoc "Persistence contract for global manifests and immutable workspace deployments."

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RuntimeState

  @callback provision_workspace(ProvisionWorkspace.t()) :: :ok | {:error, Error.t()}
  @callback register_manifest(RegisterManifest.t()) :: {:ok, Version.t()} | {:error, Error.t()}
  @callback register_execution_packages(RegisterExecutionPackages.t()) ::
              :ok | {:error, Error.t()}
  @callback missing_execution_package_hashes(MissingExecutionPackageHashes.t()) ::
              {:ok, [String.t()]} | {:error, Error.t()}
  @callback get_execution_package(GetExecutionPackage.t()) ::
              {:ok, Favn.Manifest.ExecutionPackage.t()} | {:error, Error.t()}
  @callback get_manifest(ManifestSelector.t()) :: {:ok, Version.t()} | {:error, Error.t()}
  @callback get_deployment_manifest(GetDeploymentManifest.t()) ::
              {:ok, Version.t()} | {:error, Error.t()}
  @callback page_workspaces(PageWorkspaces.t()) ::
              {:ok, CursorPage.t(String.t())} | {:error, Error.t()}
  @callback deploy_manifest(DeployManifest.t()) :: {:ok, RuntimeState.t()} | {:error, Error.t()}
  @callback get_runtime_state(GetRuntimeState.t()) ::
              {:ok, RuntimeState.t()} | {:error, Error.t()}
  @callback get_deployment_targets(GetDeploymentTargets.t()) ::
              {:ok, [FavnOrchestrator.Persistence.Commands.DeploymentTarget.t()]}
              | {:error, Error.t()}
end
