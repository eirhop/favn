defmodule FavnOrchestrator.Persistence.RegistryStore do
  @moduledoc "Persistence contract for global manifests and immutable workspace deployments."

  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.AbandonManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.BeginManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.HeartbeatManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.RenewManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetActiveDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.GetManifestTargetDescriptors
  alias FavnOrchestrator.Persistence.Queries.GetManifestDeployment
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Results.ManifestDeployment

  @callback provision_workspace(ProvisionWorkspace.t()) :: :ok | {:error, Error.t()}
  @callback register_manifest(RegisterManifest.t()) :: {:ok, Version.t()} | {:error, Error.t()}
  @callback register_execution_packages(RegisterExecutionPackages.t()) ::
              :ok | {:error, Error.t()}
  @callback missing_execution_package_hashes(MissingExecutionPackageHashes.t()) ::
              {:ok, [String.t()]} | {:error, Error.t()}
  @callback get_execution_package(GetExecutionPackage.t()) ::
              {:ok, Favn.Manifest.ExecutionPackage.t()} | {:error, Error.t()}
  @callback get_manifest(ManifestSelector.t()) :: {:ok, Version.t()} | {:error, Error.t()}
  @callback get_manifest_size(ManifestSelector.t()) ::
              {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback get_manifest_target_descriptors(GetManifestTargetDescriptors.t()) ::
              {:ok, [TargetDescriptor.t()]} | {:error, Error.t()}
  @callback get_deployment_manifest(GetDeploymentManifest.t()) ::
              {:ok, Version.t()} | {:error, Error.t()}
  @callback get_deployment_configuration(GetDeploymentConfiguration.t()) ::
              {:ok, map()} | {:error, Error.t()}
  @callback get_active_deployment_configuration(GetActiveDeploymentConfiguration.t()) ::
              {:ok, {String.t(), map()}} | {:error, Error.t()}
  @callback page_workspaces(PageWorkspaces.t()) ::
              {:ok, CursorPage.t(String.t())} | {:error, Error.t()}
  @callback deploy_manifest(DeployManifest.t()) :: {:ok, RuntimeState.t()} | {:error, Error.t()}
  @callback begin_manifest_deployment(BeginManifestDeployment.t()) ::
              {:ok, {:new, CommandIdempotency.t()} | {:replay, RuntimeState.t()}}
              | {:error, Error.t()}
  @callback heartbeat_manifest_deployment(HeartbeatManifestDeployment.t()) ::
              :ok | {:error, Error.t()}
  @callback abandon_manifest_deployment(AbandonManifestDeployment.t()) ::
              :ok | {:error, Error.t()}
  @callback get_runtime_state(GetRuntimeState.t()) ::
              {:ok, RuntimeState.t()} | {:error, Error.t()}
  @callback get_deployment_targets(GetDeploymentTargets.t()) ::
              {:ok, [FavnOrchestrator.Persistence.Commands.DeploymentTarget.t()]}
              | {:error, Error.t()}
  @callback acquire_manifest_upload_lease(AcquireManifestUploadLease.t()) ::
              :ok | {:error, Error.t()}
  @callback renew_manifest_upload_lease(RenewManifestUploadLease.t()) ::
              :ok | {:error, Error.t()}
  @callback release_manifest_upload_lease(ReleaseManifestUploadLease.t()) ::
              :ok | {:error, Error.t()}
  @callback accept_manifest_deployment(AcceptManifestDeployment.t()) ::
              {:ok, :accepted | :replay, ManifestDeployment.t()} | {:error, Error.t()}
  @callback get_manifest_deployment(GetManifestDeployment.t()) ::
              {:ok, ManifestDeployment.t()} | {:error, Error.t()}
  @callback claim_manifest_deployment(ClaimManifestDeployment.t()) ::
              {:ok, ManifestDeployment.t() | nil} | {:error, Error.t()}
  @callback renew_manifest_deployment_claim(RenewManifestDeploymentClaim.t()) ::
              :ok | {:error, Error.t()}
  @callback update_manifest_deployment_progress(UpdateManifestDeploymentProgress.t()) ::
              :ok | {:error, Error.t()}
  @callback release_manifest_deployment_claim(ReleaseManifestDeploymentClaim.t()) ::
              :ok | {:error, Error.t()}
  @callback complete_manifest_deployment(CompleteManifestDeployment.t()) ::
              {:ok, ManifestDeployment.t()} | {:error, Error.t()}
  @callback acquire_manifest_activation_lease(AcquireManifestActivationLease.t()) ::
              {:ok, pos_integer()} | {:error, Error.t()}
  @callback renew_manifest_activation_lease(RenewManifestActivationLease.t()) ::
              :ok | {:error, Error.t()}
  @callback release_manifest_activation_lease(ReleaseManifestActivationLease.t()) ::
              :ok | {:error, Error.t()}
end
