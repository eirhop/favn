defmodule FavnOrchestrator.ManifestDeployments do
  @moduledoc """
  Durable first-party manifest archive deployment facade.

  HTTP callers receive only deployment-specific authority. This facade creates
  narrowly named internal system contexts for immutable package/manifest writes
  and activation work while preserving the caller identity on the operation.
  """

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.RenewManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment
  alias FavnOrchestrator.Persistence.Queries.GetManifestDeployment
  alias FavnOrchestrator.Persistence.Results.ManifestDeployment
  alias FavnOrchestrator.Persistence.SystemContext

  @upload_lease_seconds 60

  @doc "Returns replay, conflict, or new-upload status without reading a body."
  @spec preflight(ManifestDeploymentContext.t(), String.t(), String.t()) ::
          {:ok, :new | {:replay, ManifestDeployment.t()}} | {:error, term()}
  def preflight(%ManifestDeploymentContext{} = context, operation_id, archive_sha256) do
    case get(context, operation_id) do
      {:ok, %ManifestDeployment{archive_sha256: ^archive_sha256} = operation} ->
        {:ok, {:replay, operation}}

      {:ok, %ManifestDeployment{}} ->
        {:error, :deployment_operation_conflict}

      {:error, %FavnOrchestrator.Persistence.Error{kind: :not_found}} ->
        {:ok, :new}

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Acquires bounded PostgreSQL upload admission."
  @spec acquire_upload(ManifestDeploymentContext.t(), String.t()) :: :ok | {:error, term()}
  def acquire_upload(%ManifestDeploymentContext{} = context, lease_id) do
    now = DateTime.utc_now()

    Persistence.stores().registry.acquire_manifest_upload_lease(%AcquireManifestUploadLease{
      context: context,
      lease_id: lease_id,
      occurred_at: now,
      expires_at: DateTime.add(now, @upload_lease_seconds, :second)
    })
  end

  @doc "Renews upload admission while request chunks are arriving."
  @spec renew_upload(ManifestDeploymentContext.t(), String.t()) :: :ok | {:error, term()}
  def renew_upload(%ManifestDeploymentContext{} = context, lease_id) do
    Persistence.stores().registry.renew_manifest_upload_lease(%RenewManifestUploadLease{
      context: context,
      lease_id: lease_id,
      expires_at: DateTime.add(DateTime.utc_now(), @upload_lease_seconds, :second)
    })
  end

  @doc "Idempotently releases upload admission."
  @spec release_upload(ManifestDeploymentContext.t(), String.t()) :: :ok | {:error, term()}
  def release_upload(%ManifestDeploymentContext{} = context, lease_id) do
    Persistence.stores().registry.release_manifest_upload_lease(%ReleaseManifestUploadLease{
      context: context,
      lease_id: lease_id
    })
  end

  @doc "Registers one validated internal execution-package batch."
  @spec register_packages(ManifestDeploymentContext.t(), [ExecutionPackage.t()]) ::
          :ok | {:error, term()}
  def register_packages(%ManifestDeploymentContext{} = context, packages)
      when is_list(packages) do
    platform =
      SystemContext.platform(:manifest_deployment_package_ingest,
        roles: [:platform_operator],
        request_id: context.request_id
      )

    ExecutionPackages.register(platform, packages)
  end

  @doc "Atomically registers the manifest and accepts asynchronous activation intent."
  @spec accept(
          ManifestDeploymentContext.t(),
          String.t(),
          String.t(),
          String.t(),
          Version.t()
        ) :: {:ok, :accepted | :replay, ManifestDeployment.t()} | {:error, term()}
  def accept(context, operation_id, archive_sha256, request_fingerprint, %Version{} = version) do
    platform =
      SystemContext.platform(:manifest_deployment_accept,
        roles: [:platform_operator],
        request_id: context.request_id
      )

    workspace =
      SystemContext.workspace(context.workspace_id, :manifest_deployment_accept,
        roles: [:platform_operator],
        request_id: context.request_id
      )

    Persistence.stores().registry.accept_manifest_deployment(%AcceptManifestDeployment{
      context: context,
      platform_context: platform,
      workspace_context: workspace,
      operation_id: operation_id,
      archive_sha256: archive_sha256,
      request_fingerprint: request_fingerprint,
      version: version,
      occurred_at: DateTime.utc_now()
    })
  end

  @doc "Reads one operation only through its deployment-authorized workspace."
  @spec get(ManifestDeploymentContext.t(), String.t()) ::
          {:ok, ManifestDeployment.t()} | {:error, term()}
  def get(%ManifestDeploymentContext{} = context, operation_id) do
    Persistence.stores().registry.get_manifest_deployment(%GetManifestDeployment{
      context: context,
      operation_id: operation_id
    })
  end

  @doc "Builds the permanent request fingerprint for the fixed v1 activation selection."
  @spec fingerprint(ManifestDeploymentContext.t(), String.t(), String.t(), Version.t()) ::
          String.t()
  def fingerprint(context, operation_id, archive_sha256, %Version{} = version) do
    :crypto.hash(
      :sha256,
      :erlang.term_to_binary(
        {
          context.workspace_id,
          operation_id,
          archive_sha256,
          version.manifest_version_id,
          version.content_hash,
          version.runner_releases,
          fixed_selection()
        },
        [:deterministic]
      )
    )
    |> Base.encode16(case: :lower)
  end

  @doc "Returns the first-party all-common deployment selection."
  @spec fixed_selection() :: map()
  def fixed_selection do
    %{
      common_assets: "all",
      common_pipelines: "all",
      workspace_assets: [],
      workspace_pipelines: []
    }
  end

  @doc false
  def claim_next(owner, expires_at) do
    Persistence.stores().registry.claim_manifest_deployment(%ClaimManifestDeployment{
      platform_context:
        SystemContext.platform(:manifest_deployment_claim, roles: [:platform_operator]),
      owner: owner,
      expires_at: expires_at,
      occurred_at: DateTime.utc_now()
    })
  end

  @doc false
  def renew_claim(operation, owner, expires_at) do
    Persistence.stores().registry.renew_manifest_deployment_claim(%RenewManifestDeploymentClaim{
      platform_context:
        SystemContext.platform(:manifest_deployment_claim_renewal,
          roles: [:platform_operator]
        ),
      workspace_id: operation.workspace_id,
      operation_id: operation.operation_id,
      owner: owner,
      fence: operation.claim_fence,
      expires_at: expires_at
    })
  end

  @doc false
  def update_progress(operation, owner, completed, total) do
    Persistence.stores().registry.update_manifest_deployment_progress(
      %UpdateManifestDeploymentProgress{
        platform_context:
          SystemContext.platform(:manifest_deployment_progress, roles: [:platform_operator]),
        workspace_id: operation.workspace_id,
        operation_id: operation.operation_id,
        owner: owner,
        fence: operation.claim_fence,
        completed: completed,
        total: total,
        occurred_at: DateTime.utc_now()
      }
    )
  end

  @doc false
  def release_claim(operation, owner) do
    Persistence.stores().registry.release_manifest_deployment_claim(
      %ReleaseManifestDeploymentClaim{
        platform_context:
          SystemContext.platform(:manifest_deployment_claim_release,
            roles: [:platform_operator]
          ),
        workspace_id: operation.workspace_id,
        operation_id: operation.operation_id,
        owner: owner,
        fence: operation.claim_fence,
        occurred_at: DateTime.utc_now()
      }
    )
  end

  @doc false
  def complete(operation, owner, state, opts \\ []) do
    Persistence.stores().registry.complete_manifest_deployment(%CompleteManifestDeployment{
      platform_context:
        SystemContext.platform(:manifest_deployment_completion, roles: [:platform_operator]),
      workspace_id: operation.workspace_id,
      operation_id: operation.operation_id,
      owner: owner,
      fence: operation.claim_fence,
      state: state,
      deployment_id: Keyword.get(opts, :deployment_id),
      failure_class: Keyword.get(opts, :failure_class),
      activation_diagnostics: Keyword.get(opts, :activation_diagnostics),
      occurred_at: DateTime.utc_now()
    })
  end
end
