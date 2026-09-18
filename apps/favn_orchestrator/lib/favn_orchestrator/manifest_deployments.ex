defmodule FavnOrchestrator.ManifestDeployments do
  @moduledoc """
  Durable local and first-party archive deployment facade.

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
          String.t(),
          Version.t()
        ) :: {:ok, :accepted | :replay, ManifestDeployment.t()} | {:error, term()}
  def accept(
        context,
        operation_id,
        upload_lease_id,
        archive_sha256,
        request_fingerprint,
        %Version{} = version
      ) do
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
      upload_lease_id: upload_lease_id,
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
  def claim_next(owner, expires_at, inspection_timeout_ms \\ 300_000) do
    Persistence.stores().registry.claim_manifest_deployment(%ClaimManifestDeployment{
      platform_context:
        SystemContext.platform(:manifest_deployment_claim, roles: [:platform_operator]),
      owner: owner,
      expires_at: expires_at,
      inspection_timeout_ms: inspection_timeout_ms,
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

  @doc "Accepts local deployment intent with a 45-second owner lease; replay requires the same session and manifest."
  @spec accept_local(
          FavnOrchestrator.Persistence.WorkspaceContext.t(),
          String.t(),
          String.t(),
          String.t()
        ) ::
          {:ok, :accepted | :replay, ManifestDeployment.t()} | {:error, term()}
  def accept_local(context, operation_id, session_id, manifest_version_id) do
    now = DateTime.utc_now()

    Persistence.stores().registry.accept_local_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.AcceptLocalManifestDeployment{
        workspace_context: context,
        operation_id: operation_id,
        session_id: session_id,
        manifest_version_id: manifest_version_id,
        occurred_at: now,
        expires_at: DateTime.add(now, 45, :second)
      }
    )
  end

  @doc "Renews a still-live local owner; expired ownership cannot be revived."
  @spec renew_local(FavnOrchestrator.Persistence.WorkspaceContext.t(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def renew_local(context, operation_id, session_id) do
    now = DateTime.utc_now()

    Persistence.stores().registry.renew_local_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.RenewLocalManifestDeployment{
        workspace_context: context,
        operation_id: operation_id,
        session_id: session_id,
        occurred_at: now,
        expires_at: DateTime.add(now, 45, :second)
      }
    )
  end

  @doc "Requests cancellation without claiming that activation rolled back or execution stopped."
  @spec cancel(FavnOrchestrator.Persistence.WorkspaceContext.t(), String.t(), atom()) ::
          {:ok, ManifestDeployment.t() | :cancelled_before_acceptance} | {:error, term()}
  def cancel(context, operation_id, reason) do
    Persistence.stores().registry.cancel_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.CancelManifestDeployment{
        workspace_context: context,
        operation_id: operation_id,
        reason: reason,
        occurred_at: DateTime.utc_now()
      }
    )
  end

  @doc "Reads an operation using existing workspace operator authority."
  @spec get_local(FavnOrchestrator.Persistence.WorkspaceContext.t(), String.t()) ::
          {:ok, ManifestDeployment.t()} | {:error, term()}
  def get_local(context, operation_id) do
    if FavnOrchestrator.Persistence.WorkspaceContext.valid?(context) and
         :platform_operator in context.roles do
      with {:ok, deployment_context} <-
             ManifestDeploymentContext.new(
               context.principal_id,
               context.workspace_id,
               operation_id
             ),
           do: get(deployment_context, operation_id)
    else
      {:error, :forbidden}
    end
  end

  @doc "Returns aggregate inspection counts and at most 100 task identities; cursor is the previous task ID."
  @spec inspections(FavnOrchestrator.Persistence.WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def inspections(context, operation_id, opts \\ []) do
    Persistence.stores().registry.deployment_inspections(
      %FavnOrchestrator.Persistence.Queries.DeploymentInspections{
        workspace_context: context,
        operation_id: operation_id,
        after_task_id: Keyword.get(opts, :after_task_id),
        limit: Keyword.get(opts, :limit, 100)
      }
    )
  end

  @doc false
  def reconcile do
    with {:ok, batches} <-
           Persistence.stores().registry.reconcile_manifest_deployments(
             %FavnOrchestrator.Persistence.Commands.ReconcileManifestDeployments{
               platform_context:
                 SystemContext.platform(:deployment_reconciliation, roles: [:platform_operator]),
               occurred_at: DateTime.utc_now()
             }
           ) do
      Enum.each(batches, fn batch ->
        Enum.each(
          batch.task_ids,
          &FavnOrchestrator.RunnerTasks.request_cancellation(
            batch.workspace_id,
            &1,
            :deployment_owner_closed,
            wait_for_ack: false
          )
        )

        :telemetry.execute(
          [:favn, :deployment, :inspection_cleanup],
          %{task_count: length(batch.task_ids)},
          Map.take(batch, [:workspace_id, :operation_id, :cleanup_state])
        )
      end)

      {:ok, batches}
    end
  end

  @doc "Pins binding versions for one attempt; a changed base requires fresh inspections in a new attempt."
  @spec pin_inspection_base(
          FavnOrchestrator.Persistence.WorkspaceContext.t(),
          String.t() | nil,
          map()
        ) :: :ok | {:error, term()}
  def pin_inspection_base(_context, nil, _bindings), do: :ok

  def pin_inspection_base(context, operation_id, bindings) do
    pins = Map.new(bindings, fn {target, binding} -> {target, binding.version} end)
    hash = :crypto.hash(:sha256, :erlang.term_to_binary(pins, [:deterministic]))

    Persistence.stores().registry.pin_deployment_inspection_base(
      %FavnOrchestrator.Persistence.Commands.PinDeploymentInspectionBase{
        workspace_context: context,
        operation_id: operation_id,
        binding_hash: hash
      }
    )
  end

  @doc """
  Settles at most 100 explicitly verified read-only inspection assignments.

  The operator must stop the exact runner executions and backend queries first,
  then attest both facts and provide an evidence reference. An operation ID of
  nil scopes this command to legacy unowned inspections. Assignment generations
  fence stale attestations; this command does not infer quiescence from expiry.
  """
  @spec resolve_inspections(
          FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections.t()
        ) :: {:ok, non_neg_integer()} | {:error, term()}
  def resolve_inspections(
        %FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections{} = command
      ),
      do: Persistence.stores().registry.resolve_deployment_inspections(command)
end
