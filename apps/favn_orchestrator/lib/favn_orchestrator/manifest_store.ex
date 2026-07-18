defmodule FavnOrchestrator.ManifestStore do
  @moduledoc """
  Manifest persistence and activation facade for orchestrator runtime.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.CapacityConfiguration
  alias FavnOrchestrator.Persistence.DeploymentSchedules
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @doc "Provisions one customer workspace through explicit platform authority."
  @spec provision_workspace(ProvisionWorkspace.t()) :: :ok | {:error, Error.t()}
  def provision_workspace(%ProvisionWorkspace{} = command) do
    Persistence.stores().registry.provision_workspace(command)
  end

  @doc "Registers one immutable global manifest release."
  @spec register_manifest(PlatformContext.t(), Version.t()) ::
          {:ok, Version.t()} | {:error, Error.t()}
  def register_manifest(%PlatformContext{} = context, %Version{} = version) do
    if Enum.any?(context.roles, &(&1 in [:platform_operator, :platform_admin])) do
      Persistence.stores().registry.register_manifest(%RegisterManifest{
        platform_context: context,
        version: version
      })
    else
      {:error, Error.new(:forbidden, "platform manifest write role required")}
    end
  end

  @doc "Publishes a release or returns the already-published canonical release."
  @spec publish_manifest(PlatformContext.t(), Version.t()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish_manifest(%PlatformContext{} = context, %Version{} = version) do
    with {:ok, verified} <- Version.verify(version) do
      case get_manifest_by_content_hash(context, verified.content_hash) do
        {:ok, existing} ->
          {:ok, :already_published, existing}

        {:error, %Error{kind: :not_found}} ->
          case register_manifest(context, verified) do
            {:ok, persisted} -> {:ok, :published, persisted}
            {:error, %Error{kind: :conflict}} -> resolve_publish_conflict(context, verified)
            {:error, _reason} = error -> error
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc "Fetches an immutable release under an explicit authority context."
  @spec get_manifest(WorkspaceContext.t() | PlatformContext.t(), String.t()) ::
          {:ok, Version.t()} | {:error, Error.t()}
  def get_manifest(context, manifest_version_id)
      when is_binary(manifest_version_id) do
    with :ok <- validate_read_context(context) do
      Persistence.stores().registry.get_manifest(%ById{
        manifest_version_id: manifest_version_id
      })
    end
  end

  @doc "Fetches an immutable release by canonical content hash."
  @spec get_manifest_by_content_hash(
          WorkspaceContext.t() | PlatformContext.t(),
          String.t()
        ) :: {:ok, Version.t()} | {:error, Error.t()}
  def get_manifest_by_content_hash(context, content_hash) when is_binary(content_hash) do
    with :ok <- validate_read_context(context) do
      Persistence.stores().registry.get_manifest(%ByContentHash{content_hash: content_hash})
    end
  end

  @doc "Atomically creates and activates one immutable workspace deployment."
  @spec deploy_manifest(DeployManifest.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, Error.t()}
  def deploy_manifest(%DeployManifest{} = command) do
    Persistence.stores().registry.deploy_manifest(command)
  end

  @doc "Plans, creates, and activates one exact workspace deployment."
  @spec deploy_manifest(
          WorkspaceContext.t(),
          String.t(),
          DeploymentPlanner.t(),
          keyword()
        ) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, term()}
  def deploy_manifest(
        %WorkspaceContext{} = context,
        manifest_version_id,
        %DeploymentPlanner{} = selection,
        opts
      )
      when is_binary(manifest_version_id) and is_list(opts) do
    allowed = [
      :deployment_id,
      :configuration,
      :configuration_version,
      :schedules,
      :capacity_scopes,
      :idempotency,
      :occurred_at
    ]

    with [] <- Keyword.keys(opts) -- allowed,
         deployment_id when is_binary(deployment_id) and deployment_id != "" <-
           Keyword.get(opts, :deployment_id),
         {:ok, version} <- get_manifest(context, manifest_version_id),
         {:ok, targets} <- DeploymentPlanner.plan(version, selection),
         occurred_at <- Keyword.get(opts, :occurred_at, DateTime.utc_now()),
         {:ok, schedules} <- deployment_schedules(version, targets, occurred_at, opts) do
      command = %DeployManifest{
        workspace_context: context,
        deployment_id: deployment_id,
        manifest_version_id: manifest_version_id,
        configuration: Keyword.get(opts, :configuration, %{}),
        configuration_version: Keyword.get(opts, :configuration_version, 1),
        targets: targets,
        schedules: schedules,
        capacity_scopes:
          merge_capacity_scopes(
            CapacityConfiguration.deployment_scopes(context.workspace_id),
            Keyword.get(opts, :capacity_scopes, [])
          ),
        idempotency: Keyword.get(opts, :idempotency),
        occurred_at: occurred_at
      }

      deploy_manifest(command)
    else
      unknown when is_list(unknown) -> {:error, {:unknown_deployment_options, unknown}}
      nil -> {:error, :deployment_id_required}
      "" -> {:error, :deployment_id_required}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_deployment_options}
    end
  end

  defp deployment_schedules(version, targets, occurred_at, opts) do
    case Keyword.fetch(opts, :schedules) do
      {:ok, schedules} when is_list(schedules) -> {:ok, schedules}
      {:ok, _invalid} -> {:error, :invalid_deployment_schedules}
      :error -> DeploymentSchedules.plan(version, targets, occurred_at)
    end
  end

  defp merge_capacity_scopes(configured, explicit) do
    (configured ++ explicit)
    |> Enum.reduce(%{}, &Map.put(&2, &1.scope_id, &1))
    |> Map.values()
    |> Enum.sort_by(& &1.scope_id)
  end

  @doc "Returns one workspace's active deployment state."
  @spec get_runtime_state(WorkspaceContext.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, Error.t()}
  def get_runtime_state(%WorkspaceContext{} = context) do
    Persistence.stores().registry.get_runtime_state(%GetRuntimeState{workspace_context: context})
  end

  @doc "Returns the exact target grants for one workspace deployment."
  @spec get_deployment_targets(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, [FavnOrchestrator.Persistence.Commands.DeploymentTarget.t()]}
          | {:error, Error.t() | term()}
  def get_deployment_targets(%WorkspaceContext{} = context, deployment_id, opts \\ [])
      when is_binary(deployment_id) and is_list(opts) do
    with [] <- Keyword.keys(opts) -- [:customer_visible_only] do
      Persistence.stores().registry.get_deployment_targets(%GetDeploymentTargets{
        workspace_context: context,
        deployment_id: deployment_id,
        customer_visible_only: Keyword.get(opts, :customer_visible_only, false)
      })
    else
      unknown -> {:error, {:unknown_deployment_target_options, unknown}}
    end
  end

  @doc "Returns the active workspace deployment and its exact target grants."
  @spec get_active_deployment(WorkspaceContext.t(), keyword()) ::
          {:ok,
           {FavnOrchestrator.Persistence.Results.RuntimeState.t(),
            [FavnOrchestrator.Persistence.Commands.DeploymentTarget.t()]}}
          | {:error, term()}
  def get_active_deployment(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with {:ok, runtime} <- get_runtime_state(context),
         {:ok, targets} <- get_deployment_targets(context, runtime.deployment_id, opts) do
      {:ok, {runtime, targets}}
    end
  end

  @doc "Returns the active manifest release for one workspace."
  @spec get_active_manifest(WorkspaceContext.t()) :: {:ok, Version.t()} | {:error, Error.t()}
  def get_active_manifest(%WorkspaceContext{} = context) do
    with {:ok, runtime} <- get_runtime_state(context) do
      get_manifest(context, runtime.manifest_version_id)
    end
  end

  defp resolve_publish_conflict(%PlatformContext{} = context, %Version{} = version) do
    case get_manifest_by_content_hash(context, version.content_hash) do
      {:ok, existing} -> {:ok, :already_published, existing}
      {:error, _reason} -> {:error, Error.new(:conflict, "manifest release conflict")}
    end
  end

  defp validate_read_context(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "valid workspace context required")}
  end

  defp validate_read_context(%PlatformContext{} = context) do
    if PlatformContext.valid?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "valid platform context required")}
  end

  defp validate_read_context(_context),
    do: {:error, Error.new(:forbidden, "explicit persistence context required")}
end
