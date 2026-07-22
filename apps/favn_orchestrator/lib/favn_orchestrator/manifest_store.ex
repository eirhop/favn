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
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.Results.CursorPage

  @doc "Provisions one customer workspace through explicit platform authority."
  @spec provision_workspace(ProvisionWorkspace.t()) :: :ok | {:error, Error.t()}
  def provision_workspace(%ProvisionWorkspace{} = command) do
    Persistence.stores().registry.provision_workspace(command)
  end

  @doc "Pages active workspace identities for internal platform services."
  @spec page_workspaces(PlatformContext.t(), keyword()) ::
          {:ok, CursorPage.t(String.t())} | {:error, Error.t() | term()}
  def page_workspaces(%PlatformContext{} = context, opts \\ []) when is_list(opts) do
    with [] <- Keyword.keys(opts) -- [:after, :limit] do
      Persistence.stores().registry.page_workspaces(%PageWorkspaces{
        platform_context: context,
        after: Keyword.get(opts, :after),
        limit: Keyword.get(opts, :limit, 100)
      })
    else
      unknown -> {:error, {:unknown_workspace_page_options, unknown}}
    end
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
  def get_manifest(%PlatformContext{} = context, manifest_version_id)
      when is_binary(manifest_version_id) do
    with :ok <- validate_read_context(context) do
      Persistence.stores().registry.get_manifest(%ById{
        manifest_version_id: manifest_version_id
      })
    end
  end

  def get_manifest(%WorkspaceContext{} = context, manifest_version_id)
      when is_binary(manifest_version_id) do
    with {:ok, runtime} <- get_runtime_state(context),
         true <- runtime.manifest_version_id == manifest_version_id,
         {:ok, version} <-
           get_deployment_manifest(
             context,
             runtime.deployment_id,
             runtime.manifest_version_id
           ) do
      {:ok, version}
    else
      false -> {:error, Error.new(:not_found, "manifest is not active in workspace")}
      {:error, _reason} = error -> error
    end
  end

  @doc "Fetches a manifest through one exact historical or active workspace deployment."
  @spec get_deployment_manifest(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, Version.t()} | {:error, Error.t()}
  def get_deployment_manifest(
        %WorkspaceContext{} = context,
        deployment_id,
        manifest_version_id
      )
      when is_binary(deployment_id) and is_binary(manifest_version_id) do
    with :ok <- validate_workspace_read_context(context) do
      Persistence.stores().registry.get_deployment_manifest(%GetDeploymentManifest{
        workspace_context: context,
        deployment_id: deployment_id,
        manifest_version_id: manifest_version_id
      })
    end
  end

  @doc "Fetches an immutable release by canonical content hash."
  @spec get_manifest_by_content_hash(PlatformContext.t(), String.t()) ::
          {:ok, Version.t()} | {:error, Error.t()}
  def get_manifest_by_content_hash(%PlatformContext{} = context, content_hash)
      when is_binary(content_hash) do
    with :ok <- validate_read_context(context) do
      Persistence.stores().registry.get_manifest(%ByContentHash{content_hash: content_hash})
    end
  end

  def get_manifest_by_content_hash(_context, _content_hash),
    do: {:error, Error.new(:forbidden, "platform manifest read authority required")}

  @doc "Atomically creates and activates one immutable workspace deployment."
  @spec deploy_manifest(DeployManifest.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, Error.t()}
  def deploy_manifest(%DeployManifest{} = command) do
    Persistence.stores().registry.deploy_manifest(command)
  end

  @doc "Plans, creates, and activates one exact workspace deployment."
  @spec deploy_manifest(
          PlatformContext.t(),
          WorkspaceContext.t(),
          String.t(),
          DeploymentPlanner.t(),
          keyword()
        ) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, term()}
  def deploy_manifest(
        %PlatformContext{} = platform_context,
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
      :target_compatibilities,
      :idempotency,
      :occurred_at
    ]

    with [] <- Keyword.keys(opts) -- allowed,
         deployment_id when is_binary(deployment_id) and deployment_id != "" <-
           Keyword.get(opts, :deployment_id),
         {:ok, version} <- get_manifest(platform_context, manifest_version_id),
         {:ok, targets} <- DeploymentPlanner.plan(version, selection),
         occurred_at <- Keyword.get(opts, :occurred_at, DateTime.utc_now()),
         {:ok, schedules} <- deployment_schedules(version, targets, occurred_at, opts) do
      command = %DeployManifest{
        platform_context: platform_context,
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
        target_compatibilities: Keyword.get(opts, :target_compatibilities, []),
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
    with :ok <- validate_workspace_read_context(context) do
      Persistence.stores().registry.get_runtime_state(%GetRuntimeState{
        workspace_context: context
      })
    end
  end

  @doc "Returns the exact target grants for one workspace deployment."
  @spec get_deployment_targets(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, [FavnOrchestrator.Persistence.Commands.DeploymentTarget.t()]}
          | {:error, Error.t() | term()}
  def get_deployment_targets(%WorkspaceContext{} = context, deployment_id, opts \\ [])
      when is_binary(deployment_id) and is_list(opts) do
    with :ok <- validate_workspace_read_context(context),
         [] <- Keyword.keys(opts) -- [:customer_visible_only] do
      Persistence.stores().registry.get_deployment_targets(%GetDeploymentTargets{
        workspace_context: context,
        deployment_id: deployment_id,
        customer_visible_only: Keyword.get(opts, :customer_visible_only, false)
      })
    else
      {:error, _reason} = error -> error
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

  defp validate_read_context(%PlatformContext{} = context) do
    if PlatformContext.valid?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "valid platform context required")}
  end

  defp validate_workspace_read_context(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
         ),
       do: :ok,
       else: {:error, Error.new(:forbidden, "workspace read role required")}
  end
end
