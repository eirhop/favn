defmodule FavnOrchestrator.ManifestStore do
  @moduledoc """
  Manifest persistence and activation facade for orchestrator runtime.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.Manifest.TargetDescriptor
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.DeploymentSchedules
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.GetManifestTargetDescriptors
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetActiveDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.BoundedWorker
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.ManifestStore.Lease

  @scalar_query_bytes 16 * 1_024 * 1_024
  @scoped_token_key {__MODULE__, :memory_capacity_token}

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

  defp register_manifest(%PlatformContext{} = context, %Version{} = version) do
    if Enum.any?(context.roles, &(&1 in [:platform_operator, :platform_admin])) do
      case Persistence.stores().registry.register_manifest(%RegisterManifest{
             platform_context: context,
             version: version
           }) do
        {:error,
         %Error{
           kind: :limit_exceeded,
           details: %{reason: :manifest_memory_budget_exceeded}
         }} ->
          {:error, :manifest_memory_budget_exceeded}

        result ->
          result
      end
    else
      {:error, Error.new(:forbidden, "platform manifest write role required")}
    end
  end

  @doc "Publishes a release or returns the already-published canonical release."
  @spec publish_manifest(PlatformContext.t(), Version.t(), keyword()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish_manifest(%PlatformContext{} = context, %Version{} = version, opts)
      when is_list(opts) do
    with :ok <- require_memory_capacity_token(opts),
         %MemoryCapacity{} = token <- Keyword.fetch!(opts, :memory_capacity_token),
         {:ok, verified} <- verify_publishable_manifest(version, token) do
      with_scoped_token(opts, fn token ->
        result =
          case load_manifest_by_content_hash(context, verified.content_hash, token) do
            {:ok, existing} ->
              {:ok, :already_published, existing}

            {:error, %Error{kind: :not_found}} ->
              case register_manifest(context, verified) do
                {:ok, persisted} ->
                  {:ok, :published, persisted}

                {:error, %Error{kind: :conflict}} ->
                  resolve_publish_conflict(context, verified, token)

                {:error, _reason} = error ->
                  error
              end

            {:error, _reason} = error ->
              error
          end

        result
      end)
    end
  end

  defp verify_publishable_manifest(%Version{} = version, token) do
    budget = Budget.live_index()

    with :ok <- resize_scoped_working(token, budget) do
      BoundedWorker.run_serialized(
        fn ->
          with {:ok, verified} <- Version.verify(version),
               {:ok, canonical_json} <- Serializer.encode_manifest(verified.manifest),
               {:ok, _read_budget} <- Budget.persisted_index(byte_size(canonical_json)) do
            {:ok, verified}
          end
        end,
        budget,
        Budget.serialized_result_limit(budget)
      )
    end
  end

  defp require_memory_capacity_token(opts) do
    case Keyword.get(opts, :memory_capacity_token) do
      %MemoryCapacity{} -> :ok
      nil -> {:error, :memory_capacity_token_required}
      _invalid -> {:error, :invalid_memory_capacity_token}
    end
  end

  defp load_manifest_by_content_hash(%PlatformContext{} = context, content_hash, token)
       when is_binary(content_hash) do
    selector = %ByContentHash{content_hash: content_hash}

    with :ok <- validate_read_context(context),
         {:ok, bytes} <- Persistence.stores().registry.get_manifest_size(selector),
         {:ok, budget} <- Budget.persisted_index(bytes),
         :ok <- resize_scoped_working(token, budget) do
      Persistence.stores().registry.get_manifest(selector)
    end
  end

  @doc """
  Loads one immutable manifest only for the duration of the callback.

  The same owner token may be supplied with `:memory_capacity_token` so nested
  activation and planning resize one reservation instead of stacking leases.
  Without that token, returning the manifest directly or through a container or
  closure is rejected. Callbacks must not send or store the scoped value.
  """
  @spec with_manifest(
          WorkspaceContext.t() | PlatformContext.t(),
          String.t(),
          keyword(),
          (Version.t() -> result)
        ) :: result | {:error, term()}
        when result: term()
  def with_manifest(context, manifest_version_id, opts \\ [], fun)
      when is_binary(manifest_version_id) and is_list(opts) and is_function(fun, 1) do
    with_scoped_token(opts, fn token ->
      selector = %ById{manifest_version_id: manifest_version_id}

      with :ok <- authorize_manifest_read(context, manifest_version_id),
           {:ok, bytes} <- Persistence.stores().registry.get_manifest_size(selector),
           {:ok, budget} <- Budget.persisted_index(bytes),
           :ok <- resize_scoped_working(token, budget),
           {:ok, version} <- load_authorized_manifest(context, manifest_version_id) do
        fun.(version)
      end
    end)
  end

  @doc false
  @spec checkout_manifest(
          WorkspaceContext.t() | PlatformContext.t(),
          String.t(),
          keyword()
        ) :: {:ok, Lease.t()} | {:error, term()}
  def checkout_manifest(context, manifest_version_id, opts \\ [])
      when is_binary(manifest_version_id) and is_list(opts) do
    copies = Keyword.get(opts, :copies, 0)
    handoff_to = Keyword.get(opts, :handoff_to)

    with true <- is_integer(copies) and copies >= 0,
         true <- is_nil(handoff_to) or is_pid(handoff_to),
         {:ok, token} <- MemoryCapacity.acquire(@scalar_query_bytes, kind: :manifest_retained) do
      selector = %ById{manifest_version_id: manifest_version_id}

      result =
        with :ok <- authorize_manifest_read(context, manifest_version_id),
             {:ok, bytes} <- Persistence.stores().registry.get_manifest_size(selector),
             {:ok, budget} <- Budget.persisted_index(bytes),
             :ok <- resize_scoped_working(token, budget),
             {:ok, version} <- load_authorized_manifest(context, manifest_version_id),
             {:ok, lease} <- build_manifest_lease(token, version, copies, handoff_to) do
          {:ok, lease}
        end

      case result do
        {:ok, %Lease{}} = success -> success
        {:error, _reason} = error -> release_manifest(error, token)
      end
    else
      false -> {:error, :invalid_manifest_checkout_options}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec checkout_deployment_manifest(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          keyword()
        ) :: {:ok, Lease.t()} | {:error, term()}
  def checkout_deployment_manifest(context, deployment_id, manifest_version_id, opts \\ [])
      when is_struct(context, WorkspaceContext) and is_binary(deployment_id) and
             is_binary(manifest_version_id) and is_list(opts) do
    copies = Keyword.get(opts, :copies, 0)
    handoff_to = Keyword.get(opts, :handoff_to)

    with true <- is_integer(copies) and copies >= 0,
         true <- is_nil(handoff_to) or is_pid(handoff_to),
         {:ok, token} <- MemoryCapacity.acquire(@scalar_query_bytes, kind: :manifest_retained) do
      selector = %ById{manifest_version_id: manifest_version_id}

      result =
        with :ok <- validate_workspace_read_context(context),
             {:ok, bytes} <- Persistence.stores().registry.get_manifest_size(selector),
             {:ok, budget} <- Budget.persisted_index(bytes),
             :ok <- resize_scoped_working(token, budget),
             {:ok, version} <-
               load_deployment_manifest(context, deployment_id, manifest_version_id),
             {:ok, lease} <- build_manifest_lease(token, version, copies, handoff_to) do
          {:ok, lease}
        end

      case result do
        {:ok, %Lease{}} = success -> success
        {:error, _reason} = error -> release_manifest(error, token)
      end
    else
      false -> {:error, :invalid_manifest_checkout_options}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec release_manifest(Lease.t()) :: :ok
  def release_manifest(%Lease{capacity_token: token}), do: MemoryCapacity.release(token)

  defp release_manifest(error, token) do
    :ok = MemoryCapacity.release(token)
    error
  end

  defp maybe_handoff(_token, nil), do: :ok
  defp maybe_handoff(token, owner), do: MemoryCapacity.handoff(token, owner)

  defp build_manifest_lease(token, version, copies, handoff_to) do
    retained_bytes = Budget.retained_term_bytes(version)
    reserved_bytes = retained_bytes * (copies + 1)

    with :ok <- MemoryCapacity.transfer(token, retained_bytes, copies * retained_bytes),
         :ok <- maybe_handoff(token, handoff_to) do
      {:ok,
       %Lease{
         version: version,
         capacity_token: token,
         reserved_bytes: reserved_bytes
       }}
    end
  end

  @doc "Uses a manifest through one exact historical or active workspace deployment."
  @spec with_deployment_manifest(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          keyword(),
          (Version.t() -> result)
        ) :: result | {:error, term()}
        when result: term()
  def with_deployment_manifest(context, deployment_id, manifest_version_id, opts \\ [], fun)
      when is_struct(context, WorkspaceContext) and is_binary(deployment_id) and
             is_binary(manifest_version_id) and is_list(opts) and is_function(fun, 1) do
    with_scoped_token(opts, fn token ->
      selector = %ById{manifest_version_id: manifest_version_id}

      with :ok <- validate_workspace_read_context(context),
           {:ok, bytes} <- Persistence.stores().registry.get_manifest_size(selector),
           {:ok, budget} <- Budget.persisted_index(bytes),
           :ok <- resize_scoped_working(token, budget),
           {:ok, version} <-
             load_deployment_manifest(context, deployment_id, manifest_version_id) do
        fun.(version)
      end
    end)
  end

  @doc """
  Loads and compiles an index only for the duration of the callback.

  Without an explicit owner token, returning the index directly or through a
  container or closure is rejected. Callbacks must not send or store it.
  """
  @spec with_index(Version.t(), keyword(), (Index.t() -> result)) ::
          result | {:error, term()}
        when result: term()
  def with_index(%Version{} = version, opts \\ [], fun)
      when is_list(opts) and is_function(fun, 1) do
    with_scoped_token(opts, fn token ->
      budget = Budget.live_index()

      with :ok <- resize_scoped_working(token, budget),
           {:ok, %Index{} = handoff} <-
             BoundedWorker.run_serialized(
               fn -> build_index_handoff(version) end,
               budget,
               Budget.serialized_result_limit(budget)
             ) do
        index = Index.restore_worker_handoff(handoff, version.manifest.assets)
        fun.(index)
      end
    end)
  end

  defp build_index_handoff(version) do
    with {:ok, %Index{} = index} <- Index.build_from_version(version) do
      {:ok, Index.prepare_worker_handoff(index)}
    end
  end

  @doc """
  Fetches selected validated target descriptors without activating the manifest.

  This is the bounded compatibility-planning path for immutable historical
  manifests whose full runtime contract is no longer activatable.
  """
  @spec get_manifest_target_descriptors(
          PlatformContext.t(),
          String.t(),
          [String.t()]
        ) :: {:ok, [TargetDescriptor.t()]} | {:error, Error.t()}
  def get_manifest_target_descriptors(
        %PlatformContext{} = context,
        manifest_version_id,
        target_ids
      )
      when is_binary(manifest_version_id) and is_list(target_ids) do
    Persistence.stores().registry.get_manifest_target_descriptors(%GetManifestTargetDescriptors{
      platform_context: context,
      manifest_version_id: manifest_version_id,
      target_ids: target_ids
    })
  end

  defp load_deployment_manifest(
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

  @doc "Returns one immutable deployment's validated non-secret configuration."
  @spec get_deployment_configuration(WorkspaceContext.t(), String.t()) ::
          {:ok, map()} | {:error, Error.t()}
  def get_deployment_configuration(%WorkspaceContext{} = context, deployment_id)
      when is_binary(deployment_id) do
    with :ok <- validate_workspace_read_context(context) do
      Persistence.stores().registry.get_deployment_configuration(%GetDeploymentConfiguration{
        workspace_context: context,
        deployment_id: deployment_id
      })
    end
  end

  @doc "Returns the active immutable deployment id and validated non-secret configuration."
  @spec get_active_deployment_configuration(WorkspaceContext.t()) ::
          {:ok, {String.t(), map()}} | {:error, Error.t()}
  def get_active_deployment_configuration(%WorkspaceContext{} = context) do
    with :ok <- validate_workspace_read_context(context) do
      Persistence.stores().registry.get_active_deployment_configuration(
        %GetActiveDeploymentConfiguration{workspace_context: context}
      )
    end
  end

  @doc "Atomically creates and activates one immutable workspace deployment."
  @spec deploy_manifest(DeployManifest.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, Error.t()}
  def deploy_manifest(%DeployManifest{} = command) do
    Persistence.stores().registry.deploy_manifest(command)
  end

  @doc "Reserves deployment planning or replays its exact committed result."
  @spec begin_manifest_deployment(WorkspaceContext.t(), CommandIdempotency.t() | nil) ::
          {:ok,
           {:new, CommandIdempotency.t() | nil}
           | {:replay, FavnOrchestrator.Persistence.Results.RuntimeState.t()}}
          | {:error, term()}
  def begin_manifest_deployment(_context, nil), do: {:ok, {:new, nil}}

  def begin_manifest_deployment(
        %WorkspaceContext{} = context,
        %CommandIdempotency{} = idempotency
      ) do
    Persistence.stores().registry.begin_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.BeginManifestDeployment{
        workspace_context: context,
        idempotency: idempotency
      }
    )
  end

  @doc "Renews the current generation-fenced deployment-planning lease."
  @spec heartbeat_manifest_deployment(WorkspaceContext.t(), CommandIdempotency.t()) ::
          :ok | {:error, term()}
  def heartbeat_manifest_deployment(
        %WorkspaceContext{} = context,
        %CommandIdempotency{} = idempotency
      ) do
    Persistence.stores().registry.heartbeat_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.HeartbeatManifestDeployment{
        workspace_context: context,
        idempotency: idempotency
      }
    )
  end

  @doc "Releases the current uncommitted deployment-planning reservation."
  @spec abandon_manifest_deployment(WorkspaceContext.t(), CommandIdempotency.t()) ::
          :ok | {:error, term()}
  def abandon_manifest_deployment(
        %WorkspaceContext{} = context,
        %CommandIdempotency{} = idempotency
      ) do
    Persistence.stores().registry.abandon_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.AbandonManifestDeployment{
        workspace_context: context,
        idempotency: idempotency
      }
    )
  end

  @doc "Plans, creates, and activates one exact workspace deployment."
  @spec deploy_manifest(
          PlatformContext.t(),
          WorkspaceContext.t(),
          Version.t(),
          DeploymentPlanner.t(),
          keyword()
        ) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, term()}
  def deploy_manifest(
        %PlatformContext{} = platform_context,
        %WorkspaceContext{} = context,
        %Version{} = version,
        %DeploymentPlanner{} = selection,
        opts
      )
      when is_list(opts) do
    allowed = [
      :deployment_id,
      :configuration,
      :configuration_version,
      :schedules,
      :capacity_scopes,
      :target_compatibilities,
      :activation_diagnostics,
      :activation_lease,
      :expected_active_deployment_id,
      :idempotency,
      :occurred_at,
      :memory_capacity_token
    ]

    with [] <- Keyword.keys(opts) -- allowed,
         deployment_id when is_binary(deployment_id) and deployment_id != "" <-
           Keyword.get(opts, :deployment_id),
         {:ok, targets} <- DeploymentPlanner.plan(version, selection, capacity_opts(opts)),
         occurred_at <- Keyword.get(opts, :occurred_at, DateTime.utc_now()),
         {:ok, schedules} <-
           deployment_schedules(version, targets, occurred_at, opts, capacity_opts(opts)) do
      command = %DeployManifest{
        platform_context: platform_context,
        workspace_context: context,
        deployment_id: deployment_id,
        manifest_version_id: version.manifest_version_id,
        configuration: Keyword.get(opts, :configuration, %{}),
        configuration_version: Keyword.get(opts, :configuration_version, 1),
        targets: targets,
        schedules: schedules,
        capacity_scopes: merge_capacity_scopes([], Keyword.get(opts, :capacity_scopes, [])),
        target_compatibilities: Keyword.get(opts, :target_compatibilities, []),
        activation_diagnostics: Keyword.get(opts, :activation_diagnostics),
        activation_lease: Keyword.get(opts, :activation_lease),
        expected_active_deployment_id:
          Keyword.get(opts, :expected_active_deployment_id, :unchecked),
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

  defp deployment_schedules(version, targets, occurred_at, opts, capacity_opts) do
    case Keyword.fetch(opts, :schedules) do
      {:ok, schedules} when is_list(schedules) -> {:ok, schedules}
      {:ok, _invalid} -> {:error, :invalid_deployment_schedules}
      :error -> DeploymentSchedules.plan(version, targets, occurred_at, capacity_opts)
    end
  end

  defp capacity_opts(opts) do
    case Keyword.get(opts, :memory_capacity_token) do
      nil -> []
      token -> [memory_capacity_token: token]
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

  defp with_scoped_token(opts, fun) do
    case Keyword.get(opts, :memory_capacity_token) do
      %MemoryCapacity{} = token ->
        with_current_scoped_token(token, fn -> fun.(token) end)

      nil ->
        case Process.get(@scoped_token_key) do
          %MemoryCapacity{} = token ->
            fun.(token)

          nil ->
            MemoryCapacity.with_lease(
              @scalar_query_bytes,
              [kind: :manifest_use],
              fn token ->
                with_current_scoped_token(token, fn ->
                  result = fun.(token)
                  validate_temporary_scoped_result(result)
                end)
              end
            )
        end

      _invalid ->
        {:error, :invalid_memory_capacity_token}
    end
  end

  defp with_current_scoped_token(token, fun) do
    previous = Process.put(@scoped_token_key, token)

    try do
      fun.()
    after
      restore_process_value(@scoped_token_key, previous)
    end
  end

  defp resize_scoped_working(token, requested_bytes),
    do: MemoryCapacity.grow(token, requested_bytes)

  defp restore_process_value(key, nil), do: Process.delete(key)
  defp restore_process_value(key, value), do: Process.put(key, value)

  defp validate_temporary_scoped_result(result) do
    if contains_scoped_value?(result),
      do: {:error, :scoped_manifest_value_escape},
      else: result
  end

  defp contains_scoped_value?(%Version{manifest: nil} = version) do
    version
    |> Map.from_struct()
    |> Map.delete(:manifest)
    |> contains_scoped_value?()
  end

  defp contains_scoped_value?(%Version{}), do: true
  defp contains_scoped_value?(%Index{}), do: true

  defp contains_scoped_value?(value) when is_map(value) do
    value
    |> Map.to_list()
    |> Enum.any?(fn {key, item} ->
      contains_scoped_value?(key) or contains_scoped_value?(item)
    end)
  end

  defp contains_scoped_value?(value) when is_tuple(value),
    do: tuple_contains_scoped_value?(value, tuple_size(value) - 1)

  defp contains_scoped_value?([]), do: false

  defp contains_scoped_value?([head | tail]),
    do: contains_scoped_value?(head) or contains_scoped_value?(tail)

  defp contains_scoped_value?(value) when is_function(value) do
    {:env, environment} = :erlang.fun_info(value, :env)
    contains_scoped_value?(environment)
  end

  defp contains_scoped_value?(_value), do: false

  defp tuple_contains_scoped_value?(_value, index) when index < 0, do: false

  defp tuple_contains_scoped_value?(value, index) do
    contains_scoped_value?(elem(value, index)) or
      tuple_contains_scoped_value?(value, index - 1)
  end

  defp authorize_manifest_read(%PlatformContext{} = context, _manifest_version_id),
    do: validate_read_context(context)

  defp authorize_manifest_read(%WorkspaceContext{} = context, manifest_version_id) do
    with :ok <- validate_workspace_read_context(context),
         {:ok, runtime} <- get_runtime_state(context),
         true <- runtime.manifest_version_id == manifest_version_id do
      :ok
    else
      false -> {:error, Error.new(:not_found, "manifest is not active in workspace")}
      {:error, _reason} = error -> error
    end
  end

  defp load_authorized_manifest(%PlatformContext{}, manifest_version_id) do
    Persistence.stores().registry.get_manifest(%ById{manifest_version_id: manifest_version_id})
  end

  defp load_authorized_manifest(%WorkspaceContext{} = context, manifest_version_id) do
    with {:ok, runtime} <- get_runtime_state(context),
         true <- runtime.manifest_version_id == manifest_version_id do
      load_deployment_manifest(context, runtime.deployment_id, manifest_version_id)
    else
      false -> {:error, Error.new(:not_found, "manifest is not active in workspace")}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_publish_conflict(
         %PlatformContext{} = context,
         %Version{} = version,
         token
       ) do
    case load_manifest_by_content_hash(context, version.content_hash, token) do
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
