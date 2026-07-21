defmodule FavnStoragePostgres.Registry.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RegistryStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Storage.RunSnapshotCodec.ManifestAtoms
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.DeploymentConfig
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Idempotency.Transaction, as: IdempotencyTransaction
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Registry.ManifestCache
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AuthPlatformAuditEntry
  alias FavnStoragePostgres.Schemas.ExecutionPackage, as: ExecutionPackageRecord
  alias FavnStoragePostgres.Schemas.ManifestExecutionPackage
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.ScheduleCursor
  alias FavnStoragePostgres.Schemas.Workspace
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment
  alias FavnStoragePostgres.Schemas.WorkspaceDeploymentTarget
  alias FavnStoragePostgres.Schemas.WorkspaceRuntimeState

  @max_manifest_bytes 256 * 1_024 * 1_024
  @max_execution_package_bytes 4 * 1_024 * 1_024
  @max_execution_package_batch_bytes 32 * 1_024 * 1_024
  @max_execution_packages_per_command 1_000
  @execution_package_insert_size 100
  @execution_package_validation_batch_size 500
  @max_deployment_targets 10_000
  @max_deployment_target_descriptor_bytes 256 * 1_024
  @max_deployment_target_catalog_bytes 32 * 1_024 * 1_024
  @max_deployment_schedules 2_000
  @max_capacity_scopes 1_000
  @bulk_insert_size 500
  @current_manifest_schema Compatibility.current_schema_version()

  @impl true
  def provision_workspace(%ProvisionWorkspace{} = command) do
    with :ok <- validate_platform_provision(command),
         {:ok, result} <-
           Repo.transaction(fn ->
             attrs = %{
               workspace_id: command.workspace_id,
               slug: command.slug,
               display_name: command.display_name,
               status: "active",
               version: 1,
               inserted_at: command.occurred_at,
               updated_at: command.occurred_at
             }

             changeset = Workspace.changeset(%Workspace{}, attrs)

             if changeset.valid? do
               case Repo.insert_all(Workspace, [attrs], on_conflict: :nothing) do
                 {1, _rows} ->
                   Repo.insert!(%WorkspaceRuntimeState{
                     workspace_id: command.workspace_id,
                     revision: 0,
                     updated_at: command.occurred_at
                   })

                   OutboxWriter.insert!(%{
                     workspace_id: command.workspace_id,
                     command_id: "workspace.provision:" <> command.workspace_id,
                     event_kind: "workspace.provisioned",
                     aggregate_kind: "workspace",
                     aggregate_id: command.workspace_id,
                     aggregate_version: 1,
                     occurred_at: command.occurred_at,
                     payload: %{
                       "workspace_id" => command.workspace_id,
                       "slug" => command.slug,
                       "provisioned_by" => command.platform_context.principal_id
                     }
                   })

                   :ok

                 {0, _rows} ->
                   replay_workspace_provision!(command)
               end
             else
               Repo.rollback(changeset_error(changeset))
             end
           end) do
      result
    else
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_workspaces(%PageWorkspaces{} = query) do
    with :ok <- validate_workspace_page(query) do
      rows =
        Workspace
        |> where([workspace], workspace.status == "active")
        |> workspace_after(query.after)
        |> order_by([workspace], asc: workspace.workspace_id)
        |> limit(^(query.limit + 1))
        |> select([workspace], workspace.workspace_id)
        |> Repo.all()

      items = Enum.take(rows, query.limit)
      has_more? = length(rows) > query.limit

      {:ok,
       %CursorPage{
         items: items,
         limit: query.limit,
         has_more?: has_more?,
         next_cursor: if(has_more?, do: List.last(items))
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_workspace_page(%PageWorkspaces{} = query) do
    if PlatformContext.valid?(query.platform_context) and
         Enum.any?(
           query.platform_context.roles,
           &(&1 in [:platform_reader, :platform_operator, :platform_admin])
         ) and
         is_integer(query.limit) and query.limit in 1..500 and
         (is_nil(query.after) or valid_id?(query.after)) do
      :ok
    else
      {:error, Error.new(:forbidden, "valid platform workspace-read authority required")}
    end
  end

  defp workspace_after(query, nil), do: query

  defp workspace_after(query, cursor),
    do: where(query, [workspace], workspace.workspace_id > ^cursor)

  defp replay_workspace_provision!(command) do
    workspace = Repo.get(Workspace, command.workspace_id)
    runtime_state = Repo.get(WorkspaceRuntimeState, command.workspace_id)

    if match?(%Workspace{}, workspace) and match?(%WorkspaceRuntimeState{}, runtime_state) and
         workspace.slug == command.slug and workspace.display_name == command.display_name and
         workspace.status == "active" do
      :ok
    else
      Repo.rollback(Error.new(:conflict, "workspace identity has different content"))
    end
  end

  @impl true
  def register_manifest(%RegisterManifest{version: %Version{} = version} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         {:ok, verified} <- Version.verify(version),
         :ok <- validate_manifest_identity(verified),
         :ok <- validate_serialization_format(verified),
         {:ok, manifest_json} <- Serializer.encode_manifest(verified.manifest),
         :ok <- validate_manifest_size(manifest_json),
         {:ok, manifest} <- Jason.decode(manifest_json),
         {:ok, atom_strings} <-
           ManifestAtoms.extract(%{
             content_hash: verified.content_hash,
             manifest_index_json: manifest_json
           }),
         {:ok, hash} <- decode_hash(verified.content_hash),
         {:ok, stored} <-
           Repo.transaction(fn ->
             required_refs = Publication.required_package_refs(verified)

             with :ok <- validate_execution_package_refs!(required_refs),
                  {:ok, stored} <-
                    insert_or_replay_manifest(verified, hash, manifest, atom_strings),
                  :ok <- link_manifest_execution_packages!(stored, required_refs) do
               insert_manifest_audit!(command.platform_context, stored)
               stored
             else
               {:error, error} -> Repo.rollback(error)
             end
           end),
         :ok <- ManifestCache.put(stored) do
      {:ok, stored}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "invalid manifest release", details: %{reason: inspect(reason)})}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def register_execution_packages(%RegisterExecutionPackages{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         {:ok, records} <- encode_execution_packages(command.packages),
         {:ok, :ok} <-
           Repo.transaction(fn ->
             with :ok <- insert_execution_packages(records),
                  :ok <- verify_execution_packages(records) do
               insert_execution_package_audit!(command.platform_context, records)
             else
               {:error, error} -> Repo.rollback(error)
             end
           end) do
      :ok
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, :execution_package_batch_too_large} ->
        {:error, Error.new(:limit_exceeded, "execution package batch exceeds the 32 MiB limit")}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "invalid execution packages", details: %{reason: inspect(reason)})}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def missing_execution_package_hashes(%MissingExecutionPackageHashes{} = query) do
    with :ok <- validate_platform_read(query.platform_context),
         {:ok, hashes} <- normalize_package_hashes(query.hashes) do
      present =
        ExecutionPackageRecord
        |> where([package], package.content_hash in ^Enum.map(hashes, &elem(&1, 1)))
        |> select([package], package.content_hash)
        |> Repo.all()
        |> MapSet.new()

      {:ok,
       for(
         {encoded, decoded} <- hashes,
         not MapSet.member?(present, decoded),
         do: encoded
       )}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "invalid execution package hashes",
           details: %{reason: inspect(reason)}
         )}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_execution_package(%GetExecutionPackage{} = query) do
    with :ok <- validate_workspace_package_read(query.workspace_context),
         :ok <- validate_execution_package_query(query),
         {:ok, hash} <- decode_hash(query.content_hash),
         %ExecutionPackageRecord{} = row <- authorized_execution_package(query, hash),
         {:ok, package} <- ExecutionPackage.from_published(row.payload),
         :ok <- validate_stored_package_identity(row, package) do
      {:ok, package}
    else
      nil ->
        {:error, Error.new(:not_found, "execution package not found")}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:internal, "persisted execution package is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp authorized_execution_package(query, hash) do
    {module, name} = query.asset_ref
    target_id = TargetIdentity.for_asset(query.asset_ref)
    workspace_id = query.workspace_context.workspace_id

    ExecutionPackageRecord
    |> join(:inner, [package], link in ManifestExecutionPackage,
      on: link.package_hash == package.content_hash
    )
    |> join(:inner, [_package, link], deployment in WorkspaceDeployment,
      on:
        deployment.workspace_id == ^workspace_id and
          deployment.deployment_id == ^query.deployment_id and
          deployment.manifest_version_id == link.manifest_version_id
    )
    |> join(:inner, [_package, _link, deployment], target in WorkspaceDeploymentTarget,
      on:
        target.workspace_id == deployment.workspace_id and
          target.deployment_id == deployment.deployment_id and target.target_kind == "asset" and
          target.target_id == ^target_id
    )
    |> where(
      [package, link],
      package.content_hash == ^hash and
        link.manifest_version_id == ^query.manifest_version_id and
        link.asset_module == ^Atom.to_string(module) and link.asset_name == ^Atom.to_string(name)
    )
    |> select([package], package)
    |> Repo.one()
  end

  defp validate_execution_package_query(%GetExecutionPackage{} = query) do
    with deployment_id when is_binary(deployment_id) and byte_size(deployment_id) in 1..255 <-
           query.deployment_id,
         manifest_version_id
         when is_binary(manifest_version_id) and byte_size(manifest_version_id) in 1..255 <-
           query.manifest_version_id,
         {module, name} when is_atom(module) and is_atom(name) <- query.asset_ref,
         true <- canonical_hash?(query.content_hash) do
      :ok
    else
      _invalid -> {:error, Error.new(:invalid, "invalid execution package query")}
    end
  end

  defp validate_platform_manifest_write(context) do
    if PlatformContext.valid?(context) and
         Enum.any?(context.roles, &(&1 in [:platform_operator, :platform_admin])) do
      :ok
    else
      {:error, Error.new(:forbidden, "platform manifest write role required")}
    end
  end

  defp insert_manifest_audit!(context, version) do
    now = version.inserted_at || DateTime.utc_now()

    Repo.insert_all(
      AuthPlatformAuditEntry,
      [
        %{
          command_id: "manifest.register:" <> version.manifest_version_id,
          principal_id: context.principal_id,
          action: "manifest.registered",
          subject_kind: "manifest",
          subject_id: version.manifest_version_id,
          detail: %{"content_hash" => version.content_hash},
          occurred_at: now,
          inserted_at: now
        }
      ],
      on_conflict: :nothing,
      conflict_target: [:command_id, :action]
    )

    :ok
  end

  defp insert_execution_package_audit!(_context, []), do: :ok

  defp insert_execution_package_audit!(context, records) do
    fingerprint =
      records
      |> Enum.map(& &1.content_hash)
      |> Enum.sort()
      |> IO.iodata_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    now = DateTime.utc_now()

    Repo.insert_all(
      AuthPlatformAuditEntry,
      [
        %{
          command_id: "execution_packages.register:" <> fingerprint,
          principal_id: context.principal_id,
          action: "execution_packages.registered",
          subject_kind: "execution_package_batch",
          subject_id: fingerprint,
          detail: %{"count" => length(records), "fingerprint" => fingerprint},
          occurred_at: now,
          inserted_at: now
        }
      ],
      on_conflict: :nothing,
      conflict_target: [:command_id, :action]
    )

    :ok
  end

  @impl true
  def get_manifest(%ById{manifest_version_id: id}) when byte_size(id) in 1..255 do
    selector = %ById{manifest_version_id: id}

    case ManifestCache.get(selector) do
      {:ok, version} -> {:ok, version}
      :miss -> selector |> load_manifest() |> cache_manifest()
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  def get_manifest(%ById{}), do: {:error, Error.new(:invalid, "invalid manifest identity")}

  def get_manifest(%ByContentHash{content_hash: content_hash}) do
    selector = %ByContentHash{content_hash: content_hash}

    case ManifestCache.get(selector) do
      {:ok, version} -> {:ok, version}
      :miss -> selector |> load_manifest() |> cache_manifest()
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_deployment_manifest(%GetDeploymentManifest{} = query) do
    context = query.workspace_context

    with true <- WorkspaceContext.valid?(context),
         true <- valid_id?(query.deployment_id) and valid_id?(query.manifest_version_id),
         true <-
           Repo.exists?(
             from(deployment in WorkspaceDeployment,
               where:
                 deployment.workspace_id == ^context.workspace_id and
                   deployment.deployment_id == ^query.deployment_id and
                   deployment.manifest_version_id == ^query.manifest_version_id
             )
           ) do
      get_manifest(%ById{manifest_version_id: query.manifest_version_id})
    else
      false -> {:error, Error.new(:not_found, "workspace deployment manifest not found")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp load_manifest(%ById{manifest_version_id: id}) do
    ManifestVersion
    |> Repo.get(id)
    |> decode_manifest_row()
  end

  defp load_manifest(%ByContentHash{content_hash: content_hash}) do
    case decode_hash(content_hash) do
      {:ok, hash} ->
        ManifestVersion
        |> where([manifest], manifest.content_hash == ^hash)
        |> Repo.one()
        |> decode_manifest_row()

      {:error, _reason} ->
        {:error, Error.new(:invalid, "invalid manifest content hash")}
    end
  end

  defp get_activatable_manifest(manifest_version_id) do
    case Repo.get(ManifestVersion, manifest_version_id) do
      nil ->
        {:error, Error.new(:not_found, "manifest release not found")}

      %ManifestVersion{schema_version: schema_version}
      when schema_version < @current_manifest_schema ->
        {:error,
         Error.new(:invalid, "historical manifest cannot be activated",
           details: %{
             reason: :historical_manifest_not_activatable,
             schema_version: schema_version,
             current_schema_version: @current_manifest_schema
           }
         )}

      %ManifestVersion{} = row ->
        row
        |> decode_manifest_row()
        |> cache_manifest()
    end
  end

  defp cache_manifest({:ok, %Version{} = version} = result) do
    :ok = ManifestCache.put(version)
    result
  end

  defp cache_manifest(error), do: error

  @impl true
  def deploy_manifest(%DeployManifest{} = command) do
    with :ok <- validate_deploy_command(command),
         {:ok, configuration} <- validate_configuration(command.configuration),
         {:ok, manifest} <- get_activatable_manifest(command.manifest_version_id),
         :ok <- validate_targets(command.targets, manifest),
         schedules <- normalize_schedules(command.schedules),
         capacities <- normalize_capacities(command.capacity_scopes),
         {:ok, configuration_fingerprint} <-
           CanonicalJSON.hash(%{
             "configuration" => configuration,
             "schedules" => schedules,
             "capacity_scopes" => capacities
           }),
         targets <- normalize_targets(command.targets),
         {:ok, target_fingerprint} <- CanonicalJSON.hash(targets),
         {:ok, result} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.execute!(
               command.workspace_context.workspace_id,
               command.idempotency,
               fn ->
                 deploy_manifest!(
                   command,
                   configuration,
                   configuration_fingerprint,
                   target_fingerprint,
                   targets,
                   manifest_summary(manifest)
                 )
               end,
               &encode_idempotent_deployment/1,
               &decode_idempotent_deployment/1
             )
           end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_deployment_targets(%GetDeploymentTargets{} = query) do
    with :ok <- validate_deployment_target_query(query) do
      rows =
        WorkspaceDeploymentTarget
        |> where(
          [target],
          target.workspace_id == ^query.workspace_context.workspace_id and
            target.deployment_id == ^query.deployment_id
        )
        |> visible_targets(query.customer_visible_only)
        |> order_by([target], asc: target.target_kind, asc: target.target_id)
        |> Repo.all()

      {:ok,
       Enum.map(rows, fn row ->
         %DeploymentTarget{
           target_kind: String.to_existing_atom(row.target_kind),
           target_id: row.target_id,
           selection_source: String.to_existing_atom(row.selection_source),
           customer_visible: row.customer_visible,
           descriptor: row.descriptor || %{}
         }
       end)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp visible_targets(query, false), do: query
  defp visible_targets(query, true), do: where(query, [target], target.customer_visible)

  defp validate_deployment_target_query(%GetDeploymentTargets{} = query) do
    if WorkspaceContext.valid?(query.workspace_context) and valid_id?(query.deployment_id) and
         is_boolean(query.customer_visible_only) do
      :ok
    else
      {:error, Error.new(:forbidden, "valid workspace deployment context required")}
    end
  end

  defp deploy_manifest!(
         command,
         configuration,
         configuration_fingerprint,
         target_fingerprint,
         targets,
         manifest_summary
       ) do
    locked_runtime_state = lock_runtime_state!(command.workspace_context.workspace_id)

    {deployment, replayed?} =
      insert_or_replay_deployment!(
        command,
        configuration,
        configuration_fingerprint,
        target_fingerprint
      )

    insert_targets!(command, targets)
    insert_schedules!(command)
    sync_capacity_scopes!(command, replayed?)

    if replayed? and locked_runtime_state.active_deployment_id != deployment.deployment_id do
      Repo.rollback(
        Error.new(
          :conflict,
          "deployment command was already committed and is no longer active"
        )
      )
    end

    runtime_state = activate_deployment!(command, deployment)

    unless replayed? do
      OutboxWriter.insert!(%{
        workspace_id: command.workspace_context.workspace_id,
        command_id: "workspace.deploy:" <> command.deployment_id,
        event_kind: "workspace.deployment.activated",
        aggregate_kind: "workspace_deployment",
        aggregate_id: command.deployment_id,
        aggregate_version: runtime_state.revision,
        occurred_at: command.occurred_at,
        payload: %{
          "deployment_id" => command.deployment_id,
          "manifest_version_id" => command.manifest_version_id,
          "runtime_revision" => runtime_state.revision,
          "target_catalog_fingerprint" => Base.encode16(target_fingerprint, case: :lower)
        }
      })
    end

    runtime_result(runtime_state, command.manifest_version_id, manifest_summary)
  end

  @impl true
  def get_runtime_state(%GetRuntimeState{workspace_context: context}) do
    with :ok <- validate_workspace_read(context) do
      query =
        from(state in WorkspaceRuntimeState,
          join: deployment in WorkspaceDeployment,
          on:
            deployment.workspace_id == state.workspace_id and
              deployment.deployment_id == state.active_deployment_id,
          join: manifest in ManifestVersion,
          on: manifest.manifest_version_id == deployment.manifest_version_id,
          where: state.workspace_id == ^context.workspace_id,
          select:
            {state, deployment.manifest_version_id, manifest.content_hash,
             manifest.schema_version, manifest.runner_contract_version, manifest.asset_count,
             manifest.pipeline_count, manifest.schedule_count}
        )

      case Repo.one(query) do
        {%WorkspaceRuntimeState{} = state, manifest_version_id, content_hash, schema_version,
         runner_contract_version, asset_count, pipeline_count, schedule_count} ->
          {:ok,
           runtime_result(state, manifest_version_id, %{
             content_hash: content_hash,
             schema_version: schema_version,
             runner_contract_version: runner_contract_version,
             asset_count: asset_count,
             pipeline_count: pipeline_count,
             schedule_count: schedule_count
           })}

        nil ->
          {:error, Error.new(:not_found, "workspace has no active deployment")}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_platform_provision(%ProvisionWorkspace{platform_context: context} = command) do
    cond do
      not PlatformContext.valid?(context) ->
        {:error, :invalid}

      not Enum.any?(context.roles, &(&1 in [:platform_operator, :platform_admin])) ->
        {:error, :invalid}

      not valid_id?(command.workspace_id) ->
        {:error, :invalid}

      not is_binary(command.slug) or byte_size(command.slug) > 63 ->
        {:error, :invalid}

      not valid_id?(command.display_name) ->
        {:error, :invalid}

      not match?(%DateTime{}, command.occurred_at) ->
        {:error, :invalid}

      true ->
        :ok
    end
  end

  defp validate_deploy_command(%DeployManifest{} = command) do
    with :ok <- validate_deploy_context(command.platform_context, command.workspace_context),
         true <- valid_deploy_identity?(command),
         true <- valid_deploy_collections?(command),
         true <- valid_deploy_contents?(command) do
      :ok
    else
      false -> {:error, :invalid}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp validate_deploy_context(platform_context, workspace_context) do
    if PlatformContext.valid?(platform_context) and
         Enum.any?(platform_context.roles, &(&1 in [:platform_operator, :platform_admin])) and
         WorkspaceContext.valid?(workspace_context) and
         Enum.any?(workspace_context.roles, &(&1 in [:workspace_admin, :platform_operator])) do
      :ok
    else
      {:error, Error.new(:forbidden, "platform and workspace deployment roles required")}
    end
  end

  defp valid_deploy_identity?(command) do
    valid_id?(command.deployment_id) and valid_id?(command.manifest_version_id) and
      is_integer(command.configuration_version) and command.configuration_version >= 1 and
      match?(%DateTime{}, command.occurred_at)
  end

  defp valid_deploy_collections?(command) do
    bounded_list?(command.targets, @max_deployment_targets) and
      bounded_list?(command.schedules, @max_deployment_schedules) and
      bounded_list?(command.capacity_scopes, @max_capacity_scopes)
  end

  defp valid_deploy_contents?(command) do
    valid_schedules?(command.schedules, command.targets) and
      valid_capacities?(command.capacity_scopes) and bounded_target_catalog?(command.targets)
  end

  defp bounded_target_catalog?(targets) do
    targets
    |> Enum.map(& &1.descriptor)
    |> Jason.encode_to_iodata!()
    |> IO.iodata_length()
    |> Kernel.<=(@max_deployment_target_catalog_bytes)
  rescue
    _error -> false
  end

  defp bounded_list?(value, maximum), do: is_list(value) and length(value) <= maximum

  defp validate_serialization_format(%Version{serialization_format: "json-v1"}), do: :ok
  defp validate_serialization_format(_version), do: {:error, :unsupported_serialization_format}

  defp validate_manifest_identity(%Version{} = version) do
    if valid_id?(version.manifest_version_id),
      do: :ok,
      else: {:error, :invalid_manifest_identity}
  end

  defp validate_manifest_size(manifest_json) when byte_size(manifest_json) <= @max_manifest_bytes,
    do: :ok

  defp validate_manifest_size(_manifest_json), do: {:error, :manifest_payload_too_large}

  defp validate_configuration(configuration) do
    case DeploymentConfig.validate(configuration) do
      {:ok, validated} ->
        {:ok, validated}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "deployment configuration is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp insert_or_replay_manifest(version, hash, manifest, atom_strings) do
    row = %{
      manifest_version_id: version.manifest_version_id,
      content_hash: hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      required_runner_release_id: version.required_runner_release_id,
      payload_version: 1,
      asset_count: length(List.wrap(version.manifest.assets)),
      pipeline_count: length(List.wrap(version.manifest.pipelines)),
      schedule_count: length(List.wrap(version.manifest.schedules)),
      atom_strings: atom_strings |> MapSet.to_list() |> Enum.sort(),
      manifest: manifest,
      inserted_at: version.inserted_at || DateTime.utc_now()
    }

    case Repo.insert_all(ManifestVersion, [row], on_conflict: :nothing) do
      {0, _rows} -> manifest_conflict_or_replay(version, hash)
      {1, _rows} -> {:ok, version}
    end
  end

  defp manifest_conflict_or_replay(version, hash) do
    query =
      from(manifest in ManifestVersion,
        where:
          manifest.manifest_version_id == ^version.manifest_version_id or
            manifest.content_hash == ^hash,
        limit: 1
      )

    case Repo.one(query) |> decode_manifest_row() do
      {:ok, %Version{} = stored}
      when stored.manifest_version_id == version.manifest_version_id and
             stored.content_hash == version.content_hash ->
        {:ok, stored}

      {:ok, _stored} ->
        {:error, Error.new(:conflict, "manifest identity has different canonical content")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp decode_manifest_row(nil), do: {:error, Error.new(:not_found, "manifest release not found")}

  defp decode_manifest_row(%ManifestVersion{} = row) do
    manifest_json = Jason.encode!(row.manifest)

    with {:ok, manifest} <- Serializer.decode_manifest(manifest_json),
         {:ok, version} <-
           Version.from_published(manifest,
             manifest_version_id: row.manifest_version_id,
             content_hash: Base.encode16(row.content_hash, case: :lower),
             schema_version: row.schema_version,
             runner_contract_version: row.runner_contract_version,
             required_runner_release_id: row.required_runner_release_id,
             serialization_format: "json-v1",
             inserted_at: row.inserted_at
           ) do
      {:ok, version}
    else
      {:error, reason} ->
        {:error,
         Error.new(:internal, "persisted manifest is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp validate_targets(targets, version) when is_list(targets) do
    allowed = manifest_target_ids(version)

    with true <- Enum.all?(targets, &match?(%DeploymentTarget{}, &1)),
         true <- Enum.all?(targets, &valid_target?/1),
         keys <- Enum.map(targets, &{&1.target_kind, &1.target_id}),
         true <- length(keys) == length(Enum.uniq(keys)),
         true <- Enum.all?(keys, &MapSet.member?(allowed, &1)) do
      :ok
    else
      _value -> {:error, Error.new(:invalid, "deployment target catalog is invalid")}
    end
  end

  defp manifest_target_ids(%Version{manifest: manifest}) do
    assets =
      Enum.map(manifest.assets, fn asset ->
        {:asset, TargetIdentity.for_asset(asset.ref)}
      end)

    pipelines =
      Enum.map(manifest.pipelines, fn pipeline ->
        {:pipeline, TargetIdentity.for_pipeline({pipeline.module, pipeline.name})}
      end)

    MapSet.new(assets ++ pipelines)
  end

  defp valid_target?(%DeploymentTarget{} = target) do
    target.target_kind in [:asset, :pipeline] and valid_id?(target.target_id) and
      target.selection_source in [:common, :explicit, :dependency] and
      is_boolean(target.customer_visible) and is_map(target.descriptor) and
      descriptor_value(target.descriptor, :target_id) == target.target_id and
      valid_descriptor_label?(descriptor_value(target.descriptor, :label)) and
      bounded_json?(target.descriptor, @max_deployment_target_descriptor_bytes)
  end

  defp descriptor_value(descriptor, key),
    do: Map.get(descriptor, key, Map.get(descriptor, Atom.to_string(key)))

  defp valid_descriptor_label?(label),
    do: is_binary(label) and label != "" and byte_size(label) <= 1_024

  defp normalize_targets(targets) do
    targets
    |> Enum.map(fn target ->
      %{
        "target_kind" => Atom.to_string(target.target_kind),
        "target_id" => target.target_id,
        "selection_source" => Atom.to_string(target.selection_source),
        "customer_visible" => target.customer_visible,
        "descriptor" => target.descriptor
      }
    end)
    |> Enum.sort_by(&{&1["target_kind"], &1["target_id"]})
  end

  defp valid_schedules?(schedules, targets) when is_list(schedules) do
    pipeline_ids =
      targets
      |> Enum.filter(&(&1.target_kind == :pipeline))
      |> MapSet.new(& &1.target_id)

    Enum.all?(schedules, fn
      %DeploymentSchedule{} = schedule ->
        valid_id?(schedule.pipeline_target_id) and valid_id?(schedule.schedule_id) and
          valid_id?(schedule.schedule_fingerprint) and is_map(schedule.definition) and
          bounded_json?(schedule.definition, 65_536) and
          match?(%DateTime{}, schedule.next_due_at) and is_map(schedule.cursor) and
          bounded_json?(schedule.cursor, 65_536) and
          MapSet.member?(pipeline_ids, schedule.pipeline_target_id)

      _schedule ->
        false
    end) and
      schedules
      |> Enum.map(&{&1.pipeline_target_id, &1.schedule_id})
      |> then(&(length(&1) == length(Enum.uniq(&1))))
  end

  defp normalize_schedules(schedules) do
    schedules
    |> Enum.map(fn schedule ->
      %{
        "pipeline_target_id" => schedule.pipeline_target_id,
        "schedule_id" => schedule.schedule_id,
        "schedule_fingerprint" => schedule.schedule_fingerprint,
        "definition" => schedule.definition,
        "next_due_at" => DateTime.to_iso8601(schedule.next_due_at),
        "cursor" => schedule.cursor
      }
    end)
    |> Enum.sort_by(&{&1["pipeline_target_id"], &1["schedule_id"]})
  end

  defp valid_capacities?(capacities) when is_list(capacities) do
    Enum.all?(capacities, fn
      %DeploymentCapacityScope{} = capacity ->
        valid_id?(capacity.scope_id) and
          capacity.scope_kind in [:workspace, :pool, :pipeline, :run] and
          valid_id?(capacity.scope_key) and is_integer(capacity.capacity_limit) and
          capacity.capacity_limit > 0

      _capacity ->
        false
    end) and
      capacities
      |> Enum.map(& &1.scope_id)
      |> then(&(length(&1) == length(Enum.uniq(&1))))
  end

  defp normalize_capacities(capacities) do
    capacities
    |> Enum.map(fn capacity ->
      %{
        "scope_id" => capacity.scope_id,
        "scope_kind" => Atom.to_string(capacity.scope_kind),
        "scope_key" => capacity.scope_key,
        "capacity_limit" => capacity.capacity_limit
      }
    end)
    |> Enum.sort_by(& &1["scope_id"])
  end

  defp lock_runtime_state!(workspace_id) do
    query =
      from(state in WorkspaceRuntimeState,
        where: state.workspace_id == ^workspace_id,
        lock: "FOR UPDATE"
      )

    Repo.one!(query)
  end

  defp insert_or_replay_deployment!(command, configuration, config_hash, target_hash) do
    attrs = %{
      workspace_id: command.workspace_context.workspace_id,
      deployment_id: command.deployment_id,
      manifest_version_id: command.manifest_version_id,
      configuration: configuration,
      configuration_fingerprint: config_hash,
      target_catalog_fingerprint: target_hash,
      configuration_version: command.configuration_version,
      deployed_by_actor_id: command.workspace_context.principal_id,
      inserted_at: command.occurred_at
    }

    case Repo.insert_all(WorkspaceDeployment, [attrs], on_conflict: :nothing) do
      {0, _rows} ->
        existing =
          Repo.get_by!(WorkspaceDeployment,
            workspace_id: command.workspace_context.workspace_id,
            deployment_id: command.deployment_id
          )

        if existing.manifest_version_id == command.manifest_version_id and
             existing.configuration_version == command.configuration_version and
             existing.configuration_fingerprint == config_hash and
             existing.target_catalog_fingerprint == target_hash do
          {existing, true}
        else
          Repo.rollback(Error.new(:conflict, "deployment identity has different content"))
        end

      {1, _rows} ->
        {struct!(WorkspaceDeployment, attrs), false}
    end
  end

  defp insert_targets!(command, targets) do
    rows =
      Enum.map(targets, fn target ->
        %{
          workspace_id: command.workspace_context.workspace_id,
          deployment_id: command.deployment_id,
          target_kind: target["target_kind"],
          target_id: target["target_id"],
          selection_source: target["selection_source"],
          customer_visible: target["customer_visible"],
          descriptor: target["descriptor"],
          inserted_at: command.occurred_at
        }
      end)

    Enum.each(Enum.chunk_every(rows, @bulk_insert_size), fn chunk ->
      {_count, _rows} =
        Repo.insert_all(WorkspaceDeploymentTarget, chunk,
          on_conflict: :nothing,
          conflict_target: [:workspace_id, :deployment_id, :target_kind, :target_id]
        )
    end)

    stored_count =
      from(target in WorkspaceDeploymentTarget,
        where:
          target.workspace_id == ^command.workspace_context.workspace_id and
            target.deployment_id == ^command.deployment_id,
        select: count()
      )
      |> Repo.one()

    if stored_count != length(rows) do
      Repo.rollback(
        Error.new(:conflict, "deployment target catalog conflicts with committed state")
      )
    end
  end

  defp insert_schedules!(%DeployManifest{schedules: []}), do: :ok

  defp insert_schedules!(command) do
    rows =
      Enum.map(command.schedules, fn schedule ->
        %{
          workspace_id: command.workspace_context.workspace_id,
          deployment_id: command.deployment_id,
          target_kind: "pipeline",
          pipeline_target_id: schedule.pipeline_target_id,
          schedule_id: schedule.schedule_id,
          schedule_fingerprint: schedule.schedule_fingerprint,
          definition: schedule.definition,
          next_due_at: database_datetime(schedule.next_due_at),
          cursor: schedule.cursor,
          version: 1,
          claim_generation: 0,
          updated_at: command.occurred_at
        }
      end)

    Enum.each(Enum.chunk_every(rows, @bulk_insert_size), fn chunk ->
      {_count, _rows} =
        Repo.insert_all(ScheduleCursor, chunk,
          on_conflict: :nothing,
          conflict_target: [:workspace_id, :deployment_id, :pipeline_target_id, :schedule_id]
        )
    end)

    stored_count =
      from(cursor in ScheduleCursor,
        where:
          cursor.workspace_id == ^command.workspace_context.workspace_id and
            cursor.deployment_id == ^command.deployment_id,
        select: count()
      )
      |> Repo.one()

    if stored_count != length(rows) do
      Repo.rollback(
        Error.new(:conflict, "deployment schedule catalog conflicts with committed state")
      )
    end
  end

  defp sync_capacity_scopes!(%DeployManifest{capacity_scopes: []}, _replayed?), do: :ok
  defp sync_capacity_scopes!(_command, true), do: :ok

  defp sync_capacity_scopes!(command, false) do
    scope_ids = Enum.map(command.capacity_scopes, & &1.scope_id)
    scope_kinds = Enum.map(command.capacity_scopes, &Atom.to_string(&1.scope_kind))
    scope_keys = Enum.map(command.capacity_scopes, & &1.scope_key)
    limits = Enum.map(command.capacity_scopes, & &1.capacity_limit)

    %{num_rows: count} =
      SQL.query!(
        Repo,
        """
        WITH incoming AS (
          SELECT *
          FROM unnest($2::text[], $3::text[], $4::text[], $5::integer[])
            AS scope(scope_id, scope_kind, scope_key, capacity_limit)
        )
        INSERT INTO favn_control.capacity_scopes
          (scope_id, workspace_id, scope_kind, scope_key, capacity_limit,
           active_count, version, inserted_at, updated_at)
        SELECT incoming.scope_id, $1, incoming.scope_kind, incoming.scope_key,
               incoming.capacity_limit, 0, 1, $6, $6
        FROM incoming
        ON CONFLICT (scope_id) DO UPDATE
        SET capacity_limit = EXCLUDED.capacity_limit,
            version = capacity_scopes.version + 1,
            updated_at = EXCLUDED.updated_at
        WHERE capacity_scopes.workspace_id = EXCLUDED.workspace_id
          AND capacity_scopes.scope_kind = EXCLUDED.scope_kind
          AND capacity_scopes.scope_key = EXCLUDED.scope_key
          AND capacity_scopes.active_count <= EXCLUDED.capacity_limit
        """,
        [
          command.workspace_context.workspace_id,
          scope_ids,
          scope_kinds,
          scope_keys,
          limits,
          command.occurred_at
        ]
      )

    if count != length(command.capacity_scopes) do
      Repo.rollback(Error.new(:conflict, "capacity scope ownership or active count conflicts"))
    end
  end

  defp activate_deployment!(command, deployment) do
    state = Repo.get!(WorkspaceRuntimeState, command.workspace_context.workspace_id)

    if state.active_deployment_id == deployment.deployment_id do
      state
    else
      state
      |> Ecto.Changeset.change(%{
        active_deployment_id: deployment.deployment_id,
        revision: state.revision + 1,
        activated_by_actor_id: command.workspace_context.principal_id,
        activated_at: command.occurred_at,
        updated_at: command.occurred_at
      })
      |> Repo.update!()
    end
  end

  defp runtime_result(state, manifest_version_id, manifest_summary) do
    %RuntimeState{
      workspace_id: state.workspace_id,
      deployment_id: state.active_deployment_id,
      manifest_version_id: manifest_version_id,
      revision: state.revision,
      activated_at: state.activated_at,
      manifest_content_hash: manifest_summary_value(manifest_summary, :content_hash),
      schema_version: manifest_summary_value(manifest_summary, :schema_version),
      runner_contract_version: manifest_summary_value(manifest_summary, :runner_contract_version),
      asset_count: manifest_summary_value(manifest_summary, :asset_count),
      pipeline_count: manifest_summary_value(manifest_summary, :pipeline_count),
      schedule_count: manifest_summary_value(manifest_summary, :schedule_count)
    }
  end

  defp manifest_summary_value(nil, _key), do: nil

  defp manifest_summary_value(summary, :content_hash) do
    case Map.fetch!(summary, :content_hash) do
      hash when is_binary(hash) and byte_size(hash) == 32 -> Base.encode16(hash, case: :lower)
      hash when is_binary(hash) -> hash
    end
  end

  defp manifest_summary_value(summary, key), do: Map.fetch!(summary, key)

  defp manifest_summary(%Version{} = version) do
    %{
      content_hash: version.content_hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      asset_count: length(List.wrap(version.manifest.assets)),
      pipeline_count: length(List.wrap(version.manifest.pipelines)),
      schedule_count: length(List.wrap(version.manifest.schedules))
    }
  end

  defp encode_idempotent_deployment(%RuntimeState{} = result) do
    {:ok,
     %{
       response: %{
         "workspace_id" => result.workspace_id,
         "deployment_id" => result.deployment_id,
         "manifest_version_id" => result.manifest_version_id,
         "revision" => result.revision,
         "activated_at" => result.activated_at && DateTime.to_iso8601(result.activated_at),
         "manifest_content_hash" => result.manifest_content_hash,
         "schema_version" => result.schema_version,
         "runner_contract_version" => result.runner_contract_version,
         "asset_count" => result.asset_count,
         "pipeline_count" => result.pipeline_count,
         "schedule_count" => result.schedule_count
       },
       response_status: 200,
       resource_kind: "workspace_deployment",
       resource_id: result.deployment_id
     }}
  end

  defp decode_idempotent_deployment(%{response: response}) when is_map(response) do
    with {:ok, activated_at} <- decode_optional_datetime(Map.get(response, "activated_at")),
         workspace_id when is_binary(workspace_id) <- Map.get(response, "workspace_id"),
         deployment_id when is_binary(deployment_id) <- Map.get(response, "deployment_id"),
         manifest_version_id when is_binary(manifest_version_id) <-
           Map.get(response, "manifest_version_id"),
         revision when is_integer(revision) and revision >= 0 <- Map.get(response, "revision") do
      {:ok,
       %RuntimeState{
         workspace_id: workspace_id,
         deployment_id: deployment_id,
         manifest_version_id: manifest_version_id,
         revision: revision,
         activated_at: activated_at,
         manifest_content_hash: Map.get(response, "manifest_content_hash"),
         schema_version: Map.get(response, "schema_version"),
         runner_contract_version: Map.get(response, "runner_contract_version"),
         asset_count: Map.get(response, "asset_count"),
         pipeline_count: Map.get(response, "pipeline_count"),
         schedule_count: Map.get(response, "schedule_count")
       }}
    else
      _other -> {:error, Error.new(:internal, "idempotent deployment replay record is invalid")}
    end
  end

  defp decode_idempotent_deployment(_encoded),
    do: {:error, Error.new(:internal, "idempotent deployment replay record is invalid")}

  defp decode_optional_datetime(nil), do: {:ok, nil}

  defp decode_optional_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _error -> {:error, :invalid_datetime}
    end
  end

  defp decode_optional_datetime(_value), do: {:error, :invalid_datetime}

  defp bounded_json?(value, maximum) do
    case CanonicalJSON.encode(value) do
      {:ok, encoded} -> byte_size(encoded) <= maximum
      {:error, _reason} -> false
    end
  end

  defp encode_execution_packages(packages)
       when is_list(packages) and length(packages) <= @max_execution_packages_per_command do
    packages
    |> Enum.reduce_while({:ok, [], MapSet.new(), 0}, fn package,
                                                        {:ok, records, hashes, total_bytes} ->
      with %ExecutionPackage{} <- package,
           {:ok, canonical} <- ExecutionPackage.verify(package),
           false <- MapSet.member?(hashes, canonical.content_hash),
           {:ok, encoded} <- Serializer.encode_manifest(canonical),
           :ok <- validate_execution_package_size(encoded, total_bytes),
           {:ok, payload} <- Jason.decode(encoded),
           {:ok, hash} <- decode_hash(canonical.content_hash) do
        {module, name} = canonical.asset_ref

        record = %{
          content_hash: hash,
          asset_module: Atom.to_string(module),
          asset_name: Atom.to_string(name),
          runtime_input_resolver: runtime_input_resolver(canonical),
          payload: payload,
          inserted_at: DateTime.utc_now()
        }

        {:cont,
         {:ok, [record | records], MapSet.put(hashes, canonical.content_hash),
          total_bytes + byte_size(encoded)}}
      else
        true -> {:halt, {:error, :duplicate_execution_package}}
        {:error, reason} -> {:halt, {:error, reason}}
        _invalid -> {:halt, {:error, :invalid_execution_package}}
      end
    end)
    |> case do
      {:ok, records, _hashes, _total_bytes} -> {:ok, Enum.reverse(records)}
      {:error, _reason} = error -> error
    end
  end

  defp encode_execution_packages(_packages), do: {:error, :too_many_execution_packages}

  defp validate_execution_package_size(encoded, total_bytes) do
    cond do
      byte_size(encoded) > @max_execution_package_bytes ->
        {:error, :execution_package_too_large}

      total_bytes + byte_size(encoded) > @max_execution_package_batch_bytes ->
        {:error, :execution_package_batch_too_large}

      true ->
        :ok
    end
  end

  defp runtime_input_resolver(%ExecutionPackage{
         sql_execution: %{runtime_inputs: %{module: module}}
       })
       when is_atom(module),
       do: Atom.to_string(module)

  defp runtime_input_resolver(%ExecutionPackage{}), do: nil

  defp insert_execution_packages(records) do
    records
    |> Enum.chunk_every(@execution_package_insert_size)
    |> Enum.each(&Repo.insert_all(ExecutionPackageRecord, &1, on_conflict: :nothing))

    :ok
  end

  defp verify_execution_packages([]), do: :ok

  defp verify_execution_packages(records) do
    hashes = Enum.map(records, & &1.content_hash)

    stored =
      ExecutionPackageRecord
      |> where([package], package.content_hash in ^hashes)
      |> Repo.all()
      |> Map.new(&{&1.content_hash, &1})

    if Enum.all?(records, fn record ->
         case Map.get(stored, record.content_hash) do
           %ExecutionPackageRecord{} = row ->
             row.asset_module == record.asset_module and row.asset_name == record.asset_name and
               row.runtime_input_resolver == record.runtime_input_resolver and
               row.payload == record.payload

           nil ->
             false
         end
       end) do
      :ok
    else
      {:error, Error.new(:conflict, "execution package has different canonical content")}
    end
  end

  defp normalize_package_hashes(hashes)
       when is_list(hashes) and length(hashes) <= @max_execution_packages_per_command do
    hashes
    |> Enum.reduce_while({:ok, [], MapSet.new()}, fn hash, {:ok, acc, seen} ->
      with true <- canonical_hash?(hash),
           {:ok, decoded} <- decode_hash(hash) do
        if MapSet.member?(seen, hash) do
          {:cont, {:ok, acc, seen}}
        else
          {:cont, {:ok, [{hash, decoded} | acc], MapSet.put(seen, hash)}}
        end
      else
        _invalid -> {:halt, {:error, :invalid_execution_package_hash}}
      end
    end)
    |> case do
      {:ok, normalized, _seen} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_package_hashes(_hashes), do: {:error, :too_many_execution_package_hashes}

  defp validate_execution_package_refs!([]), do: :ok

  defp validate_execution_package_refs!(refs) do
    decoded =
      Enum.reduce_while(refs, {:ok, []}, fn {hash, ref}, {:ok, acc} ->
        case decode_hash(hash) do
          {:ok, bytes} -> {:cont, {:ok, [{bytes, ref, hash} | acc]}}
          :error -> {:halt, {:error, Error.new(:invalid, "manifest package hash is invalid")}}
        end
      end)

    with {:ok, decoded} <- decoded do
      rows =
        decoded
        |> Enum.map(&elem(&1, 0))
        |> Enum.chunk_every(@execution_package_validation_batch_size)
        |> Enum.flat_map(fn hashes ->
          ExecutionPackageRecord
          |> where([package], package.content_hash in ^hashes)
          |> select(
            [package],
            {package.content_hash, package.asset_module, package.asset_name}
          )
          |> lock("FOR KEY SHARE")
          |> Repo.all()
        end)
        |> Map.new(fn {hash, module, name} -> {hash, {module, name}} end)

      Enum.reduce_while(decoded, :ok, fn {hash, {module, name} = expected, encoded}, :ok ->
        case Map.get(rows, hash) do
          nil ->
            {:halt,
             {:error,
              Error.new(:invalid, "manifest references missing execution packages",
                details: %{reason: :missing_execution_packages, hashes: [encoded]}
              )}}

          {stored_module, stored_name} ->
            if stored_module == Atom.to_string(module) and stored_name == Atom.to_string(name) do
              {:cont, :ok}
            else
              {:halt,
               {:error,
                Error.new(:invalid, "execution package asset does not match manifest",
                  details: %{
                    reason: :execution_package_asset_mismatch,
                    hash: encoded,
                    expected: inspect(expected)
                  }
                )}}
            end
        end
      end)
    end
  end

  defp link_manifest_execution_packages!(_version, []), do: :ok

  defp link_manifest_execution_packages!(version, refs) do
    rows =
      Enum.map(refs, fn {hash, {module, name}} ->
        {:ok, decoded} = decode_hash(hash)

        %{
          manifest_version_id: version.manifest_version_id,
          package_hash: decoded,
          asset_module: Atom.to_string(module),
          asset_name: Atom.to_string(name)
        }
      end)

    rows
    |> Enum.chunk_every(@bulk_insert_size)
    |> Enum.each(&Repo.insert_all(ManifestExecutionPackage, &1, on_conflict: :nothing))

    linked_at = DateTime.utc_now()

    rows
    |> Enum.map(& &1.package_hash)
    |> Enum.chunk_every(@bulk_insert_size)
    |> Enum.each(fn hashes ->
      ExecutionPackageRecord
      |> where(
        [package],
        package.content_hash in ^hashes and is_nil(package.first_linked_at)
      )
      |> Repo.update_all(set: [first_linked_at: linked_at])
    end)

    persisted_count =
      ManifestExecutionPackage
      |> where([link], link.manifest_version_id == ^version.manifest_version_id)
      |> Repo.aggregate(:count)

    if persisted_count == length(rows) do
      :ok
    else
      {:error, Error.new(:conflict, "manifest execution-package links differ from content")}
    end
  end

  defp validate_stored_package_identity(row, package) do
    {module, name} = package.asset_ref
    {:ok, hash} = decode_hash(package.content_hash)

    if row.content_hash == hash and row.asset_module == Atom.to_string(module) and
         row.asset_name == Atom.to_string(name) do
      :ok
    else
      {:error, :execution_package_identity_mismatch}
    end
  end

  defp validate_platform_read(%PlatformContext{} = context) do
    if PlatformContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:platform_reader, :platform_operator, :platform_admin])
         ) do
      :ok
    else
      {:error, Error.new(:forbidden, "platform execution-package read role required")}
    end
  end

  defp validate_platform_read(_context),
    do: {:error, Error.new(:forbidden, "platform execution-package read role required")}

  defp validate_workspace_package_read(%WorkspaceContext{} = context) do
    validate_workspace_read(context)
  end

  defp validate_workspace_package_read(_context),
    do: {:error, Error.new(:forbidden, "valid workspace context required")}

  defp validate_workspace_read(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
         ),
       do: :ok,
       else: {:error, Error.new(:forbidden, "workspace read role required")}
  end

  defp validate_workspace_read(_context),
    do: {:error, Error.new(:forbidden, "workspace read role required")}

  defp database_datetime(%DateTime{} = datetime),
    do: DateTime.add(datetime, 0, :microsecond)

  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255

  defp decode_hash(hash) when is_binary(hash) and byte_size(hash) == 64 do
    if canonical_hash?(hash), do: Base.decode16(hash, case: :lower), else: :error
  end

  defp decode_hash(_hash), do: {:error, :invalid_content_hash}

  defp canonical_hash?(hash) when is_binary(hash), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)
  defp canonical_hash?(_hash), do: false

  defp changeset_error(changeset) do
    if changeset.errors[:slug] do
      Error.new(:conflict, "workspace slug already exists")
    else
      Error.new(:invalid, "workspace attributes are invalid")
    end
  end
end
