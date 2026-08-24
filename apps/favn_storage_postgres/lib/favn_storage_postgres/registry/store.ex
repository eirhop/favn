defmodule FavnStoragePostgres.Registry.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RegistryStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.Connection.CircuitPolicySet
  alias Favn.Manifest.Compatibility
  alias Favn.ExecutionPool.PolicySet
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestActivationDiagnostics
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.ConnectionCircuitPolicy
  alias FavnOrchestrator.ExecutionPoolPolicy
  alias FavnOrchestrator.Persistence.CapacityConfiguration
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.AbandonManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.BeginManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility
  alias FavnOrchestrator.Persistence.Commands.HeartbeatManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.RenewManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestDeploymentClaim
  alias FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetActiveDeploymentConfiguration
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentManifest
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.GetManifestTargetDescriptors
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaces
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Results.ManifestDeployment
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
  alias FavnStoragePostgres.Schemas.AssetEvidenceBinding
  alias FavnStoragePostgres.Schemas.AssetTargetBinding
  alias FavnStoragePostgres.Schemas.ExecutionPackage, as: ExecutionPackageRecord
  alias FavnStoragePostgres.Schemas.ManifestExecutionPackage
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.ManifestDeploymentOperation
  alias FavnStoragePostgres.Schemas.ManifestDeploymentUploadLease
  alias FavnStoragePostgres.Schemas.ManifestActivationLease
  alias FavnStoragePostgres.Schemas.ResourceCircuit
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
  @max_manifest_target_descriptors 500
  @max_deployment_targets 10_000
  @max_deployment_target_descriptor_bytes 256 * 1_024
  @max_deployment_target_catalog_bytes 32 * 1_024 * 1_024
  @max_deployment_schedules 2_000
  @max_capacity_scopes 1_000
  @max_activation_execution_pool_diagnostics 100
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
  def get_manifest_target_descriptors(%GetManifestTargetDescriptors{} = query) do
    with :ok <- validate_platform_manifest_read(query.platform_context),
         :ok <- validate_manifest_target_descriptor_query(query),
         {:ok, rows} <- select_manifest_target_descriptors(query),
         {:ok, descriptors} <- decode_manifest_target_descriptors(rows, query.target_ids) do
      {:ok, descriptors}
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

  @impl true
  def get_deployment_configuration(%GetDeploymentConfiguration{} = query) do
    context = query.workspace_context

    with true <- WorkspaceContext.valid?(context),
         true <- valid_id?(query.deployment_id),
         %WorkspaceDeployment{configuration: configuration} <-
           Repo.get_by(WorkspaceDeployment,
             workspace_id: context.workspace_id,
             deployment_id: query.deployment_id
           ) do
      {:ok, configuration || %{}}
    else
      false -> {:error, Error.new(:forbidden, "valid workspace deployment context required")}
      nil -> {:error, Error.new(:not_found, "workspace deployment configuration not found")}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_active_deployment_configuration(%GetActiveDeploymentConfiguration{} = query) do
    context = query.workspace_context

    with true <- WorkspaceContext.valid?(context),
         {deployment_id, configuration} when is_binary(deployment_id) and is_map(configuration) <-
           from(state in WorkspaceRuntimeState,
             join: deployment in WorkspaceDeployment,
             on:
               deployment.workspace_id == state.workspace_id and
                 deployment.deployment_id == state.active_deployment_id,
             where: state.workspace_id == ^context.workspace_id,
             select: {deployment.deployment_id, deployment.configuration}
           )
           |> Repo.one() do
      {:ok, {deployment_id, configuration}}
    else
      false -> {:error, Error.new(:forbidden, "valid workspace deployment context required")}
      nil -> {:error, Error.new(:not_found, "workspace has no active deployment")}
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

  defp select_manifest_target_descriptors(query) do
    case SQL.query(
           Repo,
           """
           SELECT selected.descriptor
           FROM favn_control.manifest_versions AS manifest
           LEFT JOIN LATERAL (
             SELECT asset -> 'target_descriptor' AS descriptor
             FROM jsonb_array_elements(
               COALESCE(manifest.manifest -> 'assets', '[]'::jsonb)
             ) AS asset
             WHERE asset -> 'target_descriptor' ->> 'target_id' = ANY($2::text[])
           ) AS selected ON TRUE
           WHERE manifest.manifest_version_id = $1
           ORDER BY selected.descriptor ->> 'target_id'
           """,
           [query.manifest_version_id, query.target_ids]
         ) do
      {:ok, %{rows: []}} ->
        {:error, Error.new(:not_found, "manifest release not found")}

      {:ok, %{rows: rows}} ->
        {:ok, rows}

      {:error, error} ->
        {:error, ErrorMapper.map(error)}
    end
  end

  defp decode_manifest_target_descriptors(rows, target_ids) do
    allowed = MapSet.new(target_ids)

    rows
    |> Enum.reduce_while({:ok, %{}}, fn
      [nil], {:ok, descriptors} ->
        {:cont, {:ok, descriptors}}

      [value], {:ok, descriptors} ->
        case TargetDescriptor.from_value(value) do
          {:ok, %TargetDescriptor{} = descriptor} ->
            cond do
              not MapSet.member?(allowed, descriptor.target_id) ->
                {:halt, invalid_persisted_manifest_target_descriptor()}

              Map.has_key?(descriptors, descriptor.target_id) ->
                {:halt, invalid_persisted_manifest_target_descriptor()}

              true ->
                {:cont, {:ok, Map.put(descriptors, descriptor.target_id, descriptor)}}
            end

          _invalid ->
            {:halt, invalid_persisted_manifest_target_descriptor()}
        end
    end)
    |> then(fn
      {:ok, descriptors} ->
        {:ok, descriptors |> Map.values() |> Enum.sort_by(& &1.target_id)}

      error ->
        error
    end)
  end

  defp invalid_persisted_manifest_target_descriptor do
    {:error,
     Error.new(:internal, "persisted manifest target descriptor is invalid",
       details: %{reason: :invalid_target_descriptor}
     )}
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
  def begin_manifest_deployment(%BeginManifestDeployment{} = query) do
    with :ok <- validate_deployment_begin_query(query),
         {:ok, result} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.prepare!(
               query.workspace_context.workspace_id,
               query.idempotency,
               &decode_idempotent_deployment/1
             )
           end) do
      case result do
        {:new, generation} ->
          {:ok,
           {:new,
            %CommandIdempotency{
              query.idempotency
              | reservation_generation: generation
            }}}

        {:replay, %RuntimeState{} = runtime} ->
          {:ok, {:replay, runtime}}
      end
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def heartbeat_manifest_deployment(%HeartbeatManifestDeployment{} = command) do
    with :ok <- validate_deployment_reservation_command(command),
         {:ok, :ok} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.heartbeat!(
               command.workspace_context.workspace_id,
               command.idempotency
             )
           end) do
      :ok
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def abandon_manifest_deployment(%AbandonManifestDeployment{} = command) do
    with :ok <- validate_deployment_reservation_command(command),
         {:ok, :ok} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.abandon!(
               command.workspace_context.workspace_id,
               command.idempotency
             )
           end) do
      :ok
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def deploy_manifest(%DeployManifest{} = command) do
    with :ok <- validate_deploy_command(command),
         {:ok, configuration} <- validate_configuration(command.configuration),
         {:ok, manifest} <- get_activatable_manifest(command.manifest_version_id),
         :ok <- validate_execution_pool_deployment(command, configuration, manifest),
         :ok <- validate_connection_circuit_deployment(configuration, manifest),
         :ok <- validate_targets(command.targets, manifest),
         :ok <-
           validate_target_compatibilities(
             command.target_compatibilities,
             command.targets,
             manifest
           ),
         {:ok, evidence_bindings} <- evidence_bindings(command.targets, manifest),
         schedules <- normalize_schedules(command.schedules),
         capacities <- normalize_capacities(command.capacity_scopes),
         {:ok, configuration_fingerprint} <-
           CanonicalJSON.hash(%{
             "configuration" => configuration,
             "schedules" => schedules,
             "capacity_scopes" => capacities
           }),
         targets <- normalize_targets(command.targets),
         target_compatibilities <-
           normalize_target_compatibilities(command.target_compatibilities),
         {:ok, target_fingerprint} <-
           CanonicalJSON.hash(%{
             "targets" => targets,
             "target_compatibilities" =>
               Enum.map(target_compatibilities, &Map.delete(&1, "expected_binding_version"))
           }),
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
                   target_compatibilities,
                   evidence_bindings,
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

  defp validate_deployment_begin_query(%BeginManifestDeployment{
         workspace_context: workspace_context,
         idempotency: %CommandIdempotency{} = idempotency
       }) do
    if WorkspaceContext.valid?(workspace_context) and idempotency.operation == "manifest.activate" do
      :ok
    else
      {:error, Error.new(:forbidden, "valid workspace deployment replay context required")}
    end
  end

  defp validate_deployment_begin_query(_query),
    do: {:error, Error.new(:forbidden, "valid workspace deployment replay context required")}

  defp validate_deployment_reservation_command(%{
         workspace_context: %WorkspaceContext{} = workspace_context,
         idempotency: %CommandIdempotency{reservation_generation: generation} = idempotency
       })
       when is_integer(generation) and generation > 0 do
    if WorkspaceContext.valid?(workspace_context) and idempotency.operation == "manifest.activate" do
      :ok
    else
      {:error, Error.new(:forbidden, "valid workspace deployment reservation required")}
    end
  end

  defp validate_deployment_reservation_command(_command),
    do: {:error, Error.new(:forbidden, "valid workspace deployment reservation required")}

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
         target_compatibilities,
         evidence_bindings,
         manifest_summary
       ) do
    verify_manifest_activation_lease!(command)
    locked_runtime_state = lock_runtime_state!(command.workspace_context.workspace_id)
    verify_expected_active_deployment!(command, locked_runtime_state)

    {deployment, deployment_status} =
      insert_or_replay_deployment!(
        command,
        configuration,
        configuration_fingerprint,
        target_fingerprint
      )

    unless deployment_status == :content_reuse do
      insert_targets!(command, targets)
      insert_schedules!(command)
    end

    sync_capacity_scopes!(command, deployment_status == :exact_replay)
    sync_execution_pool_circuits!(command, configuration, deployment_status == :exact_replay)

    if deployment_status == :exact_replay and
         locked_runtime_state.active_deployment_id != deployment.deployment_id do
      Repo.rollback(
        Error.new(
          :conflict,
          "deployment command was already committed and is no longer active"
        )
      )
    end

    persist_evidence_bindings!(command, evidence_bindings)
    persist_target_compatibilities!(command, target_compatibilities)

    runtime_state = activate_deployment!(command, deployment)

    unless deployment_status == :exact_replay do
      OutboxWriter.insert!(%{
        workspace_id: command.workspace_context.workspace_id,
        command_id: "workspace.deploy:" <> command.deployment_id,
        event_kind: "workspace.deployment.activated",
        aggregate_kind: "workspace_deployment",
        aggregate_id: deployment.deployment_id,
        aggregate_version: runtime_state.revision,
        occurred_at: command.occurred_at,
        payload: %{
          "deployment_id" => deployment.deployment_id,
          "manifest_version_id" => command.manifest_version_id,
          "runtime_revision" => runtime_state.revision,
          "target_catalog_fingerprint" => Base.encode16(target_fingerprint, case: :lower),
          "activation_diagnostics" =>
            ManifestActivationDiagnostics.to_map(command.activation_diagnostics)
        }
      })
    end

    runtime_result(
      runtime_state,
      command.manifest_version_id,
      manifest_summary,
      command.activation_diagnostics,
      execution_pool_diagnostics!(configuration)
    )
  end

  defp verify_manifest_activation_lease!(%DeployManifest{
         activation_lease: %{owner: owner, fence: fence},
         workspace_context: workspace_context,
         deployment_id: deployment_id
       }) do
    case Repo.get(ManifestActivationLease, workspace_context.workspace_id) do
      %ManifestActivationLease{
        operation_id: ^deployment_id,
        owner: ^owner,
        fencing_token: ^fence,
        expires_at: expires_at
      } ->
        if DateTime.compare(expires_at, DateTime.utc_now()) == :gt do
          :ok
        else
          Repo.rollback(Error.new(:conflict, "manifest activation lease expired"))
        end

      _missing_or_stale ->
        Repo.rollback(Error.new(:conflict, "manifest activation lease was lost"))
    end
  end

  defp verify_manifest_activation_lease!(%DeployManifest{activation_lease: nil}), do: :ok

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
             manifest.schema_version, manifest.runner_contract_version, manifest.manifest,
             manifest.runner_releases, manifest.asset_count, manifest.pipeline_count,
             manifest.schedule_count}
        )

      case Repo.one(query) do
        {%WorkspaceRuntimeState{} = state, manifest_version_id, content_hash, schema_version,
         runner_contract_version, _manifest_payload, runner_releases, asset_count, pipeline_count,
         schedule_count} ->
          {:ok,
           runtime_result(
             state,
             manifest_version_id,
             %{
               content_hash: content_hash,
               schema_version: schema_version,
               runner_contract_version: runner_contract_version,
               runner_releases: runner_releases,
               asset_count: asset_count,
               pipeline_count: pipeline_count,
               schedule_count: schedule_count
             },
             nil,
             %{items: [], count: 0, truncated: false}
           )}

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
      match?(%DateTime{}, command.occurred_at) and
      (command.expected_active_deployment_id in [nil, :unchecked] or
         valid_id?(command.expected_active_deployment_id))
  end

  defp verify_expected_active_deployment!(
         %DeployManifest{expected_active_deployment_id: :unchecked},
         _runtime_state
       ),
       do: :ok

  defp verify_expected_active_deployment!(command, runtime_state) do
    if runtime_state.active_deployment_id == command.expected_active_deployment_id do
      :ok
    else
      Repo.rollback(
        Error.new(:conflict, "active deployment changed while activation was planned",
          retryable?: true,
          details: %{reason: :active_deployment_changed}
        )
      )
    end
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

  defp validate_execution_pool_deployment(command, configuration, %Version{} = version) do
    defaults = version.manifest.execution_pools
    policy_configuration = config_value(configuration, "execution_pool_policy", nil)

    cond do
      is_nil(policy_configuration) and map_size(defaults) == 0 ->
        :ok

      not is_map(policy_configuration) ->
        {:error, Error.new(:invalid, "execution-pool deployment policy is missing")}

      true ->
        validate_execution_pool_deployment_policy(command, policy_configuration, defaults)
    end
  end

  defp validate_execution_pool_deployment_policy(command, policy_configuration, defaults) do
    with {:ok, defaults} <- PolicySet.new(defaults),
         {:ok, overrides} <-
           policy_configuration
           |> config_value("operator_overrides", %{})
           |> PolicySet.new(),
         true <- Enum.all?(Map.keys(overrides), &Map.has_key?(defaults, &1)),
         {:ok, orphaned} <-
           policy_configuration
           |> config_value("orphaned_overrides", %{})
           |> PolicySet.new(),
         true <- Enum.all?(Map.keys(orphaned), &(not Map.has_key?(defaults, &1))),
         {:ok, effective} <-
           ExecutionPoolPolicy.effective(%{"execution_pool_policy" => policy_configuration}),
         true <- effective == Map.merge(defaults, overrides),
         true <-
           config_value(policy_configuration, "manifest_fingerprint", nil) ==
             PolicySet.fingerprint(defaults),
         true <- capacity_scopes_match?(command, effective) do
      :ok
    else
      false ->
        {:error, Error.new(:invalid, "execution-pool deployment policy is inconsistent")}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "execution-pool deployment policy is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp validate_connection_circuit_deployment(configuration, %Version{} = version) do
    with {:ok, defaults} <- CircuitPolicySet.new(version.manifest.connection_circuits),
         {:ok, effective} <- ConnectionCircuitPolicy.effective(configuration),
         true <- effective == defaults do
      :ok
    else
      false ->
        {:error, Error.new(:invalid, "connection circuit deployment policy is inconsistent")}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "connection circuit deployment policy is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp capacity_scopes_match?(command, effective) do
    expected =
      CapacityConfiguration.deployment_scopes(command.workspace_context.workspace_id, effective)

    actual =
      Enum.filter(command.capacity_scopes, fn scope ->
        scope.scope_kind == :pool or
          (scope.scope_kind == :workspace and scope.scope_key == "global")
      end)

    normalize_policy_scopes(actual) == normalize_policy_scopes(expected)
  end

  defp normalize_policy_scopes(scopes) do
    scopes
    |> Enum.map(&{&1.scope_id, &1.scope_kind, &1.scope_key, &1.capacity_limit})
    |> Enum.sort()
  end

  defp config_value(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Enum.reduce_while(map, default, fn
          {candidate, value}, _acc when is_atom(candidate) ->
            if Atom.to_string(candidate) == key,
              do: {:halt, value},
              else: {:cont, default}

          _entry, _acc ->
            {:cont, default}
        end)
    end
  end

  defp insert_or_replay_manifest(version, hash, manifest, atom_strings) do
    row = %{
      manifest_version_id: version.manifest_version_id,
      content_hash: hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      runner_releases: version.runner_releases,
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

  defp decode_manifest_row(%ManifestVersion{schema_version: schema_version})
       when schema_version < @current_manifest_schema do
    {:error,
     Error.new(:invalid, "historical manifest cannot be used as a current release",
       details: %{
         reason: :historical_manifest_not_activatable,
         schema_version: schema_version,
         current_schema_version: @current_manifest_schema
       }
     )}
  end

  defp decode_manifest_row(%ManifestVersion{} = row) do
    manifest_json = Jason.encode!(row.manifest)

    with {:ok, manifest} <- Serializer.decode_manifest(manifest_json),
         {:ok, version} <-
           Version.from_published(manifest,
             manifest_version_id: row.manifest_version_id,
             content_hash: Base.encode16(row.content_hash, case: :lower),
             schema_version: row.schema_version,
             runner_contract_version: row.runner_contract_version,
             runner_releases: Map.get(manifest, "runner_releases"),
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

  defp evidence_bindings(targets, %Version{} = version) do
    selected_asset_ids =
      targets
      |> Enum.filter(&(&1.target_kind == :asset))
      |> MapSet.new(& &1.target_id)

    bindings =
      version.manifest.assets
      |> Enum.filter(fn asset ->
        is_nil(asset.target_descriptor) and
          MapSet.member?(selected_asset_ids, TargetIdentity.for_asset(asset.ref))
      end)
      |> Enum.map(fn asset ->
        %{
          target_id: TargetIdentity.for_asset(asset.ref),
          evidence_generation_id: asset.semantic_generation_id
        }
      end)
      |> Enum.sort_by(& &1.target_id)

    if Enum.all?(bindings, &valid_evidence_binding?/1) do
      {:ok, bindings}
    else
      {:error, Error.new(:invalid, "manifest asset evidence bindings are invalid")}
    end
  end

  defp valid_evidence_binding?(binding) do
    valid_id?(binding.target_id) and
      is_binary(binding.evidence_generation_id) and
      Regex.match?(~r/\Aag_[0-9a-f]{64}\z/, binding.evidence_generation_id)
  end

  defp validate_target_compatibilities(decisions, targets, manifest) when is_list(decisions) do
    asset_target_ids =
      targets
      |> Enum.filter(&(&1.target_kind == :asset))
      |> MapSet.new(& &1.target_id)

    target_ids = Enum.map(decisions, &Map.get(&1, :target_id))
    target_id_set = MapSet.new(target_ids)
    decisions_by_target = Map.new(decisions, &{&1.target_id, &1})

    required_descriptors =
      manifest.manifest.assets
      |> Enum.filter(&MapSet.member?(asset_target_ids, TargetIdentity.for_asset(&1.ref)))
      |> Enum.flat_map(fn
        %{target_descriptor: %TargetDescriptor{} = descriptor} ->
          [{descriptor.target_id, descriptor.descriptor_hash}]

        _asset ->
          []
      end)

    required_target_ids = MapSet.new(required_descriptors, &elem(&1, 0))

    if length(decisions) <= @max_deployment_targets and
         Enum.all?(decisions, &valid_target_compatibility?/1) and
         length(target_ids) == length(Enum.uniq(target_ids)) and
         target_id_set == required_target_ids and
         Enum.all?(required_descriptors, fn {target_id, descriptor_hash} ->
           match?(
             %DeploymentTargetCompatibility{desired_descriptor_hash: ^descriptor_hash},
             Map.get(decisions_by_target, target_id)
           )
         end) do
      :ok
    else
      {:error, Error.new(:invalid, "deployment target compatibility catalog is invalid")}
    end
  end

  defp validate_target_compatibilities(_decisions, _targets, _manifest),
    do: {:error, Error.new(:invalid, "deployment target compatibility catalog is invalid")}

  defp valid_target_compatibility?(%DeploymentTargetCompatibility{} = decision) do
    valid_id?(decision.target_id) and canonical_hash?(decision.desired_descriptor_hash) and
      decision.compatibility_status in [
        :ready,
        :uninitialized,
        :rebuild_available,
        :rebuild_required,
        :unexpected_drift,
        :operator_decision
      ] and
      valid_id?(decision.reason_code) and is_map(decision.compatibility_diff) and
      bounded_json?(decision.compatibility_diff, 262_144) and
      (is_nil(decision.expected_binding_version) or
         (is_integer(decision.expected_binding_version) and
            decision.expected_binding_version > 0)) and
      valid_optional_uuid?(decision.expected_active_generation_id) and
      valid_optional_hash?(decision.active_physical_fingerprint)
  end

  defp valid_target_compatibility?(_decision), do: false

  defp normalize_target_compatibilities(decisions) do
    decisions
    |> Enum.map(fn decision ->
      %{
        "target_id" => decision.target_id,
        "desired_descriptor_hash" => decision.desired_descriptor_hash,
        "compatibility_status" => Atom.to_string(decision.compatibility_status),
        "reason_code" => decision.reason_code,
        "compatibility_diff" => canonical_json_value(decision.compatibility_diff),
        "expected_binding_version" => decision.expected_binding_version,
        "expected_active_generation_id" => decision.expected_active_generation_id,
        "active_physical_fingerprint" => decision.active_physical_fingerprint
      }
    end)
    |> Enum.sort_by(& &1["target_id"])
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
        resolve_existing_deployment!(command, config_hash, target_hash)

      {1, _rows} ->
        {struct!(WorkspaceDeployment, attrs), :inserted}
    end
  end

  defp resolve_existing_deployment!(command, config_hash, target_hash) do
    identity =
      Repo.get_by(WorkspaceDeployment,
        workspace_id: command.workspace_context.workspace_id,
        deployment_id: command.deployment_id
      )

    case identity do
      %WorkspaceDeployment{} = existing ->
        if matching_deployment_content?(existing, command, config_hash, target_hash) do
          {existing, :exact_replay}
        else
          Repo.rollback(Error.new(:conflict, "deployment identity has different content"))
        end

      nil ->
        reuse_existing_deployment!(command, config_hash, target_hash)
    end
  end

  defp reuse_existing_deployment!(command, config_hash, target_hash) do
    existing =
      Repo.get_by(WorkspaceDeployment,
        workspace_id: command.workspace_context.workspace_id,
        manifest_version_id: command.manifest_version_id,
        configuration_fingerprint: config_hash,
        target_catalog_fingerprint: target_hash
      )

    if match?(%WorkspaceDeployment{}, existing) and
         existing.configuration_version == command.configuration_version do
      {existing, :content_reuse}
    else
      Repo.rollback(Error.new(:conflict, "deployment content conflicts with committed state"))
    end
  end

  defp matching_deployment_content?(existing, command, config_hash, target_hash) do
    existing.manifest_version_id == command.manifest_version_id and
      existing.configuration_version == command.configuration_version and
      existing.configuration_fingerprint == config_hash and
      existing.target_catalog_fingerprint == target_hash
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

  defp persist_evidence_bindings!(_command, []), do: :ok

  defp persist_evidence_bindings!(command, bindings) do
    rows =
      Enum.map(bindings, fn binding ->
        %{
          workspace_id: command.workspace_context.workspace_id,
          target_id: binding.target_id,
          evidence_generation_id: binding.evidence_generation_id,
          initial_manifest_id: command.manifest_version_id,
          created_at: command.occurred_at
        }
      end)

    Enum.each(Enum.chunk_every(rows, @bulk_insert_size), fn chunk ->
      {_count, _rows} =
        Repo.insert_all(AssetEvidenceBinding, chunk,
          on_conflict: :nothing,
          conflict_target: [:workspace_id, :target_id]
        )
    end)

    stored_count =
      bindings
      |> Enum.map(& &1.target_id)
      |> Enum.chunk_every(@bulk_insert_size)
      |> Enum.reduce(0, fn target_ids, count ->
        batch_count =
          from(binding in AssetEvidenceBinding,
            where:
              binding.workspace_id == ^command.workspace_context.workspace_id and
                binding.target_id in ^target_ids,
            select: count()
          )
          |> Repo.one()

        count + batch_count
      end)

    if stored_count != length(bindings) do
      Repo.rollback(Error.new(:conflict, "asset evidence bindings conflict with committed state"))
    end
  end

  defp persist_target_compatibilities!(_command, []), do: :ok

  defp persist_target_compatibilities!(command, decisions) do
    Enum.each(decisions, &persist_target_compatibility!(command, &1))
  end

  defp persist_target_compatibility!(command, decision) do
    workspace_id = command.workspace_context.workspace_id
    target_id = decision["target_id"]

    binding =
      from(binding in AssetTargetBinding,
        where: binding.workspace_id == ^workspace_id and binding.target_id == ^target_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      is_nil(binding) ->
        insert_target_compatibility!(command, decision)

      exact_target_compatibility_replay?(binding, command, decision) ->
        :ok

      binding.version != decision["expected_binding_version"] or
          binding.active_generation_id != decision["expected_active_generation_id"] ->
        Repo.rollback(Error.new(:conflict, "target compatibility decision is stale"))

      true ->
        binding
        |> Ecto.Changeset.change(%{
          desired_manifest_id: command.manifest_version_id,
          desired_descriptor_hash: decision["desired_descriptor_hash"],
          compatibility_status: decision["compatibility_status"],
          reason_code: decision["reason_code"],
          compatibility_diff: decision["compatibility_diff"],
          active_physical_fingerprint: decision["active_physical_fingerprint"],
          version: binding.version + 1,
          updated_at: command.occurred_at
        })
        |> Repo.update!()
    end
  end

  defp insert_target_compatibility!(command, decision) do
    if is_nil(decision["expected_binding_version"]) and
         is_nil(decision["expected_active_generation_id"]) do
      %AssetTargetBinding{
        workspace_id: command.workspace_context.workspace_id,
        target_id: decision["target_id"],
        active_generation_id: nil,
        desired_manifest_id: command.manifest_version_id,
        desired_descriptor_hash: decision["desired_descriptor_hash"],
        compatibility_status: decision["compatibility_status"],
        reason_code: decision["reason_code"],
        compatibility_diff: decision["compatibility_diff"],
        active_physical_fingerprint: decision["active_physical_fingerprint"],
        version: 1,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()
    else
      Repo.rollback(Error.new(:conflict, "target compatibility decision expected a binding"))
    end
  end

  defp exact_target_compatibility_replay?(binding, command, decision) do
    binding.active_generation_id == decision["expected_active_generation_id"] and
      binding.desired_manifest_id == command.manifest_version_id and
      binding.desired_descriptor_hash == decision["desired_descriptor_hash"] and
      binding.compatibility_status == decision["compatibility_status"] and
      binding.reason_code == decision["reason_code"] and
      binding.compatibility_diff == decision["compatibility_diff"] and
      binding.active_physical_fingerprint == decision["active_physical_fingerprint"]
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

  defp sync_execution_pool_circuits!(_command, _configuration, true), do: :ok

  defp sync_execution_pool_circuits!(command, configuration, false) do
    with {:ok, policies} <- ExecutionPoolPolicy.effective(configuration) do
      Enum.each(policies, fn
        {_pool, %{circuit_breaker: nil}} ->
          :ok

        {pool, %{circuit_breaker: policy}} ->
          Repo.insert_all(
            ResourceCircuit,
            [
              %{
                workspace_id: command.workspace_context.workspace_id,
                resource_kind: "execution_pool",
                resource_name: pool,
                state: "closed",
                consecutive_failures: 0,
                failure_threshold: policy.failure_threshold,
                probe_after_ms: policy.probe_after_ms,
                version: 1,
                inserted_at: command.occurred_at,
                updated_at: command.occurred_at
              }
            ],
            on_conflict: :nothing
          )

          from(circuit in ResourceCircuit,
            where:
              circuit.workspace_id == ^command.workspace_context.workspace_id and
                circuit.resource_kind == "execution_pool" and circuit.resource_name == ^pool and
                circuit.state == "closed" and
                circuit.consecutive_failures >= ^policy.failure_threshold
          )
          |> Repo.update_all(
            set: [
              state: "open",
              opened_at: command.occurred_at,
              next_probe_at:
                DateTime.add(command.occurred_at, policy.probe_after_ms, :millisecond),
              probe_owner_id: nil,
              probe_expires_at: nil,
              updated_at: command.occurred_at
            ],
            inc: [version: 1]
          )

          SQL.query!(
            Repo,
            """
            UPDATE favn_control.resource_circuits
            SET next_probe_at = opened_at + ($3 * interval '1 millisecond')
            WHERE workspace_id = $1
              AND resource_kind = 'execution_pool'
              AND resource_name = $2
              AND state = 'open'
              AND opened_at IS NOT NULL
              AND probe_after_ms <> $3
            """,
            [
              command.workspace_context.workspace_id,
              pool,
              policy.probe_after_ms
            ]
          )

          from(circuit in ResourceCircuit,
            where:
              circuit.workspace_id == ^command.workspace_context.workspace_id and
                circuit.resource_kind == "execution_pool" and circuit.resource_name == ^pool
          )
          |> Repo.update_all(
            set: [
              failure_threshold: policy.failure_threshold,
              probe_after_ms: policy.probe_after_ms,
              updated_at: command.occurred_at
            ],
            inc: [version: 1]
          )
      end)
    else
      {:error, reason} ->
        Repo.rollback(
          Error.new(:invalid, "execution-pool deployment policy is invalid",
            details: %{reason: inspect(reason)}
          )
        )
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

  defp runtime_result(
         state,
         manifest_version_id,
         manifest_summary,
         activation_diagnostics,
         execution_pool_diagnostics
       ) do
    %RuntimeState{
      workspace_id: state.workspace_id,
      deployment_id: state.active_deployment_id,
      manifest_version_id: manifest_version_id,
      revision: state.revision,
      activated_at: state.activated_at,
      manifest_content_hash: manifest_summary_value(manifest_summary, :content_hash),
      schema_version: manifest_summary_value(manifest_summary, :schema_version),
      runner_contract_version: manifest_summary_value(manifest_summary, :runner_contract_version),
      runner_releases: manifest_summary_value(manifest_summary, :runner_releases),
      asset_count: manifest_summary_value(manifest_summary, :asset_count),
      pipeline_count: manifest_summary_value(manifest_summary, :pipeline_count),
      schedule_count: manifest_summary_value(manifest_summary, :schedule_count),
      execution_pools: execution_pool_diagnostics.items,
      execution_pool_count: execution_pool_diagnostics.count,
      execution_pools_truncated: execution_pool_diagnostics.truncated,
      activation_diagnostics: activation_diagnostics
    }
  end

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
      runner_releases: version.runner_releases,
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
         "runner_releases" => result.runner_releases,
         "asset_count" => result.asset_count,
         "pipeline_count" => result.pipeline_count,
         "schedule_count" => result.schedule_count,
         "execution_pools" => result.execution_pools || [],
         "execution_pool_count" => result.execution_pool_count || 0,
         "execution_pools_truncated" => result.execution_pools_truncated || false,
         "activation_diagnostics" =>
           ManifestActivationDiagnostics.to_map(result.activation_diagnostics)
       },
       response_status: 200,
       resource_kind: "workspace_deployment",
       resource_id: result.deployment_id
     }}
  end

  defp decode_idempotent_deployment(%{response: response}) when is_map(response) do
    with {:ok, activated_at} <- decode_optional_datetime(Map.get(response, "activated_at")),
         {:ok, activation_diagnostics} <-
           ManifestActivationDiagnostics.from_map(Map.get(response, "activation_diagnostics")),
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
         runner_releases: Map.get(response, "runner_releases"),
         asset_count: Map.get(response, "asset_count"),
         pipeline_count: Map.get(response, "pipeline_count"),
         schedule_count: Map.get(response, "schedule_count"),
         execution_pools: Map.get(response, "execution_pools", []),
         execution_pool_count: Map.get(response, "execution_pool_count", 0),
         execution_pools_truncated: Map.get(response, "execution_pools_truncated", false),
         activation_diagnostics: activation_diagnostics
       }}
    else
      _other -> {:error, Error.new(:internal, "idempotent deployment replay record is invalid")}
    end
  end

  defp decode_idempotent_deployment(_encoded),
    do: {:error, Error.new(:internal, "idempotent deployment replay record is invalid")}

  defp execution_pool_diagnostics!(configuration) do
    case ExecutionPoolPolicy.diagnostics(configuration) do
      {:ok, diagnostics} ->
        %{
          items: Enum.take(diagnostics, @max_activation_execution_pool_diagnostics),
          count: length(diagnostics),
          truncated: length(diagnostics) > @max_activation_execution_pool_diagnostics
        }

      {:error, reason} ->
        Repo.rollback(
          Error.new(:invalid, "execution-pool policy is invalid",
            details: %{reason: inspect(reason)}
          )
        )
    end
  end

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

  defp validate_manifest_target_descriptor_query(query) do
    target_ids = query.target_ids

    if valid_id?(query.manifest_version_id) and is_list(target_ids) and target_ids != [] and
         length(target_ids) <= @max_manifest_target_descriptors and
         length(target_ids) == length(Enum.uniq(target_ids)) and
         Enum.all?(target_ids, &valid_id?/1) do
      :ok
    else
      {:error, Error.new(:invalid, "manifest target descriptor query is invalid")}
    end
  end

  defp validate_platform_manifest_read(%PlatformContext{} = context) do
    if PlatformContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:platform_reader, :platform_operator, :platform_admin])
         ) do
      :ok
    else
      {:error, Error.new(:forbidden, "platform manifest read role required")}
    end
  end

  defp validate_platform_manifest_read(_context),
    do: {:error, Error.new(:forbidden, "platform manifest read role required")}

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

  @impl true
  def acquire_manifest_upload_lease(%AcquireManifestUploadLease{} = command) do
    with :ok <- validate_deployment_context(command.context),
         true <- valid_id?(command.lease_id),
         true <- match?(%DateTime{}, command.expires_at),
         true <- match?(%DateTime{}, command.occurred_at),
         {:ok, :ok} <-
           Repo.transaction(fn ->
             SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [
               "favn:manifest-upload-admission"
             ])

             ManifestDeploymentUploadLease
             |> where([lease], lease.expires_at <= ^command.occurred_at)
             |> Repo.delete_all()

             global_count = Repo.aggregate(ManifestDeploymentUploadLease, :count)

             identity_count =
               ManifestDeploymentUploadLease
               |> where([lease], lease.service_identity == ^command.context.service_identity)
               |> Repo.aggregate(:count)

             workspace_count =
               ManifestDeploymentUploadLease
               |> where([lease], lease.workspace_id == ^command.context.workspace_id)
               |> Repo.aggregate(:count)

             if global_count < 2 and identity_count < 1 and workspace_count < 1 do
               now = database_datetime(command.occurred_at)

               Repo.insert!(%ManifestDeploymentUploadLease{
                 lease_id: command.lease_id,
                 workspace_id: command.context.workspace_id,
                 service_identity: command.context.service_identity,
                 expires_at: database_datetime(command.expires_at),
                 inserted_at: now,
                 updated_at: now
               })

               :ok
             else
               Repo.rollback(
                 Error.new(:limit_exceeded, "manifest deployment upload admission is busy",
                   details: %{reason: :deployment_upload_busy}
                 )
               )
             end
           end) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest upload lease")}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def renew_manifest_upload_lease(%RenewManifestUploadLease{} = command) do
    with :ok <- validate_deployment_context(command.context),
         true <- valid_id?(command.lease_id),
         true <- match?(%DateTime{}, command.expires_at),
         {1, _rows} <-
           ManifestDeploymentUploadLease
           |> where(
             [lease],
             lease.lease_id == ^command.lease_id and
               lease.workspace_id == ^command.context.workspace_id and
               lease.service_identity == ^command.context.service_identity
           )
           |> Repo.update_all(
             set: [
               expires_at: database_datetime(command.expires_at),
               updated_at: DateTime.utc_now()
             ]
           ) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest upload lease")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest upload lease was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def release_manifest_upload_lease(%ReleaseManifestUploadLease{} = command) do
    with :ok <- validate_deployment_context(command.context),
         true <- valid_id?(command.lease_id) do
      ManifestDeploymentUploadLease
      |> where(
        [lease],
        lease.lease_id == ^command.lease_id and
          lease.workspace_id == ^command.context.workspace_id and
          lease.service_identity == ^command.context.service_identity
      )
      |> Repo.delete_all()

      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest upload lease")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def accept_manifest_deployment(%AcceptManifestDeployment{} = command) do
    version = command.version

    with :ok <- validate_manifest_accept(command),
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
         {:ok, archive_hash} <- decode_hash(command.archive_sha256),
         {:ok, fingerprint} <- decode_hash(command.request_fingerprint),
         {:ok, {status, operation, stored}} <-
           Repo.transaction(fn ->
             SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [
               "manifest-deployment:#{command.context.workspace_id}:#{command.operation_id}"
             ])

             require_manifest_upload_lease!(command)

             case Repo.get_by(ManifestDeploymentOperation,
                    workspace_id: command.context.workspace_id,
                    operation_id: command.operation_id
                  ) do
               %ManifestDeploymentOperation{} = existing ->
                 verify_manifest_deployment_replay!(
                   existing,
                   archive_hash,
                   fingerprint,
                   verified
                 )

               nil ->
                 required_refs = Publication.required_package_refs(verified)

                 with :ok <- validate_execution_package_refs!(required_refs),
                      {:ok, stored} <-
                        insert_or_replay_manifest(verified, hash, manifest, atom_strings),
                      :ok <- link_manifest_execution_packages!(stored, required_refs) do
                   insert_manifest_audit!(command.platform_context, stored)

                   operation =
                     insert_manifest_deployment_operation!(
                       command,
                       stored,
                       archive_hash,
                       fingerprint
                     )

                   insert_manifest_deployment_audit!(command, stored)

                   {:accepted, operation, stored}
                 else
                   {:error, error} -> Repo.rollback(error)
                 end
             end
           end),
         :ok <- ManifestCache.put(stored) do
      {:ok, status, operation}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(:invalid, "invalid manifest deployment", details: %{reason: inspect(reason)})}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_manifest_deployment(%{context: context, operation_id: operation_id}) do
    with :ok <- validate_deployment_context(context),
         true <- valid_operation_id?(operation_id),
         %ManifestDeploymentOperation{} = row <-
           Repo.get_by(ManifestDeploymentOperation,
             workspace_id: context.workspace_id,
             operation_id: operation_id
           ) do
      {:ok, manifest_deployment_result(row)}
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment identity")}
      nil -> {:error, Error.new(:not_found, "manifest deployment was not found")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def claim_manifest_deployment(%ClaimManifestDeployment{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         true <- valid_id?(command.owner),
         true <- match?(%DateTime{}, command.expires_at),
         true <- match?(%DateTime{}, command.occurred_at),
         {:ok, operation} <-
           Repo.transaction(fn ->
             query =
               ManifestDeploymentOperation
               |> where(
                 [operation],
                 (operation.state == "accepted" and
                    fragment(
                      "NOT EXISTS (SELECT 1 FROM favn_control.manifest_deployment_operations AS active WHERE active.workspace_id = ? AND active.state = 'activating')",
                      operation.workspace_id
                    )) or
                   (operation.state == "activating" and
                      operation.claim_expires_at <= ^command.occurred_at)
               )
               |> order_by([operation], asc: operation.accepted_at)
               |> limit(1)
               |> lock("FOR UPDATE SKIP LOCKED")

             case Repo.one(query) do
               nil ->
                 nil

               operation ->
                 fence = operation.claim_fence + 1

                 changes = [
                   state: "activating",
                   claim_owner: command.owner,
                   claim_fence: fence,
                   claim_expires_at: database_datetime(command.expires_at),
                   activating_at:
                     operation.activating_at || database_datetime(command.occurred_at),
                   updated_at: database_datetime(command.occurred_at)
                 ]

                 {1, [claimed]} =
                   ManifestDeploymentOperation
                   |> where(
                     [row],
                     row.workspace_id == ^operation.workspace_id and
                       row.operation_id == ^operation.operation_id
                   )
                   |> Repo.update_all(set: changes, returning: true)

                 manifest_deployment_result(claimed)
             end
           end) do
      {:ok, operation}
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment claim")}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def renew_manifest_deployment_claim(%RenewManifestDeploymentClaim{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         true <- valid_claim_identity?(command),
         {1, _rows} <-
           ManifestDeploymentOperation
           |> where(
             [operation],
             operation.workspace_id == ^command.workspace_id and
               operation.operation_id == ^command.operation_id and
               operation.state == "activating" and operation.claim_owner == ^command.owner and
               operation.claim_fence == ^command.fence
           )
           |> Repo.update_all(
             set: [
               claim_expires_at: database_datetime(command.expires_at),
               updated_at: DateTime.utc_now()
             ]
           ) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment claim")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest deployment claim was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def update_manifest_deployment_progress(%UpdateManifestDeploymentProgress{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         true <- valid_claim_identity?(command),
         true <- is_integer(command.completed) and is_integer(command.total),
         true <- command.completed >= 0 and command.completed <= command.total,
         true <- match?(%DateTime{}, command.occurred_at),
         {1, _rows} <-
           ManifestDeploymentOperation
           |> where(
             [operation],
             operation.workspace_id == ^command.workspace_id and
               operation.operation_id == ^command.operation_id and
               operation.state == "activating" and operation.claim_owner == ^command.owner and
               operation.claim_fence == ^command.fence
           )
           |> Repo.update_all(
             set: [
               inspection_completed: command.completed,
               inspection_total: command.total,
               updated_at: database_datetime(command.occurred_at)
             ]
           ) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment progress")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest deployment claim was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def release_manifest_deployment_claim(%ReleaseManifestDeploymentClaim{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         true <- valid_claim_identity?(command),
         {1, _rows} <-
           ManifestDeploymentOperation
           |> where(
             [operation],
             operation.workspace_id == ^command.workspace_id and
               operation.operation_id == ^command.operation_id and
               operation.state == "activating" and operation.claim_owner == ^command.owner and
               operation.claim_fence == ^command.fence
           )
           |> Repo.update_all(
             set: [
               state: "accepted",
               claim_owner: nil,
               claim_expires_at: nil,
               updated_at: database_datetime(command.occurred_at)
             ]
           ) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment claim")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest deployment claim was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def complete_manifest_deployment(%CompleteManifestDeployment{} = command) do
    with :ok <- validate_platform_manifest_write(command.platform_context),
         true <- valid_claim_identity?(command),
         true <- command.state in [:succeeded, :needs_attention, :failed, :unknown],
         true <- valid_manifest_completion?(command),
         {1, [row]} <-
           ManifestDeploymentOperation
           |> where(
             [operation],
             operation.workspace_id == ^command.workspace_id and
               operation.operation_id == ^command.operation_id and
               operation.state == "activating" and operation.claim_owner == ^command.owner and
               operation.claim_fence == ^command.fence
           )
           |> Repo.update_all(
             set: [
               state: Atom.to_string(command.state),
               deployment_id: command.deployment_id,
               failure_class: command.failure_class,
               activation_diagnostics: canonical_json_value(command.activation_diagnostics),
               claim_owner: nil,
               claim_expires_at: nil,
               terminal_at: database_datetime(command.occurred_at),
               updated_at: database_datetime(command.occurred_at)
             ],
             returning: true
           ) do
      {:ok, manifest_deployment_result(row)}
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment completion")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest deployment claim was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def acquire_manifest_activation_lease(%AcquireManifestActivationLease{} = command) do
    with :ok <- validate_activation_lease_command(command),
         {:ok, result} <-
           Repo.transaction(fn ->
             SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [
               "manifest-activation:#{command.workspace_context.workspace_id}"
             ])

             existing = Repo.get(ManifestActivationLease, command.workspace_context.workspace_id)

             cond do
               is_nil(existing) or
                   DateTime.compare(existing.expires_at, command.occurred_at) != :gt ->
                 fence = if existing, do: existing.fencing_token + 1, else: 1
                 now = database_datetime(command.occurred_at)

                 Repo.insert!(
                   %ManifestActivationLease{
                     workspace_id: command.workspace_context.workspace_id,
                     operation_id: command.operation_id,
                     owner: command.owner,
                     fencing_token: fence,
                     expires_at: database_datetime(command.expires_at),
                     inserted_at: (existing && existing.inserted_at) || now,
                     updated_at: now
                   },
                   on_conflict:
                     {:replace, [:operation_id, :owner, :fencing_token, :expires_at, :updated_at]},
                   conflict_target: [:workspace_id]
                 )

                 {:ok, fence}

               existing.operation_id == command.operation_id and existing.owner == command.owner ->
                 {:ok, existing.fencing_token}

               true ->
                 {:error, :busy}
             end
           end) do
      case result do
        {:ok, fence} ->
          {:ok, fence}

        {:error, :busy} ->
          {:error,
           Error.new(:conflict, "manifest activation is already in progress",
             details: %{reason: :manifest_activation_in_progress}
           )}
      end
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def renew_manifest_activation_lease(%RenewManifestActivationLease{} = command) do
    with :ok <- validate_activation_lease_command(command),
         true <- is_integer(command.fence) and command.fence > 0,
         {1, _rows} <-
           ManifestActivationLease
           |> where(
             [lease],
             lease.workspace_id == ^command.workspace_context.workspace_id and
               lease.operation_id == ^command.operation_id and lease.owner == ^command.owner and
               lease.fencing_token == ^command.fence
           )
           |> Repo.update_all(
             set: [
               expires_at: database_datetime(command.expires_at),
               updated_at: DateTime.utc_now()
             ]
           ) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest activation lease")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest activation lease was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def release_manifest_activation_lease(%ReleaseManifestActivationLease{} = command) do
    with :ok <- validate_activation_lease_identity(command),
         true <- is_integer(command.fence) and command.fence > 0,
         {1, _rows} <-
           ManifestActivationLease
           |> where(
             [lease],
             lease.workspace_id == ^command.workspace_context.workspace_id and
               lease.operation_id == ^command.operation_id and lease.owner == ^command.owner and
               lease.fencing_token == ^command.fence
           )
           |> Repo.delete_all() do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest activation lease")}
      {0, _rows} -> {:error, Error.new(:conflict, "manifest activation lease was lost")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_deployment_context(%ManifestDeploymentContext{} = context) do
    if ManifestDeploymentContext.valid?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "valid manifest deployment authority required")}
  end

  defp validate_deployment_context(_context),
    do: {:error, Error.new(:forbidden, "valid manifest deployment authority required")}

  defp validate_manifest_accept(%AcceptManifestDeployment{} = command) do
    with :ok <- validate_deployment_context(command.context),
         :ok <- validate_platform_manifest_write(command.platform_context),
         true <- WorkspaceContext.valid?(command.workspace_context),
         true <- :platform_operator in command.workspace_context.roles,
         true <- command.context.workspace_id == command.workspace_context.workspace_id,
         true <- valid_operation_id?(command.operation_id),
         true <- valid_id?(command.upload_lease_id),
         true <- canonical_hash?(command.archive_sha256),
         true <- canonical_hash?(command.request_fingerprint),
         true <- match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      false -> {:error, Error.new(:invalid, "invalid manifest deployment acceptance")}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp require_manifest_upload_lease!(command) do
    lease =
      ManifestDeploymentUploadLease
      |> where(
        [lease],
        lease.lease_id == ^command.upload_lease_id and
          lease.workspace_id == ^command.context.workspace_id and
          lease.service_identity == ^command.context.service_identity and
          fragment("? > clock_timestamp()", lease.expires_at)
      )
      |> lock("FOR UPDATE")
      |> Repo.one()

    if is_nil(lease) do
      Repo.rollback(
        Error.new(:conflict, "manifest upload lease was lost",
          details: %{reason: :manifest_upload_lease_lost}
        )
      )
    end

    :ok
  end

  defp insert_manifest_deployment_operation!(command, version, archive_hash, fingerprint) do
    now = database_datetime(command.occurred_at)

    row = %ManifestDeploymentOperation{
      workspace_id: command.context.workspace_id,
      operation_id: command.operation_id,
      archive_sha256: archive_hash,
      request_fingerprint: fingerprint,
      service_identity: command.context.service_identity,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: Base.decode16!(version.content_hash, case: :lower),
      runner_releases: canonical_json_value(version.runner_releases),
      state: "accepted",
      claim_fence: 0,
      accepted_at: now,
      inserted_at: now,
      updated_at: now
    }

    Repo.insert!(row)
  end

  defp insert_manifest_deployment_audit!(command, version) do
    command_hash =
      :crypto.hash(
        :sha256,
        command.context.workspace_id <> <<0>> <> command.operation_id
      )
      |> Base.encode16(case: :lower)

    now = database_datetime(command.occurred_at)

    Repo.insert!(%AuthPlatformAuditEntry{
      command_id: "manifest.deploy.accept:" <> command_hash,
      principal_id: "service:" <> command.context.service_identity,
      action: "manifest.deployment.accepted",
      subject_kind: "manifest_deployment",
      subject_id: command.operation_id,
      detail: %{
        "workspace_id" => command.context.workspace_id,
        "manifest_version_id" => version.manifest_version_id,
        "content_hash" => version.content_hash,
        "archive_sha256" => command.archive_sha256
      },
      occurred_at: now,
      inserted_at: now
    })

    :ok
  end

  defp verify_manifest_deployment_replay!(existing, archive_hash, fingerprint, version) do
    if existing.archive_sha256 == archive_hash and existing.request_fingerprint == fingerprint and
         existing.manifest_version_id == version.manifest_version_id and
         existing.manifest_content_hash == Base.decode16!(version.content_hash, case: :lower) do
      {:replay, manifest_deployment_result(existing), version}
    else
      Repo.rollback(
        Error.new(:conflict, "manifest deployment operation id has different content",
          details: %{reason: :deployment_operation_conflict}
        )
      )
    end
  end

  defp manifest_deployment_result(row) do
    %ManifestDeployment{
      workspace_id: row.workspace_id,
      operation_id: row.operation_id,
      archive_sha256: Base.encode16(row.archive_sha256, case: :lower),
      request_fingerprint: Base.encode16(row.request_fingerprint, case: :lower),
      service_identity: row.service_identity,
      manifest_version_id: row.manifest_version_id,
      manifest_content_hash: Base.encode16(row.manifest_content_hash, case: :lower),
      runner_releases: row.runner_releases,
      state: String.to_existing_atom(row.state),
      deployment_id: row.deployment_id,
      failure_class: row.failure_class,
      activation_diagnostics: row.activation_diagnostics,
      claim_owner: row.claim_owner,
      claim_fence: if(row.claim_fence > 0, do: row.claim_fence),
      claim_expires_at: row.claim_expires_at,
      inspection_total: row.inspection_total,
      inspection_completed: row.inspection_completed,
      accepted_at: row.accepted_at,
      activating_at: row.activating_at,
      terminal_at: row.terminal_at,
      inserted_at: row.inserted_at,
      updated_at: row.updated_at
    }
  end

  defp valid_claim_identity?(command) do
    valid_id?(command.workspace_id) and valid_operation_id?(command.operation_id) and
      valid_id?(command.owner) and is_integer(command.fence) and command.fence > 0
  end

  defp valid_manifest_completion?(%{
         state: state,
         deployment_id: deployment_id,
         activation_diagnostics: diagnostics
       })
       when state in [:succeeded, :needs_attention] do
    with true <- valid_id?(deployment_id),
         {:ok, %ManifestActivationDiagnostics{} = parsed} <-
           ManifestActivationDiagnostics.from_map(diagnostics) do
      (state == :succeeded and parsed.unresolved_inspection_count == 0) or
        (state == :needs_attention and parsed.unresolved_inspection_count > 0)
    else
      _invalid -> false
    end
  end

  defp valid_manifest_completion?(%{
         state: state,
         failure_class: failure_class,
         activation_diagnostics: nil
       })
       when state in [:failed, :unknown],
       do: valid_id?(failure_class)

  defp validate_activation_lease_command(command) do
    if validate_activation_lease_identity(command) == :ok and
         match?(%DateTime{}, command.expires_at) and
         (not Map.has_key?(command, :occurred_at) or match?(%DateTime{}, command.occurred_at)) do
      :ok
    else
      {:error, Error.new(:forbidden, "valid manifest activation lease authority required")}
    end
  end

  defp validate_activation_lease_identity(command) do
    if WorkspaceContext.valid?(command.workspace_context) and valid_id?(command.operation_id) and
         valid_id?(command.owner) do
      :ok
    else
      {:error, Error.new(:forbidden, "valid manifest activation lease authority required")}
    end
  end

  defp valid_operation_id?(value) do
    is_binary(value) and byte_size(value) in 1..128 and
      Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/, value)
  end

  defp database_datetime(%DateTime{} = datetime),
    do: DateTime.add(datetime, 0, :microsecond)

  defp canonical_json_value(value), do: value |> Jason.encode!() |> Jason.decode!()

  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255

  defp decode_hash(hash) when is_binary(hash) and byte_size(hash) == 64 do
    if canonical_hash?(hash), do: Base.decode16(hash, case: :lower), else: :error
  end

  defp decode_hash(_hash), do: {:error, :invalid_content_hash}

  defp canonical_hash?(hash) when is_binary(hash), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)
  defp canonical_hash?(_hash), do: false

  defp valid_optional_hash?(nil), do: true
  defp valid_optional_hash?(hash), do: canonical_hash?(hash)

  defp valid_optional_uuid?(nil), do: true
  defp valid_optional_uuid?(value), do: match?({:ok, _uuid}, Ecto.UUID.cast(value))

  defp changeset_error(changeset) do
    if changeset.errors[:slug] do
      Error.new(:conflict, "workspace slug already exists")
    else
      Error.new(:invalid, "workspace attributes are invalid")
    end
  end
end
