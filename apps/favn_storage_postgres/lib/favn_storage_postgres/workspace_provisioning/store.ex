defmodule FavnStoragePostgres.WorkspaceProvisioning.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.WorkspaceProvisioningStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspaceAdministrator
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetWorkspaceProvisioning
  alias FavnOrchestrator.Persistence.Results.WorkspaceProvisioning
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AuthActor
  alias FavnStoragePostgres.Schemas.AuthAuditEntry
  alias FavnStoragePostgres.Schemas.AuthCredential
  alias FavnStoragePostgres.Schemas.AuthExternalIdentity
  alias FavnStoragePostgres.Schemas.AuthPlatformAuditEntry
  alias FavnStoragePostgres.Schemas.AuthPlatformGrant
  alias FavnStoragePostgres.Schemas.AuthWorkspaceMembership
  alias FavnStoragePostgres.Schemas.Workspace
  alias FavnStoragePostgres.Schemas.WorkspaceProvisioningOperation
  alias FavnStoragePostgres.Schemas.WorkspaceRuntimeState

  @provider "azure_container_apps_entra"
  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/

  @impl true
  def provision(%ProvisionWorkspaceAdministrator{} = command) do
    provision(command, [])
  end

  @doc false
  @spec provision(ProvisionWorkspaceAdministrator.t(), keyword()) ::
          {:ok, WorkspaceProvisioning.t()} | {:error, Error.t()}
  def provision(%ProvisionWorkspaceAdministrator{} = command, opts) when is_list(opts) do
    with :ok <- validate_command(command),
         {:ok, result} <-
           Repo.transaction(fn ->
             lock_request!(command)

             case existing_operation(command) do
               nil -> create_provisioning!(command, opts)
               operation -> replay_provisioning!(operation, command)
             end
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
  def get(%GetWorkspaceProvisioning{} = query) do
    with :ok <- validate_query(query),
         %WorkspaceProvisioningOperation{} = operation <-
           Repo.get_by(WorkspaceProvisioningOperation, workspace_id: query.workspace_id) do
      ready_result!(operation, false)
    else
      nil ->
        legacy_ready_result(query.workspace_id)

      {:error, %Error{} = error} ->
        {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp create_provisioning!(command, opts) do
    ensure_no_workspace_administrator!(command.workspace_id)
    ensure_workspace!(command)
    checkpoint!(opts, :workspace)
    actor_state = ensure_actor!(command)
    checkpoint!(opts, :actor)
    insert_membership!(command)
    checkpoint!(opts, :membership)
    ensure_platform_grant!(command)
    checkpoint!(opts, :platform_grant)
    ensure_authentication!(command, actor_state)
    checkpoint!(opts, :authentication)
    insert_audit!(command)
    operation = insert_operation!(command)
    checkpoint!(opts, :receipt)
    ready_result!(operation, false) |> unwrap!()
  end

  defp replay_provisioning!(operation, command) do
    expected_fingerprint = decode_fingerprint!(command.request_fingerprint)

    if operation.operation_id == command.operation_id and
         operation.workspace_id == command.workspace_id and
         operation.request_fingerprint == expected_fingerprint and
         operation.actor_id == command.actor_id and operation.username == command.username and
         operation.authentication_mode == Atom.to_string(command.authentication_mode) do
      ready_result!(operation, true) |> unwrap!()
    else
      rollback_conflict!("workspace provisioning operation changed")
    end
  end

  defp existing_operation(command) do
    operations =
      from(operation in WorkspaceProvisioningOperation,
        where:
          operation.operation_id == ^command.operation_id or
            operation.workspace_id == ^command.workspace_id,
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    case operations do
      [] ->
        nil

      [operation] ->
        operation

      _crossed_identities ->
        rollback_conflict!("operation and workspace identify different receipts")
    end
  end

  defp lock_request!(command) do
    [
      "actor:" <> command.actor_id,
      "operation:" <> command.operation_id,
      "workspace:" <> command.workspace_id
    ]
    |> Enum.sort()
    |> Enum.each(fn identity ->
      SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [identity])
    end)
  end

  defp ensure_no_workspace_administrator!(workspace_id) do
    existing =
      from(membership in AuthWorkspaceMembership,
        where:
          membership.workspace_id == ^workspace_id and membership.status == "active" and
            fragment("? @> ARRAY['workspace_admin']::text[]", membership.roles),
        select: membership.actor_id,
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    if existing != [], do: rollback_conflict!("workspace already has an administrator")
  end

  defp ensure_workspace!(command) do
    attrs = %{
      workspace_id: command.workspace_id,
      slug: command.slug,
      display_name: command.workspace_name,
      status: "active",
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }

    case Repo.get(Workspace, command.workspace_id) do
      nil ->
        %Workspace{}
        |> Workspace.changeset(attrs)
        |> Repo.insert!()

        Repo.insert!(%WorkspaceRuntimeState{
          workspace_id: command.workspace_id,
          revision: 0,
          updated_at: command.occurred_at
        })

        OutboxWriter.insert!(%{
          workspace_id: command.workspace_id,
          command_id: command.operation_id,
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

      %Workspace{slug: slug, display_name: name, status: "active"}
      when slug == command.slug and name == command.workspace_name ->
        if is_nil(Repo.get(WorkspaceRuntimeState, command.workspace_id)) do
          rollback_conflict!("workspace runtime state is missing")
        end

      %Workspace{} ->
        rollback_conflict!("workspace identity has different content")
    end
  end

  defp ensure_actor!(command) do
    normalized_username = normalize_username(command.username)

    existing =
      from(actor in AuthActor,
        where:
          actor.actor_id == ^command.actor_id or
            actor.normalized_username == ^normalized_username,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    case existing do
      nil ->
        Repo.insert!(%AuthActor{
          actor_id: command.actor_id,
          username: command.username,
          normalized_username: normalized_username,
          display_name: command.display_name,
          creation_command_id: command.operation_id,
          creation_hash: creation_hash!(command),
          status: "active",
          version: 1,
          inserted_at: command.occurred_at,
          updated_at: command.occurred_at
        })

        :created

      %AuthActor{
        actor_id: actor_id,
        username: username,
        normalized_username: ^normalized_username,
        display_name: display_name,
        status: "active"
      }
      when actor_id == command.actor_id and username == command.username and
             display_name == command.display_name ->
        :existing

      %AuthActor{} ->
        rollback_conflict!("administrator actor has different content")
    end
  end

  defp insert_membership!(command) do
    Repo.insert!(%AuthWorkspaceMembership{
      workspace_id: command.workspace_id,
      actor_id: command.actor_id,
      roles: ["workspace_admin"],
      status: "active",
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    })
  end

  defp ensure_platform_grant!(command) do
    case Repo.get(AuthPlatformGrant, command.actor_id) do
      nil ->
        Repo.insert!(%AuthPlatformGrant{
          actor_id: command.actor_id,
          roles: ["platform_admin"],
          status: "active",
          version: 1,
          inserted_at: command.occurred_at,
          updated_at: command.occurred_at
        })

      %AuthPlatformGrant{roles: roles, status: "active"} ->
        if "platform_admin" in roles,
          do: :ok,
          else: rollback_conflict!("administrator platform grant has different content")

      %AuthPlatformGrant{} ->
        rollback_conflict!("administrator platform grant has different content")
    end
  end

  defp ensure_authentication!(%{authentication_mode: :password} = command, :created) do
    Repo.insert!(%AuthCredential{
      actor_id: command.actor_id,
      password_hash: command.password_hash,
      algorithm: "argon2id",
      version: 1,
      changed_at: command.occurred_at,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    })
  end

  defp ensure_authentication!(%{authentication_mode: :password}, :existing) do
    rollback_conflict!("an existing password administrator cannot be reprovisioned")
  end

  defp ensure_authentication!(%{authentication_mode: :entra} = command, :created) do
    case Repo.get_by(AuthExternalIdentity,
           provider: @provider,
           tenant_id: command.tenant_id,
           subject_id: command.object_id
         ) do
      nil ->
        Repo.insert!(%AuthExternalIdentity{
          provider: @provider,
          tenant_id: command.tenant_id,
          subject_id: command.object_id,
          actor_id: command.actor_id,
          linked_at: command.occurred_at,
          inserted_at: command.occurred_at
        })

      %AuthExternalIdentity{actor_id: actor_id} when actor_id == command.actor_id ->
        :ok

      %AuthExternalIdentity{} ->
        rollback_conflict!("Entra identity is linked to another actor")
    end
  end

  defp ensure_authentication!(%{authentication_mode: :entra} = command, :existing) do
    credential = Repo.get(AuthCredential, command.actor_id)

    identity =
      Repo.get_by(AuthExternalIdentity,
        provider: @provider,
        tenant_id: command.tenant_id,
        subject_id: command.object_id,
        actor_id: command.actor_id
      )

    if is_nil(credential) and match?(%AuthExternalIdentity{}, identity),
      do: :ok,
      else: rollback_conflict!("existing actor does not have the exact selected Entra identity")
  end

  defp insert_audit!(command) do
    Repo.insert!(%AuthAuditEntry{
      workspace_id: command.workspace_id,
      command_id: command.operation_id,
      principal_id: command.platform_context.principal_id,
      action: "workspace.initial_administrator.provisioned",
      subject_kind: "actor",
      subject_id: command.actor_id,
      detail: %{
        "authentication_mode" => Atom.to_string(command.authentication_mode),
        "roles" => ["workspace_admin"]
      },
      occurred_at: command.occurred_at,
      inserted_at: command.occurred_at
    })

    Repo.insert!(%AuthPlatformAuditEntry{
      command_id: command.operation_id,
      principal_id: command.platform_context.principal_id,
      action: "workspace.provisioning.ready",
      subject_kind: "workspace",
      subject_id: command.workspace_id,
      detail: platform_audit_detail(command),
      occurred_at: command.occurred_at,
      inserted_at: command.occurred_at
    })
  end

  defp platform_audit_detail(command) do
    %{
      "actor_id" => command.actor_id,
      "authentication_mode" => Atom.to_string(command.authentication_mode),
      "platform_roles" => ["platform_admin"],
      "request_fingerprint" => command.request_fingerprint,
      "username" => command.username,
      "workspace_roles" => ["workspace_admin"]
    }
    |> maybe_add_identity_fingerprints(command)
  end

  defp maybe_add_identity_fingerprints(detail, %{authentication_mode: :password}), do: detail

  defp maybe_add_identity_fingerprints(detail, command) do
    detail
    |> Map.put("tenant_fingerprint", fingerprint(command.tenant_id))
    |> Map.put("object_fingerprint", fingerprint(command.object_id))
  end

  defp insert_operation!(command) do
    Repo.insert!(%WorkspaceProvisioningOperation{
      operation_id: command.operation_id,
      workspace_id: command.workspace_id,
      request_fingerprint: decode_fingerprint!(command.request_fingerprint),
      actor_id: command.actor_id,
      username: command.username,
      authentication_mode: Atom.to_string(command.authentication_mode),
      tenant_fingerprint: identity_fingerprint(command.tenant_id),
      object_fingerprint: identity_fingerprint(command.object_id),
      status: "ready",
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    })
  end

  defp ready_result!(operation, replayed?) do
    workspace = Repo.get(Workspace, operation.workspace_id)
    actor = Repo.get(AuthActor, operation.actor_id)

    membership =
      Repo.get_by(AuthWorkspaceMembership,
        workspace_id: operation.workspace_id,
        actor_id: operation.actor_id
      )

    grant = Repo.get(AuthPlatformGrant, operation.actor_id)
    credential = Repo.get(AuthCredential, operation.actor_id)

    external_identities =
      from(identity in AuthExternalIdentity,
        where: identity.actor_id == ^operation.actor_id,
        select: {identity.provider, identity.tenant_id, identity.subject_id}
      )
      |> Repo.all()

    if ready_state?(
         operation,
         workspace,
         actor,
         membership,
         grant,
         credential,
         external_identities
       ) do
      {:ok,
       %WorkspaceProvisioning{
         operation_id: operation.operation_id,
         workspace_id: operation.workspace_id,
         slug: workspace.slug,
         workspace_name: workspace.display_name,
         actor_id: operation.actor_id,
         username: operation.username,
         authentication_mode: String.to_existing_atom(operation.authentication_mode),
         workspace_roles: [:workspace_admin],
         platform_roles: [:platform_admin],
         status: :ready,
         replayed?: replayed?
       }}
    else
      {:error,
       Error.new(:constraint, "workspace provisioning receipt is not ready",
         details: %{reason_code: "workspace_provisioning_incomplete"}
       )}
    end
  end

  defp legacy_ready_result(workspace_id) do
    memberships =
      from(membership in AuthWorkspaceMembership,
        where:
          membership.workspace_id == ^workspace_id and membership.status == "active" and
            fragment("? @> ARRAY['workspace_admin']::text[]", membership.roles)
      )
      |> Repo.all()

    case memberships do
      [%AuthWorkspaceMembership{} = membership] ->
        legacy_actor_result(workspace_id, membership)

      _missing_or_ambiguous ->
        provisioning_not_found()
    end
  end

  defp legacy_actor_result(workspace_id, membership) do
    workspace = Repo.get(Workspace, workspace_id)
    actor = Repo.get(AuthActor, membership.actor_id)
    grant = Repo.get(AuthPlatformGrant, membership.actor_id)
    credential = Repo.get(AuthCredential, membership.actor_id)

    identities =
      from(identity in AuthExternalIdentity,
        where: identity.actor_id == ^membership.actor_id and identity.provider == @provider
      )
      |> Repo.all()

    authentication_mode = legacy_authentication_mode(credential, identities)

    if match?(%Workspace{status: "active"}, workspace) and
         match?(%AuthActor{status: "active"}, actor) and
         match?(%AuthPlatformGrant{status: "active"}, grant) and
         "workspace_admin" in membership.roles and "platform_admin" in grant.roles and
         authentication_mode in [:entra, :password] do
      {:ok,
       %WorkspaceProvisioning{
         operation_id: "legacy:" <> workspace_id,
         workspace_id: workspace_id,
         slug: workspace.slug,
         workspace_name: workspace.display_name,
         actor_id: actor.actor_id,
         username: actor.username,
         authentication_mode: authentication_mode,
         workspace_roles: [:workspace_admin],
         platform_roles: [:platform_admin],
         status: :ready,
         replayed?: false
       }}
    else
      provisioning_not_found()
    end
  end

  defp legacy_authentication_mode(_credential, [%AuthExternalIdentity{}]), do: :entra
  defp legacy_authentication_mode(%AuthCredential{algorithm: "argon2id"}, []), do: :password
  defp legacy_authentication_mode(_credential, _identities), do: nil

  defp provisioning_not_found do
    {:error,
     Error.new(:not_found, "workspace provisioning receipt not found",
       details: %{reason_code: "workspace_provisioning_not_found"}
     )}
  end

  defp ready_state?(
         %{status: "ready", authentication_mode: mode} = operation,
         %Workspace{status: "active"},
         %AuthActor{status: "active"},
         %AuthWorkspaceMembership{status: "active", roles: workspace_roles},
         %AuthPlatformGrant{status: "active", roles: platform_roles},
         credential,
         external_identities
       ) do
    "workspace_admin" in workspace_roles and "platform_admin" in platform_roles and
      authentication_ready?(mode, operation, credential, external_identities)
  end

  defp ready_state?(_operation, _workspace, _actor, _membership, _grant, _credential, _links),
    do: false

  defp authentication_ready?("password", _operation, credential, _external_identities),
    do: match?(%AuthCredential{algorithm: "argon2id"}, credential)

  defp authentication_ready?("entra", operation, _credential, external_identities) do
    Enum.any?(external_identities, fn
      {@provider, tenant_id, object_id} ->
        identity_fingerprint(tenant_id) == operation.tenant_fingerprint and
          identity_fingerprint(object_id) == operation.object_fingerprint

      _other ->
        false
    end)
  end

  defp authentication_ready?(_mode, _operation, _credential, _external_identities), do: false

  defp validate_command(command) do
    context = command.platform_context

    valid? =
      PlatformContext.valid?(context) and :platform_admin in context.roles and
        valid_id?(command.operation_id) and valid_fingerprint?(command.request_fingerprint) and
        Enum.all?(
          [
            command.workspace_id,
            command.slug,
            command.workspace_name,
            command.actor_id,
            command.username,
            command.display_name
          ],
          &valid_id?/1
        ) and command.authentication_mode in [:entra, :password] and
        valid_authentication?(command) and match?(%DateTime{}, command.occurred_at)

    if valid?,
      do: :ok,
      else: {:error, Error.new(:invalid, "invalid workspace provisioning command")}
  end

  defp validate_query(query) do
    if PlatformContext.valid?(query.platform_context) and
         :platform_admin in query.platform_context.roles and valid_id?(query.workspace_id),
       do: :ok,
       else: {:error, Error.new(:forbidden, "platform administrator authority required")}
  end

  defp valid_authentication?(%{
         authentication_mode: :password,
         password_hash: hash,
         tenant_id: nil,
         object_id: nil
       }),
       do: is_binary(hash) and String.starts_with?(hash, "$argon2")

  defp valid_authentication?(%{
         authentication_mode: :entra,
         password_hash: nil,
         tenant_id: tenant_id,
         object_id: object_id
       }),
       do: uuid?(tenant_id) and uuid?(object_id)

  defp valid_authentication?(_command), do: false

  defp valid_fingerprint?(value) when is_binary(value),
    do: Regex.match?(~r/\A[0-9a-f]{64}\z/, value)

  defp valid_fingerprint?(_value), do: false

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255
  defp uuid?(value) when is_binary(value), do: Regex.match?(@uuid_pattern, value)
  defp uuid?(_value), do: false

  defp decode_fingerprint!(fingerprint) do
    {:ok, decoded} = Base.decode16(fingerprint, case: :lower)
    decoded
  end

  defp creation_hash!(command) do
    {:ok, hash} =
      CanonicalJSON.hash(%{
        actor_id: command.actor_id,
        authentication_mode: command.authentication_mode,
        display_name: command.display_name,
        operation_id: command.operation_id,
        request_fingerprint: command.request_fingerprint,
        username: normalize_username(command.username),
        workspace_id: command.workspace_id
      })

    hash
  end

  defp fingerprint(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.url_encode64(padding: false)
  end

  defp identity_fingerprint(nil), do: nil
  defp identity_fingerprint(value), do: :crypto.hash(:sha256, value)

  defp normalize_username(username) do
    username |> String.trim() |> String.normalize(:nfkc) |> String.downcase()
  end

  defp checkpoint!(opts, stage) do
    case Keyword.get(opts, :after_step) do
      function when is_function(function, 1) ->
        case function.(stage) do
          :ok -> :ok
          {:error, reason} -> Repo.rollback(ErrorMapper.map(reason))
        end

      nil ->
        :ok
    end
  end

  defp unwrap!({:ok, result}), do: result
  defp unwrap!({:error, %Error{} = error}), do: Repo.rollback(error)

  defp rollback_conflict!(message) do
    Repo.rollback(
      Error.new(:conflict, message, details: %{reason_code: "workspace_provisioning_conflict"})
    )
  end
end
