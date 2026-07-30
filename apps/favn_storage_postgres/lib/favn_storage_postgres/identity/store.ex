defmodule FavnStoragePostgres.Identity.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.IdentityStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AttachActorMembership
  alias FavnOrchestrator.Persistence.Commands.BootstrapAdministrator
  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.CompleteOperatorCommand
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.RecordAudit
  alias FavnOrchestrator.Persistence.Commands.RecoverAdministratorCredential
  alias FavnOrchestrator.Persistence.Commands.ReserveOperatorCommand
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.RotateWorkspaceSession
  alias FavnOrchestrator.Persistence.Commands.SetActorAccess
  alias FavnOrchestrator.Persistence.Commands.SetActorStatus
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.ListActorMemberships
  alias FavnOrchestrator.Persistence.Queries.PageActors
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Queries.PageSessions
  alias FavnOrchestrator.Persistence.Results.Actor, as: ActorResult
  alias FavnOrchestrator.Persistence.Results.AuditEntry, as: AuditResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult
  alias FavnOrchestrator.Persistence.Results.WorkspaceMembership, as: WorkspaceMembershipResult
  alias FavnOrchestrator.Persistence.Selectors.ActorById
  alias FavnOrchestrator.Persistence.Selectors.ActorByUsername
  alias FavnOrchestrator.Persistence.Selectors.SessionById
  alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AuthActor
  alias FavnStoragePostgres.Schemas.AuthAuditEntry
  alias FavnStoragePostgres.Schemas.AuthCredential
  alias FavnStoragePostgres.Schemas.AuthPlatformAuditEntry
  alias FavnStoragePostgres.Schemas.AuthPlatformGrant
  alias FavnStoragePostgres.Schemas.AuthOperatorCommand
  alias FavnStoragePostgres.Schemas.AuthSession
  alias FavnStoragePostgres.Schemas.AuthWorkspaceMembership
  alias FavnStoragePostgres.Schemas.Workspace

  @workspace_roles [:customer_reader, :customer_operator, :workspace_admin]
  @platform_roles [:platform_reader, :platform_operator, :platform_admin]
  @access_statuses [:active, :suspended, :revoked]
  @impl true
  def create_actor(%CreateActor{} = command) do
    with :ok <- validate_create_actor(command) do
      transaction(fn -> create_actor!(command) end)
    end
  end

  @impl true
  def get_actor(%GetActor{} = query) do
    with :ok <- validate_get_actor(query) do
      case actor_query(query.workspace_context.workspace_id, query.selector) |> Repo.one() do
        nil -> {:error, Error.new(:not_found, "actor membership not found")}
        tuple -> {:ok, actor_result(tuple)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_actors(%PageActors{} = page) do
    with :ok <- validate_page_actors(page) do
      workspace_id = page.workspace_context.workspace_id

      query =
        from(actor in AuthActor,
          join: membership in AuthWorkspaceMembership,
          on: membership.actor_id == actor.actor_id,
          left_join: credential in AuthCredential,
          on: credential.actor_id == actor.actor_id,
          where: membership.workspace_id == ^workspace_id,
          order_by: [asc: actor.actor_id],
          limit: ^(page.limit + 1),
          select: {actor, membership, credential}
        )
        |> actor_status(page.status)
        |> after_actor(page.after)

      rows = Repo.all(query)
      page_rows = Enum.take(rows, page.limit)
      items = Enum.map(page_rows, &actor_result/1)
      has_more? = length(rows) > page.limit
      last = List.last(items)

      {:ok,
       %CursorPage{
         items: items,
         limit: page.limit,
         has_more?: has_more?,
         next_cursor: if(has_more? and last, do: %{actor_id: last.actor_id})
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def list_actor_memberships(%ListActorMemberships{} = query) do
    with :ok <- validate_list_actor_memberships(query) do
      memberships =
        from(membership in AuthWorkspaceMembership,
          join: workspace in Workspace,
          on: workspace.workspace_id == membership.workspace_id,
          where: membership.actor_id == ^query.actor_id,
          order_by: [asc: workspace.display_name, asc: workspace.workspace_id],
          select: {membership, workspace}
        )
        |> Repo.all()
        |> Enum.map(&membership_result/1)

      {:ok, memberships}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def set_access(%SetActorAccess{} = command) do
    with :ok <- validate_set_access(command) do
      transaction(fn -> set_access!(command) end)
    end
  end

  @impl true
  def attach_actor_membership(%AttachActorMembership{} = command) do
    with :ok <- validate_attach_actor_membership(command) do
      transaction(fn -> attach_actor_membership!(command) end)
    end
  end

  @impl true
  def bootstrap_administrator(%BootstrapAdministrator{} = command) do
    with :ok <- validate_bootstrap_administrator(command) do
      transaction(fn -> bootstrap_administrator!(command) end)
    end
  end

  @impl true
  def recover_administrator_credential(%RecoverAdministratorCredential{} = command) do
    with :ok <- validate_recover_administrator_credential(command) do
      transaction(fn -> recover_administrator_credential!(command) end)
    end
  end

  @impl true
  def set_actor_status(%SetActorStatus{} = command) do
    with :ok <- validate_set_actor_status(command),
         {:ok, :ok} <- transaction(fn -> set_actor_status!(command) end) do
      :ok
    end
  end

  @impl true
  def change_password(%ChangeActorPassword{} = command) do
    with :ok <- validate_change_password(command),
         {:ok, :ok} <- transaction(fn -> change_password!(command) end) do
      :ok
    end
  end

  @impl true
  def create_session(%CreateSession{} = command) do
    with :ok <- validate_create_session(command) do
      transaction(fn -> create_session!(command) end)
    end
  end

  @impl true
  def rotate_workspace_session(%RotateWorkspaceSession{} = command) do
    with :ok <- validate_rotate_workspace_session(command) do
      transaction(fn -> rotate_workspace_session!(command) end)
    end
  end

  @impl true
  def get_session(%GetSession{} = query) do
    with :ok <- validate_get_session(query) do
      workspace_id = query.workspace_context.workspace_id

      case session_query(workspace_id, query.selector) |> Repo.one() do
        nil -> {:error, Error.new(:not_found, "session not found")}
        session -> {:ok, session_result(session)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_sessions(%PageSessions{} = page) do
    with :ok <- validate_page_sessions(page) do
      workspace_id = page.workspace_context.workspace_id

      rows =
        AuthSession
        |> where([session], session.workspace_id == ^workspace_id)
        |> session_actor(page.actor_id)
        |> session_status(page.status)
        |> after_session(page.after)
        |> order_by([session], desc: session.inserted_at, desc: session.session_id)
        |> limit(^(page.limit + 1))
        |> Repo.all()

      page_rows = Enum.take(rows, page.limit)
      items = Enum.map(page_rows, &session_result/1)
      has_more? = length(rows) > page.limit
      last = List.last(items)

      {:ok,
       %CursorPage{
         items: items,
         limit: page.limit,
         has_more?: has_more?,
         next_cursor:
           if(has_more? and last,
             do: %{inserted_at: last.issued_at, session_id: last.session_id}
           )
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def revoke_sessions(%RevokeSessions{} = command) do
    with :ok <- validate_revoke(command),
         {:ok, :ok} <- transaction(fn -> revoke_sessions!(command) end) do
      :ok
    end
  end

  @impl true
  def record_audit(%RecordAudit{} = command) do
    with :ok <- validate_record_audit(command),
         {:ok, :ok} <- transaction(fn -> record_audit!(command) end) do
      :ok
    end
  end

  @impl true
  def reserve_operator_command(%ReserveOperatorCommand{} = command) do
    with :ok <- validate_reserve_operator_command(command) do
      transaction(fn -> reserve_operator_command!(command) end)
    end
  end

  @impl true
  def complete_operator_command(%CompleteOperatorCommand{} = command) do
    with :ok <- validate_complete_operator_command(command),
         {:ok, :ok} <- transaction(fn -> complete_operator_command!(command) end) do
      :ok
    end
  end

  @impl true
  def page_audit(%PageAudit{} = page) do
    with :ok <- validate_page_audit(page) do
      rows = page_audit_rows(page)
      page_rows = Enum.take(rows, page.limit)
      items = Enum.map(page_rows, &audit_result/1)
      has_more? = length(rows) > page.limit
      last = List.last(items)

      {:ok,
       %CursorPage{
         items: items,
         limit: page.limit,
         has_more?: has_more?,
         next_cursor: if(has_more? and last, do: %{audit_id: last.audit_id})
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp create_actor!(command) do
    workspace_id = command.workspace_context.workspace_id
    normalized_username = normalize_username(command.username)
    creation_hash = creation_hash!(command, normalized_username)

    existing =
      from(actor in AuthActor,
        where:
          actor.actor_id == ^command.actor_id or
            actor.normalized_username == ^normalized_username or
            actor.creation_command_id == ^command.command_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      not is_nil(existing) and existing.actor_id == command.actor_id and
        existing.creation_command_id == command.command_id and
          existing.creation_hash == creation_hash ->
        load_actor!(workspace_id, command.actor_id)

      existing ->
        Repo.rollback(Error.new(:conflict, "actor identity or username already exists"))

      true ->
        insert_actor!(command, normalized_username, creation_hash)
    end
  end

  defp insert_actor!(command, normalized_username, creation_hash) do
    workspace_id = command.workspace_context.workspace_id

    %AuthActor{
      actor_id: command.actor_id,
      username: command.username,
      normalized_username: normalized_username,
      display_name: command.display_name,
      creation_command_id: command.command_id,
      creation_hash: creation_hash,
      status: "active",
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()

    %AuthCredential{
      actor_id: command.actor_id,
      password_hash: command.password_hash,
      algorithm: "argon2id",
      version: 1,
      changed_at: command.occurred_at,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()

    %AuthWorkspaceMembership{
      workspace_id: workspace_id,
      actor_id: command.actor_id,
      roles: role_strings(command.roles),
      status: "active",
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()

    workspace_audit!(
      command.workspace_context,
      command.command_id,
      "actor.created",
      command.actor_id,
      %{
        "roles" => role_strings(command.roles)
      },
      command.occurred_at
    )

    load_actor!(workspace_id, command.actor_id)
  end

  defp bootstrap_administrator!(command) do
    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext('favn.admin.bootstrap'))", [])

    if administrator_exists?() do
      Repo.rollback(
        Error.new(:conflict, "administrator bootstrap is already complete",
          details: %{reason_code: "administrator_already_exists"}
        )
      )
    end

    existing_workspace_ids =
      from(workspace in Workspace,
        where: workspace.workspace_id in ^command.workspace_ids,
        select: workspace.workspace_id
      )
      |> Repo.all()
      |> Enum.sort()

    if existing_workspace_ids != command.workspace_ids do
      Repo.rollback(
        Error.new(:not_found, "one or more bootstrap workspaces do not exist",
          details: %{reason_code: "bootstrap_workspace_not_found"}
        )
      )
    end

    [first_workspace_id | remaining_workspace_ids] = command.workspace_ids

    {:ok, first_context} =
      WorkspaceContext.new(
        first_workspace_id,
        command.platform_context.principal_id,
        [:workspace_admin]
      )

    actor =
      create_actor!(%CreateActor{
        workspace_context: first_context,
        command_id: command.command_id,
        actor_id: command.actor_id,
        username: command.username,
        display_name: command.display_name,
        password_hash: command.password_hash,
        roles: [:workspace_admin],
        occurred_at: command.occurred_at
      })

    Enum.each(remaining_workspace_ids, fn workspace_id ->
      {:ok, context} =
        WorkspaceContext.new(
          workspace_id,
          command.platform_context.principal_id,
          [:workspace_admin]
        )

      %AuthWorkspaceMembership{
        workspace_id: workspace_id,
        actor_id: command.actor_id,
        roles: ["workspace_admin"],
        status: "active",
        version: 1,
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()

      workspace_audit!(
        context,
        command.command_id,
        "actor.created",
        command.actor_id,
        %{"roles" => ["workspace_admin"]},
        command.occurred_at
      )
    end)

    %AuthPlatformGrant{
      actor_id: command.actor_id,
      roles: ["platform_admin"],
      status: "active",
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()

    platform_audit!(
      command.platform_context,
      command.command_id,
      "administrator.bootstrapped",
      command.actor_id,
      %{
        "username" => command.username,
        "workspace_ids" => command.workspace_ids,
        "workspace_roles" => ["workspace_admin"],
        "platform_roles" => ["platform_admin"]
      },
      command.occurred_at
    )

    actor
  end

  defp recover_administrator_credential!(command) do
    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtext('favn.admin.recover'))", [])

    normalized_username = normalize_username(command.username)

    actor =
      from(actor in AuthActor,
        where: actor.normalized_username == ^normalized_username,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    if is_nil(actor) do
      Repo.rollback(
        Error.new(:not_found, "administrator not found",
          details: %{reason_code: "administrator_not_found"}
        )
      )
    end

    unless administrator?(actor.actor_id) do
      Repo.rollback(
        Error.new(:forbidden, "credential recovery is restricted to administrators",
          details: %{reason_code: "administrator_required"}
        )
      )
    end

    credential =
      from(credential in AuthCredential,
        where: credential.actor_id == ^actor.actor_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one!()

    fingerprint = hash!(command.password_hash) |> Base.url_encode64(padding: false)

    expected_detail = %{
      "credential_fingerprint" => fingerprint,
      "credential_version" => credential.version + 1,
      "actor_reactivated" => actor.status != "active",
      "sessions_revoked" => true
    }

    case platform_audit_by_command(
           command.command_id,
           "administrator.credential.recovered"
         ) do
      %AuthPlatformAuditEntry{
        subject_id: actor_id,
        detail: ^expected_detail
      }
      when actor_id == actor.actor_id ->
        actor.actor_id

      %AuthPlatformAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "administrator recovery command changed"))

      nil ->
        actor
        |> Ecto.Changeset.change(%{
          status: "active",
          version: actor.version + 1,
          updated_at: command.occurred_at
        })
        |> Repo.update!()

        credential
        |> Ecto.Changeset.change(%{
          password_hash: command.password_hash,
          version: credential.version + 1,
          changed_at: command.occurred_at,
          updated_at: command.occurred_at
        })
        |> Repo.update!()

        revoke_actor_sessions!(actor.actor_id, command.occurred_at)

        platform_audit!(
          command.platform_context,
          command.command_id,
          "administrator.credential.recovered",
          actor.actor_id,
          expected_detail,
          command.occurred_at
        )

        actor.actor_id
    end
  end

  defp administrator_exists? do
    %{rows: [[exists?]]} =
      SQL.query!(
        Repo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM favn_control.auth_workspace_memberships
          WHERE 'workspace_admin' = ANY(roles)
          UNION ALL
          SELECT 1
          FROM favn_control.auth_platform_grants
          WHERE 'platform_admin' = ANY(roles)
        )
        """,
        []
      )

    exists?
  end

  defp administrator?(actor_id) do
    %{rows: [[administrator?]]} =
      SQL.query!(
        Repo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM favn_control.auth_workspace_memberships
          WHERE actor_id = $1 AND 'workspace_admin' = ANY(roles)
          UNION ALL
          SELECT 1
          FROM favn_control.auth_platform_grants
          WHERE actor_id = $1 AND 'platform_admin' = ANY(roles)
        )
        """,
        [actor_id]
      )

    administrator?
  end

  defp set_access!(%{scope_kind: :workspace} = command) do
    workspace_id = command.workspace_id
    actor = lock_actor!(command.actor_id)

    locked_memberships =
      from(membership in AuthWorkspaceMembership,
        where: membership.workspace_id == ^workspace_id,
        order_by: [asc: membership.actor_id],
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    membership =
      Enum.find(locked_memberships, &(&1.actor_id == command.actor_id))

    expected_detail = access_audit_detail(command)

    case audit_by_command(workspace_id, command.command_id, "access.workspace.changed") do
      %AuthAuditEntry{detail: ^expected_detail} ->
        load_actor!(workspace_id, command.actor_id)

      %AuthAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "access command has different content"))

      nil ->
        preserve_last_workspace_admin!(membership, locked_memberships, command)
        updated = upsert_workspace_access!(membership, actor, command)

        if command.status != :active do
          revoke_workspace_actor_sessions!(
            workspace_id,
            actor.actor_id,
            command.occurred_at
          )
        end

        workspace_audit!(
          command.authority,
          command.command_id,
          "access.workspace.changed",
          actor.actor_id,
          access_audit_detail(updated),
          command.occurred_at
        )

        load_actor!(workspace_id, actor.actor_id)
    end
  end

  defp set_access!(%{scope_kind: :platform} = command) do
    actor = lock_actor!(command.actor_id)

    grant =
      from(grant in AuthPlatformGrant,
        where: grant.actor_id == ^command.actor_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    expected_detail = access_audit_detail(command)

    case platform_audit_by_command(command.command_id, "access.platform.changed") do
      %AuthPlatformAuditEntry{detail: ^expected_detail} ->
        platform_actor_result(actor, grant || Repo.get!(AuthPlatformGrant, actor.actor_id))

      %AuthPlatformAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "access command has different content"))

      nil ->
        updated = upsert_platform_access!(grant, actor, command)

        platform_audit!(
          command.authority,
          command.command_id,
          "access.platform.changed",
          actor.actor_id,
          access_audit_detail(updated),
          command.occurred_at
        )

        platform_actor_result(actor, updated)
    end
  end

  defp attach_actor_membership!(command) do
    workspace_id = command.workspace_context.workspace_id
    normalized_username = normalize_username(command.username)

    actor =
      from(actor in AuthActor,
        where: actor.normalized_username == ^normalized_username,
        lock: "FOR UPDATE"
      )
      |> Repo.one()
      |> case do
        %AuthActor{status: "active"} = actor -> actor
        _missing_or_disabled -> Repo.rollback(Error.new(:not_found, "actor not available"))
      end

    locked_memberships =
      from(membership in AuthWorkspaceMembership,
        where: membership.workspace_id == ^workspace_id,
        order_by: [asc: membership.actor_id],
        lock: "FOR UPDATE"
      )
      |> Repo.all()

    existing = Enum.find(locked_memberships, &(&1.actor_id == actor.actor_id))

    expected_detail = %{
      "roles" => role_strings(command.roles),
      "status" => "active",
      "version" => 1
    }

    case audit_by_command(workspace_id, command.command_id, "actor.membership.attached") do
      %AuthAuditEntry{subject_id: actor_id, detail: ^expected_detail}
      when actor_id == actor.actor_id ->
        load_actor!(workspace_id, actor.actor_id)

      %AuthAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "membership command has different content"))

      nil when not is_nil(existing) ->
        Repo.rollback(Error.new(:conflict, "actor already belongs to workspace"))

      nil ->
        %AuthWorkspaceMembership{
          workspace_id: workspace_id,
          actor_id: actor.actor_id,
          roles: role_strings(command.roles),
          status: "active",
          version: 1,
          inserted_at: command.occurred_at,
          updated_at: command.occurred_at
        }
        |> Repo.insert!()

        workspace_audit!(
          command.workspace_context,
          command.command_id,
          "actor.membership.attached",
          actor.actor_id,
          expected_detail,
          command.occurred_at
        )

        load_actor!(workspace_id, actor.actor_id)
    end
  end

  defp set_actor_status!(command) do
    actor = lock_actor!(command.actor_id)

    expected_detail = %{
      "status" => Atom.to_string(command.status),
      "version" => command.expected_version + 1,
      "sessions_revoked" => command.status == :disabled
    }

    case platform_audit_by_command(command.command_id, "actor.status.changed") do
      %AuthPlatformAuditEntry{detail: ^expected_detail} ->
        :ok

      %AuthPlatformAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "actor status command has different content"))

      nil ->
        if actor.version != command.expected_version do
          Repo.rollback(Error.new(:conflict, "actor version changed"))
        end

        actor
        |> Ecto.Changeset.change(%{
          status: if(command.status == :disabled, do: "suspended", else: "active"),
          version: actor.version + 1,
          updated_at: command.occurred_at
        })
        |> Repo.update!()

        if command.status == :disabled do
          revoke_actor_sessions!(command.actor_id, command.occurred_at)
        end

        platform_audit!(
          command.platform_context,
          command.command_id,
          "actor.status.changed",
          command.actor_id,
          expected_detail,
          command.occurred_at
        )

        :ok
    end
  end

  defp upsert_workspace_access!(nil, actor, %{expected_version: 0} = command) do
    %AuthWorkspaceMembership{
      workspace_id: command.workspace_id,
      actor_id: actor.actor_id,
      roles: role_strings(command.roles),
      status: Atom.to_string(command.status),
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()
  end

  defp upsert_workspace_access!(nil, _actor, _command),
    do: Repo.rollback(Error.new(:conflict, "workspace membership does not exist"))

  defp upsert_workspace_access!(membership, _actor, command) do
    if membership.version == command.expected_version do
      membership
      |> Ecto.Changeset.change(%{
        roles: role_strings(command.roles),
        status: Atom.to_string(command.status),
        version: membership.version + 1,
        updated_at: command.occurred_at
      })
      |> Repo.update!()
    else
      Repo.rollback(Error.new(:conflict, "workspace membership version changed"))
    end
  end

  defp upsert_platform_access!(nil, actor, %{expected_version: 0} = command) do
    %AuthPlatformGrant{
      actor_id: actor.actor_id,
      roles: role_strings(command.roles),
      status: Atom.to_string(command.status),
      version: 1,
      inserted_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()
  end

  defp upsert_platform_access!(nil, _actor, _command),
    do: Repo.rollback(Error.new(:conflict, "platform grant does not exist"))

  defp upsert_platform_access!(grant, _actor, command) do
    if grant.version == command.expected_version do
      grant
      |> Ecto.Changeset.change(%{
        roles: role_strings(command.roles),
        status: Atom.to_string(command.status),
        version: grant.version + 1,
        updated_at: command.occurred_at
      })
      |> Repo.update!()
    else
      Repo.rollback(Error.new(:conflict, "platform grant version changed"))
    end
  end

  defp change_password!(command) do
    workspace_id = command.workspace_context.workspace_id
    ensure_membership!(workspace_id, command.actor_id)

    fingerprint = hash!(command.password_hash) |> Base.url_encode64(padding: false)

    case audit_by_command(workspace_id, command.command_id, "actor.password.changed") do
      %AuthAuditEntry{detail: %{"password_fingerprint" => ^fingerprint}} ->
        :ok

      %AuthAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "password command has different content"))

      nil ->
        credential =
          from(credential in AuthCredential,
            where: credential.actor_id == ^command.actor_id,
            lock: "FOR UPDATE"
          )
          |> Repo.one!()

        if credential.version != command.expected_credential_version do
          Repo.rollback(Error.new(:conflict, "credential version changed"))
        end

        credential
        |> Ecto.Changeset.change(%{
          password_hash: command.password_hash,
          version: credential.version + 1,
          changed_at: command.occurred_at,
          updated_at: command.occurred_at
        })
        |> Repo.update!()

        if command.revoke_sessions? do
          revoke_actor_sessions!(command.actor_id, command.occurred_at)
        end

        workspace_audit!(
          command.workspace_context,
          command.command_id,
          "actor.password.changed",
          command.actor_id,
          %{
            "password_fingerprint" => fingerprint,
            "sessions_revoked" => command.revoke_sessions?
          },
          command.occurred_at
        )

        :ok
    end
  end

  defp create_session!(command) do
    workspace_id = command.workspace_context.workspace_id
    ensure_active_actor!(command.actor_id)
    ensure_membership!(workspace_id, command.actor_id)

    existing =
      from(session in AuthSession,
        where:
          session.session_id == ^command.session_id or session.token_hash == ^command.token_hash or
            session.creation_command_id == ^command.command_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      not is_nil(existing) and existing.session_id == command.session_id and
        existing.actor_id == command.actor_id and existing.token_hash == command.token_hash and
        existing.workspace_id == workspace_id and existing.expires_at == command.expires_at and
          existing.creation_command_id == command.command_id ->
        session_result(existing)

      existing ->
        Repo.rollback(Error.new(:conflict, "session identity has different content"))

      true ->
        ensure_credential_version!(command.actor_id, command.expected_credential_version)

        session =
          %AuthSession{
            session_id: command.session_id,
            actor_id: command.actor_id,
            workspace_id: workspace_id,
            creation_command_id: command.command_id,
            token_hash: command.token_hash,
            provider: command.provider,
            status: "active",
            expires_at: command.expires_at,
            inserted_at: command.occurred_at,
            updated_at: command.occurred_at
          }
          |> Repo.insert!()

        workspace_audit!(
          command.workspace_context,
          command.command_id,
          "session.created",
          command.session_id,
          %{"actor_id" => command.actor_id, "workspace_id" => workspace_id},
          command.occurred_at,
          "session"
        )

        session_result(session)
    end
  end

  defp preserve_last_workspace_admin!(
         %AuthWorkspaceMembership{status: "active", roles: roles} = membership,
         memberships,
         command
       ) do
    removes_admin? =
      "workspace_admin" in roles and
        (command.status != :active or :workspace_admin not in command.roles)

    if removes_admin? do
      other_admins =
        Enum.count(memberships, fn other ->
          other.actor_id != membership.actor_id and other.status == "active" and
            "workspace_admin" in other.roles
        end)

      if other_admins == 0 do
        Repo.rollback(Error.new(:conflict, "workspace must retain an active administrator"))
      end
    end

    :ok
  end

  defp preserve_last_workspace_admin!(_membership, _memberships, _command), do: :ok

  defp rotate_workspace_session!(command) do
    source_workspace_id = command.source_context.workspace_id
    source = lock_session!(command.source_session_id)

    cond do
      source.actor_id != command.actor_id or
          command.source_context.principal_id != command.actor_id ->
        Repo.rollback(Error.new(:forbidden, "session actor mismatch"))

      source.workspace_id != source_workspace_id or source.status != "active" or
          not future?(source.expires_at) ->
        Repo.rollback(Error.new(:forbidden, "source session is not active"))

      source_workspace_id == command.target_workspace_id ->
        Repo.rollback(Error.new(:invalid, "target workspace is already active"))

      true ->
        :ok
    end

    ensure_active_actor!(command.actor_id)
    ensure_membership!(command.target_workspace_id, command.actor_id)

    existing =
      from(session in AuthSession,
        where:
          session.session_id == ^command.session_id or session.token_hash == ^command.token_hash or
            session.creation_command_id == ^command.command_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    if existing do
      Repo.rollback(Error.new(:conflict, "workspace switch session already exists"))
    end

    target =
      %AuthSession{
        session_id: command.session_id,
        actor_id: command.actor_id,
        workspace_id: command.target_workspace_id,
        creation_command_id: command.command_id,
        token_hash: command.token_hash,
        provider: command.provider,
        status: "active",
        expires_at: command.expires_at,
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()

    revoke_session_row!(source, command.occurred_at)

    source_context = command.source_context

    {:ok, target_context} =
      WorkspaceContext.new(
        command.target_workspace_id,
        command.actor_id,
        [:customer_reader],
        request_id: command.session_id
      )

    detail = %{
      "actor_id" => command.actor_id,
      "source_workspace_id" => source_workspace_id,
      "target_workspace_id" => command.target_workspace_id,
      "source_session_id" => command.source_session_id,
      "target_session_id" => command.session_id
    }

    workspace_audit!(
      source_context,
      command.command_id,
      "session.workspace_switched",
      command.source_session_id,
      detail,
      command.occurred_at,
      "session"
    )

    workspace_audit!(
      target_context,
      command.command_id,
      "session.workspace_switched",
      command.session_id,
      detail,
      command.occurred_at,
      "session"
    )

    session_result(target)
  end

  defp revoke_sessions!(command) do
    workspace_id = command.workspace_context.workspace_id

    if audit_replay?(workspace_id, command.command_id, "session.revoked") do
      :ok
    else
      {actor_id, subject_id} =
        case {command.session_id, command.actor_id} do
          {session_id, nil} ->
            session = lock_session!(session_id)
            ensure_session_workspace!(session, workspace_id)
            ensure_membership!(workspace_id, session.actor_id)
            revoke_session_row!(session, command.occurred_at)
            {session.actor_id, session_id}

          {nil, actor_id} ->
            ensure_membership!(workspace_id, actor_id)
            revoke_workspace_actor_sessions!(workspace_id, actor_id, command.occurred_at)
            {actor_id, actor_id}
        end

      workspace_audit!(
        command.workspace_context,
        command.command_id,
        "session.revoked",
        subject_id,
        %{"actor_id" => actor_id},
        command.occurred_at,
        "session"
      )

      :ok
    end
  end

  defp revoke_actor_sessions!(actor_id, occurred_at) do
    from(session in AuthSession,
      where: session.actor_id == ^actor_id and session.status == "active"
    )
    |> Repo.update_all(set: [status: "revoked", revoked_at: occurred_at, updated_at: occurred_at])

    :ok
  end

  defp revoke_workspace_actor_sessions!(workspace_id, actor_id, occurred_at) do
    from(session in AuthSession,
      where:
        session.workspace_id == ^workspace_id and session.actor_id == ^actor_id and
          session.status == "active"
    )
    |> Repo.update_all(set: [status: "revoked", revoked_at: occurred_at, updated_at: occurred_at])

    :ok
  end

  defp revoke_session_row!(%AuthSession{status: "active"} = session, occurred_at) do
    session
    |> Ecto.Changeset.change(%{
      status: "revoked",
      revoked_at: occurred_at,
      updated_at: occurred_at
    })
    |> Repo.update!()
  end

  defp revoke_session_row!(_session, _occurred_at), do: :ok

  defp lock_actor!(actor_id) do
    from(actor in AuthActor, where: actor.actor_id == ^actor_id, lock: "FOR UPDATE")
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "actor not found"))
      actor -> actor
    end
  end

  defp ensure_active_actor!(actor_id) do
    case lock_actor!(actor_id) do
      %AuthActor{status: "active"} = actor -> actor
      _actor -> Repo.rollback(Error.new(:forbidden, "actor is disabled"))
    end
  end

  defp lock_session!(session_id) do
    from(session in AuthSession, where: session.session_id == ^session_id, lock: "FOR UPDATE")
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "session not found"))
      session -> session
    end
  end

  defp ensure_membership!(workspace_id, actor_id) do
    case Repo.get_by(AuthWorkspaceMembership, workspace_id: workspace_id, actor_id: actor_id) do
      %AuthWorkspaceMembership{status: "active"} = membership -> membership
      _other -> Repo.rollback(Error.new(:not_found, "active actor membership not found"))
    end
  end

  defp ensure_credential_version!(_actor_id, nil), do: :ok

  defp ensure_credential_version!(actor_id, expected_version) do
    credential =
      from(credential in AuthCredential,
        where: credential.actor_id == ^actor_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    case credential do
      %AuthCredential{version: ^expected_version} ->
        :ok

      _credential ->
        Repo.rollback(
          Error.new(:conflict, "credential changed before session issuance",
            details: %{reason_code: "credential_version_changed"}
          )
        )
    end
  end

  defp ensure_session_workspace!(%AuthSession{workspace_id: workspace_id}, workspace_id), do: :ok

  defp ensure_session_workspace!(_session, _workspace_id),
    do: Repo.rollback(Error.new(:not_found, "session not found"))

  defp load_actor!(workspace_id, actor_id) do
    case actor_query(workspace_id, %ActorById{actor_id: actor_id}) |> Repo.one() do
      nil -> Repo.rollback(Error.new(:not_found, "actor membership not found"))
      tuple -> actor_result(tuple)
    end
  end

  defp actor_query(workspace_id, selector) do
    from(actor in AuthActor,
      join: membership in AuthWorkspaceMembership,
      on: membership.actor_id == actor.actor_id,
      left_join: credential in AuthCredential,
      on: credential.actor_id == actor.actor_id,
      where: membership.workspace_id == ^workspace_id,
      select: {actor, membership, credential}
    )
    |> select_actor(selector)
  end

  defp select_actor(query, %ActorById{actor_id: actor_id}),
    do: where(query, [actor, _membership, _credential], actor.actor_id == ^actor_id)

  defp select_actor(query, %ActorByUsername{username: username}) do
    normalized = normalize_username(username)

    where(
      query,
      [actor, _membership, _credential],
      actor.normalized_username == ^normalized
    )
  end

  defp session_query(workspace_id, selector) do
    from(session in AuthSession,
      join: membership in AuthWorkspaceMembership,
      on: membership.actor_id == session.actor_id,
      where:
        session.workspace_id == ^workspace_id and membership.workspace_id == ^workspace_id and
          membership.status == "active",
      select: session
    )
    |> select_session(selector)
  end

  defp select_session(query, %SessionById{session_id: session_id}),
    do: where(query, [session, _membership], session.session_id == ^session_id)

  defp select_session(query, %SessionByTokenHash{token_hash: token_hash}),
    do: where(query, [session, _membership], session.token_hash == ^token_hash)

  defp actor_result({actor, membership, credential}) do
    %ActorResult{
      actor_id: actor.actor_id,
      username: actor.username,
      display_name: actor.display_name,
      status: String.to_existing_atom(actor.status),
      workspace_id: membership.workspace_id,
      membership_status: String.to_existing_atom(membership.status),
      roles: Enum.map(membership.roles, &String.to_existing_atom/1),
      credential_hash: credential && credential.password_hash,
      credential_version: credential && credential.version,
      access_version: membership.version,
      version: actor.version
    }
  end

  defp membership_result({membership, workspace}) do
    %WorkspaceMembershipResult{
      workspace_id: membership.workspace_id,
      workspace_slug: workspace.slug,
      workspace_name: workspace.display_name,
      roles: Enum.map(membership.roles, &String.to_existing_atom/1),
      status: String.to_existing_atom(membership.status),
      version: membership.version
    }
  end

  defp platform_actor_result(actor, grant) do
    credential = Repo.get(AuthCredential, actor.actor_id)

    %ActorResult{
      actor_id: actor.actor_id,
      username: actor.username,
      display_name: actor.display_name,
      status: String.to_existing_atom(actor.status),
      workspace_id: nil,
      membership_status: String.to_existing_atom(grant.status),
      roles: Enum.map(grant.roles, &String.to_existing_atom/1),
      credential_hash: credential && credential.password_hash,
      credential_version: credential && credential.version,
      access_version: grant.version,
      version: actor.version
    }
  end

  defp session_result(session) do
    status =
      if session.status == "active" and not future?(session.expires_at),
        do: :expired,
        else: String.to_existing_atom(session.status)

    %SessionResult{
      session_id: session.session_id,
      actor_id: session.actor_id,
      workspace_id: session.workspace_id,
      provider: session.provider,
      issued_at: session.inserted_at,
      status: status,
      expires_at: session.expires_at,
      revoked_at: session.revoked_at,
      last_seen_at: session.last_seen_at
    }
  end

  defp page_audit_rows(%{scope: %WorkspaceContext{} = context} = page) do
    AuthAuditEntry
    |> where([entry], entry.workspace_id == ^context.workspace_id)
    |> after_audit(page.after)
    |> order_by([entry], desc: entry.audit_id)
    |> limit(^(page.limit + 1))
    |> Repo.all()
  end

  defp page_audit_rows(%{scope: %PlatformContext{}} = page) do
    AuthPlatformAuditEntry
    |> after_audit(page.after)
    |> order_by([entry], desc: entry.audit_id)
    |> limit(^(page.limit + 1))
    |> Repo.all()
  end

  defp audit_result(%AuthAuditEntry{} = entry) do
    %AuditResult{
      audit_id: entry.audit_id,
      workspace_id: entry.workspace_id,
      principal_id: entry.principal_id,
      action: entry.action,
      subject_kind: entry.subject_kind,
      subject_id: entry.subject_id,
      detail: entry.detail,
      occurred_at: entry.occurred_at
    }
  end

  defp audit_result(%AuthPlatformAuditEntry{} = entry) do
    %AuditResult{
      audit_id: entry.audit_id,
      workspace_id: nil,
      principal_id: entry.principal_id,
      action: entry.action,
      subject_kind: entry.subject_kind,
      subject_id: entry.subject_id,
      detail: entry.detail,
      occurred_at: entry.occurred_at
    }
  end

  defp workspace_audit!(
         context,
         command_id,
         action,
         subject_id,
         detail,
         occurred_at,
         subject_kind \\ "actor"
       ) do
    %AuthAuditEntry{
      workspace_id: context.workspace_id,
      command_id: command_id,
      principal_id: context.principal_id,
      action: action,
      subject_kind: subject_kind,
      subject_id: subject_id,
      detail: Redaction.redact(detail),
      occurred_at: occurred_at,
      inserted_at: occurred_at
    }
    |> Repo.insert!()
  end

  defp record_audit!(%RecordAudit{scope: %WorkspaceContext{} = context} = command) do
    detail = normalized_audit_detail!(command.detail)

    case audit_by_command(context.workspace_id, command.command_id, command.action) do
      %AuthAuditEntry{
        principal_id: principal_id,
        subject_kind: subject_kind,
        subject_id: subject_id,
        detail: existing_detail
      }
      when principal_id == context.principal_id and subject_kind == command.subject_kind and
             subject_id == command.subject_id and existing_detail == detail ->
        :ok

      %AuthAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "audit command has different content"))

      nil ->
        %AuthAuditEntry{
          workspace_id: context.workspace_id,
          command_id: command.command_id,
          principal_id: context.principal_id,
          action: command.action,
          subject_kind: command.subject_kind,
          subject_id: command.subject_id,
          detail: detail,
          occurred_at: command.occurred_at,
          inserted_at: command.occurred_at
        }
        |> Repo.insert!(
          on_conflict: :nothing,
          conflict_target: [:workspace_id, :command_id, :action]
        )

        record_audit!(command)
    end
  end

  defp record_audit!(%RecordAudit{scope: %PlatformContext{} = context} = command) do
    detail = normalized_audit_detail!(command.detail)

    case platform_audit_by_command(command.command_id, command.action) do
      %AuthPlatformAuditEntry{
        principal_id: principal_id,
        subject_kind: subject_kind,
        subject_id: subject_id,
        detail: existing_detail
      }
      when principal_id == context.principal_id and subject_kind == command.subject_kind and
             subject_id == command.subject_id and existing_detail == detail ->
        :ok

      %AuthPlatformAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "audit command has different content"))

      nil ->
        %AuthPlatformAuditEntry{
          command_id: command.command_id,
          principal_id: context.principal_id,
          action: command.action,
          subject_kind: command.subject_kind,
          subject_id: command.subject_id,
          detail: detail,
          occurred_at: command.occurred_at,
          inserted_at: command.occurred_at
        }
        |> Repo.insert!(
          on_conflict: :nothing,
          conflict_target: [:command_id, :action]
        )

        record_audit!(command)
    end
  end

  defp reserve_operator_command!(command) do
    now = command.occurred_at

    {intent, replayed?} =
      case replayable_operator_intent(command) do
        nil -> {insert_and_lock_operator_intent!(command, now), false}
        intent -> {intent, true}
      end

    validate_operator_intent!(intent, command)

    intent =
      if replayed?,
        do: recover_operator_intent!(intent, command, now),
        else: intent

    unless replayed? do
      record_audit!(%RecordAudit{
        scope: command.workspace_context,
        command_id: operator_audit_id(intent, "requested"),
        action: command.operation,
        subject_kind: command.resource_type,
        subject_id: command.resource_id,
        detail: command.detail,
        occurred_at: now
      })
    end

    %{
      key_hash: intent.key_hash,
      request_fingerprint: intent.request_fingerprint,
      expires_at: intent.expires_at,
      replayed?: replayed?
    }
  end

  defp replayable_operator_intent(command) do
    workspace_id = command.workspace_context.workspace_id

    from(intent in AuthOperatorCommand,
      where:
        intent.workspace_id == ^workspace_id and intent.actor_id == ^command.actor_id and
          intent.operation == ^command.operation and intent.key_hash == ^command.key_hash,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp validate_operator_intent!(intent, command) do
    mismatch? =
      intent.request_fingerprint != command.request_fingerprint or
        intent.key_hash != command.key_hash or
        intent.resource_type != command.resource_type or
        intent.resource_id != command.resource_id or intent.actor_id != command.actor_id

    if mismatch? do
      unresolved? = intent.status in ["pending", "unknown"]

      Repo.rollback(
        Error.new(:conflict, "operator command has different unresolved content",
          retryable?: unresolved?,
          details: %{reason_code: "operator_command_unresolved"}
        )
      )
    end
  end

  defp recover_operator_intent!(intent, command, now) do
    intent =
      if intent.status == "pending" and DateTime.compare(intent.expires_at, now) != :gt do
        intent
        |> Ecto.Changeset.change(status: "unknown", terminal_at: now, updated_at: now)
        |> Repo.update!()
        |> tap(&record_expired_operator_intent!(command.workspace_context, &1, now))
      else
        intent
      end

    previous_session_id = intent.session_id

    recovered =
      intent
      |> Ecto.Changeset.change(
        session_id: command.session_id,
        expires_at: command.expires_at,
        updated_at: now
      )
      |> Repo.update!()

    renew_domain_idempotency!(recovered, command.expires_at)

    if previous_session_id != command.session_id do
      record_operator_recovery_audit!(
        command.workspace_context,
        recovered,
        previous_session_id,
        now
      )
    end

    recovered
  end

  defp renew_domain_idempotency!(intent, expires_at) do
    SQL.query!(
      Repo,
      """
      UPDATE favn_control.idempotency_records
      SET expires_at = GREATEST(expires_at, $6), updated_at = clock_timestamp()
      WHERE workspace_id = $1 AND operation = $2 AND principal_kind = 'actor'
        AND principal_id = $3 AND key_hash = $4 AND request_fingerprint = $5
      """,
      [
        intent.workspace_id,
        intent.operation,
        intent.actor_id,
        intent.key_hash,
        intent.request_fingerprint,
        expires_at
      ]
    )

    :ok
  end

  defp record_expired_operator_intent!(context, intent, now) do
    record_audit!(%RecordAudit{
      scope: context,
      command_id: operator_audit_id(intent, "unknown"),
      action: intent.operation,
      subject_kind: intent.resource_type,
      subject_id: intent.resource_id,
      detail: %{
        actor_id: intent.actor_id,
        session_id: intent.session_id,
        outcome: "unknown",
        result: %{reason: "intent_expired_before_terminal_result"}
      },
      occurred_at: now
    })
  end

  defp record_operator_recovery_audit!(context, intent, previous_session_id, now) do
    record_audit!(%RecordAudit{
      scope: context,
      command_id:
        operator_audit_id(
          intent,
          Enum.join(["recovered", previous_session_id, intent.session_id], ":")
        ),
      action: intent.operation <> ".recovered",
      subject_kind: intent.resource_type,
      subject_id: intent.resource_id,
      detail: %{
        actor_id: intent.actor_id,
        previous_session_id: previous_session_id,
        session_id: intent.session_id,
        outcome: "recovered"
      },
      occurred_at: now
    })
  end

  defp insert_and_lock_operator_intent!(command, now) do
    workspace_id = command.workspace_context.workspace_id

    %AuthOperatorCommand{
      intent_id:
        operator_intent_id(
          workspace_id,
          command.actor_id,
          command.operation,
          command.key_hash
        ),
      workspace_id: workspace_id,
      actor_id: command.actor_id,
      session_id: command.session_id,
      operation: command.operation,
      resource_type: command.resource_type,
      resource_id: command.resource_id,
      key_hash: command.key_hash,
      request_fingerprint: command.request_fingerprint,
      status: "pending",
      expires_at: command.expires_at,
      inserted_at: now,
      updated_at: now
    }
    |> Repo.insert!(
      on_conflict: :nothing,
      conflict_target:
        {:unsafe_fragment,
         ~s<(workspace_id, operation, resource_type, resource_id) WHERE status IN ('pending', 'unknown')>}
    )

    from(intent in AuthOperatorCommand,
      where:
        intent.workspace_id == ^workspace_id and intent.operation == ^command.operation and
          intent.resource_type == ^command.resource_type and
          intent.resource_id == ^command.resource_id and intent.status in ["pending", "unknown"],
      lock: "FOR UPDATE"
    )
    |> Repo.one!()
  end

  defp complete_operator_command!(command) do
    workspace_id = command.workspace_context.workspace_id

    intent =
      from(intent in AuthOperatorCommand,
        where:
          intent.workspace_id == ^workspace_id and intent.actor_id == ^command.actor_id and
            intent.session_id == ^command.session_id and intent.operation == ^command.operation and
            intent.key_hash == ^command.key_hash,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    case intent do
      nil ->
        Repo.rollback(Error.new(:not_found, "operator command intent not found"))

      %AuthOperatorCommand{request_fingerprint: fingerprint}
      when fingerprint != command.request_fingerprint ->
        Repo.rollback(Error.new(:conflict, "operator command request changed"))

      %AuthOperatorCommand{status: status} = intent when status in ["pending", "unknown"] ->
        if status == "unknown" and command.outcome == "unknown" do
          replay_operator_result!(command, intent)
        else
          resolve_operator_result!(command, intent)
        end

      %AuthOperatorCommand{} = intent ->
        replay_operator_result!(command, intent)
    end
  end

  defp resolve_operator_result!(command, intent) do
    changes = %{
      status: command.outcome,
      result_resource_type: command.resource_type,
      result_resource_id: command.resource_id,
      result_detail: normalized_audit_detail!(command.detail),
      terminal_at: command.occurred_at,
      updated_at: command.occurred_at
    }

    intent
    |> Ecto.Changeset.change(changes)
    |> Repo.update!()

    record_operator_result_audit!(command, intent)
    :ok
  end

  defp replay_operator_result!(command, intent) do
    detail = normalized_audit_detail!(command.detail)

    if intent.status == "unknown" and command.outcome == "unknown" do
      :ok
    else
      same_result? =
        intent.status == command.outcome and
          intent.result_resource_type == command.resource_type and
          intent.result_resource_id == command.resource_id and
          semantic_operator_result(intent.result_detail) == semantic_operator_result(detail)

      if same_result?,
        do: :ok,
        else: Repo.rollback(Error.new(:conflict, "operator command result changed"))
    end
  end

  defp semantic_operator_result(detail) when is_map(detail), do: Map.delete(detail, "session_id")
  defp semantic_operator_result(detail), do: detail

  defp record_operator_result_audit!(command, intent) do
    record_audit!(%RecordAudit{
      scope: command.workspace_context,
      command_id: operator_audit_id(intent, command.outcome),
      action: command.operation,
      subject_kind: command.resource_type,
      subject_id: command.resource_id,
      detail: command.detail,
      occurred_at: command.occurred_at
    })
  end

  defp operator_intent_id(workspace_id, actor_id, operation, key_hash) do
    digest =
      :crypto.hash(:sha256, Enum.join([workspace_id, actor_id, operation, key_hash], <<0>>))
      |> Base.encode16(case: :lower)

    "operator_intent:" <> digest
  end

  defp operator_audit_id(intent, outcome) do
    digest =
      :crypto.hash(
        :sha256,
        Enum.join([intent.workspace_id, intent.intent_id, intent.operation, outcome], <<0>>)
      )
      |> Base.encode16(case: :lower)

    "audit:" <> digest
  end

  defp platform_audit!(context, command_id, action, subject_id, detail, occurred_at) do
    %AuthPlatformAuditEntry{
      command_id: command_id,
      principal_id: context.principal_id,
      action: action,
      subject_kind: "actor",
      subject_id: subject_id,
      detail: Redaction.redact(detail),
      occurred_at: occurred_at,
      inserted_at: occurred_at
    }
    |> Repo.insert!()
  end

  defp audit_by_command(workspace_id, command_id, action),
    do:
      Repo.get_by(AuthAuditEntry,
        workspace_id: workspace_id,
        command_id: command_id,
        action: action
      )

  defp audit_replay?(workspace_id, command_id, action),
    do: not is_nil(audit_by_command(workspace_id, command_id, action))

  defp platform_audit_by_command(command_id, action),
    do: Repo.get_by(AuthPlatformAuditEntry, command_id: command_id, action: action)

  defp access_audit_detail(%SetActorAccess{} = command) do
    %{
      "roles" => role_strings(command.roles),
      "status" => Atom.to_string(command.status),
      "version" => command.expected_version + 1
    }
  end

  defp access_audit_detail(access) do
    %{"roles" => access.roles, "status" => access.status, "version" => access.version}
  end

  defp normalize_username(username) do
    username |> String.trim() |> String.normalize(:nfkc) |> String.downcase()
  end

  defp creation_hash!(command, normalized_username) do
    hash!(%{
      actor_id: command.actor_id,
      normalized_username: normalized_username,
      display_name: command.display_name,
      password_hash_fingerprint: Base.url_encode64(hash!(command.password_hash), padding: false),
      workspace_id: command.workspace_context.workspace_id,
      roles: role_strings(command.roles)
    })
  end

  defp hash!(value) do
    {:ok, hash} = CanonicalJSON.hash(value)
    hash
  end

  defp role_strings(roles), do: roles |> Enum.map(&Atom.to_string/1) |> Enum.sort()

  defp future?(timestamp) do
    %{rows: [[future?]]} =
      SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [timestamp])

    future?
  end

  defp actor_status(query, nil), do: query

  defp actor_status(query, status),
    do:
      where(
        query,
        [_actor, membership, _credential],
        membership.status == ^Atom.to_string(status)
      )

  defp after_actor(query, nil), do: query

  defp after_actor(query, %{actor_id: actor_id}),
    do: where(query, [actor, _membership, _credential], actor.actor_id > ^actor_id)

  defp session_actor(query, nil), do: query

  defp session_actor(query, actor_id),
    do: where(query, [session], session.actor_id == ^actor_id)

  defp session_status(query, nil), do: query

  defp session_status(query, :expired) do
    where(
      query,
      [session],
      session.status == "expired" or
        (session.status == "active" and session.expires_at <= fragment("clock_timestamp()"))
    )
  end

  defp session_status(query, status),
    do: where(query, [session], session.status == ^Atom.to_string(status))

  defp after_session(query, nil), do: query

  defp after_session(query, %{inserted_at: inserted_at, session_id: session_id}) do
    where(
      query,
      [session],
      session.inserted_at < ^inserted_at or
        (session.inserted_at == ^inserted_at and session.session_id < ^session_id)
    )
  end

  defp after_audit(query, nil), do: query

  defp after_audit(query, %{audit_id: audit_id}),
    do: where(query, [entry], entry.audit_id < ^audit_id)

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_create_actor(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.actor_id], &valid_id?/1) and
         is_binary(command.username) and normalize_username(command.username) != "" and
         byte_size(normalize_username(command.username)) <= 255 and
         is_binary(command.display_name) and command.display_name != "" and
         byte_size(command.display_name) <= 255 and valid_password_hash?(command.password_hash) and
         valid_roles?(command.roles, @workspace_roles) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_get_actor(query) do
    selector? =
      case query.selector do
        %ActorById{actor_id: actor_id} ->
          valid_id?(actor_id)

        %ActorByUsername{username: username} ->
          is_binary(username) and normalize_username(username) != ""

        _other ->
          false
      end

    if workspace_context?(query.workspace_context) and selector?,
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_page_actors(page) do
    cursor? = is_nil(page.after) or match?(%{actor_id: id} when is_binary(id), page.after)

    if workspace_context?(page.workspace_context) and
         (is_nil(page.status) or page.status in @access_statuses) and cursor? and
         valid_bound?(page.limit, 1, 500),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_list_actor_memberships(query) do
    context = query.workspace_context

    if workspace_context?(context) and valid_id?(query.actor_id) and
         context.principal_id == query.actor_id,
       do: :ok,
       else: {:error, Error.new(:forbidden, "memberships are self-only")}
  end

  defp validate_set_access(%{scope_kind: :workspace} = command) do
    if workspace_context?(command.authority) and
         command.workspace_id == command.authority.workspace_id and
         common_access_valid?(command, @workspace_roles),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_set_access(%{scope_kind: :platform} = command) do
    if PlatformContext.valid?(command.authority) and is_nil(command.workspace_id) and
         common_access_valid?(command, @platform_roles),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_set_access(_command), do: {:error, ErrorMapper.map(:invalid)}

  defp validate_attach_actor_membership(command) do
    normalized_username = normalize_username(command.username)

    if workspace_context?(command.workspace_context) and valid_id?(command.command_id) and
         normalized_username != "" and byte_size(normalized_username) <= 255 and
         valid_roles?(command.roles, @workspace_roles) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_bootstrap_administrator(command) do
    context = command.platform_context
    workspace_ids = command.workspace_ids

    if PlatformContext.valid?(context) and :platform_admin in context.roles and
         Enum.all?([command.command_id, command.actor_id], &valid_id?/1) and
         is_list(workspace_ids) and workspace_ids != [] and
         workspace_ids == workspace_ids |> Enum.uniq() |> Enum.sort() and
         Enum.all?(workspace_ids, &valid_id?/1) and
         is_binary(command.username) and normalize_username(command.username) != "" and
         byte_size(normalize_username(command.username)) <= 255 and
         is_binary(command.display_name) and command.display_name != "" and
         byte_size(command.display_name) <= 255 and valid_password_hash?(command.password_hash) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else:
         {:error,
          Error.new(:forbidden, "explicit platform administrator bootstrap authority required")}
  end

  defp validate_recover_administrator_credential(command) do
    context = command.platform_context

    if PlatformContext.valid?(context) and :platform_admin in context.roles and
         valid_id?(command.command_id) and is_binary(command.username) and
         normalize_username(command.username) != "" and
         byte_size(normalize_username(command.username)) <= 255 and
         valid_password_hash?(command.password_hash) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else:
         {:error,
          Error.new(:forbidden, "explicit platform administrator recovery authority required")}
  end

  defp validate_set_actor_status(command) do
    context = command.platform_context

    if PlatformContext.valid?(context) and :platform_admin in context.roles and
         valid_id?(command.command_id) and valid_id?(command.actor_id) and
         command.status in [:active, :disabled] and is_integer(command.expected_version) and
         command.expected_version > 0 and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, Error.new(:forbidden, "platform administrator authority required")}
  end

  defp common_access_valid?(command, allowed_roles) do
    valid_id?(command.command_id) and valid_id?(command.actor_id) and
      valid_roles?(command.roles, allowed_roles) and command.status in @access_statuses and
      is_integer(command.expected_version) and command.expected_version >= 0 and
      match?(%DateTime{}, command.occurred_at)
  end

  defp validate_change_password(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.actor_id], &valid_id?/1) and
         valid_password_hash?(command.password_hash) and
         is_integer(command.expected_credential_version) and
         command.expected_credential_version > 0 and is_boolean(command.revoke_sessions?) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_create_session(command) do
    valid_credential_version? =
      case command.provider do
        "password_local" ->
          is_integer(command.expected_credential_version) and
            command.expected_credential_version > 0

        "trusted_local_dev" ->
          is_nil(command.expected_credential_version)

        _provider ->
          false
      end

    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.session_id, command.actor_id], &valid_id?/1) and
         is_binary(command.token_hash) and byte_size(command.token_hash) >= 32 and
         valid_credential_version? and
         match?(%DateTime{}, command.expires_at) and match?(%DateTime{}, command.occurred_at) and
         DateTime.compare(command.expires_at, command.occurred_at) == :gt,
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_rotate_workspace_session(command) do
    if workspace_context?(command.source_context) and
         Enum.all?(
           [
             command.command_id,
             command.source_session_id,
             command.target_workspace_id,
             command.session_id,
             command.actor_id
           ],
           &valid_id?/1
         ) and is_binary(command.token_hash) and byte_size(command.token_hash) >= 32 and
         command.provider in ["password_local", "trusted_local_dev"] and
         match?(%DateTime{}, command.expires_at) and
         match?(%DateTime{}, command.occurred_at) and
         DateTime.compare(command.expires_at, command.occurred_at) == :gt,
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_get_session(query) do
    selector? =
      case query.selector do
        %SessionById{session_id: session_id} ->
          valid_id?(session_id)

        %SessionByTokenHash{token_hash: token_hash} ->
          is_binary(token_hash) and byte_size(token_hash) >= 32

        _other ->
          false
      end

    if workspace_context?(query.workspace_context) and selector?,
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_page_sessions(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{inserted_at: %DateTime{}, session_id: session_id} when is_binary(session_id),
          page.after
        )

    if workspace_context?(page.workspace_context) and
         (is_nil(page.actor_id) or valid_id?(page.actor_id)) and
         (is_nil(page.status) or page.status in [:active, :revoked, :expired]) and cursor? and
         valid_bound?(page.limit, 1, 500),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_revoke(command) do
    identity? =
      case {command.session_id, command.actor_id} do
        {session_id, nil} -> valid_id?(session_id)
        {nil, actor_id} -> valid_id?(actor_id)
        _other -> false
      end

    if workspace_context?(command.workspace_context) and valid_id?(command.command_id) and
         identity? and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_record_audit(command) do
    scope? = WorkspaceContext.valid?(command.scope) or PlatformContext.valid?(command.scope)

    detail? =
      case Jason.encode(Redaction.redact(command.detail)) do
        {:ok, encoded} -> byte_size(encoded) in 2..65_536
        {:error, _reason} -> false
      end

    if scope? and
         Enum.all?(
           [command.command_id, command.action, command.subject_kind, command.subject_id],
           &valid_id?/1
         ) and is_map(command.detail) and detail? and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp normalized_audit_detail!(detail) do
    detail
    |> Redaction.redact()
    |> Jason.encode!()
    |> Jason.decode!()
  end

  defp validate_reserve_operator_command(command) do
    valid? =
      WorkspaceContext.valid?(command.workspace_context) and
        command.workspace_context.principal_id == command.actor_id and
        Enum.all?(
          [
            command.actor_id,
            command.session_id,
            command.operation,
            command.resource_type,
            command.resource_id,
            command.key_hash,
            command.request_fingerprint
          ],
          &valid_id?/1
        ) and is_map(command.detail) and valid_audit_detail?(command.detail) and
        match?(%DateTime{}, command.expires_at) and match?(%DateTime{}, command.occurred_at) and
        DateTime.compare(command.expires_at, command.occurred_at) == :gt

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_complete_operator_command(command) do
    valid? =
      WorkspaceContext.valid?(command.workspace_context) and
        command.workspace_context.principal_id == command.actor_id and
        command.outcome in ["accepted", "partial", "rejected", "unknown"] and
        Enum.all?(
          [
            command.actor_id,
            command.session_id,
            command.operation,
            command.key_hash,
            command.request_fingerprint,
            command.resource_type,
            command.resource_id
          ],
          &valid_id?/1
        ) and is_map(command.detail) and valid_audit_detail?(command.detail) and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_audit_detail?(detail) do
    case Jason.encode(Redaction.redact(detail)) do
      {:ok, encoded} -> byte_size(encoded) in 2..65_536
      {:error, _reason} -> false
    end
  end

  defp validate_page_audit(page) do
    scope? = WorkspaceContext.valid?(page.scope) or PlatformContext.valid?(page.scope)
    cursor? = is_nil(page.after) or match?(%{audit_id: id} when is_integer(id), page.after)

    if scope? and cursor? and valid_bound?(page.limit, 1, 500),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_roles?(roles, allowed),
    do:
      is_list(roles) and roles != [] and length(roles) <= 16 and
        Enum.all?(roles, &(&1 in allowed)) and
        length(roles) == length(Enum.uniq(roles))

  defp valid_password_hash?(hash),
    do: is_binary(hash) and String.starts_with?(hash, "$argon2id$") and byte_size(hash) <= 1_024

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
