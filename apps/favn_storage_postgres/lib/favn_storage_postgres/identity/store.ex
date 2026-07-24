defmodule FavnStoragePostgres.Identity.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.IdentityStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.RecordAudit
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.SetActorAccess
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.PageActors
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Results.Actor, as: ActorResult
  alias FavnOrchestrator.Persistence.Results.AuditEntry, as: AuditResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult
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
  alias FavnStoragePostgres.Schemas.AuthSession
  alias FavnStoragePostgres.Schemas.AuthWorkspaceMembership

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
  def set_access(%SetActorAccess{} = command) do
    with :ok <- validate_set_access(command) do
      transaction(fn -> set_access!(command) end)
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

  defp set_access!(%{scope_kind: :workspace} = command) do
    workspace_id = command.workspace_id
    actor = lock_actor!(command.actor_id)

    membership =
      from(membership in AuthWorkspaceMembership,
        where:
          membership.workspace_id == ^workspace_id and membership.actor_id == ^command.actor_id,
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    expected_detail = access_audit_detail(command)

    case audit_by_command(workspace_id, command.command_id, "access.workspace.changed") do
      %AuthAuditEntry{detail: ^expected_detail} ->
        load_actor!(workspace_id, command.actor_id)

      %AuthAuditEntry{} ->
        Repo.rollback(Error.new(:conflict, "access command has different content"))

      nil ->
        updated = upsert_workspace_access!(membership, actor, command)

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
        existing.expires_at == command.expires_at and
          existing.creation_command_id == command.command_id ->
        session_result(existing)

      existing ->
        Repo.rollback(Error.new(:conflict, "session identity has different content"))

      true ->
        session =
          %AuthSession{
            session_id: command.session_id,
            actor_id: command.actor_id,
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
          %{"actor_id" => command.actor_id},
          command.occurred_at,
          "session"
        )

        session_result(session)
    end
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
            ensure_membership!(workspace_id, session.actor_id)
            revoke_session_row!(session, command.occurred_at)
            {session.actor_id, session_id}

          {nil, actor_id} ->
            ensure_membership!(workspace_id, actor_id)
            revoke_actor_sessions!(actor_id, command.occurred_at)
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
      where: membership.workspace_id == ^workspace_id and membership.status == "active",
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
    %AuthAuditEntry{
      workspace_id: context.workspace_id,
      command_id: command.command_id,
      principal_id: context.principal_id,
      action: command.action,
      subject_kind: command.subject_kind,
      subject_id: command.subject_id,
      detail: Redaction.redact(command.detail),
      occurred_at: command.occurred_at,
      inserted_at: command.occurred_at
    }
    |> Repo.insert!(
      on_conflict: :nothing,
      conflict_target: [:workspace_id, :command_id, :action]
    )

    :ok
  end

  defp record_audit!(%RecordAudit{scope: %PlatformContext{} = context} = command) do
    %AuthPlatformAuditEntry{
      command_id: command.command_id,
      principal_id: context.principal_id,
      action: command.action,
      subject_kind: command.subject_kind,
      subject_id: command.subject_id,
      detail: Redaction.redact(command.detail),
      occurred_at: command.occurred_at,
      inserted_at: command.occurred_at
    }
    |> Repo.insert!(
      on_conflict: :nothing,
      conflict_target: [:command_id, :action]
    )

    :ok
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

  defp common_access_valid?(command, allowed_roles) do
    valid_id?(command.command_id) and valid_id?(command.actor_id) and
      valid_roles?(command.roles, allowed_roles) and command.status in @access_statuses and
      is_integer(command.expected_version) and command.expected_version >= 0 and
      match?(%DateTime{}, command.occurred_at)
  end

  defp validate_change_password(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.actor_id], &valid_id?/1) and
         valid_password_hash?(command.password_hash) and is_boolean(command.revoke_sessions?) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_create_session(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.session_id, command.actor_id], &valid_id?/1) and
         is_binary(command.token_hash) and byte_size(command.token_hash) >= 32 and
         command.provider == "password_local" and
         match?(%DateTime{}, command.expires_at) and match?(%DateTime{}, command.occurred_at) and
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

    if scope? and
         Enum.all?(
           [command.command_id, command.action, command.subject_kind, command.subject_id],
           &valid_id?/1
         ) and is_map(command.detail) and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
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
