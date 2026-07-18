defmodule FavnOrchestrator.Identity do
  @moduledoc """
  Workspace-scoped actor, credential, session, and authorization use cases.

  This module translates the operator-facing `viewer | operator | admin` roles
  into the persistence contract's workspace roles. Raw credentials and session
  token hashes never escape this boundary.
  """

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.RecordAudit
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.SetActorAccess
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.PageActors
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Results.Actor, as: ActorResult
  alias FavnOrchestrator.Persistence.Results.AuditEntry
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult
  alias FavnOrchestrator.Persistence.Selectors.ActorById
  alias FavnOrchestrator.Persistence.Selectors.ActorByUsername
  alias FavnOrchestrator.Persistence.Selectors.SessionById
  alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @type actor :: %{
          required(:id) => String.t(),
          required(:username) => String.t(),
          required(:display_name) => String.t(),
          required(:roles) => [:viewer | :operator | :admin],
          required(:status) => :active | :disabled,
          required(:workspace_id) => String.t(),
          required(:access_version) => pos_integer()
        }

  @type session :: %{
          required(:id) => String.t(),
          required(:actor_id) => String.t(),
          required(:provider) => String.t(),
          required(:issued_at) => DateTime.t(),
          required(:expires_at) => DateTime.t(),
          required(:revoked_at) => DateTime.t() | nil,
          optional(:token) => String.t()
        }

  @doc "Creates a global actor and its first membership in the command workspace."
  @spec create_actor(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          String.t(),
          [atom() | String.t()],
          keyword()
        ) :: {:ok, actor()} | {:error, term()}
  def create_actor(context, username, password, display_name, roles, opts \\ [])

  def create_actor(
        %WorkspaceContext{} = context,
        username,
        password,
        display_name,
        roles,
        opts
      )
      when is_binary(username) and is_binary(password) and is_binary(display_name) and
             is_list(roles) and is_list(opts) do
    with :ok <- authorize(context, :admin),
         :ok <- validate_opts(opts, [:actor_id, :command_id, :occurred_at]),
         {:ok, attrs} <- Credentials.normalize_actor(username, display_name, roles),
         :ok <- Credentials.validate_password(password),
         actor_id <- Keyword.get(opts, :actor_id, random_id("act")),
         occurred_at <- Keyword.get(opts, :occurred_at, DateTime.utc_now()),
         command_id <- Keyword.get(opts, :command_id, command_id("actor-create", actor_id)),
         {:ok, actor} <-
           store().create_actor(%CreateActor{
             workspace_context: context,
             command_id: command_id,
             actor_id: actor_id,
             username: attrs.username,
             display_name: attrs.display_name,
             password_hash: password_hash(password),
             roles: persistence_roles(attrs.roles),
             occurred_at: occurred_at
           }) do
      {:ok, actor_map(actor)}
    else
      {:error, %Error{kind: :conflict}} -> {:error, :username_taken}
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} = error -> error
    end
  end

  @doc "Authenticates a password in exactly one workspace."
  @spec authenticate_password(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, actor()} | {:error, :invalid_credentials}
  def authenticate_password(%WorkspaceContext{} = context, username, password)
      when is_binary(username) and is_binary(password) do
    with true <- Credentials.valid_login_input?(username, password),
         {:ok, actor} <-
           store().get_actor(%GetActor{
             workspace_context: context,
             selector: %ActorByUsername{username: String.trim(username)}
           }),
         :ok <- active_actor?(actor),
         :ok <- Credentials.verify_password(password, %{password_hash: actor.credential_hash}) do
      {:ok, actor_map(actor)}
    else
      _invalid -> Credentials.dummy_verify()
    end
  end

  @doc "Issues one opaque session for an active workspace member."
  @spec issue_session(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, session()} | {:error, term()}
  def issue_session(%WorkspaceContext{} = context, actor_id, opts \\ [])
      when is_binary(actor_id) and is_list(opts) do
    with {:ok, issued} <- Session.issue(actor_id, opts),
         {:ok, persisted} <-
           store().create_session(%CreateSession{
             workspace_context: context,
             command_id: command_id("session-create", issued.id),
             session_id: issued.id,
             actor_id: actor_id,
             token_hash: Session.token_hash(issued.token),
             provider: issued.provider,
             expires_at: issued.expires_at,
             occurred_at: issued.issued_at
           }) do
      {:ok, persisted |> session_map() |> Map.put(:token, issued.token)}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} = error -> error
    end
  end

  @doc "Resolves an active session token and its actor in one workspace."
  @spec introspect_session(WorkspaceContext.t(), String.t()) ::
          {:ok, session(), actor()} | {:error, :invalid_session | term()}
  def introspect_session(%WorkspaceContext{} = context, token) when is_binary(token) do
    with true <- Session.valid_token?(token),
         {:ok, session} <-
           store().get_session(%GetSession{
             workspace_context: context,
             selector: %SessionByTokenHash{token_hash: Session.token_hash(token)}
           }),
         :ok <- active_session?(session),
         {:ok, actor} <- get_actor_result(context, session.actor_id),
         :ok <- active_actor?(actor) do
      {:ok, session_map(session), actor_map(actor)}
    else
      _invalid -> {:error, :invalid_session}
    end
  end

  @doc false
  @spec authorize_session(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          :viewer | :operator | :admin
        ) :: {:ok, WorkspaceContext.t(), session(), actor()} | {:error, term()}
  def authorize_session(%WorkspaceContext{} = context, actor_id, session_id, required_role)
      when is_binary(actor_id) and is_binary(session_id) and
             required_role in [:viewer, :operator, :admin] do
    with {:ok, session} <-
           store().get_session(%GetSession{
             workspace_context: context,
             selector: %SessionById{session_id: session_id}
           }),
         true <- session.actor_id == actor_id,
         :ok <- active_session?(session),
         {:ok, actor} <- get_actor_result(context, actor_id),
         :ok <- active_actor?(actor),
         true <- role_allowed?(actor.roles, required_role),
         {:ok, authorized} <-
           WorkspaceContext.new(context.workspace_id, actor_id, actor.roles,
             request_id: session_id
           ) do
      {:ok, authorized, session_map(session), actor_map(actor)}
    else
      false -> {:error, :forbidden}
      {:error, _reason} -> {:error, :unauthenticated}
    end
  end

  @doc "Fetches one actor only through a workspace membership."
  @spec get_actor(WorkspaceContext.t(), String.t()) :: {:ok, actor()} | {:error, term()}
  def get_actor(%WorkspaceContext{} = context, actor_id) when is_binary(actor_id) do
    with :ok <- authorize(context, :viewer),
         {:ok, actor} <- get_actor_result(context, actor_id) do
      {:ok, actor_map(actor)}
    else
      {:error, %Error{kind: :not_found}} -> {:error, :actor_not_found}
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns one bounded keyset page of actors in a workspace."
  @spec page_actors(WorkspaceContext.t(), keyword()) ::
          {:ok, CursorPage.t(actor())} | {:error, term()}
  def page_actors(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with :ok <- authorize(context, :admin),
         :ok <- validate_opts(opts, [:after, :limit, :status]),
         {:ok, page} <-
           store().page_actors(%PageActors{
             workspace_context: context,
             after: Keyword.get(opts, :after),
             limit: Keyword.get(opts, :limit, 50),
             status: Keyword.get(opts, :status)
           }) do
      {:ok, %{page | items: Enum.map(page.items, &actor_map/1)}}
    end
  end

  @doc "Replaces one actor's workspace roles with optimistic concurrency."
  @spec set_roles(WorkspaceContext.t(), String.t(), [atom() | String.t()], pos_integer()) ::
          {:ok, actor()} | {:error, term()}
  def set_roles(%WorkspaceContext{} = context, actor_id, roles, expected_version)
      when is_binary(actor_id) and is_list(roles) and is_integer(expected_version) do
    with :ok <- authorize(context, :admin),
         {:ok, roles} <- Credentials.normalize_roles(roles),
         {:ok, actor} <-
           store().set_access(%SetActorAccess{
             authority: context,
             command_id:
               command_id(
                 "actor-access",
                 actor_access_identity(actor_id, roles, expected_version)
               ),
             actor_id: actor_id,
             scope_kind: :workspace,
             workspace_id: context.workspace_id,
             roles: persistence_roles(roles),
             status: :active,
             expected_version: expected_version,
             occurred_at: DateTime.utc_now()
           }) do
      {:ok, actor_map(actor)}
    end
  end

  @doc "Adds or replaces an actor membership in the authority workspace."
  @spec set_membership(
          WorkspaceContext.t(),
          String.t(),
          [atom() | String.t()],
          non_neg_integer()
        ) :: {:ok, actor()} | {:error, term()}
  def set_membership(context, actor_id, roles, expected_version),
    do: set_roles(context, actor_id, roles, expected_version)

  @doc "Changes a password and revokes all active sessions atomically."
  @spec change_password(WorkspaceContext.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def change_password(%WorkspaceContext{} = context, actor_id, password)
      when is_binary(actor_id) and is_binary(password) do
    with :ok <- authorize_self(context, actor_id),
         :ok <- Credentials.validate_password(password) do
      store().change_password(%ChangeActorPassword{
        workspace_context: context,
        command_id: command_id("actor-password", random_identity()),
        actor_id: actor_id,
        password_hash: password_hash(password),
        occurred_at: DateTime.utc_now(),
        revoke_sessions?: true
      })
    end
  end

  @doc "Revokes one session after proving it belongs to the workspace."
  @spec revoke_session(WorkspaceContext.t(), String.t()) :: :ok | {:error, term()}
  def revoke_session(%WorkspaceContext{} = context, session_id) when is_binary(session_id) do
    with {:ok, session} <-
           store().get_session(%GetSession{
             workspace_context: context,
             selector: %SessionById{session_id: session_id}
           }),
         :ok <- authorize_session_revocation(context, session.actor_id) do
      store().revoke_sessions(%RevokeSessions{
        workspace_context: context,
        command_id: command_id("session-revoke", session_id),
        session_id: session_id,
        actor_id: nil,
        occurred_at: DateTime.utc_now()
      })
    end
  end

  @doc "Appends redacted operational audit evidence under explicit persistence authority."
  @spec record_audit(
          WorkspaceContext.t() | FavnOrchestrator.Persistence.PlatformContext.t(),
          map()
        ) ::
          :ok | {:error, term()}
  def record_audit(scope, entry) when is_map(entry) do
    store().record_audit(%RecordAudit{
      scope: scope,
      command_id: audit_command_id(scope, entry),
      action: Map.fetch!(entry, :action),
      subject_kind: Map.get(entry, :resource_type, "api_request"),
      subject_id: audit_subject_id(entry),
      detail: entry,
      occurred_at: DateTime.utc_now()
    })
  rescue
    _error -> {:error, :invalid_audit_entry}
  end

  defp audit_command_id(scope, entry) do
    case audit_idempotency_key_hash(entry) do
      key_hash when is_binary(key_hash) and key_hash != "" ->
        scope_id = Map.get(scope, :workspace_id, "platform")

        digest =
          :crypto.hash(
            :sha256,
            Enum.join(
              [
                scope_id,
                Map.fetch!(entry, :action),
                Map.get(entry, :resource_type, "api_request"),
                audit_subject_id(entry),
                key_hash
              ],
              <<0>>
            )
          )
          |> Base.encode16(case: :lower)

        "audit:" <> digest

      _missing ->
        random_id("audit")
    end
  end

  defp audit_idempotency_key_hash(entry) do
    case Map.get(entry, :idempotency) || Map.get(entry, "idempotency") do
      value when is_map(value) -> Map.get(value, :key_hash) || Map.get(value, "key_hash")
      _missing -> nil
    end
  end

  @doc "Returns one bounded keyset page of redacted workspace authorization audit."
  @spec page_audit(WorkspaceContext.t(), keyword()) ::
          {:ok, CursorPage.t(AuditEntry.t())} | {:error, term()}
  def page_audit(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with :ok <- authorize(context, :admin),
         :ok <- validate_opts(opts, [:after, :limit]) do
      store().page_audit(%PageAudit{
        scope: context,
        after: Keyword.get(opts, :after),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  defp get_actor_result(context, actor_id) do
    store().get_actor(%GetActor{
      workspace_context: context,
      selector: %ActorById{actor_id: actor_id}
    })
  end

  defp store, do: Persistence.stores().identity

  defp authorize_self(%WorkspaceContext{} = context, actor_id) do
    if WorkspaceContext.valid?(context) and context.principal_id == actor_id,
      do: :ok,
      else: {:error, :forbidden}
  end

  defp authorize_session_revocation(%WorkspaceContext{} = context, actor_id) do
    if context.principal_id == actor_id or :workspace_admin in context.roles,
      do: :ok,
      else: {:error, :forbidden}
  end

  defp actor_access_identity(actor_id, roles, expected_version) do
    normalized_roles = roles |> Enum.map(&Atom.to_string/1) |> Enum.sort() |> Enum.join(",")
    "#{actor_id}:#{expected_version}:#{normalized_roles}"
  end

  defp audit_subject_id(entry) do
    Map.get(entry, :resource_id) || Map.get(entry, :target_session_id) ||
      Map.get(entry, :session_id) || Map.get(entry, :actor_id) || "api"
  end

  defp authorize(%WorkspaceContext{} = context, :viewer) do
    if Enum.any?(
         context.roles,
         &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
       ),
       do: :ok,
       else: {:error, :forbidden}
  end

  defp authorize(%WorkspaceContext{} = context, :admin) do
    if Enum.any?(context.roles, &(&1 in [:workspace_admin, :platform_operator])),
      do: :ok,
      else: {:error, :forbidden}
  end

  defp active_actor?(%ActorResult{status: :active, membership_status: :active}), do: :ok
  defp active_actor?(_actor), do: {:error, :invalid_credentials}

  defp active_session?(%SessionResult{status: :active}), do: :ok

  defp active_session?(_session), do: {:error, :invalid_session}

  defp actor_map(%ActorResult{} = actor) do
    %{
      id: actor.actor_id,
      username: actor.username,
      display_name: actor.display_name,
      roles: operator_roles(actor.roles),
      status:
        if(actor.status == :active and actor.membership_status == :active,
          do: :active,
          else: :disabled
        ),
      workspace_id: actor.workspace_id,
      access_version: actor.access_version,
      inserted_at: nil,
      updated_at: nil
    }
  end

  defp session_map(%SessionResult{} = session) do
    %{
      id: session.session_id,
      actor_id: session.actor_id,
      provider: session.provider,
      issued_at: session.issued_at,
      expires_at: session.expires_at,
      revoked_at: session.revoked_at
    }
  end

  defp persistence_roles(roles) do
    roles
    |> Enum.map(fn
      :viewer -> :customer_reader
      :operator -> :customer_operator
      :admin -> :workspace_admin
    end)
    |> Enum.uniq()
  end

  defp operator_roles(roles) do
    roles
    |> Enum.map(fn
      :customer_reader -> :viewer
      :customer_operator -> :operator
      :workspace_admin -> :admin
    end)
    |> Enum.uniq()
  end

  defp role_allowed?(roles, required_role) do
    operator_roles = operator_roles(roles)

    case required_role do
      :viewer -> Enum.any?(operator_roles, &(&1 in [:viewer, :operator, :admin]))
      :operator -> Enum.any?(operator_roles, &(&1 in [:operator, :admin]))
      :admin -> :admin in operator_roles
    end
  end

  defp password_hash(password) do
    %{password_hash: hash} = Credentials.hash_password(password)
    hash
  end

  defp validate_opts(opts, allowed) do
    if Keyword.keyword?(opts) do
      case Keyword.keys(opts) -- allowed do
        [] -> :ok
        unknown -> {:error, {:unknown_options, unknown}}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp command_id(operation, identity) do
    digest = :crypto.hash(:sha256, identity) |> Base.encode16(case: :lower)
    operation <> ":" <> digest
  end

  defp random_id(prefix), do: prefix <> "_" <> Session.random_id()
  defp random_identity, do: Session.random_id()
end
