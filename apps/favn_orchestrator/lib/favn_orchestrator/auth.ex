defmodule FavnOrchestrator.Auth do
  @moduledoc """
  Orchestrator-owned actor, session, and authorization helpers.
  """

  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Auth.Store
  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @type actor :: Store.actor()
  @type session :: Store.session()

  @spec bootstrap_configured_actor() :: :ok | {:error, term()}
  def bootstrap_configured_actor do
    username = Application.get_env(:favn_orchestrator, :auth_bootstrap_username)
    password = Application.get_env(:favn_orchestrator, :auth_bootstrap_password)

    display_name =
      Application.get_env(:favn_orchestrator, :auth_bootstrap_display_name, "Favn Admin")

    roles = Application.get_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    result =
      if is_binary(username) and username != "" and is_binary(password) and password != "" do
        bootstrap_workspace_actor(username, password, display_name, roles)
      else
        :ok
      end

    if result == :ok do
      Application.delete_env(:favn_orchestrator, :auth_bootstrap_password)
    end

    result
  end

  @doc "Authenticates and issues a session within one explicit workspace."
  @spec password_login(WorkspaceContext.t(), String.t(), String.t(), keyword() | map()) ::
          {:ok, session(), actor()} | {:error, term()}
  def password_login(%WorkspaceContext{} = context, username, password, opts) do
    with {:ok, actor} <- Store.authenticate_password(context, username, password, opts),
         {:ok, session} <-
           Store.issue_session(context, actor.id,
             provider: "password_local",
             expected_credential_version: actor.credential_version
           ) do
      {:ok, session, Map.delete(actor, :credential_version)}
    end
  end

  @doc "Issues a Favn session for one pre-linked, upstream-authenticated identity."
  @spec external_login(WorkspaceContext.t(), map()) ::
          {:ok, session(), actor()} | {:error, term()}
  def external_login(%WorkspaceContext{} = context, identity) when is_map(identity) do
    result =
      with {:ok, actor} <- Identity.authenticate_external_identity(context, identity),
           {:ok, session} <-
             Store.issue_session(context, actor.id,
               provider: "azure_container_apps_entra",
               external_tenant_id: identity.tenant_id,
               external_subject_id: identity.subject_id
             ) do
        {:ok, session, actor}
      end

    case result do
      {:ok, _session, _actor} = success ->
        success

      {:error, _reason} ->
        record_external_login_denial(context, identity)
        {:error, :invalid_credentials}
    end
  end

  @doc false
  @spec trusted_local_development_login(String.t(), String.t(), String.t()) ::
          {:ok, session()} | {:error, :trusted_local_development_unavailable}
  def trusted_local_development_login(workspace_id, username, capability)
      when is_binary(workspace_id) and is_binary(username) and is_binary(capability) do
    with :ok <- authorize_trusted_local_development(workspace_id, username, capability),
         {:ok, context} <-
           WorkspaceContext.new(
             workspace_id,
             "auth:trusted-local-development",
             [:workspace_admin]
           ),
         actor_id <- deterministic_bootstrap_actor_id(username),
         {:ok, session} <-
           Store.issue_session(context, actor_id, provider: "trusted_local_dev") do
      {:ok, session}
    else
      _unavailable -> {:error, :trusted_local_development_unavailable}
    end
  end

  defp record_external_login_denial(context, identity) do
    tenant_id = Map.get(identity, :tenant_id, "")
    subject_id = Map.get(identity, :subject_id, "")

    _ =
      Identity.record_audit(context, %{
        action: "external_login.denied",
        resource_type: "external_identity",
        resource_id: external_identity_fingerprint(tenant_id, subject_id),
        outcome: "denied",
        provider: "azure_container_apps_entra"
      })

    :ok
  end

  defp external_identity_fingerprint(tenant_id, subject_id) do
    :crypto.hash(:sha256, Enum.join([tenant_id, subject_id], <<0>>))
    |> Base.encode16(case: :lower)
    |> String.slice(0, 16)
  end

  @doc "Resolves one session only within its explicit workspace."
  @spec introspect_session(WorkspaceContext.t(), String.t()) ::
          {:ok, session(), actor()} | {:error, term()}
  def introspect_session(%WorkspaceContext{} = context, session_token)
      when is_binary(session_token) do
    Store.introspect_session(context, session_token)
  end

  @doc "Lists the authenticated actor's workspaces without exposing other tenants to admins."
  @spec list_actor_workspaces(WorkspaceContext.t(), String.t()) ::
          {:ok, [map()]} | {:error, term()}
  def list_actor_workspaces(%WorkspaceContext{} = context, actor_id) when is_binary(actor_id) do
    Identity.list_actor_memberships(context, actor_id)
  end

  @doc "Rotates the authenticated session into another active workspace."
  @spec switch_workspace(WorkspaceContext.t(), session(), String.t()) ::
          {:ok, session()} | {:error, term()}
  def switch_workspace(%WorkspaceContext{} = context, session, target_workspace_id)
      when is_map(session) and is_binary(target_workspace_id) do
    Identity.rotate_workspace_session(context, session, target_workspace_id)
  end

  @doc "Revokes one session only within its explicit workspace."
  @spec revoke_session(WorkspaceContext.t(), String.t()) :: :ok | {:error, term()}
  def revoke_session(%WorkspaceContext{} = context, session_id) when is_binary(session_id) do
    Store.revoke_session(context, session_id)
  end

  @doc "Returns one bounded page of actors in the authorized workspace."
  @spec page_actors(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_actors(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    Identity.page_actors(context, opts)
  end

  @doc "Creates one actor membership in the authorized workspace."
  @spec create_actor(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          String.t(),
          [atom() | String.t()]
        ) :: {:ok, actor()} | {:error, term()}
  def create_actor(%WorkspaceContext{} = context, username, password, display_name, roles) do
    Identity.create_actor(context, username, password, display_name, roles)
  end

  @doc "Attaches an exact existing global username to the authorized workspace."
  @spec attach_actor_membership(WorkspaceContext.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def attach_actor_membership(%WorkspaceContext{} = context, username, roles) do
    Identity.attach_actor_membership(context, username, roles)
  end

  @doc "Replaces one actor's workspace roles using the current access version."
  @spec update_actor_roles(WorkspaceContext.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def update_actor_roles(%WorkspaceContext{} = context, actor_id, roles) do
    with {:ok, actor} <- Identity.get_actor(context, actor_id) do
      Identity.set_roles(context, actor_id, roles, actor.access_version)
    end
  end

  @doc "Changes current-workspace roles and membership status."
  @spec update_actor_membership(
          WorkspaceContext.t(),
          String.t(),
          [atom() | String.t()],
          :active | :suspended | :revoked
        ) :: {:ok, actor()} | {:error, term()}
  def update_actor_membership(%WorkspaceContext{} = context, actor_id, roles, status) do
    with {:ok, actor} <- Identity.get_actor(context, actor_id) do
      Identity.set_membership_access(context, actor_id, roles, status, actor.access_version)
    end
  end

  @doc "Returns workspace-bound sessions for current-workspace administration."
  @spec page_sessions(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_sessions(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    Identity.page_sessions(context, opts)
  end

  @doc "Verifies and changes the authenticated actor's own global password."
  @spec set_actor_password(WorkspaceContext.t(), String.t(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def set_actor_password(%WorkspaceContext{} = context, actor_id, current_password, new_password) do
    Identity.change_password(context, actor_id, current_password, new_password)
  end

  @doc "Fetches an actor through one explicit workspace membership."
  @spec get_actor(WorkspaceContext.t(), String.t()) :: {:ok, actor()} | {:error, term()}
  def get_actor(%WorkspaceContext{} = context, actor_id) do
    Identity.get_actor(context, actor_id)
  end

  @doc "Returns one bounded page of workspace authorization audit records."
  @spec page_audit(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_audit(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    Identity.page_audit(context, opts)
  end

  @doc "Resolves forwarded actor/session identity within an explicit workspace."
  @spec actor_from_forwarded_context(WorkspaceContext.t(), String.t() | nil, String.t()) ::
          {:ok, session(), actor()} | {:error, term()}
  def actor_from_forwarded_context(%WorkspaceContext{} = context, actor_id, session_token)
      when is_binary(session_token) do
    with {:ok, session, actor} <- Store.introspect_session(context, session_token),
         true <- is_nil(actor_id) or actor_id == "" or actor.id == actor_id do
      {:ok, session, actor}
    else
      false -> {:error, :actor_session_mismatch}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec has_role?(actor(), atom()) :: boolean()
  def has_role?(actor, required_role) when required_role in [:viewer, :operator, :admin] do
    actor
    |> Map.get(:roles, [])
    |> Enum.any?(&(role_weight(&1) >= role_weight(required_role)))
  end

  defp role_weight(:viewer), do: 10
  defp role_weight(:operator), do: 20
  defp role_weight(:admin), do: 30
  defp role_weight(_), do: 0

  defp bootstrap_workspace_actor(username, password, display_name, roles) do
    case configured_workspace_ids() do
      [] ->
        {:error, :workspace_ids_required_for_bootstrap}

      workspace_ids ->
        actor_id = deterministic_bootstrap_actor_id(username)

        Enum.reduce_while(workspace_ids, :ok, fn workspace_id, :ok ->
          {:ok, context} =
            WorkspaceContext.new(workspace_id, "favn:identity-bootstrap", [:workspace_admin])

          result =
            Identity.create_actor(context, username, password, display_name, roles,
              actor_id: actor_id,
              command_id: "identity-bootstrap:create:#{actor_id}"
            )

          case result do
            {:ok, _actor} ->
              {:cont, :ok}

            {:error, :username_taken} ->
              ensure_bootstrap_membership(context, actor_id, roles)

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end)
    end
  end

  defp ensure_bootstrap_membership(context, actor_id, roles) do
    case Identity.get_actor(context, actor_id) do
      {:ok, _actor} ->
        {:cont, :ok}

      {:error, :actor_not_found} ->
        case Identity.set_membership(context, actor_id, roles, 0) do
          {:ok, _actor} -> {:cont, :ok}
          {:error, _reason} -> {:halt, {:error, :bootstrap_actor_conflict}}
        end

      {:error, _reason} ->
        {:halt, {:error, :bootstrap_actor_conflict}}
    end
  end

  defp configured_workspace_ids do
    Application.get_env(:favn_orchestrator, :workspace_ids, [])
  end

  defp deterministic_bootstrap_actor_id(username) do
    digest =
      username
      |> String.trim()
      |> String.normalize(:nfkc)
      |> String.downcase()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "act_bootstrap_" <> String.slice(digest, 0, 32)
  end

  defp authorize_trusted_local_development(workspace_id, username, capability) do
    case Application.get_env(:favn_orchestrator, :trusted_local_development_auth) do
      %{
        workspace_id: ^workspace_id,
        username: ^username,
        capability_hash: expected_hash
      }
      when is_binary(expected_hash) ->
        provided_hash = ServiceTokens.hash_token(capability)

        if byte_size(provided_hash) == byte_size(expected_hash) and
             Plug.Crypto.secure_compare(provided_hash, expected_hash),
           do: :ok,
           else: {:error, :invalid_capability}

      _disabled ->
        {:error, :disabled}
    end
  end
end
