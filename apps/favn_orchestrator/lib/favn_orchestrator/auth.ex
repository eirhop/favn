defmodule FavnOrchestrator.Auth do
  @moduledoc """
  Orchestrator-owned actor, session, and authorization helpers.
  """

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

    if is_binary(username) and username != "" and is_binary(password) and password != "" do
      bootstrap_workspace_actor(username, password, display_name, roles)
    else
      :ok
    end
  end

  @doc "Authenticates and issues a session within one explicit workspace."
  @spec password_login(WorkspaceContext.t(), String.t(), String.t(), keyword() | map()) ::
          {:ok, session(), actor()} | {:error, term()}
  def password_login(%WorkspaceContext{} = context, username, password, opts) do
    with {:ok, actor} <- Store.authenticate_password(context, username, password, opts),
         {:ok, session} <- Store.issue_session(context, actor.id, provider: "password_local") do
      {:ok, session, actor}
    end
  end

  @doc "Resolves one session only within its explicit workspace."
  @spec introspect_session(WorkspaceContext.t(), String.t()) ::
          {:ok, session(), actor()} | {:error, term()}
  def introspect_session(%WorkspaceContext{} = context, session_token)
      when is_binary(session_token) do
    Store.introspect_session(context, session_token)
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

  @doc "Replaces one actor's workspace roles using the current access version."
  @spec update_actor_roles(WorkspaceContext.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def update_actor_roles(%WorkspaceContext{} = context, actor_id, roles) do
    with {:ok, actor} <- Identity.get_actor(context, actor_id) do
      Identity.set_roles(context, actor_id, roles, actor.access_version)
    end
  end

  @doc "Changes the authenticated actor's own password and revokes its active sessions."
  @spec set_actor_password(WorkspaceContext.t(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def set_actor_password(%WorkspaceContext{} = context, actor_id, password) do
    Identity.change_password(context, actor_id, password)
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
end
