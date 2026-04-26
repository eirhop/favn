defmodule FavnOrchestrator.Auth do
  @moduledoc """
  Orchestrator-owned actor, session, and authorization helpers.
  """

  alias FavnOrchestrator.Auth.Store

  @type actor :: Store.actor()
  @type session :: Store.session()

  @spec bootstrap_configured_actor() :: :ok | {:error, term()}
  def bootstrap_configured_actor do
    username = Application.get_env(:favn_orchestrator, :auth_bootstrap_username)
    password = Application.get_env(:favn_orchestrator, :auth_bootstrap_password)

    display_name =
      Application.get_env(:favn_orchestrator, :auth_bootstrap_display_name, "Favn Admin")

    roles = Application.get_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    cond do
      not is_binary(username) or username == "" ->
        :ok

      not is_binary(password) or password == "" ->
        :ok

      true ->
        case Store.create_actor(username, password, display_name, roles) do
          {:ok, _actor} -> :ok
          {:error, :username_taken} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec password_login(String.t(), String.t()) :: {:ok, session(), actor()} | {:error, term()}
  def password_login(username, password) do
    with {:ok, actor} <- Store.authenticate_password(username, password),
         {:ok, session} <- Store.issue_session(actor.id, provider: "password_local") do
      {:ok, session, actor}
    end
  end

  @spec introspect_session(String.t()) :: {:ok, session(), actor()} | {:error, term()}
  def introspect_session(session_id) when is_binary(session_id) do
    Store.introspect_session(session_id)
  end

  @spec revoke_session(String.t()) :: :ok
  def revoke_session(session_id) when is_binary(session_id) do
    Store.revoke_session(session_id)
  end

  @spec list_actors() :: [actor()]
  def list_actors, do: Store.list_actors()

  @spec create_actor(String.t(), String.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def create_actor(username, password, display_name, roles)
      when is_binary(username) and is_binary(password) and is_binary(display_name) and
             is_list(roles) do
    Store.create_actor(username, password, display_name, roles)
  end

  @spec update_actor_roles(String.t(), [atom() | String.t()]) :: {:ok, actor()} | {:error, term()}
  def update_actor_roles(actor_id, roles) when is_binary(actor_id) and is_list(roles) do
    Store.update_actor_roles(actor_id, roles)
  end

  @spec set_actor_password(String.t(), String.t()) :: :ok | {:error, term()}
  def set_actor_password(actor_id, password) when is_binary(actor_id) and is_binary(password) do
    Store.set_actor_password(actor_id, password)
  end

  @spec get_actor(String.t()) :: {:ok, actor()} | {:error, term()}
  def get_actor(actor_id), do: Store.get_actor(actor_id)

  @spec actor_from_forwarded_context(String.t(), String.t()) ::
          {:ok, session(), actor()} | {:error, term()}
  def actor_from_forwarded_context(actor_id, session_id)
      when is_binary(actor_id) and is_binary(session_id) do
    with {:ok, session, actor} <- Store.introspect_session(session_id),
         true <- actor.id == actor_id do
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

  @spec put_audit(map()) :: :ok
  def put_audit(entry) when is_map(entry) do
    Store.add_audit(entry)
  end

  @spec list_audit(keyword()) :: [map()]
  def list_audit(opts \\ []) when is_list(opts) do
    Store.list_audit(opts)
  end

  defp role_weight(:viewer), do: 10
  defp role_weight(:operator), do: 20
  defp role_weight(:admin), do: 30
  defp role_weight(_), do: 0
end
