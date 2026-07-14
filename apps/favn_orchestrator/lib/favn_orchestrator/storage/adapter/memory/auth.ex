defmodule FavnOrchestrator.Storage.Adapter.Memory.Auth do
  @moduledoc """
  Pure authentication persistence operations for the in-memory adapter.

  Secondary indexes are updated atomically with their records so the adapter
  preserves the uniqueness guarantees provided by database-backed adapters.
  """

  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @doc false
  @spec put_actor(State.t(), map()) :: {:ok | {:error, :username_taken}, State.t()}
  def put_actor(%State{} = state, actor) when is_map(actor) do
    case username_available(state, actor.username, actor.id) do
      :ok -> {:ok, store_actor(state, actor)}
      {:error, :username_taken} = error -> {error, state}
    end
  end

  @doc false
  @spec put_actor_with_credential(State.t(), map(), map()) ::
          {:ok | {:error, :username_taken}, State.t()}
  def put_actor_with_credential(%State{} = state, actor, credential)
      when is_map(actor) and is_map(credential) do
    case put_actor(state, actor) do
      {:ok, next_state} ->
        {:ok,
         %{
           next_state
           | auth_credentials: Map.put(next_state.auth_credentials, actor.id, credential)
         }}

      {{:error, _reason} = error, _state} ->
        {error, state}
    end
  end

  @doc false
  @spec get_actor(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_actor(%State{} = state, actor_id), do: fetch(state.auth_actors, actor_id)

  @doc false
  @spec get_actor_by_username(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_actor_by_username(%State{} = state, username) do
    with {:ok, actor_id} <- fetch(state.auth_usernames, username) do
      fetch(state.auth_actors, actor_id)
    end
  end

  @doc false
  @spec list_actors(State.t()) :: [map()]
  def list_actors(%State{} = state) do
    state.auth_actors
    |> Map.values()
    |> Enum.sort_by(& &1.username)
  end

  @doc false
  @spec put_credential(State.t(), String.t(), map()) :: State.t()
  def put_credential(%State{} = state, actor_id, credential) do
    %{state | auth_credentials: Map.put(state.auth_credentials, actor_id, credential)}
  end

  @doc false
  @spec get_credential(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_credential(%State{} = state, actor_id), do: fetch(state.auth_credentials, actor_id)

  @doc false
  @spec update_password(State.t(), String.t(), map(), map(), DateTime.t()) ::
          {:ok | {:error, :username_taken}, State.t()}
  def update_password(%State{} = state, actor_id, actor, credential, %DateTime{} = revoked_at) do
    case put_actor(state, actor) do
      {:ok, next_state} ->
        next_state =
          next_state
          |> put_credential(actor_id, credential)
          |> revoke_sessions_for_actor(actor_id, revoked_at)

        {:ok, next_state}

      {{:error, _reason} = error, _state} ->
        {error, state}
    end
  end

  @doc false
  @spec put_session(State.t(), map()) :: {:ok | {:error, :session_token_taken}, State.t()}
  def put_session(%State{} = state, session) when is_map(session) do
    case token_hash_available(state, session.token_hash, session.id) do
      :ok ->
        hashes =
          state
          |> remove_previous_session_hash(session.id)
          |> Map.put(session.token_hash, session.id)

        {:ok,
         %{
           state
           | auth_sessions: Map.put(state.auth_sessions, session.id, session),
             auth_session_hashes: hashes
         }}

      {:error, :session_token_taken} = error ->
        {error, state}
    end
  end

  @doc false
  @spec get_session(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_session(%State{} = state, session_id), do: fetch(state.auth_sessions, session_id)

  @doc false
  @spec get_session_by_token_hash(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_session_by_token_hash(%State{} = state, token_hash) do
    with {:ok, session_id} <- fetch(state.auth_session_hashes, token_hash) do
      fetch(state.auth_sessions, session_id)
    end
  end

  @doc false
  @spec revoke_session(State.t(), String.t(), DateTime.t()) ::
          {:ok | {:error, :not_found}, State.t()}
  def revoke_session(%State{} = state, session_id, %DateTime{} = revoked_at) do
    case Map.fetch(state.auth_sessions, session_id) do
      {:ok, session} ->
        session = set_revoked_at_once(session, revoked_at)
        sessions = Map.put(state.auth_sessions, session_id, session)
        {:ok, %{state | auth_sessions: sessions}}

      :error ->
        {{:error, :not_found}, state}
    end
  end

  @doc false
  @spec revoke_sessions_for_actor(State.t(), String.t(), DateTime.t()) :: State.t()
  def revoke_sessions_for_actor(%State{} = state, actor_id, %DateTime{} = revoked_at) do
    sessions =
      Map.new(state.auth_sessions, fn {session_id, session} ->
        if session.actor_id == actor_id do
          {session_id, set_revoked_at_once(session, revoked_at)}
        else
          {session_id, session}
        end
      end)

    %{state | auth_sessions: sessions}
  end

  @doc false
  @spec put_audit(State.t(), map()) :: State.t()
  def put_audit(%State{} = state, entry), do: %{state | auth_audits: [entry | state.auth_audits]}

  @doc false
  @spec list_audit(State.t(), keyword()) :: [map()]
  def list_audit(%State{} = state, opts) do
    limit = opts |> Keyword.get(:limit, 100) |> max(1) |> min(500)
    Enum.take(state.auth_audits, limit)
  end

  defp store_actor(state, actor) do
    usernames =
      state
      |> remove_previous_username(actor.id)
      |> Map.put(actor.username, actor.id)

    %{
      state
      | auth_actors: Map.put(state.auth_actors, actor.id, actor),
        auth_usernames: usernames
    }
  end

  defp username_available(state, username, actor_id) do
    case Map.get(state.auth_usernames, username) do
      nil -> :ok
      ^actor_id -> :ok
      _other_actor_id -> {:error, :username_taken}
    end
  end

  defp token_hash_available(state, token_hash, session_id) do
    case Map.get(state.auth_session_hashes, token_hash) do
      nil -> :ok
      ^session_id -> :ok
      _other_session_id -> {:error, :session_token_taken}
    end
  end

  defp remove_previous_username(state, actor_id) do
    case Map.get(state.auth_actors, actor_id) do
      %{username: username} -> Map.delete(state.auth_usernames, username)
      nil -> state.auth_usernames
    end
  end

  defp remove_previous_session_hash(state, session_id) do
    case Map.get(state.auth_sessions, session_id) do
      %{token_hash: token_hash} -> Map.delete(state.auth_session_hashes, token_hash)
      nil -> state.auth_session_hashes
    end
  end

  defp set_revoked_at_once(%{revoked_at: nil} = session, revoked_at),
    do: %{session | revoked_at: revoked_at}

  defp set_revoked_at_once(session, _revoked_at), do: session

  defp fetch(values, key) do
    case Map.fetch(values, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end
end
