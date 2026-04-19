defmodule FavnOrchestrator.Auth.Store do
  @moduledoc false

  use GenServer

  @type actor :: %{
          required(:id) => String.t(),
          required(:username) => String.t(),
          required(:display_name) => String.t(),
          required(:roles) => [atom()],
          required(:status) => :active | :disabled,
          required(:inserted_at) => DateTime.t(),
          required(:updated_at) => DateTime.t()
        }

  @type session :: %{
          required(:id) => String.t(),
          required(:actor_id) => String.t(),
          required(:provider) => String.t(),
          required(:issued_at) => DateTime.t(),
          required(:expires_at) => DateTime.t(),
          required(:revoked_at) => DateTime.t() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @spec create_actor(String.t(), String.t(), String.t(), [atom()]) ::
          {:ok, actor()} | {:error, term()}
  def create_actor(username, password, display_name, roles)
      when is_binary(username) and is_binary(password) and is_binary(display_name) and
             is_list(roles) do
    GenServer.call(__MODULE__, {:create_actor, username, password, display_name, roles})
  end

  @spec list_actors() :: [actor()]
  def list_actors do
    GenServer.call(__MODULE__, :list_actors)
  end

  @spec update_actor_roles(String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, :actor_not_found}
  def update_actor_roles(actor_id, roles) when is_binary(actor_id) and is_list(roles) do
    GenServer.call(__MODULE__, {:update_actor_roles, actor_id, roles})
  end

  @spec set_actor_password(String.t(), String.t()) ::
          :ok | {:error, :actor_not_found | :password_too_short}
  def set_actor_password(actor_id, password)
      when is_binary(actor_id) and is_binary(password) do
    GenServer.call(__MODULE__, {:set_actor_password, actor_id, password})
  end

  @spec get_actor(String.t()) :: {:ok, actor()} | {:error, :actor_not_found}
  def get_actor(actor_id) when is_binary(actor_id) do
    GenServer.call(__MODULE__, {:get_actor, actor_id})
  end

  @spec authenticate_password(String.t(), String.t()) ::
          {:ok, actor()} | {:error, :invalid_credentials}
  def authenticate_password(username, password)
      when is_binary(username) and is_binary(password) do
    GenServer.call(__MODULE__, {:authenticate_password, username, password})
  end

  @spec issue_session(String.t(), keyword()) :: {:ok, session()} | {:error, term()}
  def issue_session(actor_id, opts \\ []) when is_binary(actor_id) and is_list(opts) do
    GenServer.call(__MODULE__, {:issue_session, actor_id, opts})
  end

  @spec introspect_session(String.t()) ::
          {:ok, session(), actor()} | {:error, :invalid_session | :actor_not_found}
  def introspect_session(session_id) when is_binary(session_id) do
    GenServer.call(__MODULE__, {:introspect_session, session_id})
  end

  @spec revoke_session(String.t()) :: :ok
  def revoke_session(session_id) when is_binary(session_id) do
    GenServer.call(__MODULE__, {:revoke_session, session_id})
  end

  @spec add_audit(map()) :: :ok
  def add_audit(entry) when is_map(entry) do
    GenServer.call(__MODULE__, {:add_audit, entry})
  end

  @spec list_audit(keyword()) :: [map()]
  def list_audit(opts \\ []) when is_list(opts) do
    GenServer.call(__MODULE__, {:list_audit, opts})
  end

  @impl true
  def init(_state) do
    {:ok,
     %{
       actors: %{},
       usernames: %{},
       credentials: %{},
       sessions: %{},
       audits: []
     }}
  end

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok,
     %{
       actors: %{},
       usernames: %{},
       credentials: %{},
       sessions: %{},
       audits: []
     }}
  end

  def handle_call({:create_actor, username, password, display_name, roles}, _from, state) do
    normalized_username = String.trim(username)

    cond do
      normalized_username == "" ->
        {:reply, {:error, :invalid_username}, state}

      byte_size(password) < 8 ->
        {:reply, {:error, :password_too_short}, state}

      Map.has_key?(state.usernames, normalized_username) ->
        {:reply, {:error, :username_taken}, state}

      true ->
        now = DateTime.utc_now()
        actor_id = "act_" <> random_id()

        actor = %{
          id: actor_id,
          username: normalized_username,
          display_name: display_name,
          roles: normalize_roles(roles),
          status: :active,
          inserted_at: now,
          updated_at: now
        }

        credential = hash_password(password)

        next_state = %{
          state
          | actors: Map.put(state.actors, actor_id, actor),
            usernames: Map.put(state.usernames, normalized_username, actor_id),
            credentials: Map.put(state.credentials, actor_id, credential)
        }

        {:reply, {:ok, actor}, next_state}
    end
  end

  def handle_call(:list_actors, _from, state) do
    actors =
      state.actors
      |> Map.values()
      |> Enum.sort_by(& &1.username)

    {:reply, actors, state}
  end

  def handle_call({:update_actor_roles, actor_id, roles}, _from, state) do
    now = DateTime.utc_now()

    case Map.fetch(state.actors, actor_id) do
      {:ok, actor} ->
        updated_actor = %{actor | roles: normalize_roles(roles), updated_at: now}

        {:reply, {:ok, updated_actor},
         %{state | actors: Map.put(state.actors, actor_id, updated_actor)}}

      :error ->
        {:reply, {:error, :actor_not_found}, state}
    end
  end

  def handle_call({:set_actor_password, actor_id, password}, _from, state) do
    cond do
      byte_size(password) < 8 ->
        {:reply, {:error, :password_too_short}, state}

      not Map.has_key?(state.actors, actor_id) ->
        {:reply, {:error, :actor_not_found}, state}

      true ->
        now = DateTime.utc_now()
        updated_credential = hash_password(password)

        actors =
          Map.update!(state.actors, actor_id, fn actor ->
            %{actor | updated_at: now}
          end)

        sessions =
          Enum.reduce(state.sessions, state.sessions, fn {session_id, session}, acc ->
            if session.actor_id == actor_id and is_nil(session.revoked_at) do
              Map.put(acc, session_id, %{session | revoked_at: now})
            else
              acc
            end
          end)

        next_state = %{
          state
          | actors: actors,
            credentials: Map.put(state.credentials, actor_id, updated_credential),
            sessions: sessions
        }

        {:reply, :ok, next_state}
    end
  end

  def handle_call({:get_actor, actor_id}, _from, state) do
    case Map.fetch(state.actors, actor_id) do
      {:ok, actor} -> {:reply, {:ok, actor}, state}
      :error -> {:reply, {:error, :actor_not_found}, state}
    end
  end

  def handle_call({:authenticate_password, username, password}, _from, state) do
    normalized_username = String.trim(username)

    with {:ok, actor_id} <- fetch_username(state, normalized_username),
         {:ok, actor} <- fetch_actor(state, actor_id),
         :ok <- ensure_actor_active(actor),
         {:ok, credential} <- fetch_credential(state, actor_id),
         :ok <- verify_password(password, credential) do
      {:reply, {:ok, actor}, state}
    else
      _other ->
        {:reply, {:error, :invalid_credentials}, state}
    end
  end

  def handle_call({:issue_session, actor_id, opts}, _from, state) do
    with {:ok, actor} <- fetch_actor(state, actor_id),
         :ok <- ensure_actor_active(actor) do
      now = DateTime.utc_now()
      ttl_seconds = Keyword.get(opts, :ttl_seconds, default_session_ttl_seconds())
      expires_at = DateTime.add(now, ttl_seconds, :second)

      session = %{
        id: "ses_" <> random_id(),
        actor_id: actor_id,
        provider: Keyword.get(opts, :provider, "password_local"),
        issued_at: now,
        expires_at: expires_at,
        revoked_at: nil
      }

      {:reply, {:ok, session}, %{state | sessions: Map.put(state.sessions, session.id, session)}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:introspect_session, session_id}, _from, state) do
    with {:ok, session} <- fetch_session(state, session_id),
         :ok <- ensure_session_active(session),
         {:ok, actor} <- fetch_actor(state, session.actor_id),
         :ok <- ensure_actor_active(actor) do
      {:reply, {:ok, session, actor}, state}
    else
      {:error, :actor_not_found} -> {:reply, {:error, :actor_not_found}, state}
      _ -> {:reply, {:error, :invalid_session}, state}
    end
  end

  def handle_call({:revoke_session, session_id}, _from, state) do
    now = DateTime.utc_now()

    sessions =
      Map.update(state.sessions, session_id, nil, fn session ->
        if is_nil(session.revoked_at), do: %{session | revoked_at: now}, else: session
      end)

    {:reply, :ok, %{state | sessions: sessions}}
  end

  def handle_call({:add_audit, entry}, _from, state) do
    normalized = Map.put_new(entry, :occurred_at, DateTime.utc_now())
    {:reply, :ok, %{state | audits: [normalized | state.audits]}}
  end

  def handle_call({:list_audit, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 100)
    {:reply, state.audits |> Enum.take(limit) |> Enum.reverse(), state}
  end

  defp fetch_username(state, username) do
    case Map.fetch(state.usernames, username) do
      {:ok, actor_id} -> {:ok, actor_id}
      :error -> {:error, :actor_not_found}
    end
  end

  defp fetch_actor(state, actor_id) do
    case Map.fetch(state.actors, actor_id) do
      {:ok, actor} -> {:ok, actor}
      :error -> {:error, :actor_not_found}
    end
  end

  defp fetch_credential(state, actor_id) do
    case Map.fetch(state.credentials, actor_id) do
      {:ok, credential} -> {:ok, credential}
      :error -> {:error, :credential_not_found}
    end
  end

  defp fetch_session(state, session_id) do
    case Map.fetch(state.sessions, session_id) do
      {:ok, session} -> {:ok, session}
      :error -> {:error, :session_not_found}
    end
  end

  defp ensure_actor_active(%{status: :active}), do: :ok
  defp ensure_actor_active(_actor), do: {:error, :actor_disabled}

  defp ensure_session_active(session) do
    now = DateTime.utc_now()

    cond do
      not is_nil(session.revoked_at) -> {:error, :session_revoked}
      DateTime.compare(session.expires_at, now) == :lt -> {:error, :session_expired}
      true -> :ok
    end
  end

  defp normalize_roles(roles) do
    roles
    |> Enum.map(fn
      role when role in [:viewer, :operator, :admin] -> role
      "viewer" -> :viewer
      "operator" -> :operator
      "admin" -> :admin
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> case do
      [] -> [:viewer]
      value -> value
    end
  end

  defp hash_password(password) do
    salt = :crypto.strong_rand_bytes(16)
    iterations = 100_000
    digest = :crypto.pbkdf2_hmac(:sha256, password, salt, iterations, 32)

    %{
      algorithm: :pbkdf2_sha256,
      iterations: iterations,
      salt: Base.encode64(salt),
      digest: Base.encode64(digest)
    }
  end

  defp verify_password(password, credential) do
    salt = Base.decode64!(credential.salt)
    expected = Base.decode64!(credential.digest)

    candidate =
      :crypto.pbkdf2_hmac(:sha256, password, salt, credential.iterations, byte_size(expected))

    if Plug.Crypto.secure_compare(expected, candidate) do
      :ok
    else
      {:error, :invalid_credentials}
    end
  end

  defp random_id do
    :crypto.strong_rand_bytes(10)
    |> Base.url_encode64(padding: false)
  end

  defp default_session_ttl_seconds do
    Application.get_env(:favn_orchestrator, :auth_session_ttl_seconds, 43_200)
  end
end
