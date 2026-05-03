defmodule FavnOrchestrator.Auth.Store do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @min_password_length 15
  @max_password_length 1_024

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
          required(:revoked_at) => DateTime.t() | nil,
          optional(:token) => String.t(),
          optional(:token_hash) => String.t()
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

  @spec create_actor(String.t(), String.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def create_actor(username, password, display_name, roles)
      when is_binary(username) and is_binary(password) and is_binary(display_name) and
             is_list(roles) do
    GenServer.call(__MODULE__, {:create_actor, username, password, display_name, roles})
  end

  @spec list_actors() :: [actor()]
  def list_actors, do: GenServer.call(__MODULE__, :list_actors)

  @spec update_actor_roles(String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, :actor_not_found}
  def update_actor_roles(actor_id, roles) when is_binary(actor_id) and is_list(roles) do
    GenServer.call(__MODULE__, {:update_actor_roles, actor_id, roles})
  end

  @spec set_actor_password(String.t(), String.t()) ::
          :ok
          | {:error,
             :actor_not_found | :password_too_short | :password_too_long | :password_blank}
  def set_actor_password(actor_id, password) when is_binary(actor_id) and is_binary(password) do
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
  def introspect_session(session_token) when is_binary(session_token) do
    GenServer.call(__MODULE__, {:introspect_session, session_token})
  end

  @spec revoke_session(String.t()) :: :ok | {:error, term()}
  def revoke_session(session_id) when is_binary(session_id) do
    GenServer.call(__MODULE__, {:revoke_session, session_id})
  end

  @spec add_audit(map()) :: :ok | {:error, term()}
  def add_audit(entry) when is_map(entry) do
    GenServer.call(__MODULE__, {:add_audit, entry})
  end

  @spec list_audit(keyword()) :: [map()]
  def list_audit(opts \\ []) when is_list(opts) do
    GenServer.call(__MODULE__, {:list_audit, opts})
  end

  @impl true
  def init(_state), do: {:ok, %{}}

  @impl true
  def handle_call(:reset, _from, state) do
    case Storage.adapter_module() do
      Memory ->
        :ok = Memory.reset(Storage.adapter_opts())

      _adapter ->
        :ok
    end

    {:reply, :ok, state}
  end

  def handle_call({:create_actor, username, password, display_name, roles}, _from, state) do
    normalized_username = String.trim(username)

    cond do
      normalized_username == "" ->
        {:reply, {:error, :invalid_username}, state}

      match?({:error, _reason}, validate_password_policy(password)) ->
        {:error, reason} = validate_password_policy(password)
        {:reply, {:error, reason}, state}

      match?({:ok, _actor}, Storage.get_auth_actor_by_username(normalized_username)) ->
        {:reply, {:error, :username_taken}, state}

      true ->
        now = DateTime.utc_now()

        actor = %{
          id: "act_" <> random_id(),
          username: normalized_username,
          display_name: display_name,
          roles: normalize_roles(roles),
          status: :active,
          inserted_at: now,
          updated_at: now
        }

        case Storage.put_auth_actor_with_credential(actor, hash_password(password)) do
          :ok -> {:reply, {:ok, actor}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:list_actors, _from, state) do
    {:reply, Storage.list_auth_actors() |> unwrap_list(), state}
  end

  def handle_call({:update_actor_roles, actor_id, roles}, _from, state) do
    case Storage.get_auth_actor(actor_id) do
      {:ok, actor} ->
        updated_actor = %{actor | roles: normalize_roles(roles), updated_at: DateTime.utc_now()}

        case Storage.put_auth_actor(updated_actor) do
          :ok -> {:reply, {:ok, updated_actor}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:error, _reason} ->
        {:reply, {:error, :actor_not_found}, state}
    end
  end

  def handle_call({:set_actor_password, actor_id, password}, _from, state) do
    if match?({:error, _reason}, validate_password_policy(password)) do
      {:error, reason} = validate_password_policy(password)
      {:reply, {:error, reason}, state}
    else
      case Storage.get_auth_actor(actor_id) do
        {:ok, actor} ->
          now = DateTime.utc_now()
          updated_actor = %{actor | updated_at: now}

          case Storage.update_auth_actor_password(
                 actor_id,
                 updated_actor,
                 hash_password(password),
                 now
               ) do
            :ok -> {:reply, :ok, state}
            {:error, reason} -> {:reply, {:error, reason}, state}
          end

        {:error, _reason} ->
          {:reply, {:error, :actor_not_found}, state}
      end
    end
  end

  def handle_call({:get_actor, actor_id}, _from, state) do
    {:reply, normalize_actor_result(Storage.get_auth_actor(actor_id)), state}
  end

  def handle_call({:authenticate_password, username, password}, _from, state) do
    normalized_username = String.trim(username)

    reply =
      case Storage.get_auth_actor_by_username(normalized_username) do
        {:ok, actor} ->
          authenticate_existing_actor(actor, password)

        {:error, _reason} ->
          dummy_password_verify()
      end

    {:reply, reply, state}
  end

  def handle_call({:issue_session, actor_id, opts}, _from, state) do
    with {:ok, actor} <- Storage.get_auth_actor(actor_id),
         :ok <- ensure_actor_active(actor) do
      now = DateTime.utc_now()

      case session_ttl_seconds(opts) do
        {:ok, ttl_seconds} ->
          token = raw_session_token()

          session = %{
            id: "ses_" <> random_id(),
            actor_id: actor_id,
            provider: Keyword.get(opts, :provider, "password_local"),
            issued_at: now,
            expires_at: DateTime.add(now, ttl_seconds, :second),
            revoked_at: nil,
            token_hash: token_hash(token)
          }

          case Storage.put_auth_session(session) do
            :ok ->
              {:reply, {:ok, session |> Map.drop([:token_hash]) |> Map.put(:token, token)}, state}

            {:error, reason} ->
              {:reply, {:error, reason}, state}
          end

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:error, _reason} -> {:reply, {:error, :actor_not_found}, state}
    end
  end

  def handle_call({:introspect_session, session_token}, _from, state) do
    with {:ok, session} <- Storage.get_auth_session_by_token_hash(token_hash(session_token)),
         :ok <- ensure_session_active(session),
         {:ok, actor} <- Storage.get_auth_actor(session.actor_id),
         :ok <- ensure_actor_active(actor) do
      {:reply, {:ok, Map.drop(session, [:token_hash]), actor}, state}
    else
      {:error, :not_found} -> {:reply, {:error, :invalid_session}, state}
      {:error, :actor_not_found} -> {:reply, {:error, :actor_not_found}, state}
      _other -> {:reply, {:error, :invalid_session}, state}
    end
  end

  def handle_call({:revoke_session, session_id}, _from, state) do
    {:reply, Storage.revoke_auth_session(session_id, DateTime.utc_now()), state}
  end

  def handle_call({:add_audit, entry}, _from, state) do
    normalized =
      entry
      |> Redaction.redact()
      |> Map.put_new(:id, "aud_" <> random_id())
      |> Map.put_new(:occurred_at, DateTime.utc_now())

    {:reply, Storage.put_auth_audit(normalized), state}
  end

  def handle_call({:list_audit, opts}, _from, state) do
    {:reply, Storage.list_auth_audit(opts) |> unwrap_list(), state}
  end

  defp normalize_actor_result({:ok, actor}), do: {:ok, actor}
  defp normalize_actor_result({:error, _reason}), do: {:error, :actor_not_found}

  defp unwrap_list({:ok, values}), do: values
  defp unwrap_list({:error, _reason}), do: []

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
    %{password_hash: Argon2.hash_pwd_salt(password)}
  end

  defp verify_password(password, %{password_hash: password_hash}) when is_binary(password_hash) do
    if String.starts_with?(password_hash, "$argon2") do
      case Argon2.verify_pass(password, password_hash) do
        true -> :ok
        false -> {:error, :invalid_credentials}
      end
    else
      dummy_password_verify()
    end
  rescue
    _exception -> dummy_password_verify()
  end

  defp verify_password(_password, _credential), do: dummy_password_verify()

  defp authenticate_existing_actor(actor, password) do
    case Storage.get_auth_credential(actor.id) do
      {:ok, credential} ->
        password_result = verify_password(password, credential)

        with :ok <- ensure_actor_active(actor),
             :ok <- password_result do
          {:ok, actor}
        else
          _other -> {:error, :invalid_credentials}
        end

      {:error, _reason} ->
        dummy_password_verify()
    end
  end

  defp dummy_password_verify do
    Argon2.no_user_verify()
    {:error, :invalid_credentials}
  end

  defp validate_password_policy(password) do
    password_length = password |> String.trim() |> String.length()

    cond do
      String.trim(password) == "" -> {:error, :password_blank}
      password_length < @min_password_length -> {:error, :password_too_short}
      password_length > @max_password_length -> {:error, :password_too_long}
      true -> :ok
    end
  end

  defp session_ttl_seconds(opts) do
    opts
    |> Keyword.get(:ttl_seconds, default_session_ttl_seconds())
    |> case do
      ttl when is_integer(ttl) and ttl > 0 -> {:ok, ttl}
      _ttl -> {:error, :invalid_session_ttl}
    end
  end

  defp raw_session_token do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp token_hash(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.url_encode64(padding: false)
  end

  defp random_id do
    10
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp default_session_ttl_seconds do
    Application.get_env(:favn_orchestrator, :auth_session_ttl_seconds, 43_200)
  end
end
