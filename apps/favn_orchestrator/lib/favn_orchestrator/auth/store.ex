defmodule FavnOrchestrator.Auth.Store do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.LoginLimiter
  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @default_login_failure_limit 5
  @default_login_backoff_seconds 60
  @max_remote_identity_bytes 256
  @local_dev_actor_id "act_local_dev"

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
    GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec reset() :: :ok
  def reset, do: GenServer.call(__MODULE__, :reset)

  @spec create_actor(String.t(), String.t(), String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def create_actor(username, password, display_name, roles) do
    with {:ok, attrs} <- Credentials.normalize_actor(username, display_name, roles),
         :ok <- Credentials.validate_password(password) do
      actor = new_actor(attrs)

      case Storage.put_auth_actor_with_credential(actor, Credentials.hash_password(password)) do
        :ok -> {:ok, actor}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec list_actors() :: [actor()]
  def list_actors, do: Storage.list_auth_actors() |> unwrap_list()

  @spec update_actor_roles(String.t(), [atom() | String.t()]) ::
          {:ok, actor()} | {:error, term()}
  def update_actor_roles(actor_id, roles) when is_binary(actor_id) do
    with {:ok, roles} <- Credentials.normalize_roles(roles),
         {:ok, actor} <- get_actor(actor_id) do
      updated_actor = %{actor | roles: roles, updated_at: DateTime.utc_now()}

      case Storage.put_auth_actor(updated_actor) do
        :ok -> {:ok, updated_actor}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def update_actor_roles(_actor_id, _roles), do: {:error, :invalid_actor}

  @spec set_actor_password(String.t(), String.t()) :: :ok | {:error, term()}
  def set_actor_password(actor_id, password) when is_binary(actor_id) do
    with :ok <- Credentials.validate_password(password),
         {:ok, actor} <- get_actor(actor_id) do
      now = DateTime.utc_now()
      updated_actor = %{actor | updated_at: now}

      Storage.update_auth_actor_password(
        actor_id,
        updated_actor,
        Credentials.hash_password(password),
        now
      )
    end
  end

  def set_actor_password(_actor_id, _password), do: {:error, :invalid_actor}

  @spec get_actor(String.t()) :: {:ok, actor()} | {:error, :actor_not_found}
  def get_actor(actor_id) when is_binary(actor_id) do
    normalize_actor_result(Storage.get_auth_actor(actor_id))
  end

  def get_actor(_actor_id), do: {:error, :actor_not_found}

  @spec authenticate_password(String.t(), String.t(), keyword() | map()) ::
          {:ok, actor()} | {:error, :invalid_credentials}
  def authenticate_password(username, password, opts \\ []) do
    with true <- Credentials.valid_login_input?(username, password),
         normalized_username = String.trim(username),
         true <- normalized_username != "",
         {:ok, keys} <- login_attempt_keys(normalized_username, opts) do
      authenticate_limited(normalized_username, password, keys)
    else
      _invalid -> Credentials.dummy_verify()
    end
  end

  @spec issue_session(String.t(), keyword()) :: {:ok, session()} | {:error, term()}
  def issue_session(actor_id, opts \\ [])

  def issue_session(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    issue_session_record(actor_id, opts)
  end

  def issue_session(_actor_id, _opts), do: {:error, :invalid_session_options}

  @doc false
  @spec trusted_local_dev_context(String.t(), String.t(), [atom()]) ::
          {:ok, session(), actor()} | {:error, term()}
  def trusted_local_dev_context(username, display_name, roles) do
    with {:ok, attrs} <- Credentials.normalize_actor(username, display_name, roles) do
      GenServer.call(__MODULE__, {:trusted_local_dev_context, attrs})
    end
  end

  @spec introspect_session(String.t()) ::
          {:ok, session(), actor()} | {:error, :invalid_session | :actor_not_found}
  def introspect_session(session_token) do
    if Session.valid_token?(session_token) do
      case active_context(session_token) do
        {:ok, session, actor} -> {:ok, session, actor}
        {:error, :actor_not_found} -> {:error, :actor_not_found}
        {:error, _reason} -> {:error, :invalid_session}
      end
    else
      {:error, :invalid_session}
    end
  end

  @spec revoke_session(String.t()) :: :ok | {:error, term()}
  def revoke_session(session_id) when is_binary(session_id) and session_id != "" do
    Storage.revoke_auth_session(session_id, DateTime.utc_now())
  end

  def revoke_session(_session_id), do: {:error, :invalid_session}

  @spec add_audit(map()) :: :ok | {:error, term()}
  def add_audit(entry) when is_map(entry) do
    normalized =
      entry
      |> Redaction.redact_operational_bounded()
      |> Map.put_new(:id, "aud_" <> Session.random_id())
      |> Map.put_new(:occurred_at, DateTime.utc_now())

    Storage.put_auth_audit(normalized)
  end

  def add_audit(_entry), do: {:error, :invalid_audit_entry}

  @spec list_audit(keyword()) :: [map()]
  def list_audit(opts \\ [])

  def list_audit(opts) when is_list(opts),
    do: Storage.list_auth_audit(opts) |> unwrap_list()

  def list_audit(_opts), do: []

  @impl true
  def init(_state), do: {:ok, %{local_dev_contexts: %{}, login_attempts: %{}}}

  @impl true
  def handle_call(:reset, _from, state) do
    if Storage.adapter_module() == Memory do
      :ok = Memory.reset(Storage.adapter_opts())
    end

    {:reply, :ok, %{state | local_dev_contexts: %{}, login_attempts: %{}}}
  end

  def handle_call({:begin_login, keys}, _from, state) do
    {decision, attempts} =
      LoginLimiter.begin_attempt(
        state.login_attempts,
        keys,
        DateTime.utc_now(),
        login_failure_limit(),
        login_backoff_seconds()
      )

    {:reply, decision, %{state | login_attempts: attempts}}
  end

  def handle_call({:finish_login, keys, result}, _from, state) do
    attempts =
      LoginLimiter.finish_attempt(
        state.login_attempts,
        keys,
        result,
        DateTime.utc_now(),
        login_failure_limit(),
        login_backoff_seconds()
      )

    {:reply, :ok, %{state | login_attempts: attempts}}
  end

  def handle_call({:trusted_local_dev_context, attrs}, _from, state) do
    key = {attrs.username, attrs.roles}

    case cached_local_dev_context(state.local_dev_contexts[key], attrs.roles) do
      {:ok, session, actor} ->
        {:reply, {:ok, session, actor}, state}

      {:error, _reason} ->
        with {:ok, actor} <- ensure_local_dev_actor(attrs),
             {:ok, session} <- issue_session_record(actor.id, provider: "trusted_local_dev") do
          contexts = Map.put(state.local_dev_contexts, key, %{session_token: session.token})

          {:reply, {:ok, Map.delete(session, :token), actor},
           %{state | local_dev_contexts: contexts}}
        else
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  defp authenticate_limited(username, password, keys) do
    case GenServer.call(__MODULE__, {:begin_login, keys}) do
      :allowed ->
        result = authenticate_existing_actor(username, password)
        :ok = GenServer.call(__MODULE__, {:finish_login, keys, result_tag(result)})
        result

      :blocked ->
        Credentials.dummy_verify()
    end
  end

  defp authenticate_existing_actor(username, password) do
    case Storage.get_auth_actor_by_username(username) do
      {:ok, actor} -> authenticate_actor_credential(actor, password)
      {:error, _reason} -> Credentials.dummy_verify()
    end
  end

  defp authenticate_actor_credential(actor, password) do
    case Storage.get_auth_credential(actor.id) do
      {:ok, credential} ->
        password_result = Credentials.verify_password(password, credential)

        with :ok <- ensure_actor_active(actor),
             :ok <- password_result do
          {:ok, actor}
        else
          _reason -> {:error, :invalid_credentials}
        end

      {:error, _reason} ->
        Credentials.dummy_verify()
    end
  end

  defp result_tag({:ok, _actor}), do: :ok
  defp result_tag({:error, _reason}), do: :error

  defp login_attempt_keys(username, opts) do
    with {:ok, remote_identity} <- remote_identity(opts) do
      primary = {:credential, username, remote_identity}

      keys =
        if is_nil(remote_identity), do: [primary], else: [primary, {:remote, remote_identity}]

      {:ok, keys}
    end
  end

  defp remote_identity(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:remote_identity] == [] do
      normalize_remote_identity(Keyword.get(opts, :remote_identity))
    else
      {:error, :invalid_login_options}
    end
  end

  defp remote_identity(opts) when is_map(opts) do
    allowed = [:remote_identity, "remote_identity"]

    if Map.keys(opts) -- allowed == [] do
      normalize_remote_identity(
        Map.get(opts, :remote_identity) || Map.get(opts, "remote_identity")
      )
    else
      {:error, :invalid_login_options}
    end
  end

  defp remote_identity(_opts), do: {:error, :invalid_login_options}

  defp normalize_remote_identity(nil), do: {:ok, nil}

  defp normalize_remote_identity(value)
       when is_binary(value) and byte_size(value) <= @max_remote_identity_bytes do
    case String.trim(value) do
      "" -> {:ok, nil}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_remote_identity(_value), do: {:error, :invalid_remote_identity}

  defp cached_local_dev_context(nil, _roles), do: {:error, :not_cached}

  defp cached_local_dev_context(%{session_token: token}, roles) do
    with {:ok, session, actor} <- active_context(token),
         true <- actor.id == @local_dev_actor_id and Enum.all?(roles, &(&1 in actor.roles)) do
      {:ok, session, actor}
    else
      false -> {:error, :local_dev_context_changed}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_local_dev_actor(attrs) do
    case Storage.get_auth_actor_by_username(attrs.username) do
      {:ok, %{id: @local_dev_actor_id} = actor} ->
        with :ok <- ensure_actor_active(actor), do: update_local_dev_actor(actor, attrs)

      {:ok, _other_actor} ->
        {:error, :reserved_local_dev_username}

      {:error, :not_found} ->
        create_local_dev_actor(attrs)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp update_local_dev_actor(actor, attrs) do
    if actor.display_name == attrs.display_name and actor.roles == attrs.roles do
      {:ok, actor}
    else
      updated = %{
        actor
        | display_name: attrs.display_name,
          roles: attrs.roles,
          updated_at: DateTime.utc_now()
      }

      case Storage.put_auth_actor(updated) do
        :ok -> {:ok, updated}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp create_local_dev_actor(attrs) do
    actor = new_actor(Map.put(attrs, :id, @local_dev_actor_id))
    credential = Credentials.hash_password(Session.raw_token())

    case Storage.put_auth_actor_with_credential(actor, credential) do
      :ok -> {:ok, actor}
      {:error, reason} -> {:error, reason}
    end
  end

  defp issue_session_record(actor_id, opts) do
    with {:ok, actor} <- get_actor(actor_id),
         :ok <- ensure_actor_active(actor),
         {:ok, session} <- Session.issue(actor_id, opts),
         :ok <- Storage.put_auth_session(Map.delete(session, :token)) do
      {:ok, Map.delete(session, :token_hash)}
    else
      {:error, reason} -> {:error, normalize_session_error(reason)}
    end
  end

  defp active_context(session_token) do
    with {:ok, session} <-
           Storage.get_auth_session_by_token_hash(Session.token_hash(session_token)),
         :ok <- Session.active?(session),
         {:ok, actor} <- get_actor(session.actor_id),
         :ok <- ensure_actor_active(actor) do
      {:ok, Map.drop(session, [:token_hash]), actor}
    end
  end

  defp new_actor(attrs) do
    now = DateTime.utc_now()

    %{
      id: Map.get(attrs, :id, "act_" <> Session.random_id()),
      username: attrs.username,
      display_name: attrs.display_name,
      roles: attrs.roles,
      status: :active,
      inserted_at: now,
      updated_at: now
    }
  end

  defp ensure_actor_active(%{status: :active}), do: :ok
  defp ensure_actor_active(_actor), do: {:error, :actor_disabled}

  defp normalize_actor_result({:ok, actor}), do: {:ok, actor}
  defp normalize_actor_result({:error, _reason}), do: {:error, :actor_not_found}

  defp normalize_session_error(reason)
       when reason in [
              :invalid_session_options,
              :invalid_session_provider,
              :invalid_session_ttl
            ],
       do: reason

  defp normalize_session_error(_reason), do: :actor_not_found

  defp unwrap_list({:ok, values}), do: values
  defp unwrap_list({:error, _reason}), do: []

  defp login_failure_limit do
    positive_config(:auth_login_failure_limit, @default_login_failure_limit)
  end

  defp login_backoff_seconds do
    positive_config(:auth_login_backoff_seconds, @default_login_backoff_seconds)
  end

  defp positive_config(key, default) do
    case Application.get_env(:favn_orchestrator, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end
end
