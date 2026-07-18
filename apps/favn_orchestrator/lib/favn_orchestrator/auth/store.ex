defmodule FavnOrchestrator.Auth.Store do
  @moduledoc """
  Node-local login throttling and trusted-development session caching.

  Actors, credentials, memberships, and sessions are authoritative PostgreSQL
  data owned by `FavnOrchestrator.Identity`. This process deliberately keeps
  only disposable rate-limit counters and opaque development session tokens.
  """

  use GenServer

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.LoginLimiter
  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @default_login_failure_limit 5
  @default_login_backoff_seconds 60
  @max_remote_identity_bytes 256
  @local_dev_actor_id "act_local_dev"

  @type actor :: Identity.actor()
  @type session :: Identity.session()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc false
  @spec reset() :: :ok
  def reset, do: GenServer.call(__MODULE__, :reset)

  @doc false
  @spec authenticate_password(WorkspaceContext.t(), String.t(), String.t(), keyword() | map()) ::
          {:ok, actor()} | {:error, :invalid_credentials}
  def authenticate_password(%WorkspaceContext{} = context, username, password, opts) do
    with true <- Credentials.valid_login_input?(username, password),
         normalized_username = String.trim(username),
         true <- normalized_username != "",
         {:ok, keys} <- login_attempt_keys(normalized_username, opts) do
      authenticate_limited(keys, fn ->
        Identity.authenticate_password(context, normalized_username, password)
      end)
    else
      _invalid -> Credentials.dummy_verify()
    end
  end

  @doc false
  @spec issue_session(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, session()} | {:error, term()}
  def issue_session(%WorkspaceContext{} = context, actor_id, opts)
      when is_binary(actor_id) and is_list(opts) do
    Identity.issue_session(context, actor_id, opts)
  end

  @doc false
  @spec introspect_session(WorkspaceContext.t(), String.t()) ::
          {:ok, session(), actor()} | {:error, term()}
  def introspect_session(%WorkspaceContext{} = context, session_token) do
    Identity.introspect_session(context, session_token)
  end

  @doc false
  @spec revoke_session(WorkspaceContext.t(), String.t()) :: :ok | {:error, term()}
  def revoke_session(%WorkspaceContext{} = context, session_id)
      when is_binary(session_id) and session_id != "" do
    Identity.revoke_session(context, session_id)
  end

  @doc false
  @spec trusted_local_dev_context(WorkspaceContext.t(), String.t(), String.t(), [atom()]) ::
          {:ok, session(), actor()} | {:error, term()}
  def trusted_local_dev_context(%WorkspaceContext{} = context, username, display_name, roles) do
    with {:ok, attrs} <- Credentials.normalize_actor(username, display_name, roles) do
      GenServer.call(__MODULE__, {:trusted_local_dev_context, context, attrs})
    end
  end

  @impl true
  def init(_state), do: {:ok, %{local_dev_contexts: %{}, login_attempts: %{}}}

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{local_dev_contexts: %{}, login_attempts: %{}}}
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

  def handle_call({:trusted_local_dev_context, context, attrs}, _from, state) do
    key = {context.workspace_id, attrs.username, attrs.roles}

    result =
      case cached_local_dev_context(state.local_dev_contexts[key], context, attrs.roles) do
        {:ok, _session, _actor} = cached ->
          cached

        {:error, _reason} ->
          with {:ok, actor} <- ensure_local_dev_actor(context, attrs),
               {:ok, session} <-
                 Identity.issue_session(context, actor.id, provider: "trusted_local_dev") do
            {:ok, Map.delete(session, :token), actor, session.token}
          end
      end

    case result do
      {:ok, session, actor, token} ->
        contexts = Map.put(state.local_dev_contexts, key, %{session_token: token})
        {:reply, {:ok, session, actor}, %{state | local_dev_contexts: contexts}}

      {:ok, session, actor} ->
        {:reply, {:ok, session, actor}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp authenticate_limited(keys, authenticate) do
    case GenServer.call(__MODULE__, {:begin_login, keys}) do
      :allowed ->
        result = authenticate.()
        :ok = GenServer.call(__MODULE__, {:finish_login, keys, result_tag(result)})
        result

      :blocked ->
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
    if Map.keys(opts) -- [:remote_identity, "remote_identity"] == [] do
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

  defp cached_local_dev_context(nil, _context, _roles), do: {:error, :not_cached}

  defp cached_local_dev_context(%{session_token: token}, context, roles) do
    with {:ok, session, actor} <- Identity.introspect_session(context, token),
         true <- actor.id == @local_dev_actor_id and Enum.all?(roles, &(&1 in actor.roles)) do
      {:ok, session, actor}
    else
      false -> {:error, :local_dev_context_changed}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_local_dev_actor(context, attrs) do
    case Identity.get_actor(context, @local_dev_actor_id) do
      {:ok, actor} when actor.username == attrs.username and actor.roles == attrs.roles ->
        {:ok, actor}

      {:ok, actor} when actor.username == attrs.username ->
        Identity.set_roles(context, actor.id, attrs.roles, actor.access_version)

      {:ok, _actor} ->
        {:error, :reserved_local_dev_actor}

      {:error, :actor_not_found} ->
        create_or_join_local_dev_actor(context, attrs)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_or_join_local_dev_actor(context, attrs) do
    password = Session.raw_token() <> Session.raw_token()

    case Identity.create_actor(
           context,
           attrs.username,
           password,
           attrs.display_name,
           attrs.roles,
           actor_id: @local_dev_actor_id,
           command_id: "identity-local-dev:create"
         ) do
      {:ok, actor} ->
        {:ok, actor}

      {:error, :username_taken} ->
        Identity.set_membership(context, @local_dev_actor_id, attrs.roles, 0)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp login_failure_limit,
    do: positive_config(:auth_login_failure_limit, @default_login_failure_limit)

  defp login_backoff_seconds,
    do: positive_config(:auth_login_backoff_seconds, @default_login_backoff_seconds)

  defp positive_config(key, default) do
    case Application.get_env(:favn_orchestrator, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end
end
