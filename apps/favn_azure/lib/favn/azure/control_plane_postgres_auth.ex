defmodule Favn.Azure.ControlPlanePostgresAuth do
  @moduledoc """
  Supplies fresh-enough managed-identity passwords for control-plane PostgreSQL connections.

  The lifecycle is independent from runner credential caches. Connection callers
  retain only the registered server name; identity configuration remains inside
  the redacted provider process.
  """

  alias Favn.Azure.ControlPlanePostgresAuth.Server
  alias Favn.Azure.ControlPlanePostgresAuth.Supervisor, as: AuthSupervisor
  alias Favn.Azure.{PostgresAuthenticationError, PostgresEntraToken, Token, TokenError}

  @supervisor AuthSupervisor
  @server Server
  @task_supervisor Favn.Azure.ControlPlanePostgresAuth.TaskSupervisor
  @cache Favn.Azure.ControlPlanePostgresAuth.Cache
  @minimum_validity_seconds 300
  @fetch_timeout 10_000
  @max_fetch_timeout 60_000
  @start_event [:favn, :azure, :postgres_auth, :start]
  @stop_event [:favn, :azure, :postgres_auth, :stop]

  @allowed_options [
    :client_id,
    :endpoint,
    :credential_provider,
    :provider_options,
    :minimum_validity_seconds,
    :fetch_timeout,
    :refresh_before_seconds,
    :max_entries,
    :max_inflight,
    :max_waiters_per_key,
    :supervisor_name,
    :server_name,
    :task_supervisor,
    :cache_name,
    :clock
  ]

  @doc false
  @spec applications(keyword()) :: {:ok, [atom()]}
  def applications(_options), do: {:ok, [:favn_azure]}

  @doc false
  @spec child_specs(keyword()) :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  def child_specs(options) do
    with {:ok, options} <- normalize_options(options) do
      {:ok,
       [
         Supervisor.child_spec(
           {AuthSupervisor, options},
           id: Keyword.fetch!(options, :supervisor_name)
         )
       ]}
    end
  end

  @doc false
  @spec connection_reference(keyword()) :: {:ok, keyword()} | {:error, term()}
  def connection_reference(options) do
    with {:ok, options} <- normalize_options(options) do
      {:ok,
       [
         server: Keyword.fetch!(options, :server_name),
         timeout: Keyword.fetch!(options, :fetch_timeout) + 1_000
       ]}
    end
  end

  @doc false
  @spec connection_password(keyword()) ::
          {:ok, String.t()} | {:error, PostgresAuthenticationError.t()}
  def connection_password(reference) do
    started_at = System.monotonic_time()

    with {:ok, server, timeout} <- normalize_reference(reference),
         {:ok, config} <- credential_config(server, timeout),
         :ok <- emit_start(config),
         {:ok, token} <- fetch_token(config),
         true <- Token.valid_for?(token, config.minimum_validity_seconds, config.clock.()) do
      GenServer.cast(server, {:password_delivery, :ok})
      emit_stop(started_at, :ok, false, config.user_assigned?)
      {:ok, token.access_token}
    else
      false ->
        error = authentication_error(:insufficient_validity, true)
        record_failure(reference, error.class)
        emit_stop(started_at, error.class, error.retryable?, user_assigned?(reference))
        {:error, error}

      {:error, %PostgresAuthenticationError{} = error} ->
        record_failure(reference, error.class)
        emit_stop(started_at, error.class, error.retryable?, user_assigned?(reference))
        {:error, error}

      {:error, %TokenError{} = error} ->
        mapped = map_token_error(error)
        record_failure(reference, mapped.class)
        emit_stop(started_at, mapped.class, mapped.retryable?, user_assigned?(reference))
        {:error, mapped}
    end
  end

  @doc false
  @spec status(keyword()) :: map()
  def status(options) do
    with {:ok, options} <- normalize_options(options),
         server <- Keyword.fetch!(options, :server_name),
         true <- alive?(server),
         status when is_map(status) <- GenServer.call(server, :status, 1_000) do
      status
    else
      _unavailable -> %{lifecycle_ready?: false}
    end
  catch
    :exit, _reason -> %{lifecycle_ready?: false}
  end

  defp normalize_options(options) when is_list(options) do
    with true <- Keyword.keyword?(options),
         [] <- Keyword.keys(options) -- @allowed_options,
         {:ok, client_id} <- client_id(Keyword.get(options, :client_id)),
         {:ok, endpoint} <- endpoint(Keyword.get(options, :endpoint, :auto)),
         {:ok, credential_provider} <-
           credential_provider(Keyword.get(options, :credential_provider, "managed_identity")),
         {:ok, provider_options} <- provider_options(Keyword.get(options, :provider_options, [])),
         {:ok, minimum_validity_seconds} <-
           bounded_integer(
             options,
             :minimum_validity_seconds,
             @minimum_validity_seconds,
             1,
             3_600
           ),
         {:ok, fetch_timeout} <-
           bounded_integer(options, :fetch_timeout, @fetch_timeout, 1, @max_fetch_timeout),
         {:ok, clock} <- clock(Keyword.get(options, :clock, &DateTime.utc_now/0)) do
      {:ok,
       options
       |> Keyword.put(:client_id, client_id)
       |> Keyword.put(:endpoint, endpoint)
       |> Keyword.put(:credential_provider, credential_provider)
       |> Keyword.put(:provider_options, provider_options)
       |> Keyword.put(:minimum_validity_seconds, minimum_validity_seconds)
       |> Keyword.put(:refresh_before_seconds, minimum_validity_seconds)
       |> Keyword.put(:fetch_timeout, fetch_timeout)
       |> Keyword.put(:supervisor_name, Keyword.get(options, :supervisor_name, @supervisor))
       |> Keyword.put(:server_name, Keyword.get(options, :server_name, @server))
       |> Keyword.put(:task_supervisor, Keyword.get(options, :task_supervisor, @task_supervisor))
       |> Keyword.put(:cache_name, Keyword.get(options, :cache_name, @cache))
       |> Keyword.put(:clock, clock)}
    else
      _invalid -> {:error, :invalid_control_plane_postgres_auth_options}
    end
  end

  defp normalize_options(_options), do: {:error, :invalid_control_plane_postgres_auth_options}

  defp normalize_reference(reference) when is_list(reference) do
    case {Keyword.get(reference, :server), Keyword.get(reference, :timeout)} do
      {server, timeout}
      when (is_atom(server) or is_pid(server)) and is_integer(timeout) and timeout > 0 ->
        {:ok, server, timeout}

      _invalid ->
        {:error, authentication_error(:invalid_config, false)}
    end
  end

  defp normalize_reference(_reference),
    do: {:error, authentication_error(:invalid_config, false)}

  defp credential_config(server, timeout) do
    if alive?(server) do
      GenServer.call(server, :credential_config, timeout)
    else
      {:error, authentication_error(:provider_unavailable, true)}
    end
  catch
    :exit, _reason -> {:error, authentication_error(:provider_unavailable, true)}
  end

  defp fetch_token(config) do
    options =
      [cache: config.cache_name, timeout: config.fetch_timeout] ++ config.provider_options

    PostgresEntraToken.fetch_token(config.auth, options)
  end

  defp map_token_error(%TokenError{type: :invalid_config}),
    do: authentication_error(:invalid_config, false)

  defp map_token_error(%TokenError{type: :authentication_error}),
    do: authentication_error(:identity_rejected, false)

  defp map_token_error(%TokenError{type: :connection_error, details: details}) do
    if Map.get(details, :reason) in [:provider_timeout, :call_timeout, :timeout],
      do: authentication_error(:token_timeout, true),
      else: authentication_error(:identity_unavailable, true)
  end

  defp map_token_error(%TokenError{retryable?: retryable?}),
    do: authentication_error(:provider_unavailable, retryable?)

  defp authentication_error(class, retryable?) do
    %PostgresAuthenticationError{
      class: class,
      message: "Azure PostgreSQL managed-identity authentication failed",
      retryable?: retryable?
    }
  end

  defp record_failure(reference, class) do
    case normalize_reference(reference) do
      {:ok, server, _timeout} when is_pid(server) ->
        if Process.alive?(server),
          do: GenServer.cast(server, {:password_delivery, {:error, class}})

      {:ok, server, _timeout} ->
        if Process.whereis(server),
          do: GenServer.cast(server, {:password_delivery, {:error, class}})

      _invalid ->
        :ok
    end
  end

  defp emit_start(config) do
    :telemetry.execute(@start_event, %{system_time: System.system_time()}, %{
      mode: :azure_managed_identity,
      user_assigned?: config.user_assigned?
    })

    :ok
  end

  defp emit_stop(started_at, class, retryable?, user_assigned?) do
    :telemetry.execute(
      @stop_event,
      %{duration: System.monotonic_time() - started_at},
      %{
        mode: :azure_managed_identity,
        outcome: class,
        retryable?: retryable?,
        user_assigned?: user_assigned?
      }
    )
  end

  defp user_assigned?(reference) do
    with {:ok, server, timeout} <- normalize_reference(reference),
         {:ok, config} <- credential_config(server, min(timeout, 1_000)) do
      config.user_assigned?
    else
      _unavailable -> false
    end
  end

  defp alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp alive?(name), do: not is_nil(Process.whereis(name))

  defp client_id(nil), do: {:ok, nil}

  defp client_id(value)
       when is_binary(value) and value != "" and byte_size(value) <= 1_024,
       do: {:ok, value}

  defp client_id(_invalid), do: :error

  defp endpoint(value) when value in [:auto, :imds, :azure_app_service], do: {:ok, value}
  defp endpoint(_invalid), do: :error

  defp credential_provider("managed_identity"), do: {:ok, "managed_identity"}

  defp credential_provider(provider) when is_atom(provider) and not is_nil(provider),
    do: {:ok, provider}

  defp credential_provider(_invalid), do: :error

  defp provider_options(options) when is_list(options) do
    if Keyword.keyword?(options) and :erlang.external_size(options) <= 65_536,
      do: {:ok, options},
      else: :error
  rescue
    _error -> :error
  end

  defp provider_options(_invalid), do: :error

  defp bounded_integer(options, key, default, minimum, maximum) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value >= minimum and value <= maximum -> {:ok, value}
      _invalid -> :error
    end
  end

  defp clock(value) when is_function(value, 0), do: {:ok, value}
  defp clock(_invalid), do: :error
end
