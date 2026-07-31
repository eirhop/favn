defmodule FavnStoragePostgres.Config do
  @moduledoc """
  Validates and normalizes the Favn-owned PostgreSQL connection configuration.

  Returned diagnostics contain no URL, password, certificate contents, or other
  credential material.
  """

  @default_statement_timeout 15_000
  @default_lock_timeout 3_000
  @default_idle_transaction_timeout 15_000
  @max_production_pool_size 200
  @max_production_timeout_ms 120_000

  alias Favn.DeploymentMode
  alias FavnStoragePostgres.Authentication
  alias FavnStoragePostgres.ConnectionConfig

  @doc "Returns validated repo options from application configuration and overrides."
  @spec repo_options(keyword()) :: {:ok, keyword()} | {:error, term()}
  def repo_options(overrides \\ []) when is_list(overrides) do
    with {:ok, config} <- connection_config(overrides) do
      {:ok, config.repo_options}
    end
  end

  @doc false
  @spec connection_config(keyword()) :: {:ok, ConnectionConfig.t()} | {:error, term()}
  def connection_config(overrides \\ []) when is_list(overrides) do
    configured = Application.get_env(:favn_storage_postgres, FavnStoragePostgres.Repo, [])
    options = Keyword.merge(configured, overrides)

    with {:ok, authentication} <- authentication(options),
         :ok <- Authentication.validate(authentication),
         {:ok, endpoint_options, hostname} <- endpoint_options(options, authentication),
         :ok <- reject_sensitive_connection_errors(options),
         {:ok, pool_size} <- positive_integer(options, :pool_size, 15),
         {:ok, queue_target} <- positive_integer(options, :queue_target, 50),
         {:ok, queue_interval} <- positive_integer(options, :queue_interval, 1_000),
         {:ok, timeout} <- positive_integer(options, :timeout, 15_000),
         {:ok, instance_id} <- instance_id(options),
         {:ok, ssl} <- ssl_options(options, hostname),
         base_options <-
           options
           |> Keyword.drop(internal_option_keys())
           |> Keyword.merge(endpoint_options)
           |> Keyword.put(:pool_size, pool_size)
           |> Keyword.put(:queue_target, queue_target)
           |> Keyword.put(:queue_interval, queue_interval)
           |> Keyword.put(:timeout, timeout)
           |> Keyword.put(:ssl, ssl)
           |> Keyword.put(:show_sensitive_data_on_connection_error, false)
           |> Keyword.put(:migration_default_prefix, "favn_control")
           |> Keyword.put(:after_connect, {__MODULE__, :after_connect, [instance_id]}),
         {:ok, repo_options} <- Authentication.install(base_options, authentication) do
      {:ok,
       %ConnectionConfig{
         authentication: authentication,
         repo_options: repo_options,
         notification_options: notification_options(repo_options)
       }}
    end
  end

  @doc "Returns validated repo options parsed from an explicit process environment map."
  @spec repo_options_from_env(map()) :: {:ok, keyword()} | {:error, term()}
  def repo_options_from_env(env \\ System.get_env()) when is_map(env) do
    with {:ok, config} <- connection_config_from_env(env) do
      {:ok, config.repo_options}
    end
  end

  @doc false
  @spec connection_config_from_env(map()) :: {:ok, ConnectionConfig.t()} | {:error, term()}
  def connection_config_from_env(env \\ System.get_env()) when is_map(env) do
    with {:ok, authentication_options} <- authentication_options_from_env(env),
         {:ok, _deployment_mode} <- DeploymentMode.from_env(env),
         {:ok, ssl_options} <- env_ssl_options(env),
         {:ok, pool_size} <-
           env_bounded_integer(env, "FAVN_DATABASE_POOL_SIZE", 15, @max_production_pool_size),
         {:ok, queue_target} <-
           env_bounded_integer(
             env,
             "FAVN_DATABASE_QUEUE_TARGET_MS",
             50,
             @max_production_timeout_ms
           ),
         {:ok, queue_interval} <-
           env_bounded_integer(
             env,
             "FAVN_DATABASE_QUEUE_INTERVAL_MS",
             1_000,
             @max_production_timeout_ms
           ),
         {:ok, timeout} <-
           env_bounded_integer(
             env,
             "FAVN_DATABASE_TIMEOUT_MS",
             15_000,
             @max_production_timeout_ms
           ) do
      connection_config(
        authentication_options ++
          [
            pool_size: pool_size,
            queue_target: queue_target,
            queue_interval: queue_interval,
            timeout: timeout,
            instance_id: Map.get(env, "FAVN_INSTANCE_ID", "release-task")
          ] ++ ssl_options
      )
    end
  end

  @doc "Applies bounded, UTC PostgreSQL session defaults to a new connection."
  @spec after_connect(pid(), String.t()) :: :ok
  def after_connect(connection, instance_id) when is_binary(instance_id) do
    settings = [
      {"application_name", application_name(instance_id)},
      {"statement_timeout", Integer.to_string(@default_statement_timeout)},
      {"lock_timeout", Integer.to_string(@default_lock_timeout)},
      {"idle_in_transaction_session_timeout",
       Integer.to_string(@default_idle_transaction_timeout)},
      {"timezone", "UTC"}
    ]

    Postgrex.query!(connection, "SET search_path TO pg_catalog, favn_control", [])

    Enum.each(settings, fn {name, value} ->
      Postgrex.query!(connection, "SELECT pg_catalog.set_config($1, $2, false)", [name, value])
    end)

    :ok
  end

  @doc "Returns redacted pool and TLS configuration for diagnostics."
  @spec redacted(keyword()) :: map()
  def redacted(options) do
    %{
      configured?:
        Keyword.has_key?(options, :url) or
          (Keyword.has_key?(options, :hostname) and Keyword.has_key?(options, :database)),
      authentication_mode:
        if(Keyword.has_key?(options, :configure),
          do: :azure_managed_identity,
          else: :password
        ),
      pool_size: Keyword.get(options, :pool_size),
      queue_target_ms: Keyword.get(options, :queue_target),
      queue_interval_ms: Keyword.get(options, :queue_interval),
      timeout_ms: Keyword.get(options, :timeout),
      tls?: Keyword.get(options, :ssl, false) != false
    }
  end

  defp fetch_url(options) do
    case Keyword.get(options, :url) do
      url when is_binary(url) and url != "" -> {:ok, url}
      _value -> {:error, :database_url_required}
    end
  end

  defp validate_url(url) do
    uri = URI.parse(url)

    cond do
      is_binary(uri.query) ->
        {:error, :database_url_query_parameters_not_allowed}

      valid_postgres_url?(uri) ->
        :ok

      true ->
        {:error, :invalid_database_url}
    end
  end

  defp valid_postgres_url?(%URI{
         scheme: scheme,
         host: host,
         path: "/" <> database,
         query: nil,
         fragment: nil,
         port: port
       }) do
    scheme in ["ecto", "postgres", "postgresql"] and is_binary(host) and host != "" and
      database != "" and not String.contains?(database, "/") and
      (is_nil(port) or port in 1..65_535)
  end

  defp valid_postgres_url?(_uri), do: false

  defp validate_deployment_url(_url, _options), do: :ok

  defp authentication(options) do
    case Keyword.get(options, :authentication, :password) do
      :password = authentication ->
        {:ok, authentication}

      {:dynamic, provider, provider_options} = authentication
      when is_atom(provider) and is_list(provider_options) ->
        {:ok, authentication}

      _invalid ->
        {:error, :invalid_database_authentication}
    end
  end

  defp endpoint_options(options, :password) do
    with {:ok, url} <- fetch_url(options),
         :ok <- validate_url(url),
         :ok <- validate_deployment_url(url, options) do
      {:ok, [url: url], URI.parse(url).host}
    end
  end

  defp endpoint_options(options, {:dynamic, _provider, _provider_options}) do
    with :ok <- reject_database_url(options),
         {:ok, hostname} <- database_component(options, :hostname, 253),
         {:ok, database} <- database_component(options, :database, 255),
         {:ok, username} <- database_component(options, :username, 255),
         {:ok, port} <- database_port(options) do
      {:ok,
       [
         hostname: hostname,
         port: port,
         database: database,
         username: username,
         password: nil
       ], hostname}
    end
  end

  defp reject_database_url(options) do
    if Keyword.has_key?(options, :url),
      do: {:error, :database_url_not_allowed_with_dynamic_authentication},
      else: :ok
  end

  defp database_component(options, key, maximum_bytes) do
    case Keyword.get(options, key) do
      value
      when is_binary(value) and value != "" and byte_size(value) <= maximum_bytes ->
        {:ok, value}

      _invalid ->
        {:error, {:invalid_database_option, key}}
    end
  end

  defp database_port(options) do
    case Keyword.get(options, :port, 5432) do
      port when is_integer(port) and port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, {:invalid_database_option, :port}}
    end
  end

  defp reject_sensitive_connection_errors(options) do
    if Keyword.get(options, :show_sensitive_data_on_connection_error, false) == true,
      do: {:error, :sensitive_database_connection_errors_not_allowed},
      else: :ok
  end

  defp positive_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> {:error, {:invalid_database_option, key}}
    end
  end

  defp required_env(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, :database_url_required}
    end
  end

  defp authentication_options_from_env(env) do
    case Map.get(env, "FAVN_DATABASE_AUTH_MODE", "password") do
      "password" ->
        with {:ok, url} <- required_env(env, "FAVN_DATABASE_URL"),
             :ok <- reject_azure_database_env(env) do
          {:ok, [authentication: :password, url: url]}
        end

      "azure_managed_identity" ->
        with :ok <- reject_password_database_env(env),
             {:ok, hostname} <- required_database_env(env, "FAVN_DATABASE_HOST"),
             {:ok, database} <- required_database_env(env, "FAVN_DATABASE_NAME"),
             {:ok, username} <- required_database_env(env, "FAVN_DATABASE_USERNAME"),
             {:ok, port} <- env_bounded_integer(env, "FAVN_DATABASE_PORT", 5432, 65_535),
             {:ok, client_id} <-
               optional_managed_identity_client_id(env) do
          provider = Module.concat([Favn, Azure, ControlPlanePostgresAuth])

          {:ok,
           [
             authentication: {:dynamic, provider, maybe_put([], :client_id, client_id)},
             hostname: hostname,
             port: port,
             database: database,
             username: username
           ]}
        end

      _invalid ->
        {:error, {:invalid_database_env, "FAVN_DATABASE_AUTH_MODE"}}
    end
  end

  defp reject_azure_database_env(env) do
    names = [
      "FAVN_DATABASE_HOST",
      "FAVN_DATABASE_PORT",
      "FAVN_DATABASE_NAME",
      "FAVN_DATABASE_USERNAME",
      "FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID"
    ]

    reject_present_env(env, names, :azure_database_options_not_allowed_with_password)
  end

  defp reject_password_database_env(env) do
    reject_present_env(
      env,
      ["FAVN_DATABASE_URL"],
      :database_url_not_allowed_with_dynamic_authentication
    )
  end

  defp reject_present_env(env, names, reason) do
    if Enum.any?(names, &Map.has_key?(env, &1)), do: {:error, reason}, else: :ok
  end

  defp required_database_env(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:invalid_database_env, name}}
    end
  end

  defp optional_managed_identity_client_id(env) do
    name = "FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID"

    case Map.get(env, name) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        if Regex.match?(
             ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/,
             value
           ),
           do: {:ok, value},
           else: {:error, {:invalid_database_env, name}}

      _invalid ->
        {:error, {:invalid_database_env, name}}
    end
  end

  defp env_ssl_options(env) do
    default = if production?(), do: nil, else: "disable"

    case Map.get(env, "FAVN_DATABASE_SSL_MODE", default) do
      mode when mode in ["verify-full", "verify_full"] ->
        {:ok,
         [ssl_mode: :verify_full]
         |> maybe_put(:ssl_ca_file, Map.get(env, "FAVN_DATABASE_SSL_CA_FILE"))}

      "disable" ->
        if not production?(),
          do: {:ok, [ssl_mode: :disable]},
          else: {:error, :production_tls_required}

      nil ->
        {:error, :database_ssl_mode_required}

      _invalid ->
        {:error, :invalid_ssl_mode}
    end
  end

  defp env_bounded_integer(env, name, default, maximum) do
    case Map.get(env, name) do
      nil ->
        {:ok, default}

      value when is_binary(value) ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= 1 and parsed <= maximum -> {:ok, parsed}
          _invalid -> {:error, {:invalid_database_env, name}}
        end

      _invalid ->
        {:error, {:invalid_database_env, name}}
    end
  end

  defp maybe_put(options, _key, nil), do: options
  defp maybe_put(options, key, value), do: Keyword.put(options, key, value)

  defp ssl_options(options, hostname) do
    mode = Keyword.get(options, :ssl_mode, default_ssl_mode())

    case mode do
      :disable ->
        if production?(),
          do: {:error, :production_tls_required},
          else: {:ok, false}

      :verify_full ->
        verified_tls_options(options, hostname)

      _mode ->
        {:error, :invalid_ssl_mode}
    end
  end

  defp verified_tls_options(options, hostname) do
    ca_file = Keyword.get(options, :ssl_ca_file)

    with true <- is_binary(hostname) and hostname != "",
         {:ok, trust_options} <- trust_options(ca_file) do
      {:ok,
       [
         verify: :verify_peer,
         server_name_indication: String.to_charlist(hostname),
         customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
       ] ++ trust_options}
    else
      _invalid -> {:error, :database_tls_trust_required}
    end
  end

  defp trust_options(nil) do
    case :public_key.cacerts_get() do
      certificates when is_list(certificates) and certificates != [] ->
        {:ok, [cacerts: certificates]}

      _missing ->
        {:error, :database_tls_trust_required}
    end
  end

  defp trust_options(ca_file) when is_binary(ca_file) do
    if Path.type(ca_file) == :absolute and File.regular?(ca_file),
      do: {:ok, [cacertfile: ca_file]},
      else: {:error, :database_tls_trust_required}
  end

  defp trust_options(_ca_file), do: {:error, :database_tls_trust_required}

  defp instance_id(options) do
    case Keyword.get(options, :instance_id, "local") do
      instance_id when is_binary(instance_id) and byte_size(instance_id) in 1..160 ->
        {:ok, instance_id}

      _invalid ->
        {:error, {:invalid_database_option, :instance_id}}
    end
  end

  defp application_name(instance_id),
    do: "favn_orchestrator:#{String.slice(instance_id, 0, 64)}"

  defp notification_options(repo_options) do
    endpoint_options =
      case Keyword.fetch(repo_options, :url) do
        {:ok, url} -> Ecto.Repo.Supervisor.parse_url(url)
        :error -> Keyword.take(repo_options, [:hostname, :port, :database, :username, :password])
      end

    repo_options
    |> Keyword.take([:ssl, :socket_options, :connect_timeout, :timeout, :configure])
    |> Keyword.merge(endpoint_options)
    |> Keyword.put(:show_sensitive_data_on_connection_error, false)
    |> Keyword.put(:auto_reconnect, true)
    |> Keyword.put(:sync_connect, true)
  end

  defp internal_option_keys do
    [
      :authentication,
      :url,
      :hostname,
      :port,
      :database,
      :username,
      :password,
      :ssl_mode,
      :ssl_ca_file,
      :allow_insecure_database?,
      :deployment_mode,
      :instance_id
    ]
  end

  defp default_ssl_mode, do: if(production?(), do: :verify_full, else: :disable)
  defp production?, do: Application.get_env(:favn_storage_postgres, :environment) == :prod
end
