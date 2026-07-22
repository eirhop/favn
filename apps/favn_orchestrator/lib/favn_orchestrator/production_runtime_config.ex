defmodule FavnOrchestrator.ProductionRuntimeConfig do
  @moduledoc """
  Production runtime configuration for the orchestrator process.

  PostgreSQL is the only production persistence backend. This module validates
  the deployment-facing environment contract and freezes the backend composition
  before supervised runtime components start.
  """

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.API.ManifestPublication.Config, as: ManifestPublicationConfig
  alias Favn.RuntimeInput.KeyringConfig
  alias Favn.DeploymentMode

  @max_session_ttl_seconds 30 * 24 * 60 * 60
  @max_postgres_pool_size 200
  @max_postgres_timeout_ms 120_000
  @max_scheduler_tick_ms 24 * 60 * 60 * 1_000
  @max_missed_occurrences 100_000
  @default_active_run_plan_max_bytes 512 * 1_024 * 1_024
  @min_active_run_plan_max_bytes 64 * 1_024 * 1_024
  @max_active_run_plan_max_bytes 8 * 1_024 * 1_024 * 1_024
  @max_runner_rpc_timeout_ms 120_000
  @max_runner_diagnostics_timeout_ms 30_000
  @max_runner_await_buffer_ms 120_000
  @max_shutdown_drain_timeout_ms 3_600_000
  @max_workspace_count 1_000
  @max_workspace_env_bytes 65_536
  @min_http_body_limit_bytes 64 * 1_024
  @max_http_body_limit_bytes 8 * 1_024 * 1_024
  @max_http_connections 100_000
  @max_http_request_timeout_ms 120_000
  @max_http_idle_timeout_ms 300_000
  @runner_module Module.concat(["FavnRunner"])

  @type runtime_input_pin_config :: %{
          keys: %{pos_integer() => binary()},
          current_version: pos_integer()
        }

  @type config :: %{
          deployment_mode: DeploymentMode.t(),
          instance_id: String.t(),
          postgres: keyword(),
          runtime_input_pin: runtime_input_pin_config(),
          api_server: keyword(),
          http_server: map(),
          manifest_publication: keyword(),
          api_service_tokens: [ServiceTokens.token_config()],
          workspace_ids: [String.t()],
          auth_bootstrap: keyword(),
          auth_session_ttl_seconds: pos_integer(),
          active_run_plan_max_bytes: pos_integer(),
          scheduler: keyword(),
          shutdown_drain_timeout_ms: pos_integer(),
          runner: map(),
          runner_client: module(),
          runner_client_opts: keyword()
        }

  @postgres_backend Module.concat([FavnStoragePostgres, Backend])

  @doc "Applies production env config only for the production control-plane profile."
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, term()}
  def apply_from_env_if_configured(env) when is_map(env) do
    if Application.get_env(:favn_orchestrator, :local_dev_mode, false) do
      :ok
    else
      if Application.get_env(:favn_orchestrator, :production_runtime_config, false) do
        apply_from_env(env)
      else
        :ok
      end
    end
  end

  @doc """
  Validates and applies production env config.
  """
  @spec apply_from_env(map()) :: :ok | {:error, term()}
  def apply_from_env(env) when is_map(env) do
    with {:ok, config} <- validate(env) do
      apply(config)
    end
  end

  @doc "Applies one already validated production configuration exactly once at boot."
  @spec apply(config()) :: :ok
  def apply(config) when is_map(config) do
    postgres = Keyword.put(config.postgres, :instance_id, config.instance_id)

    Application.put_env(:favn_orchestrator, :persistence_backend, @postgres_backend)
    Application.put_env(:favn_orchestrator, :persistence_options, postgres)
    Application.put_env(:favn_orchestrator, :instance_id, config.instance_id)
    Application.put_env(:favn_orchestrator, :http_server, config.http_server)

    Application.put_env(
      :favn_orchestrator,
      :shutdown_drain_timeout_ms,
      config.shutdown_drain_timeout_ms
    )

    Application.put_env(
      :favn_storage_postgres,
      Module.concat(["FavnStoragePostgres.Repo"]),
      postgres
    )

    Application.put_env(
      :favn_storage_postgres,
      :runtime_input_pin_keys,
      config.runtime_input_pin.keys
    )

    Application.put_env(
      :favn_storage_postgres,
      :runtime_input_pin_current_key_version,
      config.runtime_input_pin.current_version
    )

    Application.put_env(:favn_orchestrator, :api_server, config.api_server)

    Application.put_env(
      :favn_orchestrator,
      :manifest_publication,
      config.manifest_publication
    )

    Application.put_env(:favn_orchestrator, :api_service_tokens, config.api_service_tokens)
    Application.put_env(:favn_orchestrator, :workspace_ids, config.workspace_ids)
    Application.delete_env(:favn_orchestrator, :api_service_tokens_env)

    Application.put_env(
      :favn_orchestrator,
      :auth_bootstrap_username,
      Keyword.fetch!(config.auth_bootstrap, :username)
    )

    Application.put_env(
      :favn_orchestrator,
      :auth_bootstrap_password,
      Keyword.fetch!(config.auth_bootstrap, :password)
    )

    Application.put_env(
      :favn_orchestrator,
      :auth_bootstrap_display_name,
      Keyword.fetch!(config.auth_bootstrap, :display_name)
    )

    Application.put_env(
      :favn_orchestrator,
      :auth_bootstrap_roles,
      Keyword.fetch!(config.auth_bootstrap, :roles)
    )

    Application.put_env(
      :favn_orchestrator,
      :local_dev_mode,
      config.deployment_mode == :local_development
    )

    Application.put_env(
      :favn_orchestrator,
      :auth_session_ttl_seconds,
      config.auth_session_ttl_seconds
    )

    Application.put_env(
      :favn_orchestrator,
      :active_run_plan_max_bytes,
      config.active_run_plan_max_bytes
    )

    Application.put_env(:favn_orchestrator, :scheduler, config.scheduler)
    Application.put_env(:favn_orchestrator, :runner_client, config.runner_client)

    Application.put_env(
      :favn_orchestrator,
      :runner_client_opts,
      install_runner_node_atom(config.runner_client_opts)
    )

    Application.put_env(
      :favn_orchestrator,
      :production_runtime_diagnostics,
      diagnostics(config)
    )

    :ok
  end

  @doc """
  Validates production runtime env values without mutating application env.
  """
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env) when is_map(env) do
    with {:ok, deployment_mode} <- DeploymentMode.from_env(env),
         {:ok, {runner, runner_client_opts}} <- runner(env),
         {:ok, instance_id} <- instance_id(env, runner.control_plane_node),
         {:ok, {postgres, runtime_input_pin}} <- postgres(env, deployment_mode),
         {:ok, api_server} <- api_server(env),
         {:ok, http_server} <- http_server(env),
         {:ok, manifest_publication} <- manifest_publication(env),
         {:ok, tokens} <- api_service_tokens(env),
         {:ok, workspace_ids} <- workspace_ids(env),
         {:ok, auth_bootstrap} <- auth_bootstrap(env),
         {:ok, auth_session_ttl_seconds} <- auth_session_ttl_seconds(env),
         {:ok, active_run_plan_max_bytes} <- active_run_plan_max_bytes(env),
         {:ok, scheduler} <- scheduler(env, workspace_ids),
         {:ok, shutdown_drain_timeout_ms} <- shutdown_drain_timeout_ms(env) do
      {:ok,
       %{
         deployment_mode: deployment_mode,
         instance_id: instance_id,
         postgres: postgres,
         runtime_input_pin: runtime_input_pin,
         api_server: api_server,
         http_server: http_server,
         manifest_publication: ManifestPublicationConfig.to_keyword(manifest_publication),
         api_service_tokens: tokens,
         workspace_ids: workspace_ids,
         auth_bootstrap: auth_bootstrap,
         auth_session_ttl_seconds: auth_session_ttl_seconds,
         active_run_plan_max_bytes: active_run_plan_max_bytes,
         scheduler: scheduler,
         shutdown_drain_timeout_ms: shutdown_drain_timeout_ms,
         runner: runner,
         runner_client: FavnOrchestrator.RunnerClient.BeamNode,
         runner_client_opts: runner_client_opts
       }}
    else
      {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
    end
  end

  @doc """
  Returns redacted diagnostics for a validated config.
  """
  @spec diagnostics(config()) :: map()
  def diagnostics(config) when is_map(config) do
    %{
      status: :ok,
      deployment_mode: config.deployment_mode,
      instance: %{configured?: true},
      storage: %{backend: :postgres, database: %{configured?: true, url: :redacted}},
      postgres: %{
        ssl_mode: Keyword.fetch!(config.postgres, :ssl_mode),
        pool_size: Keyword.fetch!(config.postgres, :pool_size),
        timeout_ms: Keyword.fetch!(config.postgres, :timeout),
        trust_store: :redacted
      },
      api_server: %{
        enabled: Keyword.fetch!(config.api_server, :enabled),
        host: Keyword.fetch!(config.api_server, :host),
        port: Keyword.fetch!(config.api_server, :port)
      },
      http_server: config.http_server,
      manifest_publication: Map.new(config.manifest_publication),
      api_service_tokens: %{
        count: length(config.api_service_tokens),
        ids: config.api_service_tokens |> Enum.map(& &1.service_identity) |> Enum.sort(),
        redacted: true
      },
      workspaces: %{configured_count: length(config.workspace_ids)},
      auth_bootstrap: %{username_configured?: true, password_configured?: true, redacted: true},
      auth_session: %{ttl_seconds: config.auth_session_ttl_seconds},
      active_run_plan: %{max_bytes: config.active_run_plan_max_bytes},
      runtime_input_pin: %{
        current_version: config.runtime_input_pin.current_version,
        retained_versions: config.runtime_input_pin.keys |> Map.keys() |> Enum.sort()
      },
      scheduler: Map.new(config.scheduler),
      shutdown: %{drain_timeout_ms: config.shutdown_drain_timeout_ms},
      runner: config.runner
    }
  end

  @spec postgres_backend() :: module()
  def postgres_backend, do: @postgres_backend

  defp postgres(env, deployment_mode) do
    with {:ok, url} <- required_secret(env, "FAVN_DATABASE_URL"),
         :ok <- postgres_url("FAVN_DATABASE_URL", url),
         :ok <- deployment_database_url(url, deployment_mode),
         {:ok, tls_options} <- postgres_tls(env, deployment_mode),
         {:ok, pool_size} <- int(env, "FAVN_DATABASE_POOL_SIZE", "15", 1, @max_postgres_pool_size),
         {:ok, queue_target} <-
           int(env, "FAVN_DATABASE_QUEUE_TARGET_MS", "50", 1, @max_postgres_timeout_ms),
         {:ok, queue_interval} <-
           int(env, "FAVN_DATABASE_QUEUE_INTERVAL_MS", "1000", 1, @max_postgres_timeout_ms),
         {:ok, timeout} <-
           int(env, "FAVN_DATABASE_TIMEOUT_MS", "15000", 1, @max_postgres_timeout_ms),
         {:ok, runtime_input_pin} <- KeyringConfig.from_env(env) do
      {:ok,
       {
         [
           url: url,
           pool_size: pool_size,
           queue_target: queue_target,
           queue_interval: queue_interval,
           timeout: timeout
         ] ++ tls_options,
         runtime_input_pin
       }}
    end
  end

  defp postgres_tls(env, :production) do
    case fetch(env, "FAVN_DATABASE_SSL_MODE") do
      :error ->
        {:error, {:missing_env, "FAVN_DATABASE_SSL_MODE"}}

      {:ok, mode} when mode in ["verify-full", "verify_full"] ->
        case fetch(env, "FAVN_DATABASE_SSL_CA_FILE") do
          {:ok, ca_file} ->
            with :ok <- absolute_regular_file("FAVN_DATABASE_SSL_CA_FILE", ca_file) do
              {:ok, [ssl_mode: :verify_full, ssl_ca_file: ca_file]}
            end

          :error ->
            {:ok, [ssl_mode: :verify_full]}
        end

      {:ok, "disable"} ->
        {:error, {:invalid_env, "FAVN_DATABASE_SSL_MODE", "verify-full"}}

      {:ok, _mode} ->
        {:error, {:invalid_env, "FAVN_DATABASE_SSL_MODE", "verify-full"}}
    end
  end

  defp postgres_tls(env, :local_development) do
    case fetch(env, "FAVN_DATABASE_SSL_MODE") do
      {:ok, "disable"} ->
        {:ok, [ssl_mode: :disable, deployment_mode: :local_development]}

      {:ok, _mode} ->
        {:error, {:invalid_env, "FAVN_DATABASE_SSL_MODE", "disable in local-development"}}

      :error ->
        {:error, {:missing_env, "FAVN_DATABASE_SSL_MODE"}}
    end
  end

  defp deployment_database_url(_url, :production), do: :ok

  defp deployment_database_url(url, :local_development) do
    case URI.parse(url) do
      %URI{host: "postgres.favn.internal", port: port} when port in [nil, 5432] ->
        :ok

      _invalid ->
        {:error,
         {:invalid_env, "FAVN_DATABASE_URL", :redacted,
          "postgres.favn.internal:5432 in local-development"}}
    end
  end

  defp api_server(env) do
    with {:ok, host} <- required_or_default(env, "FAVN_ORCHESTRATOR_API_BIND_HOST", "0.0.0.0"),
         :ok <- ipv4_host("FAVN_ORCHESTRATOR_API_BIND_HOST", host),
         {:ok, port} <- int(env, "FAVN_ORCHESTRATOR_API_PORT", "4101", 1, 65_535) do
      {:ok, [enabled: true, host: host, port: port]}
    end
  end

  defp manifest_publication(env) do
    with {:ok, compressed_limit_bytes} <-
           int(
             env,
             "FAVN_ORCHESTRATOR_MANIFEST_COMPRESSED_LIMIT_BYTES",
             Integer.to_string(ManifestPublicationConfig.default_compressed_limit_bytes()),
             1 * 1_024 * 1_024,
             32 * 1_024 * 1_024
           ),
         {:ok, decompressed_limit_bytes} <-
           int(
             env,
             "FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES",
             Integer.to_string(ManifestPublicationConfig.default_decompressed_limit_bytes()),
             1 * 1_024 * 1_024,
             128 * 1_024 * 1_024
           ) do
      ManifestPublicationConfig.new(
        compressed_limit_bytes: compressed_limit_bytes,
        decompressed_limit_bytes: decompressed_limit_bytes
      )
    end
  end

  defp api_service_tokens(env) do
    with {:ok, raw} <- required(env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS") do
      ServiceTokens.from_env_string(raw)
    end
  end

  defp auth_bootstrap(env) do
    with {:ok, username} <- required(env, "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME"),
         {:ok, password} <- required_secret(env, "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD"),
         {:ok, display_name} <-
           required_or_default(env, "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME", "Favn Admin"),
         {:ok, roles_raw} <-
           required_or_default(env, "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES", "admin"),
         roles = roles_raw |> String.split(",", trim: true) |> Enum.map(&String.trim/1),
         {:ok, actor} <- normalize_bootstrap_actor(username, display_name, roles),
         :ok <- validate_bootstrap_password(password) do
      {:ok,
       [
         username: actor.username,
         password: password,
         display_name: actor.display_name,
         roles: actor.roles
       ]}
    end
  end

  defp auth_session_ttl_seconds(env) do
    int(
      env,
      "FAVN_ORCHESTRATOR_AUTH_SESSION_TTL",
      "43200",
      1,
      @max_session_ttl_seconds
    )
  end

  defp active_run_plan_max_bytes(env) do
    int(
      env,
      "FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES",
      Integer.to_string(@default_active_run_plan_max_bytes),
      @min_active_run_plan_max_bytes,
      @max_active_run_plan_max_bytes
    )
  end

  defp scheduler(env, workspace_ids) do
    with {:ok, enabled?} <- bool(env, "FAVN_SCHEDULER_ENABLED", "true"),
         {:ok, tick_ms} <-
           int(env, "FAVN_SCHEDULER_TICK_MS", "15000", 100, @max_scheduler_tick_ms),
         {:ok, max_missed} <-
           int(
             env,
             "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES",
             "1000",
             0,
             @max_missed_occurrences
           ) do
      {:ok,
       [
         enabled: enabled?,
         workspace_ids: if(enabled?, do: workspace_ids, else: []),
         tick_ms: tick_ms,
         max_missed_all_occurrences: max_missed
       ]}
    end
  end

  defp workspace_ids(env) do
    with {:ok, raw} <- required(env, "FAVN_WORKSPACE_IDS") do
      workspace_ids =
        raw
        |> String.split(",", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))

      if byte_size(raw) <= @max_workspace_env_bytes and workspace_ids != [] and
           length(workspace_ids) <= @max_workspace_count and
           length(workspace_ids) == length(Enum.uniq(workspace_ids)) and
           Enum.all?(workspace_ids, &(byte_size(&1) in 1..255)),
         do: {:ok, workspace_ids},
         else:
           {:error,
            {:invalid_env, "FAVN_WORKSPACE_IDS", :redacted,
             "unique comma-separated ids (maximum 1000)"}}
    end
  end

  defp instance_id(env, default) do
    with {:ok, instance_id} <- required_or_default(env, "FAVN_INSTANCE_ID", default) do
      if byte_size(instance_id) in 1..160 and Regex.match?(~r/^[A-Za-z0-9_.@-]+$/, instance_id),
        do: {:ok, instance_id},
        else: {:error, {:invalid_env, "FAVN_INSTANCE_ID", "1..160 safe identifier bytes"}}
    end
  end

  defp http_server(env) do
    with {:ok, max_connections} <-
           int(env, "FAVN_HTTP_MAX_CONNECTIONS", "1024", 1, @max_http_connections),
         {:ok, request_timeout_ms} <-
           int(
             env,
             "FAVN_HTTP_REQUEST_TIMEOUT_MS",
             "30000",
             1_000,
             @max_http_request_timeout_ms
           ),
         {:ok, idle_timeout_ms} <-
           int(
             env,
             "FAVN_HTTP_IDLE_TIMEOUT_MS",
             "60000",
             1_000,
             @max_http_idle_timeout_ms
           ),
         {:ok, body_limit_bytes} <-
           int(
             env,
             "FAVN_HTTP_BODY_LIMIT_BYTES",
             Integer.to_string(1 * 1_024 * 1_024),
             @min_http_body_limit_bytes,
             @max_http_body_limit_bytes
           ) do
      {:ok,
       %{
         max_connections: max_connections,
         request_timeout_ms: request_timeout_ms,
         idle_timeout_ms: idle_timeout_ms,
         body_limit_bytes: body_limit_bytes
       }}
    end
  end

  defp shutdown_drain_timeout_ms(env) do
    int(
      env,
      "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS",
      "120000",
      1_000,
      @max_shutdown_drain_timeout_ms
    )
  end

  defp runner(env) do
    with {:ok, control_plane_node} <- node_name(env, "FAVN_CONTROL_PLANE_NODE"),
         {:ok, runner_node} <- node_name(env, "FAVN_RUNNER_NODE"),
         :ok <- distinct_nodes(control_plane_node, runner_node),
         :ok <- current_control_plane_node(control_plane_node),
         {:ok, cookie} <- required_secret(env, "FAVN_DISTRIBUTION_COOKIE"),
         :ok <- distribution_cookie(cookie),
         :ok <- current_distribution_cookie(cookie),
         {:ok, distribution_port} <- required_port(env, "FAVN_BEAM_DISTRIBUTION_PORT"),
         {:ok, epmd_port} <- int(env, "ERL_EPMD_PORT", "4369", 1, 65_535),
         {:ok, rpc_timeout_ms} <-
           int(
             env,
             "FAVN_RUNNER_RPC_TIMEOUT_MS",
             "15000",
             100,
             @max_runner_rpc_timeout_ms
           ),
         {:ok, diagnostics_timeout_ms} <-
           int(
             env,
             "FAVN_RUNNER_DIAGNOSTICS_TIMEOUT_MS",
             "5000",
             100,
             @max_runner_diagnostics_timeout_ms
           ),
         {:ok, await_buffer_ms} <-
           int(
             env,
             "FAVN_RUNNER_AWAIT_TIMEOUT_BUFFER_MS",
             "2000",
             0,
             @max_runner_await_buffer_ms
           ) do
      {:ok,
       {
         %{
           topology: :beam_node,
           control_plane_node: control_plane_node,
           runner_node: runner_node,
           distribution_port: distribution_port,
           epmd_port: epmd_port,
           cookie_configured?: true
         },
         [
           runner_node: runner_node,
           runner_module: @runner_module,
           runner_rpc_timeout_ms: rpc_timeout_ms,
           runner_diagnostics_timeout_ms: diagnostics_timeout_ms,
           runner_await_timeout_buffer_ms: await_buffer_ms
         ]
       }}
    end
  end

  defp node_name(env, name) do
    with {:ok, value} <- required(env, name),
         [local_name, host] <- String.split(value, "@", parts: 2),
         true <- valid_node_part?(local_name),
         true <- valid_node_host?(host) do
      {:ok, local_name <> "@" <> host}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_env, name, "long name@private-dns-name"}}
    end
  end

  defp valid_node_part?(value) do
    byte_size(value) in 1..255 and Regex.match?(~r/^[A-Za-z0-9_.-]+$/, value)
  end

  defp valid_node_host?(host) do
    normalized = String.downcase(host)

    valid_node_part?(host) and
      normalized not in ["localhost", "nohost", "127.0.0.1", "::1"] and
      not String.ends_with?(normalized, ".localhost") and
      not loopback_host?(host)
  end

  defp loopback_host?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _b, _c, _d}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _other -> false
    end
  end

  defp distinct_nodes(node, node),
    do: {:error, {:invalid_env, "FAVN_RUNNER_NODE", "different from control-plane node"}}

  defp distinct_nodes(_control_plane_node, _runner_node), do: :ok

  defp current_control_plane_node(control_plane_node) do
    if Node.alive?() and Atom.to_string(node()) != control_plane_node do
      {:error, {:invalid_env, "FAVN_CONTROL_PLANE_NODE", "equal to the running release node"}}
    else
      :ok
    end
  end

  defp distribution_cookie(cookie) do
    unique_bytes = cookie |> :binary.bin_to_list() |> MapSet.new() |> MapSet.size()

    if byte_size(cookie) in 32..255 and unique_bytes >= 12 and
         not Regex.match?(~r/\s/, cookie) do
      :ok
    else
      {:error, {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :insufficient_entropy}}
    end
  end

  defp current_distribution_cookie(cookie) do
    if Node.alive?() and Atom.to_string(Node.get_cookie()) != cookie do
      {:error, {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :running_cookie_mismatch}}
    else
      :ok
    end
  end

  defp install_runner_node_atom(opts) do
    Keyword.update!(opts, :runner_node, &String.to_atom/1)
  end

  defp required_port(env, name) do
    with {:ok, value} <- required(env, name) do
      case Integer.parse(value) do
        {port, ""} when port in 1..65_535 -> {:ok, port}
        _invalid -> {:error, {:invalid_env, name, "1..65535"}}
      end
    end
  end

  defp required(env, name) do
    case fetch(env, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_env, name}}
    end
  end

  defp required_or_default(env, name, default) do
    case fetch(env, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:ok, default}
    end
  end

  defp required_secret(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: {:error, {:missing_env, name}}, else: {:ok, value}

      _other ->
        {:error, {:missing_env, name}}
    end
  end

  defp fetch(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: :error, else: {:ok, value}

      _other ->
        :error
    end
  end

  defp bool(env, name, default) do
    with {:ok, value} <- required_or_default(env, name, default) do
      case String.downcase(value) do
        "true" -> {:ok, true}
        "false" -> {:ok, false}
        _other -> {:error, {:invalid_env, name, value, "boolean"}}
      end
    end
  end

  defp int(env, name, default, min, max) do
    with {:ok, value} <- required_or_default(env, name, default) do
      case Integer.parse(value) do
        {int, ""} when int >= min and (is_nil(max) or int <= max) ->
          {:ok, int}

        _other ->
          {:error, {:invalid_env, name, value, range(min, max)}}
      end
    end
  end

  defp range(min, nil), do: ">= #{min}"
  defp range(min, max), do: "#{min}..#{max}"

  defp absolute_regular_file(name, path) do
    if Path.type(path) == :absolute and File.regular?(path) do
      :ok
    else
      {:error, {:invalid_env, name, path, "absolute readable file"}}
    end
  end

  defp postgres_url(name, url) do
    uri = URI.parse(url)

    cond do
      is_binary(uri.query) ->
        {:error,
         {:invalid_env, name, url,
          "PostgreSQL connection URL without query parameters; use dedicated FAVN_DATABASE_* variables"}}

      valid_postgres_url?(uri) ->
        :ok

      true ->
        {:error, {:invalid_env, name, url, "PostgreSQL connection URL"}}
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

  defp ipv4_host(name, host) do
    case :inet.parse_ipv4_address(String.to_charlist(host)) do
      {:ok, _ip} -> :ok
      {:error, _reason} -> {:error, {:invalid_env, name, host, "IPv4 address"}}
    end
  end

  defp normalize_bootstrap_actor(username, display_name, roles) do
    case Credentials.normalize_actor(username, display_name, roles) do
      {:ok, actor} ->
        {:ok, actor}

      {:error, :invalid_roles} ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES", "viewer,operator,admin"}}

      {:error, reason} when reason in [:invalid_display_name, :display_name_too_long] ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME", "display name"}}

      {:error, _reason} ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME", "username"}}
    end
  end

  defp validate_bootstrap_password(password) do
    case Credentials.validate_password(password) do
      :ok ->
        :ok

      {:error, _reason} ->
        {:error, {:invalid_env, "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", "15..1024 byte password"}}
    end
  end

  defp redact({:invalid_secret_env, name, reason}), do: {:invalid_secret_env, name, reason}
  defp redact({:missing_env, name}), do: {:missing_env, name}
  defp redact({:invalid_env, name, expected}), do: {:invalid_env, name, expected}
  defp redact({:invalid_env, name, _value, expected}), do: {:invalid_env, name, expected}
  defp redact({:runtime_config_unavailable, name}), do: {:runtime_config_unavailable, name}
  defp redact(_reason), do: :invalid_runtime_config
end
