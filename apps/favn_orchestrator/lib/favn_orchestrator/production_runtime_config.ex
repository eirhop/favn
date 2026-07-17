defmodule FavnOrchestrator.ProductionRuntimeConfig do
  @moduledoc """
  Production runtime configuration for the orchestrator process.

  This module owns the first production control-plane environment contract. It
  intentionally accepts only the SQLite production mode for Phase 1 and writes
  validated values into the `:favn_orchestrator` application environment before
  supervised runtime components start.
  """

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.API.ManifestPublication.Config, as: ManifestPublicationConfig

  @max_session_ttl_seconds 30 * 24 * 60 * 60
  @max_sqlite_busy_timeout_ms 60 * 60 * 1_000
  @max_scheduler_tick_ms 24 * 60 * 60 * 1_000
  @max_missed_occurrences 100_000

  @type config :: %{
          storage: :sqlite,
          sqlite: keyword(),
          api_server: keyword(),
          manifest_publication: keyword(),
          api_service_tokens: [ServiceTokens.token_config()],
          auth_bootstrap: keyword(),
          auth_session_ttl_seconds: pos_integer(),
          scheduler: keyword(),
          runner: map(),
          runner_client: module(),
          runner_client_opts: keyword()
        }

  @sqlite_adapter Module.concat([Favn, Storage, Adapter, SQLite])

  @doc """
  Applies production env config when explicitly configured.

  Startup calls this before children are built. Local/test environments that do
  not set `FAVN_STORAGE` continue using ordinary application config.
  """
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, term()}
  def apply_from_env_if_configured(env \\ System.get_env()) when is_map(env) do
    if Map.has_key?(env, "FAVN_STORAGE") or
         Application.get_env(:favn_orchestrator, :production_runtime_config, false) do
      apply_from_env(env)
    else
      :ok
    end
  end

  @doc """
  Validates and applies production env config.
  """
  @spec apply_from_env(map()) :: :ok | {:error, term()}
  def apply_from_env(env \\ System.get_env()) when is_map(env) do
    with {:ok, config} <- validate(env) do
      Application.put_env(:favn_orchestrator, :storage_adapter, @sqlite_adapter)
      Application.put_env(:favn_orchestrator, :storage_adapter_opts, config.sqlite)
      Application.put_env(:favn_orchestrator, :api_server, config.api_server)

      Application.put_env(
        :favn_orchestrator,
        :manifest_publication,
        config.manifest_publication
      )

      Application.put_env(:favn_orchestrator, :api_service_tokens, config.api_service_tokens)
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

      Application.put_env(:favn_orchestrator, :local_dev_mode, false)

      Application.put_env(
        :favn_orchestrator,
        :auth_session_ttl_seconds,
        config.auth_session_ttl_seconds
      )

      Application.put_env(:favn_orchestrator, :scheduler, config.scheduler)
      Application.put_env(:favn_orchestrator, :runner_client, config.runner_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, config.runner_client_opts)

      Application.put_env(
        :favn_orchestrator,
        :production_runtime_diagnostics,
        diagnostics(config)
      )

      :ok
    end
  end

  @doc """
  Validates production runtime env values without mutating application env.
  """
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env \\ System.get_env()) when is_map(env) do
    with {:ok, storage} <- storage(env),
         {:ok, sqlite} <- sqlite(env),
         {:ok, api_server} <- api_server(env),
         {:ok, manifest_publication} <- manifest_publication(env),
         {:ok, tokens} <- api_service_tokens(env),
         {:ok, auth_bootstrap} <- auth_bootstrap(env),
         {:ok, auth_session_ttl_seconds} <- auth_session_ttl_seconds(env),
         {:ok, scheduler} <- scheduler(env),
         {:ok, runner} <- runner(env) do
      {:ok,
       %{
         storage: storage,
         sqlite: sqlite,
         api_server: api_server,
         manifest_publication: ManifestPublicationConfig.to_keyword(manifest_publication),
         api_service_tokens: tokens,
         auth_bootstrap: auth_bootstrap,
         auth_session_ttl_seconds: auth_session_ttl_seconds,
         scheduler: scheduler,
         runner: runner,
         runner_client: FavnOrchestrator.RunnerClient.LocalNode,
         runner_client_opts: []
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
      storage: %{adapter: :sqlite, database: %{configured?: true, path: :redacted}},
      sqlite: %{
        migration_mode: Keyword.fetch!(config.sqlite, :migration_mode),
        busy_timeout: Keyword.fetch!(config.sqlite, :busy_timeout),
        pool_size: Keyword.fetch!(config.sqlite, :pool_size)
      },
      api_server: %{
        enabled: Keyword.fetch!(config.api_server, :enabled),
        host: Keyword.fetch!(config.api_server, :host),
        port: Keyword.fetch!(config.api_server, :port)
      },
      manifest_publication: Map.new(config.manifest_publication),
      api_service_tokens: %{count: length(config.api_service_tokens), redacted: true},
      auth_bootstrap: %{username_configured?: true, password_configured?: true, redacted: true},
      auth_session: %{ttl_seconds: config.auth_session_ttl_seconds},
      scheduler: Map.new(config.scheduler),
      runner: config.runner
    }
  end

  @spec sqlite_adapter() :: module()
  def sqlite_adapter, do: @sqlite_adapter

  defp storage(env) do
    case fetch(env, "FAVN_STORAGE") do
      {:ok, "sqlite"} -> {:ok, :sqlite}
      {:ok, other} -> {:error, {:invalid_env, "FAVN_STORAGE", other, "sqlite"}}
      :error -> {:error, {:missing_env, "FAVN_STORAGE"}}
    end
  end

  defp sqlite(env) do
    with {:ok, path} <- required(env, "FAVN_SQLITE_PATH"),
         :ok <- absolute_path("FAVN_SQLITE_PATH", path),
         {:ok, migration_mode} <-
           enum(env, "FAVN_SQLITE_MIGRATION_MODE", "manual", ~w(manual auto)),
         {:ok, busy_timeout} <-
           int(
             env,
             "FAVN_SQLITE_BUSY_TIMEOUT_MS",
             "5000",
             1,
             @max_sqlite_busy_timeout_ms
           ),
         {:ok, pool_size} <- int(env, "FAVN_SQLITE_POOL_SIZE", "1", 1, nil),
         :ok <- phase_one_pool_size(pool_size) do
      {:ok,
       [
         database: path,
         migration_mode: migration_mode_atom(migration_mode),
         busy_timeout: busy_timeout,
         pool_size: pool_size,
         require_absolute_path: true
       ]}
    end
  end

  defp api_server(env) do
    with {:ok, host} <- required_or_default(env, "FAVN_ORCHESTRATOR_API_BIND_HOST", "127.0.0.1"),
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
             1,
             ManifestPublicationConfig.maximum_compressed_limit_bytes()
           ),
         {:ok, decompressed_limit_bytes} <-
           int(
             env,
             "FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES",
             Integer.to_string(ManifestPublicationConfig.default_decompressed_limit_bytes()),
             1,
             ManifestPublicationConfig.maximum_decompressed_limit_bytes()
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

  defp scheduler(env) do
    with {:ok, enabled?} <- bool(env, "FAVN_SCHEDULER_ENABLED", "true"),
         {:ok, tick_ms} <-
           int(env, "FAVN_SCHEDULER_TICK_MS", "15000", 100, @max_scheduler_tick_ms),
         {:ok, max_missed} <-
           int(
             env,
             "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES",
             "1000",
             1,
             @max_missed_occurrences
           ) do
      {:ok,
       [
         enabled: enabled?,
         tick_ms: tick_ms,
         max_missed_all_occurrences: max_missed
       ]}
    end
  end

  defp runner(env) do
    module = Module.concat([FavnRunner, ProductionRuntimeConfig])

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :validate, 1) do
      case module.validate(env) do
        {:ok, config} -> {:ok, config}
        {:error, %{error: reason}} -> {:error, reason}
        {:error, reason} -> {:error, reason}
      end
    else
      _other -> {:error, {:runtime_config_unavailable, "FAVN_RUNNER_MODE"}}
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

  defp enum(env, name, default, allowed) do
    with {:ok, value} <- required_or_default(env, name, default) do
      if value in allowed do
        {:ok, value}
      else
        {:error, {:invalid_env, name, value, allowed}}
      end
    end
  end

  defp bool(env, name, default) do
    with {:ok, value} <- required_or_default(env, name, default) do
      case String.downcase(value) do
        "true" -> {:ok, true}
        "1" -> {:ok, true}
        "false" -> {:ok, false}
        "0" -> {:ok, false}
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

  defp absolute_path(name, path) do
    if Path.type(path) == :absolute do
      :ok
    else
      {:error, {:invalid_env, name, path, "absolute path"}}
    end
  end

  defp ipv4_host(name, host) do
    case :inet.parse_ipv4_address(String.to_charlist(host)) do
      {:ok, _ip} -> :ok
      {:error, _reason} -> {:error, {:invalid_env, name, host, "IPv4 address"}}
    end
  end

  defp phase_one_pool_size(1), do: :ok
  defp phase_one_pool_size(value), do: {:error, {:invalid_env, "FAVN_SQLITE_POOL_SIZE", value, 1}}

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

  defp migration_mode_atom("manual"), do: :manual
  defp migration_mode_atom("auto"), do: :auto

  defp redact({:invalid_secret_env, name, reason}), do: {:invalid_secret_env, name, reason}
  defp redact({:missing_env, name}), do: {:missing_env, name}
  defp redact({:invalid_env, name, expected}), do: {:invalid_env, name, expected}
  defp redact({:invalid_env, name, _value, expected}), do: {:invalid_env, name, expected}
  defp redact({:runtime_config_unavailable, name}), do: {:runtime_config_unavailable, name}
  defp redact(_reason), do: :invalid_runtime_config
end
