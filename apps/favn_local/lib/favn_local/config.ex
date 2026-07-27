defmodule FavnLocal.Config do
  @moduledoc false

  alias Favn.RunnerRelease
  alias FavnLocal.SourceRelease
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnStoragePostgres.Config, as: PostgresConfig
  alias FavnView.ApplicationConfig, as: ViewConfig

  @default_workspace_id "local-dev"
  @default_orchestrator_port 4101
  @default_view_port 4173
  @pin_key_bytes 32

  @enforce_keys [
    :root_dir,
    :workspace_id,
    :orchestrator_port,
    :view_port,
    :log_level,
    :operator_node,
    :runner_node,
    :distribution_cookie,
    :service_token,
    :view_secret_key_base,
    :bootstrap_password,
    :runner_release_id,
    :postgres_options,
    :runtime_input_pin_key
  ]
  defstruct @enforce_keys ++ [scheduler_enabled?: false]

  @type t :: %__MODULE__{
          root_dir: Path.t(),
          workspace_id: String.t(),
          orchestrator_port: :inet.port_number(),
          view_port: :inet.port_number(),
          log_level: Logger.level(),
          operator_node: node(),
          runner_node: node(),
          distribution_cookie: String.t(),
          service_token: String.t(),
          view_secret_key_base: String.t(),
          bootstrap_password: String.t(),
          runner_release_id: String.t(),
          postgres_options: keyword(),
          runtime_input_pin_key: binary(),
          scheduler_enabled?: boolean()
        }

  @spec load(keyword()) :: {:ok, t()} | {:error, term()}
  def load(opts \\ []) when is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()
    dev = Application.get_env(:favn, :dev, [])
    sources = %{opts: opts, env: env, dev: dev}

    with {:ok, database_url} <- required_env(env, "FAVN_DATABASE_URL"),
         {:ok, pin_key} <- runtime_input_pin_key(env),
         {:ok, log_level} <- log_level(env),
         {:ok, postgres_options} <-
           PostgresConfig.repo_options(
             url: database_url,
             ssl_mode: postgres_ssl_mode(env),
             ssl_ca_file: optional_env(env, "FAVN_DATABASE_SSL_CA_FILE"),
             pool_size: configured_integer(dev, :database_pool_size, 10),
             instance_id: "source-dev"
           ),
         {:ok, workspace_id} <-
           identifier(
             Keyword.get(
               opts,
               :workspace_id,
               Keyword.get(dev, :workspace_id, @default_workspace_id)
             )
           ),
         view_credentials <- view_credentials(root_dir, workspace_id),
         {:ok, orchestrator_port} <-
           port_setting(
             sources,
             :orchestrator_port,
             "FAVN_ORCHESTRATOR_API_PORT",
             @default_orchestrator_port
           ),
         {:ok, view_port} <-
           port_setting(sources, :view_port, "FAVN_VIEW_PORT", @default_view_port),
         {:ok, runner_release_id} <- runner_release_id(opts) do
      suffix = random_hex(8)

      {:ok,
       %__MODULE__{
         root_dir: root_dir,
         workspace_id: workspace_id,
         orchestrator_port: orchestrator_port,
         view_port: view_port,
         log_level: log_level,
         operator_node: String.to_atom("favn_local_operator_#{suffix}@127.0.0.1"),
         runner_node: String.to_atom("favn_local_runner_#{suffix}@127.0.0.1"),
         distribution_cookie: random_secret(48),
         service_token: random_secret(48),
         view_secret_key_base: view_credentials.secret_key_base,
         bootstrap_password: view_credentials.password,
         runner_release_id: runner_release_id,
         postgres_options: postgres_options,
         runtime_input_pin_key: pin_key,
         scheduler_enabled?:
           Keyword.get(opts, :scheduler, Keyword.get(dev, :scheduler_enabled, false))
       }}
    end
  end

  @spec apply(t()) :: :ok
  def apply(%__MODULE__{} = config) do
    :ok =
      Favn.LogLevel.configure_from_env(%{
        "FAVN_LOG_LEVEL" => Atom.to_string(config.log_level)
      })

    service_tokens = [
      %{
        service_identity: "favn-local",
        token_hash: ServiceTokens.hash_token(config.service_token),
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ]

    Application.put_env(:favn_storage_postgres, FavnStoragePostgres.Repo, config.postgres_options)

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_keys, %{
      1 => config.runtime_input_pin_key
    })

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_current_key_version, 1)

    Application.put_env(:favn_orchestrator, :start_runtime, true)
    Application.put_env(:favn_orchestrator, :production_runtime_config, false)
    Application.put_env(:favn_orchestrator, :control_plane_runtime_config, false)
    Application.put_env(:favn_orchestrator, :persistence_backend, FavnStoragePostgres.Backend)
    Application.put_env(:favn_orchestrator, :persistence_options, config.postgres_options)
    Application.put_env(:favn_orchestrator, :instance_id, Atom.to_string(config.operator_node))
    Application.put_env(:favn_orchestrator, :workspace_ids, [config.workspace_id])
    Application.put_env(:favn_orchestrator, :api_service_tokens, service_tokens)
    Application.delete_env(:favn_orchestrator, :api_service_tokens_env)
    Application.put_env(:favn_orchestrator, :runner_pools, default: [mode: :resident])

    Application.put_env(
      :favn_orchestrator,
      :api_server,
      enabled: true,
      host: "127.0.0.1",
      port: config.orchestrator_port
    )

    Application.put_env(
      :favn_orchestrator,
      :scheduler,
      enabled: config.scheduler_enabled?,
      tick_ms: 1_000,
      workspace_ids: [config.workspace_id]
    )

    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, "admin")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, config.bootstrap_password)
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Local Administrator")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    capability_hash = ServiceTokens.hash_token(config.service_token)

    Application.put_env(:favn_orchestrator, :trusted_local_development_auth, %{
      workspace_id: config.workspace_id,
      username: "admin",
      capability_hash: capability_hash
    })

    Application.put_env(:favn_view, :production_runtime_config, false)
    Application.put_env(:favn_view, :dev_routes, false)

    Application.put_env(:favn_view, :source_development_passwordless_login, %{
      workspace_id: config.workspace_id,
      username: "admin",
      capability: config.service_token
    })

    :ok =
      ViewConfig.configure(
        [
          server: true,
          http: [ip: {127, 0, 0, 1}, port: config.view_port],
          url: [host: "127.0.0.1", port: config.view_port],
          check_origin: false,
          code_reloader: false,
          reloadable_apps: [],
          watchers: [],
          live_reload: nil,
          secret_key_base: config.view_secret_key_base
        ],
        secure: false
      )

    :ok
  end

  @doc false
  @spec clear_source_development_auth() :: :ok
  def clear_source_development_auth do
    Application.delete_env(:favn_view, :source_development_passwordless_login)
    Application.delete_env(:favn_orchestrator, :trusted_local_development_auth)
    :ok
  end

  defp required_env(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, {:missing_env, name}}
          value -> {:ok, value}
        end

      _missing ->
        {:error, {:missing_env, name}}
    end
  end

  defp optional_env(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" -> value
      _missing -> nil
    end
  end

  defp runtime_input_pin_key(env) do
    with {:ok, encoded} <- required_env(env, "FAVN_RUNTIME_INPUT_PIN_KEY") do
      cond do
        byte_size(encoded) == @pin_key_bytes ->
          {:ok, encoded}

        true ->
          case Base.decode64(encoded) do
            {:ok, key} when byte_size(key) == @pin_key_bytes ->
              {:ok, key}

            _invalid ->
              {:error, {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEY", :invalid_key}}
          end
      end
    end
  end

  defp postgres_ssl_mode(env) do
    case Map.get(env, "FAVN_DATABASE_SSL_MODE", "disable") do
      "verify-full" -> :verify_full
      _other -> :disable
    end
  end

  defp log_level(env) do
    case Favn.LogLevel.parse(Map.get(env, "FAVN_LOG_LEVEL", "info")) do
      {:ok, level} ->
        {:ok, level}

      {:error, :invalid_log_level} ->
        {:error,
         {:invalid_env, "FAVN_LOG_LEVEL",
          "debug, info, notice, warning, error, critical, alert, or emergency"}}
    end
  end

  defp configured_integer(config, key, default) do
    case Keyword.get(config, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp identifier(value) when is_binary(value) and byte_size(value) in 1..255,
    do: {:ok, value}

  defp identifier(_value), do: {:error, {:invalid_dev_config, :workspace_id}}

  defp port(value) when is_integer(value) and value in 1..65_535, do: {:ok, value}
  defp port(_value), do: {:error, {:invalid_dev_config, :port}}

  # An explicit option beats the environment, which beats project configuration.
  # The environment has to win over `config :favn, :dev` so that several isolated
  # stacks can run on one machine without editing committed configuration.
  defp port_setting(sources, key, env_name, default) do
    case Keyword.fetch(sources.opts, key) do
      {:ok, value} -> port(value)
      :error -> configured_port(sources, key, env_name, default)
    end
  end

  defp configured_port(sources, key, env_name, default) do
    case optional_env(sources.env, env_name) do
      nil -> port(Keyword.get(sources.dev, key, default))
      value -> parsed_port(env_name, value)
    end
  end

  defp parsed_port(env_name, value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, {:invalid_env, env_name, "1..65535"}}
    end
  end

  defp runner_release_id(opts) do
    with {:ok, value} <- configured_runner_release_id(opts) do
      case RunnerRelease.validate_id(value) do
        :ok -> {:ok, value}
        {:error, _reason} -> {:error, {:invalid_runner_release_id, value}}
      end
    end
  end

  defp configured_runner_release_id(opts) do
    case Keyword.fetch(opts, :runner_release_id) do
      {:ok, value} -> {:ok, value}
      :error -> SourceRelease.current()
    end
  end

  defp random_hex(bytes), do: bytes |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

  defp random_secret(bytes),
    do: bytes |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)

  defp view_credentials(root_dir, workspace_id) do
    credentials_path = Path.join([root_dir, ".favn", "local", "credentials.json"])

    persisted =
      with {:ok, bytes} <- File.read(credentials_path),
           {:ok, credentials} when is_map(credentials) <- JSON.decode(bytes) do
        credentials
      else
        _missing_or_invalid -> %{}
      end

    %{
      password: persisted_secret(persisted, "view_password", 1, fn -> random_secret(48) end),
      secret_key_base:
        if persisted["view_workspace_id"] == workspace_id do
          persisted_secret(persisted, "view_secret_key_base", 64, fn ->
            Base.encode64(:crypto.strong_rand_bytes(64))
          end)
        else
          Base.encode64(:crypto.strong_rand_bytes(64))
        end
    }
  end

  defp persisted_secret(credentials, key, minimum_bytes, fallback) do
    case Map.get(credentials, key) do
      value when is_binary(value) and byte_size(value) >= minimum_bytes -> value
      _missing_or_invalid -> fallback.()
    end
  end
end
