defmodule FavnLocal.Config do
  @moduledoc false

  alias Favn.RunnerRelease
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.RunnerClient.BeamNode
  alias FavnStoragePostgres.Config, as: PostgresConfig

  @default_workspace_id "local-dev"
  @default_orchestrator_port 4101
  @default_view_port 4173
  @pin_key_bytes 32

  @enforce_keys [
    :root_dir,
    :workspace_id,
    :orchestrator_port,
    :view_port,
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

    with {:ok, database_url} <- required_env(env, "FAVN_DATABASE_URL"),
         {:ok, pin_key} <- runtime_input_pin_key(env),
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
         {:ok, orchestrator_port} <-
           port(
             Keyword.get(
               opts,
               :orchestrator_port,
               Keyword.get(dev, :orchestrator_port, @default_orchestrator_port)
             )
           ),
         {:ok, view_port} <-
           port(Keyword.get(opts, :view_port, Keyword.get(dev, :view_port, @default_view_port))),
         {:ok, runner_release_id} <- runner_release_id(opts) do
      suffix = random_hex(8)

      {:ok,
       %__MODULE__{
         root_dir: root_dir,
         workspace_id: workspace_id,
         orchestrator_port: orchestrator_port,
         view_port: view_port,
         operator_node: String.to_atom("favn_local_operator_#{suffix}@127.0.0.1"),
         runner_node: String.to_atom("favn_local_runner_#{suffix}@127.0.0.1"),
         distribution_cookie: random_secret(48),
         service_token: random_secret(48),
         view_secret_key_base: Base.encode64(:crypto.strong_rand_bytes(64)),
         bootstrap_password: bootstrap_password(root_dir),
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
    service_tokens = [
      %{
        service_identity: "favn-local",
        token_hash: ServiceTokens.hash_token(config.service_token),
        enabled: true,
        platform_roles: [:platform_admin]
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
    Application.put_env(:favn_orchestrator, :runner_client, BeamNode)

    Application.put_env(
      :favn_orchestrator,
      :runner_client_opts,
      runner_node: config.runner_node,
      runner_module: FavnRunner,
      runner_rpc_timeout_ms: 15_000,
      runner_diagnostics_timeout_ms: 5_000
    )

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
      workspaces: [config.workspace_id]
    )

    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, "admin")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, config.bootstrap_password)
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Local Administrator")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    endpoint =
      :favn_view
      |> Application.get_env(FavnView.Endpoint, [])
      |> Keyword.merge(
        server: true,
        http: [ip: {127, 0, 0, 1}, port: config.view_port],
        url: [host: "127.0.0.1", port: config.view_port],
        check_origin: false,
        code_reloader: false,
        reloadable_apps: [],
        watchers: [],
        live_reload: nil,
        secret_key_base: config.view_secret_key_base
      )

    Application.put_env(:favn_view, :production_runtime_config, false)
    Application.put_env(:favn_view, :dev_routes, false)
    Application.put_env(:favn_view, FavnView.Endpoint, endpoint)

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

  defp runner_release_id(opts) do
    value = Keyword.get(opts, :runner_release_id, "rr_" <> random_hex(32))

    case RunnerRelease.validate_id(value) do
      :ok -> {:ok, value}
      {:error, _reason} -> {:error, {:invalid_runner_release_id, value}}
    end
  end

  defp random_hex(bytes), do: bytes |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

  defp random_secret(bytes),
    do: bytes |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)

  defp bootstrap_password(root_dir) do
    credentials_path = Path.join([root_dir, ".favn", "local", "credentials.json"])

    with {:ok, bytes} <- File.read(credentials_path),
         {:ok, %{"view_password" => password}} <- JSON.decode(bytes),
         true <- is_binary(password) and password != "" do
      password
    else
      _missing_or_invalid -> random_secret(48)
    end
  end
end
