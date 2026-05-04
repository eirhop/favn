defmodule Favn.Dev.RuntimeLaunch do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.ConsumerConfigTransport
  alias Favn.Dev.LocalDistribution
  alias Favn.Dev.Paths

  @loopback_host "127.0.0.1"
  @distribution_port_base 45_000
  @distribution_port_span 20_000

  @spec distribution_port(:runner | :orchestrator | :control, keyword()) :: pos_integer()
  def distribution_port(service, opts) when service in [:runner, :orchestrator, :control] do
    root_dir = Paths.root_dir(opts)
    base = @distribution_port_base + :erlang.phash2(root_dir, @distribution_port_span)

    case service do
      :runner -> base
      :orchestrator -> base + 1
      :control -> base + 2
    end
  end

  @spec runner_spec(map(), keyword(), map(), map()) :: map()
  def runner_spec(runtime, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    distribution = local_distribution!(opts)

    code =
      """
      delimiter = if match?({:win32, _}, :os.type()), do: ";", else: ":"

      System.get_env("FAVN_DEV_CONSUMER_EBIN_PATHS", "")
      |> String.split(delimiter, trim: true)
      |> Enum.each(&Code.prepend_path/1)

      #{ConsumerConfigTransport.bootstrap_eval_snippet()}

      {:ok, _} = Application.ensure_all_started(:favn_runner)
      Process.sleep(:infinity)
      """
      |> String.trim()

    base_args =
      distributed_erlang_args(:runner, opts, distribution) ++
        [
          "--sname",
          node_names.runner_short,
          "--cookie",
          secrets["rpc_cookie"]
        ]

    consumer_ebin_paths = ConsumerCodePath.ebin_paths(opts)

    args =
      base_args ++
        ["-S", "mix", "run", "--no-compile", "--no-start", "--eval", code]

    %{
      name: "runner",
      exec: elixir,
      args: args,
      cwd: runtime["runner_root"],
      log_path: Paths.runner_log_path(Paths.root_dir(opts)),
      env:
        Map.put(
          runtime_env(distribution),
          "FAVN_DEV_CONSUMER_EBIN_PATHS",
          Enum.join(consumer_ebin_paths, path_separator())
        )
        |> Map.put(
          "FAVN_DEV_CONSUMER_FAVN_CONFIG",
          ConsumerConfigTransport.collect_and_encode(opts)
        )
    }
  end

  @spec orchestrator_spec(map(), Config.t(), keyword(), map(), map()) :: map()
  def orchestrator_spec(runtime, %Config{} = config, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    sqlite_path = Path.expand(config.sqlite_path, Paths.root_dir(opts))
    distribution = local_distribution!(opts)

    code =
      """
      storage = System.fetch_env!("FAVN_DEV_STORAGE")

      api_bind_ip =
        System.fetch_env!("FAVN_ORCHESTRATOR_API_BIND_IP")
        |> String.split(".", parts: 4)
        |> Enum.map(&String.to_integer/1)
        |> List.to_tuple()

      Application.put_env(
        :favn_orchestrator,
        :api_server,
        enabled: System.get_env("FAVN_ORCHESTRATOR_API_ENABLED", "0") == "1",
        port: String.to_integer(System.fetch_env!("FAVN_ORCHESTRATOR_API_PORT")),
        bind_ip: api_bind_ip
      )

      Application.put_env(
        :favn_orchestrator,
        :api_service_tokens,
        []
      )

      Application.put_env(
        :favn_orchestrator,
        :api_service_tokens_env,
        System.get_env("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "")
      )

      Application.put_env(:favn_orchestrator, :local_dev_mode, true)

      case storage do
        "memory" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, FavnOrchestrator.Storage.Adapter.Memory)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

        "sqlite" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.SQLite)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts,
            database: System.fetch_env!("FAVN_DEV_SQLITE_PATH"),
            migration_mode: :auto
          )
          {:ok, _} = Application.ensure_all_started(:favn_storage_sqlite)

        "postgres" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.Postgres)

          Application.put_env(
            :favn_orchestrator,
            :storage_adapter_opts,
            hostname: System.fetch_env!("FAVN_DEV_POSTGRES_HOST"),
            port: String.to_integer(System.fetch_env!("FAVN_DEV_POSTGRES_PORT")),
            username: System.fetch_env!("FAVN_DEV_POSTGRES_USERNAME"),
            password: System.fetch_env!("FAVN_DEV_POSTGRES_PASSWORD"),
            database: System.fetch_env!("FAVN_DEV_POSTGRES_DATABASE"),
            ssl: System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true",
            pool_size: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_POOL_SIZE", "10"))
          )

          {:ok, _} = Application.ensure_all_started(:favn_storage_postgres)

        other ->
          raise ArgumentError,
                "unsupported FAVN_DEV_STORAGE=\#{inspect(other)}; expected memory, sqlite, or postgres"
      end

      validate_runner_node_name! = fn node_name ->
        valid_part? = fn part ->
          byte_size(part) in 1..128 and String.match?(part, ~r/^[A-Za-z0-9_-]+$/)
        end

        case String.split(node_name, "@", parts: 2) do
          [short_name, host] when byte_size(node_name) <= 255 ->
            if not String.contains?(host, ".") and valid_part?.(short_name) and valid_part?.(host) do
              node_name
            else
              raise ArgumentError, "invalid FAVN_DEV_RUNNER_NODE"
            end

          _other ->
            raise ArgumentError, "invalid FAVN_DEV_RUNNER_NODE"
        end
      end

      runner_node = System.fetch_env!("FAVN_DEV_RUNNER_NODE") |> validate_runner_node_name!.() |> String.to_atom()
      Application.put_env(:favn_orchestrator, :runner_client, FavnOrchestrator.RunnerClient.LocalNode)
      Application.put_env(:favn_orchestrator, :runner_client_opts, [runner_node: runner_node])
      Application.put_env(:favn_orchestrator, :scheduler,
        enabled: System.get_env("FAVN_DEV_SCHEDULER_ENABLED", "0") == "1",
        tick_ms: 15_000
      )

      {:ok, _} = Application.ensure_all_started(:favn_orchestrator)
      Process.sleep(:infinity)
      """
      |> String.trim()

    %{
      name: "orchestrator",
      exec: elixir,
      args:
        distributed_erlang_args(:orchestrator, opts, distribution) ++
          [
            "--sname",
            node_names.orchestrator_short,
            "--cookie",
            secrets["rpc_cookie"],
            "-S",
            "mix",
            "run",
            "--no-compile",
            "--no-start",
            "--eval",
            code
          ],
      cwd: runtime["orchestrator_root"],
      log_path: Paths.orchestrator_log_path(Paths.root_dir(opts)),
      env:
        runtime_env(distribution)
        |> Map.merge(%{
          "FAVN_DEV_STORAGE" => Atom.to_string(config.storage),
          "FAVN_DEV_SQLITE_PATH" => sqlite_path,
          "FAVN_DEV_POSTGRES_HOST" => config.postgres.hostname,
          "FAVN_DEV_POSTGRES_PORT" => Integer.to_string(config.postgres.port),
          "FAVN_DEV_POSTGRES_USERNAME" => config.postgres.username,
          "FAVN_DEV_POSTGRES_PASSWORD" => config.postgres.password,
          "FAVN_DEV_POSTGRES_DATABASE" => config.postgres.database,
          "FAVN_DEV_POSTGRES_SSL" => if(config.postgres.ssl, do: "true", else: "false"),
          "FAVN_DEV_POSTGRES_POOL_SIZE" => Integer.to_string(config.postgres.pool_size),
          "FAVN_DEV_SCHEDULER_ENABLED" => if(config.scheduler_enabled, do: "1", else: "0"),
          "FAVN_DEV_RUNNER_NODE" => node_names.runner_full,
          "FAVN_ORCHESTRATOR_API_ENABLED" =>
            if(config.orchestrator_api_enabled, do: "1", else: "0"),
          "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(config.orchestrator_port),
          "FAVN_ORCHESTRATOR_API_BIND_IP" => @loopback_host,
          "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "favn_web:" <> secrets["service_token"]
        })
    }
  end

  @spec web_spec(map(), Config.t(), keyword(), map()) :: map()
  def web_spec(runtime, %Config{} = config, opts, secrets)
      when is_map(runtime) and is_list(opts) and is_map(secrets) do
    node = System.find_executable("node") || "node"
    vite = Path.join(runtime["web_root"], "node_modules/vite/bin/vite.js")

    %{
      name: "web",
      exec: node,
      args: [
        vite,
        "preview",
        "--host",
        @loopback_host,
        "--port",
        Integer.to_string(config.web_port)
      ],
      cwd: runtime["web_root"],
      log_path: Paths.web_log_path(Paths.root_dir(opts)),
      env: %{
        "FAVN_WEB_ORCHESTRATOR_BASE_URL" => config.orchestrator_base_url,
        "FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"],
        "FAVN_WEB_SESSION_SECRET" => secrets["web_session_secret"]
      }
    }
  end

  defp runtime_env(distribution) do
    %{"MIX_ENV" => "dev", "ERL_EPMD_ADDRESS" => distribution.bind_ip}
  end

  defp distributed_erlang_args(service, opts, distribution) do
    port = distribution_port(service, opts)

    [
      "--erl",
      LocalDistribution.erl_flags(distribution, port)
    ]
  end

  defp local_distribution!(opts) do
    case LocalDistribution.preflight(opts) do
      {:ok, distribution} -> distribution
      {:error, reason} -> raise ArgumentError, LocalDistribution.format_error(reason)
    end
  end

  defp path_separator do
    case :os.type() do
      {:win32, _name} -> ";"
      _other -> ":"
    end
  end
end
