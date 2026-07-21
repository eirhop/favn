defmodule Favn.Dev.RuntimeLaunch do
  @moduledoc false

  alias Favn.Dev.ChildEnvironment
  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.ConsumerConfigTransport
  alias Favn.Dev.EnvFile
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

      Favn.Dev.ConsumerConfigTransport.apply_from_env!()

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
        distribution
        |> runtime_env(opts)
        |> Map.put(
          "FAVN_DEV_CONSUMER_EBIN_PATHS",
          Enum.join(consumer_ebin_paths, path_separator())
        )
        |> Map.put(
          "FAVN_DEV_CONSUMER_FAVN_CONFIG",
          ConsumerConfigTransport.collect_and_encode(opts)
        )
    }
  end

  @spec operator_spec(map(), Config.t(), keyword(), map(), map()) :: map()
  def operator_spec(runtime, %Config{} = config, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    distribution = local_distribution!(opts)
    runtime_input_pin_key = Map.fetch!(secrets, "runtime_input_pin_key")

    code =
      """
      Favn.Dev.ConsumerConfigTransport.apply_from_env!()

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

      workspace_id = System.fetch_env!("FAVN_LOCAL_WORKSPACE_ID")

      Application.put_env(:favn_orchestrator, :persistence_backend, FavnStoragePostgres.Backend)
      Application.put_env(:favn_orchestrator, :workspace_ids, [workspace_id])

      Application.put_env(
        :favn_orchestrator,
        :persistence_options,
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: if(System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true", do: :verify_full, else: :disable),
        pool_size: String.to_integer(System.get_env("FAVN_DATABASE_POOL_SIZE", "10"))
      )

      Application.put_env(:favn_storage_postgres, :environment, :dev)

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
      Application.put_env(:favn_orchestrator, :runner_client, FavnOrchestrator.RunnerClient.BeamNode)
      Application.put_env(:favn_orchestrator, :runner_client_opts, [runner_node: runner_node])
      Application.put_env(:favn_orchestrator, :scheduler,
        enabled: System.get_env("FAVN_DEV_SCHEDULER_ENABLED", "0") == "1",
        workspace_ids: [workspace_id],
        tick_ms: 15_000
      )

      endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint, [])

      Application.put_env(
        :favn_view,
        FavnView.Endpoint,
        Keyword.merge(endpoint_config,
          server: true,
          http: [ip: {127, 0, 0, 1}, port: String.to_integer(System.fetch_env!("FAVN_VIEW_PORT"))]
        )
      )

      {:ok, _} = Application.ensure_all_started(:favn_orchestrator)
      {:ok, _} = Application.ensure_all_started(:favn_view)
      Process.sleep(:infinity)
      """
      |> String.trim()

    %{
      name: "operator",
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
      log_path: Paths.operator_log_path(Paths.root_dir(opts)),
      env:
        runtime_env(distribution, opts)
        |> Map.merge(%{
          "FAVN_DEV_STORAGE" => "postgres",
          "FAVN_DATABASE_URL" => config.postgres.url,
          "FAVN_LOCAL_WORKSPACE_ID" => config.workspace_id,
          "FAVN_WORKSPACE_IDS" => config.workspace_id,
          "FAVN_RUNTIME_INPUT_PIN_KEY" => runtime_input_pin_key,
          "FAVN_DEV_POSTGRES_SSL" => if(config.postgres.ssl, do: "true", else: "false"),
          "FAVN_DATABASE_POOL_SIZE" => Integer.to_string(config.postgres.pool_size),
          "FAVN_DEV_SCHEDULER_ENABLED" => if(config.scheduler_enabled, do: "1", else: "0"),
          "FAVN_DEV_RUNNER_NODE" => node_names.runner_full,
          "FAVN_ORCHESTRATOR_API_ENABLED" =>
            if(config.orchestrator_api_enabled, do: "1", else: "0"),
          "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(config.orchestrator_port),
          "FAVN_ORCHESTRATOR_API_BIND_IP" => @loopback_host,
          "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" =>
            "favn_view|platform_operator:" <> secrets["service_token"],
          "FAVN_VIEW_PORT" => Integer.to_string(config.web_port),
          "FAVN_VIEW_ORCHESTRATOR_BASE_URL" => config.orchestrator_base_url,
          "FAVN_VIEW_PUBLIC_ORIGIN" => config.web_base_url,
          "FAVN_VIEW_SECRET_KEY_BASE" => secrets["web_session_secret"],
          "FAVN_VIEW_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"],
          "FAVN_VIEW_LOCAL_DEV_TRUSTED_AUTH" => "1",
          "FAVN_DEV_CONSUMER_FAVN_CONFIG" =>
            ConsumerConfigTransport.collect_and_encode(opts, only: [:execution_pools])
        })
    }
  end

  defp runtime_env(distribution, opts) do
    opts
    |> EnvFile.loaded_env()
    |> Map.merge(%{
      "MIX_ENV" => "dev",
      "MIX_OS_CONCURRENCY_LOCK" => "0",
      "ERL_EPMD_ADDRESS" => distribution.bind_ip
    })
    |> ChildEnvironment.sanitize_proxy_variables()
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
