defmodule Favn.Dev.Stack do
  @moduledoc """
  Foreground local stack lifecycle implementation owned by `favn_local`.

  Lock scope is intentionally limited to short runtime-state mutation windows.
  The foreground wait loop never holds `.favn/lock`.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.Install
  alias Favn.Dev.Lock
  alias Favn.Dev.NodeControl
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Paths
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RunnerControl
  alias Favn.Dev.Secrets
  alias Favn.Dev.State

  @runtime_schema_version 1
  @type root_opt :: [root_dir: Path.t()]

  @spec start_foreground(root_opt()) :: :ok | {:error, term()}
  def start_foreground(opts \\ []) when is_list(opts) do
    with {:ok, startup} <- initialize_stack(opts),
         :ok <- bootstrap_manifest(startup, opts) do
      print_start_summary(startup)
      wait_foreground(startup, opts)
    else
      {:error, :stack_already_running} = error ->
        error

      {:error, {:stack_partially_running, _service_states}} = error ->
        error

      {:error, reason} = error ->
        if cleanup_required?(reason) do
          _ = cleanup_after_failure(reason, opts)
        end

        error
    end
  end

  defp cleanup_required?(:install_required), do: false
  defp cleanup_required?(:install_stale), do: false
  defp cleanup_required?({:missing_tool, _tool}), do: false
  defp cleanup_required?({:tool_check_failed, _tool, _status, _output}), do: false
  defp cleanup_required?({:port_conflict, _service, _port}), do: false
  defp cleanup_required?({:port_check_failed, _service, _port, _reason}), do: false
  defp cleanup_required?({:postgres_misconfigured, _field}), do: false
  defp cleanup_required?({:postgres_unavailable, _host, _port, _reason}), do: false
  defp cleanup_required?({:shortname_host_unavailable, _reason}), do: false
  defp cleanup_required?(:shortname_host_not_available), do: false
  defp cleanup_required?({:invalid_shortname_host, _host}), do: false
  defp cleanup_required?(_reason), do: true

  @spec stop(root_opt()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts) do
    case with_lock(opts, fn -> State.read_runtime(opts) end) do
      {:ok, runtime} ->
        stop_runtime(runtime, opts)
        with_lock(opts, fn -> State.clear_runtime(opts) end)
        :ok

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp initialize_stack(opts) do
    with_lock(opts, fn ->
      with :ok <- State.ensure_layout(opts),
           :ok <- ensure_stack_not_running(opts),
           :ok <- ensure_install_ready(opts),
           config <- Config.resolve(opts),
           :ok <- ensure_startup_prerequisites(config),
           {:ok, secrets} <- Secrets.resolve(config, opts),
           {:ok, node_names} <- build_node_names(secrets, opts),
           {:ok, services} <- start_services(config, secrets, node_names, opts),
           :ok <- write_runtime(config, secrets, node_names, services, opts) do
        {:ok, %{config: config, secrets: secrets, node_names: node_names, services: services}}
      end
    end)
  end

  defp ensure_stack_not_running(opts) do
    case State.read_runtime(opts) do
      {:ok, runtime} ->
        case runtime_health(runtime) do
          :running ->
            {:error, :stack_already_running}

          :partial ->
            {:error, {:stack_partially_running, runtime_service_statuses(runtime)}}

          :stale ->
            State.clear_runtime(opts)
        end

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_install_ready(opts) do
    if Keyword.get(opts, :skip_install_check, false) do
      :ok
    else
      Install.ensure_ready(opts)
    end
  end

  defp ensure_startup_prerequisites(config) do
    case ensure_ports_available(config) do
      :ok -> ensure_storage_ready(config)
      {:error, _reason} = error -> error
    end
  end

  defp ensure_ports_available(config) do
    case ensure_port_available(:orchestrator, config.orchestrator_port) do
      :ok -> ensure_port_available(:web, config.web_port)
      {:error, _reason} = error -> error
    end
  end

  defp ensure_port_available(service, port)
       when is_atom(service) and is_integer(port) and port > 0 do
    case :gen_tcp.listen(port, [:binary, {:active, false}, {:reuseaddr, false}]) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        :ok

      {:error, :eaddrinuse} ->
        {:error, {:port_conflict, service, port}}

      {:error, reason} ->
        {:error, {:port_check_failed, service, port, reason}}
    end
  end

  defp ensure_storage_ready(%{storage: :postgres, postgres: postgres}) when is_map(postgres) do
    case validate_postgres_config(postgres) do
      :ok -> verify_postgres_connectivity(postgres)
      {:error, _reason} = error -> error
    end
  end

  defp ensure_storage_ready(_config), do: :ok

  defp validate_postgres_config(postgres) do
    required = [
      hostname: :hostname,
      username: :username,
      password: :password,
      database: :database
    ]

    case Enum.find(required, fn {_field, key} ->
           postgres[key] |> to_string() |> String.trim() == ""
         end) do
      {field, _key} -> {:error, {:postgres_misconfigured, field}}
      nil -> :ok
    end
  end

  defp verify_postgres_connectivity(postgres) do
    host = postgres.hostname |> to_string()
    port = postgres.port

    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, {:active, false}], 1_500) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        :ok

      {:error, reason} ->
        {:error, {:postgres_unavailable, host, port, reason}}
    end
  end

  defp runtime_health(runtime) do
    statuses = runtime_service_statuses(runtime)

    cond do
      Enum.all?(statuses, &match?({_name, :running}, &1)) -> :running
      Enum.any?(statuses, &match?({_name, :running}, &1)) -> :partial
      true -> :stale
    end
  end

  defp runtime_service_statuses(runtime) do
    services = Map.get(runtime, "services", %{})

    ["web", "orchestrator", "runner"]
    |> Enum.map(fn service_name ->
      state =
        case get_in(services, [service_name, "pid"]) do
          pid when is_integer(pid) and pid > 0 ->
            if DevProcess.alive?(pid), do: :running, else: :dead

          _ ->
            :unknown
        end

      {service_name, state}
    end)
  end

  defp build_node_names(secrets, opts) when is_map(secrets) do
    root_dir = Paths.root_dir(opts)
    suffix = Integer.to_string(:erlang.phash2(root_dir, 1_000_000))

    runner_short = "favn_runner_#{suffix}"
    orchestrator_short = "favn_orchestrator_#{suffix}"
    control_short = "favn_local_ctl_#{suffix}"

    with :ok <- NodeControl.ensure_local_node_started(secrets["rpc_cookie"], name: control_short),
         {:ok, runner_full} <- NodeControl.shortname_to_full(runner_short),
         {:ok, orchestrator_full} <- NodeControl.shortname_to_full(orchestrator_short),
         {:ok, control_full} <- NodeControl.shortname_to_full(control_short) do
      {:ok,
       %{
         runner_short: runner_short,
         runner_full: runner_full,
         orchestrator_short: orchestrator_short,
         orchestrator_full: orchestrator_full,
         control_short: control_short,
         control_full: control_full
       }}
    end
  end

  defp start_services(config, secrets, node_names, opts) do
    root_dir = Paths.root_dir(opts)

    specs =
      case Keyword.get(opts, :service_specs_override) do
        list when is_list(list) and list != [] ->
          list

        _ ->
          :ok = ensure_web_assets(root_dir, opts)

          [
            runner_spec(root_dir, node_names, secrets),
            orchestrator_spec(root_dir, config, node_names, secrets),
            web_spec(root_dir, config, secrets)
          ]
      end

    Enum.reduce_while(specs, {:ok, %{}}, fn spec, {:ok, acc} ->
      case DevProcess.start_service(spec) do
        {:ok, info} ->
          {:cont, {:ok, Map.put(acc, info.name, info)}}

        {:error, reason} ->
          stop_service_map(acc)
          {:halt, {:error, {:start_failed, spec.name, reason}}}
      end
    end)
  end

  defp ensure_web_assets(root_dir, opts) do
    if Keyword.get(opts, :skip_web_build, false) do
      :ok
    else
      web_cwd = Path.join(root_dir, "web/favn_web")
      built_index = Path.join(web_cwd, "dist/index.html")

      if File.exists?(built_index) do
        :ok
      else
        npm = System.find_executable("npm") || "npm"

        case System.cmd(npm, ["run", "build", "--silent"], cd: web_cwd, stderr_to_stdout: true) do
          {_output, 0} -> :ok
          {output, status} -> {:error, {:web_build_failed, status, String.trim(output)}}
        end
      end
    end
  end

  defp runner_spec(root_dir, node_names, secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    code = "Application.ensure_all_started(:favn_runner); Process.sleep(:infinity)"

    %{
      name: "runner",
      exec: elixir,
      args: [
        "--sname",
        node_names.runner_short,
        "--cookie",
        secrets["rpc_cookie"],
        "-S",
        "mix",
        "run",
        "--no-start",
        "--eval",
        code
      ],
      cwd: Path.join(root_dir, "apps/favn_runner"),
      log_path: Paths.runner_log_path(root_dir),
      env: %{"MIX_ENV" => "dev"}
    }
  end

  defp orchestrator_spec(root_dir, config, node_names, secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    sqlite_path = Path.expand(config.sqlite_path, root_dir)

    code =
      """
      storage = System.get_env("FAVN_DEV_STORAGE", "memory")

      cond do
        storage == "sqlite" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.SQLite)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts, database: System.get_env("FAVN_DEV_SQLITE_PATH"))

        storage == "postgres" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.Postgres)

          Application.put_env(
            :favn_orchestrator,
            :storage_adapter_opts,
            hostname: System.get_env("FAVN_DEV_POSTGRES_HOST"),
            port: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_PORT", "5432")),
            username: System.get_env("FAVN_DEV_POSTGRES_USERNAME"),
            password: System.get_env("FAVN_DEV_POSTGRES_PASSWORD"),
            database: System.get_env("FAVN_DEV_POSTGRES_DATABASE"),
            ssl: System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true",
            pool_size: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_POOL_SIZE", "10"))
          )
      end
      runner_node = String.to_atom(System.get_env("FAVN_DEV_RUNNER_NODE"))
      Application.put_env(:favn_orchestrator, :runner_client, FavnOrchestrator.RunnerClient.LocalNode)
      Application.put_env(:favn_orchestrator, :runner_client_opts, [runner_node: runner_node])
      Application.ensure_all_started(:favn_orchestrator)
      Process.sleep(:infinity)
      """
      |> String.trim()

    %{
      name: "orchestrator",
      exec: elixir,
      args: [
        "--sname",
        node_names.orchestrator_short,
        "--cookie",
        secrets["rpc_cookie"],
        "-S",
        "mix",
        "run",
        "--no-start",
        "--eval",
        code
      ],
      cwd: Path.join(root_dir, "apps/favn_orchestrator"),
      log_path: Paths.orchestrator_log_path(root_dir),
      env: %{
        "MIX_ENV" => "dev",
        "FAVN_DEV_STORAGE" => Atom.to_string(config.storage),
        "FAVN_DEV_SQLITE_PATH" => sqlite_path,
        "FAVN_DEV_POSTGRES_HOST" => config.postgres.hostname,
        "FAVN_DEV_POSTGRES_PORT" => Integer.to_string(config.postgres.port),
        "FAVN_DEV_POSTGRES_USERNAME" => config.postgres.username,
        "FAVN_DEV_POSTGRES_PASSWORD" => config.postgres.password,
        "FAVN_DEV_POSTGRES_DATABASE" => config.postgres.database,
        "FAVN_DEV_POSTGRES_SSL" => if(config.postgres.ssl, do: "true", else: "false"),
        "FAVN_DEV_POSTGRES_POOL_SIZE" => Integer.to_string(config.postgres.pool_size),
        "FAVN_DEV_RUNNER_NODE" => node_names.runner_full,
        "FAVN_ORCHESTRATOR_API_ENABLED" =>
          if(config.orchestrator_api_enabled, do: "1", else: "0"),
        "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(config.orchestrator_port),
        "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => secrets["service_token"]
      }
    }
  end

  defp web_spec(root_dir, config, secrets) do
    npm = System.find_executable("npm") || "npm"
    web_cwd = Path.join(root_dir, "web/favn_web")

    %{
      name: "web",
      exec: npm,
      args: [
        "run",
        "preview",
        "--",
        "--host",
        "127.0.0.1",
        "--port",
        Integer.to_string(config.web_port)
      ],
      cwd: web_cwd,
      log_path: Paths.web_log_path(root_dir),
      env: %{
        "FAVN_ORCHESTRATOR_BASE_URL" => config.orchestrator_base_url,
        "FAVN_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"],
        "FAVN_WEB_SESSION_SECRET" => secrets["web_session_secret"]
      }
    }
  end

  defp write_runtime(config, _secrets, node_names, services, opts) do
    runtime = %{
      "schema_version" => @runtime_schema_version,
      "owner_app" => "favn_local",
      "started_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "storage" => Atom.to_string(config.storage),
      "orchestrator_base_url" => config.orchestrator_base_url,
      "web_base_url" => config.web_base_url,
      "rpc_cookie_ref" => "secrets.rpc_cookie",
      "node_names" => %{
        "runner" => node_names.runner_full,
        "orchestrator" => node_names.orchestrator_full,
        "control" => node_names.control_full
      },
      "services" =>
        Map.new(services, fn {name, info} ->
          service = %{"pid" => info.pid, "log_path" => info.log_path}

          service =
            case name do
              "runner" -> Map.put(service, "node_name", node_names.runner_full)
              "orchestrator" -> Map.put(service, "node_name", node_names.orchestrator_full)
              _ -> service
            end

          {name, service}
        end),
      "service_token_ref" => "secrets.service_token",
      "web_session_secret_ref" => "secrets.web_session_secret"
    }

    State.write_runtime(runtime, opts)
  end

  defp bootstrap_manifest(startup, opts) do
    if Keyword.get(opts, :skip_bootstrap, false) do
      :ok
    else
      do_bootstrap_manifest(startup, opts)
    end
  end

  defp do_bootstrap_manifest(startup, opts) do
    %{config: config, secrets: secrets, node_names: node_names} = startup

    with :ok <- Mix.Task.reenable("compile"),
         _ <- Mix.Task.run("compile", ["--force"]),
         {:ok, build} <- FavnAuthoring.build_manifest(),
         {:ok, version} <- FavnAuthoring.pin_manifest_version(build.manifest),
         :ok <-
           RunnerControl.register_manifest(version,
             runner_node_name: node_names.runner_full,
             rpc_cookie: secrets["rpc_cookie"]
           ),
         :ok <-
           State.write_manifest_latest(
             %{
               "manifest_version_id" => version.manifest_version_id,
               "content_hash" => version.content_hash,
               "schema_version" => version.schema_version,
               "runner_contract_version" => version.runner_contract_version,
               "serialization_format" => version.serialization_format,
               "inserted_at" => datetime(version.inserted_at)
             },
             opts
           ),
         {:ok, _published} <-
           OrchestratorClient.publish_manifest(
             config.orchestrator_base_url,
             secrets["service_token"],
             %{
               manifest_version_id: version.manifest_version_id,
               manifest: version.manifest
             }
           ),
         {:ok, _activated} <-
           OrchestratorClient.activate_manifest(
             config.orchestrator_base_url,
             secrets["service_token"],
             version.manifest_version_id
           ) do
      with_lock(opts, fn ->
        with {:ok, runtime} <- State.read_runtime(opts) do
          State.write_runtime(
            Map.put(runtime, "active_manifest_version_id", version.manifest_version_id),
            opts
          )
        end
      end)

      :ok
    end
  end

  defp wait_foreground(startup, opts) do
    %{config: config, services: services} = startup

    case wait_ready(config, opts) do
      :ok ->
        monitor_services(services)
        wait_for_service_exit(startup, opts)

      {:error, reason} ->
        cleanup_after_failure(reason, opts)
        {:error, reason}
    end
  end

  defp wait_ready(config, opts) do
    if Keyword.get(opts, :skip_readiness, false) do
      :ok
    else
      case wait_http(config.orchestrator_base_url, 15_000) do
        :ok -> wait_http(config.web_base_url, 30_000)
        {:error, _reason} = error -> error
      end
    end
  end

  defp wait_http(url, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_http(url, deadline)
  end

  defp do_wait_http(url, deadline_ms) do
    case http_up?(url) do
      true ->
        :ok

      false ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, {:not_ready, url}}
        else
          Process.sleep(200)
          do_wait_http(url, deadline_ms)
        end
    end
  end

  defp http_up?(url) do
    case URI.parse(url) do
      %URI{host: host, port: port} when is_binary(host) and is_integer(port) and port > 0 ->
        case :gen_tcp.connect(String.to_charlist(host), port, [:binary, {:active, false}], 1_000) do
          {:ok, socket} ->
            :gen_tcp.close(socket)
            true

          {:error, _reason} ->
            false
        end

      _other ->
        false
    end
  end

  defp monitor_services(services) do
    Enum.each(services, fn {_name, info} ->
      Process.monitor(info.wrapper_pid)
    end)
  end

  defp wait_for_service_exit(startup, opts) do
    receive do
      {:service_exit, service, status} ->
        reason = {:service_exit, service, status}
        cleanup_after_failure(reason, opts, startup)
        {:error, reason}

      {:DOWN, _ref, :process, _pid, reason} ->
        failure = {:service_down, reason}
        cleanup_after_failure(failure, opts, startup)
        {:error, failure}
    end
  end

  defp cleanup_after_failure(reason, opts, startup \\ nil) do
    _ =
      with_lock(opts, fn ->
        State.write_last_failure(
          %{"error" => inspect(reason), "at" => DateTime.utc_now() |> DateTime.to_iso8601()},
          opts
        )
      end)

    runtime =
      case with_lock(opts, fn -> State.read_runtime(opts) end) do
        {:ok, value} -> value
        _ -> %{}
      end

    services =
      case startup do
        %{services: service_map} when is_map(service_map) ->
          Map.new(service_map, fn {name, info} -> {name, %{pid: info.pid}} end)

        _ ->
          Map.get(runtime, "services", %{})
      end

    stop_service_map(services)
    with_lock(opts, fn -> State.clear_runtime(opts) end)
    :ok
  end

  defp stop_runtime(runtime, opts) do
    services = Map.get(runtime, "services", %{})
    _ = cancel_in_flight_runs(runtime, opts)

    Enum.each(["web", "runner", "orchestrator"], fn service_name ->
      case get_in(services, [service_name, "pid"]) do
        pid when is_integer(pid) and pid > 0 -> :ok = DevProcess.stop_pid(pid)
        _ -> :ok
      end
    end)
  end

  defp stop_service_map(services) when is_map(services) do
    Enum.each(["web", "runner", "orchestrator"], fn service_name ->
      case Map.get(services, service_name) do
        %{pid: pid} when is_integer(pid) and pid > 0 -> :ok = DevProcess.stop_pid(pid)
        %{"pid" => pid} when is_integer(pid) and pid > 0 -> :ok = DevProcess.stop_pid(pid)
        _ -> :ok
      end
    end)
  end

  defp cancel_in_flight_runs(runtime, opts) do
    with {:ok, secrets} <- State.read_secrets(opts),
         token when is_binary(token) and token != "" <- secrets["service_token"],
         base_url when is_binary(base_url) <- runtime["orchestrator_base_url"],
         {:ok, run_ids} <- OrchestratorClient.in_flight_runs(base_url, token) do
      Enum.each(run_ids, fn run_id ->
        _ = OrchestratorClient.cancel_run(base_url, token, run_id)
      end)

      :ok
    else
      _ -> :ok
    end
  end

  defp print_start_summary(%{config: config, node_names: node_names, services: services}) do
    IO.puts("Favn local dev stack")
    IO.puts("storage: #{config.storage}")
    IO.puts("web: pid=#{services["web"].pid} url=#{config.web_base_url}")

    IO.puts(
      "orchestrator: pid=#{services["orchestrator"].pid} url=#{config.orchestrator_base_url}"
    )

    IO.puts("runner: pid=#{services["runner"].pid} node=#{node_names.runner_full}")
    IO.puts("logs: web=#{services["web"].log_path}")
    IO.puts("logs: orchestrator=#{services["orchestrator"].log_path}")
    IO.puts("logs: runner=#{services["runner"].log_path}")
  end

  defp with_lock(opts, fun) when is_function(fun, 0) do
    Lock.with_lock(opts, fun)
  end

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
end
