defmodule Favn.Dev.Stack do
  @moduledoc """
  Foreground local stack lifecycle implementation owned by `favn_local`.

  Lock scope is intentionally limited to short runtime-state mutation windows.
  The foreground wait loop never holds `.favn/lock`.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.Lock
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
      {:error, reason} = error ->
        _ = cleanup_after_failure(reason, opts)
        error
    end
  end

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
           config <- Config.resolve(opts),
           {:ok, secrets} <- Secrets.resolve(config, opts),
           {:ok, node_names} <- build_node_names(opts),
           {:ok, services} <- start_services(config, secrets, node_names, opts),
           :ok <- write_runtime(config, secrets, node_names, services, opts) do
        {:ok, %{config: config, secrets: secrets, node_names: node_names, services: services}}
      end
    end)
  end

  defp ensure_stack_not_running(opts) do
    case State.read_runtime(opts) do
      {:ok, runtime} ->
        if any_service_running?(runtime) do
          {:error, :stack_already_running}
        else
          State.clear_runtime(opts)
        end

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp any_service_running?(runtime) do
    runtime
    |> Map.get("services", %{})
    |> Map.values()
    |> Enum.any?(fn
      %{"pid" => pid} when is_integer(pid) and pid > 0 -> DevProcess.alive?(pid)
      _ -> false
    end)
  end

  defp build_node_names(opts) do
    root_dir = Paths.root_dir(opts)
    suffix = Integer.to_string(:erlang.phash2(root_dir, 1_000_000))

    {:ok,
     %{
       runner: "favn_runner_#{suffix}",
       orchestrator: "favn_orchestrator_#{suffix}",
       control: "favn_local_ctl_#{suffix}"
     }}
  end

  defp start_services(config, secrets, node_names, opts) do
    root_dir = Paths.root_dir(opts)

    specs =
      case Keyword.get(opts, :service_specs_override) do
        list when is_list(list) and list != [] ->
          list

        _ ->
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

  defp runner_spec(root_dir, node_names, secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    code = "Application.ensure_all_started(:favn_runner); Process.sleep(:infinity)"

    %{
      name: "runner",
      exec: elixir,
      args: [
        "--sname",
        node_names.runner,
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
      if storage == "sqlite" do
        Application.put_env(:favn_orchestrator, :storage_adapter, FavnStorageSqlite.Adapter)
        Application.put_env(:favn_orchestrator, :storage_adapter_opts, database: System.get_env("FAVN_DEV_SQLITE_PATH"))
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
        node_names.orchestrator,
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
        "FAVN_DEV_RUNNER_NODE" => node_names.runner,
        "FAVN_ORCHESTRATOR_API_ENABLED" =>
          if(config.orchestrator_api_enabled, do: "1", else: "0"),
        "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(config.orchestrator_port),
        "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => secrets["service_token"]
      }
    }
  end

  defp web_spec(root_dir, config, secrets) do
    bash = System.find_executable("bash") || "/bin/bash"
    npm = System.find_executable("npm") || "npm"
    web_cwd = Path.join(root_dir, "web/favn_web")

    command =
      "#{npm} run build --silent && #{npm} run preview -- --host 127.0.0.1 --port #{config.web_port}"

    %{
      name: "web",
      exec: bash,
      args: ["-lc", command],
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
      "node_names" => %{"runner" => node_names.runner, "orchestrator" => node_names.orchestrator},
      "services" =>
        Map.new(services, fn {name, info} ->
          service = %{"pid" => info.pid, "log_path" => info.log_path}

          service =
            case name do
              "runner" -> Map.put(service, "node_name", node_names.runner)
              "orchestrator" -> Map.put(service, "node_name", node_names.orchestrator)
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
         {:ok, build} <- Favn.build_manifest(),
         {:ok, version} <- Favn.pin_manifest_version(build.manifest),
         :ok <-
           RunnerControl.register_manifest(version,
             runner_node_name: node_names.runner,
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
    case System.cmd(
           "curl",
           ["-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "1", url],
           stderr_to_stdout: true
         ) do
      {code, 0} ->
        case Integer.parse(String.trim(code)) do
          {status, _rest} when status >= 200 and status < 500 -> true
          _ -> false
        end

      {_output, _status} ->
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

    IO.puts("runner: pid=#{services["runner"].pid} node=#{node_names.runner}")
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
