defmodule Favn.Dev.Stack do
  @moduledoc """
  Foreground local stack lifecycle implementation owned by `favn_local`.

  Lock scope is intentionally limited to short runtime-state mutation windows.
  The foreground wait loop never holds `.favn/lock`.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.DistributedErlang
  alias Favn.Dev.Install
  alias Favn.Dev.LocalContext
  alias Favn.Dev.LocalHttpClient
  alias Favn.Dev.Lock
  alias Favn.Dev.NodeControl
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Paths
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RunnerControl
  alias Favn.Dev.RuntimeLaunch
  alias Favn.Dev.RuntimeWorkspace
  alias Favn.Dev.Secrets
  alias Favn.Dev.State

  @runtime_schema_version 1
  @type root_opt :: [root_dir: Path.t()]

  @spec start_foreground(root_opt()) :: :ok | {:error, term()}
  def start_foreground(opts \\ []) when is_list(opts) do
    with {:ok, startup} <-
           progress_step(opts, "checking local state", fn -> prepare_startup(opts) end),
         :ok <-
           progress_step(opts, "compiling Favn runtime", fn ->
             compile_runtime_apps(startup.runtime, opts)
           end),
         :ok <- progress_step(opts, "compiling current project", fn -> compile_project(opts) end),
         {:ok, startup} <-
           progress_step(opts, "starting local services", fn ->
             initialize_stack(startup, opts)
           end) do
      case bootstrap_manifest(startup, opts) do
        :ok ->
          print_start_summary(startup)
          wait_foreground(startup, opts)

        {:error, reason} = error ->
          if cleanup_required?(reason) do
            _ = cleanup_after_failure(reason, opts, startup)
          end

          error
      end
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
  defp cleanup_required?({:invalid_node_name, _node_name}), do: false
  defp cleanup_required?({:invalid_rpc_cookie, _cookie}), do: false
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

  defp prepare_startup(opts) do
    with_lock(opts, fn ->
      with :ok <- State.ensure_layout(opts),
           :ok <- ensure_stack_not_running(opts),
           :ok <- ensure_install_ready(opts),
           config <- Config.resolve(opts),
           :ok <- ensure_startup_prerequisites(config, opts),
           {:ok, runtime} <- resolve_runtime(opts),
           {:ok, secrets} <- Secrets.resolve(config, opts),
           {:ok, node_names} <- build_node_names(secrets, opts) do
        {:ok, %{config: config, runtime: runtime, secrets: secrets, node_names: node_names}}
      end
    end)
  end

  defp initialize_stack(startup, opts) do
    %{config: config, runtime: runtime, secrets: secrets} = startup

    with_lock(opts, fn ->
      with :ok <- State.ensure_layout(opts),
           :ok <- ensure_stack_not_running(opts),
           {:ok, node_names} <- build_node_names(secrets, opts),
           {:ok, services} <- start_services(runtime, config, secrets, node_names, opts),
           :ok <- write_runtime(config, secrets, node_names, services, opts) do
        {:ok,
         %{
           config: config,
           runtime: runtime,
           secrets: secrets,
           node_names: node_names,
           distribution_ports: distribution_ports(opts),
           services: services
         }}
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

  defp resolve_runtime(opts) do
    case RuntimeWorkspace.read(opts) do
      {:ok, runtime} ->
        {:ok, runtime}

      {:error, :not_found} ->
        if Keyword.get(opts, :skip_install_check, false) do
          root_dir = Paths.root_dir(opts)

          {:ok,
           %{
             "materialized_root" => root_dir,
             "orchestrator_root" => root_dir,
             "runner_root" => root_dir,
             "web_root" => Path.join(root_dir, "web/favn_web")
           }}
        else
          {:error, :install_required}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_startup_prerequisites(config, opts) do
    case ensure_ports_available(config, opts) do
      :ok -> ensure_storage_ready(config)
      {:error, _reason} = error -> error
    end
  end

  defp ensure_ports_available(config, opts) do
    [
      {:orchestrator, config.orchestrator_port},
      {:web, config.web_port},
      {:runner_distribution, RuntimeLaunch.distribution_port(:runner, opts)},
      {:orchestrator_distribution, RuntimeLaunch.distribution_port(:orchestrator, opts)},
      {:control_distribution, RuntimeLaunch.distribution_port(:control, opts)}
    ]
    |> Enum.reduce_while(:ok, fn {service, port}, :ok ->
      case ensure_port_available(service, port) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ensure_port_available(service, port)
       when is_atom(service) and is_integer(port) and port > 0 do
    case :gen_tcp.listen(port, [
           :binary,
           {:active, false},
           {:reuseaddr, false},
           {:ip, {127, 0, 0, 1}}
         ]) do
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

  defp compile_runtime_apps(runtime, opts) when is_map(runtime) do
    cond do
      Keyword.get(opts, :skip_runtime_compile, false) ->
        :ok

      Keyword.has_key?(opts, :service_specs_override) ->
        :ok

      true ->
        runtime_root = runtime["materialized_root"]
        mix = System.find_executable("mix") || "mix"

        case System.cmd(mix, ["compile", "--force"],
               cd: runtime_root,
               env: %{"MIX_ENV" => "dev"},
               stderr_to_stdout: true
             ) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, {:runtime_compile_failed, :runtime_root, status, String.trim(output)}}
        end
    end
  end

  defp compile_project(opts) do
    if Keyword.get(opts, :skip_bootstrap, false) do
      :ok
    else
      Mix.Task.reenable("compile")

      case Mix.Task.run("compile", ["--force"]) do
        _ -> :ok
      end
    end
  rescue
    error -> {:error, {:compile_failed, error}}
  end

  defp build_node_names(secrets, opts) when is_map(secrets) do
    root_dir = Paths.root_dir(opts)
    suffix = Integer.to_string(:erlang.phash2(root_dir, 1_000_000))

    runner_short = "favn_runner_#{suffix}"
    orchestrator_short = "favn_orchestrator_#{suffix}"
    control_short = "favn_local_ctl_#{suffix}"

    with :ok <- maybe_start_local_node(secrets, control_short, opts),
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

  defp maybe_start_local_node(secrets, control_short, opts)
       when is_map(secrets) and is_binary(control_short) and is_list(opts) do
    case Keyword.get(opts, :service_specs_override) do
      list when is_list(list) and list != [] ->
        :ok

      _ ->
        NodeControl.ensure_local_node_started(secrets["rpc_cookie"],
          name: control_short,
          distribution_port: RuntimeLaunch.distribution_port(:control, opts)
        )
    end
  end

  defp start_services(runtime, config, secrets, node_names, opts) do
    case Keyword.get(opts, :service_specs_override) do
      list when is_list(list) and list != [] ->
        start_service_specs(list, %{}, opts)

      _ ->
        start_default_services(runtime, config, secrets, node_names, opts)
    end
  end

  defp start_default_services(runtime, config, secrets, node_names, opts) do
    with :ok <-
           progress_step(opts, "ensuring web assets", fn ->
             ensure_web_assets(runtime, opts)
           end) do
      runner_spec = RuntimeLaunch.runner_spec(runtime, opts, node_names, secrets)

      orchestrator_spec =
        RuntimeLaunch.orchestrator_spec(runtime, config, opts, node_names, secrets)

      web_spec = RuntimeLaunch.web_spec(runtime, config, opts, secrets)

      start_ordered_services(runner_spec, orchestrator_spec, web_spec, node_names, opts)
    end
  end

  defp start_ordered_services(runner_spec, orchestrator_spec, web_spec, node_names, opts) do
    runner_wait_timeout_ms = Keyword.get(opts, :runner_wait_timeout_ms, 15_000)
    runner_wait_node_name = Keyword.get(opts, :runner_wait_node_name, node_names.runner_full)

    case start_service_specs([runner_spec], %{}, opts) do
      {:ok, services} ->
        with :ok <-
               progress_step(opts, "waiting for runner node", fn ->
                 wait_runner_node_ready(runner_wait_node_name, runner_wait_timeout_ms)
               end),
             {:ok, services} <- start_service_specs([orchestrator_spec], services, opts),
             {:ok, services} <- start_service_specs([web_spec], services, opts) do
          {:ok, services}
        else
          {:error, _reason} = error ->
            stop_service_map(services)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp start_service_specs(specs, initial, opts)
       when is_list(specs) and is_map(initial) and is_list(opts) do
    Enum.reduce_while(specs, {:ok, initial}, fn spec, {:ok, acc} ->
      progress(opts, "starting #{spec.name}")

      case DevProcess.start_service(spec) do
        {:ok, info} ->
          {:cont, {:ok, Map.put(acc, info.name, info)}}

        {:error, reason} ->
          stop_service_map(acc)
          {:halt, {:error, {:start_failed, spec.name, reason}}}
      end
    end)
  end

  defp wait_runner_node_ready(runner_full, timeout_ms)
       when is_binary(runner_full) and is_integer(timeout_ms) and timeout_ms > 0 do
    with {:ok, runner_node} <- DistributedErlang.node_name_to_atom(runner_full) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms

      do_wait_runner_node_ready(runner_node, deadline)
    end
  end

  defp do_wait_runner_node_ready(runner_node, deadline_ms) when is_atom(runner_node) do
    case :net_adm.ping(runner_node) do
      :pong ->
        :ok

      :pang ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, {:runner_node_unreachable, runner_node}}
        else
          Process.sleep(100)
          do_wait_runner_node_ready(runner_node, deadline_ms)
        end
    end
  end

  defp ensure_web_assets(runtime, opts) do
    if Keyword.get(opts, :skip_web_build, false) do
      :ok
    else
      web_cwd = runtime["web_root"]
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

  defp write_runtime(config, _secrets, node_names, services, opts) do
    runtime = %{
      "schema_version" => @runtime_schema_version,
      "owner_app" => "favn_local",
      "started_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "storage" => Atom.to_string(config.storage),
      "scheduler" => if(config.scheduler_enabled, do: "enabled", else: "disabled"),
      "orchestrator_base_url" => config.orchestrator_base_url,
      "web_base_url" => config.web_base_url,
      "rpc_cookie_ref" => "secrets.rpc_cookie",
      "node_names" => %{
        "runner" => node_names.runner_full,
        "orchestrator" => node_names.orchestrator_full,
        "control" => node_names.control_full
      },
      "distribution_ports" => %{
        "runner" => distribution_ports(opts).runner,
        "orchestrator" => distribution_ports(opts).orchestrator,
        "control" => distribution_ports(opts).control
      },
      "services" =>
        Map.new(services, fn {name, info} ->
          service = %{"pid" => info.pid, "log_path" => info.log_path}

          service =
            case name do
              "runner" ->
                service
                |> Map.put("node_name", node_names.runner_full)
                |> Map.put("distribution_port", distribution_ports(opts).runner)
                |> Map.put("generation", 1)

              "orchestrator" ->
                service
                |> Map.put("node_name", node_names.orchestrator_full)
                |> Map.put("distribution_port", distribution_ports(opts).orchestrator)

              _ ->
                service
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
    %{config: config} = startup

    progress(opts, "building manifest")

    with {:ok, build} <- FavnAuthoring.build_manifest(),
         {:ok, version} <- FavnAuthoring.pin_manifest_version(build.manifest),
         :ok <-
           progress_step(opts, "waiting for orchestrator API", fn ->
             wait_orchestrator_health(config.orchestrator_base_url, 15_000)
           end),
         _ <- progress(opts, "publishing manifest"),
         {:ok, published} <-
           OrchestratorClient.publish_manifest(
             config.orchestrator_base_url,
             "",
             %{
               manifest_version_id: version.manifest_version_id,
               content_hash: version.content_hash,
               schema_version: version.schema_version,
               runner_contract_version: version.runner_contract_version,
               serialization_format: version.serialization_format,
               manifest: version.manifest
             },
             LocalContext.session_context()
           ),
         canonical_manifest_version_id <- canonical_manifest_version_id(published, version),
         :ok <- register_canonical_manifest(version, canonical_manifest_version_id, startup),
         :ok <-
           State.write_manifest_latest(
             manifest_cache(version, canonical_manifest_version_id),
             opts
           ),
         _ <- progress(opts, "activating manifest"),
         {:ok, activated} <-
           OrchestratorClient.activate_manifest(
             config.orchestrator_base_url,
             "",
             canonical_manifest_version_id,
             LocalContext.session_context()
           ) do
      active_manifest_version_id =
        canonical_manifest_version_id(activated, canonical_manifest_version_id)

      with_lock(opts, fn ->
        with {:ok, runtime} <- State.read_runtime(opts) do
          State.write_runtime(
            Map.put(runtime, "active_manifest_version_id", active_manifest_version_id),
            opts
          )
        end
      end)

      :ok
    end
  end

  defp manifest_cache(version, manifest_version_id) do
    %{
      "manifest_version_id" => manifest_version_id,
      "content_hash" => version.content_hash,
      "schema_version" => version.schema_version,
      "runner_contract_version" => version.runner_contract_version,
      "serialization_format" => version.serialization_format,
      "inserted_at" => datetime(version.inserted_at)
    }
  end

  defp register_canonical_manifest(version, manifest_version_id, startup) do
    %{secrets: secrets, node_names: node_names} = startup

    RunnerControl.register_manifest(%{version | manifest_version_id: manifest_version_id},
      runner_node_name: node_names.runner_full,
      rpc_cookie: secrets["rpc_cookie"]
    )
  end

  defp canonical_manifest_version_id(%{"data" => data}, fallback) when is_map(data) do
    get_in(data, ["registration", "canonical_manifest_version_id"]) ||
      get_in(data, ["manifest", "manifest_version_id"]) ||
      Map.get(data, "manifest_version_id") || canonical_manifest_version_id(fallback)
  end

  defp canonical_manifest_version_id(%{"manifest_version_id" => manifest_version_id}, _fallback),
    do: manifest_version_id

  defp canonical_manifest_version_id(_response, fallback),
    do: canonical_manifest_version_id(fallback)

  defp canonical_manifest_version_id(%{manifest_version_id: manifest_version_id}),
    do: manifest_version_id

  defp canonical_manifest_version_id(manifest_version_id) when is_binary(manifest_version_id),
    do: manifest_version_id

  defp wait_foreground(startup, opts) do
    %{config: config, services: services} = startup

    case wait_ready(config, opts) do
      :ok ->
        monitor_services(services)
        wait_for_service_exit(startup, opts)

      {:error, reason} ->
        cleanup_after_failure(reason, opts, startup)
        {:error, reason}
    end
  end

  defp wait_ready(config, opts) do
    if Keyword.get(opts, :skip_readiness, false) do
      :ok
    else
      case progress_step(opts, "checking orchestrator readiness", fn ->
             wait_orchestrator_health(config.orchestrator_base_url, 15_000)
           end) do
        :ok ->
          progress_step(opts, "checking web readiness", fn ->
            wait_web_http(config.web_base_url, 30_000)
          end)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp progress_step(opts, message, fun) when is_list(opts) and is_function(fun, 0) do
    progress(opts, message)
    fun.()
  end

  defp progress(opts, message) when is_list(opts) and is_binary(message) do
    case Keyword.get(opts, :progress_fun) do
      fun when is_function(fun, 1) -> fun.("Favn dev: " <> message)
      _other -> :ok
    end
  end

  defp wait_orchestrator_health(url, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_orchestrator_health(url, deadline)
  end

  defp do_wait_orchestrator_health(url, deadline_ms) do
    case OrchestratorClient.health(url) do
      :ok ->
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, {:not_ready, url, reason}}
        else
          Process.sleep(200)
          do_wait_orchestrator_health(url, deadline_ms)
        end
    end
  end

  defp wait_web_http(url, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_web_http(url, deadline)
  end

  defp do_wait_web_http(url, deadline_ms) do
    case LocalHttpClient.request(:get, url, [], nil, connect_timeout_ms: 1_000, timeout_ms: 2_000) do
      {:ok, _decoded} ->
        :ok

      {:error, {:invalid_json, _body}} ->
        :ok

      {:error, {:http_error, status, _body}} when status in 300..399 ->
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, {:not_ready, url, reason}}
        else
          Process.sleep(200)
          do_wait_web_http(url, deadline_ms)
        end
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

        if intentional_runner_replacement_exit?(service, status, startup, opts) do
          wait_for_service_exit(startup, opts)
        else
          cleanup_after_failure(reason, opts, startup)
          {:error, reason}
        end

      {:DOWN, _ref, :process, _pid, reason} ->
        failure = {:service_down, reason}
        cleanup_after_failure(failure, opts, startup)
        {:error, failure}
    after
      1_000 ->
        case poll_replacement_runner(opts) do
          :ok ->
            wait_for_service_exit(startup, opts)

          {:error, reason} ->
            cleanup_after_failure(reason, opts, startup)
            {:error, reason}
        end
    end
  end

  @doc false
  @spec runner_replacement_exit?(map(), map(), term()) :: boolean()
  def runner_replacement_exit?(startup_runner, marker, status)
      when is_map(startup_runner) and is_map(marker) do
    marker_status = Map.get(marker, "status")

    status == 0 and marker_status in ["stopping_old", "started", "completed"] and
      Map.get(marker, "old_pid") == Map.get(startup_runner, :pid) and
      Map.get(marker, "old_generation") == runner_generation(startup_runner)
  end

  def runner_replacement_exit?(_startup_runner, _marker, _status), do: false

  defp intentional_runner_replacement_exit?("runner", status, startup, opts) do
    with {:ok, runtime} <- with_lock(opts, fn -> State.read_runtime(opts) end),
         marker when is_map(marker) <- get_in(runtime, ["reload", "runner_replacement"]),
         startup_runner when is_map(startup_runner) <- get_in(startup, [:services, "runner"]) do
      runner_replacement_exit?(startup_runner, marker, status)
    else
      _ -> false
    end
  end

  defp intentional_runner_replacement_exit?(_service, _status, _startup, _opts), do: false

  defp poll_replacement_runner(opts) do
    case with_lock(opts, fn -> State.read_runtime(opts) end) do
      {:ok, runtime} ->
        runner_replacement_monitor_status(runtime)

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @spec runner_replacement_monitor_status(map(), (integer() -> boolean())) ::
          :ok | {:error, term()}
  def runner_replacement_monitor_status(runtime, alive? \\ &DevProcess.alive?/1)
      when is_map(runtime) do
    marker = get_in(runtime, ["reload", "runner_replacement"])

    cond do
      not is_map(marker) ->
        :ok

      Map.get(marker, "status") in ["stopping_old", "started"] ->
        :ok

      Map.get(marker, "status") == "completed" ->
        runner_pid = get_in(runtime, ["services", "runner", "pid"])

        if is_integer(runner_pid) and runner_pid > 0 and alive?.(runner_pid) do
          :ok
        else
          {:error, {:service_exit, "runner", :replacement_not_running}}
        end

      Map.get(marker, "status") == "failed" ->
        {:error, {:runner_replacement_failed, Map.get(marker, "error")}}

      true ->
        :ok
    end
  end

  defp runner_generation(%{generation: generation}) when is_integer(generation), do: generation

  defp runner_generation(%{"generation" => generation}) when is_integer(generation),
    do: generation

  defp runner_generation(_runner), do: 1

  defp cleanup_after_failure(reason, opts, startup \\ nil) do
    runtime =
      case with_lock(opts, fn -> State.read_runtime(opts) end) do
        {:ok, value} -> value
        _ -> %{}
      end

    _ =
      with_lock(opts, fn ->
        State.write_last_failure(
          failure_payload(reason, startup, runtime),
          opts
        )
      end)

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

  defp failure_payload(reason, startup, runtime) do
    %{"error" => inspect(reason), "at" => DateTime.utc_now() |> DateTime.to_iso8601()}
    |> maybe_put_failure_details(reason)
    |> maybe_put_log_paths(startup, runtime)
  end

  defp maybe_put_failure_details(payload, %{
         operation: operation,
         method: method,
         url: url,
         reason: reason
       }) do
    Map.merge(payload, %{
      "operation" => Atom.to_string(operation),
      "method" => method |> Atom.to_string() |> String.upcase(),
      "url" => url,
      "reason" => inspect(reason)
    })
  end

  defp maybe_put_failure_details(payload, {:not_ready, url, reason}) do
    payload
    |> Map.put("operation", "health_check")
    |> Map.put("method", "GET")
    |> Map.put("url", url)
    |> Map.put("reason", inspect(reason))
  end

  defp maybe_put_failure_details(payload, _reason), do: payload

  defp maybe_put_log_paths(payload, startup, runtime) do
    log_paths =
      startup_log_paths(startup)
      |> Map.merge(runtime_log_paths(runtime))

    if map_size(log_paths) == 0, do: payload, else: Map.put(payload, "log_paths", log_paths)
  end

  defp startup_log_paths(%{services: services}) when is_map(services) do
    log_paths =
      services
      |> Enum.flat_map(fn {name, info} ->
        case Map.get(info, :log_path) do
          path when is_binary(path) -> [{name, path}]
          _ -> []
        end
      end)
      |> Map.new()

    log_paths
  end

  defp startup_log_paths(_startup), do: %{}

  defp runtime_log_paths(runtime) when is_map(runtime) do
    runtime
    |> Map.get("services", %{})
    |> Enum.flat_map(fn {name, service} ->
      case Map.get(service, "log_path") do
        path when is_binary(path) -> [{name, path}]
        _ -> []
      end
    end)
    |> Map.new()
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

  defp cancel_in_flight_runs(runtime, _opts) do
    context = LocalContext.session_context()

    with base_url when is_binary(base_url) <- runtime["orchestrator_base_url"],
         {:ok, run_ids} <- OrchestratorClient.in_flight_runs(base_url, "", context) do
      Enum.each(run_ids, fn run_id ->
        _ = OrchestratorClient.cancel_run(base_url, "", run_id, context)
      end)

      :ok
    else
      _ -> :ok
    end
  end

  defp print_start_summary(%{
         config: config,
         node_names: node_names,
         distribution_ports: distribution_ports,
         services: services
       }) do
    IO.puts("Favn local dev stack")
    IO.puts("storage: #{config.storage}")
    IO.puts("scheduler: #{if(config.scheduler_enabled, do: "enabled", else: "disabled")}")
    IO.puts("local URLs:")
    IO.puts("web: pid=#{services["web"].pid} url=#{config.web_base_url}")

    IO.puts(
      "orchestrator API: pid=#{services["orchestrator"].pid} url=#{config.orchestrator_base_url}"
    )

    IO.puts("internal control plane:")
    IO.puts("runner node: pid=#{services["runner"].pid} node=#{node_names.runner_full}")

    IO.puts(
      "orchestrator node: pid=#{services["orchestrator"].pid} node=#{node_names.orchestrator_full}"
    )

    IO.puts(
      "control node: node=#{node_names.control_full} distribution_port=#{distribution_ports.control}"
    )

    IO.puts("logs: web=#{services["web"].log_path}")
    IO.puts("logs: orchestrator=#{services["orchestrator"].log_path}")
    IO.puts("logs: runner=#{services["runner"].log_path}")
  end

  defp with_lock(opts, fun) when is_function(fun, 0) do
    Lock.with_lock(opts, fun)
  end

  defp distribution_ports(opts) do
    %{
      runner: RuntimeLaunch.distribution_port(:runner, opts),
      orchestrator: RuntimeLaunch.distribution_port(:orchestrator, opts),
      control: RuntimeLaunch.distribution_port(:control, opts)
    }
  end

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
end
