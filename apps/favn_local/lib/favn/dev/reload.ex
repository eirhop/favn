defmodule Favn.Dev.Reload do
  @moduledoc """
  Reload workflow for the running local stack.

  Reload semantics:
  - refuse when stack is not running
  - refuse when in-flight runs exist
  - recompile and rebuild manifest
  - restart runner and re-register manifest in live runner
  - publish and activate manifest in orchestrator without restarting orchestrator
  """

  alias Favn.Dev.Config
  alias Favn.Dev.DistributedErlang
  alias Favn.Dev.EnvFile
  alias Favn.Dev.LocalContext
  alias Favn.Dev.Lock
  alias Favn.Dev.NodeControl
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RunnerControl
  alias Favn.Dev.RuntimeLaunch
  alias Favn.Dev.RuntimeWorkspace
  alias Favn.Dev.Secrets
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @type reload_opt :: [root_dir: Path.t()]

  @spec run(reload_opt()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with {:ok, env_file} <- EnvFile.load(opts),
         opts <- Keyword.put(opts, :env_file_loaded, env_file.loaded),
         :ok <- ensure_running(opts),
         {:ok, runtime, installed_runtime, secrets} <- read_runtime_snapshot(opts),
         :ok <- ensure_no_in_flight_runs(runtime, secrets, opts),
         :ok <- compile_project(),
         {:ok, build} <- FavnAuthoring.build_manifest(),
         {:ok, version} <- FavnAuthoring.pin_manifest_version(build.manifest),
         {:ok, runtime_after_restart} <- restart_runner(runtime, installed_runtime, secrets, opts),
         {:ok, published} <- publish_manifest(version, runtime_after_restart, secrets, opts),
         canonical_manifest_version_id <- canonical_manifest_version_id(published, version),
         :ok <-
           register_manifest_in_runner(
             %{version | manifest_version_id: canonical_manifest_version_id},
             runtime_after_restart,
             secrets
           ),
         :ok <- write_manifest_cache(version, canonical_manifest_version_id, opts),
         {:ok, activated} <-
           activate_manifest(canonical_manifest_version_id, runtime_after_restart, secrets, opts),
         active_manifest_version_id <-
           canonical_manifest_version_id(activated, canonical_manifest_version_id),
         :ok <- update_runtime_manifest(active_manifest_version_id, opts) do
      IO.puts("Favn manifest reloaded: #{active_manifest_version_id}")
      :ok
    end
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      :stopped -> {:error, :stack_not_running}
      _ -> {:error, :stack_not_running}
    end
  end

  defp read_runtime_snapshot(opts) do
    Lock.with_lock(opts, fn ->
      with {:ok, runtime} <- State.read_runtime(opts),
           {:ok, installed_runtime} <- RuntimeWorkspace.read(opts),
           {:ok, secrets} <- Secrets.resolve(Config.resolve(opts), opts) do
        {:ok, runtime, installed_runtime, secrets}
      end
    end)
  end

  defp compile_project do
    Mix.Task.reenable("compile")

    case Mix.Task.run("compile", ["--force"]) do
      _ -> :ok
    end
  rescue
    error -> {:error, {:compile_failed, error}}
  end

  defp ensure_no_in_flight_runs(runtime, secrets, opts) do
    _ = secrets
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    case OrchestratorClient.in_flight_runs(base_url, "", LocalContext.session_context()) do
      {:ok, []} -> :ok
      {:ok, run_ids} -> {:error, {:in_flight_runs, run_ids}}
      {:error, _reason} = error -> error
    end
  end

  defp restart_runner(runtime, installed_runtime, secrets, opts) do
    runner_node = get_in(runtime, ["services", "runner", "node_name"])
    old_runner = Map.get(Map.get(runtime, "services", %{}), "runner", %{})
    old_runner_pid = Map.get(old_runner, "pid")
    old_generation = runner_generation(old_runner)
    new_generation = old_generation + 1

    marker = runner_replacement_marker(runtime, "stopping_old", new_generation)

    with :ok <- write_runner_replacement_marker(runtime, marker, opts) do
      if is_integer(old_runner_pid) and old_runner_pid > 0 do
        :ok = DevProcess.stop_pid(old_runner_pid)
      end

      node_names = %{runner_short: runner_sname(runner_node)}

      case DevProcess.start_service(
             RuntimeLaunch.runner_spec(installed_runtime, opts, node_names, secrets)
           ) do
        {:ok, info} ->
          started_marker =
            marker
            |> Map.put("status", "started")
            |> Map.put("new_pid", info.pid)
            |> Map.put("new_node", runner_node)
            |> Map.put("new_log_path", info.log_path)
            |> Map.put("updated_at", datetime(DateTime.utc_now()))

          runtime_started =
            runtime
            |> put_runner_runtime(info.pid, info.log_path, runner_node, new_generation)
            |> put_runner_replacement_marker(started_marker)

          with :ok <- write_runtime_snapshot(runtime_started, opts),
               :ok <- wait_runner_node_reachable(runner_node, secrets, opts) do
            completed_marker =
              started_marker
              |> Map.put("status", "completed")
              |> Map.put("updated_at", datetime(DateTime.utc_now()))

            runtime_completed = put_runner_replacement_marker(runtime_started, completed_marker)

            case write_runtime_snapshot(runtime_completed, opts) do
              :ok -> {:ok, runtime_completed}
              {:error, reason} -> {:error, reason}
            end
          else
            {:error, reason} ->
              _ = write_runner_replacement_failed(runtime_started, reason, opts)
              {:error, {:runner_restart_failed, reason}}
          end

        {:error, reason} ->
          _ =
            runtime
            |> put_runner_replacement_marker(marker)
            |> write_runner_replacement_failed(reason, opts)

          {:error, {:runner_restart_failed, reason}}
      end
    end
  end

  @doc false
  @spec runner_replacement_marker(map(), String.t(), pos_integer()) :: map()
  def runner_replacement_marker(runtime, status, generation)
      when is_map(runtime) and is_binary(status) and is_integer(generation) and generation > 0 do
    old_runner = Map.get(Map.get(runtime, "services", %{}), "runner", %{})
    now = datetime(DateTime.utc_now())

    %{
      "status" => status,
      "generation" => generation,
      "old_generation" => runner_generation(old_runner),
      "old_pid" => Map.get(old_runner, "pid"),
      "old_node" => Map.get(old_runner, "node_name"),
      "started_at" => now,
      "updated_at" => now
    }
  end

  defp write_runner_replacement_marker(runtime, marker, opts) do
    runtime
    |> put_runner_replacement_marker(marker)
    |> write_runtime_snapshot(opts)
  end

  defp write_runner_replacement_failed(runtime, reason, opts) do
    marker =
      runtime
      |> get_in(["reload", "runner_replacement"])
      |> case do
        marker when is_map(marker) -> marker
        _ -> runner_replacement_marker(runtime, "failed", runner_generation(runtime))
      end
      |> Map.put("status", "failed")
      |> Map.put("error", inspect(reason))
      |> Map.put("updated_at", datetime(DateTime.utc_now()))

    write_runner_replacement_marker(runtime, marker, opts)
  end

  defp write_runtime_snapshot(runtime, opts) do
    Lock.with_lock(opts, fn -> State.write_runtime(runtime, opts) end)
  end

  defp put_runner_runtime(runtime, pid, log_path, node_name, generation) do
    services = Map.get(runtime, "services", %{})
    runner = Map.get(services, "runner", %{})

    runner =
      runner
      |> Map.put("pid", pid)
      |> Map.put("log_path", log_path)
      |> Map.put("node_name", node_name)
      |> Map.put("generation", generation)

    Map.put(runtime, "services", Map.put(services, "runner", runner))
  end

  defp put_runner_replacement_marker(runtime, marker) do
    reload =
      runtime
      |> Map.get("reload", %{})
      |> Map.put("runner_replacement", marker)

    Map.put(runtime, "reload", reload)
  end

  defp runner_generation(%{"services" => %{"runner" => runner}}), do: runner_generation(runner)

  defp runner_generation(%{"generation" => generation}) when is_integer(generation),
    do: generation

  defp runner_generation(_runner), do: 1

  @doc false
  @spec wait_runner_node_reachable(String.t(), map(), keyword()) :: :ok | {:error, term()}
  def wait_runner_node_reachable(runner_node, secrets, opts \\ [])
      when is_binary(runner_node) and is_map(secrets) and is_list(opts) do
    timeout_ms = Keyword.get(opts, :runner_wait_timeout_ms, 10_000)

    with {:ok, rpc_cookie} <- fetch_rpc_cookie(secrets),
         :ok <- ensure_local_control_node_started(rpc_cookie, opts) do
      case Keyword.get(opts, :runner_node_wait_fun) do
        fun when is_function(fun, 2) ->
          fun.(runner_node, timeout_ms)

        _ ->
          with {:ok, node} <- DistributedErlang.node_name_to_atom(runner_node) do
            deadline = System.monotonic_time(:millisecond) + timeout_ms
            do_wait_runner_node_reachable(node, deadline)
          end
      end
    end
  end

  defp fetch_rpc_cookie(%{"rpc_cookie" => cookie}) when is_binary(cookie) and cookie != "",
    do: {:ok, cookie}

  defp fetch_rpc_cookie(_secrets), do: {:error, :missing_rpc_cookie}

  defp ensure_local_control_node_started(rpc_cookie, opts) do
    case Keyword.get(opts, :node_control_fun) do
      fun when is_function(fun, 1) -> fun.(rpc_cookie)
      _ -> NodeControl.ensure_local_node_started(rpc_cookie)
    end
  end

  defp do_wait_runner_node_reachable(node, deadline_ms) when is_atom(node) do
    case :net_adm.ping(node) do
      :pong ->
        :ok

      :pang ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, {:runner_node_unreachable, node}}
        else
          Process.sleep(100)
          do_wait_runner_node_reachable(node, deadline_ms)
        end
    end
  end

  @doc false
  @spec runner_sname(String.t()) :: String.t()
  def runner_sname(node_name) when is_binary(node_name) do
    node_name
    |> String.split("@", parts: 2)
    |> hd()
  end

  defp register_manifest_in_runner(version, runtime, secrets) do
    runner_node_name = get_in(runtime, ["services", "runner", "node_name"])

    RunnerControl.register_manifest(version,
      runner_node_name: runner_node_name,
      rpc_cookie: secrets["rpc_cookie"]
    )
  end

  defp write_manifest_cache(version, manifest_version_id, opts) do
    State.write_manifest_latest(
      %{
        "manifest_version_id" => manifest_version_id,
        "content_hash" => version.content_hash,
        "schema_version" => version.schema_version,
        "runner_contract_version" => version.runner_contract_version,
        "serialization_format" => version.serialization_format,
        "inserted_at" => datetime(version.inserted_at)
      },
      opts
    )
  end

  defp publish_manifest(version, runtime, secrets, opts) do
    _ = secrets
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    OrchestratorClient.publish_manifest(
      base_url,
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
    )
  end

  defp activate_manifest(manifest_version_id, runtime, secrets, opts) do
    _ = secrets
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    OrchestratorClient.activate_manifest(
      base_url,
      "",
      manifest_version_id,
      LocalContext.session_context()
    )
  end

  defp update_runtime_manifest(manifest_version_id, opts) do
    Lock.with_lock(opts, fn ->
      with {:ok, runtime} <- State.read_runtime(opts) do
        State.write_runtime(
          Map.put(runtime, "active_manifest_version_id", manifest_version_id),
          opts
        )
      end
    end)
  end

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)

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
end
