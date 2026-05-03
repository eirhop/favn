defmodule Favn.SingleNodeArtifactRuntimeTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 600_000

  @repo_root Path.expand("../../../..", __DIR__)
  @service_token "favnweb-runtime-credential-alpha-1234567890"

  test "generated single-node artifact start stop runtime contract" do
    ensure_executable!("curl")
    ensure_executable!("env")

    project_dir = fixture_project!()
    runtime_home = Path.join(project_dir, "runtime-home")
    sqlite_path = Path.join(project_dir, "data/control-plane.sqlite3")
    port = free_port()
    File.mkdir_p!(Path.dirname(sqlite_path))

    on_exit(fn -> File.rm_rf(project_dir) end)

    run_mix!(project_dir, ["deps.get"])

    run_mix!(project_dir, [
      "favn.install",
      "--skip-web-install"
    ])

    {build_output, 0} = run_mix!(project_dir, ["favn.build.single"])
    dist_dir = dist_dir_from_output!(build_output)

    assert File.exists?(Path.join(dist_dir, "metadata.json"))
    assert File.exists?(Path.join(dist_dir, "config/assembly.json"))
    assert executable?(Path.join(dist_dir, "bin/start"))
    assert executable?(Path.join(dist_dir, "bin/stop"))
    assert_no_dev_env!(dist_dir)

    env = runtime_env(runtime_home, sqlite_path, port)

    on_exit(fn -> stop_artifact(dist_dir, env) end)

    {start_output, start_status} = start_artifact(dist_dir, env)
    assert start_status == 0, start_failure_message(start_output, runtime_home)
    assert start_output =~ "Favn backend started with PID"

    assert {:ok, live} = poll_json(live_url(port))
    assert live["status"] == "ok"

    assert {:ok, ready} = poll_json(ready_url(port))
    assert ready["status"] == "ready"
    assert_ready_check!(ready, "api")
    assert_ready_check!(ready, "storage")
    assert_ready_check!(ready, "scheduler")
    assert_ready_check!(ready, "runner")

    assert_runtime_paths!(runtime_home, sqlite_path)
    original_pid = read_pid!(runtime_home)
    assert process_running?(original_pid)

    assert {duplicate_output, duplicate_status} = start_artifact(dist_dir, env)
    assert duplicate_status != 0
    assert duplicate_output =~ "already running"
    assert read_pid!(runtime_home) == original_pid
    assert {:ok, %{"status" => "ready"}} = poll_json(ready_url(port))

    assert {stop_output, 0} = stop_artifact(dist_dir, env)
    assert stop_output =~ "Favn backend stopped"
    refute File.exists?(pid_path(runtime_home))

    {restart_output, restart_status} = start_artifact(dist_dir, env)
    assert restart_status == 0, start_failure_message(restart_output, runtime_home)
    assert restart_output =~ "Favn backend started with PID"
    assert {:ok, %{"status" => "ready"}} = poll_json(ready_url(port))
    assert File.exists?(sqlite_path)

    assert {stop_output, 0} = stop_artifact(dist_dir, env)
    assert stop_output =~ "Favn backend stopped"

    assert_stop_idempotency!(dist_dir, Path.join(project_dir, "stop-runtime"))
    assert_invalid_configs_fail_before_serving!(dist_dir, project_dir)
  end

  defp fixture_project! do
    project_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_single_artifact_runtime_#{System.unique_integer([:positive])}"
      )

    lib_dir = Path.join(project_dir, "lib/favn_runtime_fixture")
    config_dir = Path.join(project_dir, "config")

    File.mkdir_p!(lib_dir)
    File.mkdir_p!(config_dir)

    File.write!(Path.join(project_dir, "mix.exs"), mix_exs())
    File.write!(Path.join(config_dir, "config.exs"), config_exs())
    File.write!(Path.join(lib_dir, "ping.ex"), ping_asset_ex())
    File.write!(Path.join(project_dir, "mix.lock"), "%{}\n")

    project_dir
  end

  defp mix_exs do
    """
    defmodule FavnRuntimeFixture.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_runtime_fixture,
          version: "0.1.0",
          elixir: "~> 1.19",
          start_permanent: Mix.env() == :prod,
          deps: deps()
        ]
      end

      def application do
        [extra_applications: [:logger]]
      end

      defp deps do
        [
          {:favn, path: #{inspect(Path.join(@repo_root, "apps/favn"))}}
        ]
      end
    end
    """
  end

  defp config_exs do
    """
    import Config

    config :favn,
      asset_modules: [FavnRuntimeFixture.Ping],
      pipeline_modules: [],
      schedule_modules: []
    """
  end

  defp ping_asset_ex do
    """
    defmodule FavnRuntimeFixture.Ping do
      use Favn.Asset

      def asset(_ctx), do: :ok
    end
    """
  end

  defp run_mix!(project_dir, args) do
    mix = System.find_executable("mix") || "mix"

    case System.cmd(mix, args,
           cd: project_dir,
           stderr_to_stdout: true,
           env: %{"MIX_ENV" => "prod"}
         ) do
      {output, 0 = status} ->
        {output, status}

      {output, status} ->
        flunk("mix #{Enum.join(args, " ")} failed (status=#{status}):\n#{output}")
    end
  end

  defp dist_dir_from_output!(output) do
    case Regex.run(~r/^dist: (.+)$/m, output) do
      [_line, dist_dir] -> String.trim(dist_dir)
      nil -> flunk("mix favn.build.single output did not include dist path:\n#{output}")
    end
  end

  defp runtime_env(runtime_home, sqlite_path, port) do
    %{
      "FAVN_STORAGE" => "sqlite",
      "FAVN_SQLITE_PATH" => sqlite_path,
      "FAVN_SQLITE_MIGRATION_MODE" => "auto",
      "FAVN_SQLITE_BUSY_TIMEOUT_MS" => "5000",
      "FAVN_SQLITE_POOL_SIZE" => "1",
      "FAVN_ORCHESTRATOR_API_BIND_HOST" => "127.0.0.1",
      "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(port),
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "favn_web:#{@service_token}",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" => @service_token,
      "FAVN_SCHEDULER_ENABLED" => "true",
      "FAVN_SCHEDULER_TICK_MS" => "1000",
      "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES" => "1000",
      "FAVN_RUNNER_MODE" => "local",
      "FAVN_SINGLE_NODE_HOME" => runtime_home,
      "FAVN_STARTUP_TIMEOUT_SECONDS" => "180",
      "FAVN_STOP_TIMEOUT_SECONDS" => "10",
      "MIX_ENV" => "prod"
    }
  end

  defp start_artifact(dist_dir, env), do: run_script(Path.join(dist_dir, "bin/start"), env)
  defp stop_artifact(dist_dir, env), do: run_script(Path.join(dist_dir, "bin/stop"), env)

  defp run_script(script, env) do
    env_exec = System.find_executable("env") || "env"
    env_args = ["-i" | isolated_env_args(env)] ++ [script]

    System.cmd(env_exec, env_args, stderr_to_stdout: true)
  end

  defp isolated_env_args(env) do
    inherited =
      ["PATH", "HOME", "MIX_HOME", "HEX_HOME", "REBAR_CACHE_DIR", "LANG", "LC_ALL"]
      |> Enum.flat_map(fn name ->
        case System.get_env(name) do
          nil -> []
          value -> ["#{name}=#{value}"]
        end
      end)

    configured =
      env
      |> Enum.sort_by(fn {name, _value} -> name end)
      |> Enum.map(fn {name, value} -> "#{name}=#{value}" end)

    inherited ++ configured
  end

  defp assert_runtime_paths!(runtime_home, sqlite_path) do
    assert File.exists?(Path.join(runtime_home, "run/backend.pid"))
    assert File.exists?(Path.join(runtime_home, "run/backend_boot.exs"))
    assert File.exists?(Path.join(runtime_home, "log/backend.log"))
    assert File.dir?(Path.join(runtime_home, "data"))
    assert File.exists?(sqlite_path)
  end

  defp start_failure_message(output, runtime_home) do
    log_path = Path.join(runtime_home, "log/backend.log")
    log = if File.exists?(log_path), do: File.read!(log_path), else: "<missing>"

    "generated bin/start failed:\n#{output}\nbackend log:\n#{log}"
  end

  defp assert_stop_idempotency!(dist_dir, runtime_home) do
    env = %{"FAVN_SINGLE_NODE_HOME" => runtime_home, "FAVN_STOP_TIMEOUT_SECONDS" => "2"}

    assert {missing_output, 0} = stop_artifact(dist_dir, env)
    assert missing_output =~ "Favn backend is not running"

    File.mkdir_p!(Path.join(runtime_home, "run"))
    File.write!(pid_path(runtime_home), "not-a-pid\n")
    assert {invalid_output, 0} = stop_artifact(dist_dir, env)
    assert invalid_output =~ "Removed stale Favn backend PID file"
    refute File.exists?(pid_path(runtime_home))

    File.write!(pid_path(runtime_home), "999999\n")
    assert {dead_output, 0} = stop_artifact(dist_dir, env)
    assert dead_output =~ "Removed stale Favn backend PID file"
    refute File.exists?(pid_path(runtime_home))
  end

  defp assert_invalid_configs_fail_before_serving!(dist_dir, project_dir) do
    cases = [
      {"missing storage", &Map.delete(&1, "FAVN_STORAGE")},
      {"relative sqlite path", &Map.put(&1, "FAVN_SQLITE_PATH", "relative.sqlite3")},
      {"unsupported postgres storage", &Map.put(&1, "FAVN_STORAGE", "postgres")},
      {"short service token",
       &Map.put(&1, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "favn_web:short")}
    ]

    Enum.each(cases, fn {name, mutate} ->
      runtime_home = Path.join(project_dir, "invalid-runtime/#{String.replace(name, " ", "-")}")
      sqlite_path = Path.join(project_dir, "invalid-data/#{String.replace(name, " ", "-")}.sqlite3")
      port = free_port()

      env =
        runtime_env(runtime_home, sqlite_path, port)
        |> Map.put("FAVN_STARTUP_TIMEOUT_SECONDS", "5")
        |> mutate.()

      {output, status} = start_artifact(dist_dir, env)
      assert status != 0, "#{name} unexpectedly started:\n#{output}"
      assert output =~ "Favn backend exited before readiness"
      refute File.exists?(pid_path(runtime_home))
      assert {:error, _reason} = fetch_json(live_url(port))

      log = File.read!(Path.join(runtime_home, "log/backend.log"))
      assert log =~ "invalid Favn backend production runtime config or startup"
    end)
  end

  defp poll_json(url, attempts \\ 120)
  defp poll_json(url, 0), do: fetch_json(url)

  defp poll_json(url, attempts) do
    case fetch_json(url) do
      {:ok, decoded} ->
        {:ok, decoded}

      {:error, _reason} ->
        Process.sleep(250)
        poll_json(url, attempts - 1)
    end
  end

  defp fetch_json(url) do
    curl = System.find_executable("curl") || "curl"

    case System.cmd(curl, ["-fsS", "--max-time", "1", url], stderr_to_stdout: true) do
      {body, 0} -> decode_data(body)
      {output, status} -> {:error, {:curl_failed, status, output}}
    end
  end

  defp decode_data(body) do
    with {:ok, %{"data" => data}} <- JSON.decode(body) do
      {:ok, data}
    end
  end

  defp assert_ready_check!(ready, name) do
    checks = Map.fetch!(ready, "checks")
    assert %{"status" => "ok"} = Enum.find(checks, &(&1["name"] == name))
  end

  defp assert_no_dev_env!(dist_dir) do
    [
      "metadata.json",
      "env/backend.env.example",
      "bin/start",
      "bin/stop"
    ]
    |> Enum.each(fn relative ->
      contents = File.read!(Path.join(dist_dir, relative))
      refute contents =~ "FAVN_DEV_"
    end)
  end

  defp read_pid!(runtime_home) do
    runtime_home
    |> pid_path()
    |> File.read!()
    |> String.trim()
    |> String.to_integer()
  end

  defp process_running?(pid) when is_integer(pid) do
    case System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _other -> false
    end
  end

  defp pid_path(runtime_home), do: Path.join(runtime_home, "run/backend.pid")

  defp live_url(port), do: "http://127.0.0.1:#{port}/api/orchestrator/v1/health/live"
  defp ready_url(port), do: "http://127.0.0.1:#{port}/api/orchestrator/v1/health/ready"

  defp executable?(path) do
    case File.stat(path) do
      {:ok, %{mode: mode}} -> Bitwise.band(mode, 0o111) != 0
      _other -> false
    end
  end

  defp ensure_executable!(name) do
    unless System.find_executable(name) do
      flunk("#{name} is required for the single-node artifact runtime smoke test")
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
