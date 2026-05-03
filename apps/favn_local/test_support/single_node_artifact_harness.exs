defmodule Favn.Local.SingleNodeArtifactHarness do
  @moduledoc false

  import ExUnit.Assertions

  @repo_root Path.expand("../../..", __DIR__)

  def fixture_project!(prefix \\ "favn_single_node_artifact") do
    project_dir =
      Path.join(
        System.tmp_dir!(),
        "#{prefix}_#{System.unique_integer([:positive])}"
      )

    lib_dir = Path.join(project_dir, "lib/favn_single_node_fixture")
    config_dir = Path.join(project_dir, "config")

    File.mkdir_p!(lib_dir)
    File.mkdir_p!(config_dir)

    File.write!(Path.join(project_dir, "mix.exs"), mix_exs())
    File.write!(Path.join(config_dir, "config.exs"), config_exs())
    File.write!(Path.join(lib_dir, "ping.ex"), ping_asset_ex())
    File.write!(Path.join(project_dir, "mix.lock"), "%{}\n")

    project_dir
  end

  def run_mix!(project_dir, args) do
    case run_mix(project_dir, args) do
      {output, 0 = status} ->
        {output, status}

      {output, status} ->
        flunk("mix #{Enum.join(args, " ")} failed (status=#{status}):\n#{output}")
    end
  end

  def run_mix(project_dir, args) do
    mix = System.find_executable("mix") || "mix"

    System.cmd(mix, args,
      cd: project_dir,
      stderr_to_stdout: true,
      env: %{"MIX_ENV" => "prod"}
    )
  end

  def dist_dir_from_output!(output) do
    case Regex.run(~r/^dist: (.+)$/m, output) do
      [_line, dist_dir] -> String.trim(dist_dir)
      nil -> flunk("mix favn.build.single output did not include dist path:\n#{output}")
    end
  end

  def runtime_env(runtime_home, sqlite_path, port, service_token, extra_env \\ %{}) do
    %{
      "FAVN_STORAGE" => "sqlite",
      "FAVN_SQLITE_PATH" => sqlite_path,
      "FAVN_SQLITE_MIGRATION_MODE" => "auto",
      "FAVN_SQLITE_BUSY_TIMEOUT_MS" => "5000",
      "FAVN_SQLITE_POOL_SIZE" => "1",
      "FAVN_ORCHESTRATOR_API_BIND_HOST" => "127.0.0.1",
      "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(port),
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "favn_web:#{service_token}",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" => service_token,
      "FAVN_SCHEDULER_ENABLED" => "true",
      "FAVN_SCHEDULER_TICK_MS" => "1000",
      "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES" => "1000",
      "FAVN_RUNNER_MODE" => "local",
      "FAVN_SINGLE_NODE_HOME" => runtime_home,
      "FAVN_STARTUP_TIMEOUT_SECONDS" => "180",
      "FAVN_STOP_TIMEOUT_SECONDS" => "10",
      "MIX_ENV" => "prod"
    }
    |> Map.merge(extra_env)
  end

  def start_artifact(dist_dir, env), do: run_script(Path.join(dist_dir, "bin/start"), env)
  def stop_artifact(dist_dir, env), do: run_script(Path.join(dist_dir, "bin/stop"), env)

  def run_script(script, env) do
    env_exec = System.find_executable("env") || "env"
    env_args = ["-i" | isolated_env_args(env)] ++ [script]

    System.cmd(env_exec, env_args, stderr_to_stdout: true)
  end

  def assert_runtime_paths!(runtime_home, sqlite_path) do
    assert File.exists?(Path.join(runtime_home, "run/backend.pid"))
    assert File.exists?(Path.join(runtime_home, "run/backend_boot.exs"))
    assert File.exists?(Path.join(runtime_home, "log/backend.log"))
    assert File.dir?(Path.join(runtime_home, "data"))
    assert File.exists?(sqlite_path)
  end

  def assert_ready_check!(ready, name) do
    checks = Map.fetch!(ready, "checks")
    assert %{"status" => "ok"} = Enum.find(checks, &(&1["name"] == name))
  end

  def assert_no_dev_env!(dist_dir) do
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

  def start_failure_message(output, runtime_home) do
    "generated bin/start failed:\n#{output}\nbackend log:\n#{backend_log(runtime_home)}"
  end

  def backend_log(runtime_home) do
    log_path = Path.join(runtime_home, "log/backend.log")

    if File.exists?(log_path), do: File.read!(log_path), else: "<missing>"
  end

  def read_pid!(runtime_home) do
    runtime_home
    |> pid_path()
    |> File.read!()
    |> String.trim()
    |> String.to_integer()
  end

  def process_running?(pid) when is_integer(pid) do
    case System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _other -> false
    end
  end

  def pid_path(runtime_home), do: Path.join(runtime_home, "run/backend.pid")

  def poll_json(url, attempts \\ 120)
  def poll_json(url, 0), do: fetch_json(url)

  def poll_json(url, attempts) do
    case fetch_json(url) do
      {:ok, decoded} ->
        {:ok, decoded}

      {:error, _reason} ->
        Process.sleep(250)
        poll_json(url, attempts - 1)
    end
  end

  def fetch_json(url) do
    curl = System.find_executable("curl") || "curl"

    case System.cmd(curl, ["-fsS", "--max-time", "1", url], stderr_to_stdout: true) do
      {body, 0} -> decode_data(body)
      {output, status} -> {:error, {:curl_failed, status, output}}
    end
  end

  def live_url(port), do: "http://127.0.0.1:#{port}/api/orchestrator/v1/health/live"
  def ready_url(port), do: "http://127.0.0.1:#{port}/api/orchestrator/v1/health/ready"

  def executable?(path) do
    case File.stat(path) do
      {:ok, %{mode: mode}} -> Bitwise.band(mode, 0o111) != 0
      _other -> false
    end
  end

  def ensure_executable!(name) do
    unless System.find_executable(name) do
      flunk("#{name} is required for the single-node artifact integration tests")
    end
  end

  def free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
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

  defp decode_data(body) do
    with {:ok, %{"data" => data}} <- JSON.decode(body) do
      {:ok, data}
    end
  end

  defp mix_exs do
    """
    defmodule FavnSingleNodeFixture.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_single_node_fixture,
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
      asset_modules: [FavnSingleNodeFixture.Ping],
      pipeline_modules: [],
      schedule_modules: []
    """
  end

  defp ping_asset_ex do
    """
    defmodule FavnSingleNodeFixture.Ping do
      use Favn.Asset

      def asset(_ctx), do: :ok
    end
    """
  end
end
