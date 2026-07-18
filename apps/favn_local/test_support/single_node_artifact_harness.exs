defmodule Favn.Local.SingleNodeArtifactHarness do
  @moduledoc false

  import ExUnit.Assertions

  @shared_artifact_key {__MODULE__, :shared_fixture_artifact}
  @shared_artifact_prefix "favn_issue262_acceptance"

  def fixture_project!(prefix \\ "favn_issue262_canonical"),
    do: Favn.Local.CanonicalSampleProject.create!(prefix)

  def build_fixture_artifact!(prefix \\ "favn_single_node_artifact") do
    project_dir = fixture_project!(prefix)

    run_mix!(project_dir, ["deps.get"])
    run_mix!(project_dir, ["favn.install", "--skip-web-install"])

    {build_output, 0} = run_mix!(project_dir, ["favn.build.single"])
    dist_dir = dist_dir_from_output!(build_output)

    %{
      project_dir: project_dir,
      dist_dir: dist_dir,
      manifest_path: Path.join([dist_dir, "runner", "manifest-index.json"]),
      manifest_metadata: read_manifest_metadata!(dist_dir),
      build_output: build_output
    }
  end

  def shared_fixture_artifact! do
    case :persistent_term.get(@shared_artifact_key, nil) do
      nil ->
        artifact = build_fixture_artifact!(@shared_artifact_prefix)
        :persistent_term.put(@shared_artifact_key, artifact)
        artifact

      artifact ->
        artifact
    end
  end

  def cleanup_shared_artifacts! do
    case :persistent_term.get(@shared_artifact_key, nil) do
      %{project_dir: project_dir} ->
        File.rm_rf(project_dir)
        :persistent_term.erase(@shared_artifact_key)

      nil ->
        :ok
    end
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

  def runtime_env(runtime_home, database_url, workspace_id, port, service_token, extra_env \\ %{}) do
    %{
      "FAVN_STORAGE" => "postgres",
      "FAVN_DATABASE_URL" => database_url,
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE" => "true",
      "FAVN_DATABASE_POOL_SIZE" => "3",
      "FAVN_RUNTIME_INPUT_PIN_KEYS" =>
        Jason.encode!(%{"1" => "0123456789abcdef0123456789abcdef"}),
      "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION" => "1",
      "FAVN_WORKSPACE_IDS" => workspace_id,
      "FAVN_BOOTSTRAP_WORKSPACE_ID" => workspace_id,
      "FAVN_ORCHESTRATOR_API_BIND_HOST" => "127.0.0.1",
      "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(port),
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "favn_view|platform_operator:#{service_token}",
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

  def assert_artifact_started!(dist_dir, env, runtime_home) do
    {output, status} = start_artifact(dist_dir, env)
    assert status == 0, start_failure_message(output, runtime_home)
    output
  end

  def run_script(script, env) do
    env_exec = System.find_executable("env") || "env"
    env_args = ["-i" | isolated_env_args(env)] ++ [script]

    System.cmd(env_exec, env_args, stderr_to_stdout: true)
  end

  def assert_runtime_paths!(runtime_home) do
    assert File.exists?(Path.join(runtime_home, "run/backend.pid"))
    assert File.exists?(Path.join(runtime_home, "run/backend_boot.exs"))
    assert File.exists?(Path.join(runtime_home, "log/backend.log"))
    assert File.dir?(Path.join(runtime_home, "data"))
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

  def snapshot_dist_dir!(dist_dir) do
    dist_dir
    |> snapshot_entries!("")
    |> Map.new()
  end

  def assert_dist_dir_unchanged!(before_snapshot, dist_dir) when is_map(before_snapshot) do
    after_snapshot = snapshot_dist_dir!(dist_dir)

    added = sorted_difference(Map.keys(after_snapshot), Map.keys(before_snapshot))
    removed = sorted_difference(Map.keys(before_snapshot), Map.keys(after_snapshot))

    changed =
      before_snapshot
      |> Map.keys()
      |> Enum.filter(
        &(Map.has_key?(after_snapshot, &1) and before_snapshot[&1] != after_snapshot[&1])
      )
      |> Enum.sort()

    assert added == [] and removed == [] and changed == [],
           "dist_dir changed after build:\n" <>
             format_dist_changes(added, removed, changed, before_snapshot, after_snapshot)
  end

  def assert_dist_dir_immutable!(dist_dir, fun) when is_function(fun, 0) do
    snapshot = snapshot_dist_dir!(dist_dir)
    result = fun.()
    assert_dist_dir_unchanged!(snapshot, dist_dir)
    result
  end

  def read_manifest_metadata!(dist_dir) do
    metadata_path = Path.join([dist_dir, "runner", "metadata.json"])

    case metadata_path |> File.read!() |> JSON.decode!() do
      %{"manifest" => %{"manifest_version_id" => id, "content_hash" => hash}} = metadata
      when is_binary(id) and is_binary(hash) ->
        metadata["manifest"]

      decoded ->
        flunk("runner metadata did not include manifest identity: #{inspect(decoded)}")
    end
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

  defp snapshot_entries!(root, relative_dir) do
    dir = Path.join(root, relative_dir)

    dir
    |> File.ls!()
    |> Enum.flat_map(fn basename ->
      relative = Path.join(relative_dir, basename)
      path = Path.join(root, relative)
      {:ok, stat} = File.lstat(path)

      entry = {relative, snapshot_metadata(path, stat)}

      case stat.type do
        :directory -> [entry | snapshot_entries!(root, relative)]
        _other -> [entry]
      end
    end)
  end

  defp snapshot_metadata(path, stat) do
    metadata = %{
      type: stat.type,
      size: stat.size,
      mtime: stat.mtime
    }

    case stat.type do
      :regular -> Map.put(metadata, :hash, hash_file!(path))
      :symlink -> Map.put(metadata, :target, File.read_link!(path))
      _other -> metadata
    end
  end

  defp hash_file!(path) do
    path
    |> File.read!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp sorted_difference(left, right) do
    left
    |> MapSet.new()
    |> MapSet.difference(MapSet.new(right))
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp format_dist_changes(added, removed, changed, before_snapshot, after_snapshot) do
    [
      format_path_list("added", added),
      format_path_list("removed", removed),
      format_changed_paths(changed, before_snapshot, after_snapshot)
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  defp format_path_list(_label, []), do: ""

  defp format_path_list(label, paths) do
    "#{label}:\n" <> Enum.map_join(paths, "\n", &"  #{&1}")
  end

  defp format_changed_paths([], _before_snapshot, _after_snapshot), do: ""

  defp format_changed_paths(paths, before_snapshot, after_snapshot) do
    details =
      Enum.map_join(paths, "\n", fn path ->
        "  #{path}: #{inspect(before_snapshot[path])} -> #{inspect(after_snapshot[path])}"
      end)

    "changed:\n" <> details
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
end
