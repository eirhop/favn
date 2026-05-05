defmodule Favn.SingleNodeArtifactRuntimeTest do
  use ExUnit.Case, async: false

  import Favn.Local.SingleNodeArtifactHarness

  @moduletag :integration
  @moduletag timeout: 600_000

  @service_token "favnweb-runtime-credential-alpha-1234567890"
  @admin_username "admin"
  @admin_password "admin-password-long"

  test "generated single-node artifact start stop runtime contract" do
    ensure_executable!("curl")
    ensure_executable!("env")

    project_dir = fixture_project!("favn_single_artifact_runtime")
    runtime_home = Path.join(project_dir, "runtime-home")
    sqlite_path = Path.join(project_dir, "data/control-plane.sqlite3")
    port = free_port()
    File.mkdir_p!(Path.dirname(sqlite_path))

    on_exit(fn -> File.rm_rf(project_dir) end)

    run_mix!(project_dir, ["deps.get"])
    run_mix!(project_dir, ["favn.install", "--skip-web-install"])

    {build_output, 0} = run_mix!(project_dir, ["favn.build.single"])
    dist_dir = dist_dir_from_output!(build_output)

    assert File.exists?(Path.join(dist_dir, "metadata.json"))
    assert File.exists?(Path.join(dist_dir, "config/assembly.json"))
    assert executable?(Path.join(dist_dir, "bin/start"))
    assert executable?(Path.join(dist_dir, "bin/stop"))
    assert_no_dev_env!(dist_dir)

    env = runtime_env(runtime_home, sqlite_path, port, @service_token, bootstrap_env())

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
        runtime_env(runtime_home, sqlite_path, port, @service_token, bootstrap_env())
        |> Map.put("FAVN_STARTUP_TIMEOUT_SECONDS", "5")
        |> mutate.()

      {output, status} = start_artifact(dist_dir, env)
      assert status != 0, "#{name} unexpectedly started:\n#{output}"
      assert output =~ "Favn backend exited before readiness"
      refute File.exists?(pid_path(runtime_home))
      assert {:error, _reason} = fetch_json(live_url(port))

      assert backend_log(runtime_home) =~ "invalid Favn backend production runtime config or startup"
    end)
  end

  defp bootstrap_env do
    %{
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => @admin_username,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => @admin_password,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Admin",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "admin"
    }
  end
end
