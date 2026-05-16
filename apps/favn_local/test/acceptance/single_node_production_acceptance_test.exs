defmodule Favn.Local.SingleNodeProductionAcceptanceTest do
  use ExUnit.Case, async: false

  import Favn.Local.SingleNodeArtifactHarness

  alias Favn.Dev.OrchestratorClient

  @moduletag :integration
  @moduletag :acceptance
  @moduletag timeout: 900_000

  @service_token "favnweb-runtime-credential-alpha-1234567890"
  @admin_username "admin"
  @admin_password "admin-password-long"
  @pipeline_id "pipeline:Elixir.FavnIssue262Sample.Pipelines.ProductionSmoke"
  @missing_secret_asset_id "asset:Elixir.FavnIssue262Sample.Assets.MissingSecret:asset"

  setup_all do
    ensure_executable!("curl")
    ensure_executable!("env")

    artifact = shared_fixture_artifact!()

    {:ok, artifact: artifact}
  end

  setup %{artifact: artifact} do
    snapshot = snapshot_dist_dir!(artifact.dist_dir)

    on_exit(fn ->
      assert_dist_dir_unchanged!(snapshot, artifact.dist_dir)
    end)

    :ok
  end

  test "single-node production artifact runs canonical pipeline and survives restart", %{
    artifact: artifact
  } do
    runtime = fresh_runtime!(artifact.project_dir, "production")
    env = production_env(runtime)
    base_url = base_url(runtime.port)

    on_exit(fn -> stop_artifact(artifact.dist_dir, env) end)
    on_exit(fn -> cleanup_runtime!(runtime) end)

    assert {start_output, 0} = start_artifact(artifact.dist_dir, env)
    assert start_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)
    assert_runtime_paths!(runtime.runtime_home, runtime.sqlite_path)

    assert {bootstrap_output, 0} = run_bootstrap(artifact.project_dir, artifact.manifest_path, base_url)
    assert bootstrap_output =~ "Favn single-node bootstrap complete"
    assert bootstrap_output =~ "manifest registration: already_published"
    assert bootstrap_output =~ "runner registration: accepted"
    assert bootstrap_output =~ "active manifest verification: matched"

    assert {repeat_output, 0} = run_bootstrap(artifact.project_dir, artifact.manifest_path, base_url)
    assert repeat_output =~ "manifest registration: already_published"
    assert repeat_output =~ "runner registration: accepted"

    assert {:ok, session_context} = login(base_url)
    assert_active_manifest!(base_url, artifact.manifest_metadata)

    run_payload = manifest_pinned_payload(@pipeline_id, artifact.manifest_metadata)
    idempotency_key = "issue262-production-smoke-#{System.unique_integer([:positive])}"

    assert {:ok, run} =
             OrchestratorClient.submit_run(base_url, @service_token, session_context, run_payload,
               idempotency_key: idempotency_key
             )

    assert {:ok, repeat_run} =
             OrchestratorClient.submit_run(base_url, @service_token, session_context, run_payload,
               idempotency_key: idempotency_key
             )

    assert repeat_run["id"] == run["id"]

    assert {:ok, terminal_run} = await_terminal_run(base_url, session_context, run["id"])
    assert terminal_run["status"] == "ok", backend_log(runtime.runtime_home)
    assert terminal_run["manifest_version_id"] == artifact.manifest_metadata["manifest_version_id"]

    assert {:ok, diagnostics} = OrchestratorClient.diagnostics(base_url, @service_token)
    assert diagnostics["status"] == "ok"
    assert_diagnostic!(diagnostics, "active_manifest", "ok")
    assert_diagnostic!(diagnostics, "runner", "ok")
    assert_diagnostic!(diagnostics, "scheduler", "ok")

    assert {stop_output, 0} = stop_artifact(artifact.dist_dir, env)
    assert stop_output =~ "Favn backend stopped"

    assert {restart_output, 0} = start_artifact(artifact.dist_dir, env)
    assert restart_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)

    assert_active_manifest!(base_url, artifact.manifest_metadata)
    assert {:ok, persisted_run} = OrchestratorClient.get_run(base_url, @service_token, session_context, run["id"])
    assert persisted_run["status"] == "ok"

    assert {:ok, _new_session_context} = login(base_url)

    assert {:ok, restarted_diagnostics} = OrchestratorClient.diagnostics(base_url, @service_token)
    assert restarted_diagnostics["status"] == "ok"
    assert_diagnostic!(restarted_diagnostics, "active_manifest", "ok")
    assert_diagnostic!(restarted_diagnostics, "runner", "ok")
    assert_diagnostic!(restarted_diagnostics, "scheduler", "ok")

    assert File.exists?(Path.join(runtime.runtime_home, "log/backend.log"))
    assert {stop_output, 0} = stop_artifact(artifact.dist_dir, env)
    assert stop_output =~ "Favn backend stopped"
  end

  test "generated scripts are immutable and operational commands are idempotent", %{
    artifact: artifact
  } do
    runtime = fresh_runtime!(artifact.project_dir, "scripts")
    env = production_env(runtime)

    on_exit(fn -> stop_artifact(artifact.dist_dir, env) end)
    on_exit(fn -> cleanup_runtime!(runtime) end)

    assert File.exists?(Path.join(artifact.dist_dir, "metadata.json"))
    assert File.exists?(Path.join(artifact.dist_dir, "config/assembly.json"))
    assert executable?(Path.join(artifact.dist_dir, "bin/start"))
    assert executable?(Path.join(artifact.dist_dir, "bin/stop"))
    assert_no_dev_env!(artifact.dist_dir)

    assert_dist_dir_immutable!(artifact.dist_dir, fn ->
      assert {start_output, 0} = start_artifact(artifact.dist_dir, env)
      assert start_output =~ "Favn backend started with PID"
      assert_ready!(runtime.port)

      original_pid = read_pid!(runtime.runtime_home)
      assert process_running?(original_pid)

      assert {duplicate_output, duplicate_status} = start_artifact(artifact.dist_dir, env)
      assert duplicate_status != 0
      assert duplicate_output =~ "already running"
      assert read_pid!(runtime.runtime_home) == original_pid

      assert {stop_output, 0} = stop_artifact(artifact.dist_dir, env)
      assert stop_output =~ "Favn backend stopped"
    end)

    assert_stop_idempotency!(artifact.dist_dir, Path.join(artifact.project_dir, "stop-runtime"))
    assert_invalid_configs_fail_before_serving!(artifact.dist_dir, artifact.project_dir)
  end

  test "missing asset runtime config fails before user asset execution", %{artifact: artifact} do
    runtime = fresh_runtime!(artifact.project_dir, "missing-config")
    env = production_env(runtime) |> Map.delete("FAVN_CANONICAL_MISSING_SECRET")
    base_url = base_url(runtime.port)

    on_exit(fn -> stop_artifact(artifact.dist_dir, env) end)
    on_exit(fn -> cleanup_runtime!(runtime) end)

    assert {start_output, 0} = start_artifact(artifact.dist_dir, env)
    assert start_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)

    assert {bootstrap_output, 0} = run_bootstrap(artifact.project_dir, artifact.manifest_path, base_url)
    assert bootstrap_output =~ "Favn single-node bootstrap complete"

    assert {:ok, session_context} = login(base_url)

    assert {:ok, run} =
             OrchestratorClient.submit_run(
               base_url,
               @service_token,
               session_context,
               manifest_pinned_payload(@missing_secret_asset_id, artifact.manifest_metadata)
             )

    assert {:ok, terminal_run} = await_terminal_run(base_url, session_context, run["id"])
    assert terminal_run["status"] == "error"

    encoded = inspect(terminal_run)
    assert encoded =~ "missing_runtime_config"
    assert encoded =~ "FAVN_CANONICAL_MISSING_SECRET"
    refute encoded =~ env["FAVN_CANONICAL_SOURCE_TOKEN"]
  end

  defp production_env(runtime) do
    runtime_env(runtime.runtime_home, runtime.sqlite_path, runtime.port, @service_token, %{
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => @admin_username,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => @admin_password,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Admin",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "admin",
      "FAVN_CANONICAL_SOURCE_NAME" => "canonical-source",
      "FAVN_CANONICAL_SOURCE_TOKEN" => "canonical-source-token-1234567890",
      "FAVN_CANONICAL_MISSING_SECRET" => "present-only-for-happy-path",
      "FAVN_CANONICAL_DUCKDB_PATH" => runtime.duckdb_path
    })
  end

  defp fresh_runtime!(project_dir, name) do
    runtime_home = fresh_path(project_dir, "#{name}-runtime-home")
    sqlite_path = fresh_path(project_dir, "#{name}-data/control-plane.sqlite3")
    duckdb_path = fresh_path(project_dir, "#{name}-duckdb/warehouse.duckdb")

    File.mkdir_p!(Path.dirname(sqlite_path))
    File.mkdir_p!(Path.dirname(duckdb_path))

    %{
      runtime_home: runtime_home,
      sqlite_path: sqlite_path,
      duckdb_path: duckdb_path,
      port: free_port()
    }
  end

  defp cleanup_runtime!(runtime) do
    File.rm_rf(runtime.runtime_home)
    File.rm_rf(Path.dirname(runtime.sqlite_path))
    File.rm_rf(Path.dirname(runtime.duckdb_path))
  end

  defp run_bootstrap(project_dir, manifest_path, base_url) do
    run_mix(project_dir, [
      "favn.bootstrap.single",
      "--manifest",
      manifest_path,
      "--orchestrator-url",
      base_url,
      "--service-token",
      @service_token
    ])
  end

  defp login(base_url) do
    OrchestratorClient.password_login(base_url, @service_token, @admin_username, @admin_password)
  end

  defp base_url(port), do: "http://127.0.0.1:#{port}"

  defp assert_ready!(port) do
    assert {:ok, live} = poll_json(live_url(port))
    assert live["status"] == "ok"
    assert {:ok, ready} = poll_json(ready_url(port))
    assert ready["status"] == "ready"
    assert_ready_check!(ready, "api")
    assert_ready_check!(ready, "storage")
    assert_ready_check!(ready, "scheduler")
    assert_ready_check!(ready, "runner")
  end

  defp assert_active_manifest!(base_url, manifest_metadata) do
    assert {:ok, active_manifest} = OrchestratorClient.bootstrap_active_manifest(base_url, @service_token)

    assert get_in(active_manifest, ["manifest", "manifest_version_id"]) ==
             manifest_metadata["manifest_version_id"]
  end

  defp manifest_pinned_payload(target_id, manifest_metadata) do
    payload = %{
      target: target(target_id),
      manifest_selection: %{
        mode: "version",
        manifest_version_id: manifest_metadata["manifest_version_id"]
      }
    }

    case target_id do
      "asset:" <> _ -> Map.put(payload, :dependencies, "all")
      "pipeline:" <> _ -> payload
    end
  end

  defp target("pipeline:" <> _ = id), do: %{type: "pipeline", id: id}
  defp target("asset:" <> _ = id), do: %{type: "asset", id: id}

  defp await_terminal_run(base_url, session_context, run_id, attempts \\ 180)

  defp await_terminal_run(base_url, session_context, run_id, attempts) when attempts > 0 do
    case OrchestratorClient.get_run(base_url, @service_token, session_context, run_id) do
      {:ok, %{"status" => status} = run} when status in ["ok", "error", "cancelled", "timed_out"] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(250)
        await_terminal_run(base_url, session_context, run_id, attempts - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp await_terminal_run(_base_url, _session_context, _run_id, 0),
    do: {:error, :timeout_waiting_for_terminal_run}

  defp assert_diagnostic!(diagnostics, check, status) do
    assert %{"status" => ^status} =
             diagnostics
             |> Map.fetch!("checks")
             |> Enum.find(&(&1["check"] == check))
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
       &Map.put(&1, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "favn_view:short")}
    ]

    Enum.each(cases, fn {name, mutate} ->
      runtime = fresh_runtime!(project_dir, "invalid-#{String.replace(name, " ", "-")}")

      env =
        production_env(runtime)
        |> Map.put("FAVN_STARTUP_TIMEOUT_SECONDS", "5")
        |> mutate.()

      {output, status} = start_artifact(dist_dir, env)
      assert status != 0, "#{name} unexpectedly started:\n#{output}"
      assert output =~ "Favn backend exited before readiness"
      refute File.exists?(pid_path(runtime.runtime_home))
      assert {:error, _reason} = fetch_json(live_url(runtime.port))
      assert backend_log(runtime.runtime_home) =~ "invalid Favn backend production runtime config or startup"

      cleanup_runtime!(runtime)
    end)
  end

  defp fresh_path(project_dir, relative) do
    Path.join(project_dir, "#{System.unique_integer([:positive])}-#{relative}")
  end
end
