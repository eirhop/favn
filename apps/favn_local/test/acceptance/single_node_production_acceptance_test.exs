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
  @pipeline_id "pipeline:Elixir.FavnIssue262Sample.Pipelines.ProductionSmoke:production_smoke"
  @missing_secret_asset_id "asset:Elixir.FavnIssue262Sample.Assets.MissingSecret:asset"
  @repo_root Path.expand("../../../..", __DIR__)

  setup_all do
    ensure_executable!("curl")
    ensure_executable!("env")

    migrator_database_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL acceptance tests"

    migrate_database!(migrator_database_url)
    {database_url, runtime_role} = provision_runtime_role!(migrator_database_url)

    on_exit(fn -> drop_runtime_role!(migrator_database_url, runtime_role) end)

    artifact = shared_fixture_artifact!()

    {:ok, artifact: artifact, database_url: database_url}
  end

  setup %{artifact: artifact} do
    snapshot = snapshot_dist_dir!(artifact.dist_dir)

    on_exit(fn ->
      assert_dist_dir_unchanged!(snapshot, artifact.dist_dir)
    end)

    :ok
  end

  test "single-node production artifact runs canonical pipeline and survives restart", %{
    artifact: artifact,
    database_url: database_url
  } do
    runtime = fresh_runtime!(artifact.project_dir, "production", database_url)
    provision_workspace!(runtime)
    env = production_env(runtime)
    base_url = base_url(runtime.port)

    on_exit(fn -> stop_and_cleanup_runtime(artifact.dist_dir, env, runtime) end)

    start_output = assert_artifact_started!(artifact.dist_dir, env, runtime.runtime_home)
    assert start_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)
    assert_runtime_paths!(runtime.runtime_home)

    bootstrap_output =
      assert_bootstrap_succeeded!(
        artifact.project_dir,
        artifact.manifest_path,
        base_url,
        runtime.runtime_home
      )

    assert bootstrap_output =~ "Favn single-node bootstrap complete"

    assert bootstrap_output =~ "manifest registration: published" or
             bootstrap_output =~ "manifest registration: already_published"

    assert bootstrap_output =~ "runner registration: accepted"
    assert bootstrap_output =~ "active manifest verification: matched"

    repeat_output =
      assert_bootstrap_succeeded!(
        artifact.project_dir,
        artifact.manifest_path,
        base_url,
        runtime.runtime_home
      )

    assert repeat_output =~ "manifest registration: already_published"
    assert repeat_output =~ "runner registration: accepted"

    assert {:ok, session_context} = login(base_url)

    active_manifest_metadata =
      assert_active_manifest!(base_url, session_context, artifact.manifest_metadata)

    run_payload = manifest_pinned_payload(@pipeline_id, active_manifest_metadata)
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

    terminal_run =
      await_terminal_run!(base_url, session_context, run["id"], runtime.runtime_home)

    assert terminal_run["status"] == "ok", backend_log(runtime.runtime_home)

    assert terminal_run["manifest_version_id"] ==
             active_manifest_metadata["manifest_version_id"]

    assert {:ok, diagnostics} = OrchestratorClient.diagnostics(base_url, @service_token)
    assert diagnostics["status"] == "ok"
    assert_diagnostic!(diagnostics, "active_manifest", "ok")
    assert_diagnostic!(diagnostics, "runner", "ok")
    assert_diagnostic!(diagnostics, "scheduler", "ok")

    assert {stop_output, 0} = stop_artifact(artifact.dist_dir, env)
    assert stop_output =~ "Favn backend stopped"

    restart_output = assert_artifact_started!(artifact.dist_dir, env, runtime.runtime_home)
    assert restart_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)

    assert_active_manifest!(base_url, session_context, artifact.manifest_metadata)

    assert {:ok, persisted_run} =
             OrchestratorClient.get_run(base_url, @service_token, session_context, run["id"])

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
    artifact: artifact,
    database_url: database_url
  } do
    runtime = fresh_runtime!(artifact.project_dir, "scripts", database_url)
    provision_workspace!(runtime)
    env = production_env(runtime)

    on_exit(fn -> stop_and_cleanup_runtime(artifact.dist_dir, env, runtime) end)

    assert File.exists?(Path.join(artifact.dist_dir, "metadata.json"))
    assert File.exists?(Path.join(artifact.dist_dir, "config/assembly.json"))
    assert executable?(Path.join(artifact.dist_dir, "bin/start"))
    assert executable?(Path.join(artifact.dist_dir, "bin/stop"))
    assert_no_dev_env!(artifact.dist_dir)

    assert_dist_dir_immutable!(artifact.dist_dir, fn ->
      start_output = assert_artifact_started!(artifact.dist_dir, env, runtime.runtime_home)
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

    assert_invalid_configs_fail_before_serving!(
      artifact.dist_dir,
      artifact.project_dir,
      database_url
    )
  end

  test "missing asset runtime config fails before user asset execution", %{
    artifact: artifact,
    database_url: database_url
  } do
    runtime = fresh_runtime!(artifact.project_dir, "missing-config", database_url)
    provision_workspace!(runtime)
    env = production_env(runtime) |> Map.delete("FAVN_CANONICAL_MISSING_SECRET")
    base_url = base_url(runtime.port)

    on_exit(fn -> stop_and_cleanup_runtime(artifact.dist_dir, env, runtime) end)

    start_output = assert_artifact_started!(artifact.dist_dir, env, runtime.runtime_home)
    assert start_output =~ "Favn backend started with PID"
    assert_ready!(runtime.port)

    bootstrap_output =
      assert_bootstrap_succeeded!(
        artifact.project_dir,
        artifact.manifest_path,
        base_url,
        runtime.runtime_home
      )

    assert bootstrap_output =~ "Favn single-node bootstrap complete"

    assert {:ok, session_context} = login(base_url)

    active_manifest_metadata =
      assert_active_manifest!(base_url, session_context, artifact.manifest_metadata)

    assert {:ok, run} =
             OrchestratorClient.submit_run(
               base_url,
               @service_token,
               session_context,
               manifest_pinned_payload(@missing_secret_asset_id, active_manifest_metadata)
             )

    terminal_run =
      await_terminal_run!(base_url, session_context, run["id"], runtime.runtime_home)

    assert terminal_run["status"] == "error"

    encoded = inspect(terminal_run)
    assert encoded =~ "missing_runtime_config"
    assert encoded =~ "FAVN_CANONICAL_MISSING_SECRET"
    refute encoded =~ env["FAVN_CANONICAL_SOURCE_TOKEN"]
  end

  defp production_env(runtime) do
    runtime_env(
      runtime.runtime_home,
      runtime.database_url,
      runtime.workspace_id,
      runtime.port,
      @service_token,
      %{
        "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => @admin_username,
        "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => @admin_password,
        "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Admin",
        "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "admin",
        "FAVN_CANONICAL_SOURCE_NAME" => "canonical-source",
        "FAVN_CANONICAL_SOURCE_TOKEN" => "canonical-source-token-1234567890",
        "FAVN_CANONICAL_MISSING_SECRET" => "present-only-for-happy-path",
        "FAVN_CANONICAL_DUCKDB_PATH" => runtime.duckdb_path
      }
    )
  end

  defp fresh_runtime!(project_dir, name, database_url) do
    runtime_home = fresh_path(project_dir, "#{name}-runtime-home")
    duckdb_path = fresh_path(project_dir, "#{name}-duckdb/warehouse.duckdb")
    workspace_id = "acceptance-#{name}-#{System.unique_integer([:positive])}"

    File.mkdir_p!(Path.dirname(duckdb_path))

    %{
      runtime_home: runtime_home,
      database_url: database_url,
      workspace_id: workspace_id,
      duckdb_path: duckdb_path,
      port: free_port()
    }
  end

  defp cleanup_runtime!(runtime) do
    File.rm_rf(runtime.runtime_home)
    File.rm_rf(Path.dirname(runtime.duckdb_path))
  end

  defp stop_and_cleanup_runtime(dist_dir, env, runtime) do
    try do
      stop_artifact(dist_dir, env)
    after
      cleanup_runtime!(runtime)
    end
  end

  defp run_bootstrap(project_dir, manifest_path, base_url) do
    run_mix(project_dir, [
      "favn.bootstrap.single",
      "--manifest",
      manifest_path,
      "--orchestrator-url",
      base_url,
      "--service-token",
      @service_token,
      "--workspace-id",
      runtime_workspace_id(base_url),
      "--operator-username",
      @admin_username,
      "--operator-password",
      @admin_password
    ])
  end

  defp assert_bootstrap_succeeded!(project_dir, manifest_path, base_url, runtime_home) do
    case run_bootstrap(project_dir, manifest_path, base_url) do
      {output, 0} ->
        output

      {output, status} ->
        flunk(
          "bootstrap failed (status=#{status}):\n#{output}\nbackend log:\n#{backend_log(runtime_home)}"
        )
    end
  end

  defp login(base_url) do
    OrchestratorClient.password_login(
      base_url,
      @service_token,
      runtime_workspace_id(base_url),
      @admin_username,
      @admin_password
    )
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

  defp assert_active_manifest!(base_url, session_context, manifest_metadata) do
    assert {:ok, active_manifest} =
             OrchestratorClient.bootstrap_active_manifest(
               base_url,
               @service_token,
               session_context
             )

    manifest = Map.fetch!(active_manifest, "manifest")
    assert manifest["content_hash"] == manifest_metadata["content_hash"]
    assert is_binary(manifest["manifest_version_id"])

    Map.put(manifest_metadata, "manifest_version_id", manifest["manifest_version_id"])
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
      {:ok, %{"status" => status} = run}
      when status in ["ok", "error", "cancelled", "timed_out"] ->
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

  defp await_terminal_run!(base_url, session_context, run_id, runtime_home) do
    case await_terminal_run(base_url, session_context, run_id) do
      {:ok, run} ->
        run

      {:error, reason} ->
        flunk(
          "run #{run_id} did not reach a terminal state: #{inspect(reason)}\n" <>
            "backend log:\n#{backend_log(runtime_home)}"
        )
    end
  end

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

  defp assert_invalid_configs_fail_before_serving!(dist_dir, project_dir, database_url) do
    cases = [
      {"missing storage", &Map.delete(&1, "FAVN_STORAGE")},
      {"missing database URL", &Map.delete(&1, "FAVN_DATABASE_URL")},
      {"unsupported SQLite storage", &Map.put(&1, "FAVN_STORAGE", "sqlite")},
      {"short service token",
       &Map.put(&1, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "favn_view:short")}
    ]

    Enum.each(cases, fn {name, mutate} ->
      runtime =
        fresh_runtime!(project_dir, "invalid-#{String.replace(name, " ", "-")}", database_url)

      env =
        production_env(runtime)
        |> Map.put("FAVN_STARTUP_TIMEOUT_SECONDS", "15")
        |> mutate.()

      {output, status} = start_artifact(dist_dir, env)
      assert status != 0, "#{name} unexpectedly started:\n#{output}"
      assert output =~ "Favn backend exited before readiness"
      refute File.exists?(pid_path(runtime.runtime_home))
      assert {:error, _reason} = fetch_json(live_url(runtime.port))

      assert backend_log(runtime.runtime_home) =~
               "invalid Favn backend production runtime config or startup"

      cleanup_runtime!(runtime)
    end)
  end

  defp fresh_path(project_dir, relative) do
    Path.join(project_dir, "#{System.unique_integer([:positive])}-#{relative}")
  end

  defp provision_workspace!(runtime) do
    {output, status} =
      System.cmd(
        System.find_executable("mix") || "mix",
        ["favn.postgres.provision_workspace", "--id", runtime.workspace_id],
        cd: @repo_root,
        stderr_to_stdout: true,
        env: postgres_task_env(runtime.database_url)
      )

    assert status == 0, output
    Process.put({__MODULE__, :workspace_by_url, base_url(runtime.port)}, runtime.workspace_id)
    :ok
  end

  defp migrate_database!(database_url) do
    {output, status} =
      System.cmd(
        System.find_executable("mix") || "mix",
        ["favn.postgres.migrate"],
        cd: @repo_root,
        stderr_to_stdout: true,
        env: postgres_task_env(database_url)
      )

    assert status == 0, output
  end

  defp provision_runtime_role!(database_url) do
    suffix = System.unique_integer([:positive])
    role = "favn_acceptance_#{suffix}"
    password = "acceptance_runtime_#{suffix}_credential"

    run_postgres_admin_script!(
      database_url,
      %{
        "FAVN_ACCEPTANCE_RUNTIME_ROLE" => role,
        "FAVN_ACCEPTANCE_RUNTIME_PASSWORD" => password
      },
      """
      {:ok, _} = Application.ensure_all_started(:ecto_sql)
      {:ok, _} = Application.ensure_all_started(:postgrex)
      role = System.fetch_env!("FAVN_ACCEPTANCE_RUNTIME_ROLE")
      password = System.fetch_env!("FAVN_ACCEPTANCE_RUNTIME_PASSWORD")
      quoted_role = FavnStoragePostgres.Privileges.quote_identifier!(role)
      {:ok, options} = FavnStoragePostgres.Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"), ssl_mode: :disable, pool_size: 1
      )
      {:ok, repo} = FavnStoragePostgres.Repo.start_link(options)
      Ecto.Adapters.SQL.query!(
        FavnStoragePostgres.Repo,
        "CREATE ROLE " <> quoted_role <> " LOGIN PASSWORD '" <> password <> "'",
        []
      )
      :ok = FavnStoragePostgres.Privileges.grant_runtime!(FavnStoragePostgres.Repo, role)
      GenServer.stop(repo)
      """
    )

    {database_url_with_credentials(database_url, role, password), role}
  end

  defp drop_runtime_role!(database_url, role) do
    run_postgres_admin_script!(database_url, %{"FAVN_ACCEPTANCE_RUNTIME_ROLE" => role}, """
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    role = System.fetch_env!("FAVN_ACCEPTANCE_RUNTIME_ROLE")
    quoted_role = FavnStoragePostgres.Privileges.quote_identifier!(role)
    {:ok, options} = FavnStoragePostgres.Config.repo_options(
      url: System.fetch_env!("FAVN_DATABASE_URL"), ssl_mode: :disable, pool_size: 1
    )
    {:ok, repo} = FavnStoragePostgres.Repo.start_link(options)
    Ecto.Adapters.SQL.query!(FavnStoragePostgres.Repo, "DROP OWNED BY " <> quoted_role, [])
    Ecto.Adapters.SQL.query!(FavnStoragePostgres.Repo, "DROP ROLE " <> quoted_role, [])
    GenServer.stop(repo)
    """)
  end

  defp run_postgres_admin_script!(database_url, extra_env, script) do
    {output, status} =
      System.cmd(
        System.find_executable("mix") || "mix",
        ["run", "--no-start", "-e", script],
        cd: @repo_root,
        stderr_to_stdout: true,
        env: Map.merge(postgres_task_env(database_url), extra_env)
      )

    assert status == 0, output
  end

  defp database_url_with_credentials(database_url, role, password) do
    database_url
    |> URI.parse()
    |> Map.put(:userinfo, role <> ":" <> password)
    |> URI.to_string()
  end

  defp postgres_task_env(database_url) do
    %{
      "MIX_ENV" => "test",
      "FAVN_DATABASE_URL" => database_url,
      "FAVN_RUNTIME_INPUT_PIN_KEYS" =>
        Jason.encode!(%{"1" => "0123456789abcdef0123456789abcdef"}),
      "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION" => "1"
    }
  end

  defp runtime_workspace_id(base_url) do
    Process.get({__MODULE__, :workspace_by_url, base_url}) ||
      flunk("workspace was not registered for #{base_url}")
  end
end
