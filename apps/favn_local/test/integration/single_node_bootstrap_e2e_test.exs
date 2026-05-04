defmodule Favn.SingleNodeBootstrapE2ETest do
  use ExUnit.Case, async: false

  import Favn.Local.SingleNodeArtifactHarness

  alias Favn.Dev.OrchestratorClient

  @moduletag :integration
  @moduletag timeout: 600_000

  @service_token "favnweb-runtime-credential-alpha-1234567890"
  @admin_username "admin"
  @admin_password "admin-password-long"

  test "production first-run bootstrap survives backend restart" do
    ensure_executable!("curl")
    ensure_executable!("env")

    project_dir = fixture_project!("favn_single_bootstrap_e2e")
    runtime_home = Path.join(project_dir, "runtime-home")
    sqlite_path = Path.join(project_dir, "data/control-plane.sqlite3")
    port = free_port()
    File.mkdir_p!(Path.dirname(sqlite_path))

    on_exit(fn -> File.rm_rf(project_dir) end)

    run_mix!(project_dir, ["deps.get"])
    run_mix!(project_dir, ["favn.install", "--skip-web-install"])

    {build_output, 0} = run_mix!(project_dir, ["favn.build.single"])
    dist_dir = dist_dir_from_output!(build_output)
    manifest_path = Path.join([dist_dir, "runner", "manifest.json"])
    manifest_metadata = read_manifest_metadata!(dist_dir)

    env =
      runtime_env(runtime_home, sqlite_path, port, @service_token, %{
        "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => @admin_username,
        "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => @admin_password,
        "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Admin",
        "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "admin"
      })

    base_url = "http://127.0.0.1:#{port}"

    on_exit(fn -> stop_artifact(dist_dir, env) end)

    assert {start_output, 0} = start_artifact(dist_dir, env)
    assert start_output =~ "Favn backend started with PID"
    assert {:ok, %{"status" => "ready"}} = poll_json(ready_url(port))

    {bootstrap_output, 0} = run_bootstrap(project_dir, manifest_path, base_url)

    assert bootstrap_output =~ "Favn single-node bootstrap complete"
    assert bootstrap_output =~ "manifest registration: already_published"
    assert bootstrap_output =~ "runner registration: accepted"
    assert bootstrap_output =~ "active manifest verification: matched"

    {repeat_output, 0} = run_bootstrap(project_dir, manifest_path, base_url)

    assert repeat_output =~ "manifest registration: already_published"
    assert repeat_output =~ "runner registration: accepted"

    assert {:ok, session_context} =
             OrchestratorClient.password_login(
               base_url,
               @service_token,
               @admin_username,
               @admin_password
             )

    assert {:ok, active_manifest} =
             OrchestratorClient.bootstrap_active_manifest(base_url, @service_token)

    assert get_in(active_manifest, ["manifest", "manifest_version_id"]) ==
             manifest_metadata["manifest_version_id"]

    assert {:ok, run} =
             OrchestratorClient.submit_run(base_url, @service_token, session_context, %{
               target: %{type: "asset", id: "asset:Elixir.FavnSingleNodeFixture.Ping:asset"},
               manifest_selection: %{
                 mode: "version",
                 manifest_version_id: manifest_metadata["manifest_version_id"]
               },
               dependencies: "none"
             })

    assert {:ok, terminal_run} = await_terminal_run(base_url, session_context, run["id"])
    assert terminal_run["status"] == "ok"
    assert terminal_run["manifest_version_id"] == manifest_metadata["manifest_version_id"]

    assert {stop_output, 0} = stop_artifact(dist_dir, env)
    assert stop_output =~ "Favn backend stopped"

    assert {restart_output, 0} = start_artifact(dist_dir, env)
    assert restart_output =~ "Favn backend started with PID"
    assert {:ok, %{"status" => "ready"}} = poll_json(ready_url(port))

    assert {:ok, restarted_active_manifest} =
             OrchestratorClient.bootstrap_active_manifest(base_url, @service_token)

    assert get_in(restarted_active_manifest, ["manifest", "manifest_version_id"]) ==
             manifest_metadata["manifest_version_id"]

    persisted_run = get_run_after_restart!(base_url, session_context, run["id"], runtime_home)

    assert persisted_run["status"] == "ok"

    assert {:ok, _new_session_context} =
             OrchestratorClient.password_login(
               base_url,
               @service_token,
               @admin_username,
               @admin_password
             )

    assert {:ok, diagnostics} = OrchestratorClient.diagnostics(base_url, @service_token)
    assert diagnostics["status"] == "ok"
    assert_diagnostic!(diagnostics, "active_manifest", "ok")
    assert_diagnostic!(diagnostics, "runner", "ok")
    assert_diagnostic!(diagnostics, "scheduler", "ok")

    assert {stop_output, 0} = stop_artifact(dist_dir, env)
    assert stop_output =~ "Favn backend stopped"
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

  defp read_manifest_metadata!(dist_dir) do
    metadata_path = Path.join([dist_dir, "runner", "metadata.json"])

    case metadata_path |> File.read!() |> JSON.decode!() do
      %{"manifest" => %{"manifest_version_id" => id, "content_hash" => hash}} = metadata
      when is_binary(id) and is_binary(hash) ->
        metadata["manifest"]

      decoded ->
        flunk("runner metadata did not include manifest identity: #{inspect(decoded)}")
    end
  end

  defp await_terminal_run(base_url, session_context, run_id, attempts \\ 120)

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

  defp get_run_after_restart!(base_url, session_context, run_id, runtime_home) do
    case OrchestratorClient.get_run(base_url, @service_token, session_context, run_id) do
      {:ok, run} ->
        run

      {:error, reason} ->
        flunk("get_run failed after restart: #{inspect(reason)}\nbackend log:\n#{backend_log(runtime_home)}")
    end
  end

  defp assert_diagnostic!(diagnostics, check, status) do
    assert %{"status" => ^status} =
             diagnostics
             |> Map.fetch!("checks")
             |> Enum.find(&(&1["check"] == check))
  end
end
