defmodule Favn.SingleNodeBootstrapE2ETest do
  use ExUnit.Case, async: false

  alias Favn.Dev.OrchestratorClient

  @moduletag :integration
  @moduletag timeout: 600_000

  @repo_root Path.expand("../../../..", __DIR__)
  @service_token "favnweb-runtime-credential-alpha-1234567890"
  @admin_username "admin"
  @admin_password "admin-password-long"

  test "production first-run bootstrap survives backend restart" do
    ensure_executable!("curl")
    ensure_executable!("env")

    project_dir = fixture_project!()
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

    env = runtime_env(runtime_home, sqlite_path, port)
    base_url = "http://127.0.0.1:#{port}"

    on_exit(fn -> stop_artifact(dist_dir, env) end)

    assert {start_output, 0} = start_artifact(dist_dir, env)
    assert start_output =~ "Favn backend started with PID"
    assert {:ok, %{"status" => "ready"}} = poll_json(ready_url(port))

    {bootstrap_output, 0} =
      run_mix(project_dir, [
        "favn.bootstrap.single",
        "--manifest",
        manifest_path,
        "--orchestrator-url",
        base_url,
        "--service-token",
        @service_token
      ])

    assert bootstrap_output =~ "Favn single-node bootstrap complete"
    assert bootstrap_output =~ "manifest registration: already_published"
    assert bootstrap_output =~ "runner registration: accepted"
    assert bootstrap_output =~ "active manifest verification: matched"

    {repeat_output, 0} =
      run_mix(project_dir, [
        "favn.bootstrap.single",
        "--manifest",
        manifest_path,
        "--orchestrator-url",
        base_url,
        "--service-token",
        @service_token
      ])

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
               target: %{type: "asset", id: "asset:Elixir.FavnBootstrapE2EFixture.Ping:asset"},
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

  defp fixture_project! do
    project_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_single_bootstrap_e2e_#{System.unique_integer([:positive])}"
      )

    lib_dir = Path.join(project_dir, "lib/favn_bootstrap_e2e_fixture")
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
    defmodule FavnBootstrapE2EFixture.MixProject do
      use Mix.Project

      def project do
        [
          app: :favn_bootstrap_e2e_fixture,
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
      asset_modules: [FavnBootstrapE2EFixture.Ping],
      pipeline_modules: [],
      schedule_modules: []
    """
  end

  defp ping_asset_ex do
    """
    defmodule FavnBootstrapE2EFixture.Ping do
      use Favn.Asset

      def asset(_ctx), do: :ok
    end
    """
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
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => @admin_username,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => @admin_password,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Admin",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "admin",
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

  defp run_mix!(project_dir, args) do
    case run_mix(project_dir, args) do
      {output, 0 = status} ->
        {output, status}

      {output, status} ->
        flunk("mix #{Enum.join(args, " ")} failed (status=#{status}):\n#{output}")
    end
  end

  defp run_mix(project_dir, args) do
    mix = System.find_executable("mix") || "mix"

    System.cmd(mix, args,
      cd: project_dir,
      stderr_to_stdout: true,
      env: %{"MIX_ENV" => "prod"}
    )
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

  defp dist_dir_from_output!(output) do
    case Regex.run(~r/^dist: (.+)$/m, output) do
      [_line, dist_dir] -> String.trim(dist_dir)
      nil -> flunk("mix favn.build.single output did not include dist path:\n#{output}")
    end
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
        log_path = Path.join(runtime_home, "log/backend.log")
        log = if File.exists?(log_path), do: File.read!(log_path), else: "<missing>"

        flunk("get_run failed after restart: #{inspect(reason)}\nbackend log:\n#{log}")
    end
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

  defp assert_diagnostic!(diagnostics, check, status) do
    assert %{"status" => ^status} =
             diagnostics
             |> Map.fetch!("checks")
             |> Enum.find(&(&1["check"] == check))
  end

  defp ready_url(port), do: "http://127.0.0.1:#{port}/api/orchestrator/v1/health/ready"

  defp ensure_executable!(name) do
    unless System.find_executable(name) do
      flunk("#{name} is required for the single-node bootstrap E2E test")
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
