defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Dev.Bootstrap.Single, as: BootstrapSingle
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.State
  alias Mix.Tasks.Favn.Backfill, as: BackfillTask
  alias Mix.Tasks.Favn.Bootstrap.Single, as: BootstrapSingleTask
  alias Mix.Tasks.Favn.Build.Orchestrator, as: BuildOrchestratorTask
  alias Mix.Tasks.Favn.Build.Runner, as: BuildRunnerTask
  alias Mix.Tasks.Favn.Build.Single, as: BuildSingleTask
  alias Mix.Tasks.Favn.Build.Web, as: BuildWebTask
  alias Mix.Tasks.Favn.Dev, as: DevTask
  alias Mix.Tasks.Favn.Doctor, as: DoctorTask
  alias Mix.Tasks.Favn.Init, as: InitTask
  alias Mix.Tasks.Favn.Install, as: InstallTask
  alias Mix.Tasks.Favn.Logs, as: LogsTask
  alias Mix.Tasks.Favn.Reload, as: ReloadTask
  alias Mix.Tasks.Favn.Reset, as: ResetTask
  alias Mix.Tasks.Favn.Run, as: RunTask
  alias Mix.Tasks.Favn.Status, as: StatusTask
  alias Mix.Tasks.Favn.Stop, as: StopTask

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_public_tasks_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    File.mkdir_p!(Path.join(root_dir, "web/favn_web"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))

    File.write!(Path.join(root_dir, "mix.lock"), "lock")
    File.write!(Path.join(root_dir, "web/favn_web/package.json"), "{}")
    File.write!(Path.join(root_dir, "web/favn_web/package-lock.json"), "{}")

    File.write!(
      Path.join(root_dir, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(root_dir, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "mix favn.dev raises when stack is already running", %{root_dir: root_dir} do
    current_pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "services" => %{
        "web" => %{"pid" => current_pid},
        "orchestrator" => %{"pid" => current_pid},
        "runner" => %{"pid" => current_pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    assert_raise Mix.Error, ~r/local stack already running/, fn ->
      DevTask.run(["--root-dir", root_dir])
    end
  end

  test "mix favn.dev raises when install is missing", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/install required; run mix favn.install/, fn ->
      DevTask.run(["--root-dir", root_dir])
    end
  end

  test "mix favn.dev parses scheduler flags" do
    assert Keyword.get(DevTask.parse_args(["--scheduler"]), :scheduler) == true
    assert Keyword.get(DevTask.parse_args(["--no-scheduler"]), :scheduler) == false
    assert Keyword.get(DevTask.parse_args([]), :scheduler) == nil
  end

  test "no-positional public mix favn tasks reject invalid options and unexpected args" do
    tasks = [
      {BuildOrchestratorTask, "favn.build.orchestrator"},
      {BuildRunnerTask, "favn.build.runner"},
      {BuildSingleTask, "favn.build.single"},
      {BuildWebTask, "favn.build.web"},
      {BootstrapSingleTask, "favn.bootstrap.single"},
      {DevTask, "favn.dev"},
      {InstallTask, "favn.install"},
      {LogsTask, "favn.logs"},
      {ReloadTask, "favn.reload"},
      {ResetTask, "favn.reset"},
      {StatusTask, "favn.status"},
      {StopTask, "favn.stop"}
    ]

    for {task, task_name} <- tasks do
      assert_raise Mix.Error, ~r/invalid option for mix #{Regex.escape(task_name)}/, fn ->
        task.run(["--bad-option"])
      end

      assert_raise Mix.Error, ~r/unexpected argument for mix #{Regex.escape(task_name)}/, fn ->
        task.run(["extra"])
      end
    end
  end

  test "mix favn.bootstrap.single requires manifest, orchestrator URL, and service token" do
    previous_env = bootstrap_env()
    clear_bootstrap_env()
    on_exit(fn -> restore_bootstrap_env(previous_env) end)

    assert_raise Mix.Error,
                 ~r/missing required option\(s\): --manifest, --orchestrator-url, --service-token/,
                 fn -> BootstrapSingleTask.parse_args([]) end
  end

  test "mix favn.bootstrap.single reads env defaults and lets flags win" do
    previous_env = bootstrap_env()
    clear_bootstrap_env()

    System.put_env("FAVN_BOOTSTRAP_MANIFEST_PATH", "/env/manifest.json")
    System.put_env("FAVN_WEB_ORCHESTRATOR_BASE_URL", "http://127.0.0.1:4000")
    System.put_env("FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN", "env-token")

    on_exit(fn -> restore_bootstrap_env(previous_env) end)

    opts = BootstrapSingleTask.parse_args(["--manifest", "/flag/manifest.json"])

    assert Keyword.fetch!(opts, :manifest_path) == "/flag/manifest.json"
    assert Keyword.fetch!(opts, :orchestrator_url) == "http://127.0.0.1:4000"
    assert Keyword.fetch!(opts, :service_token) == "env-token"
  end

  test "mix favn.bootstrap.single parses activation flag" do
    opts =
      BootstrapSingleTask.parse_args([
        "--manifest",
        "/tmp/manifest.json",
        "--orchestrator-url",
        "http://127.0.0.1:4101",
        "--service-token",
        "token",
        "--no-activate"
      ])

    assert Keyword.fetch!(opts, :activate?) == false
  end

  test "mix favn.bootstrap.single prints matched active-manifest verification", %{
    root_dir: root_dir
  } do
    manifest_path = write_bootstrap_manifest(root_dir)
    {:ok, version} = BootstrapSingle.read_manifest_version(manifest_path)
    {:ok, base_url, _server} = start_bootstrap_server({:matched, version.manifest_version_id})

    output =
      capture_io(fn ->
        BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
      end)

    assert output =~ "Favn single-node bootstrap complete"
    assert output =~ "active manifest verification: matched"
  end

  test "mix favn.bootstrap.single safely prints tuple active-manifest verification", %{
    root_dir: root_dir
  } do
    manifest_path = write_bootstrap_manifest(root_dir)

    for verification <- [:skipped, :mismatch] do
      {:ok, base_url, _server} = start_bootstrap_server({verification, "mv_other"})

      output =
        capture_io(fn ->
          BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
        end)

      assert output =~ "Favn single-node bootstrap complete"
      assert output =~ "active manifest verification: {:#{verification},"
    end
  end

  test "mix favn.dev raises with partial runtime recovery guidance", %{root_dir: root_dir} do
    log_path = Path.join(root_dir, ".favn/logs/runner.log")
    assert :ok = File.mkdir_p(Path.dirname(log_path))

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "sleep 30"],
      cwd: root_dir,
      log_path: log_path,
      env: %{}
    }

    assert {:ok, info} = DevProcess.start_service(spec)

    runtime = %{
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => info.pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    try do
      assert_raise Mix.Error, ~r/local stack is in a partial\/dead state/, fn ->
        DevTask.run(["--root-dir", root_dir])
      end
    after
      _ = DevProcess.stop_pid(info.pid)
      _ = State.clear_runtime(root_dir: root_dir)
    end
  end

  test "mix favn.status prints stack details", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "storage" => "memory",
      "active_manifest_version_id" => "mv_task_test",
      "orchestrator_base_url" => "http://127.0.0.1:4101",
      "web_base_url" => "http://127.0.0.1:4173",
      "node_names" => %{
        "runner" => "favn_runner_task@localhost",
        "orchestrator" => "favn_orchestrator_task@localhost",
        "control" => "favn_local_ctl_task@localhost"
      },
      "distribution_ports" => %{
        "runner" => 45_101,
        "orchestrator" => 45_102,
        "control" => 45_103
      },
      "services" => %{
        "web" => %{"pid" => pid},
        "orchestrator" => %{
          "pid" => pid,
          "node_name" => "favn_orchestrator_task@localhost",
          "distribution_port" => 45_102
        },
        "runner" => %{
          "pid" => pid,
          "node_name" => "favn_runner_task@localhost",
          "distribution_port" => 45_101
        }
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        StatusTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local dev stack"
    assert output =~ "manifest: mv_task_test"
    assert output =~ "local URLs:"
    assert output =~ "web: running pid=#{pid} url=http://127.0.0.1:4173"
    assert output =~ "orchestrator API: running pid=#{pid} url=http://127.0.0.1:4101"
    assert output =~ "internal control plane:"

    assert output =~
             "runner node: running pid=#{pid} node=favn_runner_task@localhost distribution_port=45101"

    assert output =~
             "orchestrator node: running pid=#{pid} node=favn_orchestrator_task@localhost distribution_port=45102"

    assert output =~ "control node: node=favn_local_ctl_task@localhost distribution_port=45103"
  end

  test "mix favn.status reports stale runtime hint", %{root_dir: root_dir} do
    runtime = %{
      "storage" => "memory",
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        StatusTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "status: stale"
    assert output =~ "hint: run mix favn.stop to clear stale runtime state"
  end

  test "mix favn.run requires a pipeline module" do
    assert_raise Mix.Error, ~r/missing pipeline module/, fn ->
      RunTask.run([])
    end
  end

  test "mix favn.run reports stopped local stack", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      RunTask.run(["Example.Pipeline", "--root-dir", root_dir])
    end
  end

  test "mix favn.run validates numeric options" do
    assert_raise Mix.Error, ~r/--timeout-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--timeout-ms", "0"])
    end

    assert_raise Mix.Error, ~r/--poll-interval-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--poll-interval-ms", "0"])
    end
  end

  test "mix favn.backfill parses submit command" do
    assert {:ok, {:submit, "Example.Pipeline", opts}} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--coverage-baseline-id",
               "baseline_1",
               "--no-wait"
             ])

    assert Keyword.fetch!(opts, :from) == "2026-04-01"
    assert Keyword.fetch!(opts, :to) == "2026-04-07"
    assert Keyword.fetch!(opts, :kind) == "day"
    assert Keyword.fetch!(opts, :timezone) == "Etc/UTC"
    assert Keyword.fetch!(opts, :coverage_baseline_id) == "baseline_1"
    assert Keyword.fetch!(opts, :wait) == false
  end

  test "mix favn.backfill validates submit arguments" do
    assert {:error, message} = BackfillTask.parse_args(["submit", "Example.Pipeline"])
    assert message =~ "missing required option(s): --from, --to, --kind"

    assert {:error, message} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--bad"
             ])

    assert message == "invalid option for mix favn.backfill submit"

    assert {:error, message} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--lookback",
               "2"
             ])

    assert message == "invalid option for mix favn.backfill submit"
  end

  test "mix favn.backfill parses read and rerun commands" do
    assert {:ok, {:windows, "run_1", opts}} =
             BackfillTask.parse_args([
               "windows",
               "run_1",
               "--status",
               "failed",
               "--limit",
               "50",
               "--offset",
               "100"
             ])

    assert Keyword.fetch!(opts, :status) == "failed"
    assert Keyword.fetch!(opts, :limit) == 50
    assert Keyword.fetch!(opts, :offset) == 100

    assert {:ok, {:coverage_baselines, opts}} =
             BackfillTask.parse_args([
               "coverage-baselines",
               "--pipeline-module",
               "Example.Pipeline"
             ])

    assert Keyword.fetch!(opts, :pipeline_module) == "Example.Pipeline"

    assert {:ok, {:asset_window_states, opts}} =
             BackfillTask.parse_args([
               "asset-window-states",
               "--asset-ref-module",
               "Example.Asset",
               "--asset-ref-name",
               "asset"
             ])

    assert Keyword.fetch!(opts, :asset_ref_module) == "Example.Asset"
    assert Keyword.fetch!(opts, :asset_ref_name) == "asset"

    assert {:ok, {:rerun_window, "run_1", opts}} =
             BackfillTask.parse_args(["rerun-window", "run_1", "--window-key", "day:2026-04-01"])

    assert Keyword.fetch!(opts, :window_key) == "day:2026-04-01"

    assert {:ok, {:repair, opts}} =
             BackfillTask.parse_args([
               "repair",
               "--pipeline-module",
               "Example.Pipeline",
               "--apply"
             ])

    assert Keyword.fetch!(opts, :pipeline_module) == "Example.Pipeline"
    assert Keyword.fetch!(opts, :apply) == true
  end

  test "mix favn.backfill validates repair arguments" do
    assert {:error, message} =
             BackfillTask.parse_args([
               "repair",
               "--backfill-run-id",
               "run_1",
               "--pipeline-module",
               "Example.Pipeline"
             ])

    assert message == "expected at most one repair scope: --backfill-run-id or --pipeline-module"

    assert {:error, message} = BackfillTask.parse_args(["repair", "extra"])
    assert message == "unexpected argument for mix favn.backfill repair"
  end

  test "mix favn.backfill reports stopped local stack", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      BackfillTask.run([
        "submit",
        "Example.Pipeline",
        "--from",
        "2026-04-01",
        "--to",
        "2026-04-07",
        "--kind",
        "day",
        "--root-dir",
        root_dir
      ])
    end
  end

  test "mix favn.install writes install state", %{root_dir: root_dir} do
    output =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    assert output =~ "Favn install complete"
    assert {:ok, _install} = State.read_install(root_dir: root_dir)
    assert {:ok, _toolchain} = State.read_toolchain(root_dir: root_dir)
  end

  test "mix favn.init requires duckdb sample flags" do
    assert_raise Mix.Error, ~r/missing required option\(s\): --duckdb, --sample/, fn ->
      InitTask.run([])
    end
  end

  test "mix favn.init rejects root-dir because it targets the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/usage: mix favn.init --duckdb --sample/, fn ->
      InitTask.run(["--root-dir", root_dir, "--duckdb", "--sample"])
    end
  end

  test "mix favn.init prints generated bootstrap summary", %{root_dir: root_dir} do
    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule PublicTaskSample.MixProject do
        use Mix.Project

        def project do
          [app: :public_task_sample, version: "0.1.0", deps: deps()]
        end

        def application do
          []
        end

        defp deps do
          [
            {:favn, path: "../favn/apps/favn"}
          ]
        end
      end
      """
    )

    output =
      File.cd!(root_dir, fn ->
        capture_io(fn ->
          InitTask.run(["--duckdb", "--sample"])
        end)
      end)

    assert output =~ "Favn local bootstrap complete"
    assert output =~ "pipeline: Favn.Pipelines.LocalSmoke"
    assert File.exists?(Path.join(root_dir, "lib/favn/pipelines/local_smoke.ex"))
  end

  test "mix favn.doctor rejects root-dir because it checks the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/usage: mix favn.doctor/, fn ->
      DoctorTask.run(["--root-dir", root_dir, "--skip-compile"])
    end
  end

  test "mix favn.install reports already up to date", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    output =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    assert output =~ "Favn install is already up to date"
  end

  test "mix favn.dev reports runtime compile failures under root_dir", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      Application.put_env(:favn, :local,
        orchestrator_port: free_port(),
        web_port: free_port()
      )

      assert_raise Mix.Error,
                   ~r/(runtime compile failed for runtime_root under --root-dir|local Erlang shortname host is unavailable|port conflict: .* cannot bind port)/,
                   fn ->
                     DevTask.run(["--root-dir", root_dir])
                   end
    after
      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.install reports missing prerequisite tools", %{root_dir: root_dir} do
    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      assert_raise Mix.Error, ~r/install failed: missing required tool node/, fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end

  test "mix favn.logs prints service logs", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert :ok = File.write(Path.join(root_dir, ".favn/logs/web.log"), "hello\n")

    output =
      capture_io(fn ->
        LogsTask.run(["--root-dir", root_dir, "--service", "web"])
      end)

    assert output =~ "hello"
  end

  test "mix favn.reset removes .favn when stack is not running", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert File.dir?(Path.join(root_dir, ".favn"))

    output =
      capture_io(fn ->
        ResetTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local state reset complete"
    refute File.exists?(Path.join(root_dir, ".favn"))
  end

  test "mix favn.build.runner prints build summary" do
    _ =
      capture_io(fn ->
        InstallTask.run(["--skip-web-install", "--skip-runtime-deps-install"])
      end)

    output =
      capture_io(fn ->
        BuildRunnerTask.run([])
      end)

    assert output =~ "Favn runner build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/runner/"
  end

  test "mix favn.build.runner rejects root_dir outside current mix project", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    assert_raise Mix.Error,
                 ~r/runner build is rooted in the current Mix project only/,
                 fn ->
                   BuildRunnerTask.run(["--root-dir", root_dir])
                 end
  end

  test "mix favn.build.web prints build summary", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildWebTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn web build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/web/"
  end

  test "mix favn.build.web reports missing prerequisite tools", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run([
          "--root-dir",
          root_dir,
          "--skip-web-install",
          "--skip-tool-checks"
        ])
      end)

    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      assert_raise Mix.Error, ~r/build blocked: missing required tool node/, fn ->
        BuildWebTask.run(["--root-dir", root_dir])
      end
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end

  test "mix favn.dev reports port conflicts before startup", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      Application.put_env(:favn, :local, orchestrator_port: free_port(), web_port: port)

      assert_raise Mix.Error,
                   ~r/port conflict: web cannot bind port #{port}; free the port and retry/,
                   fn ->
                     DevTask.run(["--root-dir", root_dir])
                   end
    after
      :ok = :gen_tcp.close(socket)

      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.dev reports postgres connectivity failures", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      web_port = free_port()

      Application.put_env(
        :favn,
        :local,
        storage: :postgres,
        orchestrator_port: free_port(),
        web_port: web_port,
        postgres: [
          hostname: "127.0.0.1",
          port: 1,
          username: "postgres",
          password: "postgres",
          database: "favn",
          ssl: false,
          pool_size: 10
        ]
      )

      assert_raise Mix.Error, ~r/postgres unavailable at 127.0.0.1:1/, fn ->
        DevTask.run(["--root-dir", root_dir])
      end
    after
      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.build.orchestrator prints build summary", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildOrchestratorTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn orchestrator build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/orchestrator/"
  end

  test "mix favn.build.single prints build summary" do
    _ =
      capture_io(fn ->
        InstallTask.run(["--skip-web-install", "--skip-runtime-deps-install"])
      end)

    output =
      capture_io(fn ->
        BuildSingleTask.run(["--storage", "sqlite"])
      end)

    assert output =~ "Favn single build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/single/"
  end

  defp write_bootstrap_manifest(root_dir) do
    path = Path.join(root_dir, "bootstrap_manifest.json")

    File.write!(
      path,
      JSON.encode_to_iodata!(%{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })
    )

    path
  end

  defp bootstrap_args(manifest_path, base_url) do
    [
      "--manifest",
      manifest_path,
      "--orchestrator-url",
      base_url,
      "--service-token",
      "token-1"
    ]
  end

  defp start_bootstrap_server(verification) do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: false, packet: :raw])
    {:ok, port} = :inet.port(socket)

    server =
      spawn_link(fn ->
        serve_bootstrap_requests(socket, verification, 5)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp serve_bootstrap_requests(socket, _verification, 0), do: :gen_tcp.close(socket)

  defp serve_bootstrap_requests(socket, verification, remaining) do
    {:ok, client} = :gen_tcp.accept(socket, 2_000)
    {:ok, request} = :gen_tcp.recv(client, 0, 2_000)
    send_bootstrap_response(client, request, verification)
    :gen_tcp.close(client)
    serve_bootstrap_requests(socket, verification, remaining - 1)
  end

  defp send_bootstrap_response(client, request, verification) do
    [request_line | _headers] = String.split(request, "\r\n", parts: 2)
    [_method, path, _version] = String.split(request_line, " ", parts: 3)

    cond do
      path == "/api/orchestrator/v1/bootstrap/service-token" ->
        send_json(client, 200, %{data: %{status: "ok"}})

      path == "/api/orchestrator/v1/manifests" ->
        send_json(client, 200, %{data: %{manifest: %{}}})

      path == "/api/orchestrator/v1/bootstrap/active-manifest" ->
        send_active_manifest_response(client, verification)

      String.contains?(path, "/api/orchestrator/v1/manifests/") and
          String.contains?(path, "/runner/register") ->
        send_json(client, 200, %{data: %{runner: %{}}})

      String.contains?(path, "/api/orchestrator/v1/manifests/") and
          String.contains?(path, "/activate") ->
        send_json(client, 200, %{data: %{activated: true}})

      true ->
        send_json(client, 404, %{error: %{reason: "not_found"}})
    end
  end

  defp send_active_manifest_response(client, {status, manifest_version_id})
       when status in [:matched, :mismatch] do
    send_json(client, 200, %{data: %{manifest_version_id: manifest_version_id}})
  end

  defp send_active_manifest_response(client, {:skipped, _reason}) do
    send_json(client, 503, %{error: %{reason: "unavailable"}})
  end

  defp send_json(client, status, payload) do
    body = JSON.encode!(payload)
    reason = if status == 200, do: "OK", else: "Error"

    :gen_tcp.send(client, [
      "HTTP/1.1 #{status} #{reason}\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n",
      "\r\n",
      body
    ])
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp clear_bootstrap_env do
    bootstrap_env_names()
    |> Enum.each(&System.delete_env/1)
  end

  defp bootstrap_env do
    Map.new(bootstrap_env_names(), fn name -> {name, System.get_env(name)} end)
  end

  defp restore_bootstrap_env(env) when is_map(env) do
    Enum.each(env, fn
      {name, nil} -> System.delete_env(name)
      {name, value} -> System.put_env(name, value)
    end)
  end

  defp bootstrap_env_names do
    [
      "FAVN_BOOTSTRAP_MANIFEST_PATH",
      "FAVN_WEB_ORCHESTRATOR_BASE_URL",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN"
    ]
  end
end
