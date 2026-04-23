defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.State
  alias Mix.Tasks.Favn.Build.Orchestrator, as: BuildOrchestratorTask
  alias Mix.Tasks.Favn.Build.Runner, as: BuildRunnerTask
  alias Mix.Tasks.Favn.Build.Single, as: BuildSingleTask
  alias Mix.Tasks.Favn.Build.Web, as: BuildWebTask
  alias Mix.Tasks.Favn.Dev, as: DevTask
  alias Mix.Tasks.Favn.Install, as: InstallTask
  alias Mix.Tasks.Favn.Logs, as: LogsTask
  alias Mix.Tasks.Favn.Reset, as: ResetTask
  alias Mix.Tasks.Favn.Status, as: StatusTask

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
      "services" => %{
        "web" => %{"pid" => pid},
        "orchestrator" => %{"pid" => pid},
        "runner" => %{"pid" => pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        StatusTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local dev stack"
    assert output =~ "manifest: mv_task_test"
    assert output =~ "web:"
    assert output =~ "orchestrator:"
    assert output =~ "runner:"
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

  test "mix favn.install writes install state", %{root_dir: root_dir} do
    output =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    assert output =~ "Favn install complete"
    assert {:ok, _install} = State.read_install(root_dir: root_dir)
    assert {:ok, _toolchain} = State.read_toolchain(root_dir: root_dir)
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

    assert_raise Mix.Error,
                 ~r/(runtime compile failed for runtime_root under --root-dir|local Erlang shortname host is unavailable)/,
                 fn ->
                   DevTask.run(["--root-dir", root_dir])
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
    current_root = File.cwd!()

    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", current_root, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildRunnerTask.run(["--root-dir", current_root])
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
      Application.put_env(:favn, :local, web_port: port)

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
      Application.put_env(
        :favn,
        :local,
        storage: :postgres,
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
    current_root = File.cwd!()

    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", current_root, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildSingleTask.run(["--root-dir", current_root, "--storage", "sqlite"])
      end)

    assert output =~ "Favn single build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/single/"
  end
end
