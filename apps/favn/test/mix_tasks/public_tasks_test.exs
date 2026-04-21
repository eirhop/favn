defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

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
