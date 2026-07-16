defmodule Favn.Dev.InstallTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Install
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_install_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_view"))

    File.write!(Path.join(root_dir, "mix.lock"), "lock")
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner/_build/dev/lib/generated"))
    File.write!(Path.join(root_dir, "apps/favn_runner/_build/dev/lib/generated/file"), "beam")
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner/deps/generated"))
    File.write!(Path.join(root_dir, "apps/favn_runner/deps/generated/file"), "dep")
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner/doc/generated"))
    File.write!(Path.join(root_dir, "apps/favn_runner/doc/generated/index.html"), "docs")
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner/tmp/generated"))
    File.write!(Path.join(root_dir, "apps/favn_runner/tmp/generated/file"), "temporary")

    File.write!(
      Path.join(root_dir, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(root_dir, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    File.write!(Path.join(root_dir, "apps/favn_view/mix.exs"), "defmodule View.MixProject do end")

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "run/1 writes install and toolchain metadata", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Install.run(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    assert :ok = Install.ensure_ready(root_dir: root_dir, skip_tool_checks: true)

    assert {:ok, install} = State.read_install(root_dir: root_dir)
    assert {:ok, runtime} = State.read_install_runtime(root_dir: root_dir)
    assert {:ok, toolchain} = State.read_toolchain(root_dir: root_dir)

    assert install["schema_version"] == 3
    assert is_map(install["fingerprint"])
    assert is_binary(get_in(install, ["runtime", "materialized_root"]))
    assert runtime["materialized_root"] == Path.join(root_dir, ".favn/install/runtime_root")
    assert toolchain["schema_version"] == 3

    assert File.exists?(
             Path.join(root_dir, ".favn/install/runtime_root/apps/favn_runner/mix.exs")
           )

    assert File.exists?(
             Path.join(root_dir, ".favn/install/runtime_root/apps/favn_orchestrator/mix.exs")
           )

    assert File.exists?(Path.join(root_dir, ".favn/install/runtime_root/apps/favn_view/mix.exs"))

    refute File.exists?(
             Path.join(root_dir, ".favn/install/runtime_root/apps/favn_view/priv/static/assets")
           )

    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root/apps/favn_runner/_build"))

    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root/apps/favn_runner/deps"))
    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root/apps/favn_runner/doc"))
    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root/apps/favn_runner/tmp"))
  end

  test "run/1 returns already_installed when fingerprint matches", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Install.run(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    assert {:ok, :already_installed} =
             Install.run(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )
  end

  test "ensure_ready/1 returns install_required when install state is missing", %{
    root_dir: root_dir
  } do
    assert {:error, :install_required} = Install.ensure_ready(root_dir: root_dir)
  end

  test "ensure_ready/1 returns install_stale when fingerprint differs", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)

    assert :ok =
             State.write_install_runtime(%{"materialized_root" => root_dir}, root_dir: root_dir)

    assert :ok =
             State.write_install(
               %{"schema_version" => 3, "fingerprint" => %{"consumer_mix_lock_sha256" => "old"}},
               root_dir: root_dir
             )

    assert {:error, :install_stale} =
             Install.ensure_ready(root_dir: root_dir, skip_tool_checks: true)
  end

  test "ensure_ready/1 does not require Node tooling", %{
    root_dir: root_dir
  } do
    assert {:ok, :installed} =
             Install.run(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      assert :ok = Install.ensure_ready(root_dir: root_dir)
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end

  test "run/1 reinstalls when runtime source files change", %{root_dir: root_dir} do
    source_path = Path.join(root_dir, "apps/favn_local/lib/favn/dev/runtime_launch.ex")
    File.mkdir_p!(Path.dirname(source_path))
    File.write!(source_path, "defmodule RuntimeLaunch do\n  def version, do: 1\nend\n")

    install_opts = [
      root_dir: root_dir,
      skip_web_install: true,
      skip_tool_checks: true,
      skip_runtime_deps_install: true
    ]

    assert {:ok, :installed} = Install.run(install_opts)
    assert {:ok, first_install} = State.read_install(root_dir: root_dir)

    File.write!(source_path, "defmodule RuntimeLaunch do\n  def version, do: 2\nend\n")

    assert {:ok, :installed} = Install.run(install_opts)
    assert {:ok, second_install} = State.read_install(root_dir: root_dir)

    assert get_in(first_install, [
             "fingerprint",
             "runtime_source",
             "runtime_source_tree",
             "sha256"
           ]) !=
             get_in(second_install, [
               "fingerprint",
               "runtime_source",
               "runtime_source_tree",
               "sha256"
             ])
  end

  test "failed forced reinstall cannot leave stale install state ready", %{root_dir: root_dir} do
    install_opts = [
      root_dir: root_dir,
      skip_web_install: true,
      skip_tool_checks: true,
      skip_runtime_deps_install: true
    ]

    assert {:ok, :installed} = Install.run(install_opts)
    File.write!(Path.join(root_dir, "mix.exs"), "raise \"invalid runtime project\"\n")

    failing_runner = fn _mix, ["deps.get"], _opts -> {"dependency resolution failed", 7} end

    assert {:error, {:runtime_deps_install_failed, 7, "dependency resolution failed"}} =
             Install.run(
               root_dir: root_dir,
               force: true,
               skip_web_install: true,
               skip_tool_checks: true,
               runtime_deps_command_runner: failing_runner
             )

    assert {:error, :install_required} = Install.ensure_ready(root_dir: root_dir)
  end
end
