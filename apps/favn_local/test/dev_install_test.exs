defmodule Favn.Dev.InstallTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Install
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_install_test_#{System.unique_integer([:positive])}")

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

  test "run/1 writes install and toolchain metadata", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Install.run(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    assert :ok = Install.ensure_ready(root_dir: root_dir, skip_tool_checks: true)

    assert {:ok, install} = State.read_install(root_dir: root_dir)
    assert {:ok, toolchain} = State.read_toolchain(root_dir: root_dir)

    assert install["schema_version"] == 1
    assert is_map(install["fingerprint"])
    assert is_binary(get_in(install, ["runtime_inputs", "web", "materialized_root"]))
    assert toolchain["schema_version"] == 1

    assert File.exists?(Path.join(root_dir, ".favn/install/runtimes/web/runtime_input.json"))
    assert File.exists?(Path.join(root_dir, ".favn/install/runtimes/web/source/package.json"))
    assert File.exists?(Path.join(root_dir, ".favn/install/runtimes/orchestrator/source/mix.exs"))
    assert File.exists?(Path.join(root_dir, ".favn/install/runtimes/runner/source/mix.exs"))
  end

  test "run/1 returns already_installed when fingerprint matches", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Install.run(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    assert {:ok, :already_installed} =
             Install.run(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)
  end

  test "ensure_ready/1 returns install_required when install state is missing", %{
    root_dir: root_dir
  } do
    assert {:error, :install_required} = Install.ensure_ready(root_dir: root_dir)
  end

  test "ensure_ready/1 returns install_stale when fingerprint differs", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)

    assert :ok =
             State.write_install(
               %{"schema_version" => 1, "fingerprint" => %{"mix_lock_sha256" => "old"}},
               root_dir: root_dir
             )

    assert {:error, :install_stale} =
             Install.ensure_ready(root_dir: root_dir, skip_tool_checks: true)
  end

  test "ensure_ready/1 returns missing_tool when tool prerequisite is unavailable", %{
    root_dir: root_dir
  } do
    assert {:ok, :installed} =
             Install.run(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      assert {:error, {:missing_tool, :node}} = Install.ensure_ready(root_dir: root_dir)
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end
end
