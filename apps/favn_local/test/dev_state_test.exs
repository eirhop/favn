defmodule Favn.Dev.StateTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_state_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "ensure_layout/1 creates .favn folders", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert File.dir?(Path.join(root_dir, ".favn"))
    assert File.dir?(Path.join(root_dir, ".favn/logs"))
    assert File.dir?(Path.join(root_dir, ".favn/install"))
    assert File.dir?(Path.join(root_dir, ".favn/install/cache"))
    assert File.dir?(Path.join(root_dir, ".favn/install/cache/npm"))
    assert File.dir?(Path.join(root_dir, ".favn/install/runtimes"))
    assert File.dir?(Path.join(root_dir, ".favn/install/runtime_root"))
    assert File.dir?(Path.join(root_dir, ".favn/install/runtimes/web"))
    assert File.dir?(Path.join(root_dir, ".favn/install/runtimes/orchestrator"))
    assert File.dir?(Path.join(root_dir, ".favn/install/runtimes/runner"))
    assert File.dir?(Path.join(root_dir, ".favn/build"))
    assert File.dir?(Path.join(root_dir, ".favn/dist"))
    assert File.dir?(Path.join(root_dir, ".favn/data"))
    assert File.dir?(Path.join(root_dir, ".favn/manifests"))
    assert File.dir?(Path.join(root_dir, ".favn/manifests/cache"))
    assert File.dir?(Path.join(root_dir, ".favn/history"))
    assert File.dir?(Path.join(root_dir, ".favn/history/failures"))
  end

  test "runtime state roundtrip", %{root_dir: root_dir} do
    runtime = %{"storage" => "memory", "services" => %{"web" => %{"pid" => 123}}}

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)
    assert {:ok, ^runtime} = State.read_runtime(root_dir: root_dir)

    assert :ok = State.clear_runtime(root_dir: root_dir)
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end

  test "install and toolchain state roundtrip", %{root_dir: root_dir} do
    install = %{"schema_version" => 2, "fingerprint" => %{"consumer_mix_lock_sha256" => "abc"}}
    runtime = %{"schema_version" => 1, "materialized_root" => "/tmp/runtime"}
    toolchain = %{"schema_version" => 1, "node_version" => "v22.1.0"}

    assert :ok = State.write_install(install, root_dir: root_dir)
    assert :ok = State.write_install_runtime(runtime, root_dir: root_dir)
    assert :ok = State.write_toolchain(toolchain, root_dir: root_dir)

    assert {:ok, ^install} = State.read_install(root_dir: root_dir)
    assert {:ok, ^runtime} = State.read_install_runtime(root_dir: root_dir)
    assert {:ok, ^toolchain} = State.read_toolchain(root_dir: root_dir)
  end
end
