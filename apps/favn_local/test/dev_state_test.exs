defmodule Favn.Dev.StateTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Paths
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(native_tmp_dir(), "favn_dev_state_test_#{System.unique_integer([:positive])}")

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
    assert File.dir?(Path.join(root_dir, ".favn/compose"))
    assert File.dir?(Path.join(root_dir, ".favn/build"))
    assert File.dir?(Path.join(root_dir, ".favn/build/control-plane"))
    assert File.dir?(Path.join(root_dir, ".favn/build/runner"))
    assert File.dir?(Path.join(root_dir, ".favn/dist"))
    assert File.dir?(Path.join(root_dir, ".favn/dist/runner"))
    assert File.dir?(Path.join(root_dir, ".favn/dist/manifest"))
    refute File.exists?(Path.join(root_dir, ".favn/build/web"))
    refute File.exists?(Path.join(root_dir, ".favn/build/orchestrator"))
    refute File.exists?(Path.join(root_dir, ".favn/build/single"))
    assert File.dir?(Path.join(root_dir, ".favn/data"))
    assert File.dir?(Path.join(root_dir, ".favn/manifests"))
    assert File.dir?(Path.join(root_dir, ".favn/manifests/cache"))
    assert File.dir?(Path.join(root_dir, ".favn/history"))
    assert File.dir?(Path.join(root_dir, ".favn/history/failures"))
  end

  test "runtime state roundtrip", %{root_dir: root_dir} do
    runtime = %{
      "schema_version" => 5,
      "kind" => "docker_compose",
      "compose_project" => "favn-test"
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)
    assert {:ok, ^runtime} = State.read_runtime(root_dir: root_dir)

    assert :ok = State.clear_runtime(root_dir: root_dir)
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end

  test "state writes return encode errors instead of raising", %{root_dir: root_dir} do
    assert {:error, {:encode_failed, _path, _reason}} =
             State.write_runtime(%{"pid" => self()}, root_dir: root_dir)

    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end

  test "state writes replace files without leaving partial temporaries", %{root_dir: root_dir} do
    assert :ok =
             State.write_runtime(%{"schema_version" => 1, "value" => "first"}, root_dir: root_dir)

    assert :ok =
             State.write_runtime(%{"schema_version" => 1, "value" => "second"},
               root_dir: root_dir
             )

    assert {:ok, %{"value" => "second"}} = State.read_runtime(root_dir: root_dir)

    runtime_path = Paths.runtime_path(root_dir)
    assert Path.wildcard(runtime_path <> ".tmp.*") == []

    if match?({:unix, _}, :os.type()) do
      assert {:ok, %{mode: mode}} = File.stat(runtime_path)
      assert Bitwise.band(mode, 0o077) == 0
    end
  end

  test "install state roundtrip has no source-runtime sidecar", %{root_dir: root_dir} do
    install = %{
      "schema_version" => 4,
      "image_reference" => "ghcr.io/eirhop/favn-control-plane@sha256:test"
    }

    assert :ok = State.write_install(install, root_dir: root_dir)
    assert {:ok, ^install} = State.read_install(root_dir: root_dir)
    refute File.exists?(Path.join(root_dir, ".favn/install/runtime.json"))
    refute File.exists?(Path.join(root_dir, ".favn/install/toolchain.json"))
  end

  test "maintenance lease is private, durable, and explicitly cleared", %{root_dir: root_dir} do
    maintenance = %{
      "schema_version" => 1,
      "kind" => "runner_replacement",
      "token" => String.duplicate("a", 43)
    }

    assert :ok = State.write_maintenance(maintenance, root_dir: root_dir)
    assert {:ok, ^maintenance} = State.read_maintenance(root_dir: root_dir)

    if match?({:unix, _}, :os.type()) do
      assert {:ok, %{mode: mode}} = File.stat(Paths.maintenance_path(root_dir))
      assert Bitwise.band(mode, 0o077) == 0
    end

    assert :ok = State.clear_maintenance(root_dir: root_dir)
    assert {:error, :not_found} = State.read_maintenance(root_dir: root_dir)
  end

  defp native_tmp_dir do
    if match?({:unix, _}, :os.type()) and File.dir?("/tmp"), do: "/tmp", else: System.tmp_dir!()
  end
end
