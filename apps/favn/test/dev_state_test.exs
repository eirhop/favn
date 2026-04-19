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
    assert File.dir?(Path.join(root_dir, ".favn/data"))
    assert File.dir?(Path.join(root_dir, ".favn/manifests"))
    assert File.dir?(Path.join(root_dir, ".favn/history"))
  end

  test "runtime state roundtrip", %{root_dir: root_dir} do
    runtime = %{"storage" => "memory", "services" => %{"web" => %{"pid" => 123}}}

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)
    assert {:ok, ^runtime} = State.read_runtime(root_dir: root_dir)

    assert :ok = State.clear_runtime(root_dir: root_dir)
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end
end
