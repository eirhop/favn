defmodule Favn.Dev.ResetTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Reset
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_reset_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "run/1 deletes .favn when stack is stopped", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert :ok = File.write(Path.join(root_dir, ".favn/runtime.json"), "{}")

    assert :ok = Reset.run(root_dir: root_dir)
    refute File.exists?(Path.join(root_dir, ".favn"))
  end

  test "run/1 refuses reset when runtime has live pid", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{"services" => %{"web" => %{"pid" => pid}}}
    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    assert {:error, :stack_running} = Reset.run(root_dir: root_dir)
    assert File.exists?(Path.join(root_dir, ".favn"))
  end
end
