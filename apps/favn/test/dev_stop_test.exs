defmodule Favn.Dev.StopTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_stop_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "stop/1 clears stale runtime state", %{root_dir: root_dir} do
    runtime = %{
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)
    assert :ok = Dev.stop(root_dir: root_dir)
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end
end
