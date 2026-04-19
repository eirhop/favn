defmodule Mix.Tasks.Favn.DevTaskTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.State
  alias Mix.Tasks.Favn.Dev

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_task_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "mix favn.dev fails when stack is already marked running", %{root_dir: root_dir} do
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
      Dev.run(["--root-dir", root_dir])
    end
  end
end
