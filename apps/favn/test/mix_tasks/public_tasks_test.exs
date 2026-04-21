defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Dev.State
  alias Mix.Tasks.Favn.Dev, as: DevTask
  alias Mix.Tasks.Favn.Status, as: StatusTask

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_public_tasks_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

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
end
