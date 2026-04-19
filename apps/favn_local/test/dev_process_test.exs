defmodule Favn.Dev.ProcessTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Process

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_process_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "start_service/1 and stop_pid/1 manage service lifecycle", %{root_dir: root_dir} do
    log_path = Path.join(root_dir, "service.log")

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "echo ready; sleep 30"],
      cwd: root_dir,
      log_path: log_path,
      env: %{}
    }

    assert {:ok, info} = Process.start_service(spec)
    assert is_integer(info.pid) and info.pid > 0
    assert Process.alive?(info.pid)

    assert :ok = Process.stop_pid(info.pid)
    refute Process.alive?(info.pid)
  end
end
