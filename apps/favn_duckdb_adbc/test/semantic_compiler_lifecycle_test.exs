defmodule FavnDuckdbADBC.SemanticCompilerLifecycleTest do
  use ExUnit.Case, async: true

  @moduletag :adbc_integration

  alias FavnDuckdbADBC.SemanticCompiler

  test "outer deadline retains acknowledged native PID and process group" do
    script = """
    import json, os, sys
    print(json.dumps({"process": {"supervisor_pid": os.getpid(), "worker_pid": os.getpid(), "process_group": os.getpid()}}), flush=True)
    sys.stdin.read()
    """

    port =
      Port.open(
        {:spawn_executable, System.find_executable("python3")},
        [:binary, :exit_status, :use_stdio, args: ["-I", "-c", script]]
      )

    {:os_pid, pid} = Port.info(port, :os_pid)

    try do
      assert_receive {^port, {:data, announcement}}, 5_000
      send(self(), {port, {:data, announcement}})

      assert {:error,
              {:semantic_worker_cleanup_unconfirmed,
               %{supervisor_pid: ^pid, worker_pid: ^pid, process_group: ^pid}}} =
               SemanticCompiler.await_worker(port, 10)
    after
      Port.close(port)
    end
  end

  test "outer deadline before handshake identifies the supervised OS process" do
    port =
      Port.open(
        {:spawn_executable, System.find_executable("python3")},
        [:binary, :exit_status, :use_stdio, args: ["-I", "-c", "import sys; sys.stdin.read()"]]
      )

    {:os_pid, pid} = Port.info(port, :os_pid)

    try do
      assert {:error,
              {:semantic_worker_cleanup_unconfirmed,
               %{supervisor_pid: ^pid, worker_pid: nil, process_group: nil}}} =
               SemanticCompiler.await_worker(port, 10)
    after
      Port.close(port)
    end
  end

  test "worker confirms exit, escalates blocked calls and handles caller or supervisor death" do
    assert {"ok\n", 0} =
             System.cmd("python3", ["-I", Path.join(__DIR__, "semantic_compiler_lifecycle.py")])
  end
end
