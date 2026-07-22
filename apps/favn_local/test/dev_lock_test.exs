defmodule Favn.Dev.LockTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Lock
  alias Favn.Dev.Paths

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_lock_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "with_lock/2 runs callback under lock", %{root_dir: root_dir} do
    assert {:ok, :done} =
             Lock.with_lock([root_dir: root_dir], fn ->
               path = Paths.lock_path(root_dir)
               assert File.regular?(path)

               assert {:ok, %{"owner_id" => owner_id, "pid" => pid}} =
                        path |> File.read!() |> JSON.decode()

               assert is_binary(owner_id) and owner_id != ""
               assert pid == System.pid()
               {:ok, :done}
             end)
  end

  test "a lock left by a killed CLI process is recovered", %{root_dir: root_dir} do
    ebin = :favn_local |> :code.lib_dir() |> List.to_string() |> Path.join("ebin")

    expression = """
    Favn.Dev.Lock.with_lock([root_dir: #{inspect(root_dir)}], fn ->
      IO.puts("LOCK_ACQUIRED")
      receive do
        :never -> :ok
      end
    end)
    """

    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: ["-pa", ebin, "-e", expression]
        ]
      )

    assert_receive {^port, {:data, output}}, 5_000
    assert output =~ "LOCK_ACQUIRED"
    assert File.regular?(Paths.lock_path(root_dir))

    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)])
    assert_receive {^port, {:exit_status, _status}}, 5_000

    assert :recovered =
             Lock.with_lock([root_dir: root_dir, lock_timeout_ms: 2_500], fn -> :recovered end)

    refute File.exists?(Paths.lock_path(root_dir))
  end
end
