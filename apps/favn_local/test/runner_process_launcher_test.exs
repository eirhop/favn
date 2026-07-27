defmodule FavnLocal.RunnerProcessLauncherTest do
  use ExUnit.Case, async: true

  alias FavnLocal.RunnerProcessLauncher

  test "stopping an unregistered runner closes its child port" do
    port = Port.open({:spawn_driver, ~c"ram_file_drv"}, [:binary])

    assert Port.info(port)

    assert :ok =
             RunnerProcessLauncher.stop(%{
               port: port,
               node: nil,
               release_id: "rr_test",
               runner_instance_id: "runner-test"
             })

    assert_eventually(fn -> is_nil(Port.info(port)) end)
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
