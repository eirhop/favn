defmodule FavnRunner.ControlPlaneConnectionTest do
  use ExUnit.Case, async: true

  alias FavnRunner.ControlPlaneConnection

  test "connects only to the explicit configured control-plane node" do
    connection = start_supervised!({ControlPlaneConnection, name: nil, node: node()})

    assert_eventually(fn ->
      ControlPlaneConnection.gateway(connection) ==
        {:ok, {:"Elixir.FavnOrchestrator.RunnerGateway", node()}}
    end)
  end

  test "rejects malformed node names before attempting distribution" do
    Process.flag(:trap_exit, true)

    assert {:error, :invalid_control_plane_node} =
             ControlPlaneConnection.start_link(name: nil, node: "not a node")
  end

  defp assert_eventually(fun, attempts \\ 50)
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
