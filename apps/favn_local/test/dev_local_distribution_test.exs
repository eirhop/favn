defmodule Favn.Dev.LocalDistributionTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.LocalDistribution

  test "preflight derives loopback bind IP from short hostname resolution" do
    assert {:ok, distribution} =
             LocalDistribution.preflight(
               localhost: fn -> ~c"wslhost.localdomain" end,
               resolver: fn ~c"wslhost" -> {:ok, [{192, 168, 1, 10}, {127, 0, 1, 1}]} end,
               epmd_executable: false
             )

    assert distribution.short_host == "wslhost"
    assert distribution.bind_ip == "127.0.1.1"
    assert distribution.bind_tuple == {127, 0, 1, 1}

    assert LocalDistribution.application_env(distribution, 45_123) == [
             inet_dist_use_interface: {127, 0, 1, 1},
             inet_dist_listen_min: 45_123,
             inet_dist_listen_max: 45_123
           ]

    assert LocalDistribution.erl_flags(distribution, 45_123) =~ "{127,0,1,1}"
  end

  test "preflight rejects non-loopback short hostname resolution" do
    assert {:error, {:shortname_host_not_loopback, "devhost", [{10, 0, 0, 2}]}} =
             LocalDistribution.preflight(
               localhost: fn -> ~c"devhost" end,
               resolver: fn ~c"devhost" -> {:ok, [{10, 0, 0, 2}]} end,
               epmd_executable: false
             )
  end

  test "preflight reports missing epmd executable" do
    assert {:error, :epmd_executable_missing} =
             LocalDistribution.preflight(
               localhost: fn -> ~c"devhost" end,
               resolver: fn ~c"devhost" -> {:ok, [{127, 0, 0, 1}]} end,
               epmd_executable: nil
             )
  end

  test "preflight autostarts epmd when names check fails" do
    parent = self()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    assert {:ok, distribution} =
             LocalDistribution.preflight(
               localhost: fn -> ~c"devhost" end,
               resolver: fn ~c"devhost" -> {:ok, [{127, 0, 1, 1}]} end,
               epmd_executable: "/fake/epmd",
               epmd_names: fn _opts ->
                 count = Agent.get_and_update(counter, fn count -> {count, count + 1} end)
                 send(parent, {:epmd_names, count})
                 if count == 0, do: {:error, :not_running}, else: :ok
               end,
               epmd_daemon: fn _opts ->
                 send(parent, :epmd_daemon)
                 :ok
               end
             )

    assert distribution.bind_ip == "127.0.1.1"
    assert_received {:epmd_names, 0}
    assert_received :epmd_daemon
    assert_received {:epmd_names, 1}
  end
end
