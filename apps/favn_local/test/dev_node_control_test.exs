defmodule Favn.Dev.NodeControlTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.DistributedErlang
  alias Favn.Dev.NodeControl

  test "shortname_to_full/1 uses canonical shortname host" do
    raw_host = :net_adm.localhost() |> List.to_string()

    assert {:ok, full_name} = NodeControl.shortname_to_full("favn_runner_test")

    [name, host] = String.split(full_name, "@", parts: 2)

    assert name == "favn_runner_test"
    assert host != ""
    refute String.contains?(host, ".")

    if String.contains?(raw_host, ".") do
      refute host == raw_host
    end
  end

  test "distributed Erlang atom conversion validates node names and cookies first" do
    assert {:ok, :favn_runner_test@localhost} =
             DistributedErlang.node_name_to_atom("favn_runner_test@localhost")

    assert {:ok, :favn_local_ctl_test} =
             DistributedErlang.short_node_name_to_atom("favn_local_ctl_test")

    assert {:ok, :FAVN_RPC_COOKIE_123} = DistributedErlang.cookie_to_atom("FAVN_RPC_COOKIE_123")

    assert {:error, {:invalid_node_name, "favn runner@localhost"}} =
             DistributedErlang.node_name_to_atom("favn runner@localhost")

    assert {:error, {:invalid_node_name, "favn_runner@local.host"}} =
             DistributedErlang.node_name_to_atom("favn_runner@local.host")

    assert {:error, {:invalid_rpc_cookie, "bad-cookie"}} =
             DistributedErlang.cookie_to_atom("bad-cookie")
  end

  test "shortname_to_full/1 rejects malformed short names" do
    assert {:error, {:invalid_node_name, "bad name"}} = NodeControl.shortname_to_full("bad name")
    assert {:error, {:invalid_node_name, "bad@name"}} = NodeControl.shortname_to_full("bad@name")
  end

  test "ensure_local_node_started/2 configures control node from shared preflight" do
    parent = self()

    assert :ok =
             NodeControl.ensure_local_node_started("COOKIE",
               name: "favn_local_ctl_test",
               distribution_port: 45_125,
               node_alive?: fn -> false end,
               node_start: fn name, opts ->
                 send(parent, {:node_start, name, opts})
                 {:ok, self()}
               end,
               set_cookie: fn cookie -> send(parent, {:cookie, cookie}) end,
               put_system_env: fn key, value -> send(parent, {:system_env, key, value}) end,
               put_application_env: fn app, key, value ->
                 send(parent, {:application_env, app, key, value})
               end,
                local_distribution: [
                  localhost: fn -> ~c"WSLHOST" end,
                  resolver: fn ~c"WSLHOST" -> {:ok, [{127, 0, 1, 1}]} end,
                  epmd_executable: false
                ]
              )

    assert_received {:system_env, "ERL_EPMD_ADDRESS", "127.0.1.1"}
    assert_received {:application_env, :kernel, :inet_dist_use_interface, {127, 0, 1, 1}}
    assert_received {:application_env, :kernel, :inet_dist_listen_min, 45_125}
    assert_received {:application_env, :kernel, :inet_dist_listen_max, 45_125}
    assert_received {:node_start, :favn_local_ctl_test, [name_domain: :shortnames]}
    assert_received {:cookie, :COOKIE}
  end

  test "ensure_local_node_started/2 returns actionable preflight errors" do
    assert {:error,
            {:local_distribution_preflight_failed, :epmd_executable_missing,
             "local Erlang distribution requires epmd" <> _message}} =
             NodeControl.ensure_local_node_started("COOKIE",
               node_alive?: fn -> false end,
               local_distribution: [
                 localhost: fn -> ~c"devhost" end,
                 resolver: fn ~c"devhost" -> {:ok, [{127, 0, 0, 1}]} end,
                 epmd_executable: nil
               ]
             )
  end
end
