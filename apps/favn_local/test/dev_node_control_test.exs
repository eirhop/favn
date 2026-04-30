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
end
