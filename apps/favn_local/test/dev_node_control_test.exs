defmodule Favn.Dev.NodeControlTest do
  use ExUnit.Case, async: false

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
end
