defmodule Favn.Contracts.RunnerClientTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerClient

  test "declares orchestrator-to-runner callback surface" do
    assert RunnerClient.behaviour_info(:callbacks) |> Enum.sort() ==
             [
               await_result: 3,
               cancel_work: 3,
               inspect_relation: 2,
               register_manifest: 2,
               submit_work: 2
             ]
  end
end
