defmodule Favn.Contracts.RunnerClientTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerClient

  test "declares orchestrator-to-runner callback surface" do
    assert RunnerClient.behaviour_info(:callbacks) |> Enum.sort() ==
             [
               acquire_manifest: 5,
               await_result: 3,
               cancel_work: 3,
               diagnostics: 1,
               ensure_manifest: 3,
               inspect_relation: 2,
               register_manifest: 2,
               release_manifest: 2,
               renew_manifest: 3,
               resolve_runtime_inputs: 2,
               submit_work: 2,
               subscribe_execution_logs: 3,
               unsubscribe_execution_logs: 3
             ]

    assert RunnerClient.behaviour_info(:optional_callbacks) |> Enum.sort() ==
             [
               diagnostics: 1,
               resolve_runtime_inputs: 2,
               subscribe_execution_logs: 3,
               unsubscribe_execution_logs: 3
             ]
  end
end
