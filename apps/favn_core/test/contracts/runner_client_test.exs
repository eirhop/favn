defmodule Favn.Contracts.RunnerClientTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerClient

  test "declares orchestrator-to-runner callback surface" do
    assert RunnerClient.behaviour_info(:callbacks) |> Enum.sort() ==
             [
               acquire_manifest: 5,
               activate_generation: 2,
               await_result: 3,
               cancel_work: 3,
               diagnostics: 1,
               discard_generation: 2,
               ensure_manifest: 2,
               generation_capabilities: 3,
               generation_marker: 3,
               initialize_generation_marker: 2,
               inspect_relation: 2,
               reconcile_generation: 2,
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
               activate_generation: 2,
               diagnostics: 1,
               discard_generation: 2,
               generation_capabilities: 3,
               generation_marker: 3,
               initialize_generation_marker: 2,
               reconcile_generation: 2,
               resolve_runtime_inputs: 2,
               subscribe_execution_logs: 3,
               unsubscribe_execution_logs: 3
             ]
  end
end
