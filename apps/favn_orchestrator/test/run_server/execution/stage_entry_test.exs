defmodule FavnOrchestrator.RunServer.Execution.StageEntryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution.StageEntry

  test "requires freshness inputs used after runner settlement" do
    attrs = %{
      run_id: "run-stage-entry",
      asset_step_id: "step-stage-entry",
      asset_ref: {__MODULE__, :asset},
      node_key: {{__MODULE__, :asset}, nil},
      window: nil,
      execution_id: "execution-stage-entry",
      runner_execution_id: "execution-stage-entry",
      ownership: :ownership,
      decision: %{},
      attempt: 1,
      stage: 0,
      lease: :lease,
      materialization_claim: :claim,
      execution_pool: :default,
      resource_circuit_permits: [],
      freshness_key: :freshness_key
    }

    assert_raise ArgumentError, ~r/version.*manifest_index.*freshness_context/, fn ->
      StageEntry.new!(attrs)
    end

    assert %{
             version: :version,
             manifest_index: :manifest_index,
             freshness_context: %{now: :now}
           } =
             entry =
             StageEntry.new!(
               Map.merge(attrs, %{
                 version: :version,
                 manifest_index: :manifest_index,
                 freshness_context: %{now: :now}
               })
             )

    assert is_map(entry)
    refute Map.has_key?(entry, :__struct__)
  end
end
