defmodule FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpointTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunnerTaskContext
  alias Favn.Manifest.Index
  alias Favn.Plan
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpoint
  alias FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation
  alias FavnOrchestrator.RunState

  test "checkpoint payload excludes immutable manifest assets and duplicate state aliases" do
    state = freshness_state()

    base =
      context(%{
        {{__MODULE__, :source}, "latest"} => state,
        {{__MODULE__, :source}, nil} => state,
        {{__MODULE__, :source}, :node} => state
      })

    small = Map.put(base, :assets_by_ref, %{{__MODULE__, :source} => %{sql: "small"}})

    large =
      Map.put(
        base,
        :assets_by_ref,
        Map.new(1..100_000, fn index -> {index, %{definition: String.duplicate("x", 128)}} end)
      )

    assert {:ok, small_payload} =
             PipelineFreshnessCheckpoint.encode_payload("run_checkpoint", small)

    assert {:ok, large_payload} =
             PipelineFreshnessCheckpoint.encode_payload("run_checkpoint", large)

    assert small_payload == large_payload
    assert byte_size(large_payload) < 4_096

    assert {:ok, decoded} = PipelineFreshnessCheckpoint.decode_payload(large_payload)
    assert decoded.prior_states == [state]
    refute Map.has_key?(decoded, :assets_by_ref)

    manifest_asset = %{ref: {__MODULE__, :source}, freshness: nil}
    manifest_index = %Index{assets_by_ref: %{manifest_asset.ref => manifest_asset}}
    run = %RunState{id: "run_checkpoint", plan: %Plan{nodes: %{}}}

    assert {:ok, restored} =
             PipelineFreshnessCheckpoint.restore_payload(large_payload, run, manifest_index)

    assert restored.assets_by_ref == manifest_index.assets_by_ref
    assert restored.prior_states[{{__MODULE__, :source}, "latest"}] == state
    assert restored.current_states[{{__MODULE__, :source}, nil}] == state

    assert {:error, :run_execution_checkpoint_workspace_required} =
             PipelineFreshnessCheckpoint.load(run, manifest_index)

    assert {:error, :invalid_run_execution_checkpoint_owner} =
             PipelineFreshnessCheckpoint.put(run, 0, 1, base, nil)
  end

  test "runner-task continuation size is independent of the run freshness context" do
    reference = %{
      version: 1,
      revision: 3,
      sequence: 42,
      stage: 2,
      attempt: 1,
      payload_hash: :crypto.hash(:sha256, "checkpoint")
    }

    continuation =
      PipelineTaskContinuation.new!(%{
        decision: %{
          decision: :run,
          reason: :upstream_refreshed,
          node_key: {{__MODULE__, :source}, nil},
          freshness_key: "latest"
        },
        materialization_claim: nil,
        resource_circuit_permits: [],
        freshness_checkpoint: reference,
        freshness_key: "latest"
      })

    assert {:ok, envelope} = RunnerTaskContext.encode(continuation)
    assert byte_size(Jason.encode!(envelope)) < 4_096
    refute Map.has_key?(continuation, :freshness_context)
    refute Map.has_key?(continuation, :assets_by_ref)

    refute PipelineTaskContinuation.valid?(%{
             continuation
             | freshness_checkpoint: Map.put(reference, :embedded_context, %{})
           })

    legacy_context =
      continuation
      |> Map.delete(:freshness_checkpoint)
      |> Map.put(:freshness_context, %{now: ~U[2026-07-29 10:00:00Z]})

    refute PipelineTaskContinuation.valid?(legacy_context)
    assert {:error, _} = RunnerTaskContext.encode(legacy_context)
  end

  defp context(states) do
    %{
      assets_by_ref: %{},
      refresh_policy: %RefreshPolicy{mode: :auto},
      forced_node_keys: MapSet.new(),
      prior_states: states,
      current_states: states,
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: ~U[2026-07-29 10:00:00Z]
    }
  end

  defp freshness_state do
    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__,
        asset_ref_name: :source,
        freshness_key: "latest",
        evidence_generation_id: "ag_#{String.duplicate("a", 64)}",
        status: :ok,
        freshness_version: "v1",
        latest_success_run_id: "run_prior",
        latest_success_node_key: {{__MODULE__, :source}, nil},
        latest_success_at: ~U[2026-07-29 09:00:00Z],
        latest_attempt_run_id: "run_prior",
        latest_attempt_status: :ok,
        latest_attempt_at: ~U[2026-07-29 09:00:00Z],
        manifest_version_id: "manifest",
        manifest_content_hash: String.duplicate("b", 64),
        input_versions: [],
        metadata: %{},
        updated_at: ~U[2026-07-29 09:00:00Z]
      })

    state
  end
end
