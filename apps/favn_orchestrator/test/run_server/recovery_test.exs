defmodule FavnOrchestrator.RunServer.RecoveryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Recovery
  alias FavnOrchestrator.RunServer.Execution.RecoveryPosition
  alias FavnOrchestrator.RunState

  test "resumes only fresh runs or explicit retry checkpoints" do
    fresh = run_state(event_seq: 2)
    assert {:ok, :resume} = Recovery.disposition(fresh)

    progressed = run_state(event_seq: 3)

    assert {:ok, {:uncertain, %{reason: :continuation_position_not_durable}}} =
             Recovery.disposition(progressed)

    retrying =
      run_state(
        event_seq: 4,
        metadata: %{retry_state: sequential_retry_checkpoint()}
      )

    assert {:ok, :resume} = Recovery.disposition(retrying)
  end

  test "sequential admission cannot reset a malformed persisted deadline" do
    checkpoint = sequential_retry_checkpoint()
    checkpoint = put_in(checkpoint, [:retry, :admission_deadline_ms], "invalid")
    run = run_state(event_seq: 4, metadata: %{retry_state: checkpoint})
    assert {:ok, {:uncertain, %{reason: :invalid_retry_checkpoint}}} = Recovery.disposition(run)
  end

  test "malformed retry metadata never authorizes recovery" do
    for retry_state <- [
          %{},
          %{kind: :unknown},
          %{kind: :sequential, retry: %{}},
          %{kind: :pipeline, checkpoint_sequence: 3, stage_index: 0}
        ] do
      run = run_state(event_seq: 4, metadata: %{retry_state: retry_state})

      assert {:ok, {:uncertain, %{reason: :invalid_retry_checkpoint}}} =
               Recovery.disposition(run)
    end
  end

  test "retry checkpoint kind must match the run execution mode" do
    pipeline_with_sequential =
      run_state(
        event_seq: 4,
        submit_kind: :pipeline,
        metadata: %{retry_state: sequential_retry_checkpoint()}
      )

    assert {:ok, {:uncertain, %{reason: :invalid_retry_checkpoint}}} =
             Recovery.disposition(pipeline_with_sequential)

    sequential_with_pipeline =
      run_state(
        event_seq: 4,
        metadata: %{retry_state: pipeline_retry_checkpoint()}
      )

    assert {:ok, {:uncertain, %{reason: :invalid_retry_checkpoint}}} =
             Recovery.disposition(sequential_with_pipeline)
  end

  test "active recovery fails closed once any node outcome is durable" do
    run =
      run_state(
        metadata: %{active_runner_task_ids: ["rt_active"]},
        result: %{node_results: [%{node_key: {{__MODULE__, :asset}, nil}, status: :ok}]}
      )
      |> RecoveryPosition.record_outcome(0, 1)
      |> RunState.for_step_persistence()

    assert run.result == nil

    assert {:ok,
            {:uncertain,
             %{
               reason: :active_stage_outcomes_not_resumable,
               active_runner_task_count: 1,
               runner_tasks: ["rt_active"]
             }}} = Recovery.disposition(run)
  end

  defp run_state(overrides) do
    base = %RunState{
      id: "run-recovery",
      workspace_id: "workspace-recovery",
      manifest_version_id: "manifest-recovery",
      manifest_content_hash: String.duplicate("a", 64),
      runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
      asset_ref: {__MODULE__, :asset},
      status: :running,
      event_seq: 2,
      metadata: %{}
    }

    struct!(base, overrides)
  end

  defp sequential_retry_checkpoint do
    %{
      kind: :sequential,
      sequential_index: 0,
      next_retry_at: System.system_time(:millisecond),
      retry: %{
        asset_ref: {__MODULE__, :asset},
        node_key: {{__MODULE__, :asset}, nil},
        asset_step_id: "step-recovery",
        stage: 0,
        next_attempt: 2,
        retry_after_ms: 0
      }
    }
  end

  defp pipeline_retry_checkpoint do
    %{
      kind: :pipeline,
      checkpoint_sequence: 4,
      stage_index: 0,
      next_attempt: 2,
      stage: 0,
      next_retry_at: System.system_time(:millisecond)
    }
  end
end
