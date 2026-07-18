defmodule FavnOrchestrator.RunServer.RecoveryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Recovery
  alias FavnOrchestrator.RunState

  test "resumes only fresh runs or explicit retry checkpoints" do
    fresh = run_state(event_seq: 2)
    assert :resume = Recovery.assess(fresh, evidence())

    progressed = run_state(event_seq: 3)

    assert {:uncertain, %{reason: :continuation_position_not_durable}} =
             Recovery.assess(progressed, evidence())

    retrying =
      run_state(
        event_seq: 4,
        metadata: %{retry_state: sequential_retry_checkpoint()}
      )

    assert :resume = Recovery.assess(retrying, evidence(any?: true))
  end

  test "an accepted execution is uncertain even at a retry checkpoint" do
    retrying =
      run_state(
        event_seq: 4,
        metadata: %{retry_state: sequential_retry_checkpoint()}
      )

    assert {:uncertain, %{reason: :runner_execution_may_have_been_accepted}} =
             Recovery.assess(retrying, evidence(active: [execution(:started)], any?: true))
  end

  test "malformed retry metadata never authorizes recovery" do
    for retry_state <- [
          %{},
          %{kind: :unknown},
          %{kind: :sequential, retry: %{}},
          %{kind: :pipeline, checkpoint_sequence: 3, stage_index: 0}
        ] do
      run = run_state(event_seq: 4, metadata: %{retry_state: retry_state})

      assert {:uncertain, %{reason: :invalid_retry_checkpoint}} =
               Recovery.assess(run, evidence(any?: true))
    end
  end

  test "retry checkpoint kind must match the run execution mode" do
    pipeline_with_sequential =
      run_state(
        event_seq: 4,
        submit_kind: :pipeline,
        metadata: %{retry_state: sequential_retry_checkpoint()}
      )

    assert {:uncertain, %{reason: :invalid_retry_checkpoint}} =
             Recovery.assess(pipeline_with_sequential, evidence(any?: true))

    sequential_with_pipeline =
      run_state(
        event_seq: 4,
        metadata: %{retry_state: pipeline_retry_checkpoint()}
      )

    assert {:uncertain, %{reason: :invalid_retry_checkpoint}} =
             Recovery.assess(sequential_with_pipeline, evidence(any?: true))
  end

  defp run_state(overrides) do
    base = %RunState{
      id: "run-recovery",
      workspace_id: "workspace-recovery",
      manifest_version_id: "manifest-recovery",
      manifest_content_hash: String.duplicate("a", 64),
      asset_ref: {__MODULE__, :asset},
      status: :running,
      event_seq: 2,
      metadata: %{}
    }

    struct!(base, overrides)
  end

  defp execution(status) do
    now = DateTime.utc_now()

    %RunExecutionOwnership{
      ownership_id: "ownership-recovery",
      run_id: "run-recovery",
      asset_step_id: "step-recovery",
      runner_execution_id: "execution-recovery",
      dispatch_id: "execution-recovery",
      attempt: 1,
      stage: 0,
      status: status,
      inserted_at: now,
      updated_at: now
    }
  end

  defp evidence(overrides \\ []) do
    Map.merge(%{active: [], active_truncated?: false, any?: false}, Map.new(overrides))
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
