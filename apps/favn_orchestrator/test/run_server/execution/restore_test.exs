defmodule FavnOrchestrator.RunServer.Execution.RestoreTest do
  alias FavnTestSupport.ExecutionDriver

  use ExUnit.Case, async: false

  alias Favn.Contracts.{RunnerResult, RunnerWork}
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Persistence.{Runtime, Stores}
  alias FavnOrchestrator.RunServer.Execution.{RecoveredTask, Restore, RunExecutionState}
  alias FavnOrchestrator.RunState

  defmodule Store do
    def get(_query), do: {:ok, Process.get(:restore_large_task)}
  end

  setup do
    stores = struct(Stores, Map.new(Map.keys(Map.from_struct(struct(Stores))), &{&1, Store}))
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    :ok
  end

  test "restoration retains compact entries instead of large payloads and results" do
    ref = {__MODULE__, :asset}
    key = {ref, nil}

    run =
      RunState.new(
        id: "large-restore",
        workspace_id: "workspace",
        asset_ref: ref,
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        manifest_version_id: "manifest",
        manifest_content_hash: String.duplicate("a", 64),
        plan: %Favn.Plan{dependencies: :none, nodes: %{key => %{ref: ref, stage: 0}}}
      )

    id = AssetStepIdentity.asset_step_id(run.id, key, ref)

    work = %RunnerWork{
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      asset_ref: ref,
      asset_step_id: id,
      stage: 0,
      attempt: 1,
      metadata: %{node_key: key},
      deadline_at: DateTime.add(DateTime.utc_now(), 120, :second),
      params: %{"large_input" => String.duplicate("x", 4_000_000)}
    }

    task = %{
      task_id: "large-task",
      payload: work,
      status: :succeeded,
      result: %RunnerResult{
        status: :ok,
        metadata: %{"large_result" => String.duplicate("y", 4_000_000)}
      },
      runner_pool: "default",
      required_runner_release_id: "release",
      assignment_generation: 1,
      result_version: 1,
      orchestration_context: %{kind: :sequential, materialization_claim: nil},
      payload_hash: <<1::256>>,
      orchestration_context_hash: <<2::256>>
    }

    Process.put(:restore_large_task, task)
    step = %{node_key: key, task_id: task.task_id, phase: :submitted, stage: 0, attempt: 1}

    state = %RunExecutionState{
      run: run,
      mode: :sequential,
      recovery: %{phase: :tasks, pending: List.duplicate(step, 32), tasks: [], progress: nil}
    }

    restored =
      Enum.reduce(1..32, state, fn _, state ->
        assert {:more, next} = Restore.next(state)
        next
      end)

    assert length(restored.recovery.tasks) == 32
    assert :erlang.external_size(restored) < 100_000
    assert Enum.all?(restored.recovery.tasks, & &1.recovery_pending?)
    refute_received {:runner_task_result, _, _, _}
  end

  test "an unreadable terminal reread preserves pending work instead of failing the asset" do
    task = %{
      task_id: "terminal",
      payload_hash: <<1::256>>,
      orchestration_context_hash: <<2::256>>,
      assignment_generation: 1,
      result_version: 1,
      status: :succeeded,
      data_state: :unavailable,
      payload: nil,
      result: nil,
      persistence_failure: :result
    }

    Process.put(:restore_large_task, task)

    state = %RunExecutionState{
      run: %RunState{workspace_id: "workspace"},
      awaits: %{task.task_id => %{entry: %{terminal_evidence: RecoveredTask.evidence(task)}}}
    }

    assert {:recovery_required, ^state,
            {:recovered_terminal_read_failed, "terminal",
             {:error, :recovered_terminal_data_unavailable}}} =
             ExecutionDriver.handle_event(
               state,
               {:runner_task_result, task.task_id, :read_terminal}
             )
  end

  test "recovery position must match the atomic checkpoint's stage, attempt and sequence" do
    position = %{
      "version" => 1,
      "mode" => "pipeline",
      "phase" => "admit",
      "index" => 1,
      "attempt" => 2
    }

    progress = %{position: position, position_sequence: 12}
    ref = %{stage: 1, attempt: 2, sequence: 12}

    for wrong <- [%{ref | stage: 0}, %{ref | attempt: 1}, %{ref | sequence: 11}, nil] do
      state = %RunExecutionState{
        mode: :pipeline,
        freshness_checkpoint: wrong,
        recovery: %{phase: :details, pending: [], progress: progress}
      }

      assert {:error, :recovery_position_checkpoint_mismatch} = Restore.next(state)
    end
  end

  test "typed runner timeouts retain their exact value while await timeouts retain their atom" do
    result = %RunnerResult{status: :timed_out, error: %{message: "runner timed out"}}

    event = %{
      event_type: :step_timed_out,
      data: %{"result_status" => "timed_out", "retryable?" => false}
    }

    assert {:timed_out, error, ^result} =
             RecoveredTask.settlement(%{recovered_outcome: event}, result)
             |> then(fn {status, false, error, value} -> {status, error, value} end)

    assert error == result.error

    assert {:timed_out, false, :timeout, :timeout} =
             RecoveredTask.settlement(%{recovered_outcome: %{event | data: %{}}}, result)
  end
end
