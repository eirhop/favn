defmodule FavnOrchestrator.RunServer.Execution.RecoveryProgressTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunServer.Execution.RecoveryProgress
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunEventCodec

  test "a durable success remains pending until bookkeeping is settled" do
    run = run()
    progress = RecoveryProgress.new(run)

    assert {:ok, progress} =
             RecoveryProgress.fold(progress, [
               event(run, 1, :step_started, :first, %{runner_task_id: "task-first"}),
               event(run, 2, :step_finished, :first, %{})
             ])

    step = progress.steps[step_id(run, :first)]
    assert step.phase == :outcome
    assert step.task_id == "task-first"
    assert step.outcome_sequence == 2
    assert progress.failure == nil

    assert {:ok, settled} =
             RecoveryProgress.apply_event(
               progress,
               event(run, 3, :step_settled, :first, %{runner_task_id: "task-first"})
             )

    assert settled.steps[step_id(run, :first)].phase == :settled

    assert {:error, :invalid_recovery_step} =
             RecoveryProgress.apply_event(
               settled,
               event(run, 4, :step_settled, :second, %{runner_task_id: "second"})
             )
  end

  test "records earlier terminal failure separately from a later successful sibling" do
    run = run()

    assert {:ok, progress} =
             RecoveryProgress.fold(RecoveryProgress.new(run), [
               event(run, 1, :step_failed, :first, %{retryable?: false}),
               event(run, 2, :step_started, :second, %{runner_task_id: "second"}),
               event(run, 3, :step_finished, :second, %{}),
               event(run, 4, :step_settled, :second, %{runner_task_id: "second"})
             ])

    assert progress.failure == %{sequence: 1, status: :error}
    assert progress.steps[step_id(run, :second)].status == :ok
  end

  test "freshness and blocked decisions survive without runner tasks" do
    run = run()

    assert {:ok, progress} =
             RecoveryProgress.fold(RecoveryProgress.new(run), [
               event(run, 1, :step_skipped_fresh, :first, %{attempt: 0}),
               event(run, 2, :step_blocked, :second, %{attempt: 0})
             ])

    assert progress.steps[step_id(run, :first)].phase == :settled
    assert progress.steps[step_id(run, :second)].status == :blocked
    assert progress.failure == %{sequence: 2, status: :error}
    refute Map.has_key?(progress.steps[step_id(run, :second)], :task_id)
  end

  test "retryable failure does not become the permanent run failure" do
    run = run()

    assert {:ok, progress} =
             RecoveryProgress.fold(RecoveryProgress.new(run), [
               event(run, 1, :step_failed, :first, %{retryable?: true}),
               event(run, 2, :step_retry_started, :first, %{
                 attempt: 2,
                 runner_task_id: "task-retry"
               }),
               event(run, 3, :step_finished, :first, %{attempt: 2})
             ])

    assert progress.failure == nil
    assert progress.steps[step_id(run, :first)].attempt == 2
    assert progress.steps[step_id(run, :first)].task_id == "task-retry"

    assert {:error, :invalid_recovery_step} =
             RecoveryProgress.apply_event(progress, event(run, 4, :step_failed, :first, %{}))
  end

  test "foreign identity, missing sequence and unplanned steps cannot authorize recovery" do
    run = run()
    progress = RecoveryProgress.new(run)
    first = event(run, 1, :step_finished, :first, %{})

    for event <- [
          %{first | run_id: "another-run"},
          %{first | manifest_content_hash: String.duplicate("b", 64)},
          %{first | sequence: 2}
        ] do
      assert {:error, :invalid_recovery_event_identity_or_sequence} =
               RecoveryProgress.apply_event(progress, event)
    end

    assert {:error, :invalid_recovery_step} =
             RecoveryProgress.apply_event(progress, put_in(first, [:data, "asset_step_id"], "x"))
  end

  test "result payload size does not grow reconstructed progress" do
    run = run()

    {:ok, progress} =
      RecoveryProgress.apply_event(
        RecoveryProgress.new(run),
        event(run, 1, :step_started, :first, %{runner_task_id: "first"})
      )

    small = event(run, 2, :step_finished, :first, %{node_result: %{meta: %{}}})

    large =
      event(run, 2, :step_finished, :first, %{
        node_result: %{meta: %{application_data: String.duplicate("x", 200_000)}}
      })

    assert RecoveryProgress.apply_event(progress, small) ==
             RecoveryProgress.apply_event(progress, large)
  end

  test "malformed events and altered planned stages are rejected without crashing" do
    run = run()
    progress = RecoveryProgress.new(run)

    for malformed <- [nil, %{}, %{event(run, 1, :step_finished, :first, %{}) | event_type: %{}}] do
      assert {:error, :invalid_recovery_event_identity_or_sequence} =
               RecoveryProgress.apply_event(progress, malformed)
    end

    for data <- [nil, %{"stage" => 1, "asset_step_id" => step_id(run, :first), "attempt" => 1}] do
      assert {:error, :invalid_recovery_step} =
               RecoveryProgress.apply_event(
                 progress,
                 %{event(run, 1, :step_finished, :first, %{}) | data: data}
               )
    end
  end

  test "a completed success cannot be replaced by a new start or a second outcome" do
    run = run()

    assert {:ok, progress} =
             RecoveryProgress.fold(RecoveryProgress.new(run), [
               event(run, 1, :step_started, :first, %{runner_task_id: "task"}),
               event(run, 2, :step_finished, :first, %{})
             ])

    for {kind, data} <- [
          {:step_started, %{runner_task_id: "other"}},
          {:step_retry_started, %{runner_task_id: "other", attempt: 2}},
          {:step_failed, %{}}
        ] do
      assert {:error, :invalid_recovery_step} =
               RecoveryProgress.apply_event(progress, event(run, 3, kind, :first, data))
    end
  end

  test "a settlement cannot authorize replay of a terminal or unknown outcome" do
    run = run()

    {:ok, progress} =
      RecoveryProgress.fold(RecoveryProgress.new(run), [
        event(run, 1, :step_started, :first, %{runner_task_id: "unknown-write"}),
        event(run, 2, :step_failed, :first, %{retryable?: false})
      ])

    for data <- [
          %{status: :error, retryable?: true, retry_after_ms: 0, runner_task_id: "unknown-write"},
          %{status: :ok, runner_task_id: "unknown-write"},
          %{status: :error, runner_task_id: "other-task"},
          %{status: :error},
          %{status: %{malformed: true}, runner_task_id: "unknown-write"}
        ] do
      assert {:error, :invalid_recovery_step} =
               RecoveryProgress.apply_event(
                 progress,
                 event(run, 3, :step_settled, :first, data)
               )
    end
  end

  test "a position cannot skip unfinished work or cross the pinned plan bounds" do
    run = run()
    progress = RecoveryProgress.new(run)

    for position <- [
          %{version: 1, mode: "pipeline", phase: "admit", index: 99, attempt: 1},
          %{version: 1, mode: "sequential", phase: "admit", index: 0, attempt: 1},
          %{version: 1, mode: "pipeline", phase: "advance", index: 0, attempt: 1}
        ] do
      e = %{event(run, 1, :run_execution_position, :first, %{}) | data: %{position: position}}
      assert {:error, :invalid_recovery_position} = RecoveryProgress.apply_event(progress, e)
    end
  end

  defp run do
    nodes = Map.new([:first, :second], &{key(&1), %{ref: {__MODULE__, &1}, stage: 0}})

    %RunState{
      id: "recovery-progress",
      manifest_version_id: "manifest-progress",
      manifest_content_hash: String.duplicate("a", 64),
      plan: struct(Favn.Plan, nodes: nodes, node_stages: [[key(:first), key(:second)]])
    }
  end

  defp key(name), do: {{__MODULE__, name}, nil}

  defp step_id(run, name),
    do: AssetStepIdentity.asset_step_id(run.id, key(name), {__MODULE__, name})

  defp event(run, sequence, kind, name, data) do
    data = if kind == :step_settled, do: Map.put_new(data, :status, :ok), else: data

    event = %{
      run_id: run.id,
      sequence: sequence,
      event_type: kind,
      occurred_at: ~U[2026-09-17 10:00:00Z],
      status: :running,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      data: Map.merge(%{asset_step_id: step_id(run, name), stage: 0, attempt: 1}, data)
    }

    {:ok, encoded} = RunEventCodec.encode(event)
    {:ok, decoded} = RunEventCodec.decode(encoded)
    decoded
  end
end
