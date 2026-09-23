defmodule FavnOrchestrator.RunServer.FailureCleanupTest do
  use ExUnit.Case, async: true
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunServer.FailureCleanup
  alias FavnOrchestrator.RunState

  test "unknown write outcomes include failed and cancelled tasks" do
    for status <- [:failed, :cancelled],
        retry_class <- [:unknown_do_not_retry, :reconcile_before_retry] do
      assert FailureCleanup.uncertain_write?(%{
               status: status,
               retry_class: retry_class,
               error: nil
             })
    end

    assert FailureCleanup.uncertain_write?(%{
             status: :failed,
             retry_class: :terminal,
             error: %{"outcome" => "unknown"}
           })

    refute FailureCleanup.uncertain_write?(%{
             status: :failed,
             retry_class: :safe_to_retry,
             error: nil
           })
  end

  test "cancelled before any assignment is conclusive safe evidence, even for legacy retry class" do
    task = %{
      status: :cancelled,
      retry_class: :unknown_do_not_retry,
      assignment_generation: 0,
      assigned_at: nil,
      result: nil,
      error: nil
    }

    refute FailureCleanup.uncertain_write?(task)
    assert FailureCleanup.uncertain_write?(%{task | assignment_generation: 1})
    assert FailureCleanup.uncertain_write?(%{task | error: %{outcome: :unknown}})
  end

  test "the first task pass drains every sibling before settlement can start" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: nil,
      phase: :tasks,
      tasks: [%{task_id: "old-asset"}, %{task_id: "marker"}, %{task_id: "live-sibling"}]
    }

    assert {:drain_task, _, "old-asset"} = op = FailureCleanup.operation(state)
    assert {:cont, state} = FailureCleanup.apply_result(state, op, {:ok, :settled})
    assert {:drain_task, _, "marker"} = op = FailureCleanup.operation(state)
    assert {:cont, state} = FailureCleanup.apply_result(state, op, {:ok, :waiting})
    assert {:drain_task, _, "live-sibling"} = op = FailureCleanup.operation(state)
    assert {:cont, state} = FailureCleanup.apply_result(state, op, {:ok, :waiting})

    assert {:retry, :cleanup_tasks_pending} =
             FailureCleanup.apply_result(state, {:tasks, nil}, {:ok, []})

    assert {:cont, settled_pass} =
             FailureCleanup.apply_result(%{state | waiting?: false}, {:tasks, nil}, {:ok, []})

    refute settled_pass.draining?
    assert settled_pass.cursor == nil
  end

  test "invalid evidence and unknown writes do not prevent later siblings from draining" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: nil,
      phase: :tasks,
      tasks: [%{task_id: "bad"}, %{task_id: "unknown"}, %{task_id: "live"}, %{task_id: "done"}]
    }

    assert {:cont, state} =
             FailureCleanup.apply_result(
               state,
               {:task, nil, "bad"},
               {:error, :invalid_recovered_runner_task}
             )

    assert {:cont, state} =
             FailureCleanup.apply_result(
               state,
               {:task, nil, "unknown"},
               {:ok, {:unresolved, "unknown_write", "unknown"}}
             )

    assert {:cont, state} =
             FailureCleanup.apply_result(state, {:task, nil, "live"}, {:ok, :waiting})

    assert {:cont, state} =
             FailureCleanup.apply_result(state, {:task, nil, "done"}, {:ok, :settled})

    assert state.tasks == []
    assert state.waiting?
    assert state.unresolved_count == 2

    assert {:retry, :cleanup_tasks_pending} =
             FailureCleanup.apply_result(state, {:tasks, nil}, {:ok, []})
  end

  test "unresolved registration releases terminal permits before proceeding and propagates release failure" do
    state = %FailureCleanup{
      run: %RunState{metadata: %{"failure_cleanup" => %{"version" => 1, "state" => "pending"}}},
      version: nil,
      index: nil,
      progress: nil,
      phase: :generation,
      entry: %{task_id: "asset", resource_circuit_permits: [:permit]},
      tasks: [%{task_id: "asset"}]
    }

    assert {:cont, state} =
             FailureCleanup.apply_result(state, {:generation, nil, 1}, {:error, :marker_mismatch})

    assert {:progress, _, _} = progress = FailureCleanup.operation(state)
    assert {:cont, state} = FailureCleanup.apply_result(state, progress, :ok)

    assert {:release_unresolved, _, %{task_id: "asset"}} =
             operation = FailureCleanup.operation(state)

    assert {:retry, :fenced} = FailureCleanup.apply_result(state, operation, {:error, :fenced})
    assert {:cont, state} = FailureCleanup.apply_result(state, operation, :ok)
    assert state.tasks == []
    assert state.unresolved_count == 1
  end

  test "saved await failure remains compatible with a subsequently cancelled task" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: %{steps: %{"step" => %{phase: :outcome, status: :error, retry_allowed?: false}}},
      phase: :outcome,
      entry: %{
        asset_step_id: "step",
        task_id: "task",
        cleanup_task_outcome: %{status: :cancelled, retry_class: :terminal, result: nil}
      }
    }

    event = %{sequence: 9, event_type: :step_failed, data: %{"error" => "await_lost"}}

    assert {:cont, restored} =
             FailureCleanup.apply_result(state, {:outcome, nil, nil, 9}, {:ok, %{items: [event]}})

    assert restored.phase == :settle
    assert restored.entry.recovered_outcome == event
  end

  test "release errors cannot turn pending cleanup into complete" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: nil,
      phase: :resources
    }

    error = Error.new(:unavailable, "lost acknowledgement", retryable?: true)

    assert {:retry, ^error} =
             FailureCleanup.apply_result(state, {:resources, nil}, {:error, error})

    assert {:retry, :fenced} =
             FailureCleanup.apply_result(state, {:task, nil, "t"}, {:error, :fenced})
  end

  test "a later history gap prevents settlement from its valid prefix" do
    run = %RunState{
      id: "run",
      manifest_version_id: "mv",
      manifest_content_hash: "hash",
      plan: %Favn.Plan{nodes: %{}, node_stages: []},
      event_seq: 3,
      metadata: %{"failure_cleanup" => %{"version" => 1, "state" => "pending"}}
    }

    state = %FailureCleanup{
      run: run,
      version: nil,
      index: nil,
      progress: FavnOrchestrator.RunServer.Execution.RecoveryProgress.new(run),
      phase: :events,
      history_end: 3,
      draining?: false
    }

    event = %{
      run_id: "run",
      manifest_version_id: "mv",
      manifest_content_hash: "hash",
      sequence: 1,
      event_type: :run_started,
      data: %{}
    }

    assert {:cont, prefix} =
             FailureCleanup.apply_result(
               state,
               {:events, nil, "run", 0},
               {:ok, %{items: [event]}}
             )

    assert prefix.phase == :events

    assert {:cont, gap} =
             FailureCleanup.apply_result(
               prefix,
               {:events, nil, "run", 1},
               {:ok, %{items: [%{event | sequence: 3}]}}
             )

    assert gap.phase == :resources
    assert gap.unresolved_count == 1

    assert {:progress, _, %{"unresolved" => [%{"reason_code" => "cleanup_history_incomplete"}]}} =
             FailureCleanup.operation(gap)
  end

  test "terminal evidence failure drains siblings without rediscovering terminal tasks" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: nil,
      phase: :tasks,
      tasks: [%{task_id: "malformed"}, %{task_id: "sibling"}]
    }

    assert {:cont, state} =
             FailureCleanup.apply_result(
               state,
               {:drain_task, nil, "malformed"},
               {:ok, {:unresolved, "terminal_task_resources_unresolved", "malformed"}}
             )

    refute state.waiting?

    assert {:cont, state} =
             FailureCleanup.apply_result(state, {:drain_task, nil, "sibling"}, {:ok, :settled})

    assert {:cont, state} = FailureCleanup.apply_result(state, {:tasks, nil}, {:ok, []})
    assert state.phase == :events
    assert state.unresolved_count == 1
  end

  test "missing detail on restart retains a previously saved successful result" do
    success = %Favn.Run.NodeResult{asset_step_id: "saved", attempt_count: 2, status: :ok}
    assets = [%{ref: {MyApp.Asset, :asset}, status: :ok}]
    previous = %{success | attempt_count: 1, status: :error}

    run = %RunState{
      result: %{
        node_results: [success, previous],
        asset_results: assets,
        metadata: %{result_retention: %{node_result_count: 1}}
      }
    }

    progress = %{steps: %{}, result_count: 1}

    assert {:ok, restored} = FailureCleanup.perform({:restore_results, run, progress, []})
    assert restored.result.node_results == [success, previous]
    assert restored.result.asset_results == assets
    assert restored.result.metadata.result_retention.retained_node_result_count == 2
    assert {:ok, again} = FailureCleanup.perform({:restore_results, restored, progress, []})
    assert again.result == restored.result
  end

  test "missing outcome skips that settlement while a transient read retains its place" do
    state = %FailureCleanup{
      run: %RunState{},
      version: nil,
      index: nil,
      progress: nil,
      phase: :outcome,
      entry: %{task_id: "missing", resource_circuit_permits: []},
      tasks: [%{task_id: "missing"}, %{task_id: "independent"}],
      draining?: false
    }

    operation = {:outcome, nil, "run", 5}
    unavailable = Error.new(:unavailable, "offline", retryable?: true)

    assert {:retry, ^unavailable} =
             FailureCleanup.apply_result(state, operation, {:error, unavailable})

    assert {:cont, skipped} = FailureCleanup.apply_result(state, operation, {:ok, %{items: []}})
    assert skipped.phase == :release_unresolved

    assert {:cont, next} =
             FailureCleanup.apply_result(skipped, {:release_unresolved, nil, state.entry}, :ok)

    assert next.tasks == [%{task_id: "independent"}]
    assert next.unresolved_count == 1
  end
end
