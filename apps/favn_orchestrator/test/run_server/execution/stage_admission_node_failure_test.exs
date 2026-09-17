defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionNodeFailureTest do
  @moduledoc """
  A terminal admission failure that belongs to one node must not stop its stage.

  The fixture holds node `a` on the runner while node `b` is admitted, so the
  runner result for `a` is delivered only when a test chooses to. Assets are
  non-SQL so no execution package is fetched, and plan nodes carry pinned
  target and evidence generation identities so the claim reaches the store.
  """

  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Admission
  alias FavnOrchestrator.Persistence.Results.ExecutionLease
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Recovery
  alias FavnOrchestrator.RunState

  @held_task_id "rt_admission_sibling_held"

  @conflict Error.new(:conflict, "target operation is in progress",
              details: %{reason_code: "target_operation_in_progress"}
            )

  @unavailable Error.new(:unavailable, "claim store is unavailable", retryable?: true)

  @later_conflict Error.new(:conflict, "a different target operation is in progress",
                    details: %{reason_code: "target_operation_in_progress"}
                  )

  defmodule FakeStore do
    alias FavnOrchestrator.Persistence.Results.Admission
    alias FavnOrchestrator.Persistence.Results.CapacityRelease
    alias FavnOrchestrator.Persistence.Results.ExecutionLease
    alias FavnOrchestrator.Persistence.Results.MaterializationClaim
    alias FavnOrchestrator.Persistence.Results.MaterializationDecision
    alias FavnOrchestrator.Persistence.Results.RunCommitted
    alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint

    def get_run(_query), do: {:error, :forced_missing}

    def commit_transition(command) do
      case Process.get({__MODULE__, :commit_error_by_event}, %{})
           |> Map.get(command.event.event_type) do
        nil -> do_commit_transition(command)
        reason -> {:error, reason}
      end
    end

    defp do_commit_transition(command) do
      send(self(), {:commit_transition, command})

      case Process.get({__MODULE__, :commit_results}, []) do
        [{:commit_then_error, reason} | rest] ->
          Process.put({__MODULE__, :commit_results}, rest)
          {:error, reason}

        [result | rest] ->
          Process.put({__MODULE__, :commit_results}, rest)
          result

        [] ->
          commit_transition_after_injected_results(command)
      end
    end

    defp commit_transition_after_injected_results(command) do
      case Process.get({__MODULE__, :commit_failures}, 0) do
        failures when failures > 0 ->
          Process.put({__MODULE__, :commit_failures}, failures - 1)
          {:error, :forced_commit_failure}

        _none ->
          {:ok,
           %RunCommitted{
             run: command.run,
             event: command.event,
             event_id: 1,
             outbox_event_id: 1,
             replayed?: false
           }}
      end
    end

    def admit(command) do
      case Map.get(
             Process.get({__MODULE__, :admit_error_steps}, %{}),
             command.step_id,
             Process.get({__MODULE__, :admit_error})
           ) do
        nil -> do_admit(command)
        reason -> {:error, reason}
      end
    end

    defp do_admit(command) do
      if result = Process.get({__MODULE__, :admission_result}),
        do: result,
        else: admitted(command)
    end

    defp admitted(command) do
      send(self(), {:admit_execution, command})

      {:ok,
       %Admission{
         status: :admitted,
         lease: %ExecutionLease{
           workspace_id: command.workspace_context.workspace_id,
           lease_id: command.lease_id,
           run_id: command.run_id,
           step_id: command.step_id,
           owner_id: command.owner_id,
           owner_generation: command.owner_generation,
           status: :held,
           expires_at: DateTime.add(DateTime.utc_now(), 60, :second),
           scope_ids: Enum.map(command.requests, & &1.scope_id)
         }
       }}
    end

    def acquire(_command) do
      {:ok,
       %FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission{
         status: :blocked,
         blockers: [Process.get({__MODULE__, :blocker})]
       }}
    end

    def record_recovery_candidate(command) do
      send(self(), {:recovery_candidate, command})

      case Process.get({__MODULE__, :candidate_error}) do
        nil -> :ok
        error -> {:error, error}
      end
    end

    def release_lease(command) do
      send(self(), {:release_execution_lease, command})

      {:ok,
       %CapacityRelease{
         released_lease_ids: [command.lease_id],
         expired_waiter_ids: [],
         freed_scope_ids: []
       }}
    end

    def release_run_leases(command) do
      send(self(), {:release_run_leases, command})

      {:ok, %CapacityRelease{released_lease_ids: [], expired_waiter_ids: [], freed_scope_ids: []}}
    end

    def claim(command) do
      send(self(), {:materialization_claim, command})

      if command.target_id in Process.get({__MODULE__, :claimable_target_ids}, []) do
        {:ok,
         %MaterializationDecision{
           claim_key: command.claim_key,
           status: :claimed,
           claim: %MaterializationClaim{
             workspace_id: command.workspace_context.workspace_id,
             claim_key: command.claim_key,
             deployment_id: command.deployment_id,
             target_kind: command.target_kind,
             target_id: command.target_id,
             target_generation_id: command.target_generation_id,
             evidence_generation_id: command.evidence_generation_id,
             partition_key: command.partition_key,
             run_id: command.run_id,
             owner_id: command.owner_id,
             fencing_token: 1,
             status: :claimed,
             expires_at: DateTime.add(command.occurred_at, 60, :second),
             version: 1
           }
         }}
      else
        errors = Process.get({__MODULE__, :claim_error_by_target}, %{})
        {:error, Map.get(errors, command.target_id, Process.get({__MODULE__, :claim_error}))}
      end
    end

    def finish(command) do
      send(self(), {:materialization_finish, command})

      {:ok, %MaterializationDecision{claim_key: command.claim_key, status: command.status}}
    end

    def put_execution_checkpoint(command) do
      send(self(), {:put_execution_checkpoint, command})

      {:ok,
       %RunExecutionCheckpoint{
         workspace_id: command.workspace_context.workspace_id,
         run_id: command.run_id,
         owner_id: command.owner_id,
         fencing_token: command.fencing_token,
         checkpoint_version: command.checkpoint_version,
         checkpoint_revision: command.checkpoint_revision,
         checkpoint_sequence: command.checkpoint_sequence,
         stage: command.stage,
         attempt: command.attempt,
         payload: command.payload,
         payload_hash: command.payload_hash,
         updated_at: command.occurred_at
       }}
    end
  end

  defmodule RejectedTaskStore do
    alias FavnOrchestrator.Persistence.Error

    def enqueue(command) do
      send(self(), {:rejected_enqueue, command})

      {:error,
       Error.new(:invalid, "invalid task", details: %{reason_code: "invalid_runner_task_data"})}
    end

    def get(query) do
      send(self(), {:enqueue_recovery_read, query})
      Process.get({__MODULE__, :read_result})
    end

    def request_cancellation(command) do
      if pid = :persistent_term.get({__MODULE__, :test_pid}, nil) do
        send(pid, {:accepted_store_cancellation, command})
      end

      {:error, Error.new(:unavailable, "cancellation unavailable")}
    end
  end

  defmodule AcceptingTaskStore do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Results.RunnerTask

    def enqueue(command) do
      case Process.get({__MODULE__, :enqueue_error}) do
        nil -> do_enqueue(command)
        reason -> {:error, reason}
      end
    end

    defp do_enqueue(command) do
      task = %RunnerTask{
        workspace_id: command.workspace_context.workspace_id,
        task_id: command.task_id,
        task_kind: command.task_kind,
        run_id: command.run_id,
        asset_step_id: command.asset_step_id,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        assignment_generation: 0,
        status: :queued,
        payload: command.payload,
        orchestration_context: command.orchestration_context
      }

      Process.put({__MODULE__, command.task_id}, task)
      send(self(), {:accepted_enqueue, command})

      case Map.get(Process.get({__MODULE__, :lost_replies}, %{}), command.asset_step_id) do
        nil -> {:ok, task}
        reason -> {:error, reason}
      end
    end

    def get(query) do
      case Process.get({__MODULE__, query.task_id}) do
        %RunnerTask{} = task -> {:ok, task}
        nil -> {:error, Error.new(:not_found, "runner task not found")}
      end
    end

    def request_cancellation(command) do
      if owner = :persistent_term.get({__MODULE__, :test_pid}, nil),
        do: send(owner, {:accepted_store_cancellation, command})

      {:error, Error.new(:unavailable, "cancellation unavailable")}
    end
  end

  setup context do
    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_submissions: FakeStore,
      runner_tasks: Map.get(context, :runner_task_store, FavnOrchestrator.TestRunnerTaskStore),
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      target_recovery: FakeStore,
      rebuilds: FakeStore,
      target_operation_locks: FakeStore,
      materialization: FakeStore,
      backfills: FakeStore,
      operator_reads: FakeStore,
      logs: FakeStore,
      identity: FakeStore,
      maintenance: FakeStore
    }

    start_supervised!(
      {PersistenceRuntime,
       %PersistenceRuntime{
         backend: __MODULE__,
         options: [],
         stores: stores
       }}
    )

    Process.put({FavnOrchestrator.TestRunnerTaskStore, :cancellation_recorder}, self())
    Process.put({FakeStore, :claim_error}, @conflict)
    :persistent_term.put({AcceptingTaskStore, :test_pid}, self())

    on_exit(fn ->
      Process.delete({FavnOrchestrator.TestRunnerTaskStore, :cancellation_recorder})
      :persistent_term.erase({AcceptingTaskStore, :test_pid})
    end)

    {:ok, fixture: fixture()}
  end

  test "blocked candidate retry keeps the already-committed decision sequence", %{
    fixture: fixture
  } do
    conflict =
      Error.new(:conflict, "history busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    blocker = %FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker{
      resource: Favn.Resource.Ref.new!(:execution_pool, :default),
      state: :open,
      failure_threshold: 1,
      consecutive_failures: 1
    }

    Process.put({FakeStore, :blocker}, blocker)
    Process.put({FakeStore, :candidate_error}, conflict)

    {:ok, policy} =
      Favn.ExecutionPool.Policy.new(
        max_concurrency: 4,
        circuit_breaker: [failure_threshold: 1, probe_after_ms: 60_000]
      )

    run =
      put_in(
        fixture.run,
        [Access.key(:plan), Access.key(:nodes), fixture.b_key, :execution_pool],
        :default
      )

    run = %{
      run
      | metadata:
          Map.merge(run.metadata, %{
            execution_pool_policy: %{"default" => policy},
            pipeline_execution_policy: %{
              max_concurrency: 4,
              resource_recovery: Favn.ResourceRecovery.Policy.new!(:retry_remaining)
            }
          })
    }

    state = %{fixture.state | run: run, stage_state: %{fixture.state.stage_state | run: run}}

    assert {:persist_retry, paused,
            %PersistenceRetry{event_type: :resource_recovery_candidate} = retry, ^conflict} =
             Execution.handle_event(state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_blocked}} = decision}
    assert paused.run.event_seq == decision.event.sequence
    assert_receive {:recovery_candidate, candidate}
    Process.delete({FakeStore, :candidate_error})
    assert {:ownership_gate, gated, replay} = Execution.retry_persistence(paused, retry)
    assert_receive {:recovery_candidate, ^candidate}
    assert {:cont, resumed} = Execution.resume_persisted_retry(gated, replay)
    assert resumed.run.event_seq >= decision.event.sequence
    assert ResultBuilder.latest_node_status(resumed.run, fixture.b_key) == :blocked
    refute_received {:commit_transition, %{event: %{event_type: :step_blocked}}}
    assert @held_task_id in ActiveTaskSet.task_ids(resumed.work_set)
  end

  test "classification retry preserves its committed decision and remaining nodes", %{
    fixture: fixture
  } do
    alias FavnOrchestrator.RunServer.Execution.StageClassifier

    conflict =
      Error.new(:conflict, "history busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    context = %{
      fixture.state.freshness_context
      | upstream_statuses: %{fixture.a_key => :error, fixture.b_key => :ok}
    }

    assert {:persist_retry, retry, ^conflict} =
             StageClassifier.classify(
               fixture.run,
               fixture.version,
               1,
               [fixture.d_key, fixture.e_key],
               context,
               nil
             )

    assert retry.event_type == :step_blocked
    assert_receive {:commit_transition, original}
    assert :ok = PersistenceRetry.persist(retry)
    assert_receive {:commit_transition, ^original}
    {:stage_classification, continuation} = retry.resume

    assert {:ok, run, [], _, context, _, [remaining]} =
             StageClassifier.resume_persisted(continuation, continuation.persisted_run)

    assert remaining == fixture.e_key
    assert run.event_seq == original.event.sequence
    assert context.upstream_statuses[fixture.d_key] == :blocked
    assert ResultBuilder.latest_node_status(run, fixture.d_key) == :blocked
    refute_received {:commit_transition, _duplicate}
  end

  @tag runner_task_store: RejectedTaskStore
  test "rejected enqueue releases only the proven missing task's ownership", %{fixture: fixture} do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    Process.put(
      {RejectedTaskStore, :read_result},
      {:error, Error.new(:not_found, "task not found")}
    )

    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)
    assert_receive {:rejected_enqueue, command}
    assert_receive {:enqueue_recovery_read, %{task_id: task_id}}
    assert task_id == command.task_id
    assert_receive {:materialization_finish, %{status: :failed}}
    refute task_id in ActiveTaskSet.active_runner_task_ids(awaiting.run)
    assert @held_task_id in ActiveTaskSet.active_runner_task_ids(awaiting.run)
  end

  for kind <- [:unsupported_struct, :oversized_payload] do
    @tag runner_task_store: RejectedTaskStore
    test "local #{kind} rejection clears only a proven missing task", %{
      fixture: fixture
    } do
      Process.put({FakeStore, :claimable_target_ids}, [
        TargetIdentity.for_asset(elem(fixture.b_key, 0))
      ])

      Process.put(
        {RejectedTaskStore, :read_result},
        {:error, Error.new(:not_found, "task not found")}
      )

      rejected_value =
        if unquote(kind) == :unsupported_struct,
          do: %URI{},
          else:
            String.duplicate(
              "x",
              Favn.Contracts.RunnerTask.Limits.payload_bytes(:asset_attempt) + 1
            )

      run = %{
        fixture.state.run
        | metadata: Map.put(fixture.state.run.metadata, :operator_metadata, rejected_value)
      }

      state = %{fixture.state | run: run, stage_state: %{fixture.state.stage_state | run: run}}
      assert {:cont, awaiting} = Execution.handle_event(state, :continue)
      assert_receive {:enqueue_recovery_read, %{task_id: task_id}}
      refute_received {:rejected_enqueue, _command}
      assert_receive {:materialization_finish, %{status: :failed}}
      refute task_id in ActiveTaskSet.active_runner_task_ids(awaiting.run)
      assert @held_task_id in ActiveTaskSet.active_runner_task_ids(awaiting.run)
    end
  end

  for {name, read_result} <- [
        {"existing task",
         {:ok, %FavnOrchestrator.Persistence.Results.RunnerTask{status: :queued}}},
        {"unavailable read", {:error, Error.new(:unavailable, "read unavailable")}}
      ] do
    @tag runner_task_store: RejectedTaskStore
    test "rejected enqueue retains ownership after #{name}", %{fixture: fixture} do
      Process.put({FakeStore, :claimable_target_ids}, [
        TargetIdentity.for_asset(elem(fixture.b_key, 0))
      ])

      Process.put({RejectedTaskStore, :read_result}, unquote(Macro.escape(read_result)))
      assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)
      assert_receive {:rejected_enqueue, command}
      assert command.task_id in ActiveTaskSet.active_runner_task_ids(awaiting.run)
      refute_received {:materialization_finish, _finish}
      refute_received {:release_execution_lease, _release}
    end
  end

  for history_first <- [false, true] do
    test "target writer contention queues without failure after history retry=#{history_first}",
         %{fixture: fixture} do
      busy =
        Error.new(:conflict, "target writer busy",
          retryable?: true,
          details: %{reason_code: "target_write_in_progress"}
        )

      history =
        Error.new(:conflict, "history busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      Process.put({FakeStore, :claim_error}, if(unquote(history_first), do: history, else: busy))
      directive = Execution.handle_event(fixture.state, :continue)

      directive =
        if unquote(history_first) do
          assert {:persist_retry, paused, retry, ^history} = directive
          Process.put({FakeStore, :claim_error}, busy)
          Execution.retry_persistence(paused, retry)
        else
          directive
        end

      assert {:cont, awaiting} = directive
      assert_receive {:commit_transition, %{event: %{event_type: :step_queued}}}
      assert_receive {:release_execution_lease, %{lease_id: released}}
      refute released == "lease-step-a"
      refute_received {:commit_transition, %{event: %{event_type: :step_failed}}}
      refute_received {:runner_task_cancellation_requested, _}
      assert @held_task_id in ActiveTaskSet.task_ids(awaiting.work_set)
    end
  end

  test "a node-specific claim conflict fails only its node and leaves the sibling running", %{
    fixture: fixture
  } do
    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}
    assert command.event.data.node_key == fixture.b_key
    assert command.event.data.error == @conflict
    assert command.event.data.retryable? == false
    assert command.event.data.node_result.status == :error
    assert command.event.data.node_result.runner_task_id == nil

    assert command.run.status == :running,
           "the run must stay running while the sibling drains"

    assert command.run.error == nil

    assert ResultBuilder.latest_node_status(awaiting.stage_state.run, fixture.b_key) == :error,
           "the failed node's result must be carried forward on the live run"

    assert_receive {:admit_execution, admit}
    assert_receive {:release_execution_lease, %{lease_id: released_lease_id}}
    assert released_lease_id == admit.lease_id
    refute released_lease_id == "lease-step-a", "the held sibling's lease must not be released"

    refute_received {:runner_task_cancellation_requested, _command}
    assert awaiting.status == :awaiting
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "the held sibling completes and the run fails with the admission error", %{
    fixture: fixture
  } do
    # `d` fails on its own distinguishable conflict in the next stage, so the
    # terminal error proves the FIRST failure wins rather than the last one.
    Process.put({FakeStore, :claim_error_by_target}, %{fixture.d_target_id => @later_conflict})

    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert {:terminal, failed} =
             Execution.handle_event(
               awaiting,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    refute_received {:runner_task_cancellation_requested, _command}

    assert failed.status == :error

    assert failed.error == @conflict,
           "the first terminal failure must stay the run error, not the later one"

    statuses = node_statuses(failed)
    assert statuses[fixture.a_key] == :ok
    assert statuses[fixture.b_key] == :error

    assert Map.has_key?(statuses, fixture.d_key),
           "a downstream of the successful sibling must still be attempted"

    assert statuses[fixture.e_key] == :blocked
  end

  test "only the failed node's dependent is blocked in the next stage", %{fixture: fixture} do
    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert {:terminal, _failed} =
             Execution.handle_event(
               awaiting,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    commands = drain_commits()

    blocked = commits(commands, :step_blocked)
    assert Enum.map(blocked, & &1.event.data.node_key) == [fixture.e_key]
    assert hd(blocked).event.data.reason == :upstream_blocked

    # The dependent of the successful sibling reaches admission instead of
    # being blocked, and only then fails on its own claim conflict.
    assert fixture.d_key in Enum.map(commits(commands, :step_failed), & &1.event.data.node_key)
  end

  test "a missing execution package fails only its node and releases its claim", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [fixture.f_target_id])

    state = %{
      fixture.state
      | stage_state: %{fixture.state.stage_state | deferred_node_keys: [fixture.f_key]}
    }

    assert {:cont, awaiting} = Execution.handle_event(state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}
    assert command.event.data.node_key == fixture.f_key
    assert command.event.data.error == :execution_package_required
    assert command.run.status == :running

    # The claim this node acquired is failed with the admission error.
    assert_receive {:materialization_finish, %{status: :failed}}
    assert_receive {:release_execution_lease, _release}
    refute_received {:runner_task_cancellation_requested, _command}
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "a crash while the stage drains after the failure is not resumable", %{fixture: fixture} do
    assert {:cont, _awaiting} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}

    assert {:ok,
            {:uncertain,
             %{
               reason: :active_stage_outcomes_not_resumable,
               runner_tasks: [@held_task_id]
             }}} = Recovery.disposition(command.run)
  end

  @tag runner_task_store: AcceptingTaskStore
  test "a retryable claim failure retains the sibling and replays the exact claim", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claim_error}, @unavailable)

    assert {:persist_retry, paused, %PersistenceRetry{event_type: :materialization_claim} = retry,
            @unavailable} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:materialization_claim, command}
    refute_received {:runner_task_cancellation_requested, _command}
    assert @held_task_id in ActiveTaskSet.task_ids(paused.work_set)

    Process.put({FakeStore, :claimable_target_ids}, [command.target_id])
    assert {:ownership_gate, gated, replay} = Execution.retry_persistence(paused, retry)
    assert_receive {:materialization_claim, ^command}
    assert {:cont, awaiting} = Execution.resume_persisted_retry(gated, replay)
    assert @held_task_id in ActiveTaskSet.task_ids(awaiting.work_set)
    refute_received {:runner_task_cancellation_requested, _command}
    refute_received {:materialization_finish, _command}
  end

  test "a failed failure write resumes and keeps submitting the rest of the stage", %{
    fixture: fixture
  } do
    state = %{
      fixture.state
      | stage_state: %{
          fixture.state.stage_state
          | deferred_node_keys: [fixture.b_key, fixture.c_key]
        }
    }

    Process.put({FakeStore, :commit_failures}, 1)

    assert {:persist_retry, retry_state, %PersistenceRetry{event_type: :step_failed} = retry,
            :forced_commit_failure} = Execution.handle_event(state, :continue)

    assert retry.data.node_key == fixture.b_key
    refute_received {:runner_task_cancellation_requested, _command}

    assert {:cont, awaiting} = Execution.retry_persistence(retry_state, retry)

    failed_node_keys =
      drain_commits() |> commits(:step_failed) |> Enum.map(& &1.event.data.node_key)

    assert fixture.b_key in failed_node_keys

    assert fixture.c_key in failed_node_keys,
           "the remaining node must be submitted after the retry"

    # The refill resume keeps the live stage state, so a sibling that already
    # settled in this attempt keeps its status and its dependents stay runnable.
    assert awaiting.stage_state.node_statuses[fixture.b_key] == :error
    assert awaiting.stage_state.node_statuses[fixture.c_key] == :error

    assert commits(drain_commits(), :step_failed) == [],
           "the retried write must produce exactly one durable event per node"

    refute_received {:runner_task_cancellation_requested, _command}
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  for external_cancel <- [false, true] do
    @tag runner_task_store: AcceptingTaskStore, external_cancel: external_cancel
    test "ambiguous enqueue retains same-batch tasks with external cancellation=#{external_cancel}",
         %{
           fixture: fixture,
           external_cancel: external_cancel
         } do
      conflict =
        Error.new(:conflict, "history busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      Process.put(
        {FakeStore, :claimable_target_ids},
        Enum.map([fixture.b_key, fixture.c_key], &TargetIdentity.for_asset(elem(&1, 0)))
      )

      step =
        FavnOrchestrator.AssetStepIdentity.asset_step_id(
          fixture.run.id,
          fixture.c_key,
          elem(fixture.c_key, 0)
        )

      Process.put({AcceptingTaskStore, :lost_replies}, %{
        step => Error.new(:unavailable, "reply lost", retryable?: true)
      })

      Process.put({FakeStore, :commit_results}, [{:error, conflict}])

      state = %{
        fixture.state
        | stage_state: %{
            fixture.state.stage_state
            | deferred_node_keys: [fixture.b_key, fixture.c_key]
          }
      }

      assert {:persist_retry, paused, retry, ^conflict} = Execution.handle_event(state, :continue)
      {:stage_operation, continuation} = retry.resume

      continuation =
        put_in(continuation.ctx.batch_started_ms, System.monotonic_time(:millisecond) + 10_000)

      retry = %{retry | resume: {:stage_operation, continuation}}

      if external_cancel do
        Process.put({FakeStore, :commit_error_by_event}, %{step_failed: :external_cancel})
      end

      assert {:ownership_gate, gated, ^retry} = Execution.retry_persistence(paused, retry)
      assert {:cont, draining} = Execution.resume_persisted_retry(gated, retry)
      assert_receive {:accepted_enqueue, first}
      assert_receive {:accepted_enqueue, second}
      assert_receive {:accepted_store_cancellation, %{task_id: task_id}}
      assert task_id == second.task_id
      first_id = first.task_id

      if external_cancel,
        do: assert_received({:accepted_store_cancellation, %{task_id: ^first_id}}),
        else: refute_received({:accepted_store_cancellation, %{task_id: ^first_id}})

      assert first.task_id in ActiveTaskSet.task_ids(draining.work_set)
      assert second.task_id in ActiveTaskSet.task_ids(draining.work_set)
      assert Map.has_key?(draining.awaits, first.task_id)
      assert Map.has_key?(draining.awaits, second.task_id)
      refute_received {:materialization_finish, _}
      refute_received {:release_execution_lease, _}
    end
  end

  for phase <- [:admission, :materialization_claim], action <- [:cancel, :exhaust] do
    @tag runner_task_store: AcceptingTaskStore, phase: phase, action: action
    test "same-batch #{phase} #{action} retains the preceding task ownership", %{
      fixture: fixture,
      phase: phase,
      action: action
    } do
      conflict =
        Error.new(:conflict, "history busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      b_target = TargetIdentity.for_asset(elem(fixture.b_key, 0))
      c_target = TargetIdentity.for_asset(elem(fixture.c_key, 0))
      Process.put({FakeStore, :claimable_target_ids}, [b_target])
      Process.put({FakeStore, :claim_error_by_target}, %{c_target => conflict})

      if phase == :admission do
        step =
          FavnOrchestrator.AssetStepIdentity.asset_step_id(
            fixture.run.id,
            fixture.c_key,
            elem(fixture.c_key, 0)
          )

        Process.put({FakeStore, :admit_error_steps}, %{step => conflict})
      end

      state = %{
        fixture.state
        | stage_state: %{
            fixture.state.stage_state
            | deferred_node_keys: [fixture.b_key, fixture.c_key]
          }
      }

      Process.put({FakeStore, :commit_results}, [{:error, conflict}])

      assert {:persist_retry, first_pause, first_retry, ^conflict} =
               Execution.handle_event(state, :continue)

      {:stage_operation, continuation} = first_retry.resume
      # Keep this test on the same-batch path independent of machine speed.
      continuation =
        put_in(continuation.ctx.batch_started_ms, System.monotonic_time(:millisecond) + 10_000)

      first_retry = %{first_retry | resume: {:stage_operation, continuation}}

      assert {:ownership_gate, gated, ^first_retry} =
               Execution.retry_persistence(first_pause, first_retry)

      assert {:persist_retry, paused, retry, ^conflict} =
               Execution.resume_persisted_retry(gated, first_retry)

      assert retry.event_type == phase
      assert_receive {:accepted_enqueue, command}
      previous = Enum.find(paused.paused_admission.entries, &(&1.task_id == command.task_id))
      assert previous.materialization_claim
      refute Map.get(paused.paused_admission.ctx, :materialization_claim)

      result =
        case action do
          :cancel ->
            Execution.cancel(paused, :operator)

          :exhaust ->
            retry = PersistenceRetry.rejected(retry, conflict)
            retry = %{retry | started_ms: System.monotonic_time(:millisecond) - 30_001}
            Execution.retry_persistence(paused, retry)
        end

      assert {:cont, resumed} = result
      assert command.task_id in ActiveTaskSet.task_ids(resumed.work_set)
      lease_id = previous.lease.lease_id
      claim_key = previous.materialization_claim.claim_key
      refute_received {:release_execution_lease, %{lease_id: ^lease_id}}
      refute_received {:materialization_finish, %{claim_key: ^claim_key}}
    end
  end

  for phase <- [:admission, :materialization_claim, :runner_enqueue] do
    @tag runner_task_store: AcceptingTaskStore, phase: phase
    test "cancellation after #{phase} replay owns the newly persisted resources", %{
      fixture: fixture,
      phase: phase
    } do
      conflict =
        Error.new(:conflict, "execution history owner is busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      target = TargetIdentity.for_asset(elem(fixture.b_key, 0))
      Process.put({FakeStore, :claimable_target_ids}, [target])

      case phase do
        :admission ->
          Process.put({FakeStore, :admit_error}, conflict)

        :materialization_claim ->
          Process.put({FakeStore, :claimable_target_ids}, [])
          Process.put({FakeStore, :claim_error}, conflict)

        :runner_enqueue ->
          Process.put({AcceptingTaskStore, :enqueue_error}, conflict)
      end

      assert {:persist_retry, paused, %PersistenceRetry{event_type: ^phase} = retry, ^conflict} =
               Execution.handle_event(fixture.state, :continue)

      Process.delete({FakeStore, :admit_error})
      Process.delete({AcceptingTaskStore, :enqueue_error})
      Process.put({FakeStore, :claimable_target_ids}, [target])
      assert {:ownership_gate, gated, _retry} = Execution.retry_persistence(paused, retry)
      assert {:cont, cancelled} = Execution.cancel(gated, :operator)
      assert cancelled.paused_admission == nil

      case phase do
        :admission ->
          assert_receive {:release_execution_lease, _}

        :materialization_claim ->
          assert_receive {:release_execution_lease, _}
          assert_receive {:materialization_finish, %{status: :failed}}

        :runner_enqueue ->
          assert_receive {:accepted_enqueue, command}
          assert command.task_id in ActiveTaskSet.task_ids(cancelled.work_set)
          refute_received {:release_execution_lease, _}
          refute_received {:materialization_finish, _}
      end
    end
  end

  @tag runner_task_store: AcceptingTaskStore
  test "retryable attempt-start contention pauses and resumes without cancelling siblings", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    assert {:persist_retry, paused, %PersistenceRetry{event_type: :step_started} = retry,
            ^conflict} = Execution.handle_event(fixture.state, :continue)

    assert paused.paused_admission.task_id == retry.data.runner_task_id
    assert @held_task_id in ActiveTaskSet.task_ids(paused.work_set)
    refute_received {:runner_task_cancellation_requested, _command}
    refute_received {:release_execution_lease, _command}
    refute_received {:materialization_finish, _command}

    assert {:ownership_gate, gated, ^retry} = Execution.retry_persistence(paused, retry)
    assert {:cont, awaiting} = Execution.resume_persisted_retry(gated, retry)

    assert awaiting.paused_admission == nil
    assert retry.data.runner_task_id in ActiveTaskSet.task_ids(awaiting.work_set)
    assert @held_task_id in ActiveTaskSet.task_ids(awaiting.work_set)
    assert RunExecutionState.in_flight_count(awaiting) >= 1
    refute_received {:runner_task_cancellation_requested, _command}
    refute_received {:release_execution_lease, _command}
  end

  @tag runner_task_store: AcceptingTaskStore
  test "commit reply loss replays the identical attempt-start command once", %{fixture: fixture} do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    unavailable = Error.new(:unavailable, "transition reply was lost", retryable?: true)
    Process.put({FakeStore, :commit_results}, [{:commit_then_error, unavailable}])

    assert {:persist_retry, paused, %PersistenceRetry{} = retry, ^unavailable} =
             Execution.handle_event(fixture.state, :continue)

    assert_receive {:commit_transition, first_command}
    assert first_command.event.event_type == :step_started

    assert {:ownership_gate, gated, ^retry} = Execution.retry_persistence(paused, retry)
    assert_receive {:commit_transition, replay_command}

    assert replay_command.command_id == first_command.command_id
    assert replay_command.expected_sequence == first_command.expected_sequence
    assert replay_command.run == first_command.run
    assert replay_command.event == first_command.event

    assert {:cont, resumed} = Execution.resume_persisted_retry(gated, retry)
    assert retry.data.runner_task_id in ActiveTaskSet.task_ids(resumed.work_set)
    assert_receive {:accepted_enqueue, %{task_id: task_id}}
    assert task_id == retry.data.runner_task_id
    refute_received {:accepted_enqueue, _duplicate}
  end

  @tag runner_task_store: AcceptingTaskStore
  test "cancellation during attempt-start persistence cleans only local unsubmitted work", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    assert {:persist_retry, paused, %PersistenceRetry{} = retry, ^conflict} =
             Execution.handle_event(fixture.state, :continue)

    local_task_id = retry.data.runner_task_id

    latest = %{
      paused.run
      | event_seq: paused.run.event_seq + 1,
        metadata: Map.put(paused.run.metadata, :cancel_requested, true)
    }

    assert {:cont, cancelled} = Execution.cancel(%{paused | run: latest}, :operator)

    assert cancelled.paused_admission == nil
    assert cancelled.run.event_seq == latest.event_seq
    assert cancelled.run.metadata.cancel_requested
    refute local_task_id in ActiveTaskSet.active_runner_task_ids(cancelled.run)
    assert_receive {:release_execution_lease, _command}
    assert_receive {:materialization_finish, %{status: :failed}}
    assert @held_task_id in ActiveTaskSet.task_ids(cancelled.work_set)
    refute local_task_id in ActiveTaskSet.task_ids(cancelled.work_set)
  end

  @tag runner_task_store: AcceptingTaskStore
  test "cancellation preserves an in-flight ownership renewal as a heartbeat", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    assert {:persist_retry, paused, %PersistenceRetry{} = retry, ^conflict} =
             Execution.handle_event(fixture.state, :continue)

    token = make_ref()

    state = %{
      execution_state: paused,
      storage_renewal_pending: %{
        token: token,
        timer: make_ref(),
        purpose: {:resume, retry},
        renewal_id: "same-renewal",
        reason: conflict
      }
    }

    assert {:noreply, next} =
             RunServer.handle_info({:favn_run_cancel_requested, :operator}, state)

    assert next.storage_renewal_pending.token == token
    assert next.storage_renewal_pending.renewal_id == "same-renewal"
    assert next.storage_renewal_pending.purpose == :heartbeat
  end

  @tag runner_task_store: AcceptingTaskStore
  for path <- [:initial, :replay] do
    test "expired #{path} admission removes its waiter subscription", %{fixture: fixture} do
      alias FavnOrchestrator.ExecutionAdmission.Coordinator
      alias FavnOrchestrator.Persistence.Results.{Admission, AdmissionWaiter}
      alias FavnOrchestrator.RunServer.Execution.StageAdmission
      start_supervised!(Coordinator)
      Process.put({FakeStore, :admit_error}, @unavailable)

      assert {:persist_retry, state, retry, @unavailable} =
               Execution.handle_event(fixture.state, :continue)

      {:stage_operation, pause} = retry.resume
      pause = put_in(pause.ctx.work.deadline_at, ~U[2020-01-01 00:00:00Z])
      command = retry.command

      result = %Admission{
        status: :waiting,
        waiter:
          struct!(AdmissionWaiter, %{
            workspace_id: command.workspace_context.workspace_id,
            waiter_id: "expired-waiter",
            run_id: command.run_id,
            step_id: command.step_id,
            status: :waiting,
            priority: 0,
            blocking_scope_id: hd(command.requests).scope_id,
            requests: command.requests,
            expires_at: DateTime.add(DateTime.utc_now(), 60),
            claim_generation: 0
          })
      }

      Process.delete({FakeStore, :admit_error})
      Process.put({FakeStore, :admission_result}, {:ok, result})

      if unquote(path) == :initial do
        pause = StageAdmission.adopt_operation(pause, result)
        assert Map.has_key?(:sys.get_state(Coordinator).subscribers, "expired-waiter")
        StageAdmission.resume_operation(pause, result)
      else
        retry = %{retry | resume: {:stage_operation, pause}, ambiguous?: true}
        assert {:ownership_gate, gated, replay} = Execution.retry_persistence(state, retry)
        assert Map.has_key?(:sys.get_state(Coordinator).subscribers, "expired-waiter")
        Execution.resume_persisted_retry(gated, replay)
      end

      refute Map.has_key?(:sys.get_state(Coordinator).subscribers, "expired-waiter")
    end
  end

  test "an expired paused deadline never enqueues the task and preserves the sibling", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    assert {:persist_retry, paused, %PersistenceRetry{} = retry, ^conflict} =
             Execution.handle_event(fixture.state, :continue)

    expired_pause = put_in(paused.paused_admission.ctx.work.deadline_at, ~U[2020-01-01 00:00:00Z])
    expired_retry = %{retry | resume: {:stage_operation, expired_pause.paused_admission}}

    assert {:ownership_gate, gated, ^expired_retry} =
             Execution.retry_persistence(expired_pause, expired_retry)

    assert {:cont, awaiting} = Execution.resume_persisted_retry(gated, expired_retry)

    refute_received {:accepted_enqueue, _command}
    refute_received {:accepted_store_cancellation, _command}
    assert_receive {:release_execution_lease, _command}
    assert_receive {:materialization_finish, %{status: :failed}}
    assert @held_task_id in ActiveTaskSet.task_ids(awaiting.work_set)
    assert awaiting.stage_state.node_statuses[fixture.b_key] == :error
  end

  test "a deterministic retry result leaves the persistence loop and follows terminal handling",
       %{
         fixture: fixture
       } do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    invalid = Error.new(:invalid, "invalid transition", retryable?: false)
    Process.put({FakeStore, :commit_results}, [{:error, conflict}, {:error, invalid}])

    assert {:persist_retry, paused, %PersistenceRetry{} = retry, ^conflict} =
             Execution.handle_event(fixture.state, :continue)

    assert {:recovery_required, recovering, {:attempt_start_replay_rejected, ^invalid}} =
             Execution.retry_persistence(paused, retry)

    stopped = Execution.stop_for_recovery(recovering)
    assert stopped.paused_admission == nil
    refute_receive {:commit_transition, %{event: %{event_type: :step_failed}}}
    assert_receive {:release_execution_lease, _command}
    assert_receive {:materialization_finish, %{status: :failed}}
    refute_receive {:runner_task_cancellation_requested, _command}
  end

  @tag runner_task_store: AcceptingTaskStore
  test "normal RunServer termination cleans paused local work", %{fixture: fixture} do
    Process.put({FakeStore, :claimable_target_ids}, [
      TargetIdentity.for_asset(elem(fixture.b_key, 0))
    ])

    conflict =
      Error.new(:conflict, "execution history owner is busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    assert {:persist_retry, paused, %PersistenceRetry{}, ^conflict} =
             Execution.handle_event(fixture.state, :continue)

    assert :ok = RunServer.terminate(:shutdown, %{execution_state: paused})
    assert_receive {:release_execution_lease, _command}
    assert_receive {:materialization_finish, %{status: :failed}}
  end

  for position <- [:initial, :refill] do
    @tag runner_task_store: AcceptingTaskStore
    test "#{position} cancellation tracks a same-batch task saved before the pause", %{
      fixture: fixture
    } do
      Process.put({FakeStore, :claimable_target_ids}, [
        TargetIdentity.for_asset(elem(fixture.b_key, 0))
      ])

      conflict =
        Error.new(:conflict, "execution history owner is busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      Process.put({FakeStore, :commit_results}, [{:error, conflict}])

      assert {:persist_retry, paused, %PersistenceRetry{}, ^conflict} =
               Execution.handle_event(fixture.state, :continue)

      stage_state = if unquote(position) == :initial, do: nil, else: paused.stage_state

      paused = %{
        paused
        | stage_state: stage_state,
          awaits: %{},
          await_monitors: %{},
          await_timers: %{},
          paused_admission: %{paused.paused_admission | entries: [fixture.entry]}
      }

      assert {:cont, draining} = Execution.cancel(paused, :operator)
      assert Map.has_key?(draining.awaits, @held_task_id)
      assert draining.status == :awaiting
    end
  end

  # `resume_retry/2` leaves `stage_state` set when it starts a later stage
  # attempt, so this clause is reached only for the first attempt of a stage,
  # where no node has completed yet. A later attempt resumes through the refill
  # clause above, which keeps the live stage state and its statuses.
  test "the initial-stage resume rebuilds the stage and records the node failure", %{
    fixture: fixture
  } do
    failure = %{
      status: :error,
      error: @conflict,
      node_statuses: %{fixture.b_key => :error}
    }

    resume = {:node_failed, fixture.run, [], [], MapSet.new(), [], failure, nil, %{}}

    retry =
      PersistenceRetry.new(
        fixture.run,
        :step_failed,
        %{node_key: fixture.b_key, error: @conflict},
        {:stage_admission, 1, resume}
      )

    state = %{
      fixture.state
      | stage_state: nil,
        work_set: ActiveTaskSet.from_entries(fixture.run, []),
        awaits: %{},
        await_timers: %{}
    }

    # The rebuilt stage has no deferred work and no awaits, so it finalizes and
    # runs the next stage's classification before deferring to the run loop.
    assert {:cont, resumed} = Execution.retry_persistence(state, retry)
    assert {:terminal, failed} = Execution.handle_event(resumed, :continue)
    assert failed.error == @conflict

    # The payload's node statuses reached downstream classification: only the
    # failed node's dependent is blocked.
    assert node_statuses(failed)[fixture.e_key] == :blocked
  end

  defp fixture do
    a_ref = {__MODULE__.Asset, :a}
    b_ref = {__MODULE__.Asset, :b}
    c_ref = {__MODULE__.Asset, :c}
    d_ref = {__MODULE__.Asset, :d}
    e_ref = {__MODULE__.Asset, :e}
    f_ref = {__MODULE__.Asset, :f}

    a_key = {a_ref, nil}
    b_key = {b_ref, nil}
    c_key = {c_ref, nil}
    d_key = {d_ref, nil}
    e_key = {e_ref, nil}
    f_key = {f_ref, nil}

    nodes = %{
      a_key => plan_node(a_ref, a_key, 0, []),
      b_key => plan_node(b_ref, b_key, 0, []),
      c_key => plan_node(c_ref, c_key, 0, []),
      f_key => plan_node(f_ref, f_key, 0, []),
      d_key => plan_node(d_ref, d_key, 1, [a_key]),
      e_key => plan_node(e_ref, e_key, 1, [b_key])
    }

    plan = %Plan{
      target_refs: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
      target_node_keys: [a_key, b_key, c_key, f_key, d_key, e_key],
      topo_order: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
      stages: [[a_ref, b_ref, c_ref, f_ref], [d_ref, e_ref]],
      node_stages: [[a_key, b_key, c_key, f_key], [d_key, e_key]],
      nodes: nodes
    }

    run =
      RunState.new(
        id: "run-admission-sibling-drain",
        workspace_id: "workspace-admission-sibling-drain",
        deployment_id: "deployment-admission-sibling-drain",
        manifest_version_id: "manifest-admission-sibling-drain",
        manifest_content_hash: String.duplicate("a", 64),
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: a_ref,
        target_refs: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
        plan: plan,
        metadata: %{pipeline_execution_policy: %{max_concurrency: 4}}
      )
      |> RunState.transition(status: :running)
      |> RunState.with_storage_fence("run-owner", 1)

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash,
      runner_releases: run.runner_releases
    }

    assets_by_ref =
      Map.new([a_ref, b_ref, c_ref, d_ref, e_ref], fn {module, name} = ref ->
        {ref, %Asset{ref: ref, module: module, name: name}}
      end)

    # `f` is the only SQL asset, and it has no registered package, so its
    # admission reaches the execution-package call site and fails there.
    {f_module, f_name} = f_ref

    assets_by_ref =
      Map.put(assets_by_ref, f_ref, %Asset{
        ref: f_ref,
        module: f_module,
        name: f_name,
        type: :sql
      })

    manifest_index = %Index{assets_by_ref: assets_by_ref}

    freshness_context = %{
      assets_by_ref: assets_by_ref,
      refresh_policy: %RefreshPolicy{mode: :auto},
      forced_node_keys: MapSet.new(),
      prior_states: %{},
      current_states: %{},
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: ~U[2026-09-03 10:00:00Z]
    }

    decisions =
      Map.new([a_key, b_key, c_key, f_key], fn node_key ->
        {node_key,
         %{decision: :run, reason: :forced, node_key: node_key, freshness_key: "latest"}}
      end)

    entry =
      StageEntry.new!(%{
        run_id: run.id,
        asset_step_id: "step-a",
        asset_ref: a_ref,
        node_key: a_key,
        window: nil,
        task_id: @held_task_id,
        assignment_generation: 0,
        runner_pool: "default",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        decision: Map.fetch!(decisions, a_key),
        stage: 0,
        attempt: 1,
        lease: %{
          workspace_id: run.workspace_id,
          lease_id: "lease-step-a",
          owner_id: "run-owner",
          owner_generation: 1,
          scopes: []
        },
        materialization_claim: nil,
        execution_pool: nil,
        resource_circuit_permits: [],
        freshness_key: "latest",
        version: version,
        manifest_index: manifest_index,
        freshness_context: freshness_context
      })

    timeout_token = make_ref()
    work_set = ActiveTaskSet.from_entries(run, [entry])
    run = ActiveTaskSet.sync_run_metadata(run, work_set)

    state = %RunExecutionState{
      run: run,
      version: version,
      manifest_index: manifest_index,
      mode: :pipeline,
      work_set: work_set,
      stage_groups: [{0, [a_key, b_key, c_key, f_key]}, {1, [d_key, e_key]}],
      stage_index: 0,
      stage_attempt: 1,
      stage_state:
        StageAttemptState.new(run, [], [entry], [b_key], MapSet.new(), nil, :batch_budget),
      stage_decisions: decisions,
      freshness_context: freshness_context,
      stage_freshness_context: freshness_context,
      freshness_checkpoint: %{
        version: 1,
        revision: 1,
        sequence: run.event_seq,
        stage: 0,
        attempt: 1,
        payload_hash: :crypto.hash(:sha256, "checkpoint")
      },
      awaits: %{
        @held_task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: Process.send_after(self(), :unused_timeout, 60_000),
          entry: entry,
          kind: :pipeline
        }
      },
      await_timers: %{timeout_token => @held_task_id}
    }

    %{
      run: run,
      state: state,
      version: version,
      a_key: a_key,
      b_key: b_key,
      c_key: c_key,
      d_key: d_key,
      e_key: e_key,
      f_key: f_key,
      f_target_id: TargetIdentity.for_asset(f_ref),
      d_target_id: TargetIdentity.for_asset(d_ref),
      entry: entry
    }
  end

  defp plan_node(ref, node_key, stage, upstream) do
    {_module, name} = ref

    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: [],
      stage: stage,
      execution_pool: nil,
      target_id: TargetIdentity.for_asset(ref),
      target_generation_id: nil,
      evidence_generation_id: "generation-#{name}",
      action: :run,
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :default
    }
  end

  defp ok_result(fixture) do
    %RunnerResult{
      run_id: fixture.run.id,
      manifest_version_id: fixture.run.manifest_version_id,
      manifest_content_hash: fixture.run.manifest_content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      status: :ok,
      asset_results: []
    }
  end

  defp node_statuses(%RunState{} = run) do
    run
    |> ResultBuilder.node_results()
    |> Map.new(&{&1.node_key, &1.status})
  end

  defp commits(commands, event_type),
    do: Enum.filter(commands, &(&1.event.event_type == event_type))

  defp drain_commits, do: receive_commits([])

  defp receive_commits(acc) do
    receive do
      {:commit_transition, command} -> receive_commits([command | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end

defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionNodeFailureTest.Asset do
end
