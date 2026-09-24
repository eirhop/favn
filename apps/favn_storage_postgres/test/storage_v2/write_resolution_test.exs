defmodule FavnStoragePostgres.StorageV2.WriteResolutionTest do
  use ExUnit.Case, async: false
  alias FavnTestSupport.ExecutionDriver
  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.{PlatformContext, WorkspaceContext}
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnStoragePostgres.{Config, Repo}
  alias FavnStoragePostgres.Registry.Store, as: Registry
  alias FavnStoragePostgres.RunnerTasks.Store
  alias FavnStoragePostgres.Materialization.Store, as: Materialization
  alias FavnStoragePostgres.TestSupport.TaskManifest

  defmodule HistoryConflictAdmissionStore do
    @behaviour FavnOrchestrator.Persistence.RunnerTaskStore
    for {operation, arity} <-
          FavnOrchestrator.Persistence.RunnerTaskStore.behaviour_info(:callbacks) --
            [admit: 1] do
      args = Macro.generate_arguments(arity, __MODULE__)
      @impl true
      def unquote(operation)(unquote_splicing(args)),
        do: apply(FavnStoragePostgres.RunnerTasks.Store, unquote(operation), unquote(args))
    end

    @impl true
    def admit(command) do
      if Process.delete({__MODULE__, :reject_once}) do
        {:error,
         FavnOrchestrator.Persistence.Error.new(:conflict, "history owner busy",
           retryable?: true,
           details: %{reason_code: "execution_history_owner_busy"}
         )}
      else
        FavnStoragePostgres.RunnerTasks.Store.admit(command)
      end
    end
  end

  setup_all do
    {:ok, options} =
      Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: :disable,
        pool: Sandbox,
        pool_size: 4
      )

    start_supervised!({Repo, options})
    :ok = FavnStoragePostgres.StorageV2.Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
    :ok
  end

  setup tags do
    :ok = Sandbox.checkout(Repo, isolation: "REPEATABLE READ")
    id = "write-" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    now = DateTime.utc_now()
    {:ok, platform} = PlatformContext.new("admin", id, [:platform_admin])
    {:ok, context} = WorkspaceContext.new(id, "admin", [:workspace_admin])

    :ok =
      Registry.provision_workspace(%C.ProvisionWorkspace{
        platform_context: platform,
        workspace_id: id,
        slug: id,
        display_name: id,
        occurred_at: now
      })

    f = %{
      workspace_id: id,
      workspace_context: context,
      platform_context: platform,
      runner_pool: "pool_" <> String.replace(id, "-", "_"),
      now: now
    }

    {version, work} = TaskManifest.sql_work(f)

    claim =
      TaskManifest.ownership_claim(f, version, work, Map.get(tags, :purpose, :ownership_only))

    {:ok, Map.merge(f, %{version: version, work: work, claim: claim})}
  end

  test "enqueue rejects a missing materialization owner with an explicit error", f do
    command = enqueue(f)

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.materialization_claims WHERE workspace_id=$1 AND claim_key=$2",
      [f.workspace_id, f.claim.claim_key]
    )

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :invalid}} = Store.enqueue(command)
  end

  test "target-blocked writers stay queued while unrelated work remains claimable", f do
    second_work = %{f.work | run_id: f.work.run_id <> "-second"}
    second_claim = TaskManifest.ownership_claim(f, f.version, second_work)
    second = %{f | work: second_work, claim: second_claim}
    assert {:ok, _} = Store.enqueue(enqueue(f))
    assert {:ok, first_task} = Store.claim(claim_task(f))
    assert {:ok, _} = Store.enqueue(enqueue(second))
    assert {:ok, nil} = Store.claim(claim_task(second, "blocked"))

    {unrelated_version, unrelated_work} = TaskManifest.sql_work(f, :unrelated_write_test)
    unrelated_claim = TaskManifest.ownership_claim(f, unrelated_version, unrelated_work)

    unrelated = %{
      f
      | version: unrelated_version,
        work: unrelated_work,
        claim: unrelated_claim
    }

    assert unrelated.work.logical_target_id != f.work.logical_target_id
    assert {:ok, unrelated_task} = Store.enqueue(enqueue(unrelated))

    assert {:ok, claimed_unrelated} =
             Store.claim(claim_task(unrelated, "unrelated"))

    assert claimed_unrelated.task_id == unrelated_task.task_id
    assert claimed_unrelated.task_kind == :asset_attempt
    assert claimed_unrelated.write_target_id == unrelated.work.logical_target_id

    assert {:ok, second_queued} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: "rt_" <> second.work.run_id
             })

    assert second_queued.status == :queued
    assert second_queued.assigned_runner_instance_id == nil
    assert {:ok, _} = Store.transition(transition(f, first_task))

    assert {:error, %{details: %{reason_code: "target_write_in_progress"}}} =
             Materialization.claim(claim_command(f, command_id: "third", claim_key: "third"))

    assert {:ok, _} =
             FavnStoragePostgres.TargetOperationLocks.Store.acquire_many(
               %C.AcquireTargetOperationLocks{
                 workspace_context: f.workspace_context,
                 command_id: "unrelated",
                 target_ids: ["asset:unrelated:target"],
                 operation_id: "unrelated",
                 operation_type: :rebuild,
                 lease_owner: "unrelated",
                 lease_duration_ms: 30_000,
                 occurred_at: f.now
               }
             )

    assert {:ok, %{status: :queued, payload: %{attempt: 1}}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: second_queued.task_id
             })

    assert {:ok, _} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "safe-failure",
               task_id: first_task.task_id,
               runner_instance_id: first_task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :failed,
               result: nil,
               retry_class: :terminal,
               error: Favn.Contracts.RunnerError.new(outcome: :safe_failure),
               issued_at: f.now,
               occurred_at: f.now
             })

    assert {:ok, second_task} = Store.claim(claim_task(second, "ready"))
    waiting = transition(second, second_task)
    assert {:ok, %{status: :running, assignment_generation: 1}} = Store.transition(waiting)
    assert {:ok, %{status: :running, assignment_generation: 1}} = Store.transition(waiting)
  end

  test "blocked targets are filtered before the bounded claim candidate limit", f do
    assert {:ok, _} = Store.enqueue(enqueue(f))
    assert {:ok, first_task} = Store.claim(claim_task(f))

    blocked =
      for index <- 1..55 do
        work = %{f.work | run_id: "#{f.work.run_id}-blocked-#{index}"}
        claim = TaskManifest.ownership_claim(f, f.version, work)
        blocked_fixture = %{f | work: work, claim: claim}
        assert {:ok, task} = Store.enqueue(enqueue(blocked_fixture))
        task
      end

    {unrelated_version, unrelated_work} = TaskManifest.sql_work(f, :batch_unrelated_write_test)
    unrelated_claim = TaskManifest.ownership_claim(f, unrelated_version, unrelated_work)

    unrelated = %{
      f
      | version: unrelated_version,
        work: unrelated_work,
        claim: unrelated_claim,
        now: DateTime.add(f.now, 1, :second)
    }

    assert unrelated.work.logical_target_id != f.work.logical_target_id
    assert {:ok, unrelated_task} = Store.enqueue(enqueue(unrelated))

    assert Enum.all?(blocked, fn task ->
             DateTime.before?(task.enqueued_at, unrelated_task.enqueued_at)
           end)

    assert {:ok, claimed} = Store.claim(claim_task(unrelated, "after-blocked-batch"))
    assert claimed.task_id == unrelated_task.task_id
    assert claimed.task_kind == :asset_attempt

    assert Enum.all?(blocked, fn task ->
             {:ok, queued} =
               Store.get(%Q.GetRunnerTask{
                 workspace_context: f.workspace_context,
                 task_id: task.task_id
               })

             queued.status == :queued and is_nil(queued.assigned_runner_instance_id)
           end)

    assert {:ok, _} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "release-blocked-batch",
               task_id: first_task.task_id,
               runner_instance_id: first_task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :failed,
               result: nil,
               retry_class: :terminal,
               error: Favn.Contracts.RunnerError.new(outcome: :safe_failure),
               issued_at: f.now,
               occurred_at: f.now
             })
  end

  test "sequential admission restores its original deadline and times out before dispatch", f do
    alias FavnOrchestrator.RunServer.Execution.Sequential
    alias FavnOrchestrator.RunServer.Execution.RunExecutionState
    state = sequential_state(f, 5_000)
    task = start(f)
    assert {:cont, waiting} = continue_after_history_conflict(state)
    [timer] = Map.values(waiting.retry_timers)
    retry = timer.payload
    assert retry.next_attempt == 1
    assert is_integer(retry.admission_deadline_ms)
    RunExecutionState.cancel_timers(waiting)

    assert {:ok, persisted} =
             FavnStoragePostgres.Runs.Store.get_run(%Q.GetRun{
               workspace_context: f.workspace_context,
               run_id: state.run.id
             })

    persisted_retry = persisted.metadata.retry_state.retry
    assert persisted_retry.admission_deadline_ms == retry.admission_deadline_ms

    persisted =
      FavnOrchestrator.RunState.with_storage_fence(
        persisted,
        state.run.storage_owner_id,
        state.run.storage_fencing_token
      )

    restored = %{state | run: persisted}

    assert {:retry_timer, waiting_again, scheduled_retry} =
             Sequential.resume_retry(restored, persisted_retry)

    assert scheduled_retry.admission_deadline_ms == retry.admission_deadline_ms
    # Wait on the same UTC clock as the runtime deadline guard. A monotonic
    # sleep does not prove a wall-clock deadline expired after clock correction.
    assert Enum.any?(1..200, fn _ ->
             if DateTime.to_unix(DateTime.utc_now(), :millisecond) >
                  persisted_retry.admission_deadline_ms do
               true
             else
               Process.sleep(100)
               false
             end
           end)

    assert {:terminal, timed_out} = Sequential.resume_retry(waiting_again, persisted_retry)

    assert timed_out.status == :timed_out

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.runner_tasks WHERE workspace_id=$1 AND run_id=$2",
               [f.workspace_id, state.run.id]
             )

    assert {:ok, %{status: :running}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: task.task_id
             })
  end

  test "sequential admission preserves the attempt and deadline when the owner completes", f do
    alias FavnOrchestrator.RunServer.Execution.Sequential
    alias FavnOrchestrator.RunServer.Execution.RunExecutionState
    state = sequential_state(f, 5_000)
    task = start(f)
    assert {:cont, waiting} = continue_after_history_conflict(state)
    [timer] = Map.values(waiting.retry_timers)
    RunExecutionState.cancel_timers(waiting)

    assert {:ok, _} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "safe-end",
               task_id: task.task_id,
               runner_instance_id: task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :failed,
               result: nil,
               retry_class: :terminal,
               error: Favn.Contracts.RunnerError.new(outcome: :safe_failure),
               issued_at: f.now,
               occurred_at: f.now
             })

    assert {:await, admitted, entry} = Sequential.resume_retry(waiting, timer.payload)
    assert entry.attempt == 1
    refute Map.has_key?(admitted.run.metadata, :retry_state)

    assert {:ok, queued} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: entry.task_id
             })

    assert queued.payload.attempt == 1

    assert DateTime.to_unix(queued.deadline_at, :millisecond) ==
             timer.payload.admission_deadline_ms
  end

  test "pipeline claim history retry becomes a normal wait for the live target writer", f do
    alias FavnOrchestrator.RunServer.Execution.RunExecutionState
    alias FavnOrchestrator.RunServer.Execution.FreshnessContext
    state = sequential_state(f, 5_000, dependencies: :all)
    task = start(f)
    {:ok, context} = FreshnessContext.initialize(state.run, state.manifest_index)

    state = %{
      state
      | mode: :pipeline,
        stage_groups: [{0, state.run.plan.target_node_keys}],
        freshness_context: context
    }

    install_history_conflict_store()
    Process.put({HistoryConflictAdmissionStore, :reject_once}, true)

    assert {:persist_retry, paused, retry,
            %{details: %{reason_code: "execution_history_owner_busy"}}} =
             ExecutionDriver.handle_event(state, :continue)

    assert retry.event_type == :runner_admission
    assert {:cont, waiting} = ExecutionDriver.retry_persistence(paused, retry)
    assert waiting.paused_admission == nil
    assert waiting.terminal_failure == nil
    assert waiting.stage_state.deferred_node_keys == state.run.plan.target_node_keys
    deadline = waiting.stage_admission_deadline_ms
    assert is_integer(deadline)
    RunExecutionState.cancel_timers(waiting)

    assert {:ok, _} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "pipeline-safe-end",
               task_id: task.task_id,
               runner_instance_id: task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :failed,
               result: nil,
               retry_class: :terminal,
               error: Favn.Contracts.RunnerError.new(outcome: :safe_failure),
               issued_at: f.now,
               occurred_at: f.now
             })

    assert {:cont, admitted} = ExecutionDriver.handle_event(waiting, :continue)
    assert admitted.stage_admission_deadline_ms == deadline
    assert [%{attempt: 1}] = Map.values(admitted.work_set.entries)
    RunExecutionState.cancel_timers(admitted)
  end

  test "a competing claim prevents admission without leaving a target lock", f do
    alias FavnOrchestrator.RunServer.Execution.Sequential
    state = sequential_state(f, 5_000)
    install_history_conflict_store()
    Process.put({HistoryConflictAdmissionStore, :reject_once}, true)
    assert {:persist_retry, _paused, retry, _reason} = Sequential.continue(state)
    command = retry.command
    operation_id = "combined:" <> state.run.id
    claim = %{command.claim | operation_id: operation_id}

    assert {:ok, _} =
             Materialization.claim(%{
               claim
               | command_id: claim.command_id <> ":competing",
                 owner_id: "competing-owner",
                 operation_id: nil
             })

    lock = %C.AcquireTargetOperationLocks{
      workspace_context: f.workspace_context,
      command_id: operation_id,
      target_ids: [claim.target_id],
      operation_id: operation_id,
      operation_type: :materialization,
      lease_owner: state.run.storage_owner_id,
      lease_duration_ms: 60_000,
      occurred_at: DateTime.utc_now()
    }

    command = %{command | claim: claim, target_lock: lock}

    for _ <- 1..2 do
      assert {:error, %{details: %{reason_code: "target_operation_in_progress"}}} =
               Store.admit(command)

      assert %{rows: [[0]]} =
               SQL.query!(
                 Repo,
                 "SELECT count(*) FROM favn_control.target_operation_locks WHERE workspace_id=$1",
                 [f.workspace_id]
               )

      assert %{rows: [[0]]} =
               SQL.query!(
                 Repo,
                 "SELECT count(*) FROM favn_control.runner_tasks WHERE workspace_id=$1 AND run_id=$2",
                 [f.workspace_id, state.run.id]
               )
    end
  end

  test "rebuild input expectation survives work construction and rejects changed resolution", f do
    alias Favn.RuntimeInput.Resolution
    alias FavnOrchestrator.RunServer.Execution.{Sequential, RunExecutionState}
    alias FavnOrchestrator.RuntimeInputPins
    state = sequential_state(f, 5_000)

    assert {:ok, planned} =
             Resolution.new(%{
               resolver: __MODULE__,
               params: %{version: 1},
               input_identity: "planned"
             })

    expectation = %{
      resolver: Atom.to_string(planned.resolver),
      input_identity: planned.input_identity,
      payload_fingerprint: planned.payload_fingerprint
    }

    state = put_in(state.run.metadata[:runtime_input_expectation], expectation)
    install_history_conflict_store()
    assert {:await, queued, entry} = Sequential.continue(state)

    assert {:ok, task} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: entry.task_id
             })

    assert task.payload.metadata.runtime_input_expectation == expectation

    assert {:ok, _pin} =
             RuntimeInputPins.pin_for_resolution(f.workspace_id, entry.task_id, planned)

    for attrs <- [
          %{input_identity: "changed"},
          %{params: %{version: 2}, payload_fingerprint: nil},
          %{resolver: OtherResolver}
        ] do
      assert {:ok, changed} = Resolution.new(Map.merge(Map.from_struct(planned), attrs))

      assert {:error, :rebuild_runtime_input_pin_changed} =
               RuntimeInputPins.pin_for_resolution(f.workspace_id, entry.task_id, changed)
    end

    RunExecutionState.cancel_timers(queued)
  end

  test "a valid context from another task cannot change the immutable claim binding", f do
    other_work = %{f.work | run_id: f.work.run_id <> "-context"}
    other_claim = TaskManifest.ownership_claim(f, f.version, other_work)
    assert {:ok, queued} = Store.enqueue(enqueue(f))
    other = enqueue(%{f | work: other_work, claim: other_claim})

    context_hash =
      :crypto.hash(:sha256, :erlang.term_to_binary(other.orchestration_context, [:deterministic]))

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET orchestration_context=$1,orchestration_context_hash=$2 WHERE workspace_id=$3 AND task_id=$4",
      [other.orchestration_context, context_hash, f.workspace_id, queued.task_id]
    )

    assert {:ok, %{data_state: :unavailable, persistence_failure: :context}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: queued.task_id
             })

    assert {:ok, nil} = Store.claim(claim_task(f))
  end

  test "an unknown sequential effect survives release, expiry and forged generation linkage", f do
    command = enqueue(f)

    {:ok, wrong_payload, hash} =
      Codec.encode_payload(
        :asset_attempt,
        %{f.work | target_generation_id: Ecto.UUID.generate()}
      )

    assert {:error, _} = Store.enqueue(%{command | payload: wrong_payload, payload_hash: hash})
    task = start(f)
    assert {:ok, %{status: :unknown}} = lose(f, task)

    SQL.query!(
      Repo,
      "UPDATE favn_control.materialization_claims SET expires_at='2000-01-01' WHERE workspace_id=$1",
      [f.workspace_id]
    )

    assert {:error, _} =
             Materialization.claim(
               claim_command(f, command_id: "competitor", claim_key: "competitor")
             )

    assert {:error, _} =
             Materialization.finish(%C.FinishMaterialization{
               workspace_context: f.workspace_context,
               command_id: "blind-release",
               claim_key: f.claim.claim_key,
               owner_id: f.claim.owner_id,
               fencing_token: f.claim.fencing_token,
               expected_version: f.claim.version,
               status: :released,
               occurred_at: f.now
             })

    assert %{rows: [["outcome_unknown"]]} = effect(f)
  end

  test "exact live claim replay preserves its fence after Started and unknown outcome", f do
    task = start(f)
    assert {:ok, %{claim: replay}} = Materialization.claim(claim_command(f))

    assert {replay.fencing_token, replay.expires_at} ==
             {f.claim.fencing_token, f.claim.expires_at}

    assert {:ok, _} = lose(f, task)
    assert {:ok, %{claim: replay}} = Materialization.claim(claim_command(f))
    assert replay.fencing_token == f.claim.fencing_token
    assert {:error, _} = Materialization.claim(claim_command(f, command_id: "different"))
  end

  test "administrator clearance is exact, idempotent and replays without executable data", f do
    task = start(f)
    assert {:ok, _} = lose(f, task)
    command = resolution(f, task)
    assert {:error, _} = Store.resolve_write(%{command | expected_owner_fence: 999})
    assert {:error, _} = Store.resolve_write(%{command | expected_assignment_generation: 999})
    assert {:error, _} = Store.resolve_write(%{command | backend_stopped: false})
    assert {:error, _} = Store.resolve_write(%{command | stopped_at: ~U[2000-01-01 00:00:00Z]})
    assert {:ok, receipt} = Store.resolve_write(command)
    assert receipt["task_id"] == task.task_id
    assert receipt["evidence"] == %{"disposition" => "administrator_verified_no_effect"}
    assert %{rows: [["resolved"]]} = effect(f)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET payload='{}'::jsonb, orchestration_context='{}'::jsonb WHERE workspace_id=$1 AND task_id=$2",
      [f.workspace_id, task.task_id]
    )

    assert {:ok, ^receipt} = Store.resolve_write(command)

    assert {:ok, ^receipt} =
             Store.get_write_resolution(%{
               command
               | workspace_context: %{f.workspace_context | request_id: "new-request"}
             })

    assert {:error, %{kind: :conflict}} =
             Store.get_write_resolution(%{command | reason: "changed"})

    assert {:error, %{kind: :forbidden}} =
             Store.get_write_resolution(%{
               command
               | workspace_context: %{f.workspace_context | roles: [:customer_reader]}
             })

    assert {:ok, %{status: :unknown}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: task.task_id
             })

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.materializations WHERE workspace_id=$1",
               [f.workspace_id]
             )
  end

  test "a valid receipt copied from another proof is rejected", f do
    task = start(f)
    assert {:ok, _} = lose(f, task)
    command = resolution(f, task)
    assert {:ok, receipt} = Store.resolve_write(command)

    {:ok, encoded} =
      Favn.Contracts.RunnerTask.PersistenceData.encode(
        %{receipt | "task_id" => "rt_another_task"},
        262_144
      )

    forged = %{
      "kind" => "write_resolution_v1",
      "resolution" => encoded,
      "hash" =>
        Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(encoded, [:deterministic])),
          case: :lower
        )
    }

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_task_commands SET result=$1 WHERE scope_id=$2 AND command_id=$3",
      [forged, "workspace:" <> f.workspace_id, command.command_id]
    )

    assert {:error, %{kind: :conflict}} = Store.get_write_resolution(command)
    assert {:error, %{kind: :conflict}} = Store.resolve_write(command)
  end

  test "missing or corrupt separately retained packages make task details unavailable", f do
    assert {:ok, task} = Store.enqueue(enqueue(f))
    hash = Base.decode16!(f.work.execution_package.content_hash, case: :lower)

    assert {:ok, _} =
             Registry.retained_task_execution_package(
               f.version,
               f.work.execution_package.content_hash
             )

    SQL.query!(
      Repo,
      "UPDATE favn_control.execution_packages SET payload='{}'::jsonb WHERE content_hash=$1",
      [hash]
    )

    assert {:ok, %{data_state: :unavailable, persistence_failure: :payload}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: task.task_id
             })

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.manifest_execution_packages WHERE manifest_version_id=$1",
      [f.version.manifest_version_id]
    )

    assert {:error, %{details: %{reason_code: "persisted_execution_package_invalid"}}} =
             Registry.retained_task_execution_package(
               f.version,
               f.work.execution_package.content_hash
             )

    assert {:ok, nil} = Store.claim(claim_task(f))

    assert {:ok, %{status: :failed}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: task.task_id
             })
  end

  test "public resolution reauthorizes every replay and records the administrator audit", f do
    alias FavnOrchestrator.Identity

    {:ok, actor} =
      Identity.create_actor(
        f.workspace_context,
        f.workspace_id,
        "test-only-resolution-password-123",
        "Recovery administrator",
        [:admin]
      )

    {:ok, login} = WorkspaceContext.new(f.workspace_id, "auth:test", [:customer_reader])

    {:ok, authenticated} =
      Identity.authenticate_password(login, f.workspace_id, "test-only-resolution-password-123")

    {:ok, session} =
      Identity.issue_session(login, actor.id,
        expected_credential_version: authenticated.credential_version
      )

    {:ok, operator} = FavnOrchestrator.operator_context(f.workspace_id, actor, session)
    task = start(f)
    {:ok, _} = lose(f, task)

    proof = %{
      assignment_generation: 1,
      owner_fence: f.claim.fencing_token,
      stopped_at: DateTime.utc_now(),
      stop_mechanism: :backend_session_terminated,
      runner_stopped: true,
      backend_stopped: true,
      evidence_reference: "test:no-effect",
      reason: "No data-system operation was executed.",
      disposition: :verified_no_effect
    }

    opts = [idempotency_key: "resolution:test:0123456789abcdef", issued_at: f.now]

    assert {:ok, receipt} =
             FavnOrchestrator.resolve_operator_task_write(operator, task.task_id, proof, opts)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET payload='{}'::jsonb WHERE workspace_id=$1",
      [f.workspace_id]
    )

    assert {:ok, ^receipt} =
             FavnOrchestrator.resolve_operator_task_write(operator, task.task_id, proof, opts)

    assert {:ok, page} = Identity.page_audit(f.workspace_context, limit: 100)

    assert Enum.any?(
             page.items,
             &(&1.action == "runner_task.resolve_write" and &1.principal_id == actor.id)
           )

    assert :ok = Identity.revoke_session(f.workspace_context, session.id)

    assert {:error, _} =
             FavnOrchestrator.resolve_operator_task_write(operator, task.task_id, proof, opts)
  end

  test "a replaced pre-start claim is preserved when the old task settles", f do
    assert {:ok, _} = Store.enqueue(enqueue(f))
    assert {:ok, task} = Store.claim(claim_task(f))

    SQL.query!(
      Repo,
      "UPDATE favn_control.materialization_claims SET expires_at='2000-01-01' WHERE workspace_id=$1",
      [f.workspace_id]
    )

    assert {:ok, %{claim: replacement}} =
             Materialization.claim(
               claim_command(f, command_id: "replacement", owner_id: "new-owner")
             )

    assert replacement.fencing_token == f.claim.fencing_token + 1
    assert {:error, %{kind: :fenced}} = Store.transition(transition(f, task))

    assert {:ok, %{status: :failed}} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "preflight-failed",
               task_id: task.task_id,
               runner_instance_id: task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :failed,
               result: nil,
               retry_class: :safe_to_retry,
               error: Favn.Contracts.RunnerError.new(outcome: :safe_failure, retryable?: true),
               issued_at: f.now,
               occurred_at: f.now
             })

    assert %{rows: [["claimed", "new-owner", 2, nil]]} =
             SQL.query!(
               Repo,
               "SELECT status,owner_id,fencing_token,effect_task_id FROM favn_control.materialization_claims WHERE workspace_id=$1",
               [f.workspace_id]
             )
  end

  @tag purpose: :materialization
  test "exact committed materialization settles after expiry without repeating its write", f do
    task = start(f)

    assert {:ok, pinned} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: f.workspace_context,
               task_id: task.task_id
             })

    result = %Favn.Contracts.RunnerResult{
      run_id: f.work.run_id,
      manifest_version_id: f.version.manifest_version_id,
      manifest_content_hash: f.version.content_hash,
      required_runner_release_id: f.work.required_runner_release_id,
      status: :ok,
      asset_results: [
        %Favn.Contracts.RunnerAssetResult{
          ref: f.work.asset_ref,
          status: :ok,
          asset_step_id: f.work.asset_step_id,
          target_operation: f.work.target_operation,
          logical_target_id: f.work.logical_target_id,
          target_generation_id: f.work.target_generation_id,
          write_relation: f.work.write_relation,
          write_outcome: :succeeded,
          evidence: %Favn.Contracts.RunnerAssetEvidence{
            kind: :sql,
            write_outcome: :written,
            generation_commit: %Favn.Contracts.GenerationCommit{
              marker: pinned.generation_precondition.marker,
              physical_fingerprint: String.duplicate("a", 64)
            }
          },
          attempt_count: 1
        }
      ]
    }

    {:ok, encoded} = Codec.encode_result(:asset_attempt, :succeeded, result)

    assert {:ok, _} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: f.workspace_context,
               command_id: "success",
               task_id: task.task_id,
               runner_instance_id: task.assigned_runner_instance_id,
               runner_session_generation: 1,
               assignment_generation: 1,
               result_version: 1,
               outcome: :succeeded,
               result: encoded,
               retry_class: :terminal,
               issued_at: f.now,
               occurred_at: f.now
             })

    SQL.query!(
      Repo,
      "UPDATE favn_control.materialization_claims SET expires_at='2000-01-01' WHERE workspace_id=$1",
      [f.workspace_id]
    )

    command = %C.FinishMaterialization{
      workspace_context: f.workspace_context,
      command_id: "publish",
      claim_key: f.claim.claim_key,
      owner_id: f.claim.owner_id,
      fencing_token: f.claim.fencing_token,
      expected_version: f.claim.version,
      status: :succeeded,
      materialization_id: "mat-" <> f.workspace_id,
      payload: %{},
      occurred_at: f.now
    }

    assert {:error, _} = Materialization.finish(%{command | fencing_token: 999})
    assert {:ok, _} = Materialization.finish(command)
    assert {:ok, _} = Materialization.finish(command)

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.materializations WHERE workspace_id=$1",
               [f.workspace_id]
             )
  end

  for cut <- [:queued, :assigned] do
    test "failed cleanup fences an asset at the #{cut} start boundary", f do
      assert {:ok, queued} = Store.enqueue(enqueue(f))

      task =
        if unquote(cut) == :assigned do
          assert {:ok, assigned} = Store.claim(claim_task(f))
          assigned
        else
          queued
        end

      SQL.query!(
        Repo,
        "UPDATE favn_control.runs SET status='error', snapshot=jsonb_set(snapshot, '{metadata,failure_cleanup}', '{\"version\":1,\"state\":\"pending\"}'::jsonb) WHERE workspace_id=$1 AND run_id=$2",
        [f.workspace_id, f.work.run_id]
      )

      if unquote(cut) == :queued do
        assert {:ok, nil} = Store.claim(claim_task(f, "after-failure"))

        assert {:ok, %{status: :cancelled}} =
                 Store.get(%Q.GetRunnerTask{
                   workspace_context: f.workspace_context,
                   task_id: task.task_id
                 })
      else
        assert {:error, %{kind: :fenced}} = Store.transition(transition(f, task))
      end

      assert %{rows: [["not_started"]]} = effect(f)
    end
  end

  @tag purpose: :materialization
  test "queued cancellation retains conclusive pre-start proof and permits safe claim settlement",
       f do
    assert {:ok, task} = Store.enqueue(enqueue(f))

    assert {:ok, cancelled} =
             Store.request_cancellation(%C.RequestRunnerTaskCancellation{
               workspace_context: f.workspace_context,
               command_id: "cancel-before-start",
               task_id: task.task_id,
               reason: :failed_run_cleanup,
               occurred_at: f.now,
               issued_at: f.now
             })

    assert cancelled.status == :cancelled
    assert cancelled.assignment_generation == 0
    assert cancelled.retry_class == :unknown_do_not_retry
    refute FavnOrchestrator.RunServer.FailureCleanup.uncertain_write?(cancelled)

    assert {:ok, %{claim: %{status: :failed}}} =
             Materialization.finish(%C.FinishMaterialization{
               workspace_context: f.workspace_context,
               command_id: "settle-unstarted",
               claim_key: f.claim.claim_key,
               owner_id: f.claim.owner_id,
               fencing_token: f.claim.fencing_token,
               expected_version: f.claim.version,
               status: :failed,
               error: %{reason: "cancelled_before_start"},
               occurred_at: f.now
             })
  end

  test "released ownership claims retain their reusable fencing identity", f do
    command = %C.FinishMaterialization{
      workspace_context: f.workspace_context,
      command_id: "abandon",
      claim_key: f.claim.claim_key,
      owner_id: f.claim.owner_id,
      fencing_token: f.claim.fencing_token,
      expected_version: f.claim.version,
      status: :released,
      occurred_at: f.now
    }

    assert {:error, _} = Materialization.finish(%{command | fencing_token: 999})
    assert {:ok, %{claim: %{status: :released}}} = Materialization.finish(command)
    assert {:ok, _} = Materialization.finish(command)

    assert {:ok, %{deleted_count: 0}} =
             Repo.transaction(fn ->
               FavnStoragePostgres.Maintenance.Retention.lock!()

               FavnStoragePostgres.Maintenance.RetentionFamilies.delete!(
                 :operations,
                 %FavnOrchestrator.Retention.Policy{row_limit: 10},
                 DateTime.add(f.now, 1, :second),
                 %{"phase" => 3},
                 f.workspace_id
               )
             end)

    assert %{rows: [[fence]]} =
             SQL.query!(
               Repo,
               "SELECT fencing_token FROM favn_control.materialization_claims WHERE workspace_id=$1 AND claim_key=$2",
               [f.workspace_id, f.claim.claim_key]
             )

    assert fence == f.claim.fencing_token

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.materializations WHERE workspace_id=$1",
               [f.workspace_id]
             )
  end

  defp continue_after_history_conflict(state) do
    alias FavnOrchestrator.RunServer.Execution.Sequential

    install_history_conflict_store()
    Process.put({HistoryConflictAdmissionStore, :reject_once}, true)

    assert {:persist_retry, paused, retry,
            %{details: %{reason_code: "execution_history_owner_busy"}}} =
             Sequential.continue(state)

    assert retry.event_type == :runner_admission
    ExecutionDriver.retry_persistence(paused, retry)
  end

  defp install_history_conflict_store do
    alias FavnOrchestrator.Persistence.Runtime
    alias FavnStoragePostgres.Backend

    {:ok, runtime} =
      Runtime.start_link(%Runtime{
        backend: Backend,
        options: [],
        stores: %{Backend.stores() | runner_tasks: HistoryConflictAdmissionStore}
      })

    Process.unlink(runtime)
    on_exit(fn -> if Process.alive?(runtime), do: GenServer.stop(runtime) end)
  end

  defp sequential_state(f, timeout_ms, opts \\ []) do
    ref = f.work.asset_ref
    key = {ref, nil}

    node = %{
      ref: ref,
      node_key: key,
      window: nil,
      stage: 0,
      upstream: [],
      downstream: [],
      action: :run,
      runner_pool: f.work.runner_pool,
      execution_pool: nil,
      target_id: f.work.logical_target_id,
      target_generation_id: f.work.target_generation_id,
      evidence_generation_id: f.work.target_generation_id,
      physical_relation: f.work.write_relation,
      input_generations: [],
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :asset
    }

    plan = %Favn.Plan{
      dependencies: Keyword.get(opts, :dependencies, :none),
      target_refs: [ref],
      target_node_keys: [key],
      nodes: %{key => node},
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[key]]
    }

    second_work = %{f.work | run_id: f.work.run_id <> "-sequential"}

    TaskManifest.ownership_claim(
      Map.merge(f, %{plan: plan, timeout_ms: timeout_ms}),
      f.version,
      second_work
    )

    {:ok, run} =
      FavnStoragePostgres.Runs.Store.get_run(%Q.GetRun{
        workspace_context: f.workspace_context,
        run_id: second_work.run_id
      })

    {:ok, authority} =
      FavnOrchestrator.RunOwnership.claim(f.workspace_context, run.id, "sequential-test-owner")

    run =
      FavnOrchestrator.RunState.with_storage_fence(
        run,
        authority.owner_id,
        authority.fencing_token
      )

    FavnOrchestrator.TestSupport.UnitRunAuthority.start(run)
    {:ok, index} = Favn.Manifest.Index.build_from_version(f.version)

    FavnOrchestrator.RunServer.Execution.RunExecutionState.new(run, f.version,
      manifest_index: index,
      mode: :sequential,
      manifest_lease_id: nil,
      sequential_refs: [{ref, key, 0}]
    )
  end

  defp enqueue(f) do
    {:ok, payload, hash} = Codec.encode_payload(:asset_attempt, f.work)

    {:ok, context} =
      RunnerTaskContext.encode(
        if(f.claim.purpose == :ownership_only,
          do: %{kind: :sequential, materialization_claim: f.claim},
          else: %{
            kind: :pipeline,
            materialization_claim: f.claim,
            decision: %{
              decision: :run,
              reason: :freshness_expired,
              node_key: {f.work.asset_ref, nil},
              freshness_key: "fixture"
            },
            freshness_key: "fixture",
            resource_circuit_permits: [],
            freshness_checkpoint: %{
              version: 1,
              revision: 1,
              sequence: 1,
              stage: 0,
              attempt: 1,
              payload_hash: <<0::256>>
            }
          }
        )
      )

    %C.EnqueueRunnerTask{
      workspace_context: f.workspace_context,
      command_id: "enqueue-" <> f.work.run_id,
      task_id: "rt_" <> f.work.run_id,
      domain_identity: f.work.run_id,
      task_kind: :asset_attempt,
      manifest_version_id: f.version.manifest_version_id,
      manifest_content_hash: f.version.content_hash,
      runner_pool: f.runner_pool,
      required_runner_release_id: f.work.required_runner_release_id,
      run_id: f.work.run_id,
      write_target_id: f.work.logical_target_id,
      write_claim_key: f.claim.claim_key,
      write_claim_fence: f.claim.fencing_token,
      payload: payload,
      payload_hash: hash,
      orchestration_context: context,
      retry_class: :unknown_do_not_retry,
      issued_at: f.now,
      occurred_at: f.now
    }
  end

  defp claim_task(f, suffix \\ nil),
    do: %C.ClaimRunnerTask{
      platform_context: f.platform_context,
      command_id: "claim-task-" <> f.work.run_id <> to_string(suffix),
      runner_instance_id: "runner-" <> f.work.run_id <> to_string(suffix),
      runner_session_generation: 1,
      runner_pool: f.runner_pool,
      required_runner_release_id: f.work.required_runner_release_id,
      supported_task_kinds: [:asset_attempt],
      capabilities: [],
      lease_duration_ms: 30_000,
      issued_at: f.now,
      occurred_at: f.now
    }

  defp transition(f, task),
    do: %C.TransitionRunnerTask{
      workspace_context: f.workspace_context,
      command_id: "start-" <> task.task_id,
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: 1,
      assignment_generation: 1,
      transition: :running,
      issued_at: f.now,
      occurred_at: f.now
    }

  defp start(f) do
    assert {:ok, _} = Store.enqueue(enqueue(f))
    assert {:ok, task} = Store.claim(claim_task(f))
    assert {:ok, running} = Store.transition(transition(f, task))
    running
  end

  defp lose(f, task),
    do:
      Store.release(%C.ReleaseRunnerTask{
        workspace_context: f.workspace_context,
        command_id: "lost",
        task_id: task.task_id,
        runner_instance_id: task.assigned_runner_instance_id,
        runner_session_generation: 1,
        assignment_generation: 1,
        disposition: :unknown,
        reason: Favn.Contracts.RunnerError.new(outcome: :unknown),
        issued_at: f.now,
        occurred_at: f.now
      })

  defp resolution(f, task),
    do: %C.ResolveRunnerTaskWrite{
      workspace_context: f.workspace_context,
      command_id: "resolve",
      task_id: task.task_id,
      expected_assignment_generation: 1,
      expected_owner_fence: f.claim.fencing_token,
      stopped_at: DateTime.utc_now(),
      stop_mechanism: :backend_session_terminated,
      runner_stopped: true,
      backend_stopped: true,
      evidence_reference: "test:no-effect",
      reason: "The test never executed a data-system write.",
      disposition: :verified_no_effect,
      observation_task_ids: [],
      issued_at: f.now,
      occurred_at: f.now
    }

  defp claim_command(f, overrides \\ []),
    do:
      struct!(
        %C.ClaimMaterialization{
          workspace_context: f.workspace_context,
          command_id: "claim:" <> f.work.run_id,
          claim_key: f.claim.claim_key,
          purpose: :ownership_only,
          deployment_id: f.claim.deployment_id,
          target_kind: :asset,
          target_id: f.work.logical_target_id,
          target_generation_id: f.work.target_generation_id,
          evidence_generation_id: f.work.target_generation_id,
          partition_key: "latest",
          run_id: f.work.run_id,
          owner_id: "fixture-owner",
          lease_duration_ms: 60_000,
          occurred_at: f.now
        },
        overrides
      )

  defp effect(f),
    do:
      SQL.query!(
        Repo,
        "SELECT effect_state FROM favn_control.materialization_claims WHERE workspace_id=$1",
        [f.workspace_id]
      )
end
