defmodule FavnStoragePostgres.StorageV2.RunnerTaskAdmissionTest do
  use ExUnit.Case, async: false
  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
  alias FavnOrchestrator.RunState
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunOwnership.Store, as: Ownership
  alias FavnStoragePostgres.RunnerTasks.Store, as: Tasks
  alias FavnStoragePostgres.Runs.Store, as: Runs
  alias FavnStoragePostgres.Schemas.{CapacityScope, ExecutionLease, RunEvent, RunnerTask}
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.TestSupport.RunFixture

  setup_all do
    {:ok, options} =
      Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: :disable,
        pool_size: 8
      )

    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    :ok
  end

  setup tags do
    id = Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)
    run_id = "admission-run-" <> id
    workspace = "admission-ws-" <> id
    context = RunFixture.create(workspace, [run_id], runner_pool: :atomic_admission_fixture)
    {:ok, run} = Runs.get_run(%Q.GetRun{workspace_context: context, run_id: run_id})

    {:ok, owner} =
      Ownership.claim_run(%C.ClaimRun{
        workspace_context: context,
        command_id: "owner-" <> id,
        run_id: run_id,
        owner_id: "owner",
        lease_duration_ms: 300_000
      })

    now = DateTime.utc_now()

    run = %{
      run
      | status: :running,
        storage_owner_id: owner.owner_id,
        storage_fencing_token: owner.fencing_token,
        event_seq: 2
    }

    ref = {RunFixture, :asset}

    work = %RunnerWork{
      run_id: run.id,
      asset_ref: ref,
      asset_refs: [ref],
      asset_step_id: "step",
      node_identity: %Favn.Plan.NodeIdentity{
        manifest_version_id: run.manifest_version_id,
        runner_pool: :atomic_admission_fixture,
        node_key: {ref, nil}
      },
      stage: 0,
      attempt: 1,
      runner_pool: :atomic_admission_fixture,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      deadline_at: DateTime.add(now, tags[:deadline_ms] || 120_000, :millisecond)
    }

    bare = %{kind: :sequential, materialization_claim: nil}
    {:ok, intent} = AdmissionIntent.new(run, work, bare, now)
    {:ok, metadata} = AdmissionIntent.put(run.metadata, intent)
    run = RunState.with_snapshot_hash(%{run | metadata: metadata})
    {:ok, _} = Runs.commit_transition(transition(context, run, :run_started, %{}, now))
    {:ok, enqueue, _work} = AssetRunnerTasks.prepare(run, work, {ref, nil}, 1, bare)
    {:ok, fingerprint} = AdmissionIntent.fingerprint(intent)
    {:ok, metadata} = AdmissionIntent.clear(run.metadata, intent)

    next =
      RunState.with_snapshot_hash(%{
        run
        | event_seq: 3,
          metadata: Map.put(metadata, :active_runner_task_ids, [intent.task_id])
      })

    event_data = %{
      runner_task_id: intent.task_id,
      asset_step_id: "step",
      asset_ref: ref,
      stage: 0,
      attempt: 1,
      admission_intent_hash: fingerprint
    }

    command = %C.AdmitRunnerTask{
      intent: intent,
      enqueue: enqueue,
      transition: transition(context, next, :step_started, event_data, now)
    }

    %{command: command, run: run, context: context}
  end

  test "task and start event commit together; replay survives later run progress", f do
    assert {:ok, %{status: :admitted, replayed?: false}} = Tasks.admit(f.command)
    assert task_count(f) == 1
    next = %{f.command.transition.run | event_seq: 4}

    assert {:ok, _} =
             Runs.commit_transition(
               transition(
                 f.context,
                 next,
                 :step_queued,
                 %{asset_step_id: "sibling"},
                 DateTime.utc_now()
               )
             )

    assert {:ok, %{status: :admitted, replayed?: true, task: %{data_state: :available}}} =
             Tasks.admit(f.command)

    assert task_count(f) == 1

    assert Repo.aggregate(
             from(e in RunEvent, where: e.run_id == ^f.run.id and e.event_type == "step_started"),
             :count
           ) == 1

    assert {:ok, %{event_seq: 4}} =
             Runs.get_run(%Q.GetRun{workspace_context: f.context, run_id: f.run.id})
  end

  test "enqueue validation rolls back acquired capacity and preserves the pending intent", f do
    command = with_capacity(f)

    broken = %{
      command
      | enqueue: %{command.enqueue | payload_hash: :crypto.hash(:sha256, "changed")}
    }

    assert {:error, _} = Tasks.admit(broken)
    assert task_count(f) == 0

    assert Repo.get!(CapacityScope, command.capacity.requests |> hd() |> Map.fetch!(:scope_id)).active_count ==
             0

    refute Repo.get_by(ExecutionLease,
             workspace_id: f.run.workspace_id,
             lease_id: command.capacity.lease_id
           )

    assert {:ok, run} = Runs.get_run(%Q.GetRun{workspace_context: f.context, run_id: f.run.id})
    assert run.event_seq == 2
    assert Map.has_key?(run.metadata, AdmissionIntent.metadata_key())
    assert {:ok, %{status: :admitted}} = Tasks.admit(command)
  end

  test "a stale owner cannot acquire or replay admission", f do
    assert {:ok, _} = Tasks.admit(f.command)

    stale = %{
      f.command
      | transition: %{
          f.command.transition
          | fencing_token: 99,
            run: %{f.command.transition.run | storage_fencing_token: 99}
        }
    }

    assert {:error, %{kind: :fenced}} = Tasks.admit(stale)
    assert task_count(f) == 1
  end

  test "a capacity waiter alone commits, then reevaluates without leaking a lease", f do
    command = with_capacity(f, 0)
    assert {:ok, %{status: :waiting}} = Tasks.admit(command)
    assert task_count(f) == 0
    scope = hd(command.capacity.requests).scope_id

    SQL.query!(
      Repo,
      "UPDATE favn_control.capacity_scopes SET active_count=0 WHERE scope_id=$1",
      [scope]
    )

    assert {:ok, %{status: :waiting}} = Tasks.admit(command)

    next = %{
      command
      | capacity: %{command.capacity | command_id: command.capacity.command_id <> ":recheck"}
    }

    assert {:ok, %{status: :admitted}} = Tasks.admit(next)
    assert Repo.get!(CapacityScope, scope).active_count == 1
  end

  test "a different attempt cannot reuse a capacity identity", f do
    command = with_capacity(f)

    wrong = %{
      command
      | capacity: %{
          command.capacity
          | lease_id:
              FavnOrchestrator.ExecutionAdmission.Identity.lease_id(f.run.id, "step", 0, 2)
        }
    }

    assert {:error, %{kind: :invalid}} = Tasks.admit(wrong)
    assert task_count(f) == 0
    assert Repo.get!(CapacityScope, hd(command.capacity.requests).scope_id).active_count == 0
  end

  test "valid encoded work cannot substitute a different attempt for the intent", f do
    alias FavnStoragePostgres.RunnerTasks.Codec
    alias FavnStoragePostgres.Registry.Store, as: Registry

    {:ok, version} =
      Registry.get_manifest(%Q.ManifestSelector.ById{
        manifest_version_id: f.run.manifest_version_id
      })

    {:ok, work} = Codec.decode_payload(:asset_attempt, f.command.enqueue.payload, version, [])

    {:ok, payload, hash} =
      Codec.encode_payload(:asset_attempt, %{work | attempt: 2, max_attempts: 2})

    command = with_capacity(f)
    changed = %{command | enqueue: %{command.enqueue | payload: payload, payload_hash: hash}}
    assert {:error, %{kind: :invalid}} = Tasks.admit(changed)
    assert task_count(f) == 0
    assert Repo.get!(CapacityScope, hd(command.capacity.requests).scope_id).active_count == 0
  end

  test "waiting takeover preserves scopes, original intent and exact-command rejection", f do
    command = with_capacity(f, 0)
    assert {:ok, %{status: :waiting}} = Tasks.admit(command)

    SQL.query!(
      Repo,
      "UPDATE favn_control.run_ownerships SET expires_at='2000-01-01', next_recovery_at='2000-01-01' WHERE workspace_id=$1",
      [f.run.workspace_id]
    )

    assert {:ok, owner} =
             Ownership.claim_run(%C.ClaimRun{
               workspace_context: f.context,
               command_id: "takeover:" <> f.run.id,
               run_id: f.run.id,
               owner_id: "replacement",
               lease_duration_ms: 300_000
             })

    next = %{
      command
      | transition: %{
          command.transition
          | owner_id: owner.owner_id,
            fencing_token: owner.fencing_token,
            run: %{
              command.transition.run
              | storage_owner_id: owner.owner_id,
                storage_fencing_token: owner.fencing_token
            }
        },
        capacity: %{
          command.capacity
          | owner_id: owner.owner_id,
            owner_generation: owner.fencing_token
        }
    }

    assert {:error, %{kind: :conflict}} = Tasks.admit(next)

    SQL.query!(Repo, "UPDATE favn_control.capacity_scopes SET active_count=0 WHERE scope_id=$1", [
      hd(command.capacity.requests).scope_id
    ])

    next = %{
      next
      | capacity: %{next.capacity | command_id: next.capacity.command_id <> ":takeover"}
    }

    assert {:ok, %{status: :admitted, capacity: %{lease: lease}}} = Tasks.admit(next)
    assert lease.owner_id == "replacement"
    assert lease.owner_generation == owner.fencing_token
    assert task_count(f) == 1
  end

  @tag deadline_ms: 1_000
  test "deadline expiry while capacity is locked rolls back the entire admission", f do
    command = with_capacity(f)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(
            Repo,
            "SELECT scope_id FROM favn_control.capacity_scopes WHERE scope_id=$1 FOR UPDATE",
            [hd(command.capacity.requests).scope_id]
          )

          send(parent, :capacity_locked)

          receive do
            :release -> :ok
          after
            10_000 -> raise "lock not released"
          end
        end)
      end)

    assert_receive :capacity_locked, 2_000
    contender = Task.async(fn -> Tasks.admit(command) end)
    assert Task.yield(contender, 30) == nil

    # The store checks PostgreSQL's wall clock again after obtaining the lock.
    assert Enum.any?(1..200, fn _ ->
             %{rows: [[expired?]]} =
               SQL.query!(Repo, "SELECT clock_timestamp() > $1::timestamptz", [
                 command.intent.deadline_at
               ])

             if expired? do
               true
             else
               Process.sleep(25)
               false
             end
           end)

    send(holder.pid, :release)
    assert {:ok, :ok} = Task.await(holder)

    assert {:error, %{details: %{reason_code: "admission_deadline_expired"}}} =
             Task.await(contender)

    assert task_count(f) == 0
    assert Repo.get!(CapacityScope, hd(command.capacity.requests).scope_id).active_count == 0
  end

  test "terminal capacity cleanup requires the current owner and releases exactly once", f do
    command = with_capacity(f)
    assert {:ok, admitted} = Tasks.admit(command)
    scope_id = hd(command.capacity.requests).scope_id

    release = %C.ReleaseCompletedExecution{
      workspace_context: f.context,
      run_id: f.run.id,
      task_id: admitted.task.task_id,
      owner_id: f.run.storage_owner_id,
      owner_generation: f.run.storage_fencing_token
    }

    assert {:error, _} = FavnStoragePostgres.Admission.Store.release_completed(release)
    assert Repo.get!(CapacityScope, scope_id).active_count == 1
    now = DateTime.utc_now()

    assert {:ok, _} =
             Tasks.request_cancellation(%C.RequestRunnerTaskCancellation{
               workspace_context: f.context,
               command_id: "cancel-terminal-cleanup",
               task_id: admitted.task.task_id,
               reason: %{"reason" => "test cancellation before runner execution"},
               issued_at: now,
               occurred_at: now
             })

    assert {:error, %{kind: :fenced}} =
             FavnStoragePostgres.Admission.Store.release_completed(%{
               release
               | owner_id: "stale-owner"
             })

    assert Repo.get!(CapacityScope, scope_id).active_count == 1

    SQL.query!(
      Repo,
      "UPDATE favn_control.execution_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE lease_id=$1",
      [command.capacity.lease_id]
    )

    assert {:ok, _} = FavnStoragePostgres.Admission.Store.release_completed(release)
    assert {:ok, _} = FavnStoragePostgres.Admission.Store.release_completed(release)
    assert Repo.get!(CapacityScope, scope_id).active_count == 0
  end

  test "checkpoint and position either commit together or both roll back", f do
    run = %{f.run | event_seq: 3} |> RunState.with_snapshot_hash()
    position = %{version: 1, mode: "sequential", phase: "admit", index: 0, attempt: 1}

    change =
      transition(
        f.context,
        run,
        :run_execution_position,
        %{position: position},
        DateTime.utc_now()
      )

    payload = :erlang.term_to_binary(%{test_checkpoint: true}, [:deterministic])

    command = %C.PutRunExecutionCheckpoint{
      workspace_context: f.context,
      run_id: f.run.id,
      owner_id: f.run.storage_owner_id,
      fencing_token: f.run.storage_fencing_token,
      checkpoint_version: 1,
      checkpoint_revision: 1,
      checkpoint_sequence: 3,
      stage: 0,
      attempt: 1,
      payload: payload,
      payload_hash: :crypto.hash(:sha256, payload),
      occurred_at: DateTime.utc_now(),
      transition: change
    }

    # The transition commits inside the outer transaction before this wrong revision is rejected.
    assert {:error, _} = Runs.put_execution_checkpoint(%{command | checkpoint_revision: 2})

    assert {:ok, %{event_seq: 2}} =
             Runs.get_run(%Q.GetRun{workspace_context: f.context, run_id: f.run.id})

    assert {:error, %{kind: :not_found}} =
             Runs.get_execution_checkpoint(%Q.GetRunExecutionCheckpoint{
               workspace_context: f.context,
               run_id: f.run.id
             })

    assert {:ok, checkpoint} = Runs.put_execution_checkpoint(command)
    assert checkpoint.checkpoint_sequence == 3
    assert {:ok, ^checkpoint} = Runs.put_execution_checkpoint(command)

    assert {:ok, %{event_seq: 3}} =
             Runs.get_run(%Q.GetRun{workspace_context: f.context, run_id: f.run.id})

    assert Repo.aggregate(
             from(e in RunEvent,
               where: e.run_id == ^f.run.id and e.event_type == "run_execution_position"
             ),
             :count
           ) == 1
  end

  test "ordinary enqueue and compound admission serialize on the same cancellation owner", f do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          FavnStoragePostgres.CancellationOwnership.lock!(f.run.workspace_id, f.run.id)
          send(parent, :owner_locked)
          receive do: (:release_owner -> :ok)
        end)
      end)

    assert_receive :owner_locked, 2_000
    compound = Task.async(fn -> Tasks.admit(f.command) end)
    ordinary = Task.async(fn -> Tasks.enqueue(f.command.enqueue) end)
    assert Task.yield(compound, 30) == nil
    assert Task.yield(ordinary, 30) == nil
    send(holder.pid, :release_owner)
    assert {:ok, :ok} = Task.await(holder)
    compound_result = Task.await(compound)
    assert {:ok, _} = Task.await(ordinary)

    assert match?({:ok, %{status: :admitted}}, compound_result) or
             match?(
               {:error, %{details: %{reason_code: "admission_replay_mismatch"}}},
               compound_result
             )

    assert task_count(f) == 1
  end

  test "admission waits for deployment policy before locking capacity", f do
    command = with_capacity(f)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(
            Repo,
            "SELECT workspace_id FROM favn_control.workspace_runtime_state WHERE workspace_id=$1 FOR UPDATE",
            [f.run.workspace_id]
          )

          send(parent, :deployment_policy_locked)
          receive do: (:take_capacity -> :ok)

          SQL.query!(
            Repo,
            "SELECT scope_id FROM favn_control.capacity_scopes WHERE scope_id=$1 FOR UPDATE",
            [hd(command.capacity.requests).scope_id]
          )
        end)
      end)

    assert_receive :deployment_policy_locked, 2_000

    contender =
      Task.async(fn ->
        Repo.transaction(fn ->
          %{rows: [[pid]]} = SQL.query!(Repo, "SELECT pg_backend_pid()", [])
          send(parent, {:admission_backend, pid})
          Tasks.admit(command)
        end)
      end)

    assert_receive {:admission_backend, backend}, 2_000
    await_blocked_backend!(backend)
    send(holder.pid, :take_capacity)
    assert {:ok, _} = Task.await(holder)
    assert {:ok, {:ok, %{status: :admitted}}} = Task.await(contender)
    assert task_count(f) == 1
  end

  defp await_blocked_backend!(pid, remaining \\ 200)
  defp await_blocked_backend!(_pid, 0), do: flunk("admission did not wait on policy")

  defp await_blocked_backend!(pid, remaining) do
    case SQL.query!(Repo, "SELECT cardinality(pg_blocking_pids($1))", [pid]).rows do
      [[count]] when count > 0 ->
        :ok

      _ ->
        receive do
        after
          10 -> :ok
        end

        await_blocked_backend!(pid, remaining - 1)
    end
  end

  defp with_capacity(f, limit \\ 1) do
    scope = "scope:" <> f.run.id

    Repo.insert!(%CapacityScope{
      scope_id: scope,
      workspace_id: f.run.workspace_id,
      scope_kind: "pipeline",
      scope_key: f.run.id,
      capacity_limit: max(limit, 1),
      active_count: if(limit == 0, do: 1, else: 0),
      version: 1
    })

    %{
      f.command
      | capacity: %C.AdmitExecution{
          workspace_context: f.context,
          command_id: "admit:" <> f.run.id,
          lease_id: FavnOrchestrator.ExecutionAdmission.Identity.lease_id(f.run.id, "step", 0, 1),
          waiter_id:
            FavnOrchestrator.ExecutionAdmission.Identity.waiter_id(f.run.id, "step", 0, 1),
          run_id: f.run.id,
          step_id: "step",
          owner_id: f.run.storage_owner_id,
          owner_generation: f.run.storage_fencing_token,
          lease_duration_ms: 300_000,
          waiter_ttl_ms: 300_000,
          requests: [%C.CapacityRequest{scope_id: scope}],
          occurred_at: DateTime.utc_now()
        }
    }
  end

  defp transition(context, run, event_type, data, now) do
    %C.CommitRunTransition{
      workspace_context: context,
      command_id: "transition:#{run.id}:#{run.event_seq}",
      expected_sequence: run.event_seq - 1,
      owner_id: run.storage_owner_id,
      fencing_token: run.storage_fencing_token,
      run: run,
      event: %{
        run_id: run.id,
        sequence: run.event_seq,
        event_type: event_type,
        status: :running,
        occurred_at: now,
        data: data
      }
    }
  end

  defp task_count(f),
    do: Repo.aggregate(from(t in RunnerTask, where: t.run_id == ^f.run.id), :count)
end
