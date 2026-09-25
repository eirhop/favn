defmodule FavnStoragePostgres.StorageV2.RunLeaseReliabilityTest do
  use ExUnit.Case, async: false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.{Projector, RunState}
  alias FavnStoragePostgres.{Config, Repo, RunLeaseRepo, RunTransaction}
  alias FavnStoragePostgres.RunOwnership.Store
  alias FavnStoragePostgres.Runs.Store, as: Runs
  alias FavnStoragePostgres.TestSupport.RunFixture

  setup_all do
    {:ok, opts} =
      Config.repo_options(
        url:
          FavnStoragePostgres.TestSupport.IsolatedDatabase.create!(
            System.fetch_env!("FAVN_DATABASE_URL")
          ),
        ssl_mode: :disable,
        pool_size: 4
      )

    start_supervised!({Repo, opts})
    start_supervised!({RunLeaseRepo, Keyword.put(opts, :pool_size, 2)})
    :ok = FavnStoragePostgres.StorageV2.Migrations.migrate!(Repo)
    :ok
  end

  test "failed cleanup is durable, fenced, read-only and independently discoverable", f do
    {:ok, original} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})

    info = %{
      "version" => 1,
      "state" => "pending",
      "reason_code" => "automatic_recovery_exhausted",
      "started_at" => DateTime.to_iso8601(DateTime.utc_now())
    }

    failed =
      RunState.transition(original,
        status: :error,
        error: %{type: :recovery_exhausted},
        metadata: %{"failure_cleanup" => info, :terminal_event_type => :run_failed}
      )

    assert {:ok, _} = commit_cleanup(f, original, failed, :run_failed)
    {:ok, saved_failure} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})
    assert :ok = release(f)
    make_due(f)
    assert {:ok, []} = Store.recovery_candidates(f.context, 64)
    assert {:ok, [id]} = Store.cleanup_candidates(f.context, 64)
    assert id == f.id

    assert {:error, %{kind: :conflict}} =
             Store.claim_run(%{f.claim | command_id: "wrong-purpose"})

    assert {:ok, cleanup} =
             Store.claim_run(%{f.claim | command_id: "failed-cleanup", purpose: :cleanup})

    f = %{f | ownership: cleanup}

    assert {:ok, _} =
             FavnStoragePostgres.RunnerTasks.Store.enqueue(read_command(f, "rt_cleanup", cleanup))

    complete =
      RunState.transition(failed,
        metadata: Map.put(failed.metadata, "failure_cleanup", %{info | "state" => "complete"})
      )

    assert {:error, %{kind: :conflict}} =
             commit_cleanup(f, failed, complete, :run_cleanup_completed)

    assert {:error, %{kind: :conflict}} =
             FavnStoragePostgres.Admission.Store.release_failed_run(%C.ReleaseFailedRunResources{
               workspace_context: f.context,
               run_id: f.id,
               owner_id: cleanup.owner_id,
               fencing_token: cleanup.fencing_token
             })

    # The exact helper inventory prevents a false success even without snapshot task ids.
    assert failed.runner_task_id == nil

    assert {:error, %{kind: :fenced}} =
             commit_cleanup(f, failed, %{complete | status: :ok}, :run_finished)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET status='cancelled',terminal_at=clock_timestamp() WHERE workspace_id=$1",
      [f.id]
    )

    assert {:ok, _} =
             FavnStoragePostgres.Admission.Store.release_failed_run(%C.ReleaseFailedRunResources{
               workspace_context: f.context,
               run_id: f.id,
               owner_id: cleanup.owner_id,
               fencing_token: cleanup.fencing_token
             })

    assert {:error, %{kind: :fenced}} =
             commit_cleanup(
               %{f | ownership: %{cleanup | fencing_token: cleanup.fencing_token - 1}},
               failed,
               complete,
               :run_cleanup_completed
             )

    assert {:ok, _} = commit_cleanup(f, failed, complete, :run_cleanup_completed)
    assert :ok = release(f)
    make_due(f)
    assert {:ok, []} = Store.cleanup_candidates(f.context, 64)
    assert {:ok, saved} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})
    assert saved.status == :error
    assert saved.error == saved_failure.error
  end

  test "cleanup completion cannot discard nonempty unknown write protection", f do
    {:ok, original} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})

    info = %{
      "version" => 1,
      "state" => "pending",
      "reason_code" => "automatic_recovery_exhausted"
    }

    failed =
      RunState.transition(original,
        status: :error,
        error: %{type: :recovery_exhausted},
        metadata: %{"failure_cleanup" => info, :terminal_event_type => :run_failed}
      )

    assert {:ok, _} = commit_cleanup(f, original, failed, :run_failed)
    # A retained claim is the safety evidence even when the task itself is terminal.
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.materialization_claims
        (workspace_id,claim_key,deployment_id,target_kind,target_id,partition_key,run_id,
         claim_command_id,claim_request_hash,owner_id,fencing_token,status,expires_at,version,
         inserted_at,updated_at,evidence_generation_id,effect_state,effect_task_id,effect_assignment_generation,effect_started_at)
      SELECT workspace_id,'unknown-hold',deployment_id,target_kind,target_id,'latest',run_id,
        'unknown-hold',decode(repeat('00',32),'hex'),'old-writer',1,'claimed',clock_timestamp(),1,
        clock_timestamp(),clock_timestamp(),'retained-evidence','outcome_unknown','unknown-task',1,clock_timestamp()
      FROM favn_control.run_targets WHERE workspace_id=$1 AND run_id=$1
      """,
      [f.id]
    )

    complete =
      RunState.transition(failed,
        metadata: Map.put(failed.metadata, "failure_cleanup", Map.put(info, "state", "complete"))
      )

    assert {:error, %{details: %{reason_code: "cleanup_write_hold_unresolved"}}} =
             commit_cleanup(f, failed, complete, :run_cleanup_completed)

    attention =
      RunState.transition(failed,
        metadata: Map.put(failed.metadata, "failure_cleanup", Map.put(info, "state", "attention"))
      )

    assert {:ok, _} = commit_cleanup(f, failed, attention, :run_cleanup_attention)

    assert %{rows: [["claimed", "outcome_unknown"]]} =
             SQL.query!(
               Repo,
               "SELECT status,effect_state FROM favn_control.materialization_claims WHERE workspace_id=$1",
               [f.id]
             )

    assert {:ok, []} = Store.cleanup_candidates(f.context, 64)
  end

  test "old failed cleanup retains its history until cleanup is complete", f do
    assert :ok = release(f)
    old = DateTime.add(DateTime.utc_now(), -90, :day)
    cutoff = DateTime.add(DateTime.utc_now(), -30, :day)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET terminal_at=$2,status='error',snapshot=jsonb_set(snapshot,'{metadata}', $3::jsonb) WHERE workspace_id=$1",
      [f.id, old, %{"failure_cleanup" => %{"version" => 1, "state" => "pending"}}]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.run_ownerships SET expires_at=$2,updated_at=$2 WHERE workspace_id=$1",
      [f.id, old]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.outbox_events SET publication_id=outbox_event_id,published_at=$2 WHERE workspace_id=$1",
      [f.id, old]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.projection_cursors SET last_publication_id=(SELECT COALESCE(max(publication_id),0) FROM favn_control.outbox_events)",
      []
    )

    SQL.query!(Repo, "DELETE FROM favn_control.target_statuses WHERE workspace_id=$1", [f.id])
    SQL.query!(Repo, "DELETE FROM favn_control.asset_window_states WHERE workspace_id=$1", [f.id])

    %{rows: workspaces} =
      SQL.query!(
        Repo,
        "SELECT workspace_id FROM favn_control.workspaces WHERE workspace_id<>$1",
        [f.id]
      )

    policy = %FavnOrchestrator.Retention.Policy{excluded_workspace_ids: List.flatten(workspaces)}
    assert %{eligible_count: 0} = FavnStoragePostgres.Maintenance.History.preview!(policy, cutoff)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET snapshot=jsonb_set(snapshot,'{metadata,failure_cleanup,state}','\"attention\"') WHERE workspace_id=$1",
      [f.id]
    )

    assert %{eligible_count: 0} = FavnStoragePostgres.Maintenance.History.preview!(policy, cutoff)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET snapshot=jsonb_set(snapshot,'{metadata,failure_cleanup,state}','\"complete\"') WHERE workspace_id=$1",
      [f.id]
    )

    assert %{eligible_count: 1} = FavnStoragePostgres.Maintenance.History.preview!(policy, cutoff)
  end

  defp commit_cleanup(f, before, after_run, event) do
    Runs.commit_transition(%C.CommitRunTransition{
      workspace_context: f.context,
      command_id: "cleanup-#{event}-#{after_run.event_seq}-#{f.ownership.fencing_token}",
      expected_sequence: before.event_seq,
      run: after_run,
      owner_id: f.ownership.owner_id,
      fencing_token: f.ownership.fencing_token,
      event: Projector.run_event(after_run, event, %{})
    })
  end

  test "fresh migration matches the supported schema" do
    assert {:ok, diagnostics} = FavnStoragePostgres.StorageV2.Migrations.diagnostics(Repo)
    assert diagnostics.ready?, inspect(diagnostics)
  end

  setup do
    id = "lease-" <> Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)
    context = RunFixture.create(id, [id])

    command = %C.ClaimRun{
      workspace_context: context,
      command_id: "claim-" <> id,
      run_id: id,
      owner_id: "owner",
      lease_duration_ms: 120_000
    }

    {:ok, ownership} = Store.claim_run(command)
    %{id: id, context: context, claim: command, ownership: ownership}
  end

  test "claim and renewal replays preserve expiry but observe fresh database time", f do
    {:ok, replay} = Store.claim_run(f.claim)
    assert replay.expires_at == f.ownership.expires_at
    assert DateTime.compare(replay.database_observed_at, f.ownership.database_observed_at) == :gt
    command = renewal(f)
    assert {:ok, renewed} = Store.renew_run(command)
    assert {:ok, replayed} = Store.renew_run(command)
    assert replayed.expires_at == renewed.expires_at
    assert DateTime.compare(replayed.database_observed_at, renewed.database_observed_at) == :gt
    assert :ok = release(f)
    assert {:error, %{kind: :fenced}} = Store.renew_run(command)
  end

  test "renewal bypasses broad run advisory and promptly rejects ownership row contention", f do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          FavnStoragePostgres.RunIdentity.lock!(f.id, f.id)
          send(parent, {:advisory_held, self()})

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive {:advisory_held, pid}, 2_000
    assert {:ok, _} = Store.renew_run(renewal(f))
    send(pid, :release)
    Task.await(holder)

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(
            Repo,
            "SELECT 1 FROM favn_control.run_ownerships WHERE workspace_id=$1 FOR UPDATE",
            [f.id]
          )

          send(parent, {:row_held, self()})

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive {:row_held, pid}, 2_000
    assert {:error, %{kind: :conflict, retryable?: true}} = Store.renew_run(renewal(f))
    send(pid, :release)
    Task.await(holder)
    assert {:ok, _} = Store.renew_run(renewal(f))
  end

  test "a waiting transition never holds the parent row ahead of ownership", f do
    {:ok, run} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})
    next = RunState.transition(run, status: :running)

    command = %C.CommitRunTransition{
      workspace_context: f.context,
      command_id: "transition-" <> f.id,
      expected_sequence: run.event_seq,
      run: next,
      owner_id: f.ownership.owner_id,
      fencing_token: f.ownership.fencing_token,
      event: Projector.run_event(next, :run_started, %{})
    }

    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(
            Repo,
            "SELECT 1 FROM favn_control.run_ownerships WHERE workspace_id=$1 AND run_id=$2 FOR UPDATE",
            [f.id, f.id]
          )

          send(parent, {:ownership_held, self()})
          receive do: (:release -> :ok)
        end)
      end)

    assert_receive {:ownership_held, holder_pid}, 2_000

    reader =
      Task.async(fn ->
        Repo.transaction(fn ->
          %{rows: [[pid]]} = SQL.query!(Repo, "SELECT pg_backend_pid()", [])
          send(parent, {:snapshot_reader, pid})
          Runs.commit_transition(command)
        end)
      end)

    assert_receive {:snapshot_reader, backend}, 2_000
    await_lock_wait(backend, 100)

    try do
      # This is the same parent-row lock needed by the ownership FK check.
      assert {:ok, _} =
               Repo.transaction(fn ->
                 SQL.query!(
                   Repo,
                   "SELECT 1 FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2 FOR KEY SHARE NOWAIT",
                   [f.id, f.id]
                 )
               end)
    after
      send(holder_pid, :release)
      Task.await(holder)
      Task.await(reader)
    end
  end

  test "state-only run locks are compatible with parent foreign-key checks", f do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          assert {:ok, _} = Runs.locked_snapshot(f.id, f.id)
          send(parent, {:snapshot_held, self()})
          receive do: (:release -> :ok)
        end)
      end)

    assert_receive {:snapshot_held, pid}, 2_000

    try do
      assert {:ok, _} =
               Repo.transaction(fn ->
                 SQL.query!(
                   Repo,
                   "SELECT 1 FROM favn_control.runs WHERE workspace_id=$1 AND run_id=$2 FOR KEY SHARE NOWAIT",
                   [f.id, f.id]
                 )
               end)
    after
      send(pid, :release)
      Task.await(holder)
    end
  end

  defp await_lock_wait(_pid, 0), do: flunk("snapshot reader did not wait on ownership")

  defp await_lock_wait(pid, attempts) do
    case SQL.query!(Repo, "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1", [pid]).rows do
      [["Lock"]] ->
        :ok

      _ ->
        Process.sleep(10)
        await_lock_wait(pid, attempts - 1)
    end
  end

  test "ordinary pool saturation leaves reserved renewal capacity available", f do
    parent = self()

    holders =
      for _ <- 1..4 do
        Task.async(fn ->
          Repo.checkout(fn ->
            send(parent, {:checked_out, self()})

            receive do
              :release -> :ok
            end
          end)
        end)
      end

    for _ <- holders, do: assert_receive({:checked_out, _}, 2_000)
    assert {:ok, renewed} = Store.renew_run(renewal(f))
    assert DateTime.diff(renewed.expires_at, renewed.database_observed_at, :second) >= 119

    Enum.each(holders, fn task ->
      send(task.pid, :release)
      Task.await(task)
    end)
  end

  @tag :slow
  test "PostgreSQL ends the whole transaction even while short statements keep succeeding", f do
    parent = self()

    task =
      Task.async(fn ->
        try do
          RunTransaction.transaction(
            fn ->
              SQL.query!(
                Repo,
                "UPDATE favn_control.run_ownerships SET owner_id='must-rollback' WHERE workspace_id=$1 AND run_id=$2",
                [f.id, f.id]
              )

              send(parent, :transaction_started)

              for _ <- 1..20 do
                RunTransaction.transaction(fn -> SQL.query!(Repo, "SELECT pg_sleep(1)", []) end)
              end
            end,
            timeout: 25_000
          )
        rescue
          error -> {:error, FavnStoragePostgres.ErrorMapper.map(error)}
        end
      end)

    assert_receive :transaction_started, 2_000
    started = System.monotonic_time(:millisecond)
    assert {:error, %{kind: kind, retryable?: true}} = Task.await(task, 20_000)
    assert kind in [:timeout, :unavailable]
    assert System.monotonic_time(:millisecond) - started < 18_000

    assert %{rows: [["owner"]]} =
             SQL.query!(
               Repo,
               "SELECT owner_id FROM favn_control.run_ownerships WHERE workspace_id=$1 AND run_id=$2",
               [f.id, f.id]
             )

    assert {:ok, _} = Store.renew_run(renewal(f))
  end

  test "recovery count and backoff survive release, renewal and replay", f do
    assert :ok = release(f)
    {:ok, second} = Store.claim_run(%{f.claim | command_id: "second", owner_id: "second"})
    assert second.recovery_attempts == 1
    assert :ok = release(%{f | ownership: second})
    assert {:error, %{kind: :conflict}} = Store.claim_run(%{f.claim | command_id: "too-soon"})
    make_due(f)
    {:ok, third} = Store.claim_run(%{f.claim | command_id: "third"})
    assert third.recovery_attempts == 2
    assert :ok = release(%{f | ownership: third})
    make_due(f)
    {:ok, fourth} = Store.claim_run(%{f.claim | command_id: "fourth"})
    assert fourth.recovery_attempts == 3
    assert fourth.claim_purpose == :execution
    assert :ok = release(%{f | ownership: fourth})
    make_due(f)
    assert {:ok, diagnosis} = Store.claim_run(%{f.claim | command_id: "diagnosis"})
    assert diagnosis.claim_purpose == :diagnosis
    assert diagnosis.recovery_attempts == 3
  end

  test "saved attention excludes every automatic claim; resume fences original execution owner",
       f do
    {:ok, run} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})

    annotated =
      RunState.transition(run,
        metadata: %{"recovery_attention" => %{"revision" => f.ownership.fencing_token}}
      )

    command = %C.CommitRunTransition{
      workspace_context: f.context,
      command_id: "attention-" <> f.id,
      expected_sequence: run.event_seq,
      run: annotated,
      owner_id: f.ownership.owner_id,
      fencing_token: f.ownership.fencing_token,
      event: Projector.run_event(annotated, :run_recovery_required, %{})
    }

    assert {:ok, _} = Runs.commit_transition(command)
    assert :ok = release(f)
    make_due(f)
    assert {:ok, []} = Store.recovery_candidates(f.context, 4)

    assert {:ok, []} =
             Store.claim_recovery_batch(%C.ClaimRecoveryBatch{
               workspace_context: f.context,
               batch_id: "sweep",
               owner_id: "sweep",
               lease_duration_ms: 120_000,
               unowned_grace_period_ms: 0,
               limit: 4
             })

    assert {:error, %{kind: :conflict}} =
             Store.claim_run(%{f.claim | command_id: "cannot-bypass"})

    resume = %C.ResumeRunRecovery{
      workspace_context: f.context,
      run_id: f.id,
      expected_revision: f.ownership.fencing_token,
      command_id: "resume-" <> f.id
    }

    assert {:error, %{kind: :conflict}} = Store.resume_recovery(%{resume | expected_revision: 99})
    assert :ok = Store.resume_recovery(resume)
    assert :ok = Store.resume_recovery(resume)
    assert {:error, %{kind: :fenced}} = Store.renew_run(renewal(f))
    assert {:ok, recovered} = Store.claim_run(%{f.claim | command_id: "after-resume"})
    assert recovered.fencing_token > f.ownership.fencing_token
    assert recovered.recovery_attempts == 1
  end

  test "parent cancellation after preflight prevents a member resume before leaf propagation",
       f do
    workspace = "parent-" <> f.id
    context = RunFixture.create(workspace, ["root", "child"])

    assert {:ok, owner} =
             Store.claim_run(%C.ClaimRun{
               workspace_context: context,
               command_id: "child-claim",
               run_id: "child",
               owner_id: "child-owner",
               lease_duration_ms: 120_000
             })

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET cancellation_owner_run_id='root' WHERE workspace_id=$1 AND run_id='child'",
      [workspace]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.run_ownerships SET recovery_disposition='attention',attention_revision=1 WHERE workspace_id=$1 AND run_id='child'",
      [workspace]
    )

    resume = %C.ResumeRunRecovery{
      workspace_context: context,
      run_id: "child",
      expected_revision: 1,
      command_id: "resume-child"
    }

    assert {:ok, :ready} = Store.check_resume(resume)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET cancellation_requested_at=clock_timestamp(),cancellation_status='cancelling' WHERE workspace_id=$1 AND run_id='root'",
      [workspace]
    )

    assert %{rows: [[nil]]} =
             SQL.query!(
               Repo,
               "SELECT cancellation_requested_at FROM favn_control.runs WHERE workspace_id=$1 AND run_id='child'",
               [workspace]
             )

    assert {:error, %{kind: :conflict}} = Store.check_resume(resume)
    assert {:error, %{kind: :conflict}} = Store.resume_recovery(resume)

    assert %{rows: [["attention", fence]]} =
             SQL.query!(
               Repo,
               "SELECT recovery_disposition,fencing_token FROM favn_control.run_ownerships WHERE workspace_id=$1 AND run_id='child'",
               [workspace]
             )

    assert fence == owner.fencing_token
  end

  test "two reserved connections renew the default 64 active run population within budget", f do
    ids = Enum.map(1..64, &"scale-#{f.id}-#{&1}")
    context = RunFixture.create("scale-" <> f.id, ids)

    owners =
      Enum.map(ids, fn id ->
        {:ok, owner} =
          Store.claim_run(%C.ClaimRun{
            workspace_context: context,
            command_id: "claim:" <> id,
            run_id: id,
            owner_id: "scale-owner",
            lease_duration_ms: 120_000
          })

        owner
      end)

    results =
      Task.async_stream(
        owners,
        fn owner ->
          started = System.monotonic_time(:millisecond)

          result =
            Store.renew_run(%C.RenewRunOwnership{
              workspace_context: context,
              renewal_id: "renew:" <> owner.run_id,
              run_id: owner.run_id,
              owner_id: owner.owner_id,
              fencing_token: owner.fencing_token,
              lease_duration_ms: 120_000
            })

          {result, System.monotonic_time(:millisecond) - started}
        end,
        max_concurrency: 64,
        timeout: 5_000
      )

    Enum.each(results, fn outcome ->
      assert {:ok, {result, elapsed}} = outcome
      assert {:ok, _} = result
      assert elapsed < 2_000
    end)
  end

  test "cleanup reads are protected only by their live persisted cleanup generation", f do
    alias FavnStoragePostgres.RunnerTasks.Store, as: Tasks
    old_command = read_command(f, "rt_old-read", f.ownership)
    assert {:ok, _} = Tasks.enqueue(old_command)
    assert :ok = release(f)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET cancellation_requested_at=clock_timestamp(), cancellation_status='cancelling' WHERE workspace_id=$1 AND run_id=$2",
      [f.id, f.id]
    )

    assert {:ok, cleanup} = Store.claim_run(%{f.claim | command_id: "cleanup", purpose: :cleanup})
    command = read_command(f, "rt_cleanup-read", cleanup)
    assert {:ok, queued} = Tasks.enqueue(command)
    assert queued.status == :queued
    assert {:error, %{kind: :fenced}} = Tasks.enqueue(%{command | run_authority: nil})
    assert {:error, %{kind: :fenced}} = Tasks.enqueue(%{command | run_authority: f.ownership})
    assert {:ok, _} = Tasks.enqueue(command)

    assert {:ok, work} =
             Repo.transaction(fn ->
               FavnStoragePostgres.OperationCancellation.reconcile!(f.id, f.id)
             end)

    assert "rt_old-read" in work.task_ids
    refute "rt_cleanup-read" in work.task_ids
    now = DateTime.utc_now()

    cancel = %C.RequestRunnerTaskCancellation{
      workspace_context: f.context,
      command_id: "sweep-read",
      task_id: "rt_cleanup-read",
      reason: :operation_cancelled,
      issued_at: now,
      occurred_at: now,
      preserve_cleanup?: true
    }

    assert {:ok, %{status: :queued}} = Tasks.request_cancellation(cancel)

    assert {:ok, task} =
             Tasks.claim(%C.ClaimRunnerTask{
               platform_context:
                 FavnOrchestrator.Persistence.SystemContext.platform(:cleanup_test,
                   roles: [:platform_operator]
                 ),
               command_id: "claim-read",
               runner_instance_id: "cleanup-runner",
               runner_session_generation: 1,
               runner_pool: command.runner_pool,
               required_runner_release_id: command.required_runner_release_id,
               supported_task_kinds: [:relation_inspection],
               capabilities: ["relation_inspection"],
               lease_duration_ms: 30_000,
               issued_at: now,
               occurred_at: now
             })

    assert task.task_id == "rt_cleanup-read"

    release = %C.ReleaseRunnerTask{
      workspace_context: f.context,
      command_id: "read-runner-lost",
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: task.assigned_runner_session_generation,
      assignment_generation: task.assignment_generation,
      disposition: :requeue,
      reason: :runner_lost,
      issued_at: now,
      occurred_at: now
    }

    assert {:ok, %{status: :queued}} = Tasks.release(release)

    assert {:ok, task} =
             Tasks.claim(%C.ClaimRunnerTask{
               platform_context:
                 FavnOrchestrator.Persistence.SystemContext.platform(:cleanup_test,
                   roles: [:platform_operator]
                 ),
               command_id: "claim-read-again",
               runner_instance_id: "cleanup-runner-2",
               runner_session_generation: 1,
               runner_pool: command.runner_pool,
               required_runner_release_id: command.required_runner_release_id,
               supported_task_kinds: [:relation_inspection],
               capabilities: ["relation_inspection"],
               lease_duration_ms: 30_000,
               issued_at: now,
               occurred_at: now
             })

    assert task.task_id == "rt_cleanup-read"

    assert {:ok, %{status: :cancelling}} =
             Tasks.request_cancellation(%{
               cancel
               | command_id: "explicit-read-cancel",
                 preserve_cleanup?: false
             })
  end

  test "failed attention promotion prevents a later execution claim", f do
    command = %C.RequireRunDiagnosis{
      workspace_context: f.context,
      run_id: f.id,
      owner_id: f.ownership.owner_id,
      fencing_token: f.ownership.fencing_token,
      reason_code: "attention_snapshot_unavailable"
    }

    assert :ok = Store.require_diagnosis(command)
    assert :ok = release(f)
    make_due(f)
    assert {:ok, owner} = Store.claim_run(%{f.claim | command_id: "diagnostic-takeover"})
    assert owner.claim_purpose == :diagnosis
    assert owner.recovery_attempts == 3
    assert owner.diagnosis_reason == "attention_snapshot_unavailable"
    assert {:error, %{kind: :fenced}} = Store.require_diagnosis(command)
  end

  defp read_command(f, id, authority) do
    {:ok, run} = Runs.get_run(%GetRun{workspace_context: f.context, run_id: f.id})
    release = run.runner_releases["default"]

    payload = %Favn.Contracts.RelationInspectionRequest{
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      required_runner_release_id: release,
      asset_ref: run.asset_ref,
      include: [:relation],
      sample_limit: 0
    }

    {:ok, encoded, hash} =
      Favn.Contracts.RunnerTask.PersistenceCodec.encode_payload(:relation_inspection, payload)

    {:ok, context} = FavnOrchestrator.RunnerTaskContext.encode(%{})
    now = DateTime.utc_now()

    %C.EnqueueRunnerTask{
      workspace_context: f.context,
      command_id: "enqueue:" <> id,
      task_id: id,
      domain_identity: "test:" <> id,
      task_kind: :relation_inspection,
      run_id: f.id,
      run_authority: authority,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      runner_pool: "default",
      required_runner_release_id: release,
      required_capability: "relation_inspection",
      retry_class: :safe_to_retry,
      payload: encoded,
      payload_hash: hash,
      orchestration_context: context,
      issued_at: now,
      occurred_at: now
    }
  end

  test "nested ownership transactions retain the outer total deadline" do
    {:ok, result} =
      RunTransaction.transaction(fn ->
        %{rows: [[outer]]} = SQL.query!(Repo, "SHOW transaction_timeout", [])

        {:ok, inner} =
          RunTransaction.transaction(fn ->
            %{rows: [[inner]]} = SQL.query!(Repo, "SHOW transaction_timeout", [])
            inner
          end)

        {outer, inner}
      end)

    assert result == {"15s", "15s"}
  end

  defp renewal(f),
    do: %C.RenewRunOwnership{
      workspace_context: f.context,
      run_id: f.id,
      owner_id: f.ownership.owner_id,
      fencing_token: f.ownership.fencing_token,
      renewal_id: "renew-" <> f.id,
      lease_duration_ms: 120_000
    }

  defp release(f),
    do:
      Store.release_run(%C.ReleaseRunOwnership{
        workspace_context: f.context,
        run_id: f.id,
        owner_id: f.ownership.owner_id,
        fencing_token: f.ownership.fencing_token
      })

  defp make_due(f),
    do:
      SQL.query!(
        Repo,
        "UPDATE favn_control.run_ownerships SET next_recovery_at=clock_timestamp()-interval '1 second' WHERE workspace_id=$1",
        [f.id]
      )
end
