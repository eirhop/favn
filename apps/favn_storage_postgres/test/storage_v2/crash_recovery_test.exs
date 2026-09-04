Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnStoragePostgres.StorageV2.CrashRecoveryTest do
  use ExUnit.Case, async: false
  alias Ecto.Adapters.SQL
  alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnStoragePostgres.Registry.Store, as: Registry
  alias FavnStoragePostgres.RunnerTasks.Store
  alias FavnStoragePostgres.Repo
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  setup_all do
    {:ok, options} =
      FavnStoragePostgres.Config.repo_options(
        url: System.fetch_env!("FAVN_DATABASE_URL"),
        ssl_mode: :disable,
        pool_size: 8
      )

    start_supervised!({Repo, options})
    :ok = FavnStoragePostgres.StorageV2.Migrations.migrate!(Repo)
    :ok
  end

  setup do
    id = "crash-" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    now = DateTime.utc_now()
    pool = "pool_" <> String.replace(id, "-", "_")
    version = Fixture.version("Elixir.CrashRecoveryFixture", "asset", pool)
    {:ok, platform} = PlatformContext.new("test-admin", id, [:platform_admin])

    :ok =
      Registry.provision_workspace(%C.ProvisionWorkspace{
        platform_context: platform,
        workspace_id: id,
        slug: id,
        display_name: id,
        occurred_at: now
      })

    {:ok, context} = WorkspaceContext.new(id, "test-admin", [:workspace_admin])

    {:ok, _} =
      Registry.register_manifest(%C.RegisterManifest{
        platform_context: platform,
        version: version
      })

    target = Favn.TargetIdentity.for_asset(hd(version.manifest.assets).ref)

    {:ok, _} =
      Registry.deploy_manifest(%C.DeployManifest{
        platform_context: platform,
        workspace_context: context,
        deployment_id: id,
        manifest_version_id: version.manifest_version_id,
        configuration: %{"resources" => %{}},
        targets: [
          %C.DeploymentTarget{
            target_kind: :asset,
            target_id: target,
            selection_source: :common,
            customer_visible: true,
            descriptor: %{"target_id" => target, "label" => target}
          }
        ],
        occurred_at: now
      })

    {:ok, id: id, now: now, pool: pool, version: version, platform: platform, context: context}
  end

  test "parentless tasks have verified pins and lifecycle receipts never need executable data",
       f do
    command = enqueue(f, "inspection")
    assert {:ok, %{data_state: :not_loaded, payload: nil} = queued} = Store.enqueue(command)

    assert {:ok, %{data_state: :available, payload: %Favn.Contracts.RelationInspectionRequest{}}} =
             get(f, queued.task_id)

    assert {:ok, claimed} = Store.claim(claim(f, "runner"))
    assert claimed.task_id == queued.task_id
    assert claimed.assignment_generation == 1
    assert {:ok, claimed_again} = Store.claim(claim(f, "runner"))
    assert claimed_again == claimed

    corrupt(f, queued.task_id, "orchestration_context")

    cancel = %C.RequestRunnerTaskCancellation{
      workspace_context: f.context,
      command_id: "cancel",
      task_id: queued.task_id,
      reason: :operator_request,
      issued_at: f.now,
      occurred_at: f.now
    }

    assert {:ok, %{status: :cancelling, data_state: :not_loaded}} =
             Store.request_cancellation(cancel)

    assert {:ok, %{status: :cancelling}} = Store.request_cancellation(cancel)

    assert {:ok, %{status: :cancelling, data_state: :unavailable, persistence_failure: :context}} =
             get(f, queued.task_id)
  end

  test "malformed oldest candidates do not block a healthy successor or corrupt demand", f do
    for number <- 1..51 do
      assert {:ok, task} = Store.enqueue(enqueue(f, "bad-#{number}"))
      corrupt(f, task.task_id, "payload")
    end

    assert {:ok, good} = Store.enqueue(enqueue(f, "good", DateTime.add(f.now, 1, :millisecond)))
    assert {:ok, nil} = Store.claim(claim(f, "first"))
    assert {:ok, assigned} = Store.claim(claim(f, "second"))
    assert assigned.task_id == good.task_id

    assert %{rows: [[51]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.runner_tasks WHERE workspace_id=$1 AND status='failed' AND persistence_failure='payload'",
               [f.id]
             )

    assert {:ok, demand} =
             Store.demand(%Q.GetRunnerCapacityDemand{
               platform_context: f.platform,
               runner_pool: f.pool,
               required_runner_release_id: FavnTestSupport.runner_release_id()
             })

    assert {demand.queued_count, demand.active_count, demand.outstanding_count} == {0, 1, 1}
  end

  test "an unrelated workspace cannot enqueue a globally retained manifest", f do
    command = enqueue(f, "wrong-pin")
    other = Fixture.version("Elixir.OtherCrashFixture")

    assert {:ok, _} =
             Registry.register_manifest(%C.RegisterManifest{
               platform_context: f.platform,
               version: other
             })

    assert {:error, _} =
             Store.enqueue(%{
               command
               | manifest_version_id: other.manifest_version_id,
                 manifest_content_hash: other.content_hash
             })
  end

  test "only an authorized read-only inspection can precede workspace deployment", f do
    version = Fixture.version("Elixir.PreActivationInspection", "asset", f.pool)

    assert {:ok, _} =
             Registry.register_manifest(%C.RegisterManifest{
               platform_context: f.platform,
               version: version
             })

    other = %{f | version: version}
    command = %{enqueue(other, "preactivation") | platform_context: f.platform}
    assert {:error, _} = Store.enqueue(%{command | platform_context: nil})

    assert {:error, _} =
             Store.enqueue(%{
               command
               | platform_context: %{f.platform | roles: [:platform_reader]}
             })

    assert {:error, _} =
             Store.enqueue(%{
               command
               | workspace_context: %{f.context | roles: [:customer_reader]}
             })

    for field <- [:write_target_id, :write_claim_key, :write_operation_id] do
      assert {:error, _} = Store.enqueue(Map.put(command, field, "forged"))
    end

    for field <- [:write_claim_fence, :write_lock_fence] do
      assert {:error, _} = Store.enqueue(Map.put(command, field, 1))
    end

    assert {:error, _} =
             Store.enqueue(%{command | manifest_content_hash: String.duplicate("0", 64)})

    assert {:error, _} =
             Store.enqueue(%{
               command
               | required_runner_release_id: "rr_" <> String.duplicate("0", 64)
             })

    assert {:ok, task} = Store.enqueue(command)
    assert {:ok, %{data_state: :available}} = get(f, task.task_id)
    assert {:ok, %{task_id: task_id}} = Store.claim(claim(f, "preactivation-reader"))
    assert task_id == task.task_id

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.workspace_deployments WHERE workspace_id=$1 AND manifest_version_id=$2",
               [f.id, version.manifest_version_id]
             )

    file = Path.join(System.tmp_dir!(), "favn-preactivation-" <> f.id <> ".json")
    File.write!(file, Jason.encode!(%{workspace: f.id, task_id: task.task_id}))
    on_exit(fn -> File.rm(file) end)
    args = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)
    script = Path.expand("../support/crash_recovery_process.exs", __DIR__)

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        args ++ [script, "inspect", "preactivation", file],
        stderr_to_stdout: true,
        env: [{"ERL_FLAGS", "+S 2:2"}]
      )

    assert status == 0, output
    assert output =~ "INSPECTION restored"
  end

  test "valid payloads cannot be swapped across target ownership under one manifest", f do
    first_asset = hd(f.version.manifest.assets)
    second_ref = {first_asset.module, :second_target}
    second_asset = %{first_asset | ref: second_ref, name: :second_target}

    manifest =
      %{f.version.manifest | assets: [first_asset, second_asset]}
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Favn.Manifest.Version.new(manifest)

    FavnStoragePostgres.TestSupport.TaskManifest.retain(
      %{
        workspace_id: f.id,
        workspace_context: f.context,
        platform_context: f.platform,
        now: f.now
      },
      version
    )

    f = %{f | version: version}
    first = marker_command(f, "target-one")
    assert {:ok, queued} = Store.enqueue(first)
    {:ok, payload} = Codec.decode_payload(:generation_marker_initialize, first.payload, version)

    {:ok, encoded, hash} =
      Codec.encode_payload(
        :generation_marker_initialize,
        %{
          payload
          | target_id: Favn.TargetIdentity.for_asset(second_ref),
            initialization_operation_id: "other-operation"
        }
      )

    second = %{
      marker_command(f, "target-two")
      | payload: encoded,
        payload_hash: hash,
        write_target_id: Favn.TargetIdentity.for_asset(second_ref),
        write_operation_id: "other-operation"
    }

    assert {:ok, healthy} = Store.enqueue(second)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET payload=$1,payload_hash=$2 WHERE workspace_id=$3 AND task_id=$4",
      [encoded, hash, f.id, queued.task_id]
    )

    assert {:ok, %{data_state: :unavailable, persistence_failure: :payload}} =
             get(f, queued.task_id)

    assert {:ok, assigned} = Store.claim(claim(f, "swap-reader"))
    assert assigned.task_id == healthy.task_id
    assert {:ok, %{status: :failed}} = get(f, queued.task_id)
  end

  test "scalar receipt snapshots reject extra fields and immutable overrides", f do
    command = enqueue(f, "receipt-shape")
    assert {:ok, _} = Store.enqueue(command)

    %{rows: [[binary]]} =
      SQL.query!(
        Repo,
        "SELECT snapshot FROM favn_control.runner_task_command_tasks WHERE scope_id=$1 AND command_id=$2",
        ["workspace:" <> f.id, command.command_id]
      )

    envelope = Jason.decode!(binary)
    ["map", fields] = envelope["data"]

    mutable = fn change ->
      %{
        envelope
        | "data" => [
            "map",
            Enum.map(fields, fn
              [["atom", "mutable"], ["map", values]] ->
                [["atom", "mutable"], ["map", change.(values)]]

              field ->
                field
            end)
          ]
      }
    end

    for forged <- [
          mutable.(fn values ->
            values ++ [[["atom", "task_id"], ["binary", Base.encode64("rt_forged")]]]
          end),
          mutable.(fn [_ | rest] -> rest end),
          mutable.(fn values ->
            Enum.map(values, fn
              [["atom", "assignment_generation"], _] -> [["atom", "assignment_generation"], -1]
              field -> field
            end)
          end),
          %{envelope | "data" => ["map", fields ++ [[["atom", "task_id"], nil]]]}
        ] do
      SQL.query!(
        Repo,
        "UPDATE favn_control.runner_task_command_tasks SET snapshot=$1 WHERE scope_id=$2 AND command_id=$3",
        [Jason.encode!(forged), "workspace:" <> f.id, command.command_id]
      )

      assert {:error, _} = Store.enqueue(command)
    end

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_task_command_tasks SET snapshot=$1 WHERE scope_id=$2 AND command_id=$3",
      [binary, "workspace:" <> f.id, command.command_id]
    )

    assert {:ok, _} = Store.enqueue(command)
  end

  test "a fresh process restores an existing building generation", f do
    fixture = %{
      workspace_id: f.id,
      workspace_context: f.context,
      platform_context: f.platform,
      runner_pool: f.pool,
      now: f.now
    }

    {version, work} = FavnStoragePostgres.TestSupport.TaskManifest.sql_work(fixture)
    file = Path.join(System.tmp_dir!(), "favn-building-" <> f.id <> ".json")

    File.write!(
      file,
      Jason.encode!(%{
        workspace: f.id,
        manifest_id: version.manifest_version_id,
        target_id: work.logical_target_id,
        generation_id: work.target_generation_id
      })
    )

    on_exit(fn -> File.rm(file) end)
    args = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)
    script = Path.expand("../support/crash_recovery_process.exs", __DIR__)

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        args ++ [script, "generation", "building", file],
        stderr_to_stdout: true,
        env: [{"ERL_FLAGS", "+S 2:2"}]
      )

    assert status == 0, output
    assert output =~ "GENERATION restored"
  end

  test "an interrupted mutation remains excluded after task and owner leases expire", f do
    command = marker_command(f, "mutation")
    assert {:ok, _queued} = Store.enqueue(command)
    assert {:ok, assigned} = Store.claim(claim(f, "writer"))
    assert {:ok, running} = Store.transition(transition(f, assigned))
    assert effect(f) == {"in_flight", assigned.task_id, 1}

    SQL.query!(
      Repo,
      "UPDATE favn_control.target_operation_locks SET lease_expires_at='1990-01-01' WHERE workspace_id=$1",
      [f.id]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET assignment_expires_at='1990-01-01' WHERE workspace_id=$1 AND task_id=$2",
      [f.id, running.task_id]
    )

    assert {:ok, tasks} =
             Store.recover_expired(%C.RecoverRunnerTasks{
               platform_context: f.platform,
               command_id: f.id <> "recover-write",
               owner_id: "recovery",
               limit: 50,
               issued_at: f.now,
               occurred_at: DateTime.add(f.now, 60, :second)
             })

    recovered = Enum.find(tasks, &(&1.task_id == running.task_id))

    assert {:ok, %{status: :unknown}} =
             Store.release(%C.ReleaseRunnerTask{
               workspace_context: f.context,
               command_id: "release-unknown",
               task_id: recovered.task_id,
               runner_instance_id: recovered.assigned_runner_instance_id,
               runner_session_generation: recovered.assigned_runner_session_generation,
               assignment_generation: recovered.assignment_generation,
               disposition: :unknown,
               reason: Favn.Contracts.RunnerError.new(outcome: :unknown, type: "process_crashed"),
               issued_at: f.now,
               occurred_at: DateTime.add(f.now, 61, :second)
             })

    assert effect(f) == {"outcome_unknown", assigned.task_id, 1}
    assert {:error, _} = Store.enqueue(marker_command(f, "competitor"))

    assert {:error, _} =
             FavnStoragePostgres.TargetOperationLocks.Store.acquire_many(
               %C.AcquireTargetOperationLocks{
                 workspace_context: f.context,
                 command_id: "takeover",
                 target_ids: [command.write_target_id],
                 operation_id: "new-operation",
                 operation_type: :target_recovery,
                 lease_owner: "new-owner",
                 lease_duration_ms: 30_000,
                 occurred_at: DateTime.add(f.now, 62, :second)
               }
             )

    assert {:ok, _} = Store.enqueue(enqueue(f, "read-only"))
    assert {:ok, %{task_kind: :relation_inspection}} = Store.claim(claim(f, "reader"))
    assert effect(f) == {"outcome_unknown", assigned.task_id, 1}
  end

  test "only a matching committed mutation result releases the task-owned marker lock", f do
    assert {:ok, _queued} = Store.enqueue(marker_command(f, "complete"))
    assert {:ok, assigned} = Store.claim(claim(f, "writer"))
    assert {:ok, _running} = Store.transition(transition(f, assigned))

    {kind, _payload, result} =
      Enum.find(Fixture.tasks(f.version), &(elem(&1, 0) == :generation_marker_initialize))

    {:ok, wrong} =
      Codec.encode_result(kind, :succeeded, %{result | initialization_token: "wrong"})

    complete = %C.CompleteRunnerTask{
      workspace_context: f.context,
      command_id: "complete",
      task_id: assigned.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: 1,
      result_version: 1,
      outcome: :succeeded,
      retry_class: :terminal,
      result: wrong,
      issued_at: f.now,
      occurred_at: f.now
    }

    assert {:error, _} = Store.complete(complete)
    assert effect(f) == {"in_flight", assigned.task_id, 1}
    {:ok, correct} = Codec.encode_result(kind, :succeeded, result)

    assert {:ok, %{status: :succeeded, result: nil}} =
             Store.complete(%{complete | result: correct})

    assert {:ok, %{status: :succeeded, result: ^result, data_state: :available}} =
             get(f, assigned.task_id)

    assert effect(f) == nil
    assert {:ok, %{status: :succeeded}} = Store.complete(%{complete | result: correct})

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET result=$1 WHERE workspace_id=$2 AND task_id=$3",
      [wrong, f.id, assigned.task_id]
    )

    assert {:ok, %{status: :succeeded, data_state: :unavailable, persistence_failure: :result}} =
             get(f, assigned.task_id)
  end

  test "original claim receipt retains its fence after scalar expiry recovery", f do
    command = enqueue(f, "expiry")
    assert {:ok, queued} = Store.enqueue(command)
    claim = claim(f, "runner")
    assert {:ok, original} = Store.claim(claim)
    corrupt(f, queued.task_id, "payload")

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET assignment_expires_at='1990-01-01' WHERE workspace_id=$1 AND task_id=$2",
      [f.id, queued.task_id]
    )

    recover = %C.RecoverRunnerTasks{
      platform_context: f.platform,
      command_id: f.id <> "recover",
      owner_id: "recovery",
      limit: 50,
      issued_at: f.now,
      occurred_at: DateTime.add(f.now, 60, :second)
    }

    assert {:ok, tasks} = Store.recover_expired(recover)
    recovered = Enum.find(tasks, &(&1.task_id == queued.task_id))
    assert recovered.assignment_generation == original.assignment_generation + 1
    assert recovered.payload == nil
    assert {:ok, ^tasks} = Store.recover_expired(recover)

    assert {:error, %{kind: :fenced, details: %{reason_code: "runner_task_claim_superseded"}}} =
             Store.claim(claim)

    assert {:ok, healthy} = Store.enqueue(enqueue(f, "healthy"))

    assert {:ok, %{task_id: healthy_id}} =
             Store.claim(%{claim | command_id: f.id <> "fresh-command"})

    assert healthy_id == healthy.task_id

    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET payload=$1 WHERE workspace_id=$2 AND task_id=$3",
      [command.payload, f.id, queued.task_id]
    )

    assert {:ok, %{assignment_generation: 1, status: :assigned}} = Store.claim(claim)
    assert {:ok, %{assignment_generation: 2}} = get(f, queued.task_id)
  end

  test "fenced takeover clears an unstarted binding and old cancellation cannot clear its replacement",
       f do
    assert {:ok, _} = Store.enqueue(marker_command(f, "old"))
    assert {:ok, old} = Store.claim(claim(f, "old-writer"))

    SQL.query!(
      Repo,
      "UPDATE favn_control.target_operation_locks SET lease_expires_at='2000-01-01' WHERE workspace_id=$1",
      [f.id]
    )

    assert {:ok, [replacement]} =
             FavnStoragePostgres.TargetOperationLocks.Store.acquire_many(
               %C.AcquireTargetOperationLocks{
                 workspace_context: f.context,
                 command_id: "takeover",
                 target_ids: [old.write_target_id],
                 operation_id: "replacement",
                 operation_type: :target_recovery,
                 lease_owner: "replacement",
                 lease_duration_ms: 30_000,
                 occurred_at: f.now
               }
             )

    assert replacement.fencing_token == old.write_lock_fence + 1
    command = marker_command(f, "replacement")
    {:ok, request} = Codec.decode_payload(command.task_kind, command.payload, f.version)

    {:ok, encoded, hash} =
      Codec.encode_payload(command.task_kind, %{
        request
        | initialization_operation_id: "replacement"
      })

    assert {:error, _} = Store.enqueue(%{command | write_operation_id: "replacement"})

    assert {:ok, _} =
             Store.enqueue(%{
               command
               | payload: encoded,
                 payload_hash: hash,
                 write_operation_id: "replacement"
             })

    assert {:ok, new} = Store.claim(claim(f, "new-writer"))
    assert {:error, %{kind: :fenced}} = Store.transition(transition(f, old))
    assert {:ok, _} = Store.transition(%{transition(f, new) | command_id: "new-start"})

    assert {:ok, _} =
             Store.request_cancellation(%C.RequestRunnerTaskCancellation{
               workspace_context: f.context,
               command_id: "old-cancel",
               task_id: old.task_id,
               reason: :operator_request,
               issued_at: f.now,
               occurred_at: f.now
             })

    assert {:ok, %{status: :cancelled}} = Store.complete(completion(f, old, :cancelled))
    assert effect(f) == {"in_flight", new.task_id, 1}
  end

  test "preparation failure and safe recovery terminalize a missing unstarted owner", f do
    for action <- [:complete, :recover] do
      assert {:ok, _} = Store.enqueue(marker_command(f, Atom.to_string(action)))
      assert {:ok, task} = Store.claim(claim(f, Atom.to_string(action)))

      SQL.query!(Repo, "DELETE FROM favn_control.target_operation_locks WHERE workspace_id=$1", [
        f.id
      ])

      result =
        case action do
          :complete ->
            Store.complete(completion(f, task, :failed))

          :recover ->
            Store.release(%C.ReleaseRunnerTask{
              workspace_context: f.context,
              command_id: "lost-owner-release",
              task_id: task.task_id,
              runner_instance_id: task.assigned_runner_instance_id,
              runner_session_generation: task.assigned_runner_session_generation,
              assignment_generation: task.assignment_generation,
              disposition: :requeue,
              reason: nil,
              issued_at: f.now,
              occurred_at: f.now
            })
        end

      assert {:ok, %{status: :failed}} = result
      assert effect(f) == nil
    end
  end

  test "cancellation before Started releases its owner while cancellation after Started holds it",
       f do
    for started? <- [false, true] do
      suffix = if started?, do: "started", else: "unstarted"
      assert {:ok, _} = Store.enqueue(marker_command(f, suffix))
      assert {:ok, task} = Store.claim(claim(f, suffix))
      if started?, do: assert({:ok, _} = Store.transition(transition(f, task)))

      assert {:ok, _} =
               Store.request_cancellation(%C.RequestRunnerTaskCancellation{
                 workspace_context: f.context,
                 command_id: "cancel-" <> suffix,
                 task_id: task.task_id,
                 reason: :operator_request,
                 issued_at: f.now,
                 occurred_at: f.now
               })

      assert {:ok, %{status: :cancelled}} = Store.complete(completion(f, task, :cancelled))
      assert effect(f) == if(started?, do: {"outcome_unknown", task.task_id, 1}, else: nil)
    end
  end

  test "success before Started cannot clear a declared writer", f do
    assert {:ok, _} = Store.enqueue(marker_command(f, "premature"))
    assert {:ok, task} = Store.claim(claim(f, "premature"))

    {kind, _, result} =
      Enum.find(Fixture.tasks(f.version), &(elem(&1, 0) == :generation_marker_initialize))

    {:ok, encoded} = Codec.encode_result(kind, :succeeded, result)

    assert {:error, %{kind: :fenced}} =
             Store.complete(%{
               completion(f, task, :failed)
               | outcome: :succeeded,
                 retry_class: :terminal,
                 result: encoded,
                 error: nil
             })

    assert effect(f) == {"not_started", task.task_id, nil}
  end

  defp completion(f, task, outcome),
    do: %C.CompleteRunnerTask{
      workspace_context: f.context,
      command_id: "complete-" <> task.task_id,
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: task.assigned_runner_session_generation,
      assignment_generation: task.assignment_generation,
      result_version: 1,
      outcome: outcome,
      result: nil,
      retry_class: if(outcome == :failed, do: :safe_to_retry, else: :terminal),
      error:
        if(outcome == :failed,
          do: Favn.Contracts.RunnerError.new(outcome: :safe_failure, retryable?: true),
          else: nil
        ),
      issued_at: f.now,
      occurred_at: f.now
    }

  @tag :slow
  @tag timeout: 180_000
  test "SIGKILL at lifecycle barriers survives two fresh process recoveries", f do
    SQL.query!(
      Repo,
      "CREATE TABLE IF NOT EXISTS public.favn_crash_probe (probe_id text PRIMARY KEY, effects integer NOT NULL)",
      []
    )

    script = Path.expand("../support/crash_recovery_process.exs", __DIR__)
    args = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)

    for phase <-
          ~w(queued assigned preparing running cancelling effect_committed result_committed) do
      # A fresh target per phase prevents intentional unresolved holds leaking across probes.
      phase_fixture = %{f | pool: f.pool <> "_" <> phase}
      version = Fixture.version("Elixir.CrashRecoveryFixture", phase, phase_fixture.pool)

      fixture = %{
        workspace_id: f.id,
        workspace_context: f.context,
        platform_context: f.platform,
        now: f.now
      }

      FavnStoragePostgres.TestSupport.TaskManifest.retain(fixture, version)
      phase_fixture = %{phase_fixture | version: version}
      assert {:ok, task} = Store.enqueue(marker_command(phase_fixture, phase))
      SQL.query!(Repo, "INSERT INTO public.favn_crash_probe VALUES ($1, 0)", [task.task_id])
      file = Path.join(System.tmp_dir!(), "favn-crash-" <> task.task_id <> ".json")

      File.write!(
        file,
        Jason.encode!(%{workspace: f.id, pool: phase_fixture.pool, task_id: task.task_id})
      )

      on_exit(fn -> File.rm(file) end)

      port =
        Port.open({:spawn_executable, System.find_executable("elixir")}, [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: args ++ [script, "write", phase, file],
          env: [{~c"ERL_FLAGS", ~c"+S 2:2"}]
        ])

      try do
        assert_barrier(port, phase, "")
        {:os_pid, pid} = Port.info(port, :os_pid)
        assert {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(pid)])
        assert_receive {^port, {:exit_status, status}}, 10_000
        assert status != 0
      after
        case Port.info(port, :os_pid) do
          {:os_pid, pid} ->
            System.cmd("kill", ["-KILL", Integer.to_string(pid)], stderr_to_stdout: true)
            Port.close(port)

          nil ->
            :ok
        end
      end

      for mode <- ["read1", "read2"] do
        SQL.query!(
          Repo,
          "UPDATE favn_control.runner_tasks SET assignment_expires_at='2000-01-01' WHERE workspace_id=$1 AND task_id=$2 AND status IN ('assigned','preparing','running','cancelling')",
          [f.id, task.task_id]
        )

        SQL.query!(
          Repo,
          "UPDATE favn_control.target_operation_locks SET lease_expires_at='2000-01-01' WHERE workspace_id=$1 AND target_id=$2",
          [f.id, task.write_target_id]
        )

        {output, code} =
          System.cmd(System.find_executable("elixir"), args ++ [script, mode, phase, file],
            stderr_to_stdout: true,
            env: [{"ERL_FLAGS", "+S 2:2"}]
          )

        assert code == 0, output
        assert output =~ "RECOVERED ", output
        assert {:ok, current} = get(f, task.task_id)

        expected =
          cond do
            phase == "queued" -> :queued
            phase in ["assigned", "preparing"] -> :failed
            phase == "result_committed" -> :succeeded
            true -> :unknown
          end

        assert current.status == expected

        if expected == :unknown do
          assert {:error, _} =
                   Store.enqueue(marker_command(phase_fixture, phase <> "-competitor"))
        end
      end

      assert %{rows: [[effects]]} =
               SQL.query!(Repo, "SELECT effects FROM public.favn_crash_probe WHERE probe_id=$1", [
                 task.task_id
               ])

      assert effects == if(phase in ["effect_committed", "result_committed"], do: 1, else: 0)
      assert {:ok, _} = Store.enqueue(enqueue(phase_fixture, phase <> "-read"))

      assert {:ok, %{task_kind: :relation_inspection}} =
               Store.claim(%{
                 claim(phase_fixture, phase <> "-reader")
                 | supported_task_kinds: [:relation_inspection]
               })
    end
  end

  defp assert_barrier(port, phase, output) do
    receive do
      {^port, {:data, bytes}} ->
        output = output <> bytes

        if String.contains?(output, "BARRIER " <> phase),
          do: :ok,
          else: assert_barrier(port, phase, output)

      {^port, {:exit_status, code}} ->
        flunk("crash probe exited #{code}: #{output}")
    after
      15_000 ->
        flunk("crash probe did not reach barrier: #{output}")
    end
  end

  defp enqueue(f, suffix, at \\ nil) do
    {kind, payload, _} =
      Enum.find(Fixture.tasks(f.version), &(elem(&1, 0) == :relation_inspection))

    {:ok, encoded, hash} = Codec.encode_payload(kind, payload)
    {:ok, context} = RunnerTaskContext.encode(%{})

    %C.EnqueueRunnerTask{
      workspace_context: f.context,
      command_id: "enqueue-" <> suffix,
      task_id: "rt_" <> f.id <> suffix,
      domain_identity: f.id <> suffix,
      task_kind: kind,
      manifest_version_id: f.version.manifest_version_id,
      manifest_content_hash: f.version.content_hash,
      runner_pool: f.pool,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      retry_class: :safe_to_retry,
      payload: encoded,
      payload_hash: hash,
      orchestration_context: context,
      issued_at: f.now,
      occurred_at: at || f.now
    }
  end

  defp claim(f, suffix),
    do: %C.ClaimRunnerTask{
      platform_context: f.platform,
      command_id: f.id <> suffix,
      runner_instance_id: f.id <> suffix,
      runner_session_generation: 1,
      runner_pool: f.pool,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      supported_task_kinds: Favn.Contracts.RunnerTask.task_kinds(),
      capabilities: [],
      lease_duration_ms: 30_000,
      issued_at: f.now,
      occurred_at: DateTime.add(f.now, 1, :second)
    }

  defp get(f, task_id),
    do: Store.get(%Q.GetRunnerTask{workspace_context: f.context, task_id: task_id})

  defp marker_command(f, suffix) do
    {kind, payload, _} =
      Enum.find(Fixture.tasks(f.version), &(elem(&1, 0) == :generation_marker_initialize))

    {:ok, encoded, hash} = Codec.encode_payload(kind, payload)

    %{
      enqueue(f, suffix)
      | task_kind: kind,
        payload: encoded,
        payload_hash: hash,
        retry_class: Favn.Contracts.RunnerTask.default_retry_class(kind),
        write_target_id: payload.target_id,
        write_operation_id: payload.initialization_operation_id
    }
  end

  defp transition(f, assigned),
    do: %C.TransitionRunnerTask{
      workspace_context: f.context,
      command_id: "start",
      task_id: assigned.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: 1,
      transition: :running,
      issued_at: f.now,
      occurred_at: f.now
    }

  defp effect(f) do
    case SQL.query!(
           Repo,
           "SELECT effect_state,effect_task_id,effect_assignment_generation FROM favn_control.target_operation_locks WHERE workspace_id=$1",
           [f.id]
         ).rows do
      [] -> nil
      [row] -> List.to_tuple(row)
    end
  end

  defp corrupt(f, task_id, column) when column in ["payload", "orchestration_context"] do
    SQL.query!(
      Repo,
      "UPDATE favn_control.runner_tasks SET #{column}='{}'::jsonb WHERE workspace_id=$1 AND task_id=$2",
      [f.id, task_id]
    )
  end
end
