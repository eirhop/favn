defmodule FavnStoragePostgres.StorageV2.RunnerTasksTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerTask.LeaseRenewal
  alias Favn.Contracts.RunnerTask.Registration
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerQueueSupervisor
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.RunnerTasks.Store
  alias FavnStoragePostgres.Schemas.RunnerCapacityDemand, as: Demand
  alias FavnStoragePostgres.StorageV2.Migrations

  @release "rr_" <> String.duplicate("a", 64)
  @other_release "rr_" <> String.duplicate("b", 64)

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 24)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    :ok
  end

  setup do
    unique = random_id()
    workspace_id = "runner-task-ws-#{unique}"
    runner_pool = "duckdb_#{unique}"
    now = DateTime.utc_now()
    previous_runner_pools = Application.get_env(:favn_orchestrator, :runner_pools)
    Application.put_env(:favn_orchestrator, :runner_pools, %{runner_pool => %{mode: :elastic}})

    on_exit(fn ->
      if is_nil(previous_runner_pools),
        do: Application.delete_env(:favn_orchestrator, :runner_pools),
        else: Application.put_env(:favn_orchestrator, :runner_pools, previous_runner_pools)
    end)

    {:ok, platform_context} =
      PlatformContext.new("runner-task-test", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%C.ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "runner-task-#{unique}",
        display_name: "Runner task #{unique}",
        occurred_at: now
      })

    {:ok, workspace_context} =
      WorkspaceContext.new(workspace_id, "runner-task-worker", [:workspace_admin],
        request_id: "request-#{unique}"
      )

    {:ok,
     workspace_id: workspace_id,
     runner_pool: runner_pool,
     workspace_context: workspace_context,
     platform_context: platform_context,
     now: now}
  end

  test "payload codec is typed, bounded, deterministic, and rejects mismatched kinds" do
    payload = inspection_payload()

    assert {:ok, envelope, hash} = Codec.encode_payload(:relation_inspection, payload)
    assert {:ok, ^payload} = Codec.decode_payload(:relation_inspection, envelope)
    assert {:ok, ^hash} = Codec.payload_hash(envelope)

    assert {:error, :invalid_runner_task_persistence_envelope} =
             Codec.decode_payload(:generation_capabilities, envelope)

    assert {:error, _reason} = Codec.encode_payload(:asset_attempt, payload)

    compressed =
      envelope
      |> Map.put(
        "payload",
        payload
        |> :erlang.term_to_binary(compressed: 9)
        |> Base.encode64()
      )

    assert {:error, :invalid_runner_task_persistence_envelope} =
             Codec.decode_payload(:relation_inspection, compressed)
  end

  test "known release partitions exist at zero demand before their first task", fixture do
    command = %C.EnsureRunnerCapacityDemand{
      platform_context: fixture.platform_context,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      occurred_at: fixture.now
    }

    assert {:ok, first} = Store.ensure_demand(command)
    assert {:ok, second} = Store.ensure_demand(command)
    assert first == second
    assert first.outstanding_count == 0
    assert first.healthy?

    assert {:ok, listed} =
             Store.list_demands(%Q.ListRunnerCapacityDemands{
               platform_context: fixture.platform_context,
               limit: 1_024
             })

    assert Enum.any?(listed, fn demand ->
             demand.runner_pool == fixture.runner_pool and
               demand.required_runner_release_id == @release
           end)

    assert {:ok, drain} =
             Store.release_drain(%Q.GetRunnerReleaseDrain{
               platform_context: fixture.platform_context,
               runner_pool: fixture.runner_pool,
               required_runner_release_id: @release
             })

    assert drain.blocker_count == 0
    assert drain.durable_drained?
  end

  test "capacity health aggregates every partition beyond diagnostic list limits", fixture do
    query = %Q.GetRunnerCapacityHealth{platform_context: fixture.platform_context}
    assert {:ok, before} = Store.capacity_health(query)
    history_pool = "#{fixture.runner_pool}_history"

    on_exit(fn ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.runner_capacity_demands WHERE runner_pool = $1",
        [history_pool]
      )
    end)

    rows =
      Enum.map(1..300, fn index ->
        release_id =
          :sha256
          |> :crypto.hash("#{fixture.runner_pool}:#{index}")
          |> Base.encode16(case: :lower)
          |> then(&"rr_#{&1}")

        %{
          runner_pool: history_pool,
          required_runner_release_id: release_id,
          outstanding_count: 0,
          queued_count: 0,
          active_count: 0,
          oldest_queued_at: nil,
          version: 0,
          healthy: index != 300,
          updated_at: fixture.now
        }
      end)

    assert {300, nil} = Repo.insert_all(Demand, rows)
    assert {:ok, after_insert} = Store.capacity_health(query)
    assert after_insert.partition_count == before.partition_count + 300
    assert after_insert.unhealthy_partition_count == before.unhealthy_partition_count + 1

    assert {:ok, bounded} =
             Store.list_release_drains(%Q.ListRunnerReleaseDrains{
               platform_context: fixture.platform_context,
               limit: 256
             })

    assert length(bounded) == 256
  end

  test "enqueue replays across wall-clock time, validates payload hashes, and updates exact demand",
       fixture do
    command = enqueue_command(fixture, "enqueue")
    later_replay = %{command | occurred_at: DateTime.add(command.occurred_at, 5, :second)}

    assert {:ok, first} = Store.enqueue(command)
    assert {:ok, second} = Store.enqueue(later_replay)
    assert first == second
    assert second.enqueued_at == command.occurred_at
    assert first.status == :queued
    assert first.task_kind == :relation_inspection
    assert first.retry_class == :safe_to_retry

    assert {:ok, demand} = demand(fixture)
    assert demand.outstanding_count == 1
    assert demand.queued_count == 1
    assert demand.active_count == 0
    assert demand.oldest_queued_at == command.occurred_at
    assert demand.healthy?

    assert {:ok, %{outstanding_task_count: 1, blocker_count: 1, durable_drained?: false}} =
             Store.release_drain(%Q.GetRunnerReleaseDrain{
               platform_context: fixture.platform_context,
               runner_pool: fixture.runner_pool,
               required_runner_release_id: @release
             })

    assert {:error, %{kind: :conflict}} =
             Store.enqueue(%{command | payload_hash: :crypto.strong_rand_bytes(32)})

    different_command = %{
      command
      | command_id: "enqueue-invalid-hash",
        payload_hash: :crypto.strong_rand_bytes(32)
    }

    assert {:error, %{kind: :invalid}} = Store.enqueue(different_command)

    assert {:error, %{kind: :invalid}} =
             Store.enqueue(%{
               command
               | command_id: "enqueue-changed-retry-class",
                 retry_class: :unknown_do_not_retry
             })
  end

  test "concurrent FIFO claims never double-claim and exact empty polls are replayable",
       fixture do
    tasks =
      for index <- 1..8 do
        command =
          enqueue_command(fixture, "fifo-#{index}",
            occurred_at: DateTime.add(fixture.now, index, :microsecond)
          )

        assert {:ok, task} = Store.enqueue(command)
        task
      end

    claims =
      1..12
      |> Task.async_stream(
        fn index ->
          Store.claim(claim_command(fixture, "concurrent-#{index}", "runner-#{index}"))
        end,
        max_concurrency: 12,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {:ok, result}} -> result end)

    claimed = Enum.reject(claims, &is_nil/1)
    assert length(claimed) == 8
    assert length(Enum.uniq_by(claimed, & &1.task_id)) == 8
    assert Enum.sort(Enum.map(claimed, & &1.task_id)) == Enum.sort(Enum.map(tasks, & &1.task_id))
    assert Enum.all?(claimed, &(&1.status == :assigned and &1.assignment_generation == 1))

    assert {:ok, %{queued_count: 0, active_count: 8, outstanding_count: 8}} =
             demand(fixture)

    empty_command = claim_command(fixture, "stable-empty", "empty-runner")
    assert {:ok, nil} = Store.claim(empty_command)

    assert {:ok, newly_queued} = Store.enqueue(enqueue_command(fixture, "after-empty"))
    assert {:ok, nil} = Store.claim(empty_command)
    assert {:ok, claimed_new} = Store.claim(claim_command(fixture, "new-poll", "new-runner"))
    assert claimed_new.task_id == newly_queued.task_id
  end

  test "claims and demand are global across workspace-scoped task ownership", fixture do
    second_workspace_id = "runner-task-ws-#{random_id()}"

    :ok =
      RegistryStore.provision_workspace(%C.ProvisionWorkspace{
        platform_context: fixture.platform_context,
        workspace_id: second_workspace_id,
        slug: "runner-task-#{random_id()}",
        display_name: "Second runner task workspace",
        occurred_at: fixture.now
      })

    {:ok, second_context} =
      WorkspaceContext.new(second_workspace_id, "runner-task-worker", [:workspace_admin],
        request_id: "request-#{random_id()}"
      )

    first_command = enqueue_command(fixture, "global-first")

    second_command =
      fixture
      |> enqueue_command("global-second",
        occurred_at: DateTime.add(fixture.now, 1, :microsecond)
      )
      |> Map.put(:workspace_context, second_context)

    assert {:ok, first_task} = Store.enqueue(first_command)
    assert {:ok, second_task} = Store.enqueue(second_command)

    assert {:ok, %{outstanding_count: 2, queued_count: 2, active_count: 0}} =
             demand(fixture)

    assert {:ok, first_claim} =
             Store.claim(claim_command(fixture, "global-first", "global-runner-one"))

    assert {:ok, second_claim} =
             Store.claim(claim_command(fixture, "global-second", "global-runner-two"))

    assert {first_claim.workspace_id, first_claim.task_id} ==
             {fixture.workspace_id, first_task.task_id}

    assert {second_claim.workspace_id, second_claim.task_id} ==
             {second_workspace_id, second_task.task_id}

    assert {:ok, %{outstanding_count: 2, queued_count: 0, active_count: 2}} =
             demand(fixture)
  end

  test "pool and release matching is exact", fixture do
    assert {:ok, task} = Store.enqueue(enqueue_command(fixture, "exact-binding"))

    assert {:ok, nil} =
             Store.claim(
               claim_command(fixture, "wrong-pool", "runner-wrong", runner_pool: "pure_elixir")
             )

    assert {:ok, nil} =
             Store.claim(
               claim_command(fixture, "wrong-release", "runner-wrong-release",
                 required_runner_release_id: @other_release
               )
             )

    assert {:ok, nil} =
             Store.claim(
               claim_command(fixture, "wrong-capability", "runner-wrong-capability",
                 capabilities: ["generation_marker_read"]
               )
             )

    assert {:ok, nil} =
             Store.claim(
               claim_command(fixture, "wrong-task-kind", "runner-wrong-task-kind",
                 supported_task_kinds: [:generation_marker_read]
               )
             )

    assert {:error, %{kind: :invalid}} =
             Store.claim(
               claim_command(fixture, "zero-session", "runner-zero-session",
                 runner_session_generation: 0
               )
             )

    assert {:ok, claimed} = Store.claim(claim_command(fixture, "right-binding", "runner-right"))
    assert claimed.task_id == task.task_id
  end

  test "task reads stay workspace scoped and run paging uses a stable cursor", fixture do
    first_command =
      enqueue_command(fixture, "page-first", occurred_at: fixture.now, run_id: "run-page")

    second_command =
      enqueue_command(fixture, "page-second",
        occurred_at: DateTime.add(fixture.now, 1, :microsecond),
        run_id: "run-page"
      )

    assert {:ok, first} = Store.enqueue(first_command)
    assert {:ok, second} = Store.enqueue(second_command)

    assert {:ok, ^first} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: fixture.workspace_context,
               task_id: first.task_id
             })

    query = %Q.PageRunRunnerTasks{
      workspace_context: fixture.workspace_context,
      run_id: "run-page",
      statuses: [:queued],
      limit: 1,
      cursor: nil
    }

    assert {:ok, [page_one]} = Store.page_run(query)
    assert page_one.task_id == first.task_id

    assert {:ok, [page_two]} =
             Store.page_run(%{
               query
               | cursor: {page_one.enqueued_at, page_one.task_id}
             })

    assert page_two.task_id == second.task_id
  end

  test "assignment generations fence stale runners across safe requeue", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "fence"))
    assert {:ok, first} = Store.claim(claim_command(fixture, "claim-first", "runner-first"))

    assert {:ok, requeued} =
             Store.release(
               release_command(fixture, first, "release-first", :requeue, :runner_stopped)
             )

    assert requeued.status == :queued

    assert {:ok, second} =
             Store.claim(claim_command(fixture, "claim-second", "runner-second"))

    assert second.assignment_generation == first.assignment_generation + 1

    assert {:error, %{kind: :conflict}} =
             Store.transition(transition_command(fixture, first, "stale-start", :running))

    assert {:ok, running} =
             Store.transition(transition_command(fixture, second, "current-start", :running))

    assert running.status == :running
  end

  test "log batches deduplicate exactly and reject sequence or assignment reuse", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "logs"))
    assert {:ok, claimed} = Store.claim(claim_command(fixture, "claim-logs", "runner-logs"))
    entries = [%{"level" => "info", "message" => "bounded"}]
    {:ok, payload_hash} = Favn.Contracts.RunnerTask.PersistenceCodec.hash_term(entries)

    command = %C.AppendRunnerTaskLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "log-first",
      task_id: claimed.task_id,
      runner_instance_id: claimed.assigned_runner_instance_id,
      runner_session_generation: claimed.assigned_runner_session_generation,
      assignment_generation: claimed.assignment_generation,
      batch_id: "batch-one",
      sequence: 0,
      entries: entries,
      payload_hash: payload_hash,
      occurred_at: fixture.now
    }

    assert {:ok, :persisted} = Store.append_log_batch(command)
    assert {:ok, :persisted} = Store.append_log_batch(command)

    assert {:ok, :already_persisted} =
             Store.append_log_batch(%{command | command_id: "log-exact-retry"})

    assert {:error, %{kind: :conflict}} =
             Store.append_log_batch(%{
               command
               | command_id: "log-sequence-conflict",
                 batch_id: "batch-two"
             })

    assert {:error, %{kind: :invalid}} =
             Store.append_log_batch(%{
               command
               | command_id: "log-hash-invalid",
                 batch_id: "batch-three",
                 sequence: 1,
                 payload_hash: :crypto.strong_rand_bytes(32)
             })
  end

  test "runtime-input acknowledgements persist only bounded metadata and cannot be replaced",
       fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "runtime-inputs"))

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-runtime-inputs", "runner-runtime-inputs"))

    error =
      Favn.Contracts.RunnerError.new(
        type: :runtime_input_resolution_failed,
        outcome: :safe_failure,
        retryable?: true
      )

    command = %C.PersistRunnerTaskRuntimeInputs{
      workspace_context: fixture.workspace_context,
      command_id: "runtime-inputs-resolved",
      task_id: assigned.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: assigned.assignment_generation,
      resolution_id: "resolution-one",
      status: :failed,
      payload_fingerprint: nil,
      runtime_input_pin: nil,
      error: error,
      occurred_at: DateTime.add(fixture.now, 2, :second)
    }

    assert {:ok, resolved} = Store.persist_runtime_inputs(command)
    assert resolved.runtime_input_resolution_id == "resolution-one"
    assert resolved.runtime_input_resolution_status == :failed
    assert resolved.runtime_input_payload_fingerprint == nil
    assert resolved.runtime_input_error
    assert {:ok, ^resolved} = Store.persist_runtime_inputs(command)

    assert {:ok, exact_retry} =
             Store.persist_runtime_inputs(%{
               command
               | command_id: "runtime-inputs-exact-retry"
             })

    assert exact_retry.runtime_input_resolution_id == resolved.runtime_input_resolution_id

    assert {:error, %{kind: :conflict}} =
             Store.persist_runtime_inputs(%{
               command
               | command_id: "runtime-inputs-conflict",
                 resolution_id: "resolution-two",
                 error: Favn.Contracts.RunnerError.new(type: :different_failure)
             })

    %{rows: [[resolution_id, status, persisted_fingerprint, persisted_error]]} =
      SQL.query!(
        Repo,
        """
        SELECT runtime_input_resolution_id,
               runtime_input_resolution_status,
               runtime_input_payload_fingerprint,
               runtime_input_error
        FROM favn_control.runner_tasks
        WHERE workspace_id = $1 AND task_id = $2
        """,
        [fixture.workspace_id, assigned.task_id]
      )

    assert resolution_id == "resolution-one"
    assert status == "failed"
    assert persisted_fingerprint == nil
    assert persisted_error
  end

  test "terminal results are typed and persisted once with exact demand removal", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "complete"))

    assert {:ok, claimed} =
             Store.claim(claim_command(fixture, "claim-complete", "runner-complete"))

    assert {:ok, running} =
             Store.transition(transition_command(fixture, claimed, "start-complete", :running))

    result = %RelationInspectionResult{
      required_runner_release_id: @release,
      row_count: 1,
      inspected_at: fixture.now
    }

    assert {:ok, encoded_result} =
             Codec.encode_result(:relation_inspection, :succeeded, result)

    command = complete_command(fixture, running, "complete-success", encoded_result)
    assert {:ok, completed} = Store.complete(command)
    assert completed.status == :succeeded
    assert completed.result == result
    assert {:ok, ^completed} = Store.complete(command)

    assert {:ok, %{outstanding_count: 0, queued_count: 0, active_count: 0}} =
             demand(fixture)

    assert {:error, %{kind: :conflict}} =
             Store.complete(%{
               command
               | command_id: "complete-conflict",
                 outcome: :failed,
                 result: nil,
                 error: Favn.Contracts.RunnerError.normalize(:different_result)
             })
  end

  test "cancellation is durable for queued and assigned work", fixture do
    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "cancel-queued"))

    assert {:ok, cancelled} =
             Store.request_cancellation(%C.RequestRunnerTaskCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "cancel-queued",
               task_id: queued.task_id,
               reason: :operator_request,
               occurred_at: fixture.now
             })

    assert cancelled.status == :cancelled
    assert {:ok, %{outstanding_count: 0}} = demand(fixture)

    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "cancel-active"))

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-cancel-active", "runner-cancel"))

    assert {:ok, cancelling} =
             Store.request_cancellation(%C.RequestRunnerTaskCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "request-active-cancel",
               task_id: assigned.task_id,
               reason: :operator_request,
               occurred_at: DateTime.add(fixture.now, 2, :second)
             })

    assert cancelling.status == :cancelling

    assert {:ok, acknowledged} =
             Store.acknowledge_cancellation(%C.AcknowledgeRunnerTaskCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "ack-active-cancel",
               task_id: assigned.task_id,
               runner_instance_id: assigned.assigned_runner_instance_id,
               runner_session_generation: assigned.assigned_runner_session_generation,
               assignment_generation: assigned.assignment_generation,
               occurred_at: DateTime.add(fixture.now, 3, :second)
             })

    assert acknowledged.cancellation_acknowledged_at == DateTime.add(fixture.now, 3, :second)
    assert {:ok, %{outstanding_count: 1, active_count: 1}} = demand(fixture)
  end

  test "pre-start writes requeue but running writes become unknown", fixture do
    assert {:ok, _task} =
             Store.enqueue(
               enqueue_command(fixture, "unsafe",
                 task_kind: :asset_attempt,
                 payload: %Favn.Contracts.RunnerWork{
                   runner_pool: :duckdb,
                   required_runner_release_id: @release
                 }
               )
             )

    claim_opts = [
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    ]

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-unsafe", "runner-unsafe", claim_opts))

    assert {:ok, queued} =
             Store.release(
               release_command(
                 fixture,
                 assigned,
                 "safe-prestart-requeue",
                 :requeue,
                 :lease_expired
               )
             )

    assert queued.status == :queued

    assert {:ok, reassigned} =
             Store.claim(claim_command(fixture, "reclaim-unsafe", "runner-unsafe", claim_opts))

    assert {:ok, running} =
             Store.transition(transition_command(fixture, reassigned, "start-unsafe", :running))

    assert {:error, %{kind: :conflict}} =
             Store.release(
               release_command(fixture, running, "unsafe-requeue", :requeue, :connection_lost)
             )

    reason = "authorization=Bearer super-secret-value"

    assert {:ok, unknown} =
             Store.release(release_command(fixture, running, "unsafe-unknown", :unknown, reason))

    assert unknown.status == :unknown
    encoded_error = Jason.encode!(unknown.error)
    refute encoded_error =~ "super-secret-value"
    assert encoded_error =~ "REDACTED"
    assert {:ok, %{outstanding_count: 0}} = demand(fixture)
  end

  test "whole-run cancellation waits for a dynamically registered runner and retains timeouts",
       fixture do
    start_runner_registry()

    previous_wait =
      Application.get_env(:favn_orchestrator, :runner_task_cancellation_ack_wait_ms)

    Application.put_env(:favn_orchestrator, :runner_task_cancellation_ack_wait_ms, 40)

    on_exit(fn ->
      if is_nil(previous_wait) do
        Application.delete_env(:favn_orchestrator, :runner_task_cancellation_ack_wait_ms)
      else
        Application.put_env(
          :favn_orchestrator,
          :runner_task_cancellation_ack_wait_ms,
          previous_wait
        )
      end
    end)

    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "dynamic-cancel"))
    task_id = queued.task_id
    owner = self()
    agent = spawn_link(fn -> cancellation_agent(owner, :acknowledge) end)
    runner_id = "#{fixture.workspace_id}:dynamic-cancel-runner"

    registration = %Registration{
      runner_instance_id: runner_id,
      boot_id: "boot-dynamic-cancel",
      beam_node: Atom.to_string(node()),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, registration_ack} = RunnerRegistry.register(registration, agent)

    claim = %Favn.Contracts.RunnerTask.ClaimRequest{
      command_id: fixture.workspace_id <> ":claim-dynamic-cancel",
      runner_instance_id: runner_id,
      runner_session_generation: registration_ack.runner_session_generation,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, assignment} = RunnerTasks.claim(claim)
    send(agent, {:assignment, assignment})

    run = cancellation_run(fixture, queued.task_id)

    work_set =
      run
      |> ActiveTaskSet.new()
      |> ActiveTaskSet.add_entry(%{task_id: queued.task_id})

    {cancelled, remaining} = ActiveTaskSet.cancel_all(run, work_set, :operator_request)

    assert_receive {:runner_observed_cancellation, ^task_id}
    assert ActiveTaskSet.task_ids(remaining) == []
    assert [%{status: status}] = cancelled.metadata.cancel_outcomes
    assert status in [:acknowledged, :already_completed]

    assert {:ok, %{status: :cancelled}} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: fixture.workspace_context,
               task_id: queued.task_id
             })

    assert {:ok, queued_timeout} = Store.enqueue(enqueue_command(fixture, "dynamic-timeout"))
    timeout_task_id = queued_timeout.task_id
    timeout_agent = spawn_link(fn -> cancellation_agent(owner, :ignore) end)
    timeout_runner_id = "#{fixture.workspace_id}:dynamic-timeout-runner"

    assert {:ok, timeout_ack} =
             RunnerRegistry.register(
               %{
                 registration
                 | runner_instance_id: timeout_runner_id,
                   boot_id: "boot-dynamic-timeout"
               },
               timeout_agent
             )

    timeout_claim = %{
      claim
      | command_id: fixture.workspace_id <> ":claim-dynamic-timeout",
        runner_instance_id: timeout_runner_id,
        runner_session_generation: timeout_ack.runner_session_generation
    }

    assert {:ok, timeout_assignment} = RunnerTasks.claim(timeout_claim)
    send(timeout_agent, {:assignment, timeout_assignment})

    timeout_run = cancellation_run(fixture, queued_timeout.task_id)

    timeout_work_set =
      timeout_run
      |> ActiveTaskSet.new()
      |> ActiveTaskSet.add_entry(%{task_id: queued_timeout.task_id})

    {timed_out, retained} =
      ActiveTaskSet.cancel_all(timeout_run, timeout_work_set, :operator_request)

    assert_receive {:runner_ignored_cancellation, ^timeout_task_id}
    assert ActiveTaskSet.task_ids(retained) == [queued_timeout.task_id]
    assert [%{status: :requested}] = timed_out.metadata.cancel_outcomes

    complete_cancelled_assignment(timeout_assignment)
  end

  test "periodic lease renewals use distinct replay-stable command identities", fixture do
    start_runner_registry()

    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "renewal-identity"))
    runner_id = "#{fixture.workspace_id}:renewal-runner"
    agent = spawn_link(fn -> Process.sleep(:infinity) end)

    registration = %Registration{
      runner_instance_id: runner_id,
      boot_id: "boot-renewal",
      beam_node: Atom.to_string(node()),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, registration_ack} = RunnerRegistry.register(registration, agent)

    claim = %Favn.Contracts.RunnerTask.ClaimRequest{
      command_id: fixture.workspace_id <> ":claim-renewal",
      runner_instance_id: runner_id,
      runner_session_generation: registration_ack.runner_session_generation,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, assignment} = RunnerTasks.claim(claim)
    first_at = DateTime.utc_now()

    first = %LeaseRenewal{
      workspace_id: fixture.workspace_id,
      task_id: queued.task_id,
      runner_instance_id: runner_id,
      runner_session_generation: registration_ack.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      lease_expires_at: DateTime.add(first_at, 30_000, :millisecond)
    }

    assert {:ok, first_renewed} = RunnerTasks.renew(first)
    assert {:ok, replayed} = RunnerTasks.renew(first)
    assert replayed.assignment_expires_at == first_renewed.assignment_expires_at

    second = %{
      first
      | lease_expires_at: DateTime.add(first.lease_expires_at, 10_000, :millisecond)
    }

    assert {:ok, second_renewed} = RunnerTasks.renew(second)

    assert DateTime.compare(
             second_renewed.assignment_expires_at,
             first_renewed.assignment_expires_at
           ) == :gt
  end

  test "asset terminal retries require durable safe-failure evidence", fixture do
    assert {:ok, _task} =
             Store.enqueue(
               enqueue_command(fixture, "asset-result",
                 task_kind: :asset_attempt,
                 payload: %Favn.Contracts.RunnerWork{
                   runner_pool: :duckdb,
                   required_runner_release_id: @release
                 }
               )
             )

    assert {:ok, assigned} =
             Store.claim(
               claim_command(fixture, "claim-asset-result", "runner-asset-result",
                 supported_task_kinds: [:asset_attempt],
                 capabilities: ["asset_execution"]
               )
             )

    assert {:ok, running} =
             Store.transition(
               transition_command(fixture, assigned, "start-asset-result", :running)
             )

    invalid = %C.CompleteRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "complete-asset-unknown-as-safe",
      task_id: running.task_id,
      runner_instance_id: running.assigned_runner_instance_id,
      runner_session_generation: running.assigned_runner_session_generation,
      assignment_generation: running.assignment_generation,
      result_version: 1,
      outcome: :failed,
      retry_class: :safe_to_retry,
      result: nil,
      error: Favn.Contracts.RunnerError.new(outcome: :unknown, retryable?: true),
      occurred_at: DateTime.add(fixture.now, 3, :second)
    }

    assert {:error, %{kind: :invalid}} = Store.complete(invalid)

    safe = %{
      invalid
      | command_id: "complete-asset-safe",
        error: Favn.Contracts.RunnerError.new(outcome: :safe_failure, retryable?: true)
    }

    assert {:ok, failed} = Store.complete(safe)
    assert failed.status == :failed
    assert failed.retry_class == :safe_to_retry
  end

  test "terminal safe operation failures requeue with a new assignment generation", fixture do
    assert {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "safe-operation-retry"))

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-safe-operation", "runner-safe-operation"))

    assert {:ok, running} =
             Store.transition(
               transition_command(fixture, assigned, "start-safe-operation", :running)
             )

    assert {:ok, failed} =
             Store.complete(%C.CompleteRunnerTask{
               workspace_context: fixture.workspace_context,
               command_id: "complete-safe-operation-failure",
               task_id: running.task_id,
               runner_instance_id: running.assigned_runner_instance_id,
               runner_session_generation: running.assigned_runner_session_generation,
               assignment_generation: running.assignment_generation,
               result_version: 1,
               outcome: :failed,
               retry_class: :safe_to_retry,
               result: nil,
               error:
                 Favn.Contracts.RunnerError.new(
                   outcome: :safe_failure,
                   retryable?: true
                 ),
               occurred_at: DateTime.add(fixture.now, 3, :second)
             })

    retry = %C.RetryRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "retry-safe-operation-1",
      task_id: failed.task_id,
      expected_assignment_generation: failed.assignment_generation,
      expected_result_version: failed.result_version,
      occurred_at: DateTime.add(fixture.now, 4, :second)
    }

    assert {:ok, retried} = Store.retry(retry)
    assert retried.status == :queued
    assert retried.result_version == nil
    assert retried.error == nil
    assert {:ok, ^retried} = Store.retry(retry)

    assert {:ok, %{outstanding_count: 1, queued_count: 1, active_count: 0}} =
             demand(fixture)

    assert {:ok, reassigned} =
             Store.claim(
               claim_command(fixture, "reclaim-safe-operation", "runner-safe-operation-two",
                 occurred_at: DateTime.add(fixture.now, 5, :second)
               )
             )

    assert reassigned.assignment_generation == assigned.assignment_generation + 1
  end

  test "expired assignments are claimed once for fenced recovery", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "recover"))

    assert {:ok, assigned} =
             Store.claim(
               claim_command(fixture, "claim-recover", "runner-recover", lease_duration_ms: 1)
             )

    recovery_time = DateTime.add(fixture.now, 2, :second)

    command = %C.RecoverRunnerTasks{
      platform_context: fixture.platform_context,
      command_id: platform_command_id(fixture, "recover-expired"),
      owner_id: "recovery-owner",
      occurred_at: recovery_time,
      limit: 10,
      lease_duration_ms: 30_000
    }

    assert {:ok, [recovered]} = Store.recover_expired(command)
    assert recovered.task_id == assigned.task_id
    assert recovered.assignment_generation == assigned.assignment_generation + 1
    assert recovered.assigned_runner_instance_id == "recovery-owner"
    assert {:ok, [^recovered]} = Store.recover_expired(command)

    assert {:ok, []} =
             Store.recover_expired(%{
               command
               | command_id: platform_command_id(fixture, "recover-empty"),
                 occurred_at: recovery_time
             })
  end

  test "runner session resume requires an exact durable active assignment", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "resume"))

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-resume", "runner-resume"))

    registration = %Registration{
      runner_instance_id: assigned.assigned_runner_instance_id,
      boot_id: "boot-resume",
      runner_session_generation: assigned.assigned_runner_session_generation,
      beam_node: Atom.to_string(node()),
      runner_pool: assigned.runner_pool,
      required_runner_release_id: assigned.required_runner_release_id,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"],
      active_assignment: %{
        workspace_id: assigned.workspace_id,
        task_id: assigned.task_id,
        assignment_generation: assigned.assignment_generation
      }
    }

    assert :ok = RunnerTasks.verify_registration_resume(registration)

    assert {:error, :stale_runner_task_resume} =
             RunnerTasks.verify_registration_resume(%{
               registration
               | active_assignment:
                   Map.put(
                     registration.active_assignment,
                     :assignment_generation,
                     assigned.assignment_generation + 1
                   )
             })
  end

  test "known-stale demand fails closed, rolls mutations back, and reconciles exactly", fixture do
    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "stale-base"))

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.runner_capacity_demands
      SET healthy = false
      WHERE runner_pool = $1 AND required_runner_release_id = $2
      """,
      [fixture.runner_pool, @release]
    )

    assert {:error, %{kind: :unavailable}} = demand(fixture)

    blocked = enqueue_command(fixture, "blocked-while-stale")
    assert {:error, %{kind: :unavailable}} = Store.enqueue(blocked)

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.runner_tasks WHERE workspace_id = $1 AND task_id = $2",
               [fixture.workspace_id, blocked.task_id]
             )

    assert {:ok, repaired} =
             Store.reconcile_demand(%C.ReconcileRunnerCapacityDemand{
               platform_context: fixture.platform_context,
               command_id: platform_command_id(fixture, "reconcile-demand"),
               runner_pool: fixture.runner_pool,
               required_runner_release_id: @release,
               mode: :repair,
               occurred_at: fixture.now
             })

    assert repaired.healthy?
    assert repaired.outstanding_count == 1
    assert repaired.queued_count == 1
    assert repaired.oldest_queued_at == queued.enqueued_at
    assert {:ok, ^repaired} = demand(fixture)
  end

  test "demand audit detects drift and requires an explicit repair", fixture do
    assert {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "audit-drift"))

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.runner_capacity_demands
      SET queued_count = 0, outstanding_count = 0
      WHERE runner_pool = $1 AND required_runner_release_id = $2
      """,
      [fixture.runner_pool, @release]
    )

    command = %C.ReconcileRunnerCapacityDemand{
      platform_context: fixture.platform_context,
      command_id: platform_command_id(fixture, "audit-demand-drift"),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      mode: :audit,
      occurred_at: fixture.now
    }

    assert {:ok, %{healthy?: false}} = Store.reconcile_demand(command)
    assert {:error, %{kind: :unavailable}} = demand(fixture)

    assert {:ok, repaired} =
             Store.reconcile_demand(%{
               command
               | command_id: platform_command_id(fixture, "repair-demand-drift"),
                 mode: :repair
             })

    assert repaired.healthy?
    assert repaired.outstanding_count == 1
    assert repaired.queued_count == 1
  end

  test "concurrent enqueue and reconciliation preserve exact demand", fixture do
    operations =
      Enum.flat_map(1..20, fn index ->
        enqueue = fn ->
          Store.enqueue(
            enqueue_command(fixture, "reconcile-race-#{index}",
              occurred_at: DateTime.add(fixture.now, index, :microsecond)
            )
          )
        end

        reconcile = fn ->
          Store.reconcile_demand(%C.ReconcileRunnerCapacityDemand{
            platform_context: fixture.platform_context,
            command_id: platform_command_id(fixture, "reconcile-race-#{index}"),
            runner_pool: fixture.runner_pool,
            required_runner_release_id: @release,
            occurred_at: DateTime.add(fixture.now, index, :microsecond)
          })
        end

        [enqueue, reconcile]
      end)

    results =
      operations
      |> Enum.shuffle()
      |> Task.async_stream(& &1.(),
        max_concurrency: 12,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))

    assert %{rows: [[queued_count]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.runner_tasks
               WHERE runner_pool = $1
                 AND required_runner_release_id = $2
                 AND status = 'queued'
               """,
               [fixture.runner_pool, @release]
             )

    assert {:ok, projected} = demand(fixture)
    assert queued_count == 20
    assert projected.queued_count == queued_count
    assert projected.outstanding_count == queued_count
    assert projected.active_count == 0
    assert projected.healthy?
  end

  test "claim query uses the pool release FIFO index", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "explain"))

    plan =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])
        SQL.query!(Repo, "SET LOCAL enable_sort = off", [])

        SQL.query!(
          Repo,
          """
          EXPLAIN (FORMAT TEXT)
          SELECT *
          FROM favn_control.runner_tasks
          WHERE status = 'queued'
            AND runner_pool = $1
            AND required_runner_release_id = $2
            AND task_kind = ANY($3::text[])
            AND (required_capability IS NULL OR required_capability = ANY($4::text[]))
          ORDER BY enqueued_at, workspace_id, task_id
          LIMIT 1
          FOR UPDATE SKIP LOCKED
          """,
          [
            fixture.runner_pool,
            @release,
            ["relation_inspection"],
            ["relation_inspection"]
          ]
        )
      end)
      |> then(fn {:ok, %{rows: rows}} -> rows |> List.flatten() |> Enum.join("\n") end)

    assert plan =~ "runner_tasks_claim_idx"
  end

  defp enqueue_command(fixture, suffix, opts \\ []) do
    task_kind = Keyword.get(opts, :task_kind, :relation_inspection)
    payload = Keyword.get(opts, :payload, inspection_payload())
    {:ok, encoded_payload, payload_hash} = Codec.encode_payload(task_kind, payload)

    {:ok, orchestration_context} =
      Codec.encode_orchestration_context(%{kind: :test})

    occurred_at = Keyword.get(opts, :occurred_at, fixture.now)

    %C.EnqueueRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "enqueue-#{suffix}",
      task_id: "rt_#{random_id()}",
      domain_identity: "runner-task-domain-#{suffix}-#{random_id()}",
      task_kind: task_kind,
      runner_pool: Keyword.get(opts, :runner_pool, fixture.runner_pool),
      required_runner_release_id: Keyword.get(opts, :required_runner_release_id, @release),
      retry_class:
        Keyword.get(opts, :retry_class, Favn.Contracts.RunnerTask.default_retry_class(task_kind)),
      payload: encoded_payload,
      payload_hash: payload_hash,
      orchestration_context: orchestration_context,
      run_id: Keyword.get(opts, :run_id, "run-#{suffix}"),
      operation_id: nil,
      asset_step_id: nil,
      required_capability:
        Keyword.get(
          opts,
          :required_capability,
          if(task_kind == :asset_attempt, do: "asset_execution", else: "relation_inspection")
        ),
      deadline_at: DateTime.add(occurred_at, 60, :second),
      occurred_at: occurred_at
    }
  end

  defp start_runner_registry do
    case Process.whereis(RunnerRegistry) do
      nil -> start_supervised!({RunnerRegistry, []})
      _pid -> :ok
    end

    case Process.whereis(FavnOrchestrator.RunnerQueueDynamicSupervisor) do
      nil -> start_supervised!({RunnerQueueSupervisor, []})
      _pid -> :ok
    end
  end

  defp cancellation_run(fixture, task_id) do
    RunState.new(
      id: "run-cancel:" <> task_id,
      workspace_id: fixture.workspace_id,
      manifest_version_id: "mv_runner_task",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: @release,
      asset_ref: {MyApp.RunnerTaskCancellation, :asset},
      metadata: %{in_flight_task_ids: [task_id]}
    )
  end

  defp cancellation_agent(owner, behavior) do
    receive do
      {:assignment, assignment} -> cancellation_agent_loop(owner, behavior, assignment)
    end
  end

  defp cancellation_agent_loop(owner, behavior, assignment) do
    receive do
      {:favn_runner_task, %Favn.Contracts.RunnerTask.Cancellation{} = cancellation} ->
        case behavior do
          :acknowledge ->
            ack = %Favn.Contracts.RunnerTask.CancellationAck{
              workspace_id: cancellation.workspace_id,
              task_id: cancellation.task_id,
              runner_instance_id: cancellation.runner_instance_id,
              runner_session_generation: cancellation.runner_session_generation,
              assignment_generation: cancellation.assignment_generation,
              command_id: cancellation.command_id,
              status: :observed,
              acknowledged_at: DateTime.utc_now()
            }

            assert {:ok, _ack} = RunnerTasks.acknowledge_cancellation(ack)
            complete_cancelled_assignment(assignment)
            send(owner, {:runner_observed_cancellation, cancellation.task_id})

          :ignore ->
            send(owner, {:runner_ignored_cancellation, cancellation.task_id})
        end

        cancellation_agent_loop(owner, behavior, assignment)
    end
  end

  defp complete_cancelled_assignment(assignment) do
    error = Favn.Contracts.RunnerError.cancelled(:operator_request)

    assert {:ok, _ack} =
             RunnerTasks.complete(%Favn.Contracts.RunnerTask.Result{
               workspace_id: assignment.workspace_id,
               task_id: assignment.task_id,
               task_kind: assignment.task_kind,
               runner_instance_id: assignment.runner_instance_id,
               runner_session_generation: assignment.runner_session_generation,
               assignment_generation: assignment.assignment_generation,
               outcome: :cancelled,
               retry_class: :terminal,
               result: nil,
               error: error,
               finished_at: DateTime.utc_now()
             })
  end

  defp claim_command(fixture, suffix, runner_id, opts \\ []) do
    %C.ClaimRunnerTask{
      platform_context: fixture.platform_context,
      command_id: "#{fixture.workspace_id}:claim-#{suffix}",
      runner_instance_id: "#{fixture.workspace_id}:#{runner_id}",
      runner_session_generation: Keyword.get(opts, :runner_session_generation, 1),
      runner_pool: Keyword.get(opts, :runner_pool, fixture.runner_pool),
      required_runner_release_id: Keyword.get(opts, :required_runner_release_id, @release),
      supported_task_kinds: Keyword.get(opts, :supported_task_kinds, [:relation_inspection]),
      capabilities: Keyword.get(opts, :capabilities, ["relation_inspection"]),
      lease_duration_ms: Keyword.get(opts, :lease_duration_ms, 30_000),
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.add(fixture.now, 1, :second))
    }
  end

  defp transition_command(fixture, task, command_id, transition) do
    %C.TransitionRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: command_id,
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: task.assigned_runner_session_generation,
      assignment_generation: task.assignment_generation,
      transition: transition,
      lease_duration_ms: if(transition == :renew, do: 30_000),
      occurred_at: DateTime.add(fixture.now, 2, :second)
    }
  end

  defp release_command(fixture, task, command_id, disposition, reason) do
    %C.ReleaseRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: command_id,
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: task.assigned_runner_session_generation,
      assignment_generation: task.assignment_generation,
      disposition: disposition,
      reason: reason,
      occurred_at: DateTime.add(fixture.now, 2, :second)
    }
  end

  defp complete_command(fixture, task, command_id, result) do
    %C.CompleteRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: command_id,
      task_id: task.task_id,
      runner_instance_id: task.assigned_runner_instance_id,
      runner_session_generation: task.assigned_runner_session_generation,
      assignment_generation: task.assignment_generation,
      result_version: 1,
      outcome: :succeeded,
      retry_class: :terminal,
      result: result,
      error: nil,
      occurred_at: DateTime.add(fixture.now, 3, :second)
    }
  end

  defp demand(fixture) do
    Store.demand(%Q.GetRunnerCapacityDemand{
      platform_context: fixture.platform_context,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release
    })
  end

  defp platform_command_id(fixture, suffix),
    do: "#{fixture.workspace_id}:#{suffix}"

  defp inspection_payload do
    %RelationInspectionRequest{
      manifest_version_id: "mv_runner_task",
      required_runner_release_id: @release,
      include: [:columns],
      sample_limit: 0
    }
  end

  defp random_id,
    do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
