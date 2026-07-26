defmodule FavnStoragePostgres.StorageV2.RunnerTasksTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.RunnerTasks.Store
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
    now = DateTime.utc_now()

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

    {:ok, workspace_id: workspace_id, workspace_context: workspace_context, now: now}
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

  test "enqueue is idempotent, validates payload hashes, and updates exact demand", fixture do
    command = enqueue_command(fixture, "enqueue")

    assert {:ok, first} = Store.enqueue(command)
    assert {:ok, second} = Store.enqueue(command)
    assert first == second
    assert first.status == :queued
    assert first.task_kind == :relation_inspection
    assert first.retry_class == :safe_to_retry

    assert {:ok, demand} = demand(fixture)
    assert demand.outstanding_count == 1
    assert demand.queued_count == 1
    assert demand.active_count == 0
    assert demand.oldest_queued_at == command.occurred_at
    assert demand.healthy?

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
    {:ok, payload_hash} = FavnStoragePostgres.CanonicalJSON.hash(entries)

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

    fingerprint = :crypto.hash(:sha256, "resolved runtime input metadata")

    command = %C.PersistRunnerTaskRuntimeInputs{
      workspace_context: fixture.workspace_context,
      command_id: "runtime-inputs-resolved",
      task_id: assigned.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: assigned.assignment_generation,
      resolution_id: "resolution-one",
      status: :resolved,
      payload_fingerprint: fingerprint,
      error: nil,
      occurred_at: DateTime.add(fixture.now, 2, :second)
    }

    assert {:ok, resolved} = Store.persist_runtime_inputs(command)
    assert resolved.runtime_input_resolution_id == "resolution-one"
    assert resolved.runtime_input_resolution_status == :resolved
    assert resolved.runtime_input_payload_fingerprint == fingerprint
    assert resolved.runtime_input_error == nil
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
                 payload_fingerprint: :crypto.strong_rand_bytes(32)
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
    assert status == "resolved"
    assert persisted_fingerprint == fingerprint
    assert persisted_error == nil
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
    assert completed.result == encoded_result
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

  test "expired assignments are claimed once for fenced recovery", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "recover"))

    assert {:ok, assigned} =
             Store.claim(
               claim_command(fixture, "claim-recover", "runner-recover", lease_duration_ms: 1)
             )

    recovery_time = DateTime.add(fixture.now, 2, :second)

    command = %C.RecoverRunnerTasks{
      workspace_context: fixture.workspace_context,
      command_id: "recover-expired",
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
               | command_id: "recover-empty",
                 occurred_at: recovery_time
             })
  end

  test "known-stale demand fails closed, rolls mutations back, and reconciles exactly", fixture do
    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "stale-base"))

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.runner_capacity_demands
      SET healthy = false
      WHERE workspace_id = $1 AND runner_pool = $2 AND required_runner_release_id = $3
      """,
      [fixture.workspace_id, "duckdb", @release]
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
               workspace_context: fixture.workspace_context,
               command_id: "reconcile-demand",
               runner_pool: "duckdb",
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
      WHERE workspace_id = $1 AND runner_pool = $2 AND required_runner_release_id = $3
      """,
      [fixture.workspace_id, "duckdb", @release]
    )

    command = %C.ReconcileRunnerCapacityDemand{
      workspace_context: fixture.workspace_context,
      command_id: "audit-demand-drift",
      runner_pool: "duckdb",
      required_runner_release_id: @release,
      mode: :audit,
      occurred_at: fixture.now
    }

    assert {:ok, %{healthy?: false}} = Store.reconcile_demand(command)
    assert {:error, %{kind: :unavailable}} = demand(fixture)

    assert {:ok, repaired} =
             Store.reconcile_demand(%{
               command
               | command_id: "repair-demand-drift",
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
            workspace_context: fixture.workspace_context,
            command_id: "reconcile-race-#{index}",
            runner_pool: "duckdb",
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
               WHERE workspace_id = $1
                 AND runner_pool = $2
                 AND required_runner_release_id = $3
                 AND status = 'queued'
               """,
               [fixture.workspace_id, "duckdb", @release]
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
          WHERE workspace_id = $1
            AND status = 'queued'
            AND runner_pool = $2
            AND required_runner_release_id = $3
            AND task_kind = ANY($4::text[])
            AND (required_capability IS NULL OR required_capability = ANY($5::text[]))
          ORDER BY enqueued_at, task_id
          LIMIT 1
          FOR UPDATE SKIP LOCKED
          """,
          [
            fixture.workspace_id,
            "duckdb",
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
    occurred_at = Keyword.get(opts, :occurred_at, fixture.now)

    %C.EnqueueRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "enqueue-#{suffix}",
      task_id: "rt_#{random_id()}",
      domain_identity: "runner-task-domain-#{suffix}-#{random_id()}",
      task_kind: task_kind,
      runner_pool: Keyword.get(opts, :runner_pool, "duckdb"),
      required_runner_release_id: Keyword.get(opts, :required_runner_release_id, @release),
      retry_class:
        Keyword.get(opts, :retry_class, Favn.Contracts.RunnerTask.default_retry_class(task_kind)),
      payload: encoded_payload,
      payload_hash: payload_hash,
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

  defp claim_command(fixture, suffix, runner_id, opts \\ []) do
    %C.ClaimRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "claim-#{suffix}",
      runner_instance_id: runner_id,
      runner_session_generation: Keyword.get(opts, :runner_session_generation, 1),
      runner_pool: Keyword.get(opts, :runner_pool, "duckdb"),
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
      workspace_context: fixture.workspace_context,
      runner_pool: "duckdb",
      required_runner_release_id: @release
    })
  end

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
