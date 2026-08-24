defmodule FavnStoragePostgres.StorageV2.RunnerTasksTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerTask.LeaseRenewal
  alias Favn.Contracts.RunnerTask.Registration
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunnerGateway
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
  alias FavnStoragePostgres.Schemas.RunnerTaskCommand
  alias FavnStoragePostgres.Schemas.RunnerTaskLogBatch
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.TestSupport.DistributedRunnerAgent

  @release "rr_" <> String.duplicate("a", 64)
  @other_release "rr_" <> String.duplicate("b", 64)
  @distributed_test_address {127, 0, 0, 2}
  @distributed_test_host ~c"control-plane.favn.test"

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

    assert {:ok, _claimed} =
             Store.claim(claim_command(fixture, "claim-enqueue-replay", "enqueue-replay-runner"))

    assert {:ok, ^first} = Store.enqueue(later_replay)

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

  test "expired inspection work cannot be claimed or completed late", fixture do
    expired = %{
      enqueue_command(fixture, "expired-before-claim")
      | deadline_at: DateTime.add(fixture.now, 1, :second)
    }

    assert {:ok, %{status: :queued}} = Store.enqueue(expired)

    assert {:ok, nil} =
             Store.claim(
               claim_command(fixture, "claim-expired", "runner-expired",
                 occurred_at: DateTime.add(fixture.now, 2, :second)
               )
             )

    assert {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "late-completion"))

    assert {:ok, assigned} =
             Store.claim(
               claim_command(fixture, "claim-late", "runner-late",
                 occurred_at: DateTime.add(fixture.now, 1, :second)
               )
             )

    assert {:ok, running} =
             Store.transition(transition_command(fixture, assigned, "start-late", :running))

    result = %RelationInspectionResult{
      required_runner_release_id: @release,
      row_count: 1,
      inspected_at: DateTime.add(fixture.now, 61, :second)
    }

    assert {:ok, encoded_result} =
             Codec.encode_result(:relation_inspection, :succeeded, result)

    late = %{
      complete_command(fixture, running, "complete-after-deadline", encoded_result)
      | issued_at: DateTime.add(fixture.now, 61, :second),
        occurred_at: DateTime.add(fixture.now, 61, :second)
    }

    assert {:error, %{kind: :invalid}} = Store.complete(late)
  end

  test "concurrent task ids cannot share one durable domain identity", fixture do
    first = enqueue_command(fixture, "domain-race-first")

    second =
      fixture
      |> enqueue_command("domain-race-second")
      |> Map.put(:domain_identity, first.domain_identity)

    results =
      [first, second]
      |> Task.async_stream(&Store.enqueue/1,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert 1 == Enum.count(results, &match?({:ok, _task}, &1))
    assert 1 == Enum.count(results, &match?({:error, %{kind: :conflict}}, &1))

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.runner_tasks
               WHERE workspace_id = $1 AND domain_identity = $2
               """,
               [fixture.workspace_id, first.domain_identity]
             )
  end

  test "command receipts have a bounded replay window and prune through indexed time", fixture do
    scope_id = "workspace:" <> fixture.workspace_id
    expired_command_id = "expired-receipt-#{random_id()}"

    assert {1, nil} =
             Repo.insert_all(RunnerTaskCommand, [
               %{
                 scope_id: scope_id,
                 command_id: expired_command_id,
                 operation: "claim",
                 request_hash: :crypto.strong_rand_bytes(32),
                 result: %{"kind" => "none"},
                 issued_at: DateTime.add(fixture.now, -8 * 24 * 60 * 60, :second),
                 inserted_at: DateTime.add(fixture.now, -8 * 24 * 60 * 60, :second)
               }
             ])

    stale_issued_at = DateTime.add(fixture.now, -8 * 24 * 60 * 60, :second)

    stale_command =
      fixture
      |> claim_command("outside-replay-window", "expired-runner",
        issued_at: stale_issued_at,
        occurred_at: stale_issued_at
      )
      |> Map.put(:command_id, expired_command_id)

    assert {:error, %{kind: :invalid}} = Store.claim(stale_command)

    assert {:ok, nil} =
             Store.claim(claim_command(fixture, "prune-expired-receipt", "current-runner"))

    refute Repo.get_by(RunnerTaskCommand,
             scope_id: scope_id,
             command_id: expired_command_id
           )

    assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "after-pruned-command"))

    assert {:error, %{kind: :invalid}} =
             Store.claim(%{stale_command | occurred_at: fixture.now})

    assert {:ok, claimed} =
             Store.claim(claim_command(fixture, "fresh-after-prune", "fresh-runner"))

    assert claimed.task_id == queued.task_id

    plan =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])
        SQL.query!(Repo, "SET LOCAL enable_sort = off", [])

        SQL.query!(
          Repo,
          """
          EXPLAIN (FORMAT TEXT)
          SELECT scope_id, command_id
          FROM favn_control.runner_task_commands
          WHERE inserted_at < $1
          ORDER BY inserted_at
          LIMIT 100
          FOR UPDATE SKIP LOCKED
          """,
          [fixture.now]
        )
      end)
      |> then(fn {:ok, %{rows: rows}} -> rows |> List.flatten() |> Enum.join("\n") end)

    assert plan =~ "runner_task_commands_retention_idx"
  end

  test "enqueue identity locking precedes bounded receipt pruning under contention", fixture do
    expired_at = DateTime.add(fixture.now, -8 * 24 * 60 * 60, :second)
    scope_id = "workspace:" <> fixture.workspace_id

    rows =
      Enum.map(1..100, fn index ->
        %{
          scope_id: scope_id,
          command_id: "expired-lock-order-#{index}-#{random_id()}",
          operation: "claim",
          request_hash: :crypto.strong_rand_bytes(32),
          result: %{"kind" => "none"},
          issued_at: expired_at,
          inserted_at: expired_at
        }
      end)

    assert {100, nil} = Repo.insert_all(RunnerTaskCommand, rows)
    command = enqueue_command(fixture, "lock-order")

    results =
      1..12
      |> Task.async_stream(
        fn index ->
          Store.enqueue(%{
            command
            | occurred_at: DateTime.add(command.occurred_at, index, :microsecond)
          })
        end,
        max_concurrency: 12,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, _task}, &1))
    assert 1 == results |> Enum.map(fn {:ok, task} -> task.task_id end) |> Enum.uniq() |> length()

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.runner_task_commands
               WHERE scope_id = $1 AND inserted_at = $2
               """,
               [scope_id, expired_at]
             )
  end

  test "history prune anti-joins and task deletion use their child indexes", fixture do
    plans =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL plan_cache_mode = force_generic_plan", [])
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])

        outcome =
          SQL.query!(
            Repo,
            """
            EXPLAIN (FORMAT TEXT)
            SELECT outcome.workspace_id, outcome.task_id, outcome.assignment_generation
            FROM favn_control.runner_task_outcomes AS outcome
            WHERE outcome.inserted_at < $1
              AND NOT EXISTS (
                SELECT 1
                FROM favn_control.runner_task_command_tasks AS snapshot
                WHERE snapshot.outcome_assignment_generation IS NOT NULL
                  AND snapshot.workspace_id = outcome.workspace_id
                  AND snapshot.task_id = outcome.task_id
                  AND snapshot.outcome_assignment_generation = outcome.assignment_generation
              )
            ORDER BY outcome.inserted_at
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            """,
            [fixture.now, 100]
          )

        runtime_error =
          SQL.query!(
            Repo,
            """
            EXPLAIN (FORMAT TEXT)
            SELECT history.workspace_id, history.task_id, history.resolution_id
            FROM favn_control.runner_task_runtime_input_errors AS history
            WHERE history.inserted_at < $1
              AND NOT EXISTS (
                SELECT 1
                FROM favn_control.runner_task_command_tasks AS snapshot
                WHERE snapshot.runtime_input_resolution_id IS NOT NULL
                  AND snapshot.workspace_id = history.workspace_id
                  AND snapshot.task_id = history.task_id
                  AND snapshot.runtime_input_resolution_id = history.resolution_id
              )
            ORDER BY history.inserted_at
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            """,
            [fixture.now, 100]
          )

        task_reference =
          SQL.query!(
            Repo,
            """
            EXPLAIN (FORMAT TEXT)
            SELECT 1
            FROM favn_control.runner_task_command_tasks
            WHERE workspace_id = $1 AND task_id = $2
            """,
            [fixture.workspace_id, "rt_index_probe"]
          )

        {explain_text(outcome), explain_text(runtime_error), explain_text(task_reference)}
      end)

    assert {:ok, {outcome_plan, runtime_error_plan, task_reference_plan}} = plans
    assert outcome_plan =~ "runner_task_command_tasks_outcome_idx"
    assert runtime_error_plan =~ "runner_task_command_tasks_runtime_input_error_idx"
    assert task_reference_plan =~ "runner_task_command_tasks_task_idx"

    command = enqueue_command(fixture, "task-fk-lifecycle")
    assert {:ok, task} = Store.enqueue(command)

    assert_raise Postgrex.Error, fn ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.runner_tasks WHERE workspace_id = $1 AND task_id = $2",
        [fixture.workspace_id, task.task_id]
      )
    end

    assert %{num_rows: 1} =
             SQL.query!(
               Repo,
               "DELETE FROM favn_control.runner_task_commands WHERE scope_id = $1 AND command_id = $2",
               ["workspace:" <> fixture.workspace_id, command.command_id]
             )

    assert %{num_rows: 1} =
             SQL.query!(
               Repo,
               "DELETE FROM favn_control.runner_tasks WHERE workspace_id = $1 AND task_id = $2",
               [fixture.workspace_id, task.task_id]
             )

    assert %{num_rows: 1} =
             SQL.query!(
               Repo,
               """
               DELETE FROM favn_control.runner_capacity_demands
               WHERE runner_pool = $1 AND required_runner_release_id = $2
               """,
               [fixture.runner_pool, @release]
             )
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

  test "concurrent first operation ensures share one durable issuance", fixture do
    version = manifest_version("mv-concurrent-ensure-#{random_id()}", fixture.runner_pool)
    asset_ref = {MyApp.DistributedRunnerAsset, :asset}
    request = inspection_payload()
    identity = {:concurrent_ensure, random_id()}
    issued_candidates = [fixture.now, DateTime.add(fixture.now, 1, :second)]

    results =
      issued_candidates
      |> Task.async_stream(
        fn occurred_at ->
          OperationRunnerTasks.ensure(
            fixture.workspace_context,
            version,
            asset_ref,
            :relation_inspection,
            request,
            identity,
            occurred_at: occurred_at
          )
        end,
        max_concurrency: 2,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn result ->
        assert {:ok, {:ok, task}} = result
        task
      end)

    assert [first, second] = results
    assert first == second
    assert first.enqueued_at in issued_candidates

    scope_id = "workspace:" <> fixture.workspace_id
    command_id = "enqueue:" <> first.task_id

    assert %{rows: [[1, issued_at]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*), min(issued_at)
               FROM favn_control.runner_task_commands
               WHERE scope_id = $1 AND command_id = $2
               """,
               [scope_id, command_id]
             )

    assert issued_at == DateTime.to_naive(first.enqueued_at)
  end

  test "large task fields are stored once while command receipts remain bounded", fixture do
    blob_bytes = 950_000

    payload = %Favn.Contracts.RunnerWork{
      run_id: "large-receipt-run",
      runner_pool: :duckdb,
      required_runner_release_id: @release,
      metadata: %{blob: incompressible_text(blob_bytes)}
    }

    enqueue =
      enqueue_command(fixture, "large-receipt",
        task_kind: :asset_attempt,
        payload: payload,
        orchestration_context: %{blob: incompressible_text(blob_bytes)}
      )

    assert {:ok, queued} = Store.enqueue(enqueue)

    claim =
      claim_command(fixture, "large-receipt", "large-receipt-runner",
        supported_task_kinds: [:asset_attempt],
        capabilities: ["asset_execution"]
      )

    assert {:ok, assigned} = Store.claim(claim)
    assert {:ok, ^queued} = Store.enqueue(%{enqueue | occurred_at: DateTime.utc_now()})
    assert {:ok, ^assigned} = Store.claim(%{claim | occurred_at: DateTime.utc_now()})

    assert {:ok, running} =
             Store.transition(
               transition_command(fixture, assigned, "large-receipt-start", :running)
             )

    result = %Favn.Contracts.RunnerResult{
      run_id: payload.run_id,
      manifest_version_id: "mv-large-receipt",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: @release,
      metadata: %{blob: incompressible_text(blob_bytes)}
    }

    assert {:ok, encoded_result} =
             Codec.encode_result(:asset_attempt, :succeeded, result)

    complete = complete_command(fixture, running, "large-receipt-complete", encoded_result)
    assert {:ok, completed} = Store.complete(complete)
    assert completed.result == result
    assert {:ok, ^completed} = Store.complete(%{complete | occurred_at: DateTime.utc_now()})

    assert %{rows: [[payload_size, context_size, result_size]]} =
             SQL.query!(
               Repo,
               """
               SELECT pg_column_size(payload),
                      pg_column_size(orchestration_context),
                      pg_column_size(result)
               FROM favn_control.runner_tasks
               WHERE workspace_id = $1 AND task_id = $2
               """,
               [fixture.workspace_id, queued.task_id]
             )

    assert payload_size > 262_144
    assert context_size > 262_144
    assert result_size > 262_144

    assert %{rows: [[max_receipt_size, max_snapshot_size]]} =
             SQL.query!(
               Repo,
               """
               SELECT
                 (SELECT max(pg_column_size(result))
                  FROM favn_control.runner_task_commands
                  WHERE scope_id IN ($1, 'platform:runner_tasks')),
                 (SELECT max(octet_length(snapshot))
                  FROM favn_control.runner_task_command_tasks
                  WHERE workspace_id = $2 AND task_id = $3)
               """,
               ["workspace:" <> fixture.workspace_id, fixture.workspace_id, queued.task_id]
             )

    assert max_receipt_size <= 262_144
    assert max_snapshot_size <= 262_144

    assert %{rows: [[1, outcome_size]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*), max(pg_column_size(result))
               FROM favn_control.runner_task_outcomes
               WHERE workspace_id = $1 AND task_id = $2
               """,
               [fixture.workspace_id, queued.task_id]
             )

    assert outcome_size > 262_144
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

  test "workspace activity paging includes pre-run tasks newest first", fixture do
    first_command =
      enqueue_command(fixture, "workspace-page-first",
        occurred_at: fixture.now,
        run_id: nil
      )

    second_command =
      enqueue_command(fixture, "workspace-page-second",
        occurred_at: DateTime.add(fixture.now, 1, :microsecond),
        run_id: nil
      )

    assert {:ok, first} = Store.enqueue(first_command)
    assert {:ok, second} = Store.enqueue(second_command)

    query = %Q.PageWorkspaceRunnerTasks{
      workspace_context: fixture.workspace_context,
      limit: 1
    }

    assert {:ok, [page_one]} = Store.page_workspace(query)
    assert page_one.task_id == second.task_id
    assert is_nil(page_one.payload)
    assert is_nil(page_one.orchestration_context)

    assert {:ok, [page_two]} =
             Store.page_workspace(%{
               query
               | cursor: {page_one.inserted_at, page_one.task_id}
             })

    assert page_two.task_id == first.task_id

    assert {:ok, []} =
             Store.page_workspace(%{query | statuses: [:failed, :unknown], limit: 20})

    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    assert diagnostics.definition_fingerprint_matches?
    refute "runner_tasks_workspace_recent_idx" in diagnostics.missing_critical_indexes
    refute "runner_tasks_workspace_status_recent_idx" in diagnostics.missing_critical_indexes
  end

  test "assignment generations fence stale runners across safe requeue", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "fence"))
    first_command = claim_command(fixture, "claim-first", "runner-first")
    assert {:ok, first} = Store.claim(first_command)

    assert {:ok, ^first} = Store.claim(first_command)

    assert {:ok, requeued} =
             Store.release(
               release_command(fixture, first, "release-first", :requeue, :runner_stopped)
             )

    assert requeued.status == :queued

    assert {:ok, second} =
             Store.claim(claim_command(fixture, "claim-second", "runner-second"))

    assert second.assignment_generation == first.assignment_generation + 1

    assert {:ok, ^first} = Store.claim(first_command)

    assert {:error, %{kind: :fenced}} =
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
      issued_at: fixture.now,
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

  test "log batches persist runner terms through the JSON-safe boundary", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "json-safe-logs"))

    assert {:ok, claimed} =
             Store.claim(claim_command(fixture, "claim-json-safe-logs", "runner-json-safe-logs"))

    entries =
      [
        %{
          type: :runner_event,
          event: %{
            event_type: :asset_started,
            occurred_at: fixture.now,
            payload: %{
              asset_ref: {__MODULE__, :asset},
              authorization_token: "must not persist"
            }
          }
        }
      ] ++
        Enum.map(1..60, fn index ->
          %{type: :runner_event, event: %{event_type: :asset_log, index: index}}
        end)

    {:ok, payload_hash} = Favn.Contracts.RunnerTask.PersistenceCodec.hash_term(entries)

    command = %C.AppendRunnerTaskLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "log-json-safe",
      task_id: claimed.task_id,
      runner_instance_id: claimed.assigned_runner_instance_id,
      runner_session_generation: claimed.assigned_runner_session_generation,
      assignment_generation: claimed.assignment_generation,
      batch_id: "batch-json-safe",
      sequence: 0,
      entries: entries,
      payload_hash: payload_hash,
      issued_at: fixture.now,
      occurred_at: fixture.now
    }

    assert {:ok, :persisted} = Store.append_log_batch(command)

    persisted =
      Repo.get_by!(RunnerTaskLogBatch,
        workspace_id: fixture.workspace_context.workspace_id,
        task_id: claimed.task_id,
        batch_id: command.batch_id
      )

    assert [first | remaining] = persisted.entries

    assert %{
             "type" => "runner_event",
             "event" => %{
               "event_type" => "asset_started",
               "occurred_at" => occurred_at,
               "payload" => %{
                 "asset_ref" => %{
                   "module" => "Elixir.FavnStoragePostgres.StorageV2.RunnerTasksTest",
                   "name" => "asset"
                 },
                 "authorization_token" => "[REDACTED]"
               }
             }
           } = first

    assert Enum.map(remaining, &get_in(&1, ["event", "index"])) == Enum.to_list(1..60)
    assert occurred_at == DateTime.to_iso8601(fixture.now)
  end

  test "runtime-input acknowledgements persist bounded metadata; failed resolutions are replaceable",
       fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "runtime-inputs"))

    claim = claim_command(fixture, "claim-runtime-inputs", "runner-runtime-inputs")
    assert {:ok, assigned} = Store.claim(claim)

    error =
      Favn.Contracts.RunnerError.new(
        type: :runtime_input_resolution_failed,
        message: String.duplicate("e", 200_000),
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
      issued_at: DateTime.add(fixture.now, 2, :second),
      occurred_at: DateTime.add(fixture.now, 2, :second)
    }

    assert {:ok, resolved} = Store.persist_runtime_inputs(command)
    assert resolved.runtime_input_resolution_id == "resolution-one"
    assert resolved.runtime_input_resolution_status == :failed
    assert resolved.runtime_input_payload_fingerprint == nil
    assert resolved.runtime_input_error
    assert {:ok, ^assigned} = Store.claim(%{claim | occurred_at: DateTime.utc_now()})
    assert {:ok, ^resolved} = Store.persist_runtime_inputs(command)

    assert {:ok, exact_retry} =
             Store.persist_runtime_inputs(%{
               command
               | command_id: "runtime-inputs-exact-retry"
             })

    assert exact_retry.runtime_input_resolution_id == resolved.runtime_input_resolution_id

    # A failed resolution pins nothing durable, so a later resolution from the
    # currently fenced assignment replaces it instead of wedging the task.
    assert {:ok, replaced} =
             Store.persist_runtime_inputs(%{
               command
               | command_id: "runtime-inputs-replacement",
                 resolution_id: "resolution-two",
                 error: Favn.Contracts.RunnerError.new(type: :different_failure)
             })

    assert replaced.runtime_input_resolution_id == "resolution-two"
    assert replaced.runtime_input_resolution_status == :failed

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

    assert resolution_id == "resolution-two"
    assert status == "failed"
    assert persisted_fingerprint == nil
    assert persisted_error

    assert %{rows: [[2, error_size, max_snapshot_size]]} =
             SQL.query!(
               Repo,
               """
               SELECT
                 (SELECT count(*)
                  FROM favn_control.runner_task_runtime_input_errors
                  WHERE workspace_id = $1 AND task_id = $2),
                 (SELECT max(pg_column_size(error))
                  FROM favn_control.runner_task_runtime_input_errors
                  WHERE workspace_id = $1 AND task_id = $2),
                 (SELECT max(octet_length(snapshot))
                  FROM favn_control.runner_task_command_tasks
                  WHERE workspace_id = $1 AND task_id = $2)
               """,
               [fixture.workspace_id, assigned.task_id]
             )

    assert error_size > 0
    assert error_size <= 262_144
    assert max_snapshot_size <= 262_144

    # The incident path: the assignment expires, the task is requeued, and the
    # next assignment must be able to record its own resolution outcome.
    assert {:ok, requeued} =
             Store.release(
               release_command(
                 fixture,
                 assigned,
                 "runtime-inputs-requeue",
                 :requeue,
                 :lease_expired
               )
             )

    assert requeued.status == :queued

    assert {:ok, reassigned} =
             Store.claim(
               claim_command(fixture, "reclaim-runtime-inputs", "runner-runtime-inputs-two")
             )

    assert reassigned.assignment_generation == assigned.assignment_generation + 1

    assert {:ok, regenerated} =
             Store.persist_runtime_inputs(%{
               command
               | command_id: "runtime-inputs-generation-two",
                 runner_instance_id: reassigned.assigned_runner_instance_id,
                 runner_session_generation: reassigned.assigned_runner_session_generation,
                 assignment_generation: reassigned.assignment_generation,
                 resolution_id: "resolution-three"
             })

    assert regenerated.runtime_input_resolution_id == "resolution-three"
    assert regenerated.runtime_input_resolution_status == :failed
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
               issued_at: fixture.now,
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
               issued_at: DateTime.add(fixture.now, 2, :second),
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
               issued_at: DateTime.add(fixture.now, 3, :second),
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
      issued_at: fixture.now,
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
      issued_at: fixture.now,
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
      issued_at: DateTime.add(fixture.now, 3, :second),
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

  test "operation ensure requeues a live safe failure despite its historical enqueue receipt",
       fixture do
    version = manifest_version("mv-safe-operation-retry-#{random_id()}", fixture.runner_pool)
    asset_ref = {MyApp.DistributedRunnerAsset, :asset}
    request = inspection_payload()
    identity = {:safe_operation_retry, random_id()}

    assert {:ok, queued} =
             OperationRunnerTasks.ensure(
               fixture.workspace_context,
               version,
               asset_ref,
               :relation_inspection,
               request,
               identity,
               occurred_at: fixture.now
             )

    assert queued.status == :queued

    assert {:ok, assigned} =
             Store.claim(claim_command(fixture, "claim-safe-operation", "runner-safe-operation"))

    assert {:ok, running} =
             Store.transition(
               transition_command(fixture, assigned, "start-safe-operation", :running)
             )

    first_completion = %C.CompleteRunnerTask{
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
      issued_at: DateTime.add(fixture.now, 3, :second),
      occurred_at: DateTime.add(fixture.now, 3, :second)
    }

    assert {:ok, failed} = Store.complete(first_completion)

    assert {:ok, live_failed} =
             Store.get(%Q.GetRunnerTask{
               workspace_context: fixture.workspace_context,
               task_id: failed.task_id
             })

    assert live_failed.status == :failed
    assert live_failed.result_version == failed.result_version

    assert {:ok, retried} =
             OperationRunnerTasks.ensure(
               fixture.workspace_context,
               version,
               asset_ref,
               :relation_inspection,
               request,
               identity,
               occurred_at: DateTime.add(fixture.now, 4, :second)
             )

    assert retried.status == :queued
    assert retried.result_version == nil
    assert retried.error == nil

    assert {:ok, ^retried} =
             OperationRunnerTasks.ensure(
               fixture.workspace_context,
               version,
               asset_ref,
               :relation_inspection,
               request,
               identity,
               occurred_at: DateTime.add(fixture.now, 4, :second)
             )

    assert {:ok, %{outstanding_count: 1, queued_count: 1, active_count: 0}} =
             demand(fixture)

    assert {:ok, reassigned} =
             Store.claim(
               claim_command(fixture, "reclaim-safe-operation", "runner-safe-operation-two",
                 occurred_at: DateTime.add(fixture.now, 5, :second)
               )
             )

    assert reassigned.assignment_generation == assigned.assignment_generation + 1

    assert {:ok, rerunning} =
             Store.transition(
               fixture
               |> transition_command(reassigned, "restart-safe-operation", :running)
               |> Map.merge(%{
                 issued_at: DateTime.add(fixture.now, 6, :second),
                 occurred_at: DateTime.add(fixture.now, 6, :second)
               })
             )

    second_result = %RelationInspectionResult{
      required_runner_release_id: @release,
      row_count: 2,
      inspected_at: DateTime.add(fixture.now, 6, :second)
    }

    assert {:ok, encoded_second_result} =
             Codec.encode_result(:relation_inspection, :succeeded, second_result)

    assert {:ok, succeeded} =
             fixture
             |> complete_command(
               rerunning,
               "complete-safe-operation-success",
               encoded_second_result
             )
             |> Map.merge(%{
               issued_at: DateTime.add(fixture.now, 7, :second),
               occurred_at: DateTime.add(fixture.now, 7, :second)
             })
             |> Store.complete()

    assert succeeded.status == :succeeded
    assert succeeded.assignment_generation == failed.assignment_generation + 1
    assert succeeded.result == second_result

    assert {:ok, replayed_failure} =
             Store.complete(%{
               first_completion
               | occurred_at: DateTime.add(fixture.now, 10, :second)
             })

    assert replayed_failure == failed
    assert replayed_failure.assignment_generation == assigned.assignment_generation
    assert replayed_failure.status == :failed
    assert replayed_failure.result == nil
    assert replayed_failure.error == failed.error
  end

  test "expired assignments are claimed once for fenced recovery", fixture do
    # recover_expired scans every workspace, so expired assignments left behind
    # by interrupted runs against the shared test database would surface in this
    # test's recovery batch. This module is async: false, so nothing else is
    # mid-assignment while the purge runs.
    purge_expired_assignments!()

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
      issued_at: recovery_time,
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

  @tag timeout: 120_000
  test "maximum recovery batches use bounded per-task snapshots", fixture do
    count = 500

    1..count
    |> Task.async_stream(
      fn index -> Store.enqueue(enqueue_command(fixture, "recover-max-#{index}")) end,
      max_concurrency: 16,
      ordered: false,
      timeout: 30_000
    )
    |> Enum.each(fn result -> assert {:ok, {:ok, _task}} = result end)

    1..count
    |> Task.async_stream(
      fn index ->
        Store.claim(
          claim_command(fixture, "recover-max-#{index}", "recover-max-runner-#{index}",
            lease_duration_ms: 1
          )
        )
      end,
      max_concurrency: 16,
      ordered: false,
      timeout: 30_000
    )
    |> Enum.each(fn result -> assert {:ok, {:ok, _task}} = result end)

    recovery_time = DateTime.add(fixture.now, 2, :second)

    command = %C.RecoverRunnerTasks{
      platform_context: fixture.platform_context,
      command_id: platform_command_id(fixture, "recover-max"),
      owner_id: "recovery-max-owner",
      issued_at: recovery_time,
      occurred_at: recovery_time,
      limit: count,
      lease_duration_ms: 30_000
    }

    assert {:ok, recovered} = Store.recover_expired(command)
    assert length(recovered) == count
    assert {:ok, ^recovered} = Store.recover_expired(%{command | occurred_at: DateTime.utc_now()})

    assert %{rows: [[receipt_size, snapshot_count, max_snapshot_size]]} =
             SQL.query!(
               Repo,
               """
               SELECT command_size,
                      snapshot_count,
                      max_snapshot_size
               FROM (
                 SELECT pg_column_size(result) AS command_size
                 FROM favn_control.runner_task_commands
                 WHERE scope_id = 'platform:runner_tasks' AND command_id = $1
               ) AS command
               CROSS JOIN (
                 SELECT count(*) AS snapshot_count,
                        max(octet_length(snapshot)) AS max_snapshot_size
                 FROM favn_control.runner_task_command_tasks
                 WHERE scope_id = 'platform:runner_tasks' AND command_id = $1
               ) AS snapshots
               """,
               [command.command_id]
             )

    assert receipt_size <= 262_144
    assert snapshot_count == count
    assert max_snapshot_size <= 262_144
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

    repair_command = %C.ReconcileRunnerCapacityDemand{
      platform_context: fixture.platform_context,
      command_id: platform_command_id(fixture, "reconcile-demand"),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      mode: :repair,
      issued_at: fixture.now,
      occurred_at: fixture.now
    }

    assert {:ok, repaired} = Store.reconcile_demand(repair_command)

    assert repaired.healthy?
    assert repaired.outstanding_count == 1
    assert repaired.queued_count == 1
    assert repaired.oldest_queued_at == queued.enqueued_at
    assert {:ok, ^repaired} = demand(fixture)

    assert {:ok, _claimed} =
             Store.claim(claim_command(fixture, "claim-after-reconcile", "reconcile-runner"))

    assert {:ok, ^repaired} = Store.reconcile_demand(repair_command)
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
      issued_at: fixture.now,
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
            issued_at: DateTime.add(fixture.now, index, :microsecond),
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

  @tag :slow
  @tag timeout: 360_000
  test "a cold distributed BEAM runner reaches Started within five minutes", fixture do
    start_runner_control_plane()
    version = manifest_version("mv-cold-runner-#{random_id()}", fixture.runner_pool)

    assert {:ok, ^version} =
             RegistryStore.register_manifest(%C.RegisterManifest{
               platform_context: fixture.platform_context,
               version: version
             })

    payload = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: @release,
      include: [:columns],
      sample_limit: 0
    }

    assert {:ok, queued} =
             Store.enqueue(enqueue_command(fixture, "cold-work", payload: payload))

    telemetry_handler = {__MODULE__, :cold_runner_started, make_ref()}

    :ok =
      :telemetry.attach(
        telemetry_handler,
        [:favn, :runner_task, :started],
        fn _event, measurements, metadata, owner ->
          send(owner, {:runner_task_started, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(telemetry_handler) end)
    runnable_at = System.monotonic_time(:millisecond)

    with_distributed_peer(fn peer ->
      runner_id = "actual-cold-runner-#{random_id()}"
      assert {:ok, _started} = start_actual_runner(peer, node(), runner_id, fixture.runner_pool)

      assert_receive {:runner_task_started, %{count: 1},
                      %{
                        task_id: task_id,
                        runner_instance_id: ^runner_id,
                        runner_pool: runner_pool
                      }},
                     300_000

      latency_ms = System.monotonic_time(:millisecond) - runnable_at

      assert task_id == queued.task_id
      assert runner_pool == fixture.runner_pool
      assert RunnerRegistry.count(fixture.runner_pool, @release) == 1
      assert latency_ms < 300_000
      assert_eventually(fn -> :sys.get_state(RunnerGateway).pending == %{} end)

      agent = :peer.call(peer, Process, :whereis, [FavnRunner.RunnerAgent], 30_000)
      assert is_pid(agent)

      assert {:message_queue_len, agent_mailbox} =
               :peer.call(peer, Process, :info, [
                 agent,
                 :message_queue_len
               ])

      assert agent_mailbox <= 16

      assert %{count: buffered_logs, dropped: dropped_logs} =
               :peer.call(peer, FavnRunner.TaskResultBuffer, :stats, [], 30_000)

      assert buffered_logs <= 50
      assert dropped_logs == 0

      IO.puts(
        "elastic runner actual cold-start latency_ms=#{latency_ms} " <>
          "agent_mailbox=#{agent_mailbox} buffered_logs=#{buffered_logs}"
      )
    end)
  end

  @tag :slow
  @tag timeout: 360_000
  test "three pools preserve exact demand through 0, 1, 10, and 100 distributed runners",
       fixture do
    tasks_per_pool = 1_000
    runner_counts = [0, 1, 10, 100]

    pool_fixtures =
      ~w(pure_elixir duckdb private_network)
      |> Enum.with_index(1)
      |> Enum.map(fn {pool, index} ->
        %{fixture | runner_pool: "#{pool}_#{index}_#{random_id()}"}
      end)

    Application.put_env(
      :favn_orchestrator,
      :runner_pools,
      Map.new(pool_fixtures, &{&1.runner_pool, %{mode: :elastic}})
    )

    Enum.each(pool_fixtures, fn pool_fixture ->
      1..tasks_per_pool
      |> Task.async_stream(
        fn index ->
          Store.enqueue(
            enqueue_command(pool_fixture, "scale-#{pool_fixture.runner_pool}-#{index}")
          )
        end,
        max_concurrency: 16,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.each(fn result -> assert {:ok, {:ok, _task}} = result end)
    end)

    start_runner_control_plane(max_concurrency: 384)

    Enum.each(pool_fixtures, fn pool_fixture ->
      assert RunnerRegistry.count(pool_fixture.runner_pool, @release) == 0
      assert {:ok, %{queued_count: ^tasks_per_pool, active_count: 0}} = demand(pool_fixture)
    end)

    with_distributed_peer(fn peer ->
      gateway = {RunnerGateway, node()}

      {_claimed, agents, samples} =
        Enum.reduce(runner_counts, {0, [], []}, fn
          0, accumulator ->
            accumulator

          runner_count, {previously_claimed, agents, samples} ->
            cohort_ref = make_ref()
            runnable_at = System.monotonic_time(:millisecond)

            cohort_agents =
              Enum.flat_map(pool_fixtures, fn pool_fixture ->
                spawn_distributed_agents(
                  peer,
                  runner_count,
                  self(),
                  cohort_ref,
                  gateway,
                  pool_fixture.runner_pool,
                  "#{runner_count}"
                )
              end)

            cohort_samples =
              await_started(cohort_ref, runner_count * length(pool_fixtures), runnable_at)

            assert length(Enum.uniq_by(cohort_samples, & &1.task_id)) ==
                     length(cohort_samples)

            assert Enum.all?(cohort_samples, &(&1.mailbox_len <= 4))
            assert_eventually(fn -> :sys.get_state(RunnerGateway).pending == %{} end)

            claimed_count = previously_claimed + runner_count

            Enum.each(pool_fixtures, fn pool_fixture ->
              assert RunnerRegistry.count(pool_fixture.runner_pool, @release) == claimed_count

              assert {:ok,
                      %{
                        outstanding_count: ^tasks_per_pool,
                        queued_count: queued_count,
                        active_count: ^claimed_count,
                        healthy?: true
                      }} = demand(pool_fixture)

              assert queued_count == tasks_per_pool - claimed_count
            end)

            {claimed_count, agents ++ cohort_agents, samples ++ cohort_samples}
        end)

      p95_ms =
        samples
        |> Enum.map(& &1.latency_ms)
        |> Enum.sort()
        |> Enum.at(floor(length(samples) * 0.95) - 1)

      assert p95_ms < 300_000

      assert {:message_queue_len, gateway_mailbox} =
               Process.info(Process.whereis(RunnerGateway), :message_queue_len)

      assert {:message_queue_len, registry_mailbox} =
               Process.info(Process.whereis(RunnerRegistry), :message_queue_len)

      assert gateway_mailbox <= 16
      assert registry_mailbox <= 16
      Enum.each(agents, &send(&1, :stop))

      IO.puts(
        "elastic runner distributed scale runners=#{length(samples)} p95_ms=#{p95_ms} " <>
          "gateway_mailbox=#{gateway_mailbox} registry_mailbox=#{registry_mailbox}"
      )
    end)
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

  test "expired assignment recovery uses its global lease-order index", fixture do
    assert {:ok, _task} = Store.enqueue(enqueue_command(fixture, "explain-recovery"))

    assert {:ok, assigned} =
             Store.claim(
               claim_command(fixture, "explain-recovery", "recovery-index-runner",
                 lease_duration_ms: 1
               )
             )

    plan =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL plan_cache_mode = force_generic_plan", [])
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])
        SQL.query!(Repo, "SET LOCAL enable_sort = off", [])

        SQL.query!(
          Repo,
          """
          EXPLAIN (FORMAT TEXT)
          SELECT *
          FROM favn_control.runner_tasks
          WHERE status = ANY($2::text[])
            AND assignment_expires_at <= $1
          ORDER BY assignment_expires_at, workspace_id, task_id
          LIMIT 50
          FOR UPDATE SKIP LOCKED
          """,
          [
            DateTime.add(fixture.now, 1, :second),
            ["assigned", "preparing", "running", "cancelling"]
          ]
        )
      end)
      |> then(fn {:ok, %{rows: rows}} -> rows |> List.flatten() |> Enum.join("\n") end)

    assert plan =~ "runner_tasks_expired_idx"

    assert %{rows: [[index_definition]]} =
             SQL.query!(
               Repo,
               """
               SELECT indexdef
               FROM pg_indexes
               WHERE schemaname = 'favn_control'
                 AND indexname = 'runner_tasks_expired_idx'
               """,
               []
             )

    refute index_definition =~ " WHERE "

    assert {:ok, %{status: :queued}} =
             Store.release(
               release_command(
                 fixture,
                 assigned,
                 "release-explain-recovery",
                 :requeue,
                 :index_test_cleanup
               )
             )
  end

  defp enqueue_command(fixture, suffix, opts \\ []) do
    task_kind = Keyword.get(opts, :task_kind, :relation_inspection)
    payload = Keyword.get(opts, :payload, inspection_payload())
    {:ok, encoded_payload, payload_hash} = Codec.encode_payload(task_kind, payload)

    {:ok, orchestration_context} =
      Codec.encode_orchestration_context(
        Keyword.get(opts, :orchestration_context, %{kind: :test})
      )

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
      issued_at: occurred_at,
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

  defp start_runner_control_plane(opts \\ []) do
    start_runner_registry()

    case Process.whereis(FavnOrchestrator.RunnerClaimSupervisor) do
      nil ->
        start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})

      _pid ->
        :ok
    end

    case Process.whereis(RunnerGateway) do
      nil -> start_supervised!({RunnerGateway, opts})
      _pid -> :ok
    end
  end

  defp with_distributed_peer(fun) do
    with_distribution(fn ->
      peer_name =
        "favn_storage_runner_#{System.unique_integer([:positive])}"
        |> String.to_charlist()

      assert {:ok, peer, _peer_node} =
               :peer.start_link(%{
                 name: peer_name,
                 host: ~c"127.0.0.1",
                 longnames: true,
                 connection: :standard_io,
                 wait_boot: 30_000
               })

      try do
        assert :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()], 30_000)
        configure_peer_control_plane_host(peer, node())
        load_remote_module(peer, DistributedRunnerAgent)
        fun.(peer)
      after
        if Process.alive?(peer), do: :peer.stop(peer)
      end
    end)
  end

  defp load_remote_module(peer, module) do
    assert {^module, binary, filename} = :code.get_object_code(module)
    assert {:module, ^module} = :peer.call(peer, :code, :load_binary, [module, filename, binary])
  end

  defp start_actual_runner(peer, control_plane_node, runner_id, runner_pool) do
    environment = %{
      "FAVN_CONTROL_PLANE_NODE" => Atom.to_string(control_plane_node),
      "FAVN_RUNNER_RELEASE_ID" => @release,
      "FAVN_RUNNER_BUILD_PROFILE" => "source",
      "FAVN_RUNNER_POOL" => runner_pool,
      "FAVN_RUNNER_INSTANCE_ID" => runner_id,
      "FAVN_RUNNER_LIFECYCLE_MODE" => "elastic"
    }

    assert :ok = :peer.call(peer, System, :put_env, [environment], 30_000)
    :peer.call(peer, Application, :ensure_all_started, [:favn_runner], 120_000)
  end

  defp configure_peer_control_plane_host(peer, control_plane_node) do
    [_, host] =
      control_plane_node
      |> Atom.to_string()
      |> String.split("@", parts: 2)

    host = String.to_charlist(host)
    assert {:ok, address} = :inet.getaddr(host, :inet)
    lookup = :peer.call(peer, :inet_db, :res_option, [:lookup], 30_000)
    assert is_list(lookup)

    assert :ok =
             :peer.call(peer, :inet_db, :set_lookup, [Enum.uniq([:file | lookup])], 30_000)

    assert :ok = :peer.call(peer, :inet_db, :add_host, [address, [host]], 30_000)
  end

  defp with_distribution(fun) do
    started_distribution? = not Node.alive?()
    original_lookup = :inet_db.res_option(:lookup)

    try do
      if started_distribution? do
        assert {_, 0} = System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)

        assert :ok = :inet_db.set_lookup(Enum.uniq([:file | original_lookup]))
        assert :ok = :inet_db.add_host(@distributed_test_address, [@distributed_test_host])

        local_name = "favn_storage_runner_test_#{System.unique_integer([:positive])}"
        client_name = String.to_atom(local_name <> "@" <> List.to_string(@distributed_test_host))

        assert {:ok, _pid} = Node.start(client_name, name_domain: :longnames)
      end

      fun.()
    after
      if started_distribution? do
        if Node.alive?(), do: Node.stop()
        :ok = :inet_db.del_host(@distributed_test_address)
        :ok = :inet_db.set_lookup(original_lookup)
      end
    end
  end

  defp spawn_distributed_agents(
         peer,
         count,
         owner,
         cohort_ref,
         gateway,
         runner_pool,
         cohort
       ) do
    Enum.map(1..count, fn index ->
      runner_id =
        "runner-#{cohort}-#{index}-#{random_id()}"

      :peer.call(
        peer,
        :erlang,
        :spawn,
        [
          DistributedRunnerAgent,
          :claim_and_start,
          [owner, cohort_ref, gateway, runner_id, runner_pool, @release]
        ],
        30_000
      )
    end)
  end

  defp await_started(cohort_ref, count, runnable_at),
    do: await_started(cohort_ref, count, runnable_at, [])

  defp await_started(_cohort_ref, 0, _runnable_at, samples), do: Enum.reverse(samples)

  defp await_started(cohort_ref, remaining, runnable_at, samples) do
    receive do
      {:distributed_runner_started, ^cohort_ref, agent, runner_id, task_id, runner_pool,
       mailbox_len} ->
        sample = %{
          agent: agent,
          runner_id: runner_id,
          task_id: task_id,
          runner_pool: runner_pool,
          mailbox_len: mailbox_len,
          latency_ms: System.monotonic_time(:millisecond) - runnable_at
        }

        await_started(cohort_ref, remaining - 1, runnable_at, [sample | samples])

      {:distributed_runner_failed, ^cohort_ref, runner_id, failure} ->
        flunk("distributed runner #{runner_id} failed: #{inspect(failure)}")
    after
      300_000 ->
        flunk("timed out waiting for #{remaining} distributed runners to reach Started")
    end
  end

  defp assert_eventually(fun, attempts \\ 200)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp cancellation_run(fixture, task_id) do
    RunState.new(
      id: "run-cancel:" <> task_id,
      workspace_id: fixture.workspace_id,
      manifest_version_id: "mv_runner_task",
      manifest_content_hash: String.duplicate("a", 64),
      runner_releases: %{"default" => @release},
      asset_ref: {MyApp.RunnerTaskCancellation, :asset},
      metadata: %{active_runner_task_ids: [task_id]}
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
              issued_at: cancellation.requested_at,
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
      issued_at:
        Keyword.get(
          opts,
          :issued_at,
          Keyword.get(opts, :occurred_at, DateTime.add(fixture.now, 1, :second))
        ),
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
      issued_at: DateTime.add(fixture.now, 2, :second),
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
      issued_at: DateTime.add(fixture.now, 2, :second),
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
      issued_at: DateTime.add(fixture.now, 3, :second),
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

  defp explain_text(%{rows: rows}), do: rows |> List.flatten() |> Enum.join("\n")

  defp purge_expired_assignments! do
    SQL.query!(Repo, """
    DELETE FROM favn_control.runner_task_command_tasks c
    USING favn_control.runner_tasks t
    WHERE c.workspace_id = t.workspace_id
      AND c.task_id = t.task_id
      AND t.assignment_expires_at < now()
    """)

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.runner_tasks WHERE assignment_expires_at < now()"
    )

    :ok
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

  defp manifest_version(manifest_version_id, runner_pool) do
    manifest = %Manifest{
      metadata: %{"fixture_id" => manifest_version_id},
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.DistributedRunnerAsset, :asset},
          module: MyApp.DistributedRunnerAsset,
          name: :asset,
          runner_pool: runner_pool
        }
      ],
      pipelines: []
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_contract(%{runner_pool => @release})
        |> FavnTestSupport.with_manifest_graph(),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp random_id,
    do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

  defp incompressible_text(bytes) do
    bytes
    |> Kernel.*(3)
    |> div(4)
    |> :crypto.strong_rand_bytes()
    |> Base.encode64()
    |> binary_part(0, bytes)
  end
end
