defmodule FavnStoragePostgres.StorageV2.RunnerSessionsTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Results.RunnerSessionWindowTotals
  alias FavnOrchestrator.Persistence.Results.WorkspaceRunnerTaskStats
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.RunnerTasks.Store
  alias FavnStoragePostgres.StorageV2.Migrations

  @release "rr_" <> String.duplicate("c", 64)
  @boot "cpb_current_boot"
  @earlier_boot "cpb_earlier_boot"

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
    workspace_id = "runner-session-ws-#{unique}"
    runner_pool = "duckdb_#{unique}"
    now = DateTime.utc_now()

    {:ok, platform_context} =
      PlatformContext.new("runner-session-test", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%C.ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "runner-session-#{unique}",
        display_name: "Runner session #{unique}",
        occurred_at: now
      })

    {:ok, workspace_context} =
      WorkspaceContext.new(workspace_id, "runner-session-worker", [:workspace_admin],
        request_id: "request-#{unique}"
      )

    {:ok,
     unique: unique,
     workspace_id: workspace_id,
     runner_pool: runner_pool,
     workspace_context: workspace_context,
     platform_context: platform_context,
     now: now}
  end

  test "open is idempotent by session id and repairs an orphaned open row", fixture do
    runner = "runner-#{fixture.unique}"
    first = open_command(fixture, runner, session_generation: 7)

    assert {:ok, opened} = Store.open_session(first)
    assert opened.session_id == first.session_id
    assert is_nil(opened.ended_at)

    assert {:ok, retried} = Store.open_session(first)
    assert retried.session_id == first.session_id
    assert retried.registered_at == opened.registered_at

    second =
      open_command(fixture, runner,
        session_generation: 7,
        registered_at: DateTime.add(fixture.now, 30, :second)
      )

    assert {:ok, replacement} = Store.open_session(second)
    assert is_nil(replacement.ended_at)

    assert [newest, orphaned] =
             page!(fixture, [states: :all], [first.session_id, second.session_id])

    assert newest.session_id == second.session_id
    assert orphaned.session_id == first.session_id
    assert orphaned.end_reason == :presumed_dead
  end

  test "close records the reason once and never overwrites it", fixture do
    command = open_command(fixture, "runner-close-#{fixture.unique}")
    assert {:ok, _opened} = Store.open_session(command)

    ended_at = DateTime.add(fixture.now, 10, :second)

    assert {:ok, :closed} =
             Store.close_session(%C.CloseRunnerSession{
               platform_context: fixture.platform_context,
               session_id: command.session_id,
               ended_at: ended_at,
               end_reason: :crashed,
               busy_at_exit: true,
               interrupted_task_workspace_id: fixture.workspace_id,
               interrupted_task_id: "rt_interrupted"
             })

    assert {:ok, :already_closed} =
             Store.close_session(%C.CloseRunnerSession{
               platform_context: fixture.platform_context,
               session_id: command.session_id,
               ended_at: DateTime.add(ended_at, 5, :second),
               end_reason: :shut_down
             })

    assert {:ok, :not_found} =
             Store.close_session(%C.CloseRunnerSession{
               platform_context: fixture.platform_context,
               session_id: "rs_" <> String.duplicate("0", 32),
               ended_at: ended_at,
               end_reason: :shut_down
             })

    assert [session] = page!(fixture, [states: [:crashed]], [command.session_id])
    assert session.end_reason == :crashed
    assert session.busy_at_exit == true
    assert session.interrupted_task_id == "rt_interrupted"
  end

  test "boot reconciliation closes only rows from earlier boots", fixture do
    current = open_command(fixture, "runner-current-#{fixture.unique}")
    earlier = open_command(fixture, "runner-earlier-#{fixture.unique}", boot: @earlier_boot)

    assert {:ok, _current} = Store.open_session(current)
    assert {:ok, _earlier} = Store.open_session(earlier)

    assert {:ok, 1} =
             Store.reconcile_sessions(%C.ReconcileRunnerSessions{
               platform_context: fixture.platform_context,
               control_plane_boot_id: @boot,
               ended_at: DateTime.add(fixture.now, 60, :second)
             })

    sessions = page!(fixture, [states: :all], [current.session_id, earlier.session_id])
    by_id = Map.new(sessions, &{&1.session_id, &1})
    assert is_nil(by_id[current.session_id].ended_at)
    assert by_id[earlier.session_id].end_reason == :presumed_dead

    assert {:ok, 0} =
             Store.reconcile_sessions(%C.ReconcileRunnerSessions{
               platform_context: fixture.platform_context,
               control_plane_boot_id: @boot,
               ended_at: DateTime.add(fixture.now, 61, :second)
             })
  end

  test "prune deletes only closed sessions older than the cutoff", fixture do
    old =
      open_command(fixture, "runner-old-#{fixture.unique}",
        registered_at: DateTime.add(fixture.now, -101, :day)
      )

    fresh = open_command(fixture, "runner-fresh-#{fixture.unique}")
    open = open_command(fixture, "runner-open-#{fixture.unique}")

    for command <- [old, fresh, open], do: assert({:ok, _session} = Store.open_session(command))

    close!(fixture, old.session_id, DateTime.add(fixture.now, -100, :day))
    close!(fixture, fresh.session_id, DateTime.add(fixture.now, 5, :second))

    assert {:ok, pruned} =
             Store.prune_sessions(%C.PruneRunnerSessions{
               platform_context: fixture.platform_context,
               older_than: DateTime.add(fixture.now, -90, :day)
             })

    assert pruned >= 1
    assert {:ok, sessions} = page(fixture, states: :all, limit: 200)
    ids = Enum.map(sessions, & &1.session_id)
    refute old.session_id in ids
    assert fresh.session_id in ids
    assert open.session_id in ids
  end

  test "page filters by window overlap and states", fixture do
    ancient =
      open_command(fixture, "runner-ancient-#{fixture.unique}",
        registered_at: DateTime.add(fixture.now, -11, :day)
      )

    recent = open_command(fixture, "runner-recent-#{fixture.unique}")
    connected = open_command(fixture, "runner-live-#{fixture.unique}")

    for command <- [ancient, recent, connected],
        do: assert({:ok, _session} = Store.open_session(command))

    close!(fixture, ancient.session_id, DateTime.add(fixture.now, -10, :day), :crashed)
    close!(fixture, recent.session_id, DateTime.add(fixture.now, 5, :second), :shut_down)

    window_start = DateTime.add(fixture.now, -1, :day)

    assert {:ok, windowed} = page(fixture, overlapping_after: window_start)
    windowed_ids = Enum.map(windowed, & &1.session_id)
    refute ancient.session_id in windowed_ids
    assert recent.session_id in windowed_ids
    assert connected.session_id in windowed_ids

    test_ids = [ancient.session_id, recent.session_id, connected.session_id]

    assert [only_connected] = page!(fixture, [states: [:connected]], test_ids)
    assert only_connected.session_id == connected.session_id

    assert [only_crashed] = page!(fixture, [states: [:crashed]], test_ids)
    assert only_crashed.session_id == ancient.session_id

    assert {:error, %{kind: :invalid}} = page(fixture, states: [:bogus])
    assert {:error, %{kind: :invalid}} = page(fixture, limit: 0)
  end

  test "attributed task counts, session tasks, and busy totals follow the final assignment",
       fixture do
    generation = 41

    session =
      open_command(fixture, "runner-tasks-#{fixture.unique}", session_generation: generation)

    assert {:ok, _session} = Store.open_session(session)

    with_runner_pool(fixture.runner_pool, fn ->
      assert {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "attributed"))

      assert {:ok, claimed} =
               Store.claim(
                 claim_command(fixture, "claim-attributed", session.runner_instance_id,
                   runner_session_generation: generation
                 )
               )

      assert %DateTime{} = claimed.assigned_at
      assert claimed.assigned_runner_instance_id == session.runner_instance_id

      assert {:ok, running} =
               Store.transition(
                 transition_command(fixture, claimed, "start-attributed", :running)
               )

      result = %RelationInspectionResult{
        required_runner_release_id: @release,
        row_count: 1,
        inspected_at: DateTime.add(fixture.now, 4, :second)
      }

      assert {:ok, encoded_result} = Codec.encode_result(:relation_inspection, :succeeded, result)
      assert {:ok, completed} = Store.complete(complete_command(fixture, running, encoded_result))
      assert completed.status == :succeeded
      assert %DateTime{} = completed.assigned_at

      assert [paged] = page!(fixture, [states: [:connected]], [session.session_id])
      assert paged.session_id == session.session_id
      assert paged.task_counts == %{succeeded: 1}

      assert {:ok, [task]} =
               Store.page_session_tasks(%Q.PageRunnerSessionTasks{
                 workspace_context: fixture.workspace_context,
                 runner_instance_id: session.runner_instance_id,
                 session_generation: generation,
                 registered_at: session.registered_at,
                 statuses: [:succeeded]
               })

      assert task.task_id == completed.task_id

      window_start = DateTime.add(fixture.now, -1, :hour)
      window_end = DateTime.add(fixture.now, 1, :hour)

      assert {:ok, %RunnerSessionWindowTotals{} = totals} =
               Store.session_window_totals(%Q.GetRunnerSessionWindowTotals{
                 platform_context: fixture.platform_context,
                 window_start: window_start,
                 window_end: window_end
               })

      assert totals.session_count >= 1
      assert totals.awake_ms > 0
      assert totals.busy_ms > 0
      assert totals.busy_ms <= totals.awake_ms
    end)
  end

  test "assigned_at clears when a terminal failure is retried back to queued", fixture do
    with_runner_pool(fixture.runner_pool, fn ->
      assert {:ok, _queued} = Store.enqueue(enqueue_command(fixture, "retryable"))

      assert {:ok, claimed} =
               Store.claim(
                 claim_command(fixture, "claim-retryable", "runner-retry-#{fixture.unique}")
               )

      assert %DateTime{} = claimed.assigned_at

      assert {:ok, running} =
               Store.transition(transition_command(fixture, claimed, "start-retryable", :running))

      failure = %C.CompleteRunnerTask{
        workspace_context: fixture.workspace_context,
        command_id: "complete-retryable",
        task_id: running.task_id,
        runner_instance_id: running.assigned_runner_instance_id,
        runner_session_generation: running.assigned_runner_session_generation,
        assignment_generation: running.assignment_generation,
        result_version: 1,
        outcome: :failed,
        retry_class: :safe_to_retry,
        result: nil,
        error: Favn.Contracts.RunnerError.new(outcome: :safe_failure, retryable?: true),
        issued_at: DateTime.add(fixture.now, 3, :second),
        occurred_at: DateTime.add(fixture.now, 3, :second)
      }

      assert {:ok, failed} = Store.complete(failure)
      assert %DateTime{} = failed.assigned_at

      assert {:ok, requeued} =
               Store.retry(%C.RetryRunnerTask{
                 workspace_context: fixture.workspace_context,
                 command_id: "retry-retryable",
                 task_id: failed.task_id,
                 expected_assignment_generation: failed.assignment_generation,
                 expected_result_version: failed.result_version,
                 issued_at: DateTime.add(fixture.now, 4, :second),
                 occurred_at: DateTime.add(fixture.now, 4, :second)
               })

      assert requeued.status == :queued
      assert is_nil(requeued.assigned_at)
    end)
  end

  test "workspace task stats count queued, active, and windowed failures", fixture do
    with_runner_pool(fixture.runner_pool, fn ->
      assert {:ok, queued} = Store.enqueue(enqueue_command(fixture, "stats-queued"))
      assert {:ok, _active} = Store.enqueue(enqueue_command(fixture, "stats-active"))

      assert {:ok, _claimed} =
               Store.claim(
                 claim_command(fixture, "claim-stats", "runner-stats-#{fixture.unique}")
               )

      assert {:ok, %WorkspaceRunnerTaskStats{} = stats} =
               Store.workspace_task_stats(%Q.GetWorkspaceRunnerTaskStats{
                 workspace_context: fixture.workspace_context,
                 failed_since: DateTime.add(fixture.now, -1, :day)
               })

      assert stats.queued_count == 1
      assert stats.active_count == 1
      assert stats.failed_count == 0
      assert stats.oldest_queued_at == queued.enqueued_at
    end)
  end

  defp page(fixture, opts) do
    Store.page_sessions(%Q.PageRunnerSessions{
      platform_context: fixture.platform_context,
      overlapping_after: Keyword.get(opts, :overlapping_after),
      states: Keyword.get(opts, :states, :all),
      limit: Keyword.get(opts, :limit, 50)
    })
  end

  defp page!(fixture, opts, session_ids) do
    assert {:ok, sessions} = page(fixture, Keyword.put(opts, :limit, 200))
    Enum.filter(sessions, &(&1.session_id in session_ids))
  end

  defp close!(fixture, session_id, ended_at, end_reason \\ :shut_down) do
    assert {:ok, :closed} =
             Store.close_session(%C.CloseRunnerSession{
               platform_context: fixture.platform_context,
               session_id: session_id,
               ended_at: ended_at,
               end_reason: end_reason
             })
  end

  defp open_command(fixture, runner_instance_id, opts \\ []) do
    %C.OpenRunnerSession{
      platform_context: fixture.platform_context,
      session_id: "rs_" <> (:crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)),
      runner_instance_id: runner_instance_id,
      runner_boot_id: Keyword.get(opts, :runner_boot_id, "boot-#{fixture.unique}"),
      session_generation: Keyword.get(opts, :session_generation, 1),
      control_plane_boot_id: Keyword.get(opts, :boot, @boot),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      beam_node: "runner@favn.test",
      protocol_version: 13,
      lifecycle_mode: :elastic,
      registered_at: Keyword.get(opts, :registered_at, fixture.now)
    }
  end

  defp with_runner_pool(runner_pool, fun) do
    previous = Application.get_env(:favn_orchestrator, :runner_pools)
    Application.put_env(:favn_orchestrator, :runner_pools, %{runner_pool => %{mode: :elastic}})

    try do
      fun.()
    after
      if is_nil(previous),
        do: Application.delete_env(:favn_orchestrator, :runner_pools),
        else: Application.put_env(:favn_orchestrator, :runner_pools, previous)
    end
  end

  defp enqueue_command(fixture, suffix) do
    payload = %RelationInspectionRequest{
      manifest_version_id: "mv_runner_session",
      required_runner_release_id: @release,
      include: [:columns],
      sample_limit: 0
    }

    {:ok, encoded_payload, payload_hash} = Codec.encode_payload(:relation_inspection, payload)
    {:ok, orchestration_context} = Codec.encode_orchestration_context(%{kind: :test})

    %C.EnqueueRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "enqueue-#{suffix}",
      task_id: "rt_#{random_id()}",
      domain_identity: "runner-session-domain-#{suffix}-#{random_id()}",
      task_kind: :relation_inspection,
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      retry_class: Favn.Contracts.RunnerTask.default_retry_class(:relation_inspection),
      payload: encoded_payload,
      payload_hash: payload_hash,
      orchestration_context: orchestration_context,
      run_id: "run-#{suffix}",
      operation_id: nil,
      asset_step_id: nil,
      required_capability: "relation_inspection",
      deadline_at: DateTime.add(fixture.now, 60, :second),
      issued_at: fixture.now,
      occurred_at: fixture.now
    }
  end

  defp claim_command(fixture, suffix, runner_instance_id, opts \\ []) do
    %C.ClaimRunnerTask{
      platform_context: fixture.platform_context,
      command_id: "#{fixture.workspace_id}:claim-#{suffix}",
      runner_instance_id: runner_instance_id,
      runner_session_generation: Keyword.get(opts, :runner_session_generation, 1),
      runner_pool: fixture.runner_pool,
      required_runner_release_id: @release,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"],
      lease_duration_ms: 600_000,
      issued_at: DateTime.add(fixture.now, 1, :second),
      occurred_at: DateTime.add(fixture.now, 1, :second)
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

  defp complete_command(fixture, task, result) do
    %C.CompleteRunnerTask{
      workspace_context: fixture.workspace_context,
      command_id: "complete-#{task.task_id}",
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

  defp random_id, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
