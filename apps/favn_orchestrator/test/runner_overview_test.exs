defmodule FavnOrchestrator.RunnerOverviewTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Results.RunnerSession
  alias FavnOrchestrator.Persistence.Results.RunnerSessionWindowTotals
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Results.WorkspaceRunnerTaskStats
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerOverview

  defmodule Store do
    def workspace_task_stats(query) do
      send(Process.get(:runner_overview_test_pid), {:workspace_task_stats, query})

      {:ok,
       Process.get(:runner_overview_stats, %WorkspaceRunnerTaskStats{
         queued_count: 0,
         active_count: 0,
         failed_count: 0,
         oldest_queued_at: nil
       })}
    end

    def page_sessions(query) do
      send(Process.get(:runner_overview_test_pid), {:page_sessions, query})
      {:ok, Process.get(:runner_overview_sessions, [])}
    end

    def session_window_totals(query) do
      send(Process.get(:runner_overview_test_pid), {:session_window_totals, query})

      {:ok,
       Process.get(:runner_overview_totals, %RunnerSessionWindowTotals{
         session_count: 0,
         awake_ms: 0,
         busy_ms: 0
       })}
    end

    def list_demands(query) do
      send(Process.get(:runner_overview_test_pid), {:list_demands, query})
      {:ok, Process.get(:runner_overview_demands, [])}
    end

    def page_session_tasks(query) do
      send(Process.get(:runner_overview_test_pid), {:page_session_tasks, query})
      {:ok, Process.get(:runner_overview_session_tasks, [])}
    end
  end

  setup do
    Process.put(:runner_overview_test_pid, self())
    stores = struct(Stores, runner_tasks: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    {:ok, context} = WorkspaceContext.new("workspace-runners", "operator", [:workspace_admin])
    %{context: context, now: DateTime.utc_now()}
  end

  test "returns capacity, stats, totals, and windows the failure count", %{context: context} do
    Process.put(:runner_overview_stats, %WorkspaceRunnerTaskStats{
      queued_count: 2,
      active_count: 1,
      failed_count: 3,
      oldest_queued_at: ~U[2026-09-01 07:00:00Z]
    })

    Process.put(:runner_overview_totals, %RunnerSessionWindowTotals{
      session_count: 4,
      awake_ms: 10_000,
      busy_ms: 4_000
    })

    Process.put(:runner_overview_demands, [
      %RunnerCapacityDemand{
        runner_pool: "default",
        required_runner_release_id: "rr_release",
        outstanding_count: 3,
        queued_count: 2,
        active_count: 1,
        oldest_queued_at: ~U[2026-09-01 07:00:00Z],
        healthy?: true
      }
    ])

    window_start = ~U[2026-09-01 00:00:00Z]

    assert {:ok, overview} =
             RunnerOverview.get(context, overlapping_after: window_start, limit: 25)

    assert overview.workspace_tasks.queued_count == 2
    assert overview.workspace_tasks.failed_count == 3
    assert overview.workspace_tasks.failed_since == window_start

    assert [capacity] = overview.capacity
    assert capacity.runner_pool == "default"
    assert capacity.queued_count == 2
    assert capacity.connected_runner_count == 0

    assert overview.totals.awake_ms == 10_000
    assert overview.totals.busy_ms == 4_000
    assert overview.totals.idle_ms == 6_000
    assert overview.totals.window_start == window_start

    assert_receive {:workspace_task_stats, stats_query}
    assert stats_query.failed_since == window_start

    assert_receive {:page_sessions, sessions_query}
    assert sessions_query.overlapping_after == window_start
    assert sessions_query.limit == 25
    assert sessions_query.states == :all
  end

  test "merges resumed session rows into one displayed session", %{
    context: context,
    now: now
  } do
    Process.put(:runner_overview_sessions, [
      session_row(
        session_id: "rs_b",
        registered_at: DateTime.add(now, -60, :second),
        ended_at: nil,
        end_reason: nil
      ),
      session_row(
        session_id: "rs_a",
        registered_at: DateTime.add(now, -300, :second),
        ended_at: DateTime.add(now, -60, :second),
        end_reason: :presumed_dead,
        task_counts: %{succeeded: 5}
      )
    ])

    assert {:ok, %{sessions: [merged]}} = RunnerOverview.get(context)
    assert merged.kind == :session
    assert merged.state == :connected
    assert merged.registered_at == DateTime.add(now, -300, :second)
    assert is_nil(merged.ended_at)
    assert merged.task_counts == %{succeeded: 5}
    assert merged.row_count == 2
  end

  test "collapses short no-task sessions into a struggling group", %{
    context: context,
    now: now
  } do
    rows =
      for index <- 1..3 do
        session_row(
          session_id: "rs_#{index}",
          runner_instance_id: "runner-#{index}",
          runner_boot_id: "boot-#{index}",
          registered_at: DateTime.add(now, -600 + index * 60, :second),
          ended_at: DateTime.add(now, -590 + index * 60, :second),
          end_reason: :crashed
        )
      end

    Process.put(:runner_overview_sessions, rows)

    assert {:ok, %{sessions: [group]}} = RunnerOverview.get(context)
    assert group.kind == :struggling_group
    assert group.session_count == 3
    assert group.runner_pool == "default"

    assert {:ok, %{sessions: [^group]}} = RunnerOverview.get(context, states: [:struggling])
    assert {:ok, %{sessions: []}} = RunnerOverview.get(context, states: [:connected])
  end

  test "hides another workspace's interrupted task id but keeps its own", %{
    context: context,
    now: now
  } do
    Process.put(:runner_overview_sessions, [
      session_row(
        session_id: "rs_own",
        runner_instance_id: "runner-own",
        registered_at: DateTime.add(now, -600, :second),
        ended_at: DateTime.add(now, -100, :second),
        end_reason: :crashed,
        busy_at_exit: true,
        task_counts: %{unknown: 1},
        interrupted_task_workspace_id: "workspace-runners",
        interrupted_task_id: "rt_mine"
      ),
      session_row(
        session_id: "rs_foreign",
        runner_instance_id: "runner-foreign",
        registered_at: DateTime.add(now, -700, :second),
        ended_at: DateTime.add(now, -200, :second),
        end_reason: :crashed,
        busy_at_exit: true,
        task_counts: %{unknown: 1},
        interrupted_task_workspace_id: "workspace-other",
        interrupted_task_id: "rt_secret"
      )
    ])

    assert {:ok, %{sessions: sessions}} = RunnerOverview.get(context)
    by_instance = Map.new(sessions, &{&1.runner_instance_id, &1})

    assert by_instance["runner-own"].interrupted_task_id == "rt_mine"
    assert is_nil(by_instance["runner-foreign"].interrupted_task_id)
    assert by_instance["runner-foreign"].busy_at_exit
    refute Map.has_key?(by_instance["runner-foreign"], :interrupted_task_workspace_id)
  end

  test "rejects invalid options", %{context: context} do
    assert {:error, :invalid_runner_overview_options} = RunnerOverview.get(context, limit: 0)

    assert {:error, :invalid_runner_overview_options} =
             RunnerOverview.get(context, states: [:bogus])

    assert {:error, :invalid_runner_overview_options} =
             RunnerOverview.get(context, overlapping_after: "yesterday")
  end

  test "session tasks project exact redacted errors with DuckDB remediation", %{
    context: context,
    now: now
  } do
    Process.put(:runner_overview_session_tasks, [
      %RunnerTask{
        task_id: "task-inspection",
        task_kind: :relation_inspection,
        status: :failed,
        runner_pool: "duckdb",
        required_runner_release_id: "rr_release",
        retry_class: :safe,
        enqueued_at: now,
        terminal_at: now,
        error: %{
          "type" => "driver_unavailable",
          "phase" => "pre_submit",
          "message" => "failed to initialize DuckDB ADBC connection: driver unavailable",
          "retryable?" => false
        }
      }
    ])

    assert {:ok, [task]} =
             RunnerOverview.session_tasks(context,
               runner_instance_id: "runner-a",
               session_generation: 7,
               registered_at: now
             )

    assert task.failure.code == "driver_unavailable"
    assert task.failure.message =~ "DuckDB ADBC"
    assert task.failure.remediation =~ "DUCKDB_ADBC_DRIVER"

    assert_receive {:page_session_tasks, query}
    assert query.workspace_context.workspace_id == "workspace-runners"
    assert query.runner_instance_id == "runner-a"
    assert query.session_generation == 7
    assert query.statuses == [:failed, :unknown]
  end

  test "does not suggest DuckDB configuration for an unrelated driver failure", %{
    context: context,
    now: now
  } do
    Process.put(:runner_overview_session_tasks, [
      %RunnerTask{
        task_id: "task-browser-driver",
        task_kind: :relation_inspection,
        status: :failed,
        runner_pool: "browser",
        required_runner_release_id: "rr_release",
        retry_class: :safe,
        enqueued_at: now,
        error: %{"type" => "browser_driver_error", "message" => "browser driver unavailable"}
      }
    ])

    assert {:ok, [task]} =
             RunnerOverview.session_tasks(context,
               runner_instance_id: "runner-b",
               session_generation: 1,
               registered_at: now
             )

    refute task.failure.remediation =~ "DUCKDB_ADBC_DRIVER"
  end

  test "does not expose a foreign workspace active task id" do
    session = %{
      runner_instance_id: "runner-shared",
      beam_node: "runner@node",
      runner_pool: "duckdb",
      required_runner_release_id: "rr_release",
      protocol_version: 1,
      supported_task_kinds: [:relation_inspection],
      capabilities: [:duckdb],
      lifecycle_mode: :persistent,
      status: :busy,
      registered_at: DateTime.utc_now(),
      active_assignment: %{workspace_id: "workspace-other", task_id: "task-secret"}
    }

    foreign = RunnerOverview.project_runner(session, "workspace-runners")
    same_workspace = RunnerOverview.project_runner(session, "workspace-other")

    assert is_nil(foreign.active_task_id)
    assert same_workspace.active_task_id == "task-secret"
  end

  defp session_row(overrides) do
    defaults = [
      session_id: "rs_default",
      runner_instance_id: "runner-merged",
      runner_boot_id: "boot-merged",
      session_generation: 11,
      control_plane_boot_id: "cpb_test",
      runner_pool: "default",
      required_runner_release_id: "rr_release",
      beam_node: "runner@node",
      protocol_version: 13,
      lifecycle_mode: "elastic",
      busy_at_exit: false,
      task_counts: %{}
    ]

    struct!(RunnerSession, Keyword.merge(defaults, overrides))
  end
end
