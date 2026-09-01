defmodule FavnView.Components.RunnersPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunnersPage

  @now ~U[2026-09-01 12:00:00Z]
  @scope %{workspace_configuration: %{default_timezone: "Etc/UTC"}}

  test "shows capacity starvation and grouped stats when work waits with no runner" do
    html = render_page(overview: overview())

    assert html =~ ~s(data-testid="runners-stat-header")
    assert html =~ "Workspace tasks"
    assert html =~ "Longest wait"

    assert html =~ ~s(data-testid="runners-starved-notice")
    assert html =~ "Work is waiting with no compatible runner connected."
    assert html =~ "2 tasks queued for default · rr_9f2c"

    assert html =~ ~s(data-testid="runner-capacity-row")
    assert html =~ "Oldest wait"

    assert html =~ "No runners connected"
    assert html =~ ~s(data-testid="runners-refresh")
  end

  test "renders sessions with honest ages, states, and busy totals" do
    html = render_page(overview: overview())

    assert html =~ ~s(data-testid="runner-session-row")
    assert html =~ "Crashed"
    assert html =~ "Connected"
    assert html =~ "Aug 20"
    assert html =~ "1 task interrupted (rt_crash) — outcome unknown."

    assert html =~ ~s(data-testid="runner-session-totals")
    assert html =~ "busy 1 h"
    assert html =~ "Busy time covers completed final assignments."

    assert html =~ ~s(phx-click="set_state")
    assert html =~ ~s(phx-click="set_window")
    assert html =~ ~s(data-testid="scope-crashed")
    assert html =~ ~s(data-testid="scope-week")

    assert html =~ ~s(data-testid="runner-session-tasks-toggle")
    assert html =~ ~s(phx-value-instance="runner-crashed")
  end

  test "hides a foreign workspace interrupted task id but reports the interruption" do
    session =
      session_entry(
        runner_instance_id: "runner-foreign",
        interrupted_task_id: nil,
        busy_at_exit: true
      )

    html = render_page(overview: %{overview() | sessions: [session]})

    assert html =~ "1 task interrupted in another workspace — outcome unknown."
    refute html =~ "rt_crash"
  end

  test "collapses a crash loop into one struggling group" do
    group = %{
      kind: :struggling_group,
      runner_pool: "default",
      required_runner_release_id: "rr_9f2c",
      session_count: 3,
      first_registered_at: ~U[2026-08-20 14:21:00Z],
      last_registered_at: ~U[2026-08-20 14:29:00Z],
      sessions: [
        session_entry(runner_instance_id: "runner-loop", task_counts: %{}, busy_at_exit: false)
      ]
    }

    html = render_page(overview: %{overview() | sessions: [group]})

    assert html =~ ~s(data-testid="runner-struggling-group")
    assert html =~ "Struggling to start"
    assert html =~ "3 sessions in 8 m"
    assert html =~ "Each session ended shortly after registering"
  end

  test "renders expanded session tasks with the durable error detail" do
    task = %{
      task_id: "rt_task",
      task_kind: :relation_inspection,
      status: :failed,
      run_id: "run-1",
      enqueued_at: @now,
      assigned_at: ~U[2026-09-01 11:59:00Z],
      terminal_at: @now,
      failure: %{
        title: "Driver unavailable",
        message: "failed to initialize DuckDB ADBC connection: driver is unavailable",
        remediation: "Set DUCKDB_ADBC_DRIVER and restart the runner.",
        code: "driver_unavailable"
      }
    }

    session = session_entry([])
    key = "runner-crashed:41:2026-08-20T14:33:24Z"

    html = render_page(overview: %{overview() | sessions: [session]}, expanded: %{key => [task]})

    assert html =~ ~s(data-testid="runner-task-row")
    assert html =~ "failed to initialize DuckDB ADBC connection"
    assert html =~ "DUCKDB_ADBC_DRIVER"
    assert html =~ "driver_unavailable"
    assert html =~ "Hide failed tasks"
    assert html =~ "1 m"

    unavailable =
      render_page(overview: %{overview() | sessions: [session]}, expanded: %{key => :unavailable})

    assert unavailable =~ ~s(data-testid="runner-session-tasks-error")
  end

  test "distinguishes an unavailable live registry from zero connected runners" do
    html =
      render_page(
        overview: %{overview() | registry_status: :unavailable, runners: [], runner_count: 0}
      )

    assert html =~ "Runner registry unavailable"
    assert html =~ "Durable session"
    refute html =~ "No runners connected"
  end

  test "renders loading and error page states" do
    loading =
      render_component(&RunnersPage.runners_page/1,
        current_scope: @scope,
        loading: true,
        nav_items: RunnersPage.nav_items()
      )

    assert loading =~ "Loading runner diagnostics"

    error =
      render_component(&RunnersPage.runners_page/1,
        current_scope: @scope,
        error: "Backend unavailable",
        nav_items: RunnersPage.nav_items()
      )

    assert error =~ "Could not load runner diagnostics"
    assert error =~ "Backend unavailable"
  end

  defp render_page(overrides) do
    render_component(
      &RunnersPage.runners_page/1,
      Keyword.merge(
        [
          current_scope: @scope,
          loading: false,
          error: nil,
          window: :week,
          state: :all,
          expanded: %{},
          nav_items: RunnersPage.nav_items()
        ],
        overrides
      )
    )
  end

  defp overview do
    %{
      registry_status: :available,
      runners: [],
      runner_count: 0,
      busy_runner_count: 0,
      capacity: [
        %{
          runner_pool: "default",
          required_runner_release_id: "rr_9f2c",
          queued_count: 2,
          active_count: 0,
          oldest_queued_at: ~U[2026-09-01 11:19:00Z],
          connected_runner_count: 0,
          healthy?: true
        }
      ],
      workspace_tasks: %{
        queued_count: 2,
        active_count: 0,
        failed_count: 0,
        failed_since: ~U[2026-08-25 12:00:00Z],
        oldest_queued_at: ~U[2026-09-01 11:19:00Z]
      },
      sessions: [
        session_entry([]),
        session_entry(
          runner_instance_id: "runner-live",
          state: :connected,
          ended_at: nil,
          busy_at_exit: false,
          interrupted_task_id: nil,
          task_counts: %{succeeded: 38}
        )
      ],
      totals: %{
        window_start: ~U[2026-08-25 12:00:00Z],
        window_end: @now,
        session_count: 2,
        awake_ms: 4 * 3_600_000,
        busy_ms: 3_600_000,
        idle_ms: 3 * 3_600_000
      },
      observed_at: @now
    }
  end

  defp session_entry(overrides) do
    Enum.into(overrides, %{
      kind: :session,
      runner_instance_id: "runner-crashed",
      runner_boot_id: "boot-crashed",
      session_generation: 41,
      runner_pool: "default",
      required_runner_release_id: "rr_9f2c",
      beam_node: "runner@node",
      protocol_version: 13,
      lifecycle_mode: "elastic",
      registered_at: ~U[2026-08-20 14:33:24Z],
      ended_at: ~U[2026-08-20 14:36:36Z],
      state: :crashed,
      busy_at_exit: true,
      interrupted_task_id: "rt_crash",
      task_counts: %{unknown: 1},
      row_count: 1
    })
  end
end
