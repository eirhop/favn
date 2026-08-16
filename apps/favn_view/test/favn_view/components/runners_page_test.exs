defmodule FavnView.Components.RunnersPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunnersPage

  test "keeps durable runner errors visible when no runner is connected" do
    now = ~U[2026-07-31 12:00:00Z]

    failed_task = %{
      task_id: "runner-task-inspection",
      task_kind: :relation_inspection,
      status: :failed,
      runner_pool: "duckdb",
      assigned_runner_instance_id: "runner-local",
      enqueued_at: now,
      failure: %{
        title: "Driver unavailable",
        message: "failed to initialize DuckDB ADBC connection: driver is unavailable",
        remediation: "Set DUCKDB_ADBC_DRIVER and restart the runner.",
        code: "driver_unavailable"
      }
    }

    html =
      render_component(&RunnersPage.runners_page/1,
        current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}},
        overview: %{
          runner_count: 0,
          registry_status: :available,
          runners: [],
          observed_at: now,
          tasks: [failed_task],
          failures: [failed_task]
        },
        loading: false,
        error: nil,
        nav_items: RunnersPage.nav_items()
      )

    assert html =~ "No runners connected"
    assert html =~ "failed to initialize DuckDB ADBC connection"
    assert html =~ "DUCKDB_ADBC_DRIVER"
    assert html =~ "driver_unavailable"
    assert html =~ ~s(data-testid="runners-refresh")
    assert html =~ ~s(phx-click="reload")
  end

  test "distinguishes an unavailable live registry from zero connected runners" do
    html =
      render_component(&RunnersPage.runners_page/1,
        current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}},
        overview: %{
          runner_count: 0,
          registry_status: :unavailable,
          runners: [],
          observed_at: DateTime.utc_now(),
          tasks: [],
          failures: []
        },
        loading: false,
        error: nil,
        nav_items: RunnersPage.nav_items()
      )

    assert html =~ "Runner registry unavailable"
    assert html =~ "Durable task history remains available"
    refute html =~ "No runners connected"
  end

  test "renders loading and error page states" do
    loading =
      render_component(&RunnersPage.runners_page/1,
        current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}},
        loading: true,
        nav_items: RunnersPage.nav_items()
      )

    assert loading =~ "Loading runner diagnostics"

    error =
      render_component(&RunnersPage.runners_page/1,
        current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}},
        error: "Backend unavailable",
        nav_items: RunnersPage.nav_items()
      )

    assert error =~ "Could not load runner diagnostics"
    assert error =~ "Backend unavailable"
  end
end
