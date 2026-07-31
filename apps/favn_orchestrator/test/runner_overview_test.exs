defmodule FavnOrchestrator.RunnerOverviewTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerOverview

  defmodule Store do
    def page_workspace(query) do
      send(Process.get(:runner_overview_test_pid), {:page_workspace, query})

      tasks =
        if query.statuses == [:failed, :unknown],
          do: Process.get(:runner_overview_failures, []),
          else: Process.get(:runner_overview_tasks, [])

      {:ok, tasks}
    end
  end

  setup do
    Process.put(:runner_overview_test_pid, self())
    stores = struct(Stores, runner_tasks: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    {:ok, context} = WorkspaceContext.new("workspace-runners", "operator", [:workspace_admin])
    %{context: context}
  end

  test "returns exact redacted task errors with DuckDB remediation", %{context: context} do
    now = DateTime.utc_now()

    failed_task =
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

    Process.put(:runner_overview_tasks, [failed_task])
    Process.put(:runner_overview_failures, [failed_task])

    assert {:ok, %{tasks: [task], failures: [failure]}} =
             RunnerOverview.get(context, limit: 25)

    assert task.failure.code == "driver_unavailable"
    assert task.failure.message =~ "DuckDB ADBC"
    assert task.failure.remediation =~ "DUCKDB_ADBC_DRIVER"
    assert failure.task_id == task.task_id

    assert_receive {:page_workspace, recent_query}
    assert recent_query.workspace_context.workspace_id == "workspace-runners"
    assert recent_query.statuses == :all
    assert recent_query.limit == 25

    assert_receive {:page_workspace, failure_query}
    assert failure_query.workspace_context.workspace_id == "workspace-runners"
    assert failure_query.statuses == [:failed, :unknown]
    assert failure_query.limit == 20
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

  test "does not suggest DuckDB configuration for an unrelated driver failure", %{
    context: context
  } do
    task = %RunnerTask{
      task_id: "task-browser-driver",
      task_kind: :relation_inspection,
      status: :failed,
      runner_pool: "browser",
      required_runner_release_id: "rr_release",
      retry_class: :safe,
      enqueued_at: DateTime.utc_now(),
      error: %{"type" => "browser_driver_error", "message" => "browser driver unavailable"}
    }

    Process.put(:runner_overview_tasks, [task])
    Process.put(:runner_overview_failures, [task])

    assert {:ok, %{failures: [failure]}} = RunnerOverview.get(context)
    refute failure.failure.remediation =~ "DUCKDB_ADBC_DRIVER"
  end
end
