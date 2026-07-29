defmodule FavnOrchestrator.RunSubmission.SupervisorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Results.RunSubmissionWorkspacePage
  alias FavnOrchestrator.RunSubmission.Supervisor, as: RunSubmissionSupervisor

  defmodule EmptyStore do
    def page_claimable_workspaces(_query) do
      {:ok, %RunSubmissionWorkspacePage{workspace_ids: [], has_more?: false}}
    end
  end

  test "rejects unsafe concurrency configuration" do
    previous = Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{}, _stack}} =
             RunSubmissionSupervisor.start_link(
               name: unique_name(),
               task_supervisor: unique_name(),
               coordinator: unique_name(),
               store: EmptyStore,
               config: [global_concurrency: 1, per_workspace_concurrency: 2]
             )

    Process.flag(:trap_exit, previous)
  end

  test "restarts the bounded worker subtree as one unit" do
    lifecycle =
      start_supervised!({Lifecycle, name: unique_name(), shutdown_drain_timeout_ms: 1_000})

    :ok = Lifecycle.mark_accepting(lifecycle)

    task_supervisor = unique_name()
    coordinator = unique_name()

    supervisor =
      start_supervised!(
        {RunSubmissionSupervisor,
         name: unique_name(),
         task_supervisor: task_supervisor,
         coordinator: coordinator,
         lifecycle: lifecycle,
         store: EmptyStore,
         config: [
           global_concurrency: 2,
           per_workspace_concurrency: 1,
           poll_interval_ms: 1_000
         ]}
      )

    first_task_supervisor = Process.whereis(task_supervisor)
    first_coordinator = Process.whereis(coordinator)
    Process.exit(first_coordinator, :kill)

    assert eventually(fn ->
             current_task_supervisor = Process.whereis(task_supervisor)
             current_coordinator = Process.whereis(coordinator)

             is_pid(current_task_supervisor) and is_pid(current_coordinator) and
               current_task_supervisor != first_task_supervisor and
               current_coordinator != first_coordinator and Process.alive?(supervisor)
           end)
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp unique_name, do: :"run_submission_supervisor_#{System.unique_integer([:positive])}"
end
