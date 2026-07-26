defmodule FavnOrchestrator.RunSubmission.CoordinatorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Results.RunSubmissionWorkspacePage
  alias FavnOrchestrator.RunSubmission.Coordinator

  defmodule Store do
    def page_claimable_workspaces(query) do
      ids =
        Agent.get(agent(), & &1)
        |> Enum.filter(&(is_nil(query.after) or &1 > query.after))

      selected = Enum.take(ids, query.limit)
      has_more? = length(ids) > query.limit

      {:ok,
       %RunSubmissionWorkspacePage{
         workspace_ids: selected,
         has_more?: has_more?,
         next: if(has_more?, do: List.last(selected))
       }}
    end

    def take(workspace_id), do: Agent.update(agent(), &List.delete(&1, workspace_id))
    defp agent, do: :persistent_term.get({__MODULE__, :agent})
  end

  defmodule BlockingWorker do
    def run(workspace_id, _opts) do
      Store.take(workspace_id)
      send(:persistent_term.get({__MODULE__, :test}), {:worker_started, workspace_id, self()})

      receive do
        :release -> :ok
      end
    end
  end

  defmodule StickyStore do
    def page_claimable_workspaces(_query) do
      {:ok,
       %RunSubmissionWorkspacePage{
         workspace_ids: ["workspace-hot"],
         has_more?: false
       }}
    end
  end

  defmodule StickyWorker do
    def run(workspace_id, _opts) do
      send(
        :persistent_term.get({__MODULE__, :test}),
        {:sticky_worker_started, workspace_id, self()}
      )

      receive do
        :release -> :ok
      end
    end
  end

  setup do
    {:ok, agent} = Agent.start_link(fn -> ["workspace-a", "workspace-b", "workspace-c"] end)
    :persistent_term.put({Store, :agent}, agent)
    :persistent_term.put({BlockingWorker, :test}, self())
    :persistent_term.put({StickyWorker, :test}, self())

    lifecycle =
      start_supervised!({Lifecycle, name: unique_name(), shutdown_drain_timeout_ms: 1_000})

    :ok = Lifecycle.mark_accepting(lifecycle)

    task_supervisor =
      start_supervised!({Task.Supervisor, name: unique_name(), max_children: 2})

    on_exit(fn ->
      :persistent_term.erase({Store, :agent})
      :persistent_term.erase({BlockingWorker, :test})
      :persistent_term.erase({StickyWorker, :test})
    end)

    {:ok, lifecycle: lifecycle, task_supervisor: task_supervisor}
  end

  test "enforces global and per-workspace bounds while rotating workspaces", fixture do
    coordinator =
      start_supervised!(
        {Coordinator,
         name: unique_name(),
         task_supervisor: fixture.task_supervisor,
         store: Store,
         lifecycle: fixture.lifecycle,
         worker: BlockingWorker,
         worker_options: [],
         global_concurrency: 2,
         per_workspace_concurrency: 1,
         workspace_page_size: 2,
         poll_interval_ms: 1_000}
      )

    assert_receive {:worker_started, "workspace-a", first}
    assert_receive {:worker_started, "workspace-b", second}
    refute_receive {:worker_started, "workspace-c", _pid}, 25

    assert %{
             active: 2,
             global_concurrency: 2,
             per_workspace_concurrency: 1,
             workspace_counts: %{"workspace-a" => 1, "workspace-b" => 1}
           } = Coordinator.diagnostics(coordinator)

    send(first, :release)
    assert_receive {:worker_started, "workspace-c", third}

    send(second, :release)
    send(third, :release)
  end

  test "saturates one busy workspace only to its configured cap", fixture do
    task_supervisor =
      start_supervised!(
        Supervisor.child_spec(
          {Task.Supervisor, name: unique_name(), max_children: 3},
          id: :sticky_run_submission_task_supervisor
        )
      )

    coordinator =
      start_supervised!(
        {Coordinator,
         name: unique_name(),
         task_supervisor: task_supervisor,
         store: StickyStore,
         lifecycle: fixture.lifecycle,
         worker: StickyWorker,
         worker_options: [],
         global_concurrency: 3,
         per_workspace_concurrency: 2,
         workspace_page_size: 2,
         poll_interval_ms: 1_000},
        id: :sticky_run_submission_coordinator
      )

    assert_receive {:sticky_worker_started, "workspace-hot", first}
    assert_receive {:sticky_worker_started, "workspace-hot", second}
    refute_receive {:sticky_worker_started, "workspace-hot", _third}, 25

    assert %{active: 2, workspace_counts: %{"workspace-hot" => 2}} =
             Coordinator.diagnostics(coordinator)

    send(first, :release)
    send(second, :release)
  end

  defp unique_name, do: :"run_submission_test_#{System.unique_integer([:positive])}"
end
