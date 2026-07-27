defmodule FavnOrchestrator.RunServer.Execution.ActiveTaskSetTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState

  test "tracks active task ids and syncs run metadata" do
    run = run_state()

    work_set =
      run
      |> ActiveTaskSet.new()
      |> ActiveTaskSet.add_entry(%{task_id: "task_b"})
      |> ActiveTaskSet.add_entry(%{task_id: "task_a"})

    assert ActiveTaskSet.task_ids(work_set) == ["task_a", "task_b"]

    synced = ActiveTaskSet.sync_run_metadata(run, work_set)

    assert synced.runner_execution_id == nil
    assert synced.metadata.in_flight_task_ids == ["task_a", "task_b"]
  end

  test "complete_entry removes work idempotently" do
    work_set =
      run_state()
      |> ActiveTaskSet.new()
      |> ActiveTaskSet.add_entry(%{task_id: "task_a", lease: %{lease_id: "lease_a"}})

    {%{task_id: "task_a"}, next_work_set} =
      ActiveTaskSet.complete_entry(work_set, "task_a")

    assert ActiveTaskSet.task_ids(next_work_set) == []
    assert {nil, ^next_work_set} = ActiveTaskSet.complete_entry(next_work_set, "task_a")
  end

  test "missing cleanup data is already clean" do
    assert :ok = ActiveTaskSet.release_entry(%{})
    assert :ok = ActiveTaskSet.fail_entry_claim(%{}, :cancelled)
  end

  test "reads string-keyed in-flight ids from persisted metadata" do
    run = %{run_state() | metadata: %{"in_flight_task_ids" => ["task_a", nil, 7]}}

    assert ActiveTaskSet.inflight_task_ids(run) == ["task_a"]
    assert ActiveTaskSet.task_ids(ActiveTaskSet.from_run_metadata(run)) == ["task_a"]
  end

  defp run_state do
    RunState.new(
      id: "run_work_set_test",
      manifest_version_id: "mv_work_set_test",
      manifest_content_hash: "hash_work_set_test",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      asset_ref: {MyApp.Assets.WorkSet, :asset}
    )
  end
end
