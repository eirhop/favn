defmodule FavnOrchestrator.RunServer.Execution.RunWorkSetTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunState

  test "tracks active execution ids and syncs run metadata" do
    run = run_state()

    work_set =
      run
      |> RunWorkSet.new()
      |> RunWorkSet.add_entry(%{execution_id: "exec_b"})
      |> RunWorkSet.add_entry(%{runner_execution_id: "exec_a"})

    assert RunWorkSet.execution_ids(work_set) == ["exec_a", "exec_b"]

    synced = RunWorkSet.sync_run_metadata(run, work_set)

    assert synced.runner_execution_id == "exec_a"
    assert synced.metadata.in_flight_execution_ids == ["exec_a", "exec_b"]
  end

  test "complete_entry removes work idempotently" do
    work_set =
      run_state()
      |> RunWorkSet.new()
      |> RunWorkSet.add_entry(%{execution_id: "exec_a", lease: %{lease_id: "lease_a"}})

    {%{execution_id: "exec_a"}, next_work_set} = RunWorkSet.complete_entry(work_set, "exec_a")
    assert RunWorkSet.execution_ids(next_work_set) == []

    assert {nil, ^next_work_set} = RunWorkSet.complete_entry(next_work_set, "exec_a")
  end

  test "missing cleanup data is already clean" do
    assert :ok = RunWorkSet.release_entry(%{})
    assert :ok = RunWorkSet.fail_entry_claim(%{}, :cancelled)
  end

  test "reads string-keyed in-flight ids from persisted metadata" do
    run = %{run_state() | metadata: %{"in_flight_execution_ids" => ["exec_a", nil, 7]}}

    assert RunWorkSet.inflight_execution_ids(run) == ["exec_a"]
    assert RunWorkSet.execution_ids(RunWorkSet.from_run_metadata(run)) == ["exec_a"]
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
