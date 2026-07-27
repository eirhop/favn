defmodule FavnOrchestrator.RunServer.SnapshotsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  test "clearing in-flight task metadata does not consume an event sequence" do
    run =
      RunState.new(
        id: "run-snapshot-clear",
        manifest_version_id: "manifest-v1",
        manifest_content_hash: String.duplicate("a", 64),
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: {MyApp.Asset, :asset},
        metadata: %{active_runner_task_ids: ["task-a", "task-b"]}
      )
      |> RunState.transition(status: :running)

    cleared = Snapshots.clear_inflight_tasks(run, ["task-a", nil])

    assert cleared.event_seq == run.event_seq
    assert cleared.metadata.active_runner_task_ids == ["task-b"]
    assert cleared.snapshot_hash != run.snapshot_hash
  end

  test "clearing an empty task set leaves sequence authority to the next event" do
    run =
      RunState.new(
        id: "run-snapshot-empty-clear",
        manifest_version_id: "manifest-v1",
        manifest_content_hash: String.duplicate("b", 64),
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: {MyApp.Asset, :asset}
      )
      |> RunState.transition(status: :running)

    cleared = Snapshots.clear_inflight_tasks(run, [])

    assert cleared.event_seq == run.event_seq
    assert cleared.metadata.active_runner_task_ids == []
  end
end
