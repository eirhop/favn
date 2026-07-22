defmodule FavnOrchestrator.RunServer.SnapshotsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  test "clearing in-flight execution metadata does not consume an event sequence" do
    run =
      RunState.new(
        id: "run-snapshot-clear",
        manifest_version_id: "manifest-v1",
        manifest_content_hash: String.duplicate("a", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {MyApp.Asset, :asset},
        metadata: %{in_flight_execution_ids: ["execution-a", "execution-b"]}
      )
      |> RunState.transition(status: :running)

    cleared = Snapshots.clear_inflight_executions(run, ["execution-a", nil])

    assert cleared.event_seq == run.event_seq
    assert cleared.metadata.in_flight_execution_ids == ["execution-b"]
    assert cleared.snapshot_hash != run.snapshot_hash
  end

  test "clearing an empty execution set leaves sequence authority to the next event" do
    run =
      RunState.new(
        id: "run-snapshot-empty-clear",
        manifest_version_id: "manifest-v1",
        manifest_content_hash: String.duplicate("b", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {MyApp.Asset, :asset}
      )
      |> RunState.transition(status: :running)

    cleared = Snapshots.clear_inflight_executions(run, [])

    assert cleared.event_seq == run.event_seq
    assert cleared.metadata.in_flight_execution_ids == []
  end
end
