defmodule Favn.PostgresRunSerializerTest do
  use ExUnit.Case, async: true

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Storage.Postgres.RunSerializer
  alias Favn.Window.Key

  test "snapshot_from_run/1 and run_from_snapshot/1 round-trip run structs" do
    finished_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    window_key =
      Key.new!(
        :day,
        DateTime.from_naive!(~N[2026-04-13 00:00:00], "Etc/UTC"),
        "Etc/UTC"
      )

    ref = {Favn.PostgresRunSerializerTest, :asset}
    node_key = {ref, window_key}

    result = %AssetResult{
      ref: ref,
      stage: 0,
      status: :ok,
      started_at: finished_at,
      finished_at: finished_at,
      duration_ms: 10,
      meta: %{rows: 12},
      attempt_count: 1,
      max_attempts: 1,
      attempts: [
        %{
          attempt: 1,
          started_at: finished_at,
          finished_at: finished_at,
          duration_ms: 10,
          status: :ok,
          meta: %{rows: 12},
          error: nil
        }
      ]
    }

    run = %Run{
      id: "pg-serializer-1",
      target_refs: [ref],
      status: :ok,
      submit_kind: :asset,
      replay_mode: :none,
      event_seq: 3,
      started_at: finished_at,
      finished_at: finished_at,
      params: %{anchor: :day},
      retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []},
      node_results: %{node_key => result},
      asset_results: %{ref => result}
    }

    snapshot = RunSerializer.snapshot_from_run(run)

    assert {:ok, decoded} = RunSerializer.run_from_snapshot(snapshot)
    assert decoded == run
  end
end
