defmodule Favn.MemoryStorageSemanticsTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage.Adapter.Memory

  setup do
    :ok = Favn.TestSetup.clear_memory_storage_adapter()
    :ok
  end

  test "same-seq same-hash writes are idempotent" do
    run = sample_run("memory-run-1", 1, %{payload: 1})

    assert :ok = Memory.put_run(run, [])
    assert :ok = Memory.put_run(run, [])
    assert {:ok, stored} = Memory.get_run(run.id, [])
    assert stored == run
  end

  test "same-seq different-hash writes return conflicting snapshot" do
    assert :ok = Memory.put_run(sample_run("memory-run-2", 1, %{payload: 1}), [])

    assert {:error, :conflicting_snapshot} =
             Memory.put_run(sample_run("memory-run-2", 1, %{payload: 2}), [])
  end

  test "lower-seq writes return stale write" do
    assert :ok = Memory.put_run(sample_run("memory-run-3", 2, %{payload: 2}), [])

    assert {:error, :stale_write} =
             Memory.put_run(sample_run("memory-run-3", 1, %{payload: 1}), [])
  end

  test "higher-seq writes replace prior state" do
    assert :ok = Memory.put_run(sample_run("memory-run-4", 1, %{payload: 1}), [])
    assert :ok = Memory.put_run(sample_run("memory-run-4", 2, %{payload: 2}), [])

    assert {:ok, stored} = Memory.get_run("memory-run-4", [])
    assert stored.event_seq == 2
    assert stored.params == %{payload: 2}
  end

  defp sample_run(id, event_seq, params) do
    %Run{
      id: id,
      target_refs: [],
      status: :running,
      submit_kind: :asset,
      replay_mode: :none,
      event_seq: event_seq,
      started_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
      params: params,
      retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []}
    }
  end
end
