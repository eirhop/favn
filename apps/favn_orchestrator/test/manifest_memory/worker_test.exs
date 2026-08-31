defmodule FavnOrchestrator.ManifestMemory.WorkerTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory.Worker

  test "returns only after the monitored worker has terminated" do
    owner = self()

    assert {:ok, :complete, retained_bytes} =
             Worker.run(
               fn ->
                 send(owner, {:worker_pid, self()})
                 :complete
               end,
               1024 * 1024,
               1024 * 1024
             )

    assert retained_bytes == 4 * :erlang.external_size(:complete)
    assert_receive {:worker_pid, worker}
    refute Process.alive?(worker)
  end

  test "configures the heap ceiling to kill and include shared binaries" do
    assert {:ok, {:max_heap_size, settings}, _retained_bytes} =
             Worker.run(
               fn -> Process.info(self(), :max_heap_size) end,
               1024 * 1024,
               1024 * 1024
             )

    assert settings.kill
    assert settings.include_shared_binaries
    refute settings.error_logger
  end

  test "rejects an oversized result without returning it" do
    assert {:error, :manifest_memory_budget_exceeded} =
             Worker.run(fn -> :too_large end, 1024 * 1024, 10,
               measure: fn _result -> 11 end
             )
  end

  test "kills and drains a timed-out worker" do
    owner = self()

    assert {:error, :manifest_worker_timeout} =
             Worker.run(
               fn ->
                 send(owner, {:timed_worker, self()})
                 Process.sleep(:infinity)
               end,
               1024 * 1024,
               1024 * 1024,
               timeout: 10
             )

    assert_receive {:timed_worker, worker}
    refute Process.alive?(worker)
  end

  test "converts callback failures to a typed worker error" do
    assert {:error, :manifest_worker_failed} =
             Worker.run(fn -> raise "failed" end, 1024 * 1024, 1024 * 1024)
  end
end
