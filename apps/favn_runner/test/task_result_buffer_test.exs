defmodule FavnRunner.TaskResultBufferTest do
  use ExUnit.Case, async: true

  alias FavnRunner.TaskResultBuffer

  test "bounds logs, requests flushes, and records an explicit truncation marker" do
    handler_id = "task-buffer-dropped-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :runner, :task_buffer, :dropped],
        fn event, measurements, metadata, owner ->
          send(owner, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    buffer =
      start_supervised!(
        {TaskResultBuffer,
         name: nil, max_entries: 3, flush_entries: 2, telemetry_metadata: %{runner_pool: "duckdb"}}
      )

    assert :ok = TaskResultBuffer.append(buffer, %{message: "one"})
    assert :flush = TaskResultBuffer.append(buffer, %{message: "two"})
    assert :ok = TaskResultBuffer.append(buffer, %{message: "three"})
    assert :dropped = TaskResultBuffer.append(buffer, %{message: "four"})

    assert_receive {:telemetry, [:favn, :runner, :task_buffer, :dropped], %{count: 1},
                    %{runner_pool: "duckdb"}}

    assert [
             %{message: "one"},
             %{message: "two"},
             %{message: "three"},
             %{type: :truncated, dropped_count: 1}
           ] = TaskResultBuffer.drain_logs(buffer)

    assert [] = TaskResultBuffer.drain_logs(buffer)
  end

  test "terminal results are idempotent but conflicting replacement is rejected" do
    buffer = start_supervised!({TaskResultBuffer, name: nil})

    result = %{task_id: "task_a", status: :succeeded}

    assert :ok = TaskResultBuffer.put_result(buffer, result)
    assert :ok = TaskResultBuffer.put_result(buffer, result)

    assert {:error, :result_buffer_conflict} =
             TaskResultBuffer.put_result(buffer, %{result | status: :failed})

    assert ^result = TaskResultBuffer.pending_result(buffer)
    assert :ok = TaskResultBuffer.acknowledge_result(buffer)
    assert nil == TaskResultBuffer.pending_result(buffer)
  end
end
