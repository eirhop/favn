defmodule FavnRunner.ExecutionLifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.ExecutionPackage
  alias FavnRunner.ExecutionLifecycle

  test "finalize returns waiters and makes result awaitable" do
    lifecycle = ExecutionLifecycle.new()
    work = work("run_1")
    execution_id = "rx_1"
    worker_monitor_ref = make_ref()
    waiter_monitor_ref = make_ref()
    timer_ref = make_ref()
    from = {self(), make_ref()}

    lifecycle =
      lifecycle
      |> ExecutionLifecycle.put_running(execution_id, work, self(), worker_monitor_ref)
      |> ExecutionLifecycle.add_waiter(execution_id, from, timer_ref, waiter_monitor_ref)

    result = result(work)

    assert {[waiter], [^worker_monitor_ref], lifecycle} =
             ExecutionLifecycle.finalize(lifecycle, execution_id, result)

    assert waiter.from == from
    assert waiter.timer_ref == timer_ref
    assert waiter.monitor_ref == waiter_monitor_ref
    assert {:ok, ^result} = ExecutionLifecycle.fetch_result(lifecycle, execution_id)
    assert ExecutionLifecycle.diagnostics(lifecycle).waiters == 0
  end

  test "completed retention evicts oldest completed execution but keeps running execution" do
    lifecycle = ExecutionLifecycle.new(retention: [max_completed_executions: 1])
    running_work = work("run_running")
    first_work = work("run_first")
    second_work = work("run_second")

    lifecycle =
      lifecycle
      |> ExecutionLifecycle.put_running("rx_running", running_work, self(), make_ref())
      |> ExecutionLifecycle.put_completed("rx_first", first_work, result(first_work))
      |> ExecutionLifecycle.put_completed("rx_second", second_work, result(second_work))

    assert {:error, :execution_not_found} = ExecutionLifecycle.fetch_result(lifecycle, "rx_first")
    assert {:ok, _execution} = ExecutionLifecycle.fetch_execution(lifecycle, "rx_running")
    assert {:ok, _result} = ExecutionLifecycle.fetch_result(lifecycle, "rx_second")

    diagnostics = ExecutionLifecycle.diagnostics(lifecycle)
    assert diagnostics.in_flight_executions == 1
    assert diagnostics.completed_executions == 1
    assert diagnostics.retention.evicted_completed_executions == 1
  end

  test "pre-completed execution has no worker start timestamp" do
    lifecycle = ExecutionLifecycle.new()
    work = work("run_preflight_failed")
    result = result(work)

    lifecycle = ExecutionLifecycle.put_completed(lifecycle, "rx_preflight_failed", work, result)

    assert {:ok, execution} = ExecutionLifecycle.fetch_execution(lifecycle, "rx_preflight_failed")
    assert execution.started_at == nil
    assert %DateTime{} = execution.completed_at
    assert execution.work == nil
    assert byte_size(execution.work_fingerprint) == 32
  end

  test "completed execution retains a fingerprint instead of a large execution package" do
    package = %ExecutionPackage{
      content_hash: String.duplicate("a", 64),
      asset_ref: {__MODULE__, :asset},
      sql_execution: %{sql: String.duplicate("x", 4 * 1_024 * 1_024)}
    }

    work = %{work("run_large_package") | execution_package: package}

    lifecycle =
      ExecutionLifecycle.new()
      |> ExecutionLifecycle.put_completed("rx_large_package", work, result(work))

    assert {:ok, execution} =
             ExecutionLifecycle.fetch_execution(lifecycle, "rx_large_package")

    assert execution.work == nil
    assert execution.work_fingerprint == RunnerWork.replay_fingerprint(work)
    assert :erlang.external_size(execution) < 100_000
  end

  test "completed retention is bounded per entry and in aggregate bytes" do
    large_result = fn work ->
      %{result(work) | metadata: %{payload: String.duplicate("x", 50_000)}}
    end

    first_work = work("run_byte_first")
    second_work = work("run_byte_second")

    per_entry_lifecycle =
      ExecutionLifecycle.new(
        retention: [
          max_completed_executions: 10,
          max_completed_execution_bytes: 2_000,
          max_completed_bytes: 100_000
        ]
      )
      |> ExecutionLifecycle.put_completed("rx_byte_first", first_work, large_result.(first_work))

    assert {:ok, execution} =
             ExecutionLifecycle.fetch_execution(per_entry_lifecycle, "rx_byte_first")

    assert execution.retained_bytes <= 2_000
    assert execution.result_truncated

    lifecycle =
      ExecutionLifecycle.new(
        retention: [
          max_completed_executions: 10,
          max_completed_execution_bytes: 2_000,
          max_completed_bytes: 1
        ]
      )
      |> ExecutionLifecycle.put_completed("rx_byte_first", first_work, large_result.(first_work))
      |> ExecutionLifecycle.put_completed(
        "rx_byte_second",
        second_work,
        large_result.(second_work)
      )

    diagnostics = ExecutionLifecycle.diagnostics(lifecycle)
    assert diagnostics.retention.completed_bytes <= 1
    assert diagnostics.retention.truncated_completed_executions == 2
    assert diagnostics.retention.evicted_completed_executions >= 1
  end

  test "replay fingerprint uses the verified execution package identity, not its payload" do
    package = %ExecutionPackage{
      content_hash: String.duplicate("a", 64),
      asset_ref: {__MODULE__, :asset},
      sql_execution: %{sql: String.duplicate("x", 4 * 1_024 * 1_024)}
    }

    work = %{work("run_package_fingerprint") | execution_package: package}

    same_identity = %{
      work
      | execution_package: %{package | sql_execution: %{sql: "not rehashed"}}
    }

    changed_identity = %{
      work
      | execution_package: %{package | content_hash: String.duplicate("b", 64)}
    }

    assert RunnerWork.replay_fingerprint(work) == RunnerWork.replay_fingerprint(same_identity)
    refute RunnerWork.replay_fingerprint(work) == RunnerWork.replay_fingerprint(changed_identity)
  end

  test "log and event buffers drop oldest entries under configured bounds" do
    lifecycle =
      ExecutionLifecycle.new(retention: [max_logs_per_execution: 2, max_events_per_execution: 1])

    work = work("run_buffers")
    execution_id = "rx_buffers"

    lifecycle = ExecutionLifecycle.put_running(lifecycle, execution_id, work, self(), make_ref())

    {_subscribers, lifecycle} =
      ExecutionLifecycle.append_log(lifecycle, execution_id, %{sequence: 1})

    {_subscribers, lifecycle} =
      ExecutionLifecycle.append_log(lifecycle, execution_id, %{sequence: 2})

    {_subscribers, lifecycle} =
      ExecutionLifecycle.append_log(lifecycle, execution_id, %{sequence: 3})

    lifecycle = ExecutionLifecycle.append_event(lifecycle, execution_id, %{sequence: 1})
    lifecycle = ExecutionLifecycle.append_event(lifecycle, execution_id, %{sequence: 2})

    execution = lifecycle.executions[execution_id]
    assert Enum.reverse(execution.logs) == [%{sequence: 2}, %{sequence: 3}]
    assert Enum.reverse(execution.events) == [%{sequence: 2}]

    diagnostics = ExecutionLifecycle.diagnostics(lifecycle)
    assert diagnostics.retention.dropped_logs == 1
    assert diagnostics.retention.dropped_events == 1
  end

  test "log and event buffers also enforce byte budgets" do
    lifecycle =
      ExecutionLifecycle.new(
        retention: [
          max_logs_per_execution: 100,
          max_events_per_execution: 100,
          max_log_bytes_per_execution: 200,
          max_event_bytes_per_execution: 200
        ]
      )

    work = work("run_byte_buffers")

    lifecycle =
      ExecutionLifecycle.put_running(lifecycle, "rx_byte_buffers", work, self(), make_ref())

    {_subscribers, lifecycle} =
      ExecutionLifecycle.append_log(lifecycle, "rx_byte_buffers", %{
        payload: String.duplicate("l", 500)
      })

    lifecycle =
      ExecutionLifecycle.append_event(lifecycle, "rx_byte_buffers", %{
        payload: String.duplicate("e", 500)
      })

    execution = lifecycle.executions["rx_byte_buffers"]
    assert execution.logs == []
    assert execution.events == []

    diagnostics = ExecutionLifecycle.diagnostics(lifecycle)
    assert diagnostics.retention.dropped_logs == 1
    assert diagnostics.retention.dropped_events == 1
  end

  test "subscriber monitor cleanup removes all execution memberships" do
    lifecycle = ExecutionLifecycle.new()
    work = work("run_subscriber")
    subscriber = self()
    monitor_ref = make_ref()

    lifecycle = ExecutionLifecycle.put_running(lifecycle, "rx_1", work, self(), make_ref())
    lifecycle = ExecutionLifecycle.put_running(lifecycle, "rx_2", work, self(), make_ref())

    assert {:ok, [], [], lifecycle} =
             ExecutionLifecycle.subscribe_logs(lifecycle, "rx_1", subscriber, monitor_ref)

    assert {:ok, [], [unused_monitor_ref], lifecycle} =
             ExecutionLifecycle.subscribe_logs(lifecycle, "rx_2", subscriber, make_ref())

    assert is_reference(unused_monitor_ref)
    assert ExecutionLifecycle.diagnostics(lifecycle).log_subscriptions == 2

    lifecycle = ExecutionLifecycle.remove_subscriber_monitor(lifecycle, monitor_ref)

    diagnostics = ExecutionLifecycle.diagnostics(lifecycle)
    assert diagnostics.log_subscribers == 0
    assert diagnostics.log_subscriptions == 0
  end

  defp work(run_id) do
    %RunnerWork{
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      run_id: run_id,
      manifest_version_id: "mv_" <> run_id,
      manifest_content_hash: "hash_" <> run_id,
      asset_ref: {__MODULE__, :asset}
    }
  end

  defp result(%RunnerWork{} = work) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      status: :ok,
      asset_results: [],
      metadata: %{}
    }
  end
end
