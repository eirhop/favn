defmodule FavnRunner.ExecutionLifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
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
